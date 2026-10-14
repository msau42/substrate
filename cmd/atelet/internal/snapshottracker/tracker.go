// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package snapshottracker tracks each actor's snapshots on an atelet node, and
// uploads them in the background.
package snapshottracker

import (
	"context"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
)

// Fixed until they become flags or per-request settings on UploadConfig.
const (
	// snapshotUploadDelay is how long after the oldest not-yet-durable
	// snapshot of an actor was taken atelet uploads the actor's latest local
	// snapshot, under UPLOAD_MODE_DEFERRED.
	snapshotUploadDelay = 5 * time.Minute
	// snapshotRetryDelay is how long atelet holds an actor's background
	// uploads back after one fails. Each further consecutive failure doubles
	// it, up to maxSnapshotRetryDelay.
	snapshotRetryDelay    = time.Minute
	maxSnapshotRetryDelay = 10 * time.Minute
	// snapshotWorkers is how many background uploads run at once on a node.
	snapshotWorkers = 4
)

// Store moves an actor's snapshots between this node and object
// storage.
type Store interface {
	// UploadLocalSnapshot makes ref durable at ref.URI from its local copy,
	// unless a manifest is already committed there. Either copy must hold at
	// least minFidelity. It returns the fidelity the durable copy's manifest
	// records, UNSPECIFIED when it records none, and whether it uploaded.
	UploadLocalSnapshot(ctx context.Context, actorUID string, ref SnapshotRef, minFidelity ateletpb.SnapshotFidelity) (ateletpb.SnapshotFidelity, bool, error)
}

// Tracker tracks each actor's snapshots on this node, and uploads them in the
// background.
//
// Background work runs off a workqueue of actor UIDs. Each change to an
// actor's state queues the actor for when its next upload is due:
//  1. A deferred checkpoint (CommitLocal) records the new snapshot as
//     LatestLocal. Its upload is due uploadDelay after UndurableSince.
//  2. A worker that takes the actor from the queue uploads LatestLocal if
//     the upload is due then. When it is not due yet, the worker queues the
//     actor again for when it is: the queue holds an actor until the
//     earliest time it was queued for, so a due time that moves later only
//     costs a worker that check.
//  3. The worker uploads the LatestLocal of when it runs, so snapshots
//     superseded while the actor waits are never uploaded.
//  4. The queue hands an actor to one worker at a time, and the number of
//     workers bounds how many background uploads run at once.
//  5. An upload, background or by PromoteSnapshot, leaves LatestLocal on disk
//     so a same-node Restore still reads it.
//
// PromoteSnapshot runs in the calling goroutine rather than on a worker, so
// an explicit control-plane call does not wait behind other actors'
// background work. Nothing cancels an actor's upload in progress:
// PromoteSnapshot and Forget wait for it to end, even an upload of an older
// snapshot, and CommitDurable lets it run on. A worker leaves an actor with
// an upload in progress to that upload's end, which queues the actor again.
type Tracker struct {
	// ctx bounds background uploads, and shuts the queue down when it ends.
	ctx   context.Context
	store Store
	// clock is what due times are measured on, and the queue waits on.
	clock clock.WithTicker

	// The upload delay, retry backoff, and worker count are initialized from
	// the package constants in New, and overridden in tests.
	//
	// uploadDelay is how long after UndurableSince a deferred local snapshot
	// becomes due for background upload.
	uploadDelay time.Duration
	// retries says how long an actor's background work is held back after a
	// failure: longer with each consecutive failure for the actor.
	retries workqueue.TypedRateLimiter[string]
	// workers is how many background uploads run at once.
	workers int

	// queue holds the UIDs of actors with background work scheduled.
	queue workqueue.TypedDelayingInterface[string]
	// workersDone counts the workers still running.
	workersDone sync.WaitGroup

	mu sync.Mutex
	// actors maps actorUID to the actor's entry. Read and change it through
	// findLocked, findOrAddLocked, addLocked, dropLocked, and tracksLocked.
	actors map[string]*actorSnapshots
}

// New returns a tracker over store that measures time on clk.
// Its workers run once Start is called, until ctx ends.
func New(ctx context.Context, store Store, clk clock.WithTicker) *Tracker {
	t := &Tracker{
		ctx:         ctx,
		store:       store,
		clock:       clk,
		uploadDelay: snapshotUploadDelay,
		retries:     workqueue.NewTypedItemExponentialFailureRateLimiter[string](snapshotRetryDelay, maxSnapshotRetryDelay),
		workers:     snapshotWorkers,
		queue:       workqueue.NewTypedDelayingQueueWithConfig(workqueue.TypedDelayingQueueConfig[string]{Clock: clk}),
		actors:      map[string]*actorSnapshots{},
	}
	context.AfterFunc(ctx, t.queue.ShutDown)
	return t
}

// Start starts the workers that upload actors' snapshots in the background.
func (t *Tracker) Start() {
	for range t.workers {
		t.workersDone.Go(t.runWorker)
	}
}

// Wait blocks until the workers have stopped, after the tracker's context
// ends.
func (t *Tracker) Wait() {
	t.workersDone.Wait()
}

// runWorker runs the background work of the actors it takes from the queue,
// until the queue shuts down.
func (t *Tracker) runWorker() {
	for {
		actorUID, shutdown := t.queue.Get()
		if shutdown {
			return
		}
		t.syncActor(actorUID)
		t.queue.Done(actorUID)
	}
}

// syncActor runs the actor's background upload if one is due, and otherwise
// queues the actor for when it is.
func (t *Tracker) syncActor(actorUID string) {
	t.mu.Lock()
	a := t.findLocked(actorUID)
	if a == nil || a.op != nil || t.ctx.Err() != nil {
		// Forgotten, or an upload is in progress, whose end queues the actor
		// again, or atelet is shutting down.
		t.mu.Unlock()
		return
	}
	if due, ok := a.uploadDue(t.uploadDelay); ok && !due.After(t.clock.Now()) {
		op := t.beginOpLocked(a, *a.LatestLocal)
		t.mu.Unlock()
		t.backgroundUpload(t.ctx, a, op)
		return
	}
	t.requeueLocked(a)
	t.mu.Unlock()
}

// requeueLocked queues the actor for when its next background upload is due,
// if it has one.
func (t *Tracker) requeueLocked(a *actorSnapshots) {
	if due, ok := a.uploadDue(t.uploadDelay); ok {
		t.queue.AddAfter(a.actorUID, due.Sub(t.clock.Now()))
	}
}

// beginOpLocked records an upload of target as the actor's operation in
// progress.
func (t *Tracker) beginOpLocked(a *actorSnapshots, target SnapshotRef) *actorOp {
	a.op = &actorOp{target: target, done: make(chan struct{})}
	return a.op
}

// endOpLocked records that the actor's operation in progress has ended, and
// queues the actor for its next background upload. The caller closes the
// operation's done channel once it is done with the actor's local snapshots.
func (t *Tracker) endOpLocked(a *actorSnapshots) {
	a.op = nil
	if t.tracksLocked(a) {
		t.requeueLocked(a)
	}
}

// waitOpLocked waits, releasing t.mu meanwhile, for op to end.
func (t *Tracker) waitOpLocked(ctx context.Context, op *actorOp) error {
	t.mu.Unlock()
	defer t.mu.Lock()
	select {
	case <-op.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// waitIdleLocked waits, releasing t.mu meanwhile, until the actor has no
// upload in progress, and returns the actor's entry, which may be nil, with
// t.mu held.
func (t *Tracker) waitIdleLocked(ctx context.Context, actorUID string) (*actorSnapshots, error) {
	for {
		a := t.findLocked(actorUID)
		if a == nil || a.op == nil {
			return a, nil
		}
		if err := t.waitOpLocked(ctx, a.op); err != nil {
			return nil, err
		}
	}
}

// backOffLocked holds the actor's next background upload back after one
// failed, longer with each consecutive failure, and returns until when.
func (t *Tracker) backOffLocked(a *actorSnapshots) time.Time {
	a.retryAt = t.clock.Now().Add(t.retries.When(a.actorUID))
	return a.retryAt
}

// resetBackoffLocked lifts the hold backOffLocked put on the actor's
// background work.
func (t *Tracker) resetBackoffLocked(a *actorSnapshots) {
	a.retryAt = time.Time{}
	t.retries.Forget(a.actorUID)
}

// findLocked returns the actor's entry, or nil when atelet tracks nothing of
// the actor.
func (t *Tracker) findLocked(actorUID string) *actorSnapshots {
	return t.actors[actorUID]
}

// findOrAddLocked returns the actor's entry, adding an empty one when there is
// none, so it never returns nil.
func (t *Tracker) findOrAddLocked(actorUID string) *actorSnapshots {
	a := t.findLocked(actorUID)
	if a == nil {
		a = &actorSnapshots{actorUID: actorUID}
		t.addLocked(a)
	}
	return a
}

// addLocked makes a its actor's entry.
func (t *Tracker) addLocked(a *actorSnapshots) {
	t.actors[a.actorUID] = a
}

// dropLocked removes the actor's entry.
func (t *Tracker) dropLocked(actorUID string) {
	delete(t.actors, actorUID)
}

// tracksLocked reports whether a is still its actor's entry: false once
// Forget has dropped the actor, or a failed Promote the entry it added, even
// if a new entry has been added since.
func (t *Tracker) tracksLocked(a *actorSnapshots) bool {
	return t.actors[a.actorUID] == a
}

// stateLocked returns what atelet holds for the actor.
func (t *Tracker) stateLocked(a *actorSnapshots) *ateletpb.ActorSnapshotState {
	st := &ateletpb.ActorSnapshotState{
		LatestLocal:   a.LatestLocal.report(false),
		Uploading:     a.uploading().report(false),
		LatestDurable: a.LatestDurable.report(true),
	}
	if due, ok := a.uploadDue(t.uploadDelay); ok {
		st.UploadDueTime = timestamppb.New(due)
	}
	return st
}

// State returns what atelet holds for the actor.
func (t *Tracker) State(actorUID string) *ateletpb.ActorSnapshotState {
	t.mu.Lock()
	defer t.mu.Unlock()
	if a := t.findLocked(actorUID); a != nil {
		return t.stateLocked(a)
	}
	return &ateletpb.ActorSnapshotState{}
}
