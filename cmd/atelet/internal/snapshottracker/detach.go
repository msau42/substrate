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

package snapshottracker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

// errActorRunning is what a detach of an actor that runs on this node fails
// with, wrapped in Aborted.
var errActorRunning = errors.New("actor runs on this node")

// beginDetachLocked begins a detach of the actor's latest local snapshot. It
// reports whether that snapshot is durable already, so the detach need not
// upload it.
func (t *Tracker) beginDetachLocked(a *actorSnapshots) (*actorOp, bool) {
	target := *a.LatestLocal
	return t.beginOpLocked(a, opDetach, target), a.isLatestDurable(target)
}

// postponeDetachLocked checks again a detach delay later whether to detach
// the actor.
func (t *Tracker) postponeDetachLocked(a *actorSnapshots) {
	a.DetachAt = t.clock.Now().Add(t.detachDelay)
	t.requeueLocked(a)
}

// Detach makes the actor's latest local snapshot durable, then deletes the
// actor's local snapshots. It is a no-op when atelet holds no local snapshot
// for the actor. It fails with Aborted while a Run, Restore, or Checkpoint of
// the actor is in progress or a workload of it runs on this node, and with
// Unavailable when atelet cannot tell. It runs in the calling goroutine rather
// than on a worker.
func (t *Tracker) Detach(ctx context.Context, actorUID string) (*ateletpb.ActorSnapshotState, error) {
	t.mu.Lock()
	// If an upload of the latest local snapshot ends meanwhile, the detach
	// need not upload it again.
	a, err := t.waitIdleLocked(ctx, actorUID, checkNotHeld)
	if err != nil {
		t.mu.Unlock()
		return nil, err
	}
	if a == nil || a.LatestLocal == nil {
		st := &ateletpb.ActorSnapshotState{}
		if a != nil {
			st = t.stateLocked(a)
		}
		t.mu.Unlock()
		return st, nil
	}
	op, alreadyDurable := t.beginDetachLocked(a)
	t.mu.Unlock()
	return t.finishDetach(ctx, a, op, alreadyDurable, false)
}

// checkNotHeld fails with Aborted while a Run, Restore, or Checkpoint of the
// actor is in progress on this node.
func checkNotHeld(a *actorSnapshots) error {
	if a.holds > 0 {
		return apierror.Aborted("actor %s is in use on this node", a.actorUID)
	}
	return nil
}

// finishDetach runs op, a detach of the actor's latest local snapshot: unless
// a workload of the actor runs on this node, it uploads the snapshot unless
// alreadyDurable, then deletes the actor's local snapshots. background reports
// whether a worker runs it, rather than a DetachActor call.
func (t *Tracker) finishDetach(ctx context.Context, a *actorSnapshots, op *actorOp, alreadyDurable, background bool) (*ateletpb.ActorSnapshotState, error) {
	target := op.target
	start := t.clock.Now()
	var fidelity ateletpb.SnapshotFidelity
	uploaded := false
	running := false
	err := ctx.Err()
	if err == nil {
		// A workload restored from the local snapshots may still be reading
		// them. No Run, Restore, or Checkpoint of the actor runs while op is
		// in progress.
		running, err = t.runs(ctx, a.actorUID)
	}
	if err == nil && running {
		err = apierror.Aborted("actor %s: %w", a.actorUID, errActorRunning)
	}
	if err == nil && !alreadyDurable {
		fidelity, uploaded, err = t.store.UploadLocalSnapshot(ctx, a.actorUID, target, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED)
	}

	// Record durability and clear LatestLocal, then prune the actor's local
	// snapshots. Callers waiting for the detach stay blocked until pruning
	// finishes below.
	t.mu.Lock()
	switch {
	case err == nil:
		t.recordUploadLocked(a, target, fidelity, uploaded)
		a.LatestLocal = nil
		a.UndurableSince, a.DetachAt = time.Time{}, time.Time{}
	case running:
		// The workload may stop without atelet being told, as when it
		// crashes: check again a detach delay later.
		a.DetachAt = t.clock.Now().Add(t.detachDelay)
	case ctx.Err() == nil:
		// Detach again, and upload no sooner, once the retry delay has
		// passed.
		a.DetachAt = t.backOffLocked(a)
	}
	t.mu.Unlock()

	if err == nil {
		if perr := PruneLocalCheckpoints(t.ctx, a.actorUID); perr != nil {
			slog.WarnContext(t.ctx, "failed to delete local checkpoints after detach", slog.String("actorUID", a.actorUID), slog.Any("err", perr))
		}
	}

	t.mu.Lock()
	t.endOpLocked(a)
	st := t.stateLocked(a)
	t.mu.Unlock()
	close(op.done)

	if err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "Detached actor",
		slog.String("actorUID", a.actorUID),
		slog.String("snapshot", target.UUID),
		slog.Int("generation", int(target.Generation)),
		slog.Bool("uploaded", uploaded),
		slog.Bool("requested", !background),
		slog.Duration("elapsed", t.clock.Since(start)))
	return st, nil
}
