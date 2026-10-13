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
	"time"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

// CommitLocal records snap, just stored locally under UPLOAD_MODE_DEFERRED, as
// the actor's latest local snapshot, and schedules its upload and the actor's
// detach. It returns the local snapshots to keep, snap and one still being
// uploaded, and the actor's state.
func (t *Tracker) CommitLocal(actorUID string, snap SnapshotRef) ([]string, *ateletpb.ActorSnapshotState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	a := t.findOrAddLocked(actorUID)
	local := snap
	a.LatestLocal = &local
	if a.UndurableSince.IsZero() {
		a.UndurableSince = snap.CommitTime
	}
	a.DetachAt = t.clock.Now().Add(t.detachDelay)
	keep := []string{snap.UUID}
	if up := a.uploading(); up != nil {
		keep = append(keep, up.UUID)
	}
	t.requeueLocked(a)
	t.saveLocked(a)
	return keep, t.stateLocked(a)
}

// CommitDurable records snap, uploaded during Checkpoint under
// UPLOAD_MODE_IMMEDIATE, as the actor's latest snapshot. The actor keeps no
// local snapshot, but an older snapshot's upload in progress runs on, and
// deletes that snapshot's local copy when it ends. It returns the local
// snapshots to keep, that one if any, and the actor's state.
func (t *Tracker) CommitDurable(actorUID string, snap SnapshotRef) ([]string, *ateletpb.ActorSnapshotState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	a := t.findOrAddLocked(actorUID)
	durable := snap
	a.LatestDurable = &durable
	// TODO: change this when we add detach.
	a.LatestLocal = nil
	a.UndurableSince, a.DetachAt = time.Time{}, time.Time{}
	t.resetBackoffLocked(a)
	var keep []string
	if up := a.uploading(); up != nil {
		keep = append(keep, up.UUID)
	}
	t.saveLocked(a)
	return keep, t.stateLocked(a)
}

// BeginStart records that a Run or Restore is starting the actor on this node,
// once any detach in progress has finished, and returns the func to call when
// the start ends, whether or not it succeeded. atelet does not detach the
// actor in between.
func (t *Tracker) BeginStart(ctx context.Context, actorUID string) (end func(), err error) {
	return t.hold(ctx, actorUID)
}

// BeginCheckpoint records that a Checkpoint of the actor is in progress on this
// node, once any detach in progress has finished, and returns the func to call
// when the checkpoint ends, whether or not it succeeded. atelet does not
// detach the actor in between, so a detach cannot delete the local snapshot
// the checkpoint writes.
func (t *Tracker) BeginCheckpoint(ctx context.Context, actorUID string) (end func(), err error) {
	return t.hold(ctx, actorUID)
}

// hold waits for any detach of the actor to finish, holds detaches of it off,
// and returns the func that releases the hold.
func (t *Tracker) hold(ctx context.Context, actorUID string) (release func(), err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	a, err := t.waitForDetachLocked(ctx, actorUID)
	if err != nil {
		return nil, err
	}
	if a == nil {
		// atelet holds no snapshot of the actor to detach.
		return func() {}, nil
	}
	a.holds++
	return func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		a.holds--
	}, nil
}

// Forget waits, as long as ctx allows, for the actor's upload or detach in
// progress to end, then drops the actor's state, in memory and on disk. An
// operation thus never finds its actor dropped, and a worker that takes the
// actor from the queue afterwards finds nothing to do.
func (t *Tracker) Forget(ctx context.Context, actorUID string) error {
	t.mu.Lock()
	if _, err := t.waitIdleLocked(ctx, actorUID, nil); err != nil {
		t.mu.Unlock()
		return err
	}
	t.dropLocked(actorUID)
	t.mu.Unlock()
	t.retries.Forget(actorUID)
	return removeSnapshotState(actorUID)
}
