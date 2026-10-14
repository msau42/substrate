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
// the actor's latest local snapshot, and schedules its upload. It returns the
// local snapshots to keep, snap and one still being uploaded, and the actor's
// state.
func (t *Tracker) CommitLocal(actorUID string, snap SnapshotRef) ([]string, *ateletpb.ActorSnapshotState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	a := t.findOrAddLocked(actorUID)
	local := snap
	a.LatestLocal = &local
	if a.UndurableSince.IsZero() {
		a.UndurableSince = snap.CommitTime
	}
	keep := []string{snap.UUID}
	if up := a.uploading(); up != nil {
		keep = append(keep, up.UUID)
	}
	t.requeueLocked(a)
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
	a.UndurableSince = time.Time{}
	t.resetBackoffLocked(a)
	var keep []string
	if up := a.uploading(); up != nil {
		keep = append(keep, up.UUID)
	}
	return keep, t.stateLocked(a)
}

// Forget waits, as long as ctx allows, for the actor's upload in progress to
// end, then drops the actor's state. An upload thus never finds its actor
// dropped, and a worker that takes the actor from the queue afterwards finds
// nothing to do.
func (t *Tracker) Forget(ctx context.Context, actorUID string) error {
	t.mu.Lock()
	if _, err := t.waitIdleLocked(ctx, actorUID); err != nil {
		t.mu.Unlock()
		return err
	}
	t.dropLocked(actorUID)
	t.mu.Unlock()
	t.retries.Forget(actorUID)
	return nil
}
