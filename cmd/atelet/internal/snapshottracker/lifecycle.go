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
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

// CommitLocal records snap, just stored locally under UPLOAD_MODE_DEFERRED, as
// the actor's latest local snapshot, and returns the actor's state.
func (t *Tracker) CommitLocal(actorUID string, snap SnapshotRef) *ateletpb.ActorSnapshotState {
	t.mu.Lock()
	defer t.mu.Unlock()
	a := t.findOrAddLocked(actorUID)
	local := snap
	a.LatestLocal = &local
	return t.stateLocked(a)
}

// CommitDurable records snap, uploaded during Checkpoint under
// UPLOAD_MODE_IMMEDIATE, as the actor's latest snapshot. The actor keeps no
// local snapshot. It returns the actor's state.
func (t *Tracker) CommitDurable(actorUID string, snap SnapshotRef) *ateletpb.ActorSnapshotState {
	t.mu.Lock()
	defer t.mu.Unlock()
	a := t.findOrAddLocked(actorUID)
	durable := snap
	a.LatestDurable = &durable
	// TODO: change this when we add detach.
	a.LatestLocal = nil
	return t.stateLocked(a)
}

// Forget drops the actor's state.
func (t *Tracker) Forget(actorUID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dropLocked(actorUID)
}
