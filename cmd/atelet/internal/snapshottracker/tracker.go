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

// Package snapshottracker tracks each actor's snapshots on an atelet node.
package snapshottracker

import (
	"sync"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

// Tracker tracks each actor's snapshots on this node: the latest one stored
// locally, and the latest one confirmed in object storage. State reports them.
type Tracker struct {
	mu sync.Mutex
	// actors maps actorUID to the actor's entry. Read and change it through
	// findLocked, findOrAddLocked, addLocked, and dropLocked.
	actors map[string]*actorSnapshots
}

// New returns a tracker that tracks no actor yet.
func New() *Tracker {
	return &Tracker{actors: map[string]*actorSnapshots{}}
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

// stateLocked returns what atelet holds for the actor.
func (t *Tracker) stateLocked(a *actorSnapshots) *ateletpb.ActorSnapshotState {
	return &ateletpb.ActorSnapshotState{
		LatestLocal:   a.LatestLocal.report(false),
		LatestDurable: a.LatestDurable.report(true),
	}
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
