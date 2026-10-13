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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ateletpath"
	"github.com/agent-substrate/substrate/cmd/atelet/internal/atomicfile"
	"github.com/agent-substrate/substrate/internal/nodepath"
)

// saveLocked writes what atelet tracks of the actor to its snapshot state
// file. An actor that is forgotten, or whose directory is gone, is not
// written. Best-effort: if writing fails, atelet carries on from memory, and a
// restart loses what changed.
func (t *Tracker) saveLocked(a *actorSnapshots) {
	if !t.tracksLocked(a) {
		return
	}
	if _, err := os.Stat(ateletpath.ActorPath(a.actorUID)); err != nil {
		return
	}
	data, err := json.Marshal(a.actorSnapshotsRecord)
	if err == nil {
		err = atomicfile.Write(ateletpath.ActorSnapshotStateFile(a.actorUID), data, 0o600)
	}
	if err != nil {
		slog.WarnContext(t.ctx, "Failed to save the actor's snapshot state", slog.String("actorUID", a.actorUID), slog.Any("err", err))
	}
}

// removeSnapshotState deletes the actor's snapshot state file. A missing file
// is not an error.
func removeSnapshotState(actorUID string) error {
	if err := os.Remove(ateletpath.ActorSnapshotStateFile(actorUID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("while deleting the actor's snapshot state: %w", err)
	}
	return nil
}

// Load restores, from their snapshot state files, what atelet tracked of the
// actors on this node before it restarted, and queues their uploads and
// detaches. An upload in flight at the restart is due again; a detach is due
// when it was, or straight away if that has passed. A file that cannot be read
// is skipped: atelet then knows nothing of that actor, and PromoteSnapshot
// still finds its snapshot on disk or in object storage.
func (t *Tracker) Load() error {
	entries, err := os.ReadDir(nodepath.ActorsDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("while listing actor directories: %w", err)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		actorUID := entry.Name()
		data, err := os.ReadFile(ateletpath.ActorSnapshotStateFile(actorUID))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		var rec actorSnapshotsRecord
		if err == nil {
			err = json.Unmarshal(data, &rec)
		}
		if err != nil {
			slog.WarnContext(t.ctx, "Skipping an unreadable snapshot state file", slog.String("actorUID", actorUID), slog.Any("err", err))
			continue
		}
		a := &actorSnapshots{actorUID: actorUID, actorSnapshotsRecord: rec}
		t.addLocked(a)
		t.requeueLocked(a)
	}
	return nil
}
