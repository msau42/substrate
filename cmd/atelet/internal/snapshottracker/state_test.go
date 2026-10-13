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
	"errors"
	"os"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ateletpath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"google.golang.org/protobuf/proto"
)

// restartTracker returns a tracker over store loaded from the snapshot state
// files on the node, with its workers running, as a restarted atelet builds.
func restartTracker(t *testing.T, store Store) *Tracker {
	t.Helper()
	tr := newTestTrackerOnNode(t, store)
	if err := tr.Load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	tr.Start()
	return tr
}

// A restarted atelet carries on from the state file: the same state, and the
// detach due when it was.
func TestSnapshotStateSurvivesRestart(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newTestTracker(t, store)
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	before := tr.State(trackedActorUID)
	detachDue := detachDueOf(tr, trackedActorUID)

	restarted := restartTracker(t, store)
	if got := restarted.State(trackedActorUID); !proto.Equal(got, before) {
		t.Errorf("state after the restart = %v, want %v", got, before)
	}
	if got := detachDueOf(restarted, trackedActorUID); !got.Equal(detachDue) {
		t.Errorf("detach due at %v after the restart, want %v", got, detachDue)
	}
	detachNow(t, restarted)
	if got := restarted.State(trackedActorUID); got.GetLatestLocal() != nil || uuidOf(got.GetLatestDurable()) != "s1" {
		t.Errorf("state after the detach = %v, want s1 durable and nothing local", got)
	}
}

// An upload in flight at a restart is due again: its snapshot was not made
// durable, and counts from when it was taken, whatever was taken since.
func TestSnapshotStateRestartEndsUpload(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	t0 := tr.clock.Now()
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
	startUpload(t, tr, store)
	tr.CommitLocal(trackedActorUID, testRef("s2", 2, tr.clock.Now()))

	st := restartTracker(t, store).State(trackedActorUID)
	if st.GetUploading() != nil {
		t.Errorf("uploading = %v after the restart, want unset", st.GetUploading())
	}
	if got, want := timeOf(st.GetUploadDueTime()), t0.Add(testUploadDelay); !got.Equal(want) {
		t.Errorf("upload_due_time = %v, want %v: an upload delay after the interrupted upload's snapshot", got, want)
	}
}

// A restarted atelet saved nothing about which actors run: a detach that comes
// due asks the workloads again, and leaves an actor that runs alone.
func TestSnapshotStateRestartAsksWorkloads(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newTestTracker(t, store)
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

	restarted := restartTracker(t, store)
	workloadsOf(restarted).setRunning(trackedActorUID, true)
	due := detachDueOf(restarted, trackedActorUID)
	stepTo(restarted, due)
	eventually(t, "the detach is held back", func() bool {
		return detachDueOf(restarted, trackedActorUID).Equal(due.Add(testDetachDelay))
	})
	if got := uuidOf(restarted.State(trackedActorUID).GetLatestLocal()); got != "s1" {
		t.Errorf("latest_local = %q after the restart, want s1 kept", got)
	}
}

// A detach is remembered across a restart: the state still reports the
// detached snapshot as durable, and nothing is scheduled.
func TestSnapshotStateAfterDetach(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newTestTracker(t, store)
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	before, err := tr.Detach(t.Context(), trackedActorUID)
	if err != nil {
		t.Fatalf("detach: %v", err)
	}

	restarted := restartTracker(t, store)
	if got := restarted.State(trackedActorUID); !proto.Equal(got, before) {
		t.Errorf("state after the restart = %v, want %v", got, before)
	}
	if !detachDueOf(restarted, trackedActorUID).IsZero() {
		t.Error("a detach is scheduled for a detached actor after the restart")
	}
}

// Forget deletes the actor's state file, so a restart knows nothing of it.
func TestForgetDeletesSnapshotState(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newTestTracker(t, store)
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, time.Now()))
	if _, err := os.Stat(ateletpath.ActorSnapshotStateFile(trackedActorUID)); err != nil {
		t.Fatalf("state file after a checkpoint: %v", err)
	}

	if err := tr.Forget(t.Context(), trackedActorUID); err != nil {
		t.Fatalf("forget: %v", err)
	}
	if _, err := os.Stat(ateletpath.ActorSnapshotStateFile(trackedActorUID)); !os.IsNotExist(err) {
		t.Errorf("state file after forget: stat = %v, want it deleted", err)
	}
	if got := restartTracker(t, store).State(trackedActorUID); !proto.Equal(got, &ateletpb.ActorSnapshotState{}) {
		t.Errorf("state after the restart = %v, want empty", got)
	}
}

// A state file that does not parse is skipped, and the others still load.
func TestLoadSkipsUnreadableSnapshotState(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newTestTracker(t, store)
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, time.Now()))
	const brokenUID = "actor-uid-broken"
	if err := os.MkdirAll(ateletpath.ActorPath(brokenUID), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ateletpath.ActorSnapshotStateFile(brokenUID), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}

	restarted := restartTracker(t, store)
	if got := uuidOf(restarted.State(trackedActorUID).GetLatestLocal()); got != "s1" {
		t.Errorf("latest_local = %q after the restart, want s1", got)
	}
	if got := restarted.State(brokenUID); !proto.Equal(got, &ateletpb.ActorSnapshotState{}) {
		t.Errorf("state of the actor with a broken file = %v, want empty", got)
	}
}

// atelet writes no state for an actor without a directory, and does not
// remember an actor it knew nothing of when a PromoteSnapshot for it fails.
func TestSnapshotStateNeedsActorDir(t *testing.T) {
	useTempNodeDirs(t)
	store := newFakeSnapshotStore()
	store.setErr(errors.New("boom"))
	tr := newTestTrackerOnNode(t, store)
	want := testRef("s1", 1, time.Time{})
	if _, err := tr.Promote(t.Context(), trackedActorUID, want); err == nil {
		t.Fatal("promote succeeded, want the store's error")
	}
	tr.mu.Lock()
	tracked := tr.findLocked(trackedActorUID) != nil
	tr.mu.Unlock()
	if tracked {
		t.Error("the actor is tracked after a failed promote, want it unknown")
	}

	tr.CommitLocal(trackedActorUID, testRef("s1", 1, time.Now()))
	if _, err := os.Stat(ateletpath.ActorPath(trackedActorUID)); !os.IsNotExist(err) {
		t.Errorf("actor dir: stat = %v, want none created for the state file", err)
	}
}
