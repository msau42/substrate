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
	"os"
	"slices"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// eventually polls cond until it holds, and fails the test if it does not
// within 10 seconds.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// detachDueOf returns when atelet detaches the actor, or zero if it does not.
func detachDueOf(tr *Tracker, actorUID string) time.Time {
	tr.lock(actorUID)
	defer tr.unlock(actorUID)
	if a := tr.findLocked(actorUID); a != nil {
		due, _ := a.detachDue()
		return due
	}
	return time.Time{}
}

// detachNow advances the clock to when the tracked actor's detach is due, and
// waits for a worker to detach it.
func detachNow(t *testing.T, tr *Tracker) {
	t.Helper()
	due := detachDueOf(tr, trackedActorUID)
	if due.IsZero() {
		t.Fatal("no detach scheduled")
	}
	stepTo(tr, due)
	eventually(t, "the actor is detached", func() bool {
		tr.lock(trackedActorUID)
		defer tr.unlock(trackedActorUID)
		a := tr.findLocked(trackedActorUID)
		return a != nil && a.op == nil && a.LatestLocal == nil
	})
}

// startDetach runs a Detach of the tracked actor in the background, and
// returns a channel that receives its error.
func startDetach(t *testing.T, tr *Tracker) <-chan error {
	t.Helper()
	detached := make(chan error, 1)
	go func() {
		_, err := tr.Detach(t.Context(), trackedActorUID)
		detached <- err
	}()
	return detached
}

// A detach makes the latest local snapshot durable and drops it, once any
// upload in progress has ended, and is a no-op when atelet holds no local
// snapshot for the actor.
func TestTrackerDetach(t *testing.T) {
	t.Run("requested", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr := newTestTracker(t, store)
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

		st, err := tr.Detach(t.Context(), trackedActorUID)
		if err != nil {
			t.Fatalf("detach: %v", err)
		}
		if got, wantCalls := store.uploads(), []string{"s1"}; !slices.Equal(got, wantCalls) {
			t.Errorf("uploads = %v, want %v", got, wantCalls)
		}
		if st.GetLatestLocal() != nil {
			t.Errorf("state = %v, want no local snapshot", st)
		}
		durable := st.GetLatestDurable()
		if got := durable.GetSnapshot().GetFidelity(); got != ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY {
			t.Errorf("latest_durable fidelity = %v, want the MEMORY it holds", got)
		}
		if durable.GetDurableCompleteTime() == nil || durable.GetLocalCommitTime() == nil {
			t.Errorf("latest_durable = %v, want its commit and upload times", durable)
		}
		if !detachDueOf(tr, trackedActorUID).IsZero() {
			t.Error("a detach is scheduled after a detach")
		}
	})

	t.Run("no-op when no local snapshot", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr := newTestTracker(t, store)
		st, err := tr.Detach(t.Context(), trackedActorUID)
		if err != nil {
			t.Fatalf("detach of untracked actor: %v", err)
		}
		if !proto.Equal(st, &ateletpb.ActorSnapshotState{}) {
			t.Errorf("state = %v, want empty", st)
		}
		if got := store.uploads(); len(got) != 0 {
			t.Errorf("uploads = %v, want none", got)
		}
	})

	t.Run("by the timer", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr := newIdleTestTracker(t, store)
		setWorkers(tr, 0, DefaultDetachWorkers)
		tr.Start()
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		// With no upload workers, the detach runs and makes s1 durable itself.
		detachNow(t, tr)
		if got, want := store.uploads(), []string{"s1"}; !slices.Equal(got, want) {
			t.Errorf("uploads = %v, want %v", got, want)
		}
		st := tr.State(trackedActorUID)
		if st.GetLatestLocal() != nil || uuidOf(st.GetLatestDurable()) != "s1" {
			t.Errorf("state = %v, want s1 durable and nothing local", st)
		}
	})

	t.Run("already durable, as after an immediate checkpoint", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr := newTestTracker(t, store)
		local := testRef("s1", 1, tr.clock.Now())
		durable := local
		durable.UploadTime = local.CommitTime.Add(time.Second)
		tr.CommitDurable(trackedActorUID, durable)
		tr.CommitLocal(trackedActorUID, local)
		detachNow(t, tr)
		if got := store.uploads(); len(got) != 0 {
			t.Errorf("uploads = %v, want none for an already durable snapshot", got)
		}
		st := tr.State(trackedActorUID)
		if st.GetLatestLocal() != nil || uuidOf(st.GetLatestDurable()) != "s1" {
			t.Errorf("state = %v, want s1 durable and nothing local", st)
		}
		if got := timeOf(st.GetLatestDurable().GetDurableCompleteTime()); !got.Equal(durable.UploadTime) {
			t.Errorf("durable_complete_time = %v, want %v from the checkpoint", got, durable.UploadTime)
		}
	})

	t.Run("already durable, as after a promote", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr := newTestTracker(t, store)
		local := testRef("s1", 1, tr.clock.Now())
		tr.CommitLocal(trackedActorUID, local)

		promoted := testRef("s1", 1, time.Time{})
		promoted.UploadTime = local.CommitTime.Add(time.Minute)
		tr.CommitPromoted(trackedActorUID, promoted)

		detachNow(t, tr)
		if got := store.uploads(); len(got) != 0 {
			t.Errorf("uploads = %v, want none for a promoted snapshot", got)
		}
		st := tr.State(trackedActorUID)
		if st.GetLatestLocal() != nil || uuidOf(st.GetLatestDurable()) != "s1" {
			t.Errorf("state = %v, want s1 durable and nothing local", st)
		}
		if got := timeOf(st.GetLatestDurable().GetDurableCompleteTime()); !got.Equal(promoted.UploadTime) {
			t.Errorf("durable_complete_time = %v, want %v from the promote", got, promoted.UploadTime)
		}
	})

	t.Run("by the timer after the upload", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newTestTracker(t, store)
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		uploadNow(t, tr, store, nil)
		uploadTime := tr.State(trackedActorUID).GetLatestDurable().GetDurableCompleteTime()

		detachNow(t, tr)
		if got := len(store.uploads()); got != 1 {
			t.Errorf("%d store calls, want only the background upload's", got)
		}
		st := tr.State(trackedActorUID)
		if st.GetLatestLocal() != nil || !proto.Equal(st.GetLatestDurable().GetDurableCompleteTime(), uploadTime) {
			t.Errorf("state = %v, want nothing local and s1 durable since %v", st, uploadTime)
		}
	})

	t.Run("waits for the upload of its snapshot", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newTestTracker(t, store)
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		startUpload(t, tr, store)
		detached := startDetach(t, tr)

		store.release <- nil
		if err := <-detached; err != nil {
			t.Fatalf("detach: %v", err)
		}
		if got := len(store.uploads()); got != 1 {
			t.Errorf("%d store calls, want only the in-progress upload's", got)
		}
		if got := tr.State(trackedActorUID).GetLatestDurable(); got.GetDurableCompleteTime() == nil {
			t.Errorf("latest_durable = %v, want the background upload's time", got)
		}
	})

	t.Run("waits for an older snapshot's upload", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newTestTracker(t, store)
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		startUpload(t, tr, store)
		tr.CommitLocal(trackedActorUID, testRef("s2", 2, tr.clock.Now()))
		store.setBlocking(false)
		detached := startDetach(t, tr)
		select {
		case err := <-detached:
			t.Fatalf("detach returned (%v) during the upload of s1", err)
		case <-time.After(50 * time.Millisecond):
		}

		store.release <- nil
		if err := <-detached; err != nil {
			t.Fatalf("detach: %v", err)
		}
		if got, want := store.uploads(), []string{"s1", "s2"}; !slices.Equal(got, want) {
			t.Errorf("uploads = %v, want %v", got, want)
		}
		st := tr.State(trackedActorUID)
		if st.GetLatestLocal() != nil || uuidOf(st.GetLatestDurable()) != "s2" {
			t.Errorf("state = %v, want s2 durable and nothing local", st)
		}
	})

	// A refused detach keeps the local snapshot and uploads nothing.
	for _, tc := range []struct {
		name        string
		modifyLocal func(*SnapshotRef)
		setup       func(t *testing.T, tr *Tracker)
		wantCode    codes.Code
	}{
		{
			name: "refused after a start",
			setup: func(t *testing.T, tr *Tracker) {
				end, err := tr.BeginStart(t.Context(), trackedActorUID)
				if err != nil {
					t.Fatalf("beginStart: %v", err)
				}
				end()
			},
			wantCode: codes.Aborted,
		},
		{
			name: "refused while a start is in progress",
			setup: func(t *testing.T, tr *Tracker) {
				end, err := tr.BeginStart(t.Context(), trackedActorUID)
				if err != nil {
					t.Fatalf("beginStart: %v", err)
				}
				t.Cleanup(end)
			},
			wantCode: codes.Aborted,
		},
		{
			name: "refused while a checkpoint is in progress",
			setup: func(t *testing.T, tr *Tracker) {
				_, end, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, testRef("s2", 2, time.Time{}).Snapshot)
				if err != nil {
					t.Fatalf("beginCheckpoint: %v", err)
				}
				t.Cleanup(end)
			},
			wantCode: codes.Aborted,
		},
		{
			name: "refused when durable generation mismatches local",
			setup: func(t *testing.T, tr *Tracker) {
				tr.CommitDurable(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
				tr.CommitLocal(trackedActorUID, testRef("s1", 2, tr.clock.Now()))
			},
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "refused when durable URI mismatches local",
			modifyLocal: func(local *SnapshotRef) {
				local.Snapshot.Object.SnapshotUri += "-other"
			},
			setup: func(t *testing.T, tr *Tracker) {
				tr.CommitDurable(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
			},
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "refused without an object storage location",
			modifyLocal: func(local *SnapshotRef) {
				local.Snapshot.Object = nil
			},
			wantCode: codes.FailedPrecondition,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeSnapshotStore()
			tr := newTestTracker(t, store)
			local := testRef("s1", 1, tr.clock.Now())
			if tc.modifyLocal != nil {
				tc.modifyLocal(&local)
			}
			tr.CommitLocal(trackedActorUID, local)
			if tc.setup != nil {
				tc.setup(t, tr)
			}
			if _, err := tr.Detach(t.Context(), trackedActorUID); apierror.Code(err) != tc.wantCode {
				t.Errorf("detach = %v, want %v", err, tc.wantCode)
			}
			if got := store.uploads(); len(got) != 0 {
				t.Errorf("uploads = %v, want none", got)
			}
			if got := uuidOf(tr.State(trackedActorUID).GetLatestLocal()); got != "s1" {
				t.Errorf("latest_local = %q, want s1 kept", got)
			}
		})
	}
}

// A local snapshot no detach can make durable, as a pause takes today, never
// comes due for a background detach, rather than failing it again and again.
// A checkpoint that replaces it schedules a detach again.
func TestTrackerSkipsUndetachableSnapshots(t *testing.T) {
	for _, tc := range []struct {
		name   string
		commit func(tr *Tracker)
	}{
		{
			name: "no object storage location",
			commit: func(tr *Tracker) {
				local := testRef("s1", 1, tr.clock.Now())
				local.Snapshot.Object = nil
				tr.CommitLocal(trackedActorUID, local)
			},
		},
		{
			name: "durable elsewhere",
			commit: func(tr *Tracker) {
				tr.CommitDurable(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
				local := testRef("s1", 1, tr.clock.Now())
				local.Snapshot.Object.SnapshotUri += "-other"
				tr.CommitLocal(trackedActorUID, local)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeSnapshotStore()
			tr := newTestTracker(t, store)
			tc.commit(tr)
			if due := detachDueOf(tr, trackedActorUID); !due.IsZero() {
				t.Fatalf("detach due at %v, want none", due)
			}

			tr.CommitLocal(trackedActorUID, testRef("s2", 2, tr.clock.Now()))
			detachNow(t, tr)
			if got, want := store.uploads(), []string{"s2"}; !slices.Equal(got, want) {
				t.Errorf("uploads = %v, want %v", got, want)
			}
		})
	}
}

// BeginStart unschedules the actor's detach, and saves that, before the start
// begins, so neither a restart during the start nor a failed start brings the
// detach back: only the actor's next checkpoint schedules one again. While a
// Checkpoint is in progress, a worker that finds the detach due skips it
// without changing DetachAt or queuing the actor again. The end of the
// checkpoint queues the actor again, and the detach runs then.
func TestTrackerDetachTimer(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newIdleTestTracker(t, store)
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

	endStart, err := tr.BeginStart(t.Context(), trackedActorUID)
	if err != nil {
		t.Fatalf("beginStart: %v", err)
	}
	if due := detachDueOf(newTracker(t), trackedActorUID); !due.IsZero() {
		t.Fatalf("detachDue on a fresh tracker during the start = %v, want zero from the state file", due)
	}
	endStart()
	if due := detachDueOf(tr, trackedActorUID); !due.IsZero() {
		t.Fatalf("detachDue after the start = %v, want zero whether or not it succeeded", due)
	}

	tr.CommitLocal(trackedActorUID, testRef("s2", 2, tr.clock.Now()))
	due := detachDueOf(tr, trackedActorUID)
	_, endCheckpoint, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, testRef("s3", 3, time.Time{}).Snapshot)
	if err != nil {
		t.Fatalf("beginCheckpoint: %v", err)
	}
	stepTo(tr, due)
	processNext(t, tr, opDetach)
	if got := detachDueOf(tr, trackedActorUID); !got.Equal(due) {
		t.Errorf("detachDue after a worker found the actor held = %v, want %v kept", got, due)
	}
	if n := tr.queues[opDetach].Len(); n != 0 {
		t.Errorf("actors queued for a detach while the actor is held = %d, want none until the hold ends", n)
	}
	if got := uuidOf(tr.State(trackedActorUID).GetLatestLocal()); got != "s2" {
		t.Fatalf("latest_local = %q while the actor is held, want s2 kept", got)
	}

	endCheckpoint()
	processNext(t, tr, opDetach)
	if got := tr.State(trackedActorUID).GetLatestLocal(); got != nil {
		t.Errorf("latest_local = %v once the checkpoint ended, want unset", got)
	}
	if got, want := store.uploads(), []string{"s2"}; !slices.Equal(got, want) {
		t.Errorf("uploads = %v, want %v", got, want)
	}
}

// A start that cannot save that it unscheduled the actor's detach fails before
// anything starts, and leaves the detach scheduled.
func TestTrackerStartFailsIfItCannotSave(t *testing.T) {
	tr := newIdleTestTracker(t, newFakeSnapshotStore())
	stateFile := ateletpath.ActorSnapshotStateFile(trackedActorUID)
	// A directory where the state file goes makes every save fail, so the
	// entry stays in memory.
	if err := os.MkdirAll(stateFile, 0o700); err != nil {
		t.Fatal(err)
	}
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	due := detachDueOf(tr, trackedActorUID)
	if due.IsZero() {
		t.Fatal("no detach scheduled after the checkpoint")
	}

	if _, err := tr.BeginStart(t.Context(), trackedActorUID); apierror.Code(err) != codes.Unavailable {
		t.Fatalf("beginStart = %v, want Unavailable", err)
	}
	if got := detachDueOf(tr, trackedActorUID); !got.Equal(due) {
		t.Errorf("detachDue after the failed start = %v, want %v kept", got, due)
	}
	if holds := tr.actors[trackedActorUID].holds; holds != 0 {
		t.Errorf("holds after the failed start = %d, want none", holds)
	}

	// Once saves work again, the start goes ahead.
	if err := os.Remove(stateFile); err != nil {
		t.Fatal(err)
	}
	end, err := tr.BeginStart(t.Context(), trackedActorUID)
	if err != nil {
		t.Fatalf("beginStart once saves work: %v", err)
	}
	end()
	if due := detachDueOf(newTracker(t), trackedActorUID); !due.IsZero() {
		t.Errorf("detachDue from the state file = %v, want zero", due)
	}
}

// A restore or a checkpoint that arrives during a detach waits for it to end.
func TestTrackerWaitsForDetach(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	results := map[string]<-chan error{"detach": startDetach(t, tr)}
	waitStarted(t, store)

	for name, begin := range map[string]func(context.Context, string) (func(), error){
		"beginStart": tr.BeginStart,
		"beginCheckpoint": func(ctx context.Context, uid string) (func(), error) {
			_, end, err := tr.BeginCheckpoint(ctx, uid, testRef("s2", 2, time.Time{}).Snapshot)
			return end, err
		},
	} {
		result := make(chan error, 1)
		results[name] = result
		go func() {
			end, err := begin(t.Context(), trackedActorUID)
			if err == nil {
				end()
			}
			result <- err
		}()
	}
	for name, result := range results {
		select {
		case err := <-result:
			t.Fatalf("%s returned (%v) during a detach", name, err)
		case <-time.After(50 * time.Millisecond):
		}
	}

	store.release <- nil
	for name, result := range results {
		select {
		case err := <-result:
			if err != nil {
				t.Errorf("%s: %v", name, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s did not return after the detach", name)
		}
	}
	if got := tr.State(trackedActorUID).GetLatestLocal(); got != nil {
		t.Errorf("latest_local = %v after the detach, want unset", got)
	}
}

// BeginCheckpoint and BeginPromote fail at once during a detach when the
// requested snapshot is already stale, and fail once the detach ends when a
// newer snapshot was committed while they waited.
func TestTrackerDetachWaitChecksGeneration(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	tr.CommitLocal(trackedActorUID, testRef("s1", 2, tr.clock.Now()))
	detached := startDetach(t, tr)
	waitStarted(t, store)

	stale := testRef("s0", 1, time.Time{})
	returnsPromptly(t, "BeginCheckpoint of stale generation during detach", func() {
		if _, _, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, stale.Snapshot); apierror.Code(err) != codes.FailedPrecondition {
			t.Errorf("BeginCheckpoint = %v, want FailedPrecondition", err)
		}
	})
	returnsPromptly(t, "BeginPromote of stale generation during detach", func() {
		if _, _, err := tr.BeginPromote(t.Context(), trackedActorUID, stale); apierror.Code(err) != codes.FailedPrecondition {
			t.Errorf("BeginPromote = %v, want FailedPrecondition", err)
		}
	})

	superseded := testRef("s2", 3, time.Time{})
	results := make(map[string]chan error)
	for name, begin := range map[string]func() (func(), error){
		"beginCheckpoint": func() (func(), error) {
			_, end, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, superseded.Snapshot)
			return end, err
		},
		"beginPromote": func() (func(), error) {
			_, end, err := tr.BeginPromote(t.Context(), trackedActorUID, superseded)
			return end, err
		},
	} {
		result := make(chan error, 1)
		results[name] = result
		go func() {
			end, err := begin()
			if err == nil {
				end()
			}
			result <- err
		}()
	}
	for name, result := range results {
		select {
		case err := <-result:
			t.Fatalf("%s returned (%v) during a detach", name, err)
		case <-time.After(50 * time.Millisecond):
		}
	}

	tr.CommitDurable(trackedActorUID, testRef("s3", 4, tr.clock.Now()))
	store.release <- nil
	if err := <-detached; err != nil {
		t.Fatalf("detach: %v", err)
	}
	for name, result := range results {
		select {
		case err := <-result:
			if apierror.Code(err) != codes.FailedPrecondition {
				t.Errorf("%s = %v, want FailedPrecondition", name, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s did not return after the detach", name)
		}
	}
}

// A failed detach keeps the local snapshot, and is retried no sooner than the
// retry delay after it, nor than it was due. The upload is held back the retry
// delay too.
func TestTrackerFailedDetach(t *testing.T) {
	t.Run("requested", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setErr(errors.New("boom"))
		tr := newIdleTestTracker(t, store)
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now().Add(-testUploadDelay)))
		due := detachDueOf(tr, trackedActorUID)
		if _, err := tr.Detach(t.Context(), trackedActorUID); err == nil {
			t.Fatal("detach succeeded, want the store's error")
		}
		retryAt := tr.clock.Now().Add(testRetryDelay)
		st := tr.State(trackedActorUID)
		if uuidOf(st.GetLatestLocal()) != "s1" || !timeOf(st.GetUploadDueTime()).Equal(retryAt) {
			t.Errorf("state = %v, want s1 kept and its upload due at %v", st, retryAt)
		}
		// testRetryDelay is shorter than testDetachDelay.
		if got := detachDueOf(tr, trackedActorUID); !got.Equal(due) {
			t.Errorf("detach due at %v after the failed one, want %v as scheduled", got, due)
		}
	})

	t.Run("background", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setErr(errors.New("boom"))
		tr := newIdleTestTracker(t, store)
		setWorkers(tr, 0, DefaultDetachWorkers)
		tr.Start()
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		due := detachDueOf(tr, trackedActorUID)
		stepTo(tr, due)
		retryAt := due.Add(testRetryDelay)
		eventually(t, "the failed detach is retried later", func() bool {
			return detachDueOf(tr, trackedActorUID).Equal(retryAt)
		})
		if got := uuidOf(tr.State(trackedActorUID).GetLatestLocal()); got != "s1" {
			t.Errorf("latest_local = %q after the failed detach, want s1 kept", got)
		}

		store.setErr(nil)
		detachNow(t, tr)
		if got := tr.State(trackedActorUID).GetLatestLocal(); got != nil {
			t.Errorf("latest_local = %v after the retried detach, want unset", got)
		}
	})
}

// A detach whose prune fails keeps LatestLocal, now durable, and is retried:
// the retry finds the snapshot durable and only prunes.
func TestTrackerDetachRetriesFailedPrune(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newTestTracker(t, store)
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	// A file where the local checkpoint directory belongs fails the prune.
	if err := os.WriteFile(ateletpath.LocalCheckpointsDir(trackedActorUID), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Detach(t.Context(), trackedActorUID); err == nil {
		t.Fatal("detach succeeded, want the prune's error")
	}
	if st := tr.State(trackedActorUID); uuidOf(st.GetLatestLocal()) != "s1" || uuidOf(st.GetLatestDurable()) != "s1" {
		t.Errorf("state = %v, want s1 still local and now durable", st)
	}

	if err := os.Remove(ateletpath.LocalCheckpointsDir(trackedActorUID)); err != nil {
		t.Fatal(err)
	}
	detachNow(t, tr)
	if got, want := store.uploads(), []string{"s1"}; !slices.Equal(got, want) {
		t.Errorf("uploads = %v, want %v, as the retry only prunes", got, want)
	}
	if got := tr.State(trackedActorUID).GetLatestLocal(); got != nil {
		t.Errorf("latest_local = %v after the retried detach, want unset", got)
	}
}

// Detach fails on a tracker without a store, rather than uploading through it.
func TestTrackerDetachWithoutStore(t *testing.T) {
	useActorDir(t)
	tr := newTracker(t)
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	if _, err := tr.Detach(t.Context(), trackedActorUID); apierror.Code(err) != codes.Unimplemented {
		t.Errorf("detach = %v, want Unimplemented", err)
	}
	if got := uuidOf(tr.State(trackedActorUID).GetLatestLocal()); got != "s1" {
		t.Errorf("latest_local = %q, want s1 kept", got)
	}
}

// A Checkpoint holds off detaches even when atelet tracked nothing of the
// actor as it began, as on the actor's first checkpoint on this node.
func TestTrackerHoldsFirstCheckpoint(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newTestTracker(t, store)
	s1 := testRef("s1", 1, tr.clock.Now())
	_, end, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, s1.Snapshot)
	if err != nil {
		t.Fatalf("beginCheckpoint: %v", err)
	}
	tr.CommitLocal(trackedActorUID, s1)
	if _, err := tr.Detach(t.Context(), trackedActorUID); apierror.Code(err) != codes.Aborted {
		t.Errorf("detach during the checkpoint = %v, want Aborted", err)
	}
	end()
	if _, err := tr.Detach(t.Context(), trackedActorUID); err != nil {
		t.Errorf("detach after the checkpoint: %v", err)
	}
}

// A start when no upload is due, or a checkpoint replacing the local snapshot,
// lifts the backoff of the actor's failed detaches, which have nothing left to
// retry, so the entry leaves memory.
func TestTrackerLiftsBackoffWithDetach(t *testing.T) {
	for name, drop := range map[string]func(t *testing.T, tr *Tracker){
		"start": func(t *testing.T, tr *Tracker) {
			end, err := tr.BeginStart(t.Context(), trackedActorUID)
			if err != nil {
				t.Fatalf("beginStart: %v", err)
			}
			end()
		},
		"checkpoint": func(t *testing.T, tr *Tracker) {
			tr.CommitLocal(trackedActorUID, testRef("s2", 2, tr.clock.Now()))
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := newFakeSnapshotStore()
			tr := newTestTracker(t, store)
			s1 := testRef("s1", 1, tr.clock.Now())
			tr.CommitDurable(trackedActorUID, s1)
			tr.CommitLocal(trackedActorUID, s1)
			// A file where the local checkpoint directory belongs fails the prune.
			if err := os.WriteFile(ateletpath.LocalCheckpointsDir(trackedActorUID), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := tr.Detach(t.Context(), trackedActorUID); err == nil {
				t.Fatal("detach succeeded, want the prune's error")
			}
			if !inMemory(tr) {
				t.Fatal("entry not in memory after a failed detach, want it kept for its backoff")
			}
			drop(t, tr)
			if inMemory(tr) {
				t.Error("entry in memory, want it dropped with no backoff left")
			}
		})
	}
}

// Forget during a Run, Restore, or Checkpoint drops the actor for good: the
// start's end leaves it forgotten.
func TestTrackerForgetDuringStart(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newTestTracker(t, store)
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	end, err := tr.BeginStart(t.Context(), trackedActorUID)
	if err != nil {
		t.Fatalf("beginStart: %v", err)
	}
	if err := tr.Forget(trackedActorUID); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	end()
	if st := tr.State(trackedActorUID); !proto.Equal(st, &ateletpb.ActorSnapshotState{}) {
		t.Errorf("state = %v, want empty", st)
	}
	if _, err := os.Stat(ateletpath.ActorSnapshotStateFile(trackedActorUID)); !os.IsNotExist(err) {
		t.Errorf("state file after the start ended: stat = %v, want none", err)
	}
	if inMemory(tr) {
		t.Error("entry in memory after Forget, want none")
	}
}

// Forget during a Checkpoint or PromoteSnapshot refuses that call's commit,
// so it cannot track the forgotten actor again, but not the commits of a
// later activation of the actor. The refusal ends with the call.
func TestTrackerForgetRefusesCommitsInProgress(t *testing.T) {
	beginCheckpoint := func(t *testing.T, tr *Tracker, snap SnapshotRef) func() {
		_, end, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, snap.Snapshot)
		if err != nil {
			t.Fatalf("BeginCheckpoint: %v", err)
		}
		return end
	}
	beginPromote := func(t *testing.T, tr *Tracker, snap SnapshotRef) func() {
		_, end, err := tr.BeginPromote(t.Context(), trackedActorUID, snap)
		if err != nil {
			t.Fatalf("BeginPromote: %v", err)
		}
		return end
	}
	for _, tc := range []struct {
		name   string
		begin  func(*testing.T, *Tracker, SnapshotRef) func()
		commit func(*Tracker, string, SnapshotRef) (*ateletpb.ActorSnapshotState, error)
	}{
		{name: "CommitLocal", begin: beginCheckpoint, commit: (*Tracker).CommitLocal},
		{name: "CommitDurable", begin: beginCheckpoint, commit: (*Tracker).CommitDurable},
		{name: "CommitPromoted", begin: beginPromote, commit: (*Tracker).CommitPromoted},
		{
			name: "CommittedCheckpointState",
			begin: func(t *testing.T, tr *Tracker, snap SnapshotRef) func() {
				if _, err := tr.CommitLocal(trackedActorUID, snap); err != nil {
					t.Fatalf("CommitLocal: %v", err)
				}
				return beginCheckpoint(t, tr, snap)
			},
			commit: func(tr *Tracker, actorUID string, snap SnapshotRef) (*ateletpb.ActorSnapshotState, error) {
				return tr.CommittedCheckpointState(actorUID, snap.Snapshot)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useActorDir(t)
			tr := newTracker(t)
			s1 := testRef("s1", 1, tr.clock.Now())
			end := tc.begin(t, tr, s1)
			if err := tr.Forget(trackedActorUID); err != nil {
				t.Fatalf("Forget: %v", err)
			}
			if _, err := tc.commit(tr, trackedActorUID, s1); apierror.Code(err) != codes.Aborted {
				t.Errorf("%s of s1 after Forget = %v, want Aborted", tc.name, err)
			}
			if st := tr.State(trackedActorUID); !proto.Equal(st, &ateletpb.ActorSnapshotState{}) {
				t.Errorf("state after the refused commit = %v, want empty", st)
			}
			if _, err := os.Stat(ateletpath.ActorSnapshotStateFile(trackedActorUID)); !os.IsNotExist(err) {
				t.Errorf("state file after the refused commit: stat = %v, want none", err)
			}

			if _, err := tc.commit(tr, trackedActorUID, testRef("s2", 2, tr.clock.Now())); err != nil {
				t.Errorf("%s of a later activation's s2 = %v, want it committed", tc.name, err)
			}
			end()
			if len(tr.forgotten) != 0 {
				t.Errorf("forgotten snapshots after the call ended = %v, want none", tr.forgotten)
			}
		})
	}
}

// While Forget waits for a canceled upload to end, commits of Checkpoints and
// PromoteSnapshots already in flight fail, new ones cannot begin, and
// CommitDurable or CommitPromoted waiting for that upload fails too.
func TestTrackerForgetWhileWaitingOnUpload(t *testing.T) {
	t.Run("commit and end while Forget waits", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		store.setIgnoreCancel(true)
		tr := newTestTracker(t, store)
		t0 := tr.clock.Now()
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
		uploadDone := startUpload(t, tr, store)

		s2 := testRef("s2", 2, t0.Add(time.Minute))
		_, endCheckpoint, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, s2.Snapshot)
		if err != nil {
			t.Fatalf("BeginCheckpoint: %v", err)
		}

		forgetDone := make(chan error, 1)
		go func() { forgetDone <- tr.Forget(trackedActorUID) }()
		waitAbandoned(t, tr)

		if _, err := tr.CommitLocal(trackedActorUID, s2); apierror.Code(err) != codes.Aborted {
			t.Errorf("CommitLocal while Forget waits = %v, want Aborted", err)
		}
		// End the Checkpoint before Forget drops the entry from memory.
		endCheckpoint()
		if len(tr.forgotten) != 0 {
			t.Errorf("forgotten snapshots after endCheckpoint = %v, want none", tr.forgotten)
		}

		s3 := testRef("s3", 3, t0.Add(2*time.Minute))
		if _, _, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, s3.Snapshot); apierror.Code(err) != codes.Aborted {
			t.Errorf("BeginCheckpoint while Forget waits = %v, want Aborted", err)
		}
		if _, _, err := tr.BeginPromote(t.Context(), trackedActorUID, s3); apierror.Code(err) != codes.Aborted {
			t.Errorf("BeginPromote while Forget waits = %v, want Aborted", err)
		}

		store.release <- context.Canceled
		waitClosed(t, uploadDone, "the upload of s1")
		if err := <-forgetDone; err != nil {
			t.Fatalf("Forget: %v", err)
		}
		if st := tr.State(trackedActorUID); !proto.Equal(st, &ateletpb.ActorSnapshotState{}) {
			t.Errorf("state after Forget = %v, want empty", st)
		}
		if _, err := os.Stat(ateletpath.ActorSnapshotStateFile(trackedActorUID)); !os.IsNotExist(err) {
			t.Errorf("state file after Forget: stat = %v, want none", err)
		}
	})

	t.Run("Forget while CommitDurable waits for older upload", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		store.setIgnoreCancel(true)
		tr := newTestTracker(t, store)
		t0 := tr.clock.Now()
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
		uploadDone := startUpload(t, tr, store)

		s2 := testRef("s2", 2, t0.Add(time.Minute))
		_, endCheckpoint, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, s2.Snapshot)
		if err != nil {
			t.Fatalf("BeginCheckpoint: %v", err)
		}

		commitErr := make(chan error, 1)
		go func() {
			_, err := tr.CommitDurable(trackedActorUID, s2)
			commitErr <- err
		}()
		if got := waitCanceled(t, store); got != "s1" {
			t.Fatalf("canceled upload = %q, want s1", got)
		}

		forgetDone := make(chan error, 1)
		go func() { forgetDone <- tr.Forget(trackedActorUID) }()
		waitAbandoned(t, tr)

		store.release <- context.Canceled
		waitClosed(t, uploadDone, "the upload of s1")
		if err := <-commitErr; apierror.Code(err) != codes.Aborted {
			t.Errorf("CommitDurable after Forget = %v, want Aborted", err)
		}
		if err := <-forgetDone; err != nil {
			t.Fatalf("Forget: %v", err)
		}
		endCheckpoint()
		if len(tr.forgotten) != 0 {
			t.Errorf("forgotten snapshots after endCheckpoint = %v, want none", tr.forgotten)
		}
		if st := tr.State(trackedActorUID); !proto.Equal(st, &ateletpb.ActorSnapshotState{}) {
			t.Errorf("state after Forget = %v, want empty", st)
		}
		if _, err := os.Stat(ateletpath.ActorSnapshotStateFile(trackedActorUID)); !os.IsNotExist(err) {
			t.Errorf("state file after Forget: stat = %v, want none", err)
		}
	})
}

func waitWaiting(t *testing.T, tr *Tracker) {
	t.Helper()
	eventually(t, "the call is waiting on the actor", func() bool {
		tr.lock(trackedActorUID)
		defer tr.unlock(trackedActorUID)
		a := tr.findLocked(trackedActorUID)
		return a != nil && a.waiters == 1
	})
}

// Forget aborts BeginStart, BeginCheckpoint, and BeginPromote when they are
// waiting for an operation or checkpoint in progress.
func TestTrackerForgetAbortsWaiters(t *testing.T) {
	t.Run("BeginStart waiting on detach", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newTestTracker(t, store)
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		detached := startDetach(t, tr)
		waitStarted(t, store)

		started := make(chan error, 1)
		go func() {
			_, err := tr.BeginStart(t.Context(), trackedActorUID)
			started <- err
		}()
		waitWaiting(t, tr)

		if err := tr.Forget(trackedActorUID); err != nil {
			t.Fatalf("Forget: %v", err)
		}
		if err := <-detached; !errors.Is(err, context.Canceled) {
			t.Fatalf("detach = %v, want %v", err, context.Canceled)
		}
		if err := <-started; apierror.Code(err) != codes.Aborted {
			t.Errorf("BeginStart after Forget = %v, want Aborted", err)
		}
	})

	t.Run("BeginCheckpoint waiting on in-flight checkpoint", func(t *testing.T) {
		useActorDir(t)
		tr := newTracker(t)
		s1 := testRef("s1", 1, tr.clock.Now())
		_, endFirst, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, s1.Snapshot)
		if err != nil {
			t.Fatalf("first BeginCheckpoint: %v", err)
		}

		second := make(chan error, 1)
		go func() {
			_, _, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, s1.Snapshot)
			second <- err
		}()
		waitWaiting(t, tr)

		if err := tr.Forget(trackedActorUID); err != nil {
			t.Fatalf("Forget: %v", err)
		}
		endFirst()
		if err := <-second; apierror.Code(err) != codes.Aborted {
			t.Errorf("waiting BeginCheckpoint after Forget = %v, want Aborted", err)
		}
	})

	t.Run("BeginPromote waiting on detach", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newTestTracker(t, store)
		s1 := testRef("s1", 1, tr.clock.Now())
		tr.CommitLocal(trackedActorUID, s1)
		detached := startDetach(t, tr)
		waitStarted(t, store)

		promoted := make(chan error, 1)
		go func() {
			_, _, err := tr.BeginPromote(t.Context(), trackedActorUID, s1)
			promoted <- err
		}()
		waitWaiting(t, tr)

		if err := tr.Forget(trackedActorUID); err != nil {
			t.Fatalf("Forget: %v", err)
		}
		if err := <-detached; !errors.Is(err, context.Canceled) {
			t.Fatalf("detach = %v, want %v", err, context.Canceled)
		}
		if err := <-promoted; apierror.Code(err) != codes.Aborted {
			t.Errorf("BeginPromote after Forget = %v, want Aborted", err)
		}
	})
}

// BeginStart waits for a Checkpoint that was in progress when Forget dropped
// the actor, so the Checkpoint's cleanup finishes before a new activation
// starts, and fails with Aborted if Forget runs again during that wait.
func TestTrackerBeginStartWaitsForForgottenCheckpoint(t *testing.T) {
	t.Run("starts once forgotten checkpoint ends", func(t *testing.T) {
		useActorDir(t)
		tr := newTracker(t)
		s1 := testRef("s1", 1, tr.clock.Now())
		_, endCheckpoint, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, s1.Snapshot)
		if err != nil {
			t.Fatalf("BeginCheckpoint: %v", err)
		}
		if err := tr.Forget(trackedActorUID); err != nil {
			t.Fatalf("Forget: %v", err)
		}

		started := make(chan func(), 1)
		startErr := make(chan error, 1)
		go func() {
			endStart, err := tr.BeginStart(t.Context(), trackedActorUID)
			if err != nil {
				startErr <- err
				return
			}
			started <- endStart
		}()

		waitWaiting(t, tr)
		select {
		case <-started:
			t.Fatal("BeginStart finished before the forgotten Checkpoint ended")
		case err := <-startErr:
			t.Fatalf("BeginStart failed while waiting: %v", err)
		default:
		}

		endCheckpoint()
		select {
		case endStart := <-started:
			endStart()
		case err := <-startErr:
			t.Fatalf("BeginStart: %v", err)
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for BeginStart")
		}
		if inMemory(tr) {
			t.Error("entry in memory after BeginStart ended, want none")
		}
	})

	t.Run("aborts if forgotten again while waiting", func(t *testing.T) {
		useActorDir(t)
		tr := newTracker(t)
		s1 := testRef("s1", 1, tr.clock.Now())
		_, endCheckpoint, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, s1.Snapshot)
		if err != nil {
			t.Fatalf("BeginCheckpoint: %v", err)
		}
		if err := tr.Forget(trackedActorUID); err != nil {
			t.Fatalf("first Forget: %v", err)
		}

		startErr := make(chan error, 1)
		go func() {
			_, err := tr.BeginStart(t.Context(), trackedActorUID)
			startErr <- err
		}()
		waitWaiting(t, tr)

		if err := tr.Forget(trackedActorUID); err != nil {
			t.Fatalf("second Forget: %v", err)
		}
		endCheckpoint()
		if err := <-startErr; apierror.Code(err) != codes.Aborted {
			t.Errorf("BeginStart after second Forget = %v, want Aborted", err)
		}
	})
}

// Forget cancels a detach in progress and waits for it to end, leaving the
// forgotten actor's local snapshots for Terminate to prune.
func TestTrackerForgetAbandonsDetach(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	store.setIgnoreCancel(true)
	tr := newTestTracker(t, store)
	if err := os.MkdirAll(ateletpath.LocalSnapshotDir(trackedActorUID, "s1"), 0o700); err != nil {
		t.Fatal(err)
	}
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	detached := startDetach(t, tr)
	waitStarted(t, store)

	forgetDone := make(chan error, 1)
	go func() {
		forgetDone <- tr.Forget(trackedActorUID)
	}()
	if got := waitCanceled(t, store); got != "s1" {
		t.Fatalf("canceled detach upload = %q, want s1", got)
	}
	select {
	case err := <-forgetDone:
		t.Fatalf("Forget returned (%v) before the canceled detach ended", err)
	case <-time.After(20 * time.Millisecond):
	}
	store.release <- context.Canceled
	if err := <-detached; !errors.Is(err, context.Canceled) {
		t.Errorf("detach = %v, want %v", err, context.Canceled)
	}
	select {
	case err := <-forgetDone:
		if err != nil {
			t.Errorf("Forget: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for Forget to return")
	}
	if st := tr.State(trackedActorUID); !proto.Equal(st, &ateletpb.ActorSnapshotState{}) {
		t.Errorf("state after forget = %v, want empty", st)
	}
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(trackedActorUID, "s1")); err != nil {
		t.Errorf("s1 after the cancelled detach: %v, want it left for Terminate to prune", err)
	}
}

// Detach runs in the calling goroutine, so it does not wait behind other
// actors' background work, and a worker that takes the actor afterwards finds
// nothing left to do.
func TestTrackerForegroundDetachBypassesQueue(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newIdleTestTracker(t, store)
	setWorkers(tr, 1, 0)
	tr.Start()
	for _, id := range []string{"actor-1", "actor-2", "actor-3"} {
		if err := os.MkdirAll(ateletpath.ActorPath(id), 0o700); err != nil {
			t.Fatal(err)
		}
	}

	taken := tr.clock.Now().Add(-testUploadDelay)
	tr.CommitLocal("actor-1", testRef("a1-s1", 1, taken))
	if got := waitStarted(t, store); got != "a1-s1" {
		t.Fatalf("started = %q, want a1-s1", got)
	}
	// actor-2's upload is due too, but the only worker is busy.
	tr.CommitLocal("actor-2", testRef("a2-s1", 1, taken))

	store.setBlocking(false)
	st, err := tr.Detach(t.Context(), "actor-2")
	if err != nil {
		t.Fatalf("detach of actor-2: %v", err)
	}
	if got := waitStarted(t, store); got != "a2-s1" {
		t.Fatalf("started = %q, want a2-s1", got)
	}
	if st.GetLatestLocal() != nil || uuidOf(st.GetLatestDurable()) != "a2-s1" {
		t.Errorf("actor-2 state = %v, want detached with a2-s1 durable", st)
	}

	done1 := opDone(t, tr, "actor-1")
	store.release <- nil
	waitClosed(t, done1, "actor-1's upload")
	// actor-3 is queued behind actor-2, so its upload starts only once the
	// worker has taken actor-2.
	tr.CommitLocal("actor-3", testRef("a3-s1", 1, taken))
	if got := waitStarted(t, store); got != "a3-s1" {
		t.Fatalf("started = %q, want a3-s1", got)
	}
	wantCalls := []string{"a1-s1", "a2-s1", "a3-s1"}
	if got := store.uploads(); !slices.Equal(got, wantCalls) {
		t.Errorf("uploads = %v, want %v", got, wantCalls)
	}
}

// The queue holds an actor until the earliest time it was queued for. A worker
// that takes the actor before its detach is due, as after a later checkpoint
// moved the detach out, leaves the actor's local snapshot and queues it again
// for when its detach is due. Both snapshots are durable, so no upload is due
// either.
func TestTrackerRequeuesUntilDetachIsDue(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newIdleTestTracker(t, store)
	t0 := tr.clock.Now()
	commitImmediate := func(ref SnapshotRef) {
		tr.CommitDurable(trackedActorUID, ref)
		tr.CommitLocal(trackedActorUID, ref)
	}
	commitImmediate(testRef("s1", 1, t0))
	stepTo(tr, t0.Add(time.Hour))
	commitImmediate(testRef("s2", 2, tr.clock.Now()))

	stepTo(tr, t0.Add(testDetachDelay))
	processNext(t, tr, opDetach)
	if got := uuidOf(tr.State(trackedActorUID).GetLatestLocal()); got != "s2" {
		t.Errorf("latest_local = %q before the detach is due, want s2", got)
	}
	if got := store.uploads(); len(got) != 0 {
		t.Errorf("uploads = %v, want none: both snapshots are durable", got)
	}

	stepTo(tr, t0.Add(time.Hour+testDetachDelay))
	processNext(t, tr, opDetach)
	if got := tr.State(trackedActorUID).GetLatestLocal(); got != nil {
		t.Errorf("latest_local = %v once the detach was due, want unset", got)
	}
}

// cancelingStore is a Store whose upload succeeds just after cancel ends the
// detach's context, as Forget or the Detach caller can.
type cancelingStore struct{ cancel context.CancelFunc }

func (s *cancelingStore) UploadLocalSnapshot(context.Context, string, SnapshotRef) (bool, error) {
	s.cancel()
	return true, nil
}

// A detach whose context ends as its upload succeeds records the upload, but
// deletes no local snapshot, and fails with the context's error.
func TestTrackerDetachDoesNotPruneOnceCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	tr := newIdleTestTracker(t, &cancelingStore{cancel: cancel})
	if err := os.MkdirAll(ateletpath.LocalSnapshotDir(trackedActorUID, "s1"), 0o700); err != nil {
		t.Fatal(err)
	}
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

	if _, err := tr.Detach(ctx, trackedActorUID); !errors.Is(err, context.Canceled) {
		t.Fatalf("detach = %v, want %v", err, context.Canceled)
	}
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(trackedActorUID, "s1")); err != nil {
		t.Errorf("s1 after the cancelled detach: %v, want it kept", err)
	}
	if st := tr.State(trackedActorUID); uuidOf(st.GetLatestLocal()) != "s1" || uuidOf(st.GetLatestDurable()) != "s1" {
		t.Errorf("state = %v, want s1 still local, and durable", st)
	}
}

// TestTrackerDetachActsForTheActor checks that a detach's upload names the
// actor the snapshot was committed for, across an atelet restart that reloads
// the snapshot state file, and that a promote keeps it on the local copy.
func TestTrackerDetachActsForTheActor(t *testing.T) {
	t.Run("after a restart", func(t *testing.T) {
		store := newFakeSnapshotStore()
		first := newIdleTestTracker(t, store)
		ref := testRef("s1", 1, first.clock.Now())
		ref.Actor = testActor
		first.CommitLocal(trackedActorUID, ref)

		// A new tracker over the same node directories reloads the state file.
		tr := newStoreTracker(t, store)
		tr.Start()
		if _, err := tr.Detach(t.Context(), trackedActorUID); err != nil {
			t.Fatalf("detach: %v", err)
		}
		if got := store.actorOf("s1"); got != testActor {
			t.Errorf("upload acted for %+v, want %+v", got, testActor)
		}
	})

	t.Run("kept by a promote", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr := newTestTracker(t, store)
		ref := testRef("s1", 1, tr.clock.Now())
		ref.Actor = testActor
		tr.CommitLocal(trackedActorUID, ref)
		promoted := testRef("s1", 1, time.Time{})
		promoted.Actor = testActor
		tr.CommitPromoted(trackedActorUID, promoted)

		tr.lock(trackedActorUID)
		got := tr.findLocked(trackedActorUID).LatestLocal.Actor
		tr.unlock(trackedActorUID)
		if got != testActor {
			t.Errorf("local copy's actor after promote = %+v, want %+v", got, testActor)
		}
	})
}
