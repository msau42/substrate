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
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ateletpath"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/client-go/util/workqueue"
	testingclock "k8s.io/utils/clock/testing"
)

const trackedActorUID = "actor-uid-1"

const noFidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED

// The delays of the trackers tests build. Nothing is due until a test advances
// the tracker's clock.
const (
	testUploadDelay   = time.Hour
	testRetryDelay    = 30 * time.Minute
	testMaxRetryDelay = 2 * time.Hour
)

// uploadCall is one call to fakeSnapshotStore.UploadLocalSnapshot.
type uploadCall struct {
	uuid        string
	minFidelity ateletpb.SnapshotFidelity
}

// fakeSnapshotStore is a Store that records its calls. A call for a
// URI it already holds uploads nothing.
type fakeSnapshotStore struct {
	// started receives each call's snapshot uuid as the call starts.
	started chan string
	// release ends a blocked call with the error it receives.
	release chan error

	mu       sync.Mutex
	calls    []uploadCall
	uploaded map[string]bool
	blocking bool
	err      error
}

func newFakeSnapshotStore() *fakeSnapshotStore {
	return &fakeSnapshotStore{started: make(chan string, 100), release: make(chan error)}
}

// setBlocking makes later calls wait for release, or their context.
func (f *fakeSnapshotStore) setBlocking(blocking bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blocking = blocking
}

func (f *fakeSnapshotStore) uploads() []uploadCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

func (f *fakeSnapshotStore) UploadLocalSnapshot(ctx context.Context, _ string, ref SnapshotRef, minFidelity ateletpb.SnapshotFidelity) (ateletpb.SnapshotFidelity, bool, error) {
	f.mu.Lock()
	f.calls = append(f.calls, uploadCall{ref.UUID, minFidelity})
	blocking, err := f.blocking, f.err
	f.mu.Unlock()
	f.started <- ref.UUID
	if blocking {
		select {
		case err = <-f.release:
		case <-ctx.Done():
			return noFidelity, false, ctx.Err()
		}
	}
	if err != nil {
		return noFidelity, false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.uploaded[ref.URI] {
		return ref.Fidelity, false, nil
	}
	if f.uploaded == nil {
		f.uploaded = map[string]bool{}
	}
	f.uploaded[ref.URI] = true
	return ref.Fidelity, true, nil
}

// useTempNodeDirs roots atelet's on-node state in temp directories so a test
// can drive the real filesystem layout. Not parallel-safe: the paths are
// process-global.
func useTempNodeDirs(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	origActors, origStatic := nodepath.ActorsDir, nodepath.StaticFilesDir
	nodepath.ActorsDir = filepath.Join(root, "actors")
	nodepath.StaticFilesDir = filepath.Join(root, "static-files")
	t.Cleanup(func() {
		nodepath.ActorsDir, nodepath.StaticFilesDir = origActors, origStatic
	})
}

// newTestTracker returns a tracker over store, with its workers running, that
// measures time on a fake clock: background work starts only once a test
// advances the clock to when it is due (see stepTo).
func newTestTracker(t *testing.T, store Store) *Tracker {
	t.Helper()
	tr := newIdleTestTracker(t, store)
	tr.Start()
	return tr
}

// newIdleTestTracker returns a tracker like newTestTracker's whose workers
// have not started. Its workers stop once the test ends, before the node
// directories are restored.
func newIdleTestTracker(t *testing.T, store Store) *Tracker {
	t.Helper()
	useTempNodeDirs(t)
	tr := New(t.Context(), store, testingclock.NewFakeClock(time.Now()))
	tr.uploadDelay = testUploadDelay
	tr.retries = workqueue.NewTypedItemExponentialFailureRateLimiter[string](testRetryDelay, testMaxRetryDelay)
	t.Cleanup(tr.Wait)
	return tr
}

// stepTo advances the tracker's fake clock to at, unless it is there already,
// so the background work due by then starts.
func stepTo(tr *Tracker, at time.Time) {
	clk := tr.clock.(*testingclock.FakeClock)
	if d := at.Sub(clk.Now()); d > 0 {
		clk.Step(d)
	}
}

// testRef is a MEMORY snapshot of the tracked actor.
func testRef(uuid string, generation int32, commitTime time.Time) SnapshotRef {
	return SnapshotRef{
		UUID:       uuid,
		Generation: generation,
		Fidelity:   ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		URI:        "gs://test-bucket/atespaces/ate-demo/actors/" + trackedActorUID + "/snapshots/" + uuid,
		CommitTime: commitTime,
	}
}

// waitStarted waits for the next store call to start, and returns the uuid of
// the snapshot it uploads.
func waitStarted(t *testing.T, store *fakeSnapshotStore) string {
	t.Helper()
	select {
	case uuid := <-store.started:
		return uuid
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for an upload to start")
		return ""
	}
}

// opDone returns a channel closed once the actor's upload in progress has
// ended, deleting a superseded local snapshot included.
func opDone(t *testing.T, tr *Tracker, actorUID string) <-chan struct{} {
	t.Helper()
	tr.mu.Lock()
	defer tr.mu.Unlock()
	a := tr.findLocked(actorUID)
	if a == nil || a.op == nil {
		t.Fatal("no upload in progress")
	}
	return a.op.done
}

func waitClosed(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// startUpload advances the clock to when the tracked actor's upload is due,
// and waits for a worker to start it. It returns a channel closed once the
// upload has ended.
func startUpload(t *testing.T, tr *Tracker, store *fakeSnapshotStore) <-chan struct{} {
	t.Helper()
	due := timeOf(tr.State(trackedActorUID).GetUploadDueTime())
	if due.IsZero() {
		t.Fatal("no upload scheduled")
	}
	stepTo(tr, due)
	waitStarted(t, store)
	return opDone(t, tr, trackedActorUID)
}

// uploadNow runs the tracked actor's upload once it is due, and ends it on the
// blocking store with err.
func uploadNow(t *testing.T, tr *Tracker, store *fakeSnapshotStore, err error) {
	t.Helper()
	done := startUpload(t, tr, store)
	store.release <- err
	waitClosed(t, done, "the upload")
}

// processNext takes the next actor from the queue of a tracker whose workers
// have not started, and runs its background work as a worker would.
func processNext(t *testing.T, tr *Tracker) {
	t.Helper()
	next := make(chan string, 1)
	go func() {
		if actorUID, shutdown := tr.queue.Get(); !shutdown {
			next <- actorUID
		}
	}()
	select {
	case actorUID := <-next:
		tr.syncActor(actorUID)
		tr.queue.Done(actorUID)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for an actor to be queued")
	}
}

func timeOf(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

func uuidOf(r *ateletpb.SnapshotReport) string {
	return r.GetSnapshot().GetSnapshotUuid()
}

// Deferred checkpoints keep their snapshots local, and atelet uploads the
// latest one once the oldest snapshot not yet durable has waited the upload
// delay. The local copy stays after the upload.
func TestTrackerUploadsLatestLocalSnapshot(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	t0 := tr.clock.Now()

	keep, st := tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
	if !slices.Equal(keep, []string{"s1"}) {
		t.Errorf("keep = %v, want [s1]", keep)
	}
	if got := st.GetLatestLocal(); uuidOf(got) != "s1" || !timeOf(got.GetLocalCommitTime()).Equal(t0) {
		t.Errorf("latest_local = %v, want s1 committed at %v", got, t0)
	}
	if got, want := timeOf(st.GetUploadDueTime()), t0.Add(testUploadDelay); !got.Equal(want) {
		t.Errorf("upload_due_time = %v, want %v", got, want)
	}

	_, st = tr.CommitLocal(trackedActorUID, testRef("s2", 2, t0.Add(10*time.Minute)))
	if got, want := timeOf(st.GetUploadDueTime()), t0.Add(testUploadDelay); !got.Equal(want) {
		t.Errorf("upload_due_time = %v, want %v: when the first snapshot was due", got, want)
	}

	uploadNow(t, tr, store, nil)
	if got, want := store.uploads(), []uploadCall{{"s2", noFidelity}}; !slices.Equal(got, want) {
		t.Errorf("uploads = %v, want %v", got, want)
	}
	st = tr.State(trackedActorUID)
	if got := st.GetLatestDurable(); uuidOf(got) != "s2" || got.GetDurableCompleteTime() == nil {
		t.Errorf("latest_durable = %v, want s2 with its upload time", got)
	}
	if got := uuidOf(st.GetLatestLocal()); got != "s2" {
		t.Errorf("latest_local = %q, want s2 kept after its upload", got)
	}
	if st.GetUploading() != nil || st.GetUploadDueTime() != nil {
		t.Errorf("state = %v, want no upload in progress or due", st)
	}
}

// A deferred checkpoint during an older snapshot's upload keeps that snapshot
// until the upload ends. The upload's end deletes it and schedules the new
// snapshot's upload, which the older upload does not cover, an upload delay
// after the older upload succeeded.
func TestTrackerCheckpointDuringUpload(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	for _, name := range []string{"s1", "s2"} {
		if err := os.MkdirAll(ateletpath.LocalSnapshotDir(trackedActorUID, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	t0 := tr.clock.Now()
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
	done := startUpload(t, tr, store)

	keep, st := tr.CommitLocal(trackedActorUID, testRef("s2", 2, tr.clock.Now()))
	if !slices.Equal(keep, []string{"s2", "s1"}) {
		t.Errorf("keep = %v, want [s2 s1]: s1 is being uploaded", keep)
	}
	if got := uuidOf(st.GetUploading()); got != "s1" {
		t.Errorf("uploading = %q, want s1", got)
	}
	if got, want := timeOf(st.GetUploadDueTime()), t0.Add(testUploadDelay); !got.Equal(want) {
		t.Errorf("upload_due_time = %v, want %v: s1's, which the upload in progress delays", got, want)
	}

	stepTo(tr, tr.clock.Now().Add(10*time.Minute))
	store.release <- nil
	waitClosed(t, done, "the upload of s1")
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(trackedActorUID, "s1")); !os.IsNotExist(err) {
		t.Errorf("s1 after its upload: stat = %v, want it deleted", err)
	}
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(trackedActorUID, "s2")); err != nil {
		t.Errorf("s2 after the upload of s1: %v, want it kept", err)
	}
	st = tr.State(trackedActorUID)
	if got := uuidOf(st.GetLatestDurable()); got != "s1" {
		t.Errorf("latest_durable = %q, want s1", got)
	}
	if got, want := timeOf(st.GetUploadDueTime()), tr.clock.Now().Add(testUploadDelay); !got.Equal(want) {
		t.Errorf("upload_due_time = %v, want %v: an upload delay after the upload of s1 succeeded", got, want)
	}

	uploadNow(t, tr, store, nil)
	if got := uuidOf(tr.State(trackedActorUID).GetLatestDurable()); got != "s2" {
		t.Errorf("latest_durable = %q, want s2", got)
	}
}

// An immediate checkpoint makes the actor's newest snapshot durable, and
// drops the local snapshot. An older snapshot's upload in progress runs on:
// CommitDurable keeps that snapshot, and the upload deletes it when it ends.
func TestTrackerImmediateCheckpointDuringUpload(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	if err := os.MkdirAll(ateletpath.LocalSnapshotDir(trackedActorUID, "s1"), 0o700); err != nil {
		t.Fatal(err)
	}
	t0 := tr.clock.Now()
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
	done := startUpload(t, tr, store)

	s2 := testRef("s2", 2, t0.Add(time.Minute))
	s2.UploadTime = s2.CommitTime.Add(time.Second)
	keep, st := tr.CommitDurable(trackedActorUID, s2)
	if !slices.Equal(keep, []string{"s1"}) {
		t.Errorf("keep = %v, want [s1]: s1 is being uploaded", keep)
	}
	if st.GetLatestLocal() != nil || st.GetUploadDueTime() != nil || uuidOf(st.GetUploading()) != "s1" {
		t.Errorf("state = %v, want nothing local or due, and s1 uploading", st)
	}
	if got := st.GetLatestDurable(); uuidOf(got) != "s2" || !timeOf(got.GetDurableCompleteTime()).Equal(s2.UploadTime) {
		t.Errorf("latest_durable = %v, want s2 uploaded at %v", got, s2.UploadTime)
	}

	store.release <- nil
	waitClosed(t, done, "the upload of s1")
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(trackedActorUID, "s1")); !os.IsNotExist(err) {
		t.Errorf("s1 after its upload: stat = %v, want it deleted", err)
	}
	if got := uuidOf(tr.State(trackedActorUID).GetLatestDurable()); got != "s2" {
		t.Errorf("latest_durable = %q after the upload of s1, want s2", got)
	}

	// The next deferred checkpoint is due a full delay after it was taken.
	s3Time := tr.clock.Now()
	_, st = tr.CommitLocal(trackedActorUID, testRef("s3", 3, s3Time))
	if got, want := timeOf(st.GetUploadDueTime()), s3Time.Add(testUploadDelay); !got.Equal(want) {
		t.Errorf("upload_due_time = %v, want %v", got, want)
	}
}

// State reports the latest local snapshot a deferred checkpoint leaves, and
// the latest durable one an immediate checkpoint leaves without a local copy.
// Forget drops both.
func TestTrackerState(t *testing.T) {
	tr := newTestTracker(t, newFakeSnapshotStore())
	t0 := tr.clock.Now()
	_, st := tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
	if got := st.GetLatestLocal(); uuidOf(got) != "s1" || !timeOf(got.GetLocalCommitTime()).Equal(t0) {
		t.Errorf("latest_local = %v, want s1 committed at %v", got, t0)
	}
	if st.GetLatestDurable() != nil {
		t.Errorf("latest_durable = %v, want unset", st.GetLatestDurable())
	}

	s2 := testRef("s2", 2, t0.Add(time.Minute))
	s2.UploadTime = s2.CommitTime.Add(time.Second)
	_, st = tr.CommitDurable(trackedActorUID, s2)
	if st.GetLatestLocal() != nil {
		t.Errorf("latest_local = %v after an immediate checkpoint, want unset", st.GetLatestLocal())
	}
	if got := st.GetLatestDurable(); uuidOf(got) != "s2" || !timeOf(got.GetDurableCompleteTime()).Equal(s2.UploadTime) {
		t.Errorf("latest_durable = %v, want s2 uploaded at %v", got, s2.UploadTime)
	}

	_, st = tr.CommitLocal(trackedActorUID, testRef("s3", 3, t0.Add(2*time.Minute)))
	if uuidOf(st.GetLatestLocal()) != "s3" || uuidOf(st.GetLatestDurable()) != "s2" {
		t.Errorf("state = %v, want s3 local and s2 still durable", st)
	}
	if got := tr.State(trackedActorUID); !proto.Equal(got, st) {
		t.Errorf("State() = %v, want %v, as CommitLocal returned", got, st)
	}

	if err := tr.Forget(t.Context(), trackedActorUID); err != nil {
		t.Fatalf("forget: %v", err)
	}
	if got := tr.State(trackedActorUID); !proto.Equal(got, &ateletpb.ActorSnapshotState{}) {
		t.Errorf("state after forget = %v, want empty", got)
	}
}

// PromoteSnapshot makes the named snapshot durable while keeping its local
// copy, and checks its minimum fidelity.
func TestTrackerPromote(t *testing.T) {
	t.Run("local snapshot kept after promote", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr := newTestTracker(t, store)
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		want := testRef("s1", 1, time.Time{})
		want.Fidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES

		st, err := tr.Promote(t.Context(), trackedActorUID, want)
		if err != nil {
			t.Fatalf("promote: %v", err)
		}
		if got, wantCalls := store.uploads(), []uploadCall{{"s1", ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES}}; !slices.Equal(got, wantCalls) {
			t.Errorf("uploads = %v, want %v", got, wantCalls)
		}
		if uuidOf(st.GetLatestLocal()) != "s1" || st.GetUploadDueTime() != nil {
			t.Errorf("state = %v, want s1 kept locally and no upload due", st)
		}
		durable := st.GetLatestDurable()
		if got := durable.GetSnapshot().GetFidelity(); got != ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY {
			t.Errorf("latest_durable fidelity = %v, want the MEMORY it holds", got)
		}
		if durable.GetDurableCompleteTime() == nil || durable.GetLocalCommitTime() == nil {
			t.Errorf("latest_durable = %v, want its commit and upload times", durable)
		}
	})

	t.Run("untracked, as after a restart", func(t *testing.T) {
		tr := newTestTracker(t, newFakeSnapshotStore())
		want := testRef("s2", 2, time.Time{})
		st, err := tr.Promote(t.Context(), trackedActorUID, want)
		if err != nil {
			t.Fatalf("promote: %v", err)
		}
		if got := uuidOf(st.GetLatestDurable()); got != "s2" {
			t.Errorf("latest_durable = %q, want s2", got)
		}
	})
}

// A PromoteSnapshot for a newer snapshot than the one being uploaded waits for
// that upload to end, which deletes the superseded snapshot's local copy, and
// then uploads the newer one.
func TestTrackerPromoteWaitsForOlderUpload(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newIdleTestTracker(t, store)
	for _, name := range []string{"s1", "s2"} {
		if err := os.MkdirAll(ateletpath.LocalSnapshotDir(trackedActorUID, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	t0 := tr.clock.Now()
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
	stepTo(tr, t0.Add(testUploadDelay))
	// Upload s1 as a worker would, with none running to take the actor
	// once the upload ends.
	go tr.syncActor(trackedActorUID)
	if got := waitStarted(t, store); got != "s1" {
		t.Fatalf("started = %q, want s1", got)
	}
	tr.CommitLocal(trackedActorUID, testRef("s2", 2, tr.clock.Now()))

	store.setBlocking(false)
	promoted := make(chan error, 1)
	go func() {
		_, err := tr.Promote(t.Context(), trackedActorUID, testRef("s2", 2, time.Time{}))
		promoted <- err
	}()
	select {
	case err := <-promoted:
		t.Fatalf("promote returned (%v) during the upload of s1", err)
	case <-time.After(50 * time.Millisecond):
	}

	store.release <- nil
	if err := <-promoted; err != nil {
		t.Fatalf("promote: %v", err)
	}
	wantCalls := []uploadCall{{"s1", noFidelity}, {"s2", ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY}}
	if got := store.uploads(); !slices.Equal(got, wantCalls) {
		t.Errorf("uploads = %v, want %v", got, wantCalls)
	}
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(trackedActorUID, "s1")); !os.IsNotExist(err) {
		t.Errorf("s1 after the promote of s2: stat = %v, want it deleted", err)
	}
	st := tr.State(trackedActorUID)
	if uuidOf(st.GetLatestLocal()) != "s2" || uuidOf(st.GetLatestDurable()) != "s2" || st.GetUploadDueTime() != nil {
		t.Errorf("state = %v, want s2 local and durable, and no upload due", st)
	}
}

// A checkpoint taken during a PromoteSnapshot is due for upload an upload
// delay after the promote made the older snapshot durable.
func TestTrackerCheckpointDuringPromote(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	t0 := tr.clock.Now()
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
	promoted := make(chan error, 1)
	go func() {
		_, err := tr.Promote(t.Context(), trackedActorUID, testRef("s1", 1, time.Time{}))
		promoted <- err
	}()
	waitStarted(t, store)

	stepTo(tr, t0.Add(10*time.Minute))
	tr.CommitLocal(trackedActorUID, testRef("s2", 2, tr.clock.Now()))
	stepTo(tr, t0.Add(20*time.Minute))
	store.release <- nil
	if err := <-promoted; err != nil {
		t.Fatalf("Promote: %v", err)
	}
	if got, want := timeOf(tr.State(trackedActorUID).GetUploadDueTime()), t0.Add(20*time.Minute+testUploadDelay); !got.Equal(want) {
		t.Errorf("upload_due_time = %v, want %v: an upload delay after the promote succeeded", got, want)
	}
}

// A PromoteSnapshot that succeeds lifts the hold failed background uploads put
// on the actor, even when a newer checkpoint was taken meanwhile: the next
// failure waits the first retry delay again.
func TestTrackerPromoteResetsRetryDelay(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	uploadNow(t, tr, store, errors.New("boom"))

	promoted := make(chan error, 1)
	go func() {
		_, err := tr.Promote(t.Context(), trackedActorUID, testRef("s1", 1, time.Time{}))
		promoted <- err
	}()
	waitStarted(t, store)
	tr.CommitLocal(trackedActorUID, testRef("s2", 2, tr.clock.Now()))
	store.release <- nil
	if err := <-promoted; err != nil {
		t.Fatalf("promote: %v", err)
	}

	uploadNow(t, tr, store, errors.New("boom"))
	if got, want := timeOf(tr.State(trackedActorUID).GetUploadDueTime()), tr.clock.Now().Add(testRetryDelay); !got.Equal(want) {
		t.Errorf("upload_due_time = %v, want %v: the first retry delay after the failure", got, want)
	}
}

// An immediate checkpoint during a PromoteSnapshot keeps the promoted snapshot
// for the promote, which deletes the snapshot's local copy when it ends.
func TestTrackerImmediateCheckpointDuringPromote(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	if err := os.MkdirAll(ateletpath.LocalSnapshotDir(trackedActorUID, "s1"), 0o700); err != nil {
		t.Fatal(err)
	}
	t0 := tr.clock.Now()
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
	promoted := make(chan error, 1)
	go func() {
		_, err := tr.Promote(t.Context(), trackedActorUID, testRef("s1", 1, time.Time{}))
		promoted <- err
	}()
	waitStarted(t, store)

	keep, _ := tr.CommitDurable(trackedActorUID, testRef("s2", 2, t0.Add(time.Minute)))
	if !slices.Equal(keep, []string{"s1"}) {
		t.Errorf("keep = %v, want [s1]: the promote is uploading it", keep)
	}
	store.release <- nil
	if err := <-promoted; err != nil {
		t.Fatalf("promote: %v", err)
	}
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(trackedActorUID, "s1")); !os.IsNotExist(err) {
		t.Errorf("s1 after the promote: stat = %v, want it deleted", err)
	}
	if got := uuidOf(tr.State(trackedActorUID).GetLatestDurable()); got != "s2" {
		t.Errorf("latest_durable = %q, want s2", got)
	}
}

// A failed background upload is retried once the retry delay has passed, and
// the delay doubles with each further failure.
func TestTrackerRetriesFailedUpload(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

	for _, delay := range []time.Duration{testRetryDelay, 2 * testRetryDelay} {
		uploadNow(t, tr, store, errors.New("boom"))
		st := tr.State(trackedActorUID)
		if st.GetLatestDurable() != nil {
			t.Errorf("latest_durable = %v after a failed upload, want unset", st.GetLatestDurable())
		}
		if got, want := timeOf(st.GetUploadDueTime()), tr.clock.Now().Add(delay); !got.Equal(want) {
			t.Errorf("upload_due_time = %v, want %v: %v after the failure", got, want, delay)
		}
	}

	uploadNow(t, tr, store, nil)
	if got := uuidOf(tr.State(trackedActorUID).GetLatestDurable()); got != "s1" {
		t.Errorf("latest_durable = %q after the retry, want s1", got)
	}
}

// A failed upload leaves the actor undurable since its oldest snapshot not yet
// durable, rather than since the snapshot the upload read, and a checkpoint
// taken during the upload does not move that.
func TestTrackerFailedUploadKeepsUndurableSince(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	t0 := tr.clock.Now()
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
	tr.CommitLocal(trackedActorUID, testRef("s2", 2, t0.Add(50*time.Minute)))
	done := startUpload(t, tr, store)
	tr.CommitLocal(trackedActorUID, testRef("s3", 3, tr.clock.Now()))

	store.release <- errors.New("boom")
	waitClosed(t, done, "the upload of s2")
	if got, want := timeOf(tr.State(trackedActorUID).GetUploadDueTime()), tr.clock.Now().Add(testRetryDelay); !got.Equal(want) {
		t.Errorf("upload_due_time = %v, want %v: the retry delay after the failure, as s1 has waited over an upload delay", got, want)
	}
}

// Forget waits for an upload in progress to end, then drops the actor. A
// Forget that gives up waiting keeps the actor, so a retry waits again.
func TestTrackerForgetWaitsForUpload(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	startUpload(t, tr, store)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := tr.Forget(ctx, trackedActorUID); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("forget during the upload = %v, want DeadlineExceeded", err)
	}
	if got := uuidOf(tr.State(trackedActorUID).GetLatestLocal()); got != "s1" {
		t.Fatalf("latest_local = %q after a forget gave up, want s1 kept for a retry", got)
	}

	forgot := make(chan error, 1)
	go func() { forgot <- tr.Forget(t.Context(), trackedActorUID) }()
	select {
	case err := <-forgot:
		t.Fatalf("forget returned (%v) during the upload", err)
	case <-time.After(50 * time.Millisecond):
	}
	store.release <- nil
	if err := <-forgot; err != nil {
		t.Fatalf("forget: %v", err)
	}
	if st := tr.State(trackedActorUID); !proto.Equal(st, &ateletpb.ActorSnapshotState{}) {
		t.Errorf("state after forget = %v, want empty", st)
	}
}

// The workers bound how many background uploads run at once, and an actor
// that checkpoints again while it waits for a worker uploads only its newest
// snapshot.
func TestTrackerWorkersBoundBackgroundWork(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newIdleTestTracker(t, store)
	tr.workers = 2
	tr.Start()

	// Snapshots taken an upload delay ago are due for upload straight away.
	taken := tr.clock.Now().Add(-testUploadDelay)
	for _, id := range []string{"actor-1", "actor-2", "actor-3"} {
		tr.CommitLocal(id, testRef(id+"-s1", 1, taken))
	}
	started := []string{waitStarted(t, store), waitStarted(t, store)}
	slices.Sort(started)
	if want := []string{"actor-1-s1", "actor-2-s1"}; !slices.Equal(started, want) {
		t.Fatalf("first started = %v, want %v", started, want)
	}
	select {
	case got := <-store.started:
		t.Fatalf("a third upload (%q) started while both workers are busy", got)
	default:
	}
	if st := tr.State("actor-3"); st.GetUploading() != nil || st.GetUploadDueTime() == nil {
		t.Errorf("actor-3 state while it waits = %v, want an upload due and none in progress", st)
	}

	// actor-3 checkpoints again while it waits for a worker.
	tr.CommitLocal("actor-3", testRef("actor-3-s2", 2, taken.Add(time.Minute)))

	done1 := opDone(t, tr, "actor-1")
	done2 := opDone(t, tr, "actor-2")
	// Ending one upload frees a worker, which takes actor-3.
	store.release <- nil
	if got := waitStarted(t, store); got != "actor-3-s2" {
		t.Fatalf("started = %q, want actor-3-s2", got)
	}
	done3 := opDone(t, tr, "actor-3")
	store.release <- nil
	store.release <- nil
	waitClosed(t, done1, "actor-1's upload")
	waitClosed(t, done2, "actor-2's upload")
	waitClosed(t, done3, "actor-3's upload")

	if got := uuidOf(tr.State("actor-3").GetLatestDurable()); got != "actor-3-s2" {
		t.Errorf("actor-3 latest_durable = %q, want actor-3-s2", got)
	}
	if got := len(store.uploads()); got != 3 {
		t.Errorf("uploads = %v, want 3: actor-3-s1 superseded before it was uploaded", store.uploads())
	}
}

// The queue holds an actor until the earliest time it was queued for. A worker
// that takes the actor before its upload is due, as after an immediate
// checkpoint made the snapshot it was queued for durable, does nothing and
// queues it again for when its upload is due.
func TestTrackerRequeuesUntilWorkIsDue(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newIdleTestTracker(t, store)
	t0 := tr.clock.Now()
	// Queued for the upload of s1, due an upload delay out. The immediate
	// checkpoint makes s2 durable, so the upload of s3 is due an upload delay
	// after s3 was taken, later.
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
	tr.CommitDurable(trackedActorUID, testRef("s2", 2, t0.Add(10*time.Minute)))
	tr.CommitLocal(trackedActorUID, testRef("s3", 3, t0.Add(20*time.Minute)))

	stepTo(tr, t0.Add(testUploadDelay))
	processNext(t, tr)
	if got := store.uploads(); len(got) != 0 {
		t.Errorf("uploads = %v before the upload of s3 is due, want none", got)
	}

	stepTo(tr, t0.Add(20*time.Minute+testUploadDelay))
	processNext(t, tr)
	if got, want := store.uploads(), []uploadCall{{"s3", noFidelity}}; !slices.Equal(got, want) {
		t.Errorf("uploads = %v, want %v", got, want)
	}
}
