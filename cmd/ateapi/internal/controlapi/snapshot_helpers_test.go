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

package controlapi

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestFindSnapshotByGenerationAndSnapshotAtLatestGeneration(t *testing.T) {
	if got := snapshotAtLatestGeneration(nil); got != nil {
		t.Errorf("snapshotAtLatestGeneration(nil) = %v, want nil", got)
	}
	if got := findSnapshotByGeneration(nil, 1); got != nil {
		t.Errorf("findSnapshotByGeneration(nil, 1) = %v, want nil", got)
	}

	s1 := newPausedSnapshot(t, 1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-1", "node-1")
	s2 := newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-2", "gs://b/snap-2", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	status := &ateapipb.ActorStatus{
		LastAssignedGeneration: 2,
		Snapshots:              []*ateapipb.Snapshot{s1, s2},
	}

	if got := findSnapshotByGeneration(status.GetSnapshots(), 1); got != s1 {
		t.Errorf("findSnapshotByGeneration(1) = %v, want %v", got, s1)
	}
	if got := findSnapshotByGeneration(status.GetSnapshots(), 99); got != nil {
		t.Errorf("findSnapshotByGeneration(99) = %v, want nil", got)
	}
	if got := snapshotAtLatestGeneration(status); got != s2 {
		t.Errorf("snapshotAtLatestGeneration() = %v, want %v", got, s2)
	}
}

func TestFindLatestDurableAndLocalSnapshots(t *testing.T) {
	if snap := findLatestDurableSnapshot(nil, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED); snap != nil {
		t.Errorf("findLatestDurableSnapshot(nil) = %v, want nil", snap)
	}
	if snap := findLatestLocalSnapshot(nil); snap != nil {
		t.Errorf("findLatestLocalSnapshot(nil) = %v, want nil", snap)
	}

	s1 := newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	s2 := newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-2", "gs://b/snap-2", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	s3 := newDurableSnapshot(3, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-3", "gs://b/snap-3", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
	s4 := newPausedSnapshot(t, 4, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-4", "node-1")

	// Place s2 before s1 to verify generation comparison rather than slice order.
	status := &ateapipb.ActorStatus{
		Snapshots: []*ateapipb.Snapshot{s2, s1, s3, s4},
	}

	gotSnap := findLatestDurableSnapshot(status, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	if gotSnap != s2 || gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri() != "gs://b/snap-2" {
		t.Errorf("findLatestDurableSnapshot(COMPLETED) = %v, want gen 2 (gs://b/snap-2)", gotSnap)
	}

	// A paused snapshot's durable copy is PENDING until atelet uploads it.
	gotSnap = findLatestDurableSnapshot(status, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
	if gotSnap != s4 {
		t.Errorf("findLatestDurableSnapshot(PENDING) = %v, want gen 4", gotSnap)
	}
	gotSnap = findLatestDurableSnapshot(&ateapipb.ActorStatus{Snapshots: []*ateapipb.Snapshot{s2, s1, s3}}, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
	if gotSnap != s3 || gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri() != "gs://b/snap-3" {
		t.Errorf("findLatestDurableSnapshot(PENDING) without the paused snapshot = %v, want gen 3 (gs://b/snap-3)", gotSnap)
	}

	gotLocal := findLatestLocalSnapshot(status)
	if gotLocal != s4 || gotLocal.GetUuid() != "local-4" || gotLocal.GetLocality() != "node-1" {
		t.Errorf("findLatestLocalSnapshot() = %v, want %v", gotLocal, s4)
	}
}

func TestSnapshotPruningHelpers(t *testing.T) {
	ctx := context.Background()
	// Should not panic on nil status.
	clearLocalSnapshots(nil)
	pruneSnapshots(ctx, nil, nil)

	t.Run("clearLocalSnapshots clears locality without dropping snapshots", func(t *testing.T) {
		localGen1 := newPausedSnapshot(t, 1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-1", "node-1")
		localGen2 := newPausedSnapshot(t, 2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-2", "node-2")
		status := &ateapipb.ActorStatus{
			Snapshots: []*ateapipb.Snapshot{localGen1, localGen2},
		}

		clearLocalSnapshots(status)

		want := []*ateapipb.Snapshot{
			newPausedSnapshot(t, 1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-1", ""),
			newPausedSnapshot(t, 2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-2", ""),
		}
		if diff := cmp.Diff(want, status.GetSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.Snapshots mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("pause flow: pruneSnapshots keeps the latest COMPLETED durable and the new local snapshot", func(t *testing.T) {
		completedGen1 := newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
		completedGen1.Locality = "node-1"
		localGen2 := newPausedSnapshot(t, 2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-2", "node-2")
		localGen2URI := localGen2.GetDurableSnapshot().GetObject().GetSnapshotUri()
		localGen3 := newPausedSnapshot(t, 3, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-3", "")

		status := &ateapipb.ActorStatus{
			LastAssignedGeneration: 3,
			Snapshots:              []*ateapipb.Snapshot{completedGen1, localGen2, localGen3},
		}

		localGen3.Locality = "node-3"
		pruneSnapshots(ctx, status, localGen3)

		want := []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
			newPausedSnapshot(t, 3, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-3", "node-3"),
		}
		if diff := cmp.Diff(want, status.GetSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.Snapshots mismatch (-want +got):\n%s", diff)
		}
		wantPending := []*ateapipb.PendingGCSnapshot{{Uuid: "local-2", Generation: 2, Object: &ateapipb.ObjectSnapshot{SnapshotUri: localGen2URI}}}
		if diff := cmp.Diff(wantPending, status.GetPendingGcSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.PendingGcSnapshots mismatch (-want +got):\n%s", diff)
		}
		if status.GetLastAssignedGeneration() != 3 {
			t.Errorf("LastAssignedGeneration = %d, want 3", status.GetLastAssignedGeneration())
		}
	})

	t.Run("pruneSnapshots drops a PENDING snapshot that still names a node", func(t *testing.T) {
		pendingWithLocality := newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-2", "gs://b/snap-2", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
		pendingWithLocality.Locality = "node-1"
		local := newPausedSnapshot(t, 3, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-3", "node-1")
		status := &ateapipb.ActorStatus{
			Snapshots: []*ateapipb.Snapshot{
				newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
				pendingWithLocality,
				local,
			},
		}

		pruneSnapshots(ctx, status, local)

		want := []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
			newPausedSnapshot(t, 3, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-3", "node-1"),
		}
		if diff := cmp.Diff(want, status.GetSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.Snapshots mismatch (-want +got):\n%s", diff)
		}
		wantPending := []*ateapipb.PendingGCSnapshot{{Uuid: "uuid-2", Generation: 2, Object: &ateapipb.ObjectSnapshot{SnapshotUri: "gs://b/snap-2"}}}
		if diff := cmp.Diff(wantPending, status.GetPendingGcSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.PendingGcSnapshots mismatch (-want +got):\n%s", diff)
		}
		if pendingWithLocality.GetLocality() != "" {
			t.Errorf("dropped PENDING snapshot Locality = %q, want cleared", pendingWithLocality.GetLocality())
		}
	})

	t.Run("revert flow: pruneSnapshots drops all PENDING snapshots and retains latest COMPLETED", func(t *testing.T) {
		completedWithLocality := newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
		completedWithLocality.Locality = "node-1"
		localGen2 := newPausedSnapshot(t, 2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-2", "node-2")
		status := &ateapipb.ActorStatus{
			Snapshots: []*ateapipb.Snapshot{
				completedWithLocality,
				localGen2,
				newDurableSnapshot(3, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-3", "gs://b/snap-3", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
			},
		}

		pruneSnapshots(ctx, status, nil)

		want := []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
		}
		if diff := cmp.Diff(want, status.GetSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.Snapshots mismatch (-want +got):\n%s", diff)
		}
		wantPending := []*ateapipb.PendingGCSnapshot{
			{Uuid: "local-2", Generation: 2, Object: &ateapipb.ObjectSnapshot{SnapshotUri: localGen2.GetDurableSnapshot().GetObject().GetSnapshotUri()}},
			{Uuid: "uuid-3", Generation: 3, Object: &ateapipb.ObjectSnapshot{SnapshotUri: "gs://b/snap-3"}},
		}
		if diff := cmp.Diff(wantPending, status.GetPendingGcSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.PendingGcSnapshots mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("suspend flow: pruneSnapshots drops older COMPLETED and PENDING snapshots and skips tag-owned", func(t *testing.T) {
		localGen2 := newPausedSnapshot(t, 2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-2", "node-1")
		status := &ateapipb.ActorStatus{
			Snapshots: []*ateapipb.Snapshot{
				newDurableSnapshot(0, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "tag-0", "gs://b/tag-0", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
				newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/snap-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
				localGen2,
				newDurableSnapshot(3, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-3", "gs://b/snap-3", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
			},
			PendingGcSnapshots: []*ateapipb.PendingGCSnapshot{
				{Uuid: "uuid-1", Generation: 1, Object: &ateapipb.ObjectSnapshot{SnapshotUri: "gs://b/snap-1"}},
			},
		}
		status.Snapshots[3].Locality = "node-1"

		pruneSnapshots(ctx, status, nil)

		want := []*ateapipb.Snapshot{
			newDurableSnapshot(3, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-3", "gs://b/snap-3", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
		}
		if diff := cmp.Diff(want, status.GetSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.Snapshots mismatch (-want +got):\n%s", diff)
		}
		wantPending := []*ateapipb.PendingGCSnapshot{
			{Uuid: "uuid-1", Generation: 1, Object: &ateapipb.ObjectSnapshot{SnapshotUri: "gs://b/snap-1"}},
			{Uuid: "local-2", Generation: 2, Object: &ateapipb.ObjectSnapshot{SnapshotUri: localGen2.GetDurableSnapshot().GetObject().GetSnapshotUri()}},
		}
		if diff := cmp.Diff(wantPending, status.GetPendingGcSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.PendingGcSnapshots mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("pruneSupersededDurableSnapshots moves older COMPLETED snapshots to PendingGcSnapshots", func(t *testing.T) {
		pruneSupersededDurableSnapshots(ctx, nil)

		localPending := newPausedSnapshot(t, 4, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-4", "node-1")
		status := &ateapipb.ActorStatus{
			Snapshots: []*ateapipb.Snapshot{
				newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "tag-1", "gs://b/tag-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
				newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-2", "gs://b/snap-2", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
				newDurableSnapshot(3, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-3", "gs://b/snap-3", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
				localPending,
			},
		}

		pruneSupersededDurableSnapshots(ctx, status)

		want := []*ateapipb.Snapshot{
			newDurableSnapshot(3, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-3", "gs://b/snap-3", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
			localPending,
		}
		if diff := cmp.Diff(want, status.GetSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.Snapshots mismatch (-want +got):\n%s", diff)
		}
		wantPending := []*ateapipb.PendingGCSnapshot{
			{Uuid: "uuid-2", Generation: 2, Object: &ateapipb.ObjectSnapshot{SnapshotUri: "gs://b/snap-2"}},
		}
		if diff := cmp.Diff(wantPending, status.GetPendingGcSnapshots(), protocmp.Transform()); diff != "" {
			t.Errorf("status.PendingGcSnapshots mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("pruneSnapshots drops earliest PendingGcSnapshots when at maxPendingGCSnapshots", func(t *testing.T) {
		pending := make([]*ateapipb.PendingGCSnapshot, maxPendingGCSnapshots)
		for i := range pending {
			gen := int32(i + 1)
			pending[i] = &ateapipb.PendingGCSnapshot{
				Uuid:       fmt.Sprintf("snap-%d", gen),
				Generation: gen,
				Object:     &ateapipb.ObjectSnapshot{SnapshotUri: fmt.Sprintf("gs://b/snap-%d", gen)},
			}
		}
		localSnap := newPausedSnapshot(t, maxPendingGCSnapshots+2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "local-next", "node-1")
		latestCompleted := newDurableSnapshot(maxPendingGCSnapshots+3, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "snap-latest", "gs://b/snap-latest", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
		status := &ateapipb.ActorStatus{
			Snapshots: []*ateapipb.Snapshot{
				newDurableSnapshot(maxPendingGCSnapshots+1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "snap-old-durable", "gs://b/snap-old-durable", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
				localSnap,
				latestCompleted,
			},
			PendingGcSnapshots: pending,
		}

		pruneSnapshots(ctx, status, nil)

		if got := len(status.GetPendingGcSnapshots()); got != maxPendingGCSnapshots {
			t.Fatalf("len(PendingGcSnapshots) = %d, want %d", got, maxPendingGCSnapshots)
		}
		// Generations 1 and 2 were dropped to make room for the two newly pruned
		// snapshots (maxPendingGCSnapshots+1 and maxPendingGCSnapshots+2).
		if got := status.GetPendingGcSnapshots()[0].GetGeneration(); got != 3 {
			t.Errorf("first PendingGcSnapshot generation = %d, want 3", got)
		}
		if got := status.GetPendingGcSnapshots()[maxPendingGCSnapshots-2].GetGeneration(); got != maxPendingGCSnapshots+1 {
			t.Errorf("second-to-last PendingGcSnapshot generation = %d, want %d", got, maxPendingGCSnapshots+1)
		}
		if got := status.GetPendingGcSnapshots()[maxPendingGCSnapshots-1].GetGeneration(); got != maxPendingGCSnapshots+2 {
			t.Errorf("last PendingGcSnapshot generation = %d, want %d", got, maxPendingGCSnapshots+2)
		}
	})
}

// TestLatestSnapshotIsLocalOnly pins a pause's snapshot whose upload is not
// recorded yet: the latest snapshot is node-local with no COMPLETED durable
// copy, whether it has no copy or a PENDING one. A durable latest snapshot, or
// a local snapshot that is not captured yet, do not count; a RUNNING actor
// that kept its local snapshot across a resume does.
func TestLatestSnapshotIsLocalOnly(t *testing.T) {
	local := func(gen int32) *ateapipb.Snapshot {
		return newPausedSnapshot(t, gen, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local", "node-1")
	}
	durable := func(gen int32, st ateapipb.SnapshotStorageStatus) *ateapipb.Snapshot {
		return newDurableSnapshot(gen, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "durable", "gs://b/durable", st)
	}
	uploading := local(2)
	uploading.DurableSnapshot = &ateapipb.SnapshotStorage{Status: ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING, Object: &ateapipb.ObjectSnapshot{SnapshotUri: "gs://b/up"}}

	tests := []struct {
		name   string
		status *ateapipb.ActorStatus
		want   bool
	}{
		{"nil status", nil, false},
		{"no snapshots", &ateapipb.ActorStatus{}, false},
		{"local only above a durable one", &ateapipb.ActorStatus{LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{durable(1, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED), local(2)}}, true},
		{"local only alone", &ateapipb.ActorStatus{LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{local(1)}}, true},
		{"durable only", &ateapipb.ActorStatus{LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{durable(2, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)}}, false},
		{"local with an upload pending", &ateapipb.ActorStatus{LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{uploading}}, true},
		{"local not yet captured", &ateapipb.ActorStatus{LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{newPausedSnapshot(t, 1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local", "")}}, false},
		{"older local only under a pause in progress", &ateapipb.ActorStatus{LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{local(1), newPausedSnapshot(t, 2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local", "")}}, false},
		{"latest generation dropped by a revert", &ateapipb.ActorStatus{LastAssignedGeneration: 3, Snapshots: []*ateapipb.Snapshot{local(2)}}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := latestSnapshotIsLocalOnly(tc.status); got != tc.want {
				t.Errorf("latestSnapshotIsLocalOnly = %t, want %t", got, tc.want)
			}
		})
	}
}

// TestLatestSnapshotIsLocal pins the shape a paused actor has: the latest
// snapshot is node-local, whether or not atelet has uploaded it. A latest
// snapshot with only a durable copy, or a local one not captured yet, does not
// count.
func TestLatestSnapshotIsLocal(t *testing.T) {
	local := func(gen int32) *ateapipb.Snapshot {
		return newPausedSnapshot(t, gen, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local", "node-1")
	}
	withCopy := func(st ateapipb.SnapshotStorageStatus) *ateapipb.Snapshot {
		snap := local(1)
		snap.DurableSnapshot = &ateapipb.SnapshotStorage{Status: st, Object: &ateapipb.ObjectSnapshot{SnapshotUri: "gs://b/up"}}
		return snap
	}
	durable := newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "durable", "gs://b/durable", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	notCaptured := newPausedSnapshot(t, 2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local", "")

	tests := []struct {
		name   string
		status *ateapipb.ActorStatus
		want   bool
	}{
		{"nil status", nil, false},
		{"no snapshots", &ateapipb.ActorStatus{}, false},
		{"local only", &ateapipb.ActorStatus{LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{local(1)}}, true},
		{"local and uploaded", &ateapipb.ActorStatus{LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{withCopy(ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)}}, true},
		{"local with an upload pending", &ateapipb.ActorStatus{LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{withCopy(ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)}}, true},
		{"durable only", &ateapipb.ActorStatus{LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{durable}}, false},
		{"older local under a pause in progress", &ateapipb.ActorStatus{LastAssignedGeneration: 2, Snapshots: []*ateapipb.Snapshot{local(1), notCaptured}}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := latestSnapshotIsLocal(tc.status); got != tc.want {
				t.Errorf("latestSnapshotIsLocal = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestTagDurableSnapshotURI(t *testing.T) {
	if got := tagDurableSnapshotURI(nil, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_UNSPECIFIED); got != "" {
		t.Errorf("tagDurableSnapshotURI(nil) = %q, want empty", got)
	}

	pendingTag := &ateapipb.Tag{
		Status: &ateapipb.TagStatus{
			Snapshot: newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-1", "uuid-1", "gs://b/tag-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
		},
	}
	if got := tagDurableSnapshotURI(pendingTag, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED); got != "" {
		t.Errorf("tagDurableSnapshotURI(pendingTag, COMPLETED) = %q, want empty", got)
	}
	if got := tagDurableSnapshotURI(pendingTag, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING); got != "gs://b/tag-1" {
		t.Errorf("tagDurableSnapshotURI(pendingTag, PENDING) = %q, want gs://b/tag-1", got)
	}
	if got := tagDurableSnapshotURI(pendingTag, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_UNSPECIFIED); got != "gs://b/tag-1" {
		t.Errorf("tagDurableSnapshotURI(pendingTag, UNSPECIFIED) = %q, want gs://b/tag-1", got)
	}
}
