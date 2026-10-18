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
	"log/slog"
	"slices"

	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

// maxPendingGCSnapshots is the +k8s:maxItems cap on
// ActorStatus.pending_gc_snapshots.
const maxPendingGCSnapshots = 256

// findSnapshotByGeneration returns the Snapshot in snapshots with the given
// generation, or nil if none exists.
func findSnapshotByGeneration(snapshots []*ateapipb.Snapshot, gen int32) *ateapipb.Snapshot {
	for _, snap := range snapshots {
		if snap.GetGeneration() == gen {
			return snap
		}
	}
	return nil
}

// snapshotAtLatestGeneration returns the Snapshot at status.last_assigned_generation, or
// nil if none exists.
func snapshotAtLatestGeneration(status *ateapipb.ActorStatus) *ateapipb.Snapshot {
	if status == nil {
		return nil
	}
	return findSnapshotByGeneration(status.GetSnapshots(), status.GetLastAssignedGeneration())
}

// latestSnapshot returns the highest-generation Snapshot on status that
// matches, or nil if none does.
func latestSnapshot(status *ateapipb.ActorStatus, matches func(*ateapipb.Snapshot) bool) *ateapipb.Snapshot {
	var latest *ateapipb.Snapshot
	for _, snap := range status.GetSnapshots() {
		if matches(snap) && (latest == nil || snap.GetGeneration() >= latest.GetGeneration()) {
			latest = snap
		}
	}
	return latest
}

// findLatestDurableSnapshot returns the highest-generation Snapshot on status
// whose DurableSnapshot matches storageStatus, or nil if none exists.
func findLatestDurableSnapshot(status *ateapipb.ActorStatus, storageStatus ateapipb.SnapshotStorageStatus) *ateapipb.Snapshot {
	return latestSnapshot(status, func(snap *ateapipb.Snapshot) bool {
		return snap.GetDurableSnapshot().GetStatus() == storageStatus
	})
}

// findLatestLocalSnapshot returns the highest-generation Snapshot on status
// that holds a completed local checkpoint (Locality != ""), or nil if none
// exists.
func findLatestLocalSnapshot(status *ateapipb.ActorStatus) *ateapipb.Snapshot {
	return latestSnapshot(status, func(snap *ateapipb.Snapshot) bool {
		return snap.GetLocality() != ""
	})
}

// latestSnapshotIsLocal reports whether the snapshot at the actor's latest
// generation is node-local: the shape a pause leaves behind, which atelet's
// upload of the snapshot doesn't change. A paused actor is SUSPENDED with this
// shape, and resumes only on the node named by Locality. A RUNNING actor
// resumed from pause keeps the local snapshot too, so callers that need "at
// rest" check the state as well.
func latestSnapshotIsLocal(status *ateapipb.ActorStatus) bool {
	return snapshotAtLatestGeneration(status).GetLocality() != ""
}

// latestSnapshotIsLocalOnly reports whether the snapshot at the actor's latest
// generation is node-local with no COMPLETED durable copy: a pause's snapshot
// whose upload is not recorded yet.
func latestSnapshotIsLocalOnly(status *ateapipb.ActorStatus) bool {
	snap := snapshotAtLatestGeneration(status)
	return snap.GetLocality() != "" &&
		snap.GetDurableSnapshot().GetStatus() != ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
}

// clearLocalSnapshots clears Locality across all Snapshots in status.Snapshots
// without dropping PENDING durable snapshots (so DeleteActor can still collect
// an in-flight durable snapshot after releasing the worker).
func clearLocalSnapshots(status *ateapipb.ActorStatus) {
	for _, snap := range status.GetSnapshots() {
		snap.Locality = ""
	}
}

// pruneSnapshots retains only local (the Snapshot whose local checkpoint the
// actor keeps, or nil) and the latest COMPLETED durable snapshot, dropping all
// other Snapshots.
//
// It clears Locality on every Snapshot other than local, since callers run
// after an atelet call that deleted the actor's other local checkpoints from
// the node.
//
// Dropped actor-owned snapshots with an object storage URI are moved into
// status.PendingGcSnapshots so their prefixes are deleted once a newer
// generation is durable.
//
// status.LastAssignedGeneration is not decreased so that we can keep track of
// stale snapshots that need to be cleaned up and discarded generation numbers
// are never reused.
func pruneSnapshots(ctx context.Context, status *ateapipb.ActorStatus, local *ateapipb.Snapshot) {
	if status == nil {
		return
	}
	latestDurable := findLatestDurableSnapshot(status, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	kept := status.Snapshots[:0]
	for _, snap := range status.Snapshots {
		if snap != local {
			snap.Locality = ""
		}
		if snap == local || snap == latestDurable {
			kept = append(kept, snap)
			continue
		}
		addPendingGCSnapshot(ctx, status, snap)
	}
	status.Snapshots = kept
}

// pruneSupersededDurableSnapshots drops COMPLETED snapshots older than the
// latest COMPLETED one and records actor-owned ones in
// status.PendingGcSnapshots. It leaves local and PENDING snapshots intact.
func pruneSupersededDurableSnapshots(ctx context.Context, status *ateapipb.ActorStatus) {
	if status == nil {
		return
	}
	latestDurable := findLatestDurableSnapshot(status, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	if latestDurable == nil {
		return
	}
	kept := status.Snapshots[:0]
	for _, snap := range status.Snapshots {
		if snap.GetDurableSnapshot().GetStatus() == ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED &&
			snap.GetGeneration() < latestDurable.GetGeneration() &&
			snap.GetLocality() == "" {
			addPendingGCSnapshot(ctx, status, snap)
			continue
		}
		kept = append(kept, snap)
	}
	status.Snapshots = kept
}

// addPendingGCSnapshot records a dropped actor-owned snapshot in
// status.PendingGcSnapshots so its object storage prefix can be garbage
// collected once a newer generation is durable. If the list is already at
// maxPendingGCSnapshots, the earliest entries are dropped and left in storage
// until the actor is deleted.
func addPendingGCSnapshot(ctx context.Context, status *ateapipb.ActorStatus, snap *ateapipb.Snapshot) {
	if snap.GetOwner() != ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR {
		return
	}
	uri := snap.GetDurableSnapshot().GetObject().GetSnapshotUri()
	if uri == "" {
		return
	}
	for _, existing := range status.GetPendingGcSnapshots() {
		if existing.GetGeneration() == snap.GetGeneration() {
			return
		}
	}
	if overflow := len(status.PendingGcSnapshots) - maxPendingGCSnapshots + 1; overflow > 0 {
		for _, dropped := range status.PendingGcSnapshots[:overflow] {
			slog.WarnContext(ctx, "Dropping earliest pending GC snapshot because the list is at its limit; its objects stay in storage until the actor is deleted",
				slog.String("snapshot_uuid", dropped.GetUuid()),
				slog.Int("generation", int(dropped.GetGeneration())),
				slog.String("snapshot_uri", dropped.GetObject().GetSnapshotUri()))
		}
		status.PendingGcSnapshots = slices.Clone(status.PendingGcSnapshots[overflow:])
	}
	status.PendingGcSnapshots = append(status.PendingGcSnapshots, &ateapipb.PendingGCSnapshot{
		Uuid:       snap.GetUuid(),
		Generation: snap.GetGeneration(),
		Object:     &ateapipb.ObjectSnapshot{SnapshotUri: uri},
	})
}

// newDurableSnapshot constructs a Snapshot with DurableSnapshot populated.
func newDurableSnapshot(gen int32, owner ateapipb.SnapshotOwner, fidelity ateapipb.SnapshotFidelity, templateUID, uuid, uri string, storageStatus ateapipb.SnapshotStorageStatus) *ateapipb.Snapshot {
	return &ateapipb.Snapshot{
		Uuid:             uuid,
		Generation:       gen,
		Owner:            owner,
		Fidelity:         fidelity,
		ActorTemplateUid: templateUID,
		DurableSnapshot: &ateapipb.SnapshotStorage{
			Status: storageStatus,
			Object: &ateapipb.ObjectSnapshot{SnapshotUri: uri},
		},
	}
}

// tagDurableSnapshotURI returns the durable snapshot URI on tag matching
// storageStatus (or any status if storageStatus is UNSPECIFIED), or "" if none
// exists.
func tagDurableSnapshotURI(tag *ateapipb.Tag, storageStatus ateapipb.SnapshotStorageStatus) string {
	st := tag.GetStatus().GetSnapshot().GetDurableSnapshot()
	if storageStatus != ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_UNSPECIFIED && st.GetStatus() != storageStatus {
		return ""
	}
	return st.GetObject().GetSnapshotUri()
}
