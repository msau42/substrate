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

import "github.com/agent-substrate/substrate/pkg/proto/ateapipb"

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

// latestSnapshot returns the Snapshot at status.latest_snapshot_generation, or
// nil if none exists.
func latestSnapshot(status *ateapipb.ActorStatus) *ateapipb.Snapshot {
	if status == nil {
		return nil
	}
	return findSnapshotByGeneration(status.GetSnapshots(), status.GetLatestSnapshotGeneration())
}

// findSnapshotStorage returns the SnapshotStorage entry on snap with the given
// durability, or nil if none exists.
func findSnapshotStorage(snap *ateapipb.Snapshot, durability ateapipb.SnapshotDurability) *ateapipb.SnapshotStorage {
	for _, st := range snap.GetStorage() {
		if st.GetDurability() == durability {
			return st
		}
	}
	return nil
}

// setSnapshotStorage replaces an existing SnapshotStorage entry on snap with
// the same durability, or appends entry if none exists.
func setSnapshotStorage(snap *ateapipb.Snapshot, entry *ateapipb.SnapshotStorage) {
	for i, st := range snap.Storage {
		if st.GetDurability() == entry.GetDurability() {
			snap.Storage[i] = entry
			return
		}
	}
	snap.Storage = append(snap.Storage, entry)
}

// removeSnapshotStorage removes any SnapshotStorage entry on snap with the
// given durability.
func removeSnapshotStorage(snap *ateapipb.Snapshot, durability ateapipb.SnapshotDurability) {
	if snap == nil {
		return
	}
	filtered := snap.Storage[:0]
	for _, st := range snap.Storage {
		if st.GetDurability() != durability {
			filtered = append(filtered, st)
		}
	}
	snap.Storage = filtered
}

// completedLocalSnapshot returns the highest-generation Snapshot on status
// that holds a completed LOCAL SnapshotStorage entry, along with its
// LocalSnapshot payload.
func completedLocalSnapshot(status *ateapipb.ActorStatus) (*ateapipb.Snapshot, *ateapipb.LocalSnapshot) {
	var bestSnap *ateapipb.Snapshot
	var bestLocal *ateapipb.LocalSnapshot
	for _, snap := range status.GetSnapshots() {
		st := findSnapshotStorage(snap, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL)
		if st.GetStatus() != ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED || st.GetLocal() == nil {
			continue
		}
		if bestSnap == nil || snap.GetGeneration() >= bestSnap.GetGeneration() {
			bestSnap = snap
			bestLocal = st.GetLocal()
		}
	}
	return bestSnap, bestLocal
}

// inProgressLocalSnapshotName returns the SnapshotName of the highest-generation
// in-progress LOCAL SnapshotStorage entry on status, or "" if none exists.
func inProgressLocalSnapshotName(status *ateapipb.ActorStatus) string {
	var bestSnap *ateapipb.Snapshot
	var name string
	for _, snap := range status.GetSnapshots() {
		st := findSnapshotStorage(snap, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL)
		if st.GetStatus() != ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS || st.GetLocal().GetSnapshotName() == "" {
			continue
		}
		if bestSnap == nil || snap.GetGeneration() >= bestSnap.GetGeneration() {
			bestSnap = snap
			name = st.GetLocal().GetSnapshotName()
		}
	}
	return name
}

// latestCompletedDurableSnapshot returns the highest-generation Snapshot on
// status that holds a completed DURABLE SnapshotStorage entry, along with its
// ObjectSnapshot payload.
func latestCompletedDurableSnapshot(status *ateapipb.ActorStatus) (*ateapipb.Snapshot, *ateapipb.ObjectSnapshot) {
	var bestSnap *ateapipb.Snapshot
	var bestObj *ateapipb.ObjectSnapshot
	for _, snap := range status.GetSnapshots() {
		st := findSnapshotStorage(snap, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE)
		if st.GetStatus() != ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED || st.GetObject().GetSnapshotUri() == "" {
			continue
		}
		if bestSnap == nil || snap.GetGeneration() >= bestSnap.GetGeneration() {
			bestSnap = snap
			bestObj = st.GetObject()
		}
	}
	return bestSnap, bestObj
}

// inProgressDurableSnapshotURI returns the SnapshotUri of the highest-generation
// in-progress DURABLE SnapshotStorage entry on status, or "" if none exists.
func inProgressDurableSnapshotURI(status *ateapipb.ActorStatus) string {
	var bestSnap *ateapipb.Snapshot
	var uri string
	for _, snap := range status.GetSnapshots() {
		st := findSnapshotStorage(snap, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE)
		if st.GetStatus() != ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS || st.GetObject().GetSnapshotUri() == "" {
			continue
		}
		if bestSnap == nil || snap.GetGeneration() >= bestSnap.GetGeneration() {
			bestSnap = snap
			uri = st.GetObject().GetSnapshotUri()
		}
	}
	return uri
}

// tagDurableSnapshotURI returns the completed durable snapshot URI on tag, or
// "" if tag has no completed durable snapshot.
func tagDurableSnapshotURI(tag *ateapipb.Tag) string {
	st := findSnapshotStorage(tag.GetStatus().GetSnapshot(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE)
	if st.GetStatus() != ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED {
		return ""
	}
	return st.GetObject().GetSnapshotUri()
}

// tagAnyDurableSnapshotURI returns the durable snapshot URI on tag regardless
// of whether its storage status is IN_PROGRESS or COMPLETED.
func tagAnyDurableSnapshotURI(tag *ateapipb.Tag) string {
	st := findSnapshotStorage(tag.GetStatus().GetSnapshot(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE)
	return st.GetObject().GetSnapshotUri()
}

// clearInProgressLocalSnapshot removes any in-progress LOCAL SnapshotStorage
// entries from status.Snapshots, dropping any Snapshot left with no storage
// entries. status.LatestSnapshotGeneration is not decreased so discarded
// generation numbers are never reused.
func clearInProgressLocalSnapshot(status *ateapipb.ActorStatus) {
	if status == nil {
		return
	}
	var kept []*ateapipb.Snapshot
	for _, snap := range status.Snapshots {
		var storage []*ateapipb.SnapshotStorage
		for _, st := range snap.Storage {
			if st.GetDurability() == ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL &&
				st.GetStatus() == ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS {
				continue
			}
			storage = append(storage, st)
		}
		snap.Storage = storage
		if len(snap.Storage) > 0 {
			kept = append(kept, snap)
		}
	}
	status.Snapshots = kept
}

// removeOlderLocalSnapshots removes any LOCAL SnapshotStorage entries from
// Snapshots in status.Snapshots whose generation is not keepGen, dropping any
// Snapshot left with no storage entries.
func removeOlderLocalSnapshots(status *ateapipb.ActorStatus, keepGen int32) {
	if status == nil {
		return
	}
	kept := status.Snapshots[:0]
	for _, existing := range status.Snapshots {
		if existing.GetGeneration() != keepGen {
			removeSnapshotStorage(existing, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL)
		}
		if len(existing.GetStorage()) > 0 {
			kept = append(kept, existing)
		}
	}
	status.Snapshots = kept
}

// clearLocalSnapshots removes all LOCAL SnapshotStorage entries from
// status.Snapshots, dropping any Snapshot left with no storage entries, and
// clearing status.AssignedNode. status.LatestSnapshotGeneration is not
// decreased so discarded generation numbers are never reused.
func clearLocalSnapshots(status *ateapipb.ActorStatus) {
	if status == nil {
		return
	}
	kept := status.Snapshots[:0]
	for _, snap := range status.Snapshots {
		removeSnapshotStorage(snap, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL)
		if len(snap.GetStorage()) > 0 {
			kept = append(kept, snap)
		}
	}
	status.Snapshots = kept
	status.AssignedNode = ""
}

// deletingDurableSnapshots returns the ObjectSnapshot payloads of all
// Snapshots on status whose DURABLE storage entry is in DELETING status.
func deletingDurableSnapshots(status *ateapipb.ActorStatus) []*ateapipb.ObjectSnapshot {
	var out []*ateapipb.ObjectSnapshot
	for _, snap := range status.GetSnapshots() {
		st := findSnapshotStorage(snap, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE)
		if st.GetStatus() == ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_DELETING && st.GetObject().GetSnapshotUri() != "" {
			out = append(out, st.GetObject())
		}
	}
	return out
}

// markOlderDurableSnapshotsDeleting retains the Snapshot at keepGen and any
// actor-owned Snapshots older than keepGen that hold a DURABLE SnapshotStorage
// entry (with a URI distinct from nextSnapshotURI), marking those older DURABLE
// entries DELETING and stripping any other storage entries from them. Borrowed
// snapshots (GOLDEN, TAG) are dropped.
func markOlderDurableSnapshotsDeleting(status *ateapipb.ActorStatus, keepGen int32, nextSnapshotURI string) {
	if status == nil {
		return
	}
	kept := status.Snapshots[:0]
	for _, existing := range status.Snapshots {
		if existing.GetGeneration() == keepGen {
			kept = append(kept, existing)
			continue
		}
		if existing.GetOwner() != ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR {
			continue
		}
		st := findSnapshotStorage(existing, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE)
		uri := st.GetObject().GetSnapshotUri()
		if uri == "" || uri == nextSnapshotURI {
			continue
		}
		existing.Storage = []*ateapipb.SnapshotStorage{{
			Durability: ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE,
			Status:     ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_DELETING,
			Object:     st.GetObject(),
		}}
		kept = append(kept, existing)
	}
	status.Snapshots = kept
}

// removeDeletingDurableSnapshots removes any DURABLE SnapshotStorage entries
// in DELETING status from status.Snapshots, dropping any Snapshot left with no
// storage entries.
func removeDeletingDurableSnapshots(status *ateapipb.ActorStatus) {
	if status == nil {
		return
	}
	kept := status.Snapshots[:0]
	for _, snap := range status.Snapshots {
		storage := snap.Storage[:0]
		for _, st := range snap.Storage {
			if st.GetDurability() == ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE &&
				st.GetStatus() == ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_DELETING {
				continue
			}
			storage = append(storage, st)
		}
		snap.Storage = storage
		if len(snap.Storage) > 0 {
			kept = append(kept, snap)
		}
	}
	status.Snapshots = kept
}

// retainLatestCompletedDurableSnapshot keeps only the latest Snapshot on
// status that has a completed DURABLE SnapshotStorage entry (stripping any
// non-completed or non-durable storage from it).
// status.LatestSnapshotGeneration is not decreased so discarded generation
// numbers are never reused after a revert.
func retainLatestCompletedDurableSnapshot(status *ateapipb.ActorStatus) {
	if status == nil {
		return
	}
	snap, obj := latestCompletedDurableSnapshot(status)
	if snap == nil || obj == nil {
		status.Snapshots = nil
		return
	}
	snap.Storage = []*ateapipb.SnapshotStorage{{
		Durability: ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE,
		Status:     ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED,
		Object:     obj,
	}}
	status.Snapshots = []*ateapipb.Snapshot{snap}
}

// newDurableSnapshot constructs a Snapshot with a single DURABLE ObjectSnapshot
// storage entry.
func newDurableSnapshot(gen int32, owner ateapipb.SnapshotOwner, scope ateapipb.SnapshotContentScope, templateUID, uri string, storageStatus ateapipb.SnapshotStorageStatus) *ateapipb.Snapshot {
	return &ateapipb.Snapshot{
		Generation:       gen,
		Owner:            owner,
		ContentScope:     scope,
		ActorTemplateUid: templateUID,
		Storage: []*ateapipb.SnapshotStorage{{
			Durability: ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE,
			Status:     storageStatus,
			Object:     &ateapipb.ObjectSnapshot{SnapshotUri: uri},
		}},
	}
}

// newLocalSnapshot constructs an actor Snapshot with a single LOCAL
// LocalSnapshot storage entry.
func newLocalSnapshot(gen int32, scope ateapipb.SnapshotContentScope, templateUID, snapshotName string, storageStatus ateapipb.SnapshotStorageStatus) *ateapipb.Snapshot {
	return &ateapipb.Snapshot{
		Generation:       gen,
		Owner:            ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR,
		ContentScope:     scope,
		ActorTemplateUid: templateUID,
		Storage: []*ateapipb.SnapshotStorage{{
			Durability: ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL,
			Status:     storageStatus,
			Local:      &ateapipb.LocalSnapshot{SnapshotName: snapshotName},
		}},
	}
}
