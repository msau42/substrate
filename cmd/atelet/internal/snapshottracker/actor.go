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
	"time"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SnapshotRef is one snapshot of an actor that atelet holds or has uploaded.
type SnapshotRef struct {
	UUID       string
	Generation int32
	Fidelity   ateletpb.SnapshotFidelity
	URI        string
	// CommitTime is when the checkpoint that produced the snapshot committed
	// on this node. Zero when this atelet did not take it.
	CommitTime time.Time
	// UploadTime is when this atelet committed the snapshot's manifest to
	// object storage. Zero unless it did.
	UploadTime time.Time
}

// SnapshotRefFrom returns the SnapshotRef snap names, with no commit or upload
// time.
func SnapshotRefFrom(snap *ateletpb.Snapshot) SnapshotRef {
	return SnapshotRef{
		UUID:       snap.GetSnapshotUuid(),
		Generation: snap.GetGeneration(),
		Fidelity:   snap.GetFidelity(),
		URI:        snap.GetObject().GetSnapshotUri(),
	}
}

// report converts r for a field of ActorSnapshotState. Only latest_durable
// carries the upload time.
func (r *SnapshotRef) report(withUploadTime bool) *ateletpb.SnapshotReport {
	if r == nil {
		return nil
	}
	rep := &ateletpb.SnapshotReport{
		Snapshot: &ateletpb.Snapshot{
			SnapshotUuid: r.UUID,
			Fidelity:     r.Fidelity,
			Object:       &ateletpb.ObjectStorage{SnapshotUri: r.URI},
			Generation:   r.Generation,
		},
	}
	if !r.CommitTime.IsZero() {
		rep.LocalCommitTime = timestamppb.New(r.CommitTime)
	}
	if withUploadTime && !r.UploadTime.IsZero() {
		rep.DurableCompleteTime = timestamppb.New(r.UploadTime)
	}
	return rep
}

// actorSnapshots is what atelet tracks of one actor's snapshots on this node.
// Its fields are guarded by Tracker.mu.
type actorSnapshots struct {
	actorUID string

	// LatestLocal is the newest snapshot currently stored in the actor's
	// node-local checkpoint directory. Nil after an UPLOAD_MODE_IMMEDIATE
	// checkpoint.
	LatestLocal *SnapshotRef
	// LatestDurable is the newest snapshot confirmed in object storage.
	LatestDurable *SnapshotRef
}
