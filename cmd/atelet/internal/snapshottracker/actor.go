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

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SnapshotRef is one snapshot of an actor that atelet holds or has uploaded.
// The snapshot state file keeps it as JSON, with Fidelity as its enum number.
type SnapshotRef struct {
	UUID       string                    `json:"uuid"`
	Generation int32                     `json:"generation"`
	Fidelity   ateletpb.SnapshotFidelity `json:"fidelity,omitempty"`
	URI        string                    `json:"uri"`
	// CommitTime is when the checkpoint that produced the snapshot committed
	// on this node. Zero when this atelet did not take it.
	CommitTime time.Time `json:"commitTime,omitzero"`
	// UploadTime is when this atelet committed the snapshot's manifest to
	// object storage. Zero unless it did.
	UploadTime time.Time `json:"uploadTime,omitzero"`
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

// FidelityFromLabel parses the fidelity label a snapshot manifest records:
// UNSPECIFIED when it records none.
func FidelityFromLabel(label string) ateletpb.SnapshotFidelity {
	switch label {
	case ateattr.SnapshotFidelityVolumes:
		return ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
	case ateattr.SnapshotFidelityRootfs:
		return ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
	case ateattr.SnapshotFidelityMemory:
		return ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	default:
		return ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED
	}
}

// opKind is what an operation on an actor's snapshots does.
type opKind int

const (
	// opUpload makes a snapshot durable: a background upload, or a
	// PromoteSnapshot call.
	opUpload opKind = iota
	// opDetach makes the latest local snapshot durable, then deletes the
	// actor's local snapshots: a background detach, or a DetachActor call.
	opDetach
)

// actorOp is an upload or detach of an actor's snapshot in progress.
type actorOp struct {
	kind opKind
	// target is the snapshot it uploads or detaches.
	target SnapshotRef
	// done is closed once the operation has ended, and has deleted any local
	// snapshot it superseded.
	done chan struct{}
}

// actorSnapshotsRecord is the part of actorSnapshots that the actor's snapshot
// state file keeps, so it outlives an atelet restart. An upload or detach in
// flight is not part of it: one changes the file only once it ends, and a
// restart ends it.
type actorSnapshotsRecord struct {
	// LatestLocal is the newest snapshot currently stored in the actor's
	// node-local checkpoint directory, which a restarted atelet goes on to
	// upload and detach. Nil once detached or after an UPLOAD_MODE_IMMEDIATE
	// checkpoint.
	LatestLocal *SnapshotRef `json:"latestLocal,omitempty"`
	// LatestDurable is the newest snapshot confirmed in object storage, so a
	// restarted atelet still reports it and does not upload it again.
	LatestDurable *SnapshotRef `json:"latestDurable,omitempty"`

	// UndurableSince is what uploadDelay is measured from: the CommitTime of
	// the oldest local checkpoint taken since the last durable snapshot, or,
	// for one taken while an upload ran, when that upload succeeded. Zero
	// when LatestLocal is nil or already durable. Saving it keeps a restart
	// from postponing the next upload.
	//
	// Measuring from UndurableSince rather than LatestLocal.CommitTime
	// ensures an actor that pauses and resumes repeatedly still uploads
	// within uploadDelay instead of postponing durability indefinitely. An
	// upload changes UndurableSince only once it succeeds, and a failed or
	// interrupted one leaves it as it was.
	UndurableSince time.Time `json:"undurableSince,omitzero"`
	// DetachAt is when atelet next checks whether to detach the actor: it
	// detaches it then unless a Run, Restore, or Checkpoint of the actor is
	// in progress or it runs on this node. Zero when atelet holds no local
	// snapshot of the actor. Saving it keeps a restart from postponing the
	// detach.
	DetachAt time.Time `json:"detachAt,omitzero"`
}

// actorSnapshots is what atelet tracks of one actor's snapshots on this node.
// Its fields are guarded by Tracker.mu.
type actorSnapshots struct {
	actorUID string

	// actorSnapshotsRecord holds what the snapshot state file keeps. The
	// fields after it live only in memory.
	actorSnapshotsRecord

	// retryAt holds the actor's next background upload or detach back after
	// one failed. It is not saved: a restarted atelet tries an upload again
	// as soon as it is due.
	retryAt time.Time

	// holds counts the Runs, Restores, and Checkpoints of the actor in
	// progress on this node. While it is nonzero, atelet does not detach
	// the actor. It is not saved: a restart ends them.
	holds int

	// op is the actor's upload or detach in progress, background or requested
	// by an RPC, or nil: an actor has at most one at a time, and nothing
	// cancels it. Run, Restore, and Checkpoint calls wait for a detach to end
	// via waitForDetachLocked so local files are not pruned underneath them.
	// PromoteSnapshot, DetachActor, and Forget wait for any operation to end
	// via waitIdleLocked.
	op *actorOp
}

// uploadNeeded reports whether ref still needs uploading: neither it nor a
// newer snapshot is durable.
func (a *actorSnapshots) uploadNeeded(ref SnapshotRef) bool {
	return a.LatestDurable == nil || a.LatestDurable.Generation < ref.Generation
}

// isLatestDurable reports whether ref is LatestDurable, uploaded where ref
// names.
func (a *actorSnapshots) isLatestDurable(ref SnapshotRef) bool {
	return a.LatestDurable != nil && a.LatestDurable.UUID == ref.UUID && a.LatestDurable.URI == ref.URI
}

// isLatestLocal reports whether uuid names the actor's latest local snapshot.
func (a *actorSnapshots) isLatestLocal(uuid string) bool {
	return a.LatestLocal != nil && a.LatestLocal.UUID == uuid
}

// recordDurable records that d is durable, unless a newer snapshot already is.
func (a *actorSnapshots) recordDurable(d SnapshotRef) {
	if a.LatestDurable == nil || d.Generation >= a.LatestDurable.Generation {
		a.LatestDurable = &d
	}
}

// uploading returns the snapshot an upload in progress is making durable, or
// nil.
func (a *actorSnapshots) uploading() *SnapshotRef {
	if a.op == nil || a.op.kind != opUpload {
		return nil
	}
	return &a.op.target
}

// uploadDue returns when atelet uploads the actor's latest local snapshot:
// uploadDelay after UndurableSince, or at retryAt if that is later. An older
// snapshot's upload in progress can delay it. ok is false when there is
// nothing to upload: no local snapshot, one already durable, or one being
// uploaded.
func (a *actorSnapshots) uploadDue(uploadDelay time.Duration) (due time.Time, ok bool) {
	local := a.LatestLocal
	if local == nil || !a.uploadNeeded(*local) {
		return time.Time{}, false
	}
	if up := a.uploading(); up != nil && up.UUID == local.UUID {
		return time.Time{}, false
	}
	since := a.UndurableSince
	if since.IsZero() {
		since = local.CommitTime
	}
	return later(since.Add(uploadDelay), a.retryAt), true
}

// uploadSucceeded records that an upload succeeded at now. UndurableSince
// becomes zero, or now if a checkpoint committed while the upload ran is not
// durable yet.
func (a *actorSnapshots) uploadSucceeded(now time.Time) {
	a.UndurableSince = time.Time{}
	if a.LatestLocal != nil && a.uploadNeeded(*a.LatestLocal) {
		a.UndurableSince = now
	}
}

// detachDue returns when atelet next checks whether to detach the actor: at
// DetachAt, or at retryAt if that is later. ok is false when there is nothing
// to detach: no detach is scheduled, or atelet holds no local snapshot of the
// actor.
func (a *actorSnapshots) detachDue() (due time.Time, ok bool) {
	if a.DetachAt.IsZero() || a.LatestLocal == nil {
		return time.Time{}, false
	}
	return later(a.DetachAt, a.retryAt), true
}

func later(t, u time.Time) time.Time {
	if u.After(t) {
		return u
	}
	return t
}
