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
	"slices"
	"time"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"google.golang.org/protobuf/proto"
)

// CommitLocal records snap, just stored in the actor's local checkpoint
// directory by a Checkpoint in either upload mode, as the actor's latest local
// snapshot, schedules the actor's detach and, if snap needs an upload (see
// uploadDue), its upload, and returns the actor's state. A commit of a
// snapshot already recorded or superseded by a newer committed snapshot leaves
// the state as it is. An older snapshot's upload in progress runs on, but an
// Uploading that keepUploading no longer keeps is dropped. The Checkpoint then
// prunes the local snapshots LocalSnapshotsToKeep no longer names. It fails
// with Aborted once Forget has dropped the actor during the Checkpoint (see
// checkNotForgottenLocked).
func (t *Tracker) CommitLocal(actorUID string, snap SnapshotRef) (*ateletpb.ActorSnapshotState, error) {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	if err := t.checkNotForgottenLocked(actorUID, snap.uuid()); err != nil {
		return nil, err
	}
	a := t.findOrAddLocked(actorUID)
	if !a.localNeeded(snap) {
		if a.reportDue() {
			t.clearReportDueLocked(a)
		} else {
			t.requeueLocked(a)
		}
		return t.stateLocked(a), nil
	}
	t.setLatestLocalLocked(a, snap)
	if a.UndurableSince.IsZero() && a.uploadNeeded(snap) {
		a.UndurableSince = snap.CommitTime
	}
	// Drop a failed upload that snap replaces in this save rather than leave
	// it for a worker: keepUploading tells it apart by uploadingFailed, which
	// the state file does not keep, so an atelet restart would resume it from
	// a local copy the Checkpoint's prune deletes.
	a.clearSupersededUploading()
	// Start the backoff over unless Uploading is still kept and snap is not
	// the next upload target: Uploading's retry then still waits it out.
	if !a.keepUploading() || sameUUID(a.nextUploadTarget(), &snap) {
		t.resetBackoffLocked(a)
	}
	t.cancelReportLocked(a)
	t.saveLocked(a)
	t.requeueLocked(a)
	return t.stateLocked(a), nil
}

// CommitDurable records snap, just uploaded by a Checkpoint under
// UPLOAD_MODE_IMMEDIATE, as the actor's latest durable snapshot, and returns
// the actor's state. A commit of a snapshot already durable or older than
// LatestDurable leaves the state as it is.
//
// It drops an older LatestLocal, which snap supersedes, so the Checkpoint's
// prune deletes it even if no local copy of snap is kept. The Checkpoint then
// records its local copy with CommitLocal, if it manages to keep one.
//
// Whether or not it records snap, it returns only once no upload of a
// snapshot older than LatestDurable is in progress (see durableStateLocked).
// It fails with Aborted once Forget has dropped the actor during the
// Checkpoint.
func (t *Tracker) CommitDurable(actorUID string, snap SnapshotRef) (*ateletpb.ActorSnapshotState, error) {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	if err := t.checkNotForgottenLocked(actorUID, snap.uuid()); err != nil {
		return nil, err
	}
	a := t.findOrAddLocked(actorUID)
	if !a.uploadNeeded(snap) {
		if a.reportDue() {
			t.clearReportDueLocked(a)
		} else {
			t.requeueLocked(a)
		}
		return t.durableStateLocked(a, snap.uuid())
	}
	a.LatestDurable = snap.clone()
	if a.LatestLocal != nil && a.LatestLocal.generation() < snap.generation() {
		a.clearLatestLocal()
	}
	t.cancelReportLocked(a)
	t.durableCommittedLocked(a)
	return t.durableStateLocked(a, snap.uuid())
}

// CommittedCheckpointState returns the actor's state for a Checkpoint whose
// snapshot atelet committed already, and cancels any pending background
// report: a Checkpoint returns the state to the control plane, including a
// detach, in either upload mode. It fails with Aborted once Forget has dropped
// the actor during the Checkpoint.
func (t *Tracker) CommittedCheckpointState(actorUID string, snap *ateletpb.Snapshot) (*ateletpb.ActorSnapshotState, error) {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	if err := t.checkNotForgottenLocked(actorUID, snap.GetSnapshotUuid()); err != nil {
		return nil, err
	}
	a := t.findLocked(actorUID)
	if a == nil {
		return &ateletpb.ActorSnapshotState{}, nil
	}
	if a.reportDue() {
		t.clearReportDueLocked(a)
	}
	return t.stateLocked(a), nil
}

// CheckNotForgotten fails with Aborted when Forget dropped the actor while a
// Checkpoint or PromoteSnapshot of snap was in progress.
func (t *Tracker) CheckNotForgotten(actorUID string, snap *ateletpb.Snapshot) error {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	return t.checkNotForgottenLocked(actorUID, snap.GetSnapshotUuid())
}

// LocalSnapshotsToKeep returns the actor's local snapshots atelet still needs:
// LatestLocal, and Uploading while a background upload is reading it or will
// retry it. A Checkpoint prunes every other local snapshot once it has
// committed its own. Until the next commit, a background upload only reads one
// of these, so the prune never deletes a snapshot an upload needs.
func (t *Tracker) LocalSnapshotsToKeep(actorUID string) []string {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a := t.findLocked(actorUID)
	if a == nil {
		return nil
	}
	var keep []string
	if a.LatestLocal != nil {
		keep = append(keep, a.LatestLocal.uuid())
	}
	if a.keepUploading() && !sameUUID(a.Uploading, a.LatestLocal) {
		keep = append(keep, a.Uploading.uuid())
	}
	return keep
}

// CommitPromoted records snap, a local snapshot PromoteSnapshot made durable or
// found durable already, as the actor's latest durable snapshot, and returns
// the actor's state. A promote of a snapshot already durable, or older than
// LatestDurable, keeps what was recorded.
//
// It keeps snap's local copy, the time its checkpoint committed, and any
// UploadTime on snap. It leaves DetachAt as it is.
//
// Whether or not it records snap, it returns only once no upload of a
// snapshot older than LatestDurable is in progress (see durableStateLocked).
// It fails with Aborted once Forget has dropped the actor during the
// PromoteSnapshot.
func (t *Tracker) CommitPromoted(actorUID string, snap SnapshotRef) (*ateletpb.ActorSnapshotState, error) {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	if err := t.checkNotForgottenLocked(actorUID, snap.uuid()); err != nil {
		return nil, err
	}
	a := t.findOrAddLocked(actorUID)
	// Skip when snap is already LatestDurable (so a retried promote keeps the
	// original CommitTime and UploadTime) or older than LatestDurable (a
	// newer checkpoint became durable while PromoteSnapshot ran).
	if !a.uploadNeeded(snap) {
		if a.LatestLocal != nil && a.reportDue() {
			t.clearReportDueLocked(a)
		} else {
			t.requeueLocked(a)
		}
		return t.durableStateLocked(a, snap.uuid())
	}
	// Carry over the checkpoint commit time when promoting the tracked
	// local snapshot. snap.UploadTime is already set by the caller when
	// it uploaded the snapshot.
	if sameUUID(a.LatestLocal, &snap) {
		snap.CommitTime = a.LatestLocal.CommitTime
	}
	a.LatestDurable = snap.clone()
	// A promote with a local snapshot still on the node returns the latest
	// state to the control plane. Once LatestLocal is nil, a pending detach
	// report stays due, since the control plane does not read a promote's
	// response as a detach.
	if a.LatestLocal != nil {
		t.cancelReportLocked(a)
	}
	t.durableCommittedLocked(a)
	return t.durableStateLocked(a, snap.uuid())
}

// BeginStart records that a Run or Restore is starting the actor on this node,
// once any detach or Checkpoint of the actor in progress has finished,
// including a Checkpoint that Forget dropped the actor under, and returns the
// func to call when the start ends, whether or not it succeeded. A Checkpoint
// resets the actor's directories and unmounts its volumes as it ends, which
// must not happen under the new activation.
//
// It first unschedules the actor's detach (see markStarted) and saves that, so
// atelet does not detach the actor while a workload restored from its local
// snapshot may use it, even if atelet restarts or the start fails partway. A
// failed Run or Restore leaves the detach unscheduled, since atelet cannot
// tell whether the workload is running on the worker; the local snapshot stays
// until the next checkpoint or Terminate on this node. When it cannot save
// that, it fails with Unavailable, and the caller must not start the actor.
func (t *Tracker) BeginStart(ctx context.Context, actorUID string) (end func(), err error) {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	var a *actorSnapshots
	for {
		a, err = t.waitForDetachAndCheckpointLocked(ctx, actorUID, nil, (*actorSnapshots).anyCheckpointInFlight)
		if err != nil {
			return nil, err
		}
		f := t.forgottenCheckpointInFlightLocked(actorUID)
		if f == nil {
			break
		}
		if a == nil {
			a = &actorSnapshots{actorUID: actorUID}
		}
		if err := t.waitOnEntryLocked(ctx, a, f.done, true); err != nil {
			return nil, err
		}
	}
	if a != nil {
		// markStarted clears DetachAt, and reports whether it was set:
		// only then is there a change to save. If the save fails, put
		// DetachAt and unsaved back as they were, matching the state file,
		// so the detach stays scheduled.
		if detachAt, unsaved := a.DetachAt, a.unsaved; a.markStarted() {
			if err := t.writeStateLocked(a); err != nil {
				a.DetachAt, a.unsaved = detachAt, unsaved
				return nil, apierror.Unavailable("while unscheduling the detach of actor %s before starting it: %v", actorUID, err)
			}
		}
		// No detach is scheduled now, so failed detaches have nothing left
		// to retry. Keep the backoff when an upload is still due.
		if _, ok := t.uploadDueLocked(a); !ok {
			t.resetBackoffLocked(a)
		}
	}
	return t.holdLocked(actorUID, a, nil, false), nil
}

// BeginCheckpoint records that a Checkpoint of snap is in progress on this
// node, and returns the func to call when the checkpoint ends, whether or not
// it succeeded. atelet does not detach the actor in between, so a detach
// cannot delete the local snapshot the checkpoint writes.
//
// It first waits, as long as ctx allows, for any detach in progress and any
// other Checkpoint of snap in progress: a retry waits for the attempt it
// retries. It fails with Aborted when Forget dropped the actor during an
// earlier attempt at snap still in progress, or drops the actor while this
// call waits.
//
// It reports whether snap is committed already, as the actor's latest local
// or durable snapshot. The Checkpoint is then a retry of one that committed
// snap and deleted the workload.
//
// It fails with FailedPrecondition when:
//   - snap has a stale generation for the actor: below the actor's newest
//     snapshot on this node, or equal to it under another snapshot_uuid.
//   - snap is committed under its snapshot_uuid but named differently.
func (t *Tracker) BeginCheckpoint(ctx context.Context, actorUID string, snap *ateletpb.Snapshot) (committed bool, end func(), err error) {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	if err := t.checkNotForgottenLocked(actorUID, snap.GetSnapshotUuid()); err != nil {
		return false, nil, err
	}
	// A retry that arrives while the Checkpoint it retries still runs waits
	// for it, rather than checkpoint the workload a second time. Waiting
	// answers the retry once that Checkpoint ends; failing it with a retryable
	// code would only have the control plane's caller retry the suspend.
	a, err := t.waitForDetachAndCheckpointLocked(ctx, actorUID,
		func(a *actorSnapshots) error { return a.checkGeneration(snap) },
		func(a *actorSnapshots) *inFlightSnapshot { return a.checkpointInFlight(snap.GetSnapshotUuid()) })
	if err != nil {
		return false, nil, err
	}
	if a != nil {
		if a.forgetting {
			return false, nil, apierror.Aborted("actor %s was forgotten", actorUID)
		}
		if committed, err = a.checkpointCommitted(snap); err != nil {
			return false, nil, err
		}
	}
	// Unlike BeginStart, leave DetachAt as it is. Either it is already
	// zero, because the actor's Run or Restore on this node unscheduled the
	// detach, or snap is committed already, and its detach stays scheduled.
	return committed, t.holdLocked(actorUID, a, &SnapshotRef{Snapshot: proto.CloneOf(snap)}, false), nil
}

// waitForDetachAndCheckpointLocked waits, as long as ctx allows, until no
// detach of the actor is in progress and inFlight returns no Checkpoint of it
// to wait for, and returns the actor's entry, which may be nil. It releases
// the actor's lock while it waits, and holds it again when it returns. check,
// if not nil, vets the entry on every pass, as waitForDetachLocked does. It
// fails with Aborted when Forget drops the actor during the wait.
func (t *Tracker) waitForDetachAndCheckpointLocked(ctx context.Context, actorUID string, check func(*actorSnapshots) error, inFlight func(*actorSnapshots) *inFlightSnapshot) (*actorSnapshots, error) {
	for {
		a, err := t.waitForDetachLocked(ctx, actorUID, check)
		if err != nil {
			return nil, err
		}
		var f *inFlightSnapshot
		if a != nil {
			f = inFlight(a)
		}
		if f == nil {
			return a, nil
		}
		if err := t.waitOnEntryLocked(ctx, a, f.done, true); err != nil {
			return nil, err
		}
	}
}

// NotStartedSince reports whether snap is the actor's latest local snapshot
// and no Run or Restore of the actor has begun on this node since snap was
// committed: its detach is still scheduled (see markStarted). Until then, the
// actor's directories and volumes are still the Checkpoint's to clean up.
//
// Once LatestLocal is nil (as after a detach, or when an immediate Checkpoint
// could not keep a local copy), DetachAt is zero, so BeginStart leaves no
// record of a later Run or Restore and NotStartedSince returns false. A
// later Run, Restore, or Terminate resets the actor's directories anyway.
//
// No Run or Restore begins while a Checkpoint is in progress (see
// BeginStart), so the answer a Checkpoint gets holds until it ends.
func (t *Tracker) NotStartedSince(actorUID string, snap *ateletpb.Snapshot) bool {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a := t.findLocked(actorUID)
	return a != nil && a.detachScheduled() && a.LatestLocal.uuid() == snap.GetSnapshotUuid()
}

// BeginPromote records that a PromoteSnapshot of snap is in progress on this
// node, once any detach in progress and any background upload of snap have
// finished. It fails with FailedPrecondition when snap has a stale generation
// for the actor or conflicts with LatestDurable, and with Aborted when Forget
// dropped the actor during an earlier call for snap still in progress or drops
// it while this call waits. It reports whether snap is already the actor's
// latest durable snapshot, and returns the func to call when the promote ends.
// atelet does not start a detach or a background upload of the actor in
// between.
func (t *Tracker) BeginPromote(ctx context.Context, actorUID string, snap SnapshotRef) (alreadyDurable bool, end func(), err error) {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	if err := t.checkNotForgottenLocked(actorUID, snap.uuid()); err != nil {
		return false, nil, err
	}
	a, err := t.waitForDetachOrSnapshotUploadLocked(ctx, actorUID, snap)
	if err != nil {
		return false, nil, err
	}
	if a != nil {
		if a.forgetting {
			return false, nil, apierror.Aborted("actor %s was forgotten", actorUID)
		}
		alreadyDurable, err = a.isLatestDurable(snap)
		if err != nil {
			return false, nil, err
		}
	}
	return alreadyDurable, t.holdLocked(actorUID, a, snap.clone(), true), nil
}

// holdLocked counts a Run, Restore, Checkpoint, or PromoteSnapshot of the
// actor in progress on its entry a, which holds detaches and state reports of
// the actor off (and background uploads when promote is true), and returns the
// func that ends it and queues the actor again. An actor atelet tracks nothing
// of yet (a nil a) is held too, so no detach or report can begin on a snapshot
// its first Checkpoint commits before it ends.
//
// When snap is non-nil (Checkpoint and PromoteSnapshot), it stays in
// a.inFlight until end runs, so newest includes it. Until a Checkpoint's end
// runs, a retried Checkpoint of snap and any Run or Restore of the actor wait
// for it.
func (t *Tracker) holdLocked(actorUID string, a *actorSnapshots, snap *SnapshotRef, promote bool) (end func()) {
	if a == nil {
		a = &actorSnapshots{actorUID: actorUID}
	}
	t.addLocked(a)
	a.holds++
	var flight *inFlightSnapshot
	if snap != nil {
		flight = &inFlightSnapshot{ref: snap, checkpoint: !promote, done: make(chan struct{})}
		a.inFlight = append(a.inFlight, flight)
	}
	if promote {
		a.promotes++
	}
	return func() {
		t.lock(actorUID)
		defer t.unlock(actorUID)
		a.holds--
		if flight != nil {
			a.inFlight = slices.DeleteFunc(a.inFlight, func(f *inFlightSnapshot) bool { return f == flight })
			close(flight.done)
		}
		if promote {
			a.promotes--
		}
		if flight != nil && a.forgetting {
			// Forget (via Terminate) began dropping the actor while the
			// Checkpoint or PromoteSnapshot was in progress.
			t.releaseForgottenLocked(actorUID, flight)
		}
		if !t.tracksLocked(a) {
			return
		}
		if a.holds == 0 && a.reportDeferred {
			t.queueReportLocked(a)
		}
		t.requeueLocked(a)
	}
}

// Forget drops the actor's state, in memory and on disk. It first cancels the
// actor's operation in progress, an upload or a detach, and waits for it to
// end, releasing the actor's lock meanwhile; the operation leaves the actor
// alone when it ends. The snapshot state file goes before the wait, so an
// atelet restart meanwhile does not resume the operation. It does not wait for
// a Checkpoint or PromoteSnapshot of the actor in progress, but that call can
// no longer commit its snapshot, so it cannot track the actor again.
func (t *Tracker) Forget(actorUID string) error {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	t.forgetInFlightLocked(actorUID)
	// Abandon too any operation that begins before the lock is taken back.
	for op := t.abandonOpLocked(actorUID); op != nil; op = t.abandonOpLocked(actorUID) {
		// Best-effort: the removal below reports a failure.
		_ = removeSnapshotState(actorUID)
		t.unlock(actorUID)
		<-op.done
		t.lock(actorUID)
		t.forgetInFlightLocked(actorUID)
	}
	// Start the actor's backoff over; its entry, and any hold, goes below.
	t.retries.Forget(actorUID)
	t.reporting.retries.Forget(actorUID)
	t.dropLocked(actorUID)
	t.undurableActorsTally.forget(actorUID)
	// This also deletes a file that a commit rewrote during the wait.
	return removeSnapshotState(actorUID)
}

// forgetInFlightLocked marks the actor's entry forgetting for any call waiting
// on it, and keeps its Checkpoints and PromoteSnapshots in progress in
// t.forgotten until each ends.
func (t *Tracker) forgetInFlightLocked(actorUID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// An entry with a call or waiter in progress is always in memory.
	a := t.actors[actorUID]
	if a == nil {
		return
	}
	a.forgetting = true
	for _, f := range a.inFlight {
		if !slices.Contains(t.forgotten[actorUID], f) {
			if t.forgotten == nil {
				t.forgotten = map[string][]*inFlightSnapshot{}
			}
			t.forgotten[actorUID] = append(t.forgotten[actorUID], f)
		}
	}
}

// releaseForgottenLocked drops flight from t.forgotten as its call ends.
func (t *Tracker) releaseForgottenLocked(actorUID string, flight *inFlightSnapshot) {
	t.mu.Lock()
	defer t.mu.Unlock()
	kept := slices.DeleteFunc(t.forgotten[actorUID], func(f *inFlightSnapshot) bool { return f == flight })
	if len(kept) == 0 {
		delete(t.forgotten, actorUID)
	} else {
		t.forgotten[actorUID] = kept
	}
}

// forgottenCheckpointInFlightLocked returns a Checkpoint in t.forgotten that
// was in progress when Forget dropped the actor and has not ended yet, or nil.
func (t *Tracker) forgottenCheckpointInFlightLocked(actorUID string) *inFlightSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, f := range t.forgotten[actorUID] {
		if f.checkpoint {
			return f
		}
	}
	return nil
}

// checkNotForgottenLocked fails with Aborted when Forget dropped the actor
// while a Checkpoint or PromoteSnapshot of the snapshot named uuid was in
// progress. That call's commit would track the forgotten actor again: its
// state file would come back, and atelet would upload or detach a snapshot the
// control plane discarded.
func (t *Tracker) checkNotForgottenLocked(actorUID, uuid string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, f := range t.forgotten[actorUID] {
		if f.ref.uuid() == uuid {
			return apierror.Aborted("actor %s was forgotten while snapshot %s was being taken", actorUID, uuid)
		}
	}
	return nil
}

// setLatestLocalLocked records snap as the actor's latest local snapshot and
// schedules the actor's detach a detach delay from now.
func (t *Tracker) setLatestLocalLocked(a *actorSnapshots, snap SnapshotRef) {
	a.setLatestLocal(snap, t.clock.Now().Add(t.detachDelay))
}

// clearLatestLocalLocked records that atelet holds no local snapshot of the
// actor, as after Detach pruned its local checkpoints, zeroes UndurableSince,
// and lifts the backoff of its failed background work, which has nothing left
// to retry.
func (t *Tracker) clearLatestLocalLocked(a *actorSnapshots) {
	a.Uploading = nil
	a.uploadingFailed = false
	a.UndurableSince = time.Time{}
	a.clearLatestLocal()
	t.resetBackoffLocked(a)
}

// durableCommittedLocked finishes CommitDurable or CommitPromoted once
// LatestDurable is updated. When LatestLocal is now durable too (or nil), it
// clears UndurableSince and lifts any retry backoff; when a newer LatestLocal
// still needs uploading, its UndurableSince and backoff stay in place.
func (t *Tracker) durableCommittedLocked(a *actorSnapshots) {
	if a.LatestLocal == nil || !a.uploadNeeded(*a.LatestLocal) {
		a.UndurableSince = time.Time{}
		t.resetBackoffLocked(a)
	}
	t.saveLocked(a)
	t.requeueLocked(a)
}

// durableStateLocked returns the actor's state for CommitDurable or
// CommitPromoted to return. Once the control plane learns of LatestDurable,
// it may delete the objects of older snapshots, so this first cancels any
// upload of an older snapshot in progress and waits for it to end, releasing
// the actor's lock meanwhile. The object-store plugin may still finish an
// object that upload had under way; deleting the actor collects it. It fails
// with Aborted when Forget drops the actor during the wait.
func (t *Tracker) durableStateLocked(a *actorSnapshots, uuid string) (*ateletpb.ActorSnapshotState, error) {
	actorUID := a.actorUID
	for op := a.supersededUpload(); op != nil; op = a.supersededUpload() {
		op.cancel()
		t.unlock(actorUID)
		<-op.done
		t.lock(actorUID)
		if err := t.checkNotForgottenLocked(actorUID, uuid); err != nil {
			return nil, err
		}
		if a = t.findLocked(actorUID); a == nil {
			return &ateletpb.ActorSnapshotState{}, nil
		}
	}
	return t.stateLocked(a), nil
}

// succeededLocked finishes a change that leaves the actor in a successful end
// state, such as a background upload or detach: it starts the actor's backoff
// over, saves the actor's state, and queues the actor for its next background
// work.
func (t *Tracker) succeededLocked(a *actorSnapshots) {
	t.resetBackoffLocked(a)
	t.saveLocked(a)
	t.requeueLocked(a)
}
