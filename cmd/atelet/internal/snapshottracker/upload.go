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
	"log/slog"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

// backgroundUpload runs op, a background upload of the actor's latest local
// snapshot when it began, on a worker. If a newer checkpoint superseded the
// snapshot meanwhile, it then deletes the snapshot's local copy.
func (t *Tracker) backgroundUpload(ctx context.Context, a *actorSnapshots, op *actorOp) {
	target := op.target
	start := t.clock.Now()
	fidelity, uploaded, err := t.store.UploadLocalSnapshot(ctx, a.actorUID, target, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED)
	attrs := []any{
		slog.String("actorUID", a.actorUID),
		slog.String("snapshot", target.UUID),
		slog.Int("generation", int(target.Generation)),
		slog.Duration("elapsed", t.clock.Since(start)),
	}
	if err != nil {
		slog.WarnContext(ctx, "Background snapshot upload failed", append(attrs, slog.Any("err", err))...)
	} else {
		slog.InfoContext(ctx, "Background snapshot upload finished", append(attrs, slog.Bool("uploaded", uploaded))...)
	}

	t.mu.Lock()
	switch {
	case err == nil:
		t.recordUploadLocked(a, target, fidelity, uploaded)
	case a.uploadNeeded(target) && ctx.Err() == nil:
		// Hold the next attempt back, unless atelet shutting down cut the
		// upload short.
		t.backOffLocked(a)
	}
	t.endUploadAndUnlock(a, op, true)
}

// Promote makes requested, the snapshot a PromoteSnapshot call names, durable
// in object storage while keeping its local copy on this node. requested need
// not be atelet's latest local snapshot: as after a detach or restart, the
// upload then reads its local copy on disk if there is one, or answers from
// its manifest. It runs in the calling goroutine rather than on a worker.
func (t *Tracker) Promote(ctx context.Context, actorUID string, requested SnapshotRef) (*ateletpb.ActorSnapshotState, error) {
	t.mu.Lock()
	a, err := t.waitIdleLocked(ctx, actorUID, nil)
	if err != nil {
		t.mu.Unlock()
		return nil, err
	}

	// atelet may keep nothing for this actor, as after a restart. The upload
	// still needs an entry to record itself in, so other calls see it and
	// wait for it: add an empty one, and remove it again if the upload fails.
	created := a == nil
	a = t.findOrAddLocked(actorUID)

	// Upload the requested snapshot. The request carries only the lowest
	// fidelity the caller accepts, and no commit time: when the snapshot is
	// the latest local one, take both from LatestLocal.
	target := requested
	wasLatestLocal := a.isLatestLocal(requested.UUID)
	if wasLatestLocal {
		target.Fidelity = a.LatestLocal.Fidelity
		target.CommitTime = a.LatestLocal.CommitTime
	}
	op := t.beginOpLocked(a, opUpload, target)
	t.mu.Unlock()

	start := t.clock.Now()
	fidelity, uploaded, err := t.store.UploadLocalSnapshot(ctx, actorUID, target, requested.Fidelity)

	t.mu.Lock()
	switch {
	case err == nil:
		t.recordUploadLocked(a, target, fidelity, uploaded)
	case created:
		t.dropLocked(actorUID)
	}
	st := t.endUploadAndUnlock(a, op, wasLatestLocal)
	if err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "Promoted snapshot",
		slog.String("actorUID", actorUID),
		slog.String("snapshot", target.UUID),
		slog.Int("generation", int(target.Generation)),
		slog.Bool("uploaded", uploaded),
		slog.Duration("elapsed", t.clock.Since(start)))
	return st, nil
}

// recordUploadLocked records that an upload of target succeeded. fidelity is
// what the durable copy's manifest records, or UNSPECIFIED if it records none,
// and uploaded reports whether the upload wrote the manifest rather than
// finding it committed already.
func (t *Tracker) recordUploadLocked(a *actorSnapshots, target SnapshotRef, fidelity ateletpb.SnapshotFidelity, uploaded bool) {
	now := t.clock.Now()
	d := target
	if fidelity != ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED {
		d.Fidelity = fidelity
	}
	switch {
	case uploaded:
		d.UploadTime = now
	case a.isLatestDurable(d):
		// An earlier upload by this atelet committed the manifest: keep its
		// time.
		d.UploadTime = a.LatestDurable.UploadTime
	}
	a.recordDurable(d)
	// Record where LatestLocal is durable, so a detach does not upload it
	// again.
	if a.isLatestLocal(d.UUID) {
		a.LatestLocal.URI = d.URI
	}
	a.uploadSucceeded(now)
	t.resetBackoffLocked(a)
}

// endUploadAndUnlock ends op, an upload, and releases t.mu, which the caller
// holds. If op's snapshot was the latest local snapshot when the upload began,
// and a newer checkpoint or CommitDurable has replaced it since, its local copy
// was kept only for the upload, and is deleted. It returns the actor's state.
func (t *Tracker) endUploadAndUnlock(a *actorSnapshots, op *actorOp, wasLatestLocal bool) *ateletpb.ActorSnapshotState {
	superseded := wasLatestLocal && !a.isLatestLocal(op.target.UUID)
	t.endOpLocked(a)
	st := t.stateLocked(a)
	t.mu.Unlock()

	if superseded {
		if err := removeLocalCheckpoint(t.ctx, a.actorUID, op.target.UUID); err != nil {
			slog.WarnContext(t.ctx, "failed to delete superseded local checkpoint", slog.String("actorUID", a.actorUID), slog.Any("err", err))
		}
	}
	close(op.done)
	return st
}
