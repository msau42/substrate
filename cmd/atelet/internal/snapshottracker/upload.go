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
