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
	"errors"
	"fmt"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TagActorSnapshot tags the external snapshot held by the suspended actor the
// tag's source_actor names.
// The tag is given its own copy of that snapshot, so suspending the actor again
// or deleting the actor does not garbage collect the tag's snapshot.
//
// The tag is built in 3 phases:
//  1. Reserve the tag in TAG_STATE_CREATING and record its destination snapshot.
//  2. Copy the snapshot under the reserved tag's UID.
//  3. Finalize: transition status.state to TAG_STATE_READY.
//
// The tag captures whichever snapshot the actor holds when the workflow runs.
// An actor keeps no snapshot history, so a suspend that lands first moves what
// gets tagged; that race is inherent to naming an actor rather than a snapshot.
//
// Not idempotent: the name is taken as soon as phase 1 lands, so a create that
// dies after it leaves a pending tag and every later create under that name is
// AlreadyExists. To retry, delete the tag, which collects whatever the failed
// attempt stranded, and create it again.
func (w *ActorWorkflow) TagActorSnapshot(ctx context.Context, tag *ateapipb.Tag) (*ateapipb.Tag, error) {
	actorRef := resources.ActorRefFromObjectRef(tag.GetSourceActor())

	// Serializes against a suspend of the same actor, which would otherwise
	// collect the snapshot out from under the copy.
	leaseCtx, lease, err := w.acquireActorLease(ctx, actorRef)
	if err != nil {
		return nil, err
	}
	defer lease.Close()

	// Serializes against a delete of the tag this creates, which would
	// otherwise collect the copy while it is being written.
	tagRef := resources.TagRef{Atespace: actorRef.Atespace, Name: tag.GetMetadata().GetName()}
	leaseCtx, tagLease, err := acquireTagLease(leaseCtx, w.store, tagRef)
	if err != nil {
		return nil, err
	}
	defer tagLease.Close()

	actor, actorTemplate, err := w.loadActorForTag(leaseCtx, actorRef)
	if err != nil {
		return nil, err
	}
	srcSnapshot := actor.GetStatus().GetDurableSnapshotStatus()

	reserved, err := w.ensureTagReserved(leaseCtx, tagRef, actor, actorTemplate, tag)
	if err != nil {
		return nil, err
	}
	if err := w.ensureTagSnapshotCopied(leaseCtx, reserved, srcSnapshot); err != nil {
		return nil, err
	}
	return w.ensureTagFinalized(leaseCtx, reserved)
}

// DeleteTag releases the external snapshot the tag owns and then removes the
// row, in that order: the row is the only handle on that snapshot, so dropping
// it first would leak.
//
// The workflow is built in 4 phases:
//  1. Load the tag (which names the snapshot to collect).
//  2. Transition the tag to TAG_STATE_DELETING so no new clones can use it.
//  3. Release that snapshot, tolerating a previous attempt partly collected.
//  4. Finalize: drop the row.
//
// Idempotent: a failure at any phase leaves the row in place, so the same
// delete run again rediscovers the work from it and resumes over whatever is
// left.
//
// Note that this destroys the external snapshot: an Actor created from the tag
// and never suspended is still borrowing it and becomes unrecoverable. Do not
// delete a tag while clones of it exist.
func (w *ActorWorkflow) DeleteTag(ctx context.Context, tagRef resources.TagRef, precondition store.DeletePreconditions) (*ateapipb.Tag, error) {
	// Serializes against a create of the same tag, whose copy would otherwise
	// keep writing into the prefix this is collecting.
	ctx, lease, err := acquireTagLease(ctx, w.store, tagRef)
	if err != nil {
		return nil, err
	}
	defer lease.Close()

	tag, err := w.loadTagForDelete(ctx, tagRef)
	if err != nil {
		return nil, err
	}
	// Checked before the snapshot is collected: a stale caller must not
	// reach that step.
	if err := precondition.Check(tag.GetMetadata()); err != nil {
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, status.Errorf(codes.Aborted, "Tag %s does not have uid %s", tagRef, precondition.UID)
		}
		return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
	}
	if tag, err = w.ensureTagMarkedDeleting(ctx, tagRef, tag); err != nil {
		return nil, err
	}
	if err := w.ensureTagSnapshotReleased(ctx, tag); err != nil {
		return nil, err
	}
	return w.finalizeTagDeleted(ctx, tagRef, store.DeletePreconditions{UID: tag.GetMetadata().GetUid(), Version: tag.GetMetadata().GetVersion()})
}

// loadTagForDelete fetches the row the delete works from. The row records where
// the snapshot lives, so the work is rediscovered from it rather than rebuilt
// from the source actor, which may be long gone.
func (w *ActorWorkflow) loadTagForDelete(ctx context.Context, tagRef resources.TagRef) (_ *ateapipb.Tag, err error) {
	ctx, done := stepSpan(ctx, "LoadTagForDelete")
	defer func() { err = done(err) }()

	tag, err := w.store.GetTag(ctx, tagRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Tag %s not found", tagRef)
		}
		return nil, fmt.Errorf("while getting tag %s: %w", tagRef, err)
	}
	return tag, nil
}

// ensureTagMarkedDeleting transitions the tag to TAG_STATE_DELETING and
// persists the change, returning the stored copy. Skips when a previous
// attempt already marked the tag.
func (w *ActorWorkflow) ensureTagMarkedDeleting(ctx context.Context, tagRef resources.TagRef, tag *ateapipb.Tag) (_ *ateapipb.Tag, err error) {
	ctx, done := stepSpan(ctx, "MarkTagDeleting")
	defer func() { err = done(err) }()

	if tag.GetStatus().GetState() == ateapipb.TagState_TAG_STATE_DELETING {
		markSkipped(ctx, "tag already DELETING")
		return tag, nil
	}
	stored, err := w.store.UpdateTag(ctx, tagRef, store.PreconditionFrom(tag), func(toUpdate *ateapipb.Tag) error {
		toUpdate.Status.State = ateapipb.TagState_TAG_STATE_DELETING
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Tag %s not found", tagRef)
		}
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, status.Errorf(codes.Aborted, "Tag %s does not have uid %s", tagRef, tag.GetMetadata().GetUid())
		}
		return nil, fmt.Errorf("while setting tag %s state to DELETING: %w", tagRef, err)
	}
	return stored, nil
}

// ensureTagSnapshotReleased deletes the objects the tag's external snapshot is
// made of. It tolerates a partly-collected snapshot, so a retry finishes
// cleanly. It collects the in-progress snapshot of a pending tag too.
func (w *ActorWorkflow) ensureTagSnapshotReleased(ctx context.Context, tag *ateapipb.Tag) (err error) {
	ctx, done := stepSpan(ctx, "ReleaseTagSnapshot")
	defer func() { err = done(err) }()

	return w.getSnapshotPlugin().DeleteTagSnapshot(ctx, tag)
}

// finalizeTagDeleted drops the row, once nothing it names is left behind.
func (w *ActorWorkflow) finalizeTagDeleted(ctx context.Context, tagRef resources.TagRef, precondition store.DeletePreconditions) (_ *ateapipb.Tag, err error) {
	ctx, done := stepSpan(ctx, "FinalizeTagDeleted")
	defer func() { err = done(err) }()

	tag, err := w.store.DeleteTag(ctx, tagRef, precondition)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Tag %s not found", tagRef)
		}
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, status.Errorf(codes.Aborted, "Tag %s does not have uid %s", tagRef, precondition.UID)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while deleting tag %s: %w", tagRef, err)
	}
	return tag, nil
}

// loadActorForTag fetches the actor to tag and its template, and checks that
// the actor holds an external snapshot a tag can be made from.
func (w *ActorWorkflow) loadActorForTag(ctx context.Context, actorRef resources.ActorRef) (_ *ateapipb.Actor, _ *ateapipb.ActorTemplate, err error) {
	ctx, done := stepSpan(ctx, "LoadActorForTag")
	defer func() { err = done(err) }()

	actor, err := w.store.GetActor(ctx, actorRef)
	if err != nil {
		return nil, nil, err
	}
	// Only a suspended actor's snapshot is complete. A running or
	// suspending actor's is either stale or still being written.
	if got := actor.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		return nil, nil, status.Errorf(codes.FailedPrecondition, "Actor %s must be %s to be tagged (got: %v)", actorRef, ateapipb.ActorState_ACTOR_STATE_SUSPENDED, got)
	}
	durableSnap := actor.GetStatus().GetDurableSnapshotStatus()
	if !hasDurableSnapshot(durableSnap) {
		return nil, nil, status.Errorf(codes.FailedPrecondition, "Actor %s holds no external snapshot to tag", actorRef)
	}
	// Every way an Actor comes to hold an external snapshot records the
	// template its guest state was built under: a suspend through
	// ensureSuspendedFinalized, a create from a tag through the tag's own UID.
	// A snapshot without one is a broken row, and tagging it would mint a tag
	// that names no template.
	if snapshotActorTemplateUID(durableSnap) == "" {
		return nil, nil, status.Errorf(codes.Internal, "Actor %s holds an external snapshot but records no template it was built under", actorRef)
	}
	actorTemplate, err := resolveActorTemplate(ctx, w.store, actor)
	if err != nil {
		return nil, nil, err
	}
	return actor, actorTemplate, nil
}

func snapshotActorTemplateUID(snap *ateapipb.Snapshot) string {
	if uid := snap.GetObject().GetActorTemplateUid(); uid != "" {
		return uid
	}
	return snap.GetBlock().GetActorTemplateUid()
}

// ensureTagReserved takes the tag's name in TAG_STATE_CREATING and records the
// destination snapshot URI.
//
// A name already taken is AlreadyExists, whether the tag holding it is finished
// or was left pending by a create that died. Resuming a pending row would mean
// deciding whether the objects under it still belong to the snapshot being
// tagged, and the row may not even be this actor's; deleting the tag collects
// them and frees the name, so a retry is a delete followed by a create.
func (w *ActorWorkflow) ensureTagReserved(ctx context.Context, tagRef resources.TagRef, actor *ateapipb.Actor, actorTemplate *ateapipb.ActorTemplate, tag *ateapipb.Tag) (_ *ateapipb.Tag, err error) {
	ctx, done := stepSpan(ctx, "ReserveTag")
	defer func() { err = done(err) }()

	uid := uuid.NewString()
	tagToCreate := &ateapipb.Tag{
		Metadata:    &ateapipb.ResourceMetadata{Atespace: tagRef.Atespace, Name: tagRef.Name, Uid: uid},
		Scope:       tag.GetScope(),
		SourceActor: resources.ActorRefFromActor(actor).ToObjectRef(),
	}
	reservedSnap, err := w.getSnapshotPlugin().ReserveTagSnapshot(ctx, tagToCreate, actor, actorTemplate)
	if err != nil {
		return nil, err
	}
	tagToCreate.Status = &ateapipb.TagStatus{
		State: ateapipb.TagState_TAG_STATE_CREATING,
		// The tag records the template the snapshot's guest state was built under, not
		// the one the actor currently points at. A suspended actor can be repointed,
		// and a tag that claimed the new template would hand clones the old template's
		// memory under the new one's identity, past the data-only downgrade a resume of
		// the actor itself would take.
		ActorTemplateUid: snapshotActorTemplateUID(actor.GetStatus().GetDurableSnapshotStatus()),
		Snapshot:         reservedSnap,
	}

	stored, err := w.store.CreateTag(ctx, tagToCreate)
	switch {
	case err == nil:
		return stored, nil
	case errors.Is(err, store.ErrFailedPrecondition):
		return nil, status.Errorf(codes.FailedPrecondition, "Atespace %s not found", tagRef.Atespace)
	case errors.Is(err, store.ErrAlreadyExists):
		return nil, status.Errorf(codes.AlreadyExists, "Tag %s already exists; delete it and create it again to retry", tagRef)
	}
	return nil, fmt.Errorf("while reserving tag %s: %w", tagRef, err)
}

// ensureTagSnapshotCopied copies the actor's external snapshot to the tag's own
// prefix, derived from the reserved row's freshly minted UID. The prefix is
// empty by construction, so the copy never blends with another attempt's objects.
func (w *ActorWorkflow) ensureTagSnapshotCopied(ctx context.Context, tag *ateapipb.Tag, srcSnapshot *ateapipb.Snapshot) (err error) {
	ctx, done := stepSpan(ctx, "CopyTagSnapshot")
	defer func() { err = done(err) }()

	return w.getSnapshotPlugin().CopyToTagSnapshot(ctx, tag, srcSnapshot)
}

// ensureTagFinalized publishes the copy by setting status.state to
// TAG_STATE_READY. Until this lands the tag is pending and unusable; deleting
// it collects any partial copy.
func (w *ActorWorkflow) ensureTagFinalized(ctx context.Context, tag *ateapipb.Tag) (_ *ateapipb.Tag, err error) {
	ctx, done := stepSpan(ctx, "FinalizeTag")
	defer func() { err = done(err) }()

	tagRef := resources.TagRefFromTag(tag)
	stored, err := w.store.UpdateTag(ctx, tagRef, store.PreconditionFrom(tag), func(toUpdate *ateapipb.Tag) error {
		if tag.GetStatus().GetSnapshot() != nil {
			toUpdate.Status.Snapshot = proto.CloneOf(tag.GetStatus().GetSnapshot())
		}
		toUpdate.Status.State = ateapipb.TagState_TAG_STATE_READY
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrUIDConflict) {
			return nil, status.Errorf(codes.Aborted, "Tag %s was deleted while it was being created, please retry", tagRef)
		}
		return nil, fmt.Errorf("while finalizing tag %s: %w", tagRef, err)
	}
	return stored, nil
}
