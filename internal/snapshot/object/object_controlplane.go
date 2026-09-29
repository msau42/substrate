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

package object

import (
	"context"
	"fmt"

	"github.com/agent-substrate/substrate/internal/objectstore"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/snapshot"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

// TagResolver resolves a Tag reference when an actor is cloned from a source
// tag or seeded from a golden tag.
type TagResolver func(ctx context.Context, actor *ateapipb.Actor, tagRef *ateapipb.ObjectRef, tmpl *ateapipb.ActorTemplate) (*ateapipb.Tag, error)

// ObjectSnapshotPluginControlPlane implements snapshot.SnapshotPluginControlPlane
// for the ObjectWithLocalCache strategy.
type ObjectSnapshotPluginControlPlane struct {
	store      objectstore.Store
	resolveTag TagResolver
}

var _ snapshot.SnapshotPluginControlPlane = (*ObjectSnapshotPluginControlPlane)(nil)

// ControlPlaneOption configures ObjectSnapshotPluginControlPlane.
type ControlPlaneOption func(*ObjectSnapshotPluginControlPlane)

// WithTagResolver configures a TagResolver for resolving source/golden tags
// during PrepareNewActor.
func WithTagResolver(resolver TagResolver) ControlPlaneOption {
	return func(p *ObjectSnapshotPluginControlPlane) {
		p.resolveTag = resolver
	}
}

// NewObjectSnapshotPluginControlPlane constructs a new control-plane plugin.
func NewObjectSnapshotPluginControlPlane(store objectstore.Store, opts ...ControlPlaneOption) *ObjectSnapshotPluginControlPlane {
	p := &ObjectSnapshotPluginControlPlane{
		store: store,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// PrepareNewActor returns an initial Snapshot for a newly created actor.
// If the actor clones from a source tag (or the template's golden tag) and a
// TagResolver is configured, the snapshot points to the borrowed tag URI.
// Otherwise, it returns nil because a fresh actor starts without a snapshot.
func (p *ObjectSnapshotPluginControlPlane) PrepareNewActor(
	ctx context.Context,
	actor *ateapipb.Actor,
	tmpl *ateapipb.ActorTemplate,
) (*ateapipb.SnapshotStorage, *ateapipb.Snapshot, error) {
	if actor == nil {
		return nil, nil, fmt.Errorf("actor is required")
	}
	if tmpl == nil {
		return nil, nil, fmt.Errorf("actor template is required")
	}

	tagRef := actor.GetSourceTag()
	if tagRef == nil {
		tagRef = tmpl.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag()
	}
	if tagRef == nil || p.resolveTag == nil {
		return nil, nil, nil
	}
	tag, err := p.resolveTag(ctx, actor, tagRef, tmpl)
	if err != nil {
		return nil, nil, err
	}
	if tagSnap := tag.GetStatus().GetSnapshot(); tagSnap != nil && tagSnap.GetObject().GetSnapshotUri() != "" {
		cloned := proto.CloneOf(tagSnap)
		if cloned.GetObject() != nil {
			cloned.GetObject().ActorTemplateUid = tag.GetStatus().GetActorTemplateUid()
		}
		cloned.Survivability = ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE
		return nil, cloned, nil
	}
	return nil, nil, nil
}

// PrepareNewSnapshot builds the metadata for an in-progress snapshot at the
// requested survivability rung.
func (p *ObjectSnapshotPluginControlPlane) PrepareNewSnapshot(
	ctx context.Context,
	actor *ateapipb.Actor,
	tmpl *ateapipb.ActorTemplate,
	snapshotID string,
	targetRung ateapipb.SurvivabilityRung,
) (*ateapipb.Snapshot, error) {
	if actor == nil {
		return nil, fmt.Errorf("actor is required")
	}
	if snapshotID == "" {
		snapshotID = resources.NewSnapshotName()
	}

	switch targetRung {
	case ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_RESIDENT,
		ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_LOCAL:
		localSnap := &ateapipb.LocalSnapshot{}
		if nodeName := actor.GetStatus().GetWorkerAssignment().GetNodeName(); nodeName != "" {
			localSnap.NodeVmsWithLocalSnapshots = []string{nodeName}
		}
		return &ateapipb.Snapshot{
			SnapshotId:    snapshotID,
			Local:         localSnap,
			Survivability: targetRung,
			ContentScope:  pausedContentScope(actor.GetStatus().GetLatestSnapshotStatus(), tmpl),
		}, nil

	case ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE:
		if tmpl == nil {
			return nil, fmt.Errorf("actor template is required")
		}
		atespace := actor.GetMetadata().GetAtespace()
		actorUID := actor.GetMetadata().GetUid()
		location := tmpl.GetSnapshotConfig().GetObject().GetStorageLocation()

		uri, err := resources.NewActorSnapshotURI(location, atespace, actorUID, snapshotID)
		if err != nil {
			return nil, fmt.Errorf("while building the snapshot URI for actor %s/%s: %w", atespace, actor.GetMetadata().GetName(), err)
		}

		contentScope := commitSnapshotScope(atespace, tmpl)

		return &ateapipb.Snapshot{
			SnapshotId: snapshotID,
			Object: &ateapipb.ObjectSnapshot{
				SnapshotUri:      uri.String(),
				ActorTemplateUid: tmpl.GetMetadata().GetUid(),
			},
			Survivability: ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE,
			ContentScope:  contentScope,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported survivability rung: %v", targetRung)
	}
}

// commitSnapshotScope returns the scope a commit (suspend) snapshot is taken
// with. Golden actors always commit Full regardless of the template's
// onCommit: the golden snapshot is the base an OnGolden data resume is
// combined with at restore, so it must carry the guest memory and filesystem.
func commitSnapshotScope(atespace string, tmpl *ateapipb.ActorTemplate) ateapipb.SnapshotContentScope {
	if atespace == resources.GoldenActorAtespace {
		return ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL
	}
	return tmpl.GetSnapshotConfig().GetOnCommit()
}

// pausedContentScope returns the scope a paused actor's local snapshot was
// captured with: the value recorded at pause finalization, or the template's
// onPause when unspecified.
func pausedContentScope(snap *ateapipb.Snapshot, tmpl *ateapipb.ActorTemplate) ateapipb.SnapshotContentScope {
	if scope := snap.GetContentScope(); scope != ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED {
		return scope
	}
	return tmpl.GetSnapshotConfig().GetOnPause()
}

// AssignToNode is a no-op because the worker plane downloads the snapshot on
// demand on the target node.
func (p *ObjectSnapshotPluginControlPlane) AssignToNode(
	ctx context.Context,
	actor *ateapipb.Actor,
	targetNode string,
) error {
	return nil
}

// UnassignFromNode is a no-op because the worker plane manages removing its
// local cache directory.
func (p *ObjectSnapshotPluginControlPlane) UnassignFromNode(
	ctx context.Context,
	actor *ateapipb.Actor,
	assignedNode string,
) error {
	return nil
}

// DeleteSnapshot deletes a single snapshot's prefix from object storage.
// If a durable snapshot is borrowed from a Tag (not owned by the actor), it is
// left untouched so other actors cloned from that Tag remain valid.
func (p *ObjectSnapshotPluginControlPlane) DeleteSnapshot(
	ctx context.Context,
	actor *ateapipb.Actor,
	snap *ateapipb.Snapshot,
) error {
	if p.store == nil || snap == nil {
		return nil
	}
	rawURI := snap.GetObject().GetSnapshotUri()
	if rawURI == "" {
		return nil
	}

	uri, err := resources.ParseSnapshotURI(rawURI)
	if err != nil {
		return fmt.Errorf("while parsing the snapshot %q: %w", rawURI, err)
	}
	if actor != nil {
		owner := actorSnapshotOwner(actor)
		if !uri.OwnedBy(owner) {
			if snap == actor.GetStatus().GetInProgressSnapshotStatus() {
				return fmt.Errorf("the in-progress snapshot %q is not owned by actor %s", rawURI, owner)
			}
			return nil
		}
	}
	return objectstore.DeletePrefix(ctx, p.store, uri.Prefix())
}

// DeleteActor deletes the actor's entire owner prefix
// (<location>/atespaces/<ns>/actors/<uid>/) from object storage.
func (p *ObjectSnapshotPluginControlPlane) DeleteActor(
	ctx context.Context,
	actor *ateapipb.Actor,
) error {
	if p.store == nil || actor == nil {
		return nil
	}

	prefix, err := actorSnapshotStoragePrefix(actor)
	if err != nil {
		return err
	}
	if prefix.IsZero() {
		return nil
	}
	return objectstore.DeletePrefix(ctx, p.store, prefix)
}

func actorSnapshotOwner(actor *ateapipb.Actor) resources.SnapshotOwner {
	return resources.ActorSnapshotOwner(actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetUid())
}

// actorSnapshotStoragePrefix returns the prefix holding every object the actor wrote:
// the snapshot it last took, the one a suspend was in the middle of taking, and
// anything a crashed suspend stranded. A zero prefix means the actor never
// wrote anything.
func actorSnapshotStoragePrefix(actor *ateapipb.Actor) (resources.StoragePrefix, error) {
	actorOwner := actorSnapshotOwner(actor)
	if snapshotURI := actor.GetStatus().GetDurableSnapshotStatus().GetObject().GetSnapshotUri(); snapshotURI != "" {
		uri, err := resources.ParseSnapshotURI(snapshotURI)
		if err != nil {
			return resources.StoragePrefix{}, fmt.Errorf("while parsing the external snapshot %q: %w", snapshotURI, err)
		}
		// A URI the actor does not own is a tag's snapshot, borrowed until the actor's
		// first suspend completes, which means it has written nothing of its
		// own yet.
		if uri.OwnedBy(actorOwner) {
			return uri.OwnerPrefix(), nil
		}
	}
	// Nothing of the actor's own is recorded. Unless a suspend died partway,
	// nothing was ever written under its prefix: the in-progress URI is
	// recorded before atelet uploads the first object.
	inProgress := actor.GetStatus().GetInProgressSnapshotStatus().GetObject().GetSnapshotUri()
	if inProgress == "" {
		return resources.StoragePrefix{}, nil
	}
	uri, err := resources.ParseSnapshotURI(inProgress)
	if err != nil {
		return resources.StoragePrefix{}, fmt.Errorf("while parsing the in-progress snapshot %q: %w", inProgress, err)
	}
	if !uri.OwnedBy(actorOwner) {
		return resources.StoragePrefix{}, fmt.Errorf("the in-progress snapshot %q is not owned by actor %s", inProgress, actorOwner)
	}
	return uri.OwnerPrefix(), nil
}

// ReserveTagSnapshot constructs a Snapshot pointing to the deterministic object
// storage prefix for the tag (<location>/atespaces/<ns>/tags/<tag_uid>).
func (p *ObjectSnapshotPluginControlPlane) ReserveTagSnapshot(
	ctx context.Context,
	tag *ateapipb.Tag,
	sourceActor *ateapipb.Actor,
	tmpl *ateapipb.ActorTemplate,
) (*ateapipb.Snapshot, error) {
	if tag == nil || sourceActor == nil || tmpl == nil {
		return nil, fmt.Errorf("tag, sourceActor, and tmpl are required")
	}

	location := tmpl.GetSnapshotConfig().GetObject().GetStorageLocation()
	if err := resources.ValidateSnapshotLocation(location); err != nil {
		return nil, fmt.Errorf("invalid storage location for tag: %w", err)
	}

	tagUID := tag.GetMetadata().GetUid()
	if tagUID == "" {
		tagUID = uuid.NewString()
	}
	atespace := tag.GetMetadata().GetAtespace()
	if atespace == "" {
		atespace = sourceActor.GetMetadata().GetAtespace()
	}

	dst, err := resources.NewTagSnapshotURI(location, atespace, tagUID)
	if err != nil {
		return nil, fmt.Errorf("while building tag snapshot URI: %w", err)
	}

	srcSnap := sourceActor.GetStatus().GetDurableSnapshotStatus()
	return &ateapipb.Snapshot{
		SnapshotId:    dst.Name(),
		Survivability: ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE,
		ContentScope:  srcSnap.GetContentScope(),
		Object: &ateapipb.ObjectSnapshot{
			SnapshotUri: dst.String(),
		},
	}, nil
}

// CopyToTagSnapshot performs a server-side prefix copy in object storage from
// sourceSnapshot's URI to the tag's reserved snapshot URI.
func (p *ObjectSnapshotPluginControlPlane) CopyToTagSnapshot(
	ctx context.Context,
	tag *ateapipb.Tag,
	sourceSnapshot *ateapipb.Snapshot,
) error {
	if p.store == nil {
		return nil
	}
	srcRaw := sourceSnapshot.GetObject().GetSnapshotUri()
	dstRaw := tag.GetStatus().GetSnapshot().GetObject().GetSnapshotUri()

	srcURI, err := resources.ParseSnapshotURI(srcRaw)
	if err != nil {
		return fmt.Errorf("while parsing source snapshot URI %q: %w", srcRaw, err)
	}
	dstURI, err := resources.ParseSnapshotURI(dstRaw)
	if err != nil {
		return fmt.Errorf("while parsing destination tag snapshot URI %q: %w", dstRaw, err)
	}

	if err := objectstore.CopyPrefix(ctx, p.store, srcURI.Prefix(), dstURI.Prefix()); err != nil {
		return fmt.Errorf("while copying snapshot objects to tag %s/%s: %w", tag.GetMetadata().GetAtespace(), tag.GetMetadata().GetName(), err)
	}
	return nil
}

// DeleteTagSnapshot deletes all objects under the tag's snapshot prefix in
// object storage.
func (p *ObjectSnapshotPluginControlPlane) DeleteTagSnapshot(
	ctx context.Context,
	tag *ateapipb.Tag,
) error {
	if p.store == nil || tag == nil {
		return nil
	}
	rawURI := tag.GetStatus().GetSnapshot().GetObject().GetSnapshotUri()
	uri, err := resources.ParseSnapshotURI(rawURI)
	if err != nil {
		return fmt.Errorf("while parsing tag snapshot URI %q: %w", rawURI, err)
	}
	if err := objectstore.DeletePrefix(ctx, p.store, uri.Prefix()); err != nil {
		return fmt.Errorf("while deleting tag snapshot prefix %q: %w", uri, err)
	}
	return nil
}
