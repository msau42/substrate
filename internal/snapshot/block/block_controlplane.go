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

package block

import (
	"context"
	"fmt"
	"log/slog"
	"maps"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/snapshot"
	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	storagev1listers "k8s.io/client-go/listers/storage/v1"
)

const (
	// DefaultSnapshotVolumeName is the logical volume name used for an actor's
	// block snapshot volume.
	DefaultSnapshotVolumeName = "snapshot"

	// DefaultSnapshotCapacity is the default volume capacity requested when not
	// explicitly specified by the ActorTemplate or VolumeClassSpec.
	DefaultSnapshotCapacity = "10Gi"

	// DefaultVolumeDriver is the default CSI driver identifier when no
	// StorageClass provisioner is specified.
	DefaultVolumeDriver = "pd.csi.storage.gke.io"
)

// VolumePluginRegistry resolves a control-plane VolumePlugin by CSI driver name.
type VolumePluginRegistry interface {
	GetPlugin(ctx context.Context, driverName string) (volume.VolumePluginControlPlane, error)
}

// TagResolver resolves a Tag reference when an actor is cloned from a source
// tag or seeded from a golden tag.
type TagResolver func(ctx context.Context, actor *ateapipb.Actor, tagRef *ateapipb.ObjectRef, tmpl *ateapipb.ActorTemplate) (*ateapipb.Tag, error)

// VolumeClassSpec describes the storage parameters resolved from an
// ActorTemplate's ExternalVolumeClass / StorageClass configuration.
type VolumeClassSpec struct {
	VolumeName string
	DriverName string
	Capacity   string
	Parameters map[string]string
}

// VolumeClassResolver extracts the VolumeClassSpec for an actor's snapshot
// block volume from its ActorTemplate.
type VolumeClassResolver func(ctx context.Context, tmpl *ateapipb.ActorTemplate) (VolumeClassSpec, error)

// BlockSnapshotPluginControlPlane implements snapshot.SnapshotPluginControlPlane
// using dynamically attached remote block storage volumes via CSI.
type BlockSnapshotPluginControlPlane struct {
	volPlugin          volume.VolumePluginControlPlane
	registry           VolumePluginRegistry
	scLister           storagev1listers.StorageClassLister
	resolveTag         TagResolver
	resolveVolumeClass VolumeClassResolver
	defaultVolumeClass VolumeClassSpec
}

var _ snapshot.SnapshotPluginControlPlane = (*BlockSnapshotPluginControlPlane)(nil)

// ControlPlaneOption configures BlockSnapshotPluginControlPlane.
type ControlPlaneOption func(*BlockSnapshotPluginControlPlane)

// WithVolumePluginRegistry configures a registry to look up CSI driver plugins
// dynamically by driver name.
func WithVolumePluginRegistry(registry VolumePluginRegistry) ControlPlaneOption {
	return func(p *BlockSnapshotPluginControlPlane) {
		p.registry = registry
	}
}

// WithStorageClassLister configures a Kubernetes StorageClassLister for
// resolving CSI driver names and parameters from ActorTemplate references.
func WithStorageClassLister(scLister storagev1listers.StorageClassLister) ControlPlaneOption {
	return func(p *BlockSnapshotPluginControlPlane) {
		p.scLister = scLister
	}
}

// WithTagResolver configures a TagResolver for resolving source or golden tags
// during PrepareNewActor.
func WithTagResolver(resolver TagResolver) ControlPlaneOption {
	return func(p *BlockSnapshotPluginControlPlane) {
		p.resolveTag = resolver
	}
}

// WithVolumeClassResolver overrides how VolumeClassSpec is extracted from an
// ActorTemplate.
func WithVolumeClassResolver(resolver VolumeClassResolver) ControlPlaneOption {
	return func(p *BlockSnapshotPluginControlPlane) {
		p.resolveVolumeClass = resolver
	}
}

// WithDefaultVolumeClass sets fallback volume class settings when an
// ActorTemplate does not specify an explicit StorageClass.
func WithDefaultVolumeClass(spec VolumeClassSpec) ControlPlaneOption {
	return func(p *BlockSnapshotPluginControlPlane) {
		p.defaultVolumeClass = spec
	}
}

// NewBlockSnapshotPluginControlPlane constructs a new control-plane block
// snapshot plugin.
func NewBlockSnapshotPluginControlPlane(volPlugin volume.VolumePluginControlPlane, opts ...ControlPlaneOption) *BlockSnapshotPluginControlPlane {
	p := &BlockSnapshotPluginControlPlane{
		volPlugin: volPlugin,
		defaultVolumeClass: VolumeClassSpec{
			VolumeName: DefaultSnapshotVolumeName,
			DriverName: DefaultVolumeDriver,
			Capacity:   DefaultSnapshotCapacity,
		},
	}
	for _, opt := range opts {
		opt(p)
	}
	if p.resolveVolumeClass == nil {
		p.resolveVolumeClass = p.defaultResolveVolumeClass
	}
	return p
}

// PrepareNewActor provisions a dedicated block volume for a newly created actor.
// If the actor specifies a source_tag or its template has a golden_tag, the
// new volume is cloned from the tag's underlying CSI volume snapshot.
func (p *BlockSnapshotPluginControlPlane) PrepareNewActor(
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

	classSpec, err := p.resolveVolumeClass(ctx, tmpl)
	if err != nil {
		return nil, nil, fmt.Errorf("while resolving volume class for actor %s/%s: %w",
			actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetName(), err)
	}
	if classSpec.VolumeName == "" {
		classSpec.VolumeName = DefaultSnapshotVolumeName
	}
	if classSpec.Capacity == "" {
		classSpec.Capacity = DefaultSnapshotCapacity
	}
	if classSpec.DriverName == "" {
		classSpec.DriverName = p.defaultVolumeClass.DriverName
	}

	params := make(map[string]string, len(classSpec.Parameters)+1)
	maps.Copy(params, classSpec.Parameters)

	tagRef := actor.GetSourceTag()
	if tagRef == nil {
		tagRef = tmpl.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag()
	}
	var initialSnapshotID string
	// TODO: tagTemplateUID and tagContentScope won't be needed after v2 lifecycle
	var tagTemplateUID string
	var tagContentScope ateapipb.SnapshotContentScope
	if tagRef != nil && p.resolveTag != nil {
		tag, err := p.resolveTag(ctx, actor, tagRef, tmpl)
		if err != nil {
			return nil, nil, err
		}
		if tagSnapID := extractTagCSISnapshotID(tag); tagSnapID != "" {
			params[volume.SourceSnapshotIDParameterKey] = tagSnapID
		}
		initialSnapshotID = tag.GetStatus().GetSnapshot().GetSnapshotId()
		tagTemplateUID = tag.GetStatus().GetActorTemplateUid()
		if tagTemplateUID == "" {
			tagTemplateUID = tag.GetStatus().GetSnapshot().GetBlock().GetActorTemplateUid()
		}
		tagContentScope = tag.GetStatus().GetSnapshot().GetContentScope()
	}
	if tagTemplateUID == "" {
		tagTemplateUID = tmpl.GetMetadata().GetUid()
	}

	plugin, err := p.getPlugin(ctx, classSpec.DriverName)
	if err != nil {
		return nil, nil, fmt.Errorf("while resolving volume plugin for driver %q: %w", classSpec.DriverName, err)
	}

	actorUID := actor.GetMetadata().GetUid()
	if actorUID == "" {
		actorUID = uuid.NewString()
	}
	reqVolName := actorSnapshotVolumeName(actorUID, classSpec.VolumeName)

	// TODO: update state to CREATING first so that we don't lose the volume on crash
	storageVolID, volCtx, err := plugin.CreateVolume(ctx, reqVolName, classSpec.Capacity, classSpec.DriverName, params)
	if err != nil {
		return nil, nil, fmt.Errorf("while creating block snapshot volume %q: %w", reqVolName, err)
	}

	storage := &ateapipb.SnapshotStorage{
		BlockVolume: &ateapipb.ExternalVolume{
			VolumeName:      reqVolName,
			StorageVolumeId: storageVolID,
			VolumeType:      classSpec.DriverName,
			Status:          ateapipb.ExternalVolume_STATUS_CREATED,
			VolumeContext:   maps.Clone(volCtx),
		},
	}

	if initialSnapshotID == "" {
		return storage, nil, nil
	}

	return storage, &ateapipb.Snapshot{
		SnapshotId: initialSnapshotID,
		Block: &ateapipb.BlockSnapshot{
			ActorTemplateUid: tagTemplateUID,
		},
		Survivability: ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE,
		ContentScope:  tagContentScope,
	}, nil
}

// PrepareNewSnapshot constructs snapshot metadata reusing the actor's existing
// block volume without provisioning a new volume.
func (p *BlockSnapshotPluginControlPlane) PrepareNewSnapshot(
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
		return nil, fmt.Errorf("snapshotID is required")
	}
	var contentScope ateapipb.SnapshotContentScope
	switch targetRung {
	case ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_RESIDENT,
		ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_LOCAL:
		contentScope = tmpl.GetSnapshotConfig().GetOnPause()
	case ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE:
		contentScope = commitSnapshotScope(actor.GetMetadata().GetAtespace(), tmpl)
	default:
		return nil, fmt.Errorf("unsupported survivability rung: %v", targetRung)
	}

	if _, err := p.resolveActorExternalVolume(actor); err != nil {
		return nil, err
	}

	return &ateapipb.Snapshot{
		SnapshotId: snapshotID,
		Block: &ateapipb.BlockSnapshot{
			ActorTemplateUid: tmpl.GetMetadata().GetUid(),
		},
		Survivability: targetRung,
		ContentScope:  contentScope,
	}, nil
}

func commitSnapshotScope(atespace string, tmpl *ateapipb.ActorTemplate) ateapipb.SnapshotContentScope {
	if atespace == resources.GoldenActorAtespace {
		return ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL
	}
	return tmpl.GetSnapshotConfig().GetOnCommit()
}

// PrepareSnapshotEscalation is a control-plane no-op because the actor's block
// volume is already attached and stores the checkpoint data directly.
func (p *BlockSnapshotPluginControlPlane) PrepareSnapshotEscalation(
	ctx context.Context,
	actor *ateapipb.Actor,
	tmpl *ateapipb.ActorTemplate,
	snap *ateapipb.Snapshot,
	targetRung ateapipb.SurvivabilityRung,
) (*ateapipb.Snapshot, error) {
	if snap == nil {
		return nil, nil
	}
	cloned := proto.CloneOf(snap)
	cloned.Survivability = targetRung
	if cloned.GetBlock() != nil && tmpl != nil {
		if targetRung == ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE {
			cloned.ContentScope = commitSnapshotScope(actor.GetMetadata().GetAtespace(), tmpl)
		}
		if tmplUID := tmpl.GetMetadata().GetUid(); tmplUID != "" {
			cloned.Block.ActorTemplateUid = tmplUID
		}
	}
	return cloned, nil
}

// AssignToNode attaches the actor's block snapshot volume to targetNode.
func (p *BlockSnapshotPluginControlPlane) AssignToNode(
	ctx context.Context,
	actor *ateapipb.Actor,
	targetNode string,
) error {
	if targetNode == "" {
		return fmt.Errorf("targetNode is required")
	}
	extVol, err := p.resolveActorExternalVolume(actor)
	if err != nil {
		return err
	}
	if extVol.GetStorageVolumeId() == "" {
		return fmt.Errorf("snapshot block volume has no storage volume ID")
	}
	plugin, err := p.getPlugin(ctx, extVol.GetVolumeType())
	if err != nil {
		return fmt.Errorf("while resolving volume plugin for %q: %w", extVol.GetVolumeType(), err)
	}
	if err := plugin.AttachVolume(ctx, extVol.GetStorageVolumeId(), targetNode); err != nil {
		return fmt.Errorf("while attaching block snapshot volume %q to node %q: %w", extVol.GetStorageVolumeId(), targetNode, err)
	}
	return nil
}

// UnassignFromNode detaches the actor's block snapshot volume from assignedNode.
func (p *BlockSnapshotPluginControlPlane) UnassignFromNode(
	ctx context.Context,
	actor *ateapipb.Actor,
	assignedNode string,
) error {
	if assignedNode == "" {
		return fmt.Errorf("assignedNode is required")
	}
	extVol, err := p.resolveActorExternalVolume(actor)
	if err != nil {
		return err
	}
	if extVol.GetStorageVolumeId() == "" {
		return nil
	}
	plugin, err := p.getPlugin(ctx, extVol.GetVolumeType())
	if err != nil {
		return fmt.Errorf("while resolving volume plugin for %q: %w", extVol.GetVolumeType(), err)
	}
	if err := plugin.DetachVolume(ctx, extVol.GetStorageVolumeId(), assignedNode); err != nil {
		if status.Code(err) == codes.NotFound {
			slog.WarnContext(ctx, "Block snapshot volume not found during detach, assuming already detached",
				slog.String("volume_id", extVol.GetStorageVolumeId()),
				slog.String("node", assignedNode))
			return nil
		}
		return fmt.Errorf("while detaching block snapshot volume %q from node %q: %w", extVol.GetStorageVolumeId(), assignedNode, err)
	}
	return nil
}

// DeleteSnapshot is a control-plane no-op for block snapshots. Previous local
// checkpoint directories inside the mounted block volume are pruned by the
// worker plane when committing or escalating a new checkpoint, and the block
// volume itself persists until the actor is deleted.
func (p *BlockSnapshotPluginControlPlane) DeleteSnapshot(
	ctx context.Context,
	actor *ateapipb.Actor,
	snap *ateapipb.Snapshot,
) error {
	return nil
}

// DeleteActor deletes the actor's underlying block volume via the VolumePlugin.
func (p *BlockSnapshotPluginControlPlane) DeleteActor(
	ctx context.Context,
	actor *ateapipb.Actor,
) error {
	if actor == nil {
		return nil
	}
	extVol := p.findExistingActorVolume(actor)
	if extVol == nil || extVol.GetStorageVolumeId() == "" {
		return nil
	}

	plugin, err := p.getPlugin(ctx, extVol.GetVolumeType())
	if err != nil {
		return fmt.Errorf("while resolving volume plugin for %q: %w", extVol.GetVolumeType(), err)
	}
	if err := plugin.DeleteVolume(ctx, extVol.GetStorageVolumeId()); err != nil {
		if status.Code(err) == codes.NotFound {
			slog.WarnContext(ctx, "Block snapshot volume not found during actor deletion, assuming already deleted",
				slog.String("volume_id", extVol.GetStorageVolumeId()))
			return nil
		}
		return fmt.Errorf("while deleting actor block snapshot volume %q: %w", extVol.GetStorageVolumeId(), err)
	}
	return nil
}

// ReserveTagSnapshot constructs the initial Snapshot metadata for a Tag without
// invoking CSI yet. The actual CSI volume snapshot is created during
// CopyToTagSnapshot.
func (p *BlockSnapshotPluginControlPlane) ReserveTagSnapshot(
	ctx context.Context,
	tag *ateapipb.Tag,
	sourceActor *ateapipb.Actor,
	tmpl *ateapipb.ActorTemplate,
) (*ateapipb.Snapshot, error) {
	if tag == nil || sourceActor == nil || tmpl == nil {
		return nil, fmt.Errorf("tag, sourceActor, and tmpl are required")
	}

	if tag.GetMetadata().GetUid() == "" {
		return nil, fmt.Errorf("tag UID is required")
	}

	srcSnap := sourceActor.GetStatus().GetDurableSnapshotStatus()
	ckptID := srcSnap.GetSnapshotId()
	if ckptID == "" {
		return nil, fmt.Errorf("source actor snapshot ID is required")
	}

	srcVol, err := p.resolveActorExternalVolume(sourceActor)
	if err != nil {
		return nil, fmt.Errorf("while resolving source actor block volume for tag: %w", err)
	}
	if srcVol.GetStorageVolumeId() == "" {
		return nil, fmt.Errorf("source actor block volume has no storage volume ID")
	}

	tmplUID := srcSnap.GetBlock().GetActorTemplateUid()
	if tmplUID == "" {
		tmplUID = tmpl.GetMetadata().GetUid()
	}

	return &ateapipb.Snapshot{
		SnapshotId: ckptID,
		Block: &ateapipb.BlockSnapshot{
			ActorTemplateUid: tmplUID,
			SourceVolumeId:   srcVol.GetStorageVolumeId(),
			VolumeType:       srcVol.GetVolumeType(),
		},
		Survivability: ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE,
		ContentScope:  srcSnap.GetContentScope(),
	}, nil
}

// CopyToTagSnapshot triggers a CSI CreateSnapshot on the source actor's block
// volume to create an immutable snapshot backing the tag.
func (p *BlockSnapshotPluginControlPlane) CopyToTagSnapshot(
	ctx context.Context,
	tag *ateapipb.Tag,
	sourceSnapshot *ateapipb.Snapshot,
) error {
	if tag == nil {
		return fmt.Errorf("tag is required")
	}
	tagUID := tag.GetMetadata().GetUid()
	if tagUID == "" {
		return fmt.Errorf("tag UID is required")
	}

	tagSnap := tag.GetStatus().GetSnapshot()
	if tagSnap == nil || tagSnap.GetBlock() == nil {
		return fmt.Errorf("tag block snapshot is required")
	}

	sourceVolumeID := tagSnap.GetBlock().GetSourceVolumeId()
	if sourceVolumeID == "" {
		sourceVolumeID = sourceSnapshot.GetBlock().GetSourceVolumeId()
	}
	if sourceVolumeID == "" {
		return fmt.Errorf("source volume ID is required to create a tag snapshot")
	}
	driverName := tagSnap.GetBlock().GetVolumeType()
	if driverName == "" {
		driverName = sourceSnapshot.GetBlock().GetVolumeType()
	}
	if driverName == "" {
		driverName = p.defaultVolumeClass.DriverName
	}

	tagSnapName := fmt.Sprintf("substrate-tag-%s", tagUID)

	plugin, err := p.getPlugin(ctx, driverName)
	if err != nil {
		return fmt.Errorf("while resolving volume plugin for driver %q: %w", driverName, err)
	}

	csiSnapshotID, err := plugin.CreateSnapshot(ctx, tagSnapName, sourceVolumeID, nil)
	if err != nil {
		return fmt.Errorf("while creating CSI volume snapshot %q from volume %q: %w", tagSnapName, sourceVolumeID, err)
	}

	tagSnap.GetBlock().VolumeSnapshotId = csiSnapshotID
	return nil
}

// DeleteTagSnapshot deletes the tag's underlying CSI volume snapshot.
func (p *BlockSnapshotPluginControlPlane) DeleteTagSnapshot(
	ctx context.Context,
	tag *ateapipb.Tag,
) error {
	if tag == nil {
		return nil
	}
	csiSnapshotID := extractTagCSISnapshotID(tag)
	if csiSnapshotID == "" {
		return nil
	}

	driverName := p.defaultVolumeClass.DriverName
	if volType := tag.GetStatus().GetSnapshot().GetBlock().GetVolumeType(); volType != "" {
		driverName = volType
	}

	plugin, err := p.getPlugin(ctx, driverName)
	if err != nil {
		return fmt.Errorf("while resolving volume plugin for driver %q: %w", driverName, err)
	}
	if err := plugin.DeleteSnapshot(ctx, csiSnapshotID); err != nil {
		if status.Code(err) == codes.NotFound {
			slog.WarnContext(ctx, "CSI tag snapshot not found during delete, assuming already deleted",
				slog.String("snapshot_id", csiSnapshotID))
			return nil
		}
		return fmt.Errorf("while deleting CSI tag snapshot %q: %w", csiSnapshotID, err)
	}
	return nil
}

func (p *BlockSnapshotPluginControlPlane) getPlugin(ctx context.Context, driverName string) (volume.VolumePluginControlPlane, error) {
	if p.registry != nil {
		if driverName == "" {
			driverName = p.defaultVolumeClass.DriverName
		}
		return p.registry.GetPlugin(ctx, driverName)
	}
	if p.volPlugin != nil {
		return p.volPlugin, nil
	}
	return nil, fmt.Errorf("volume plugin is not configured")
}

// TODO: revisit this
func (p *BlockSnapshotPluginControlPlane) defaultResolveVolumeClass(_ context.Context, tmpl *ateapipb.ActorTemplate) (VolumeClassSpec, error) {
	spec := VolumeClassSpec{
		VolumeName: p.defaultVolumeClass.VolumeName,
		DriverName: p.defaultVolumeClass.DriverName,
		Capacity:   p.defaultVolumeClass.Capacity,
	}
	if len(p.defaultVolumeClass.Parameters) > 0 {
		spec.Parameters = maps.Clone(p.defaultVolumeClass.Parameters)
	}

	var scName string
	for _, v := range tmpl.GetVolumes() {
		if v.GetName() != DefaultSnapshotVolumeName {
			continue
		}
		if extTmpl := v.GetExternalVolumeTemplate(); extTmpl != nil {
			spec.VolumeName = v.GetName()
			if extTmpl.GetCapacity() != "" {
				spec.Capacity = extTmpl.GetCapacity()
			}
			if extTmpl.GetStorageClassName() != "" {
				scName = extTmpl.GetStorageClassName()
			}
			break
		}
	}

	if scName != "" && p.scLister != nil {
		sc, err := p.scLister.Get(scName)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return VolumeClassSpec{}, status.Errorf(codes.FailedPrecondition, "StorageClass %q not found", scName)
			}
			return VolumeClassSpec{}, status.Errorf(codes.Internal, "failed to get StorageClass %q: %v", scName, err)
		}
		spec.DriverName = sc.Provisioner
		if len(sc.Parameters) > 0 {
			spec.Parameters = maps.Clone(sc.Parameters)
		}
	}

	return spec, nil
}

func (p *BlockSnapshotPluginControlPlane) findExistingActorVolume(actor *ateapipb.Actor) *ateapipb.ExternalVolume {
	if extVol := actor.GetStatus().GetSnapshotStorage().GetBlockVolume(); extVol != nil {
		return proto.CloneOf(extVol)
	}
	return nil
}

func (p *BlockSnapshotPluginControlPlane) resolveActorExternalVolume(
	actor *ateapipb.Actor,
) (*ateapipb.ExternalVolume, error) {
	if extVol := p.findExistingActorVolume(actor); extVol != nil {
		return extVol, nil
	}
	return nil, fmt.Errorf("actor has no block snapshot volume")
}

func extractTagCSISnapshotID(tag *ateapipb.Tag) string {
	return tag.GetStatus().GetSnapshot().GetBlock().GetVolumeSnapshotId()
}

func actorSnapshotVolumeName(actorUID, volumeName string) string {
	if volumeName == "" {
		volumeName = DefaultSnapshotVolumeName
	}
	return fmt.Sprintf("substrate-%s-%s", actorUID, volumeName)
}
