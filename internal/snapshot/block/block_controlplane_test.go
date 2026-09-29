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
	"testing"

	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	storagev1listers "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/tools/cache"
)

type recordingControlPlaneVolumePlugin struct {
	*volume.MockVolumePlugin
	volumes              map[string]bool
	snapshots            map[string]bool
	attached             map[string]string
	lastCreateName       string
	lastCreateCapacity   string
	lastCreateVolumeType string
	lastCreateParams     map[string]string
	lastSnapName         string
	lastSnapSourceVolID  string
	lastDeletedSnapID    string
	createCallCount      int
}

func newRecordingControlPlaneVolumePlugin() *recordingControlPlaneVolumePlugin {
	return &recordingControlPlaneVolumePlugin{
		MockVolumePlugin: volume.NewMockVolumePlugin(),
		volumes:          make(map[string]bool),
		snapshots:        make(map[string]bool),
		attached:         make(map[string]string),
	}
}

func (r *recordingControlPlaneVolumePlugin) CreateVolume(
	ctx context.Context,
	name string,
	capacity string,
	volumeType string,
	parameters map[string]string,
) (string, map[string]string, error) {
	r.createCallCount++
	r.lastCreateName = name
	r.lastCreateCapacity = capacity
	r.lastCreateVolumeType = volumeType
	r.lastCreateParams = parameters
	volID, volCtx, err := r.MockVolumePlugin.CreateVolume(ctx, name, capacity, volumeType, parameters)
	if err == nil {
		r.volumes[volID] = true
	}
	return volID, volCtx, err
}

func (r *recordingControlPlaneVolumePlugin) DeleteVolume(ctx context.Context, volumeID string) error {
	delete(r.volumes, volumeID)
	return r.MockVolumePlugin.DeleteVolume(ctx, volumeID)
}

func (r *recordingControlPlaneVolumePlugin) AttachVolume(ctx context.Context, volumeID string, node string) error {
	r.attached[volumeID] = node
	return r.MockVolumePlugin.AttachVolume(ctx, volumeID, node)
}

func (r *recordingControlPlaneVolumePlugin) DetachVolume(ctx context.Context, volumeID string, node string) error {
	delete(r.attached, volumeID)
	return r.MockVolumePlugin.DetachVolume(ctx, volumeID, node)
}

func (r *recordingControlPlaneVolumePlugin) CreateSnapshot(
	ctx context.Context,
	name string,
	sourceVolumeID string,
	parameters map[string]string,
) (string, error) {
	r.lastSnapName = name
	r.lastSnapSourceVolID = sourceVolumeID
	snapID, err := r.MockVolumePlugin.CreateSnapshot(ctx, name, sourceVolumeID, parameters)
	if err == nil {
		r.snapshots[snapID] = true
	}
	return snapID, err
}

func (r *recordingControlPlaneVolumePlugin) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	r.lastDeletedSnapID = snapshotID
	delete(r.snapshots, snapshotID)
	return r.MockVolumePlugin.DeleteSnapshot(ctx, snapshotID)
}

func (r *recordingControlPlaneVolumePlugin) HasVolume(volumeID string) bool {
	return r.volumes[volumeID]
}

func (r *recordingControlPlaneVolumePlugin) HasSnapshot(snapshotID string) bool {
	return r.snapshots[snapshotID]
}

func newTestStorageClassLister(t *testing.T, classes ...*storagev1.StorageClass) storagev1listers.StorageClassLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, sc := range classes {
		if err := indexer.Add(sc); err != nil {
			t.Fatalf("failed to add StorageClass to indexer: %v", err)
		}
	}
	return storagev1listers.NewStorageClassLister(indexer)
}

func testControlActor(uid string) *ateapipb.Actor {
	return &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "team-a",
			Name:     "actor-" + uid,
			Uid:      uid,
		},
	}
}

func testControlTemplate() *ateapipb.ActorTemplate {
	return &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "team-a",
			Name:     "tmpl-1",
			Uid:      "tmpl-uid-1",
		},
		SnapshotConfig: &ateapipb.SnapshotConfig{
			Block: &ateapipb.BlockSnapshotStorage{
				StorageClassName: "hyperdisk-direct-sc",
				Capacity:         "20Gi",
			},
		},
	}
}

func TestBlockControlPlane_PrepareNewActor(t *testing.T) {
	ctx := t.Context()
	scLister := newTestStorageClassLister(t, &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "hyperdisk-direct-sc"},
		Provisioner: "pd.csi.storage.gke.io",
		Parameters: map[string]string{
			"type": "hyperdisk-direct",
		},
	})

	t.Run("fresh actor provisions new block volume", func(t *testing.T) {
		volPlugin := newRecordingControlPlaneVolumePlugin()
		cp := NewBlockSnapshotPluginControlPlane(volPlugin, WithStorageClassLister(scLister))

		actor := testControlActor("uid-fresh")
		tmpl := testControlTemplate()

		storage, snap, err := cp.PrepareNewActor(ctx, actor, tmpl)
		if err != nil {
			t.Fatalf("PrepareNewActor() error = %v", err)
		}
		if snap != nil {
			t.Errorf("PrepareNewActor() snap = %v, want nil for fresh actor", snap)
		}
		if volPlugin.createCallCount != 1 {
			t.Fatalf("CreateVolume call count = %d, want 1", volPlugin.createCallCount)
		}
		if volPlugin.lastCreateCapacity != "20Gi" {
			t.Errorf("CreateVolume capacity = %q, want %q", volPlugin.lastCreateCapacity, "20Gi")
		}
		if volPlugin.lastCreateParams["type"] != "hyperdisk-direct" {
			t.Errorf("CreateVolume type param = %q, want %q", volPlugin.lastCreateParams["type"], "hyperdisk-direct")
		}
		if _, hasSource := volPlugin.lastCreateParams[volume.SourceSnapshotIDParameterKey]; hasSource {
			t.Errorf("unexpected source snapshot ID parameter on fresh actor volume creation")
		}

		extVol := storage.GetBlockVolume()
		if extVol == nil {
			t.Fatalf("storage.GetBlockVolume() = nil, want populated ExternalVolume")
		}
		if extVol.GetStatus() != ateapipb.ExternalVolume_STATUS_CREATED {
			t.Errorf("ExternalVolume.Status = %v, want STATUS_CREATED", extVol.GetStatus())
		}
		wantVolID := "mock-vol-substrate-uid-fresh-snapshot"
		if extVol.GetStorageVolumeId() != wantVolID {
			t.Errorf("StorageVolumeId = %q, want %q", extVol.GetStorageVolumeId(), wantVolID)
		}
		if extVol.GetVolumeType() != "pd.csi.storage.gke.io" {
			t.Errorf("VolumeType = %q, want %q", extVol.GetVolumeType(), "pd.csi.storage.gke.io")
		}
	})

	t.Run("cloned actor from source tag clones volume from CSI snapshot", func(t *testing.T) {
		volPlugin := newRecordingControlPlaneVolumePlugin()
		cp := NewBlockSnapshotPluginControlPlane(
			volPlugin,
			WithStorageClassLister(scLister),
			WithTagResolver(func(ctx context.Context, a *ateapipb.Actor, tagRef *ateapipb.ObjectRef, tmpl *ateapipb.ActorTemplate) (*ateapipb.Tag, error) {
				tagSnap := &ateapipb.Snapshot{
					SnapshotId:    "ckpt-99",
					Survivability: ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE,
					ContentScope:  ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
					Block: &ateapipb.BlockSnapshot{
						VolumeSnapshotId: "csi-tag-snap-99",
						ActorTemplateUid: "tmpl-uid-1",
						SourceVolumeId:   "src-vol-99",
						VolumeType:       "pd.csi.storage.gke.io",
					},
				}
				return &ateapipb.Tag{
					Metadata: &ateapipb.ResourceMetadata{Atespace: tagRef.GetAtespace(), Name: tagRef.GetName(), Uid: "tag-uid-99"},
					Status:   &ateapipb.TagStatus{Snapshot: tagSnap},
				}, nil
			}),
		)

		actor := testControlActor("uid-cloned")
		actor.SourceTag = &ateapipb.ObjectRef{Atespace: "team-a", Name: "my-tag"}

		storage, snap, err := cp.PrepareNewActor(ctx, actor, testControlTemplate())
		if err != nil {
			t.Fatalf("PrepareNewActor() error = %v", err)
		}
		if got := volPlugin.lastCreateParams[volume.SourceSnapshotIDParameterKey]; got != "csi-tag-snap-99" {
			t.Errorf("SourceSnapshotIDParameterKey = %q, want %q", got, "csi-tag-snap-99")
		}
		wantVolID := "mock-vol-substrate-uid-cloned-snapshot"
		if storage.GetBlockVolume().GetStorageVolumeId() != wantVolID {
			t.Errorf("StorageVolumeId = %q, want %q", storage.GetBlockVolume().GetStorageVolumeId(), wantVolID)
		}
		if snap.GetSnapshotId() != "ckpt-99" {
			t.Errorf("SnapshotId = %q, want %q", snap.GetSnapshotId(), "ckpt-99")
		}
		if snap.GetBlock().GetActorTemplateUid() != "tmpl-uid-1" {
			t.Errorf("ActorTemplateUid = %q, want %q", snap.GetBlock().GetActorTemplateUid(), "tmpl-uid-1")
		}
	})
}

func TestBlockControlPlane_IgnoresNonSnapshotExternalVolumes(t *testing.T) {
	ctx := t.Context()
	scLister := newTestStorageClassLister(t, &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "nfs-csi"},
		Provisioner: "nfs.csi.k8s.io",
	})
	volPlugin := newRecordingControlPlaneVolumePlugin()
	cp := NewBlockSnapshotPluginControlPlane(
		volPlugin,
		WithStorageClassLister(scLister),
		WithDefaultVolumeClass(VolumeClassSpec{
			VolumeName: DefaultSnapshotVolumeName,
			DriverName: "hostpath.csi.k8s.io",
			Capacity:   DefaultSnapshotCapacity,
		}),
	)

	tmpl := &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "team-a",
			Name:     "tmpl-ext",
			Uid:      "tmpl-ext-uid",
		},
		Volumes: []*ateapipb.Volume{
			{
				Name: "external",
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
					StorageClassName: "nfs-csi",
					Capacity:         "1Gi",
				},
			},
		},
	}
	actor := testControlActor("uid-ext")
	actor.Status = &ateapipb.ActorStatus{
		ActorVolumes: []*ateapipb.ExternalVolume{
			{
				VolumeName:      "external",
				StorageVolumeId: "nfs-vol-1",
				VolumeType:      "nfs.csi.k8s.io",
				Status:          ateapipb.ExternalVolume_STATUS_CREATED,
			},
		},
	}

	storage, snap, err := cp.PrepareNewActor(ctx, actor, tmpl)
	if err != nil {
		t.Fatalf("PrepareNewActor() error = %v", err)
	}
	if snap != nil {
		t.Errorf("PrepareNewActor() snap = %v, want nil", snap)
	}
	if got := storage.GetBlockVolume().GetVolumeType(); got != "hostpath.csi.k8s.io" {
		t.Errorf("PrepareNewActor VolumeType = %q, want %q", got, "hostpath.csi.k8s.io")
	}
	wantVolName := actorSnapshotVolumeName("uid-ext", DefaultSnapshotVolumeName)
	if got := storage.GetBlockVolume().GetVolumeName(); got != wantVolName {
		t.Errorf("PrepareNewActor VolumeName = %q, want %q", got, wantVolName)
	}

	// findExistingActorVolume should not return the user-mounted "external" volume when SnapshotStorage is nil.
	found := cp.findExistingActorVolume(actor)
	if found != nil {
		t.Errorf("findExistingActorVolume() = %v, want nil when only non-snapshot external volume is present", found)
	}
}

func TestBlockControlPlane_SnapshotAndNodeLifecycle(t *testing.T) {
	ctx := t.Context()
	volPlugin := newRecordingControlPlaneVolumePlugin()
	cp := NewBlockSnapshotPluginControlPlane(volPlugin)

	actor := testControlActor("uid-lifecycle")
	tmpl := testControlTemplate()

	storage, initialSnap, err := cp.PrepareNewActor(ctx, actor, tmpl)
	if err != nil {
		t.Fatalf("PrepareNewActor() error = %v", err)
	}
	if initialSnap != nil {
		t.Fatalf("PrepareNewActor() initialSnap = %v, want nil", initialSnap)
	}
	actor.Status = &ateapipb.ActorStatus{
		SnapshotStorage: storage,
	}

	// PrepareNewSnapshot must reuse the existing block volume without calling CreateVolume again.
	localSnap, err := cp.PrepareNewSnapshot(ctx, actor, tmpl, "snap-2", ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_LOCAL)
	if err != nil {
		t.Fatalf("PrepareNewSnapshot() error = %v", err)
	}
	if volPlugin.createCallCount != 1 {
		t.Errorf("CreateVolume call count after PrepareNewSnapshot = %d, want 1", volPlugin.createCallCount)
	}
	if localSnap.GetSnapshotId() != "snap-2" {
		t.Errorf("SnapshotId = %q, want %q", localSnap.GetSnapshotId(), "snap-2")
	}
	if localSnap.GetSurvivability() != ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_LOCAL {
		t.Errorf("Survivability = %v, want LOCAL", localSnap.GetSurvivability())
	}
	if localSnap.GetBlock().GetActorTemplateUid() != "tmpl-uid-1" {
		t.Errorf("ActorTemplateUid = %q, want %q", localSnap.GetBlock().GetActorTemplateUid(), "tmpl-uid-1")
	}

	// PrepareSnapshotEscalation updates rung without creating a volume.
	escalatedSnap, err := cp.PrepareSnapshotEscalation(ctx, actor, tmpl, localSnap, ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE)
	if err != nil {
		t.Fatalf("PrepareSnapshotEscalation() error = %v", err)
	}
	if escalatedSnap.GetSurvivability() != ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE {
		t.Errorf("escalated Survivability = %v, want DURABLE", escalatedSnap.GetSurvivability())
	}

	// AssignToNode attaches the volume to the worker node.
	if err := cp.AssignToNode(ctx, actor, "node-a"); err != nil {
		t.Fatalf("AssignToNode() error = %v", err)
	}
	wantVolID := "mock-vol-substrate-uid-lifecycle-snapshot"
	if gotNode := volPlugin.attached[wantVolID]; gotNode != "node-a" {
		t.Errorf("attached node = %q, want %q", gotNode, "node-a")
	}

	// UnassignFromNode detaches the volume from the worker node.
	if err := cp.UnassignFromNode(ctx, actor, "node-a"); err != nil {
		t.Fatalf("UnassignFromNode() error = %v", err)
	}
	if _, attached := volPlugin.attached[wantVolID]; attached {
		t.Errorf("expected volume %q to be detached", wantVolID)
	}

	// DeleteSnapshot is a control-plane no-op and leaves the actor's volume intact.
	if err := cp.DeleteSnapshot(ctx, actor, escalatedSnap); err != nil {
		t.Fatalf("DeleteSnapshot() error = %v", err)
	}
	if !volPlugin.HasVolume(wantVolID) {
		t.Errorf("expected volume %q to remain intact after DeleteSnapshot", wantVolID)
	}

	// DeleteActor deletes the underlying block volume.
	if err := cp.DeleteActor(ctx, actor); err != nil {
		t.Fatalf("DeleteActor() error = %v", err)
	}
	if volPlugin.HasVolume(wantVolID) {
		t.Errorf("expected volume %q to be deleted after DeleteActor", wantVolID)
	}
}

func TestBlockControlPlane_TagLifecycle(t *testing.T) {
	ctx := t.Context()
	volPlugin := newRecordingControlPlaneVolumePlugin()
	cp := NewBlockSnapshotPluginControlPlane(volPlugin)

	actor := testControlActor("uid-tag-source")
	tmpl := testControlTemplate()

	storage, _, err := cp.PrepareNewActor(ctx, actor, tmpl)
	if err != nil {
		t.Fatalf("PrepareNewActor() error = %v", err)
	}
	actor.Status = &ateapipb.ActorStatus{
		SnapshotStorage: storage,
	}
	actorSnap, err := cp.PrepareNewSnapshot(ctx, actor, tmpl, "ckpt-golden-1", ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE)
	if err != nil {
		t.Fatalf("PrepareNewSnapshot() error = %v", err)
	}
	actor.Status.DurableSnapshotStatus = actorSnap

	tag := &ateapipb.Tag{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "team-a",
			Name:     "v1",
			Uid:      "tag-uid-1",
		},
	}

	reservedSnap, err := cp.ReserveTagSnapshot(ctx, tag, actor, tmpl)
	if err != nil {
		t.Fatalf("ReserveTagSnapshot() error = %v", err)
	}
	if reservedSnap.GetSnapshotId() != "ckpt-golden-1" {
		t.Errorf("reserved SnapshotId = %q, want %q", reservedSnap.GetSnapshotId(), "ckpt-golden-1")
	}
	if reservedSnap.GetBlock().GetVolumeSnapshotId() != "" {
		t.Errorf("reserved VolumeSnapshotId = %q, want empty", reservedSnap.GetBlock().GetVolumeSnapshotId())
	}
	if reservedSnap.GetBlock().GetActorTemplateUid() != "tmpl-uid-1" {
		t.Errorf("reserved ActorTemplateUid = %q, want %q", reservedSnap.GetBlock().GetActorTemplateUid(), "tmpl-uid-1")
	}
	wantSrcVolID := "mock-vol-substrate-uid-tag-source-snapshot"
	if reservedSnap.GetBlock().GetSourceVolumeId() != wantSrcVolID {
		t.Errorf("reserved SourceVolumeId = %q, want %q", reservedSnap.GetBlock().GetSourceVolumeId(), wantSrcVolID)
	}
	tag.Status = &ateapipb.TagStatus{
		Snapshot: reservedSnap,
	}

	if err := cp.CopyToTagSnapshot(ctx, tag, actorSnap); err != nil {
		t.Fatalf("CopyToTagSnapshot() error = %v", err)
	}
	if volPlugin.lastSnapSourceVolID != wantSrcVolID {
		t.Errorf("CreateSnapshot sourceVolumeID = %q, want %q", volPlugin.lastSnapSourceVolID, wantSrcVolID)
	}
	wantCSISnapID := "mock-snap-substrate-tag-tag-uid-1"
	if !volPlugin.HasSnapshot(wantCSISnapID) {
		t.Errorf("expected CSI snapshot %q to exist in volume plugin", wantCSISnapID)
	}
	if got := tag.GetStatus().GetSnapshot().GetBlock().GetVolumeSnapshotId(); got != wantCSISnapID {
		t.Errorf("copied tag VolumeSnapshotId = %q, want %q", got, wantCSISnapID)
	}
	if got := tag.GetStatus().GetSnapshot().GetSnapshotId(); got != "ckpt-golden-1" {
		t.Errorf("copied tag SnapshotId = %q, want %q", got, "ckpt-golden-1")
	}

	if err := cp.DeleteTagSnapshot(ctx, tag); err != nil {
		t.Fatalf("DeleteTagSnapshot() error = %v", err)
	}
	if volPlugin.HasSnapshot(wantCSISnapID) {
		t.Errorf("expected CSI snapshot %q to be deleted after DeleteTagSnapshot", wantCSISnapID)
	}
}
