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
	"os"
	"path/filepath"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func useTempActorsDir(t *testing.T) {
	t.Helper()
	origActors := ateompath.ActorsDir
	ateompath.ActorsDir = filepath.Join(t.TempDir(), "actors")
	t.Cleanup(func() {
		ateompath.ActorsDir = origActors
	})
}

type fakeBlockFS struct {
	OSBlockFileSystem
	formattedDevices map[string]bool
	formatCalls      []string
	formatFSTypes    []string
	fsyncPaths       []string
}

func newFakeBlockFS() *fakeBlockFS {
	return &fakeBlockFS{
		formattedDevices: make(map[string]bool),
	}
}

func (f *fakeBlockFS) IsFormatted(_ context.Context, devicePath string) (bool, error) {
	return f.formattedDevices[devicePath], nil
}

func (f *fakeBlockFS) FormatFilesystem(_ context.Context, devicePath string, fsType string) error {
	f.formatCalls = append(f.formatCalls, devicePath)
	f.formatFSTypes = append(f.formatFSTypes, fsType)
	f.formattedDevices[devicePath] = true
	return nil
}

func (f *fakeBlockFS) Fsync(path string) error {
	f.fsyncPaths = append(f.fsyncPaths, path)
	return nil
}

type recordingWorkerVolumePlugin struct {
	*volume.MockVolumePlugin
	mountedVolumes   map[string]string
	unmountedVolumes map[string]string
}

func newRecordingWorkerVolumePlugin() *recordingWorkerVolumePlugin {
	m := volume.NewMockVolumePlugin()
	return &recordingWorkerVolumePlugin{
		MockVolumePlugin: m,
		mountedVolumes:   make(map[string]string),
		unmountedVolumes: make(map[string]string),
	}
}

func (r *recordingWorkerVolumePlugin) MountVolume(ctx context.Context, volumeID string, targetPath string, volumeContext map[string]string) error {
	r.mountedVolumes[volumeID] = targetPath
	return nil
}

func (r *recordingWorkerVolumePlugin) UnmountVolume(ctx context.Context, volumeID string, targetPath string) error {
	r.unmountedVolumes[volumeID] = targetPath
	return nil
}

func TestBlockWorkerPlane_PrepareSnapshotStorage(t *testing.T) {
	ctx := t.Context()
	useTempActorsDir(t)
	fs := newFakeBlockFS()
	volPlugin := newRecordingWorkerVolumePlugin()

	wp := NewBlockSnapshotPluginWorkerPlane(
		volPlugin,
		WithFileSystem(fs),
	)

	storage := &ateapipb.SnapshotStorage{
		BlockVolume: &ateapipb.ExternalVolume{
			VolumeName:      DefaultSnapshotVolumeName,
			StorageVolumeId: "vol-run-1",
			VolumeType:      "pd.csi.storage.gke.io",
			VolumeContext: map[string]string{
				DevicePathContextKey: "/dev/disk/by-id/google-vol-run-1",
			},
		},
	}

	if err := wp.PrepareSnapshotStorage(ctx, "actor-1", storage); err != nil {
		t.Fatalf("PrepareSnapshotStorage() error = %v", err)
	}
	expectedMount := ateompath.LocalCheckpointsDir("actor-1")
	if got := volPlugin.mountedVolumes["vol-run-1"]; got != expectedMount {
		t.Errorf("mounted targetPath = %q, want %q", got, expectedMount)
	}
}

func TestBlockWorkerPlane_RestoreLifecycle(t *testing.T) {
	ctx := t.Context()
	useTempActorsDir(t)
	fs := newFakeBlockFS()
	volPlugin := newRecordingWorkerVolumePlugin()

	wp := NewBlockSnapshotPluginWorkerPlane(
		volPlugin,
		WithFileSystem(fs),
	)

	actorUID := "actor-restore-1"
	snapID := "snap-restored-1"
	snapDir := ateompath.LocalSnapshotDir(actorUID, snapID)
	if err := os.MkdirAll(snapDir, 0o700); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	manifestBytes := []byte(`{"sandboxClass":"gvisor","pauseImage":"pause:latest"}`)
	if err := os.WriteFile(filepath.Join(snapDir, sandboxManifestName), manifestBytes, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	storage := &ateapipb.SnapshotStorage{
		BlockVolume: &ateapipb.ExternalVolume{
			VolumeName:      DefaultSnapshotVolumeName,
			StorageVolumeId: "vol-restore-1",
			VolumeType:      "pd.csi.storage.gke.io",
		},
	}
	snap := &ateapipb.Snapshot{
		SnapshotId: snapID,
		Block:      &ateapipb.BlockSnapshot{},
	}
	req := &ateletpb.RestoreRequest{
		ActorUid:        actorUID,
		Snapshot:        snap,
		SnapshotStorage: storage,
	}

	// FetchRestoreManifests fails if PrepareSnapshotStorage was not called first.
	if _, _, err := wp.FetchRestoreManifests(ctx, req); err == nil {
		t.Fatalf("expected FetchRestoreManifests() to fail when volume is not mounted")
	}

	if err := wp.PrepareSnapshotStorage(ctx, actorUID, storage); err != nil {
		t.Fatalf("PrepareSnapshotStorage() error = %v", err)
	}

	actorManifest, goldenManifest, err := wp.FetchRestoreManifests(ctx, req)
	if err != nil {
		t.Fatalf("FetchRestoreManifests() error = %v", err)
	}
	if string(actorManifest) != string(manifestBytes) {
		t.Errorf("actorManifest = %q, want %q", string(actorManifest), string(manifestBytes))
	}
	if goldenManifest != nil {
		t.Errorf("goldenManifest = %v, want nil", goldenManifest)
	}

	restoreDir, err := wp.PrepareRestoreDir(ctx, req)
	if err != nil {
		t.Fatalf("PrepareRestoreDir() error = %v", err)
	}
	if restoreDir != snapDir {
		t.Errorf("PrepareRestoreDir() = %q, want %q", restoreDir, snapDir)
	}
	expectedMount := ateompath.LocalCheckpointsDir(actorUID)
	if got := volPlugin.mountedVolumes["vol-restore-1"]; got != expectedMount {
		t.Errorf("mounted targetPath = %q, want %q", got, expectedMount)
	}
}

func TestBlockWorkerPlane_CheckpointCommitAndEscalate(t *testing.T) {
	ctx := t.Context()
	useTempActorsDir(t)
	fs := newFakeBlockFS()
	volPlugin := newRecordingWorkerVolumePlugin()

	var currentRung ateapipb.SurvivabilityRung
	wp := NewBlockSnapshotPluginWorkerPlane(
		volPlugin,
		WithFileSystem(fs),
		WithCheckpointMetaExtractor(func(req *ateletpb.CheckpointRequest) CheckpointRequestMetadata {
			meta := defaultCheckpointMeta(req)
			if currentRung != ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED {
				meta.Rung = currentRung
			}
			return meta
		}),
	)

	actorUID := "actor-ckpt-1"
	mountPoint := ateompath.LocalCheckpointsDir(actorUID)

	// Pre-populate lost+found and an older checkpoint directory on the mounted block volume.
	lostAndFound := filepath.Join(mountPoint, lostAndFoundDirName)
	oldSnapDir := ateompath.LocalSnapshotDir(actorUID, "snap-old")
	for _, dir := range []string{lostAndFound, oldSnapDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatalf("MkdirAll(%q) error = %v", dir, err)
		}
	}

	storage := &ateapipb.SnapshotStorage{
		BlockVolume: &ateapipb.ExternalVolume{
			VolumeName:      DefaultSnapshotVolumeName,
			StorageVolumeId: "vol-ckpt-1",
			VolumeType:      "pd.csi.storage.gke.io",
		},
	}
	req := &ateletpb.CheckpointRequest{
		ActorUid: actorUID,
		Snapshot: &ateapipb.Snapshot{
			SnapshotId:    "snap-new",
			Survivability: ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_LOCAL,
			Block:         &ateapipb.BlockSnapshot{},
		},
		SnapshotStorage: storage,
	}

	// PrepareCheckpointDir fails if volume was not mounted earlier by PrepareSnapshotStorage.
	if _, err := wp.PrepareCheckpointDir(ctx, req); err == nil {
		t.Fatalf("expected PrepareCheckpointDir() to fail when volume is not mounted")
	}

	if err := wp.PrepareSnapshotStorage(ctx, actorUID, storage); err != nil {
		t.Fatalf("PrepareSnapshotStorage() error = %v", err)
	}

	writeDir, err := wp.PrepareCheckpointDir(ctx, req)
	if err != nil {
		t.Fatalf("PrepareCheckpointDir() error = %v", err)
	}
	expectedWriteDir := ateompath.LocalSnapshotDir(actorUID, "snap-new")
	if writeDir != expectedWriteDir {
		t.Errorf("PrepareCheckpointDir() = %q, want %q", writeDir, expectedWriteDir)
	}

	// 1. RESIDENT commit returns immediately without fsync or pruning old checkpoints.
	currentRung = ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_RESIDENT
	rung, err := wp.CommitCheckpoint(ctx, req, writeDir)
	if err != nil {
		t.Fatalf("CommitCheckpoint(RESIDENT) error = %v", err)
	}
	if rung != ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_RESIDENT {
		t.Errorf("CommitCheckpoint(RESIDENT) = %v, want RESIDENT", rung)
	}
	if len(fs.fsyncPaths) != 0 {
		t.Errorf("expected no fsync calls for RESIDENT commit, got %v", fs.fsyncPaths)
	}
	if _, err := os.Stat(oldSnapDir); err != nil {
		t.Errorf("expected old snapshot dir to remain before LOCAL/DURABLE flush, stat err = %v", err)
	}

	// 2. EscalateCheckpoint from RESIDENT to DURABLE fsyncs the mount, prunes old checkpoint, and preserves lost+found.
	currentRung = ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE
	rung, err = wp.EscalateCheckpoint(ctx, req)
	if err != nil {
		t.Fatalf("EscalateCheckpoint(DURABLE) error = %v", err)
	}
	if rung != ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE {
		t.Errorf("EscalateCheckpoint(DURABLE) = %v, want DURABLE", rung)
	}
	if _, err := os.Stat(oldSnapDir); !os.IsNotExist(err) {
		t.Errorf("expected old snapshot directory %q to be pruned, stat err = %v", oldSnapDir, err)
	}
	if _, err := os.Stat(expectedWriteDir); err != nil {
		t.Errorf("expected active snapshot directory %q to be preserved, stat err = %v", expectedWriteDir, err)
	}
	if _, err := os.Stat(lostAndFound); err != nil {
		t.Errorf("expected lost+found directory %q to be preserved, stat err = %v", lostAndFound, err)
	}
	if len(fs.fsyncPaths) != 1 || fs.fsyncPaths[0] != mountPoint {
		t.Errorf("fsyncPaths = %v, want [%s]", fs.fsyncPaths, mountPoint)
	}

	// 3. Direct CommitCheckpoint at LOCAL fsyncs the mount and prunes previous checkpoint.
	req2 := &ateletpb.CheckpointRequest{
		ActorUid: actorUID,
		Snapshot: &ateapipb.Snapshot{
			SnapshotId:    "snap-newest",
			Survivability: ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_LOCAL,
			Block:         &ateapipb.BlockSnapshot{},
		},
		SnapshotStorage: storage,
	}
	writeDir2, err := wp.PrepareCheckpointDir(ctx, req2)
	if err != nil {
		t.Fatalf("PrepareCheckpointDir(snap-newest) error = %v", err)
	}
	currentRung = ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_LOCAL
	rung, err = wp.CommitCheckpoint(ctx, req2, writeDir2)
	if err != nil {
		t.Fatalf("CommitCheckpoint(LOCAL) error = %v", err)
	}
	if rung != ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_LOCAL {
		t.Errorf("CommitCheckpoint(LOCAL) = %v, want LOCAL", rung)
	}
	if _, err := os.Stat(expectedWriteDir); !os.IsNotExist(err) {
		t.Errorf("expected superseded snapshot %q to be deleted", expectedWriteDir)
	}
	if _, err := os.Stat(writeDir2); err != nil {
		t.Errorf("expected newest snapshot %q to exist, stat err = %v", writeDir2, err)
	}
	if len(fs.fsyncPaths) != 2 {
		t.Errorf("expected 2 total fsync calls, got %d", len(fs.fsyncPaths))
	}

	// 4. DetachCheckpointDir unmounts the block volume from the local checkpoint directory.
	if err := wp.DetachCheckpointDir(ctx, actorUID, "vol-ckpt-1"); err != nil {
		t.Fatalf("DetachCheckpointDir() error = %v", err)
	}
	if got := volPlugin.unmountedVolumes["vol-ckpt-1"]; got != mountPoint {
		t.Errorf("unmounted targetPath = %q, want %q", got, mountPoint)
	}
}

func TestBlockWorkerPlane_RestoreDataOnGolden(t *testing.T) {
	ctx := t.Context()
	useTempActorsDir(t)
	fs := newFakeBlockFS()
	volPlugin := newRecordingWorkerVolumePlugin()

	wp := NewBlockSnapshotPluginWorkerPlane(
		volPlugin,
		WithFileSystem(fs),
	)

	actorUID := "actor-data-on-golden"
	goldenSnapID := "snap-golden-1"
	dataSnapID := "snap-data-2"
	goldenDir := ateompath.LocalSnapshotDir(actorUID, goldenSnapID)
	dataDir := ateompath.LocalSnapshotDir(actorUID, dataSnapID)
	for _, dir := range []string{goldenDir, dataDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatalf("MkdirAll(%q) error = %v", dir, err)
		}
	}

	goldenManifestBytes := []byte(`{"sandboxClass":"gvisor","pauseImage":"pause:golden","atespace":"ate-golden","snapshotFiles":["checkpoint.img","pages.img"],"scope":"full"}`)
	if err := os.WriteFile(filepath.Join(goldenDir, sandboxManifestName), goldenManifestBytes, 0o600); err != nil {
		t.Fatalf("WriteFile(golden manifest) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(goldenDir, "checkpoint.img"), []byte("golden-ckpt"), 0o600); err != nil {
		t.Fatalf("WriteFile(checkpoint.img) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(goldenDir, "pages.img"), []byte("golden-pages"), 0o600); err != nil {
		t.Fatalf("WriteFile(pages.img) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(goldenDir, "durable-dir.tar"), []byte("golden-durable-data"), 0o600); err != nil {
		t.Fatalf("WriteFile(golden durable-dir.tar) error = %v", err)
	}

	dataManifestBytes := []byte(`{"sandboxClass":"gvisor","pauseImage":"pause:actor","snapshotFiles":["durable-dir.tar"],"scope":"data"}`)
	if err := os.WriteFile(filepath.Join(dataDir, sandboxManifestName), dataManifestBytes, 0o600); err != nil {
		t.Fatalf("WriteFile(data manifest) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "durable-dir.tar"), []byte("actor-durable-data"), 0o600); err != nil {
		t.Fatalf("WriteFile(actor durable-dir.tar) error = %v", err)
	}

	storage := &ateapipb.SnapshotStorage{
		BlockVolume: &ateapipb.ExternalVolume{
			VolumeName:      DefaultSnapshotVolumeName,
			StorageVolumeId: "vol-dog-1",
			VolumeType:      "hostpath.csi.k8s.io",
		},
	}
	snap := &ateapipb.Snapshot{
		SnapshotId:    dataSnapID,
		Survivability: ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE,
		ContentScope:  ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
		Block:         &ateapipb.BlockSnapshot{},
	}
	req := &ateletpb.RestoreRequest{
		ActorUid:          actorUID,
		Scope:             ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN,
		GoldenSnapshotUri: goldenSnapID,
		Snapshot:          snap,
		SnapshotStorage:   storage,
	}

	if err := wp.PrepareSnapshotStorage(ctx, actorUID, storage); err != nil {
		t.Fatalf("PrepareSnapshotStorage() error = %v", err)
	}

	actorManifest, goldenManifest, err := wp.FetchRestoreManifests(ctx, req)
	if err != nil {
		t.Fatalf("FetchRestoreManifests() error = %v", err)
	}
	if string(actorManifest) != string(dataManifestBytes) {
		t.Errorf("actorManifest = %q, want %q", string(actorManifest), string(dataManifestBytes))
	}
	if string(goldenManifest) != string(goldenManifestBytes) {
		t.Errorf("goldenManifest = %q, want %q", string(goldenManifest), string(goldenManifestBytes))
	}

	restoreDir, err := wp.PrepareRestoreDir(ctx, req)
	if err != nil {
		t.Fatalf("PrepareRestoreDir() error = %v", err)
	}
	if restoreDir != dataDir {
		t.Errorf("PrepareRestoreDir() = %q, want %q", restoreDir, dataDir)
	}
	if got, err := os.ReadFile(filepath.Join(dataDir, "checkpoint.img")); err != nil || string(got) != "golden-ckpt" {
		t.Errorf("staged checkpoint.img = %q (err=%v), want %q", string(got), err, "golden-ckpt")
	}
	if got, err := os.ReadFile(filepath.Join(dataDir, "durable-dir.tar")); err != nil || string(got) != "actor-durable-data" {
		t.Errorf("actor durable-dir.tar = %q (err=%v), want %q", string(got), err, "actor-durable-data")
	}
}

func TestBlockWorkerPlane_EscalatePromoteAndPreserveGolden(t *testing.T) {
	ctx := t.Context()
	useTempActorsDir(t)
	fs := newFakeBlockFS()
	volPlugin := newRecordingWorkerVolumePlugin()

	wp := NewBlockSnapshotPluginWorkerPlane(
		volPlugin,
		WithFileSystem(fs),
	)

	actorUID := "actor-escalate"
	goldenSnapID := "snap-golden-1"
	localSnapID := "snap-local-full"
	durableSnapID := "snap-durable-data"
	mountPoint := ateompath.LocalCheckpointsDir(actorUID)
	goldenDir := ateompath.LocalSnapshotDir(actorUID, goldenSnapID)
	localDir := ateompath.LocalSnapshotDir(actorUID, localSnapID)
	for _, dir := range []string{goldenDir, localDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatalf("MkdirAll(%q) error = %v", dir, err)
		}
	}

	goldenManifest := []byte(`{"sandboxClass":"gvisor","pauseImage":"pause:golden","atespace":"ate-golden","snapshotFiles":["checkpoint.img"],"scope":"full"}`)
	if err := os.WriteFile(filepath.Join(goldenDir, sandboxManifestName), goldenManifest, 0o600); err != nil {
		t.Fatalf("WriteFile(golden manifest) error = %v", err)
	}

	localManifest := []byte(`{"sandboxClass":"gvisor","pauseImage":"pause:latest","snapshotFiles":["durable-dir.tar"],"scope":"data"}`)
	if err := os.WriteFile(filepath.Join(localDir, sandboxManifestName), localManifest, 0o600); err != nil {
		t.Fatalf("WriteFile(manifest) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(localDir, "durable-dir.tar"), []byte("durable"), 0o600); err != nil {
		t.Fatalf("WriteFile(durable-dir.tar) error = %v", err)
	}

	req := &ateletpb.CheckpointRequest{
		ActorUid: actorUID,
		Atespace: "team-a",
		Type:     ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
		Scope:    ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
		Snapshot: &ateapipb.Snapshot{
			SnapshotId:    durableSnapID,
			Survivability: ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE,
			Block:         &ateapipb.BlockSnapshot{},
		},
		SnapshotStorage: &ateapipb.SnapshotStorage{
			BlockVolume: &ateapipb.ExternalVolume{
				VolumeName:      DefaultSnapshotVolumeName,
				StorageVolumeId: "vol-escalate-1",
				VolumeType:      "hostpath.csi.k8s.io",
			},
		},
	}

	escalateCtx := WithLocalSnapshotName(ctx, localSnapID)
	rung, err := wp.EscalateCheckpoint(escalateCtx, req)
	if err != nil {
		t.Fatalf("EscalateCheckpoint() error = %v", err)
	}
	if rung != ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE {
		t.Errorf("EscalateCheckpoint() rung = %v, want DURABLE", rung)
	}

	durableDir := filepath.Join(mountPoint, durableSnapID)
	if _, err := os.Stat(durableDir); err != nil {
		t.Fatalf("expected promoted durable dir %q to exist: %v", durableDir, err)
	}
	if _, err := os.Stat(goldenDir); err != nil {
		t.Errorf("expected golden checkpoint dir %q to be preserved during prune: %v", goldenDir, err)
	}
}
