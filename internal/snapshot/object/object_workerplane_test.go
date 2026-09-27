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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/snapshot/object/ategcs"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

type recordingObjectStorage struct {
	mu      sync.Mutex
	objects map[string][]byte
	putErr  error
}

func newRecordingObjectStorage() *recordingObjectStorage {
	return &recordingObjectStorage{objects: make(map[string][]byte)}
}

func (r *recordingObjectStorage) GetObject(_ context.Context, bucket, object string) (io.ReadCloser, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.objects[bucket+"/"+object]
	if !ok {
		return nil, fmt.Errorf("%w: Bucket:%q, Object:%q", ategcs.ErrObjectNotFound, bucket, object)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (r *recordingObjectStorage) PutObject(_ context.Context, bucket, object string, reader io.Reader) error {
	if r.putErr != nil {
		return r.putErr
	}
	b, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.objects == nil {
		r.objects = make(map[string][]byte)
	}
	r.objects[bucket+"/"+object] = b
	return nil
}

func (r *recordingObjectStorage) keys() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := make([]string, 0, len(r.objects))
	for k := range r.objects {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

type trackingFS struct {
	OSFileSystem
	mu         sync.Mutex
	syncedDirs []string
}

func (t *trackingFS) Syncfs(dirPath string) error {
	t.mu.Lock()
	t.syncedDirs = append(t.syncedDirs, dirPath)
	t.mu.Unlock()
	return t.OSFileSystem.Syncfs(dirPath)
}

func TestWorkerPlaneLifecycle(t *testing.T) {
	ctx := t.Context()
	baseDir := t.TempDir()
	store := newRecordingObjectStorage()
	fs := &trackingFS{}

	var currentMeta CheckpointRequestMetadata
	var currentRestoreMeta RestoreRequestMetadata

	plugin := NewObjectSnapshotPluginWorkerPlane(
		store,
		WithBaseDir(baseDir),
		WithFileSystem(fs),
		WithCheckpointMetaExtractor(func(*ateletpb.CheckpointRequest) CheckpointRequestMetadata {
			return currentMeta
		}),
		WithRestoreMetaExtractor(func(*ateletpb.RestoreRequest) RestoreRequestMetadata {
			return currentRestoreMeta
		}),
	)

	actorUID := "actor-123"
	snapshotID := "snap-abc"
	remoteURI := "gs://bucket/root/atespaces/ns/actors/actor-123/snapshots/snap-abc"
	remotePrefix := "bucket/root/atespaces/ns/actors/actor-123/snapshots/snap-abc"

	if err := plugin.PrepareSnapshotStorage(ctx, actorUID, nil); err != nil {
		t.Fatalf("PrepareSnapshotStorage() error = %v", err)
	}

	// 1. PrepareCheckpointDir creates the local checkpoint directory.
	currentMeta = CheckpointRequestMetadata{
		SnapshotID:        snapshotID,
		Rung:              ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_RESIDENT,
		ObjectSnapshotURI: remoteURI,
	}
	ckptReq := &ateletpb.CheckpointRequest{ActorUid: actorUID}
	writeDir, err := plugin.PrepareCheckpointDir(ctx, ckptReq)
	if err != nil {
		t.Fatalf("PrepareCheckpointDir() error = %v", err)
	}
	wantWriteDir := filepath.Join(baseDir, "actors", actorUID, "local-checkpoint", snapshotID)
	if writeDir != wantWriteDir {
		t.Fatalf("PrepareCheckpointDir() = %q, want %q", writeDir, wantWriteDir)
	}

	// Populate checkpoint files, rootfs-delta, and volumes on local disk.
	if err := os.WriteFile(filepath.Join(writeDir, "checkpoint.img"), []byte("memory-pages"), 0o600); err != nil {
		t.Fatal(err)
	}
	rootfsDelta := filepath.Join(baseDir, "actors", actorUID, "rootfs-delta")
	if err := os.MkdirAll(rootfsDelta, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootfsDelta, "upper.txt"), []byte("delta-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	volumes := filepath.Join(baseDir, "actors", actorUID, "volumes")
	if err := os.MkdirAll(volumes, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(volumes, "vol.txt"), []byte("volume-data"), 0o600); err != nil {
		t.Fatal(err)
	}

	// 2. CommitCheckpoint at RESIDENT returns immediately without syncing or uploading.
	rung, err := plugin.CommitCheckpoint(ctx, ckptReq, writeDir)
	if err != nil {
		t.Fatalf("CommitCheckpoint(RESIDENT) error = %v", err)
	}
	if rung != ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_RESIDENT {
		t.Errorf("CommitCheckpoint(RESIDENT) rung = %v, want RESIDENT", rung)
	}
	if len(fs.syncedDirs) != 0 {
		t.Errorf("expected 0 syncfs calls for RESIDENT, got %d", len(fs.syncedDirs))
	}

	// 3. EscalateCheckpoint to LOCAL invokes syncfs on the checkpoint directory.
	currentMeta.Rung = ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_LOCAL
	rung, err = plugin.EscalateCheckpoint(ctx, ckptReq)
	if err != nil {
		t.Fatalf("EscalateCheckpoint(LOCAL) error = %v", err)
	}
	if rung != ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_LOCAL {
		t.Errorf("EscalateCheckpoint(LOCAL) rung = %v, want LOCAL", rung)
	}
	if len(fs.syncedDirs) != 1 || fs.syncedDirs[0] != wantWriteDir {
		t.Errorf("syncedDirs = %v, want [%s]", fs.syncedDirs, wantWriteDir)
	}

	// 4. EscalateCheckpoint to DURABLE packages and uploads checkpoint, rootfs-delta, and volumes via ategcs.
	currentMeta.Rung = ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE
	rung, err = plugin.EscalateCheckpoint(ctx, ckptReq)
	if err != nil {
		t.Fatalf("EscalateCheckpoint(DURABLE) error = %v", err)
	}
	if rung != ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE {
		t.Errorf("EscalateCheckpoint(DURABLE) rung = %v, want DURABLE", rung)
	}
	for _, archive := range []string{checkpointArchiveName, rootfsDeltaArchiveName, volumesArchiveName} {
		key := remotePrefix + "/" + archive + ".zstd"
		if _, ok := store.objects[key]; !ok {
			t.Errorf("expected uploaded zstd archive at %q, got keys %v", key, store.keys())
		}
	}

	// 5. PrepareRestoreDir cache hit returns existing local checkpoint directory without downloading.
	currentRestoreMeta = RestoreRequestMetadata{
		SnapshotID:        snapshotID,
		ObjectSnapshotURI: remoteURI,
	}
	restoreDir, err := plugin.PrepareRestoreDir(ctx, &ateletpb.RestoreRequest{ActorUid: actorUID})
	if err != nil {
		t.Fatalf("PrepareRestoreDir(cache hit) error = %v", err)
	}
	if restoreDir != wantWriteDir {
		t.Errorf("PrepareRestoreDir(cache hit) = %q, want %q", restoreDir, wantWriteDir)
	}

	// 6. DetachCheckpointDir prunes the local-checkpoint directory.
	if err := plugin.DetachCheckpointDir(ctx, actorUID, snapshotID); err != nil {
		t.Fatalf("DetachCheckpointDir() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(baseDir, "actors", actorUID, "local-checkpoint")); !os.IsNotExist(err) {
		t.Fatalf("expected local-checkpoint directory to be removed, stat err = %v", err)
	}

	// Also remove rootfs-delta and volumes to verify full extraction on cache miss.
	_ = os.RemoveAll(rootfsDelta)
	_ = os.RemoveAll(volumes)

	// 7. PrepareRestoreDir cache miss downloads and extracts from object storage via ategcs.
	restoreDir, err = plugin.PrepareRestoreDir(ctx, &ateletpb.RestoreRequest{ActorUid: actorUID})
	if err != nil {
		t.Fatalf("PrepareRestoreDir(cache miss) error = %v", err)
	}
	if restoreDir != wantWriteDir {
		t.Errorf("PrepareRestoreDir(cache miss) = %q, want %q", restoreDir, wantWriteDir)
	}
	if got, err := os.ReadFile(filepath.Join(restoreDir, "checkpoint.img")); err != nil || string(got) != "memory-pages" {
		t.Errorf("restored checkpoint.img = %q (err=%v), want %q", got, err, "memory-pages")
	}
	if got, err := os.ReadFile(filepath.Join(rootfsDelta, "upper.txt")); err != nil || string(got) != "delta-data" {
		t.Errorf("restored upper.txt = %q (err=%v), want %q", got, err, "delta-data")
	}
	if got, err := os.ReadFile(filepath.Join(volumes, "vol.txt")); err != nil || string(got) != "volume-data" {
		t.Errorf("restored vol.txt = %q (err=%v), want %q", got, err, "volume-data")
	}
}

func TestWorkerPlaneManifestUploadAndRestore(t *testing.T) {
	ctx := t.Context()
	baseDir := t.TempDir()
	store := newRecordingObjectStorage()

	plugin := NewObjectSnapshotPluginWorkerPlane(
		store,
		WithBaseDir(baseDir),
	)

	actorUID := "actor-manifest-1"
	snapshotID := "snap-m1"
	remoteURI := "gs://bucket/root/atespaces/ns/actors/actor-manifest-1/snapshots/snap-m1"
	remotePrefix := "bucket/root/atespaces/ns/actors/actor-manifest-1/snapshots/snap-m1"

	ckptReq := &ateletpb.CheckpointRequest{
		ActorUid: actorUID,
		Type:     ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL,
		Scope:    ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		Config:   &ateletpb.CheckpointRequest_LocalConfig{LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotName: snapshotID}},
	}

	writeDir, err := plugin.PrepareCheckpointDir(ctx, ckptReq)
	if err != nil {
		t.Fatalf("PrepareCheckpointDir() error = %v", err)
	}

	rec := sandboxAssetsRecord{
		SandboxClass:  "microvm",
		PauseImage:    "registry.local/pause:latest",
		SnapshotFiles: []string{"config.json", "memory-ranges", ateompath.DurableDirTarFile},
		Scope:         ateattr.SnapshotScopeFull,
	}
	manifestBytes, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		"config.json":               "cfg-data",
		"memory-ranges":             "mem-data",
		ateompath.DurableDirTarFile: "durable-tar-data",
		sandboxManifestName:         string(manifestBytes),
	} {
		if err := os.WriteFile(filepath.Join(writeDir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	// Escalate FULL local checkpoint to DURABLE with DATA scope narrowing.
	escalateReq := &ateletpb.CheckpointRequest{
		ActorUid: actorUID,
		Type:     ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
		Scope:    ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
		Config: &ateletpb.CheckpointRequest_ExternalConfig{
			ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{SnapshotUri: remoteURI},
		},
	}
	rung, err := plugin.EscalateCheckpoint(ctx, escalateReq)
	if err != nil {
		t.Fatalf("EscalateCheckpoint(DURABLE, DATA) error = %v", err)
	}
	if rung != ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE {
		t.Errorf("rung = %v, want DURABLE", rung)
	}

	wantKeys := []string{
		remotePrefix + "/" + ateompath.DurableDirTarFile + ".zstd",
		remotePrefix + "/" + sandboxManifestName,
	}
	if got := store.keys(); !slices.Equal(got, wantKeys) {
		t.Fatalf("uploaded keys = %v, want %v", got, wantKeys)
	}

	// Prune local checkpoint and verify idempotent retry of EscalateCheckpoint succeeds.
	if err := plugin.DetachCheckpointDir(ctx, actorUID, snapshotID); err != nil {
		t.Fatalf("DetachCheckpointDir() error = %v", err)
	}
	if _, err := plugin.EscalateCheckpoint(ctx, escalateReq); err != nil {
		t.Fatalf("EscalateCheckpoint(already uploaded) error = %v", err)
	}

	// Restore from the manifest-backed snapshot on cache miss.
	goldenURI := "gs://bucket/golden/atespaces/ate-golden/actors/golden-1/snapshots/golden-snap-1"
	goldenPrefix := "bucket/golden/atespaces/ate-golden/actors/golden-1/snapshots/golden-snap-1"
	goldenManifestBytes := []byte(`{"sandboxClass":"microvm","pauseImage":"registry.local/pause:latest","snapshotFiles":["config.json","memory-ranges"]}`)
	store.objects[goldenPrefix+"/"+sandboxManifestName] = goldenManifestBytes

	restoreReq := &ateletpb.RestoreRequest{
		ActorUid: actorUID,
		Type:     ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
		Config: &ateletpb.RestoreRequest_ExternalConfig{
			ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{SnapshotUri: remoteURI},
		},
	}
	gotActorManifest, gotGoldenManifest, err := plugin.FetchRestoreManifests(ctx, restoreReq)
	if err != nil {
		t.Fatalf("FetchRestoreManifests() error = %v", err)
	}
	if len(gotActorManifest) == 0 {
		t.Errorf("FetchRestoreManifests() returned empty actor manifest")
	}
	if len(gotGoldenManifest) != 0 {
		t.Errorf("FetchRestoreManifests() returned unexpected golden manifest: %s", gotGoldenManifest)
	}

	dataOnGoldenReq := &ateletpb.RestoreRequest{
		ActorUid:          actorUID,
		Type:              ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
		Scope:             ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN,
		GoldenSnapshotUri: goldenURI,
		Config: &ateletpb.RestoreRequest_ExternalConfig{
			ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{SnapshotUri: remoteURI},
		},
	}
	_, gotGoldenManifest, err = plugin.FetchRestoreManifests(ctx, dataOnGoldenReq)
	if err != nil {
		t.Fatalf("FetchRestoreManifests(DATA_ON_GOLDEN) error = %v", err)
	}
	if !bytes.Equal(gotGoldenManifest, goldenManifestBytes) {
		t.Errorf("FetchRestoreManifests(DATA_ON_GOLDEN) golden = %s, want %s", gotGoldenManifest, goldenManifestBytes)
	}

	restoredDir, err := plugin.PrepareRestoreDir(ctx, restoreReq)
	if err != nil {
		t.Fatalf("PrepareRestoreDir() error = %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(restoredDir, ateompath.DurableDirTarFile)); err != nil || string(got) != "durable-tar-data" {
		t.Errorf("restored durable-dir.tar = %q (err=%v), want %q", got, err, "durable-tar-data")
	}
}
