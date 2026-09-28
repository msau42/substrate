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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/snapshot"
	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	sandboxManifestName = "manifest.json"
	lostAndFoundDirName = "lost+found"

	// DevicePathContextKey is an optional VolumeContext key specifying the raw
	// block device path on the worker node for formatting checks.
	DevicePathContextKey = "devicePath"

	// FSTypeContextKey is an optional VolumeContext key specifying the filesystem
	// type (e.g. "ext4") to format onto an unformatted block volume.
	FSTypeContextKey = "fsType"
)

// WorkerVolumePluginRegistry resolves a worker-plane VolumePlugin by CSI driver name.
type WorkerVolumePluginRegistry interface {
	GetPlugin(ctx context.Context, driverName string) (volume.VolumePluginWorkerPlane, error)
}

// VolumeMetadata carries block volume attachment details on the worker node.
type VolumeMetadata struct {
	StorageVolumeID string
	VolumeType      string
	DevicePath      string
	FSType          string
	VolumeContext   map[string]string
}

type volumeMetadataContextKey struct{}

// WithVolumeMetadata attaches VolumeMetadata to ctx for worker-plane requests.
func WithVolumeMetadata(ctx context.Context, meta VolumeMetadata) context.Context {
	return context.WithValue(ctx, volumeMetadataContextKey{}, meta)
}

// VolumeMetadataFromContext retrieves VolumeMetadata from ctx, if present.
func VolumeMetadataFromContext(ctx context.Context) (VolumeMetadata, bool) {
	meta, ok := ctx.Value(volumeMetadataContextKey{}).(VolumeMetadata)
	return meta, ok
}

type localSnapshotNameContextKey struct{}

// WithLocalSnapshotName attaches the local snapshot directory name to ctx for
// EscalateCheckpoint.
func WithLocalSnapshotName(ctx context.Context, snapshotName string) context.Context {
	return context.WithValue(ctx, localSnapshotNameContextKey{}, snapshotName)
}

// LocalSnapshotNameFromContext returns the local snapshot name stored in ctx, if any.
func LocalSnapshotNameFromContext(ctx context.Context) string {
	v, _ := ctx.Value(localSnapshotNameContextKey{}).(string)
	return v
}

// RestoreRequestMetadata extracts snapshot and block volume parameters from a
// RestoreRequest.
type RestoreRequestMetadata struct {
	SnapshotID      string
	StorageVolumeID string
	VolumeType      string
	DevicePath      string
	VolumeContext   map[string]string
}

// CheckpointRequestMetadata extracts snapshot and survivability parameters from
// a CheckpointRequest.
type CheckpointRequestMetadata struct {
	SnapshotID      string
	Rung            ateapipb.SurvivabilityRung
	StorageVolumeID string
	VolumeType      string
	VolumeContext   map[string]string
}

type mountedVolumeInfo struct {
	storageVolumeID string
	volumeType      string
}

// BlockSnapshotPluginWorkerPlane implements snapshot.SnapshotPluginWorkerPlane
// for block-backed actor snapshots.
type BlockSnapshotPluginWorkerPlane struct {
	fs        BlockFileSystem
	volPlugin volume.VolumePluginWorkerPlane
	registry  WorkerVolumePluginRegistry

	extractRestoreMeta    func(*ateletpb.RestoreRequest) RestoreRequestMetadata
	extractCheckpointMeta func(*ateletpb.CheckpointRequest) CheckpointRequestMetadata

	mu             sync.RWMutex
	mountedByActor map[string]mountedVolumeInfo
}

var _ snapshot.SnapshotPluginWorkerPlane = (*BlockSnapshotPluginWorkerPlane)(nil)

// WorkerPlaneOption configures BlockSnapshotPluginWorkerPlane.
type WorkerPlaneOption func(*BlockSnapshotPluginWorkerPlane)

// WithFileSystem injects a custom BlockFileSystem implementation.
func WithFileSystem(fs BlockFileSystem) WorkerPlaneOption {
	return func(p *BlockSnapshotPluginWorkerPlane) {
		p.fs = fs
	}
}

// WithWorkerVolumePluginRegistry configures a registry to resolve worker-plane
// VolumePlugins by CSI driver name.
func WithWorkerVolumePluginRegistry(registry WorkerVolumePluginRegistry) WorkerPlaneOption {
	return func(p *BlockSnapshotPluginWorkerPlane) {
		p.registry = registry
	}
}

// WithRestoreMetaExtractor overrides how snapshot and block volume metadata are
// extracted from a RestoreRequest.
func WithRestoreMetaExtractor(fn func(*ateletpb.RestoreRequest) RestoreRequestMetadata) WorkerPlaneOption {
	return func(p *BlockSnapshotPluginWorkerPlane) {
		p.extractRestoreMeta = fn
	}
}

// WithCheckpointMetaExtractor overrides how snapshot ID and target
// survivability rung are extracted from a CheckpointRequest.
func WithCheckpointMetaExtractor(fn func(*ateletpb.CheckpointRequest) CheckpointRequestMetadata) WorkerPlaneOption {
	return func(p *BlockSnapshotPluginWorkerPlane) {
		p.extractCheckpointMeta = fn
	}
}

// NewBlockSnapshotPluginWorkerPlane constructs a new worker-plane block
// snapshot plugin.
func NewBlockSnapshotPluginWorkerPlane(volPlugin volume.VolumePluginWorkerPlane, opts ...WorkerPlaneOption) *BlockSnapshotPluginWorkerPlane {
	p := &BlockSnapshotPluginWorkerPlane{
		fs:                    defaultBlockFileSystem(),
		volPlugin:             volPlugin,
		extractRestoreMeta:    defaultRestoreMeta,
		extractCheckpointMeta: defaultCheckpointMeta,
		mountedByActor:        make(map[string]mountedVolumeInfo),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// PrepareSnapshotStorage mounts the actor's attached block volume at the
// actor's local checkpoint directory (<ActorsDir>/<actorUID>/local-checkpoint).
func (p *BlockSnapshotPluginWorkerPlane) PrepareSnapshotStorage(
	ctx context.Context,
	actorUID string,
	storage *ateapipb.SnapshotStorage,
) error {
	if actorUID == "" {
		return fmt.Errorf("actorUID is required")
	}

	var (
		storageVolumeID string
		volumeType      string
		volumeContext   map[string]string
	)
	if extVol := storage.GetBlockVolume(); extVol != nil {
		storageVolumeID = extVol.GetStorageVolumeId()
		volumeType = extVol.GetVolumeType()
		volumeContext = extVol.GetVolumeContext()
	}
	if ctxMeta, ok := VolumeMetadataFromContext(ctx); ok {
		if storageVolumeID == "" {
			storageVolumeID = ctxMeta.StorageVolumeID
		}
		if volumeType == "" {
			volumeType = ctxMeta.VolumeType
		}
		if len(volumeContext) == 0 && len(ctxMeta.VolumeContext) > 0 {
			volumeContext = ctxMeta.VolumeContext
		}
	}
	if storageVolumeID == "" {
		return fmt.Errorf("snapshot storage is missing block storage volume ID")
	}

	return p.ensureVolumeMounted(ctx, actorUID, storageVolumeID, volumeType, volumeContext)
}

// FetchRestoreManifests reads the snapshot manifest (and golden manifest on
// DATA_ON_GOLDEN) directly from the actor's mounted block filesystem.
func (p *BlockSnapshotPluginWorkerPlane) FetchRestoreManifests(
	ctx context.Context,
	req *ateletpb.RestoreRequest,
) (actorManifest, goldenManifest []byte, err error) {
	if req == nil {
		return nil, nil, fmt.Errorf("restore request is required")
	}
	actorUID := req.GetActorUid()
	if actorUID == "" {
		return nil, nil, fmt.Errorf("restore request is missing actor UID")
	}

	meta := p.resolveRestoreMeta(ctx, actorUID, req)
	if meta.SnapshotID == "" {
		return nil, nil, fmt.Errorf("restore request is missing snapshot ID")
	}
	if meta.StorageVolumeID == "" {
		return nil, nil, fmt.Errorf("restore request is missing block snapshot storage volume ID")
	}
	if err := p.verifyVolumeMounted(actorUID, meta.StorageVolumeID); err != nil {
		return nil, nil, err
	}

	manifestPath := filepath.Join(ateompath.LocalSnapshotDir(actorUID, meta.SnapshotID), sandboxManifestName)
	actorManifest, err = p.fs.ReadFile(manifestPath)
	if err != nil {
		return nil, nil, fmt.Errorf("while reading snapshot manifest %q: %w", manifestPath, err)
	}

	if req.GetScope() == ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN {
		goldenSnapID := p.resolveGoldenCheckpointID(actorUID, req, meta)
		if goldenSnapID != "" {
			goldenManifestPath := filepath.Join(ateompath.LocalSnapshotDir(actorUID, goldenSnapID), sandboxManifestName)
			goldenManifest, err = p.fs.ReadFile(goldenManifestPath)
			if err != nil {
				return nil, nil, fmt.Errorf("while reading golden snapshot manifest %q: %w", goldenManifestPath, err)
			}
		}
	}

	return actorManifest, goldenManifest, nil
}

func (p *BlockSnapshotPluginWorkerPlane) resolveGoldenCheckpointID(
	actorUID string,
	req *ateletpb.RestoreRequest,
	meta RestoreRequestMetadata,
) string {
	goldenSnapID := filepath.Base(req.GetGoldenSnapshotUri())
	if goldenSnapID != "" && goldenSnapID != "." {
		return goldenSnapID
	}
	mountPoint := ateompath.LocalCheckpointsDir(actorUID)
	entries, err := p.fs.ReadDir(mountPoint)
	if err != nil {
		return ""
	}
	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() || name == meta.SnapshotID || name == lostAndFoundDirName ||
			name == ateompath.CheckpointStateDirName || name == ateompath.RestoreStateDirName {
			continue
		}
		if p.isGoldenCheckpointDir(filepath.Join(mountPoint, name)) {
			return name
		}
	}
	return ""
}

// PrepareRestoreDir verifies the actor's block volume is mounted and returns
// the path to the snapshot directory on the mounted volume
// (<ActorsDir>/<actorUID>/local-checkpoint/<snapshotID>).
func (p *BlockSnapshotPluginWorkerPlane) PrepareRestoreDir(
	ctx context.Context,
	req *ateletpb.RestoreRequest,
) (string, error) {
	if req == nil {
		return "", fmt.Errorf("restore request is required")
	}
	actorUID := req.GetActorUid()
	if actorUID == "" {
		return "", fmt.Errorf("restore request is missing actor UID")
	}

	meta := p.resolveRestoreMeta(ctx, actorUID, req)
	if meta.SnapshotID == "" {
		return "", fmt.Errorf("restore request is missing snapshot ID")
	}
	if meta.StorageVolumeID == "" {
		return "", fmt.Errorf("restore request is missing block snapshot storage volume ID")
	}
	if err := p.verifyVolumeMounted(actorUID, meta.StorageVolumeID); err != nil {
		return "", err
	}

	checkpointDir := ateompath.LocalSnapshotDir(actorUID, meta.SnapshotID)
	if info, err := p.fs.Stat(checkpointDir); err != nil {
		return "", fmt.Errorf("while verifying checkpoint directory %q on mounted block volume: %w", checkpointDir, err)
	} else if !info.IsDir() {
		return "", fmt.Errorf("checkpoint path %q on mounted block volume is not a directory", checkpointDir)
	}

	if req.GetScope() == ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN {
		goldenSnapID := p.resolveGoldenCheckpointID(actorUID, req, meta)
		if goldenSnapID != "" && goldenSnapID != meta.SnapshotID {
			if err := p.stageGoldenFilesForRestore(actorUID, goldenSnapID, checkpointDir); err != nil {
				return "", err
			}
		}
	}

	return checkpointDir, nil
}

func (p *BlockSnapshotPluginWorkerPlane) stageGoldenFilesForRestore(
	actorUID string,
	goldenSnapID string,
	actorCheckpointDir string,
) error {
	goldenDir := ateompath.LocalSnapshotDir(actorUID, goldenSnapID)
	entries, err := p.fs.ReadDir(goldenDir)
	if err != nil {
		return fmt.Errorf("while reading golden checkpoint directory %q: %w", goldenDir, err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == sandboxManifestName || name == ateompath.DurableDirTarFile || entry.IsDir() {
			continue
		}
		src := filepath.Join(goldenDir, name)
		dst := filepath.Join(actorCheckpointDir, name)
		_ = p.fs.RemoveAll(dst)
		if err := os.Link(src, dst); err != nil {
			return fmt.Errorf("while linking golden file %q to %q: %w", src, dst, err)
		}
	}
	return nil
}

// PrepareCheckpointDir creates and returns the checkpoint write directory
// (<ActorsDir>/<actorUID>/local-checkpoint/<snapshotID>) inside the mounted
// block volume.
func (p *BlockSnapshotPluginWorkerPlane) PrepareCheckpointDir(
	ctx context.Context,
	req *ateletpb.CheckpointRequest,
) (string, error) {
	if req == nil {
		return "", fmt.Errorf("checkpoint request is required")
	}
	actorUID := req.GetActorUid()
	if actorUID == "" {
		return "", fmt.Errorf("checkpoint request is missing actor UID")
	}
	meta := p.resolveCheckpointMeta(ctx, actorUID, req)
	if meta.SnapshotID == "" {
		return "", fmt.Errorf("checkpoint request is missing snapshot ID")
	}
	if meta.StorageVolumeID == "" {
		return "", fmt.Errorf("checkpoint request is missing block snapshot storage volume ID")
	}
	if err := p.verifyVolumeMounted(actorUID, meta.StorageVolumeID); err != nil {
		return "", err
	}

	checkpointWriteDir := ateompath.LocalSnapshotDir(actorUID, meta.SnapshotID)
	if err := p.fs.MkdirAll(checkpointWriteDir, 0o700); err != nil {
		return "", fmt.Errorf("while creating checkpoint directory %q on block volume: %w", checkpointWriteDir, err)
	}
	return checkpointWriteDir, nil
}

// CommitCheckpoint finalizes a newly written checkpoint on the mounted block
// volume:
//   - RESIDENT: returns immediately, keeping checkpoint pages in the OS page cache.
//   - LOCAL or DURABLE: executes fsync on the mounted volume to flush the OS
//     page cache to the block device, prunes any superseded checkpoint
//     directories from the mount, and returns the target survivability rung.
func (p *BlockSnapshotPluginWorkerPlane) CommitCheckpoint(
	ctx context.Context,
	req *ateletpb.CheckpointRequest,
	checkpointWriteDir string,
) (ateapipb.SurvivabilityRung, error) {
	if req == nil {
		return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED,
			fmt.Errorf("checkpoint request is required")
	}
	actorUID := req.GetActorUid()
	if actorUID == "" {
		return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED,
			fmt.Errorf("checkpoint request is missing actor UID")
	}

	meta := p.resolveCheckpointMeta(ctx, actorUID, req)
	activeSnapshotID := meta.SnapshotID
	if activeSnapshotID == "" && checkpointWriteDir != "" {
		activeSnapshotID = filepath.Base(checkpointWriteDir)
	}

	switch meta.Rung {
	case ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_RESIDENT:
		return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_RESIDENT, nil

	case ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_LOCAL,
		ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE:
		if activeSnapshotID == "" {
			return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED,
				fmt.Errorf("checkpoint request is missing snapshot ID")
		}
		preserveGolden := req.GetAtespace() != resources.GoldenActorAtespace
		return p.commitCheckpointToMount(ctx, actorUID, activeSnapshotID, preserveGolden, meta.Rung)

	default:
		return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED,
			fmt.Errorf("unsupported survivability rung: %v", meta.Rung)
	}
}

// EscalateCheckpoint elevates a RESIDENT checkpoint on the mounted block volume
// to LOCAL or DURABLE by promoting the local checkpoint directory if renamed,
// flushing the OS page cache via fsync, and pruning superseded checkpoints.
func (p *BlockSnapshotPluginWorkerPlane) EscalateCheckpoint(
	ctx context.Context,
	req *ateletpb.CheckpointRequest,
) (ateapipb.SurvivabilityRung, error) {
	if req == nil {
		return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED,
			fmt.Errorf("checkpoint request is required")
	}
	actorUID := req.GetActorUid()
	if actorUID == "" {
		return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED,
			fmt.Errorf("escalate checkpoint request is missing actor UID")
	}

	meta := p.resolveCheckpointMeta(ctx, actorUID, req)
	localSnapID := LocalSnapshotNameFromContext(ctx)
	targetSnapID := meta.SnapshotID
	if targetSnapID == "" {
		targetSnapID = localSnapID
	}
	if targetSnapID == "" {
		return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED,
			fmt.Errorf("escalate checkpoint request is missing snapshot ID")
	}

	switch meta.Rung {
	case ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_LOCAL,
		ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE:
		checkpointDir := ateompath.LocalSnapshotDir(actorUID, targetSnapID)
		if localSnapID != "" && localSnapID != targetSnapID {
			srcDir := ateompath.LocalSnapshotDir(actorUID, localSnapID)
			if _, statErr := p.fs.Stat(checkpointDir); os.IsNotExist(statErr) {
				if err := os.Rename(srcDir, checkpointDir); err != nil {
					return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED,
						fmt.Errorf("while promoting local checkpoint %q to %q: %w", srcDir, checkpointDir, err)
				}
			}
		}
		preserveGolden := req.GetAtespace() != resources.GoldenActorAtespace
		return p.commitCheckpointToMount(ctx, actorUID, targetSnapID, preserveGolden, meta.Rung)

	default:
		return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED,
			fmt.Errorf("unsupported target survivability rung for escalation: %v", meta.Rung)
	}
}

func (p *BlockSnapshotPluginWorkerPlane) commitCheckpointToMount(
	ctx context.Context,
	actorUID string,
	activeSnapshotID string,
	preserveGolden bool,
	rung ateapipb.SurvivabilityRung,
) (ateapipb.SurvivabilityRung, error) {
	checkpointDir := ateompath.LocalSnapshotDir(actorUID, activeSnapshotID)
	if info, err := p.fs.Stat(checkpointDir); err != nil {
		return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED,
			fmt.Errorf("checkpoint directory %q is not available: %w", checkpointDir, err)
	} else if !info.IsDir() {
		return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED,
			fmt.Errorf("checkpoint path %q is not a directory", checkpointDir)
	}

	mountPoint := ateompath.LocalCheckpointsDir(actorUID)
	if err := p.fs.Fsync(mountPoint); err != nil {
		return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED,
			fmt.Errorf("while syncing mounted block volume %q: %w", mountPoint, err)
	}
	if err := p.prunePreviousCheckpoints(ctx, mountPoint, activeSnapshotID, preserveGolden); err != nil {
		return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED, err
	}
	return rung, nil
}

// DetachCheckpointDir unmounts the actor's block volume from the node's
// local checkpoint directory (<ActorsDir>/<actorUID>/local-checkpoint).
func (p *BlockSnapshotPluginWorkerPlane) DetachCheckpointDir(
	ctx context.Context,
	actorUID string,
	storageID string,
) error {
	if actorUID == "" {
		return fmt.Errorf("actorUID is required")
	}

	mounted := p.getMountedVolume(actorUID)
	if storageID == "" {
		storageID = mounted.storageVolumeID
	}
	driverName := mounted.volumeType
	if ctxMeta, ok := VolumeMetadataFromContext(ctx); ok {
		if storageID == "" {
			storageID = ctxMeta.StorageVolumeID
		}
		if driverName == "" {
			driverName = ctxMeta.VolumeType
		}
	}
	if storageID == "" {
		storageID = actorSnapshotVolumeName(actorUID, DefaultSnapshotVolumeName)
	}

	plugin, err := p.getPlugin(ctx, driverName)
	if err != nil {
		return fmt.Errorf("while resolving volume plugin for %q: %w", driverName, err)
	}

	mountPoint := ateompath.LocalCheckpointsDir(actorUID)
	if err := plugin.UnmountVolume(ctx, storageID, mountPoint); err != nil {
		if status.Code(err) == codes.NotFound || errors.Is(err, os.ErrNotExist) {
			slog.WarnContext(ctx, "Block snapshot volume not found during unmount, assuming already unmounted",
				slog.String("volume_id", storageID),
				slog.String("mount_point", mountPoint))
			p.clearMountedVolume(actorUID)
			return nil
		}
		return fmt.Errorf("while unmounting block snapshot volume %q from %q: %w", storageID, mountPoint, err)
	}

	p.clearMountedVolume(actorUID)
	return nil
}

// prunePreviousCheckpoints deletes all previous checkpoint subdirectories under
// checkpointsDir except activeSnapshotID, any golden checkpoint directory when
// preserveGolden is true, checkpoint-state/restore-state directories, and ext4's
// lost+found directory.
func (p *BlockSnapshotPluginWorkerPlane) prunePreviousCheckpoints(
	ctx context.Context,
	checkpointsDir string,
	activeSnapshotID string,
	preserveGolden bool,
) error {
	entries, err := p.fs.ReadDir(checkpointsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("while listing checkpoints in mounted block volume %q: %w", checkpointsDir, err)
	}

	var errs []error
	for _, entry := range entries {
		name := entry.Name()
		if name == activeSnapshotID || name == lostAndFoundDirName ||
			name == ateompath.CheckpointStateDirName || name == ateompath.RestoreStateDirName {
			continue
		}
		path := filepath.Join(checkpointsDir, name)
		if preserveGolden && entry.IsDir() && p.isGoldenCheckpointDir(path) {
			continue
		}
		if err := p.fs.RemoveAll(path); err != nil {
			errs = append(errs, fmt.Errorf("while deleting previous checkpoint %q: %w", path, err))
			continue
		}
		slog.InfoContext(ctx, "Pruned previous checkpoint from block volume", slog.String("path", path))
	}
	return errors.Join(errs...)
}

func (p *BlockSnapshotPluginWorkerPlane) isGoldenCheckpointDir(checkpointDir string) bool {
	data, err := p.fs.ReadFile(filepath.Join(checkpointDir, sandboxManifestName))
	if err != nil {
		return false
	}
	var manifest struct {
		Atespace string `json:"atespace"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return false
	}
	return manifest.Atespace == resources.GoldenActorAtespace
}

func (p *BlockSnapshotPluginWorkerPlane) getPlugin(ctx context.Context, driverName string) (volume.VolumePluginWorkerPlane, error) {
	if p.registry != nil {
		if driverName == "" {
			driverName = DefaultVolumeDriver
		}
		return p.registry.GetPlugin(ctx, driverName)
	}
	if p.volPlugin != nil {
		return p.volPlugin, nil
	}
	return nil, fmt.Errorf("worker volume plugin is not configured")
}

func (p *BlockSnapshotPluginWorkerPlane) ensureVolumeMounted(
	ctx context.Context,
	actorUID string,
	storageVolumeID string,
	volumeType string,
	volumeContext map[string]string,
) error {
	if storageVolumeID == "" {
		return nil
	}
	if mounted := p.getMountedVolume(actorUID); mounted.storageVolumeID == storageVolumeID {
		return nil
	}

	mountPoint := ateompath.LocalCheckpointsDir(actorUID)
	if err := p.fs.MkdirAll(mountPoint, 0o750); err != nil {
		return fmt.Errorf("while creating block snapshot mount point %q: %w", mountPoint, err)
	}

	plugin, err := p.getPlugin(ctx, volumeType)
	if err != nil {
		return fmt.Errorf("while resolving volume plugin for %q: %w", volumeType, err)
	}
	if err := plugin.MountVolume(ctx, storageVolumeID, mountPoint, volumeContext); err != nil {
		return fmt.Errorf("while mounting block snapshot volume %q to %q: %w", storageVolumeID, mountPoint, err)
	}

	p.recordMountedVolume(actorUID, storageVolumeID, volumeType)
	return nil
}

func (p *BlockSnapshotPluginWorkerPlane) verifyVolumeMounted(actorUID, storageVolumeID string) error {
	mounted := p.getMountedVolume(actorUID)
	if mounted.storageVolumeID == "" {
		return fmt.Errorf("block snapshot volume %q is not mounted for actor %q", storageVolumeID, actorUID)
	}
	if mounted.storageVolumeID != storageVolumeID {
		return fmt.Errorf("mounted block snapshot volume %q for actor %q does not match requested volume %q", mounted.storageVolumeID, actorUID, storageVolumeID)
	}
	return nil
}

func (p *BlockSnapshotPluginWorkerPlane) recordMountedVolume(actorUID, storageVolumeID, volumeType string) {
	p.mu.Lock()
	p.mountedByActor[actorUID] = mountedVolumeInfo{
		storageVolumeID: storageVolumeID,
		volumeType:      volumeType,
	}
	p.mu.Unlock()
}

func (p *BlockSnapshotPluginWorkerPlane) getMountedVolume(actorUID string) mountedVolumeInfo {
	p.mu.RLock()
	info := p.mountedByActor[actorUID]
	p.mu.RUnlock()
	return info
}

func (p *BlockSnapshotPluginWorkerPlane) clearMountedVolume(actorUID string) {
	p.mu.Lock()
	delete(p.mountedByActor, actorUID)
	p.mu.Unlock()
}

func (p *BlockSnapshotPluginWorkerPlane) resolveRestoreMeta(
	ctx context.Context,
	actorUID string,
	req *ateletpb.RestoreRequest,
) RestoreRequestMetadata {
	meta := p.extractRestoreMeta(req)
	if ctxMeta, ok := VolumeMetadataFromContext(ctx); ok {
		if meta.StorageVolumeID == "" {
			meta.StorageVolumeID = ctxMeta.StorageVolumeID
		}
		if meta.VolumeType == "" {
			meta.VolumeType = ctxMeta.VolumeType
		}
		if meta.DevicePath == "" {
			meta.DevicePath = ctxMeta.DevicePath
		}
		if len(meta.VolumeContext) == 0 && len(ctxMeta.VolumeContext) > 0 {
			meta.VolumeContext = ctxMeta.VolumeContext
		}
	}
	if meta.StorageVolumeID == "" {
		if mounted := p.getMountedVolume(actorUID); mounted.storageVolumeID != "" {
			meta.StorageVolumeID = mounted.storageVolumeID
			if meta.VolumeType == "" {
				meta.VolumeType = mounted.volumeType
			}
		}
	}
	return meta
}

func (p *BlockSnapshotPluginWorkerPlane) resolveCheckpointMeta(
	ctx context.Context,
	actorUID string,
	req *ateletpb.CheckpointRequest,
) CheckpointRequestMetadata {
	meta := p.extractCheckpointMeta(req)
	if ctxMeta, ok := VolumeMetadataFromContext(ctx); ok {
		if meta.StorageVolumeID == "" {
			meta.StorageVolumeID = ctxMeta.StorageVolumeID
		}
		if meta.VolumeType == "" {
			meta.VolumeType = ctxMeta.VolumeType
		}
		if len(meta.VolumeContext) == 0 && len(ctxMeta.VolumeContext) > 0 {
			meta.VolumeContext = ctxMeta.VolumeContext
		}
	}
	if meta.StorageVolumeID == "" {
		if mounted := p.getMountedVolume(actorUID); mounted.storageVolumeID != "" {
			meta.StorageVolumeID = mounted.storageVolumeID
			if meta.VolumeType == "" {
				meta.VolumeType = mounted.volumeType
			}
		}
	}
	return meta
}

func defaultRestoreMeta(req *ateletpb.RestoreRequest) RestoreRequestMetadata {
	if req == nil {
		return RestoreRequestMetadata{}
	}
	meta := RestoreRequestMetadata{
		SnapshotID: req.GetSnapshot().GetSnapshotId(),
	}
	if ext := req.GetSnapshotStorage().GetBlockVolume(); ext != nil {
		meta.StorageVolumeID = ext.GetStorageVolumeId()
		meta.VolumeType = ext.GetVolumeType()
		meta.DevicePath = ext.GetVolumeContext()[DevicePathContextKey]
		meta.VolumeContext = ext.GetVolumeContext()
	}
	return meta
}

func defaultCheckpointMeta(req *ateletpb.CheckpointRequest) CheckpointRequestMetadata {
	if req == nil {
		return CheckpointRequestMetadata{}
	}
	meta := CheckpointRequestMetadata{
		SnapshotID: req.GetSnapshot().GetSnapshotId(),
		Rung:       req.GetSnapshot().GetSurvivability(),
	}
	if ext := req.GetSnapshotStorage().GetBlockVolume(); ext != nil {
		meta.StorageVolumeID = ext.GetStorageVolumeId()
		meta.VolumeType = ext.GetVolumeType()
		meta.VolumeContext = ext.GetVolumeContext()
	}
	return meta
}
