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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/snapshot"
	"github.com/agent-substrate/substrate/internal/snapshot/object/ategcs"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// DefaultWorkerBaseDir is the root runtime directory on the worker node.
	DefaultWorkerBaseDir = ateompath.BasePath

	actorsSubdir          = "actors"
	localCheckpointSubdir = "local-checkpoint"
	rootfsDeltaSubdir     = "rootfs-delta"
	volumesSubdir         = "volumes"

	checkpointArchiveName  = "checkpoint.tar"
	rootfsDeltaArchiveName = "rootfs-delta.tar"
	volumesArchiveName     = "volumes.tar"

	// sandboxManifestName is the object/file name of the per-snapshot manifest that
	// records the actor identity, snapshot files, and sandbox binaries. It is written
	// next to the checkpoint images so a snapshot is self-describing.
	sandboxManifestName = "manifest.json"
)

// assetEntry is one content-addressed sandbox asset (url + sha256).
type assetEntry struct {
	URL    string `json:"url"`
	SHA256 string `json:"sha256"`
}

// sandboxAssetsRecord is the sandbox runtime an actor is running, projected onto
// the local node's architecture. It serves as both the per-actor on-node record
// and the self-describing snapshot manifest (manifest.json).
type sandboxAssetsRecord struct {
	SandboxClass          string                `json:"sandboxClass"`
	Assets                map[string]assetEntry `json:"assets"`
	PauseImage            string                `json:"pauseImage"`
	Atespace              string                `json:"atespace,omitempty"`
	ActorName             string                `json:"actorName,omitempty"`
	ActorUID              string                `json:"actorUid,omitempty"`
	ActorTemplateAtespace string                `json:"actorTemplateAtespace,omitempty"`
	ActorTemplateName     string                `json:"actorTemplateName,omitempty"`
	SnapshotFiles         []string              `json:"snapshotFiles,omitempty"`
	Scope                 string                `json:"scope,omitempty"`
}

func unmarshalSandboxRecord(data []byte) (*sandboxAssetsRecord, error) {
	rec := &sandboxAssetsRecord{}
	if err := json.Unmarshal(data, rec); err != nil {
		return nil, fmt.Errorf("while parsing sandbox record/manifest: %w", err)
	}
	if rec.PauseImage == "" {
		return nil, fmt.Errorf("sandbox record/manifest has no pauseImage")
	}
	return rec, nil
}

type localSnapshotNameContextKey struct{}

// WithLocalSnapshotName attaches the local snapshot directory name to ctx for
// EscalateCheckpoint when CheckpointRequest.Config carries an ExternalConfig.
func WithLocalSnapshotName(ctx context.Context, snapshotName string) context.Context {
	return context.WithValue(ctx, localSnapshotNameContextKey{}, snapshotName)
}

// LocalSnapshotNameFromContext returns the local snapshot name stored in ctx, if any.
func LocalSnapshotNameFromContext(ctx context.Context) string {
	v, _ := ctx.Value(localSnapshotNameContextKey{}).(string)
	return v
}

// CheckpointRequestMetadata extracts v2 lifecycle fields from CheckpointRequest.
// It defaults to reading the fields on ateletpb.CheckpointRequest and can be
// overridden in tests or during proto migration.
type CheckpointRequestMetadata struct {
	SnapshotID        string
	Rung              ateapipb.SurvivabilityRung
	ObjectSnapshotURI string
	DesiredScope      ateletpb.SnapshotScope
}

// RestoreRequestMetadata extracts v2 lifecycle fields from RestoreRequest.
type RestoreRequestMetadata struct {
	SnapshotID        string
	ObjectSnapshotURI string
}

// ObjectSnapshotPluginWorkerPlane implements snapshot.SnapshotPluginWorkerPlane
// for the ObjectWithLocalCache strategy.
type ObjectSnapshotPluginWorkerPlane struct {
	baseDir   string
	fs        FileSystem
	archiver  Archiver
	gcsClient ategcs.ObjectStorage

	extractCheckpointMeta func(*ateletpb.CheckpointRequest) CheckpointRequestMetadata
	extractRestoreMeta    func(*ateletpb.RestoreRequest) RestoreRequestMetadata
}

var _ snapshot.SnapshotPluginWorkerPlane = (*ObjectSnapshotPluginWorkerPlane)(nil)

// WorkerPlaneOption configures ObjectSnapshotPluginWorkerPlane.
type WorkerPlaneOption func(*ObjectSnapshotPluginWorkerPlane)

// WithBaseDir overrides the base worker directory (default: ateompath.BasePath).
func WithBaseDir(baseDir string) WorkerPlaneOption {
	return func(p *ObjectSnapshotPluginWorkerPlane) {
		p.baseDir = baseDir
	}
}

// WithFileSystem injects a custom FileSystem implementation.
func WithFileSystem(fs FileSystem) WorkerPlaneOption {
	return func(p *ObjectSnapshotPluginWorkerPlane) {
		p.fs = fs
	}
}

// WithArchiver injects a custom TAR Archiver implementation.
func WithArchiver(archiver Archiver) WorkerPlaneOption {
	return func(p *ObjectSnapshotPluginWorkerPlane) {
		p.archiver = archiver
	}
}

// WithCheckpointMetaExtractor overrides how snapshot ID, target rung, and
// object URI are read from a CheckpointRequest.
func WithCheckpointMetaExtractor(fn func(*ateletpb.CheckpointRequest) CheckpointRequestMetadata) WorkerPlaneOption {
	return func(p *ObjectSnapshotPluginWorkerPlane) {
		p.extractCheckpointMeta = fn
	}
}

// WithRestoreMetaExtractor overrides how snapshot ID and object URI are read
// from a RestoreRequest.
func WithRestoreMetaExtractor(fn func(*ateletpb.RestoreRequest) RestoreRequestMetadata) WorkerPlaneOption {
	return func(p *ObjectSnapshotPluginWorkerPlane) {
		p.extractRestoreMeta = fn
	}
}

// NewObjectSnapshotPluginWorkerPlane constructs a new worker-plane plugin.
func NewObjectSnapshotPluginWorkerPlane(gcsClient ategcs.ObjectStorage, opts ...WorkerPlaneOption) *ObjectSnapshotPluginWorkerPlane {
	p := &ObjectSnapshotPluginWorkerPlane{
		baseDir:               DefaultWorkerBaseDir,
		fs:                    defaultFileSystem(),
		archiver:              defaultArchiver(),
		gcsClient:             gcsClient,
		extractCheckpointMeta: defaultCheckpointMeta,
		extractRestoreMeta:    defaultRestoreMeta,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// PrepareSnapshotStorage is a no-op because the node agent already initializes
// the base actor directory before invoking the plugin.
func (p *ObjectSnapshotPluginWorkerPlane) PrepareSnapshotStorage(
	ctx context.Context,
	actorUID string,
	storage *ateapipb.SnapshotStorage,
) error {
	return nil
}

// FetchRestoreManifests reads the actor snapshot's manifest (from local disk on
// a local restore or local cache hit, or from object storage on an external
// restore) and, when req.GetScope() is SNAPSHOT_SCOPE_DATA_ON_GOLDEN, also
// fetches the golden snapshot's manifest from object storage.
func (p *ObjectSnapshotPluginWorkerPlane) FetchRestoreManifests(
	ctx context.Context,
	req *ateletpb.RestoreRequest,
) (actorManifest, goldenManifest []byte, err error) {
	actorUID := req.GetActorUid()
	meta := p.extractRestoreMeta(req)

	switch {
	case req.GetType() == ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL ||
		(req.GetType() == ateletpb.CheckpointType_CHECKPOINT_TYPE_UNSPECIFIED && meta.ObjectSnapshotURI == "" && meta.SnapshotID != ""):
		if meta.SnapshotID == "" {
			return nil, nil, fmt.Errorf("restore request is missing snapshot ID")
		}
		manifestPath := filepath.Join(p.localSnapshotDir(actorUID, meta.SnapshotID), sandboxManifestName)
		actorManifest, err = os.ReadFile(manifestPath)
		if err != nil {
			return nil, nil, fmt.Errorf("while reading local snapshot manifest: %w", err)
		}

	case req.GetType() == ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL ||
		(req.GetType() == ateletpb.CheckpointType_CHECKPOINT_TYPE_UNSPECIFIED && meta.ObjectSnapshotURI != ""):
		if meta.SnapshotID != "" {
			manifestPath := filepath.Join(p.localSnapshotDir(actorUID, meta.SnapshotID), sandboxManifestName)
			if data, readErr := os.ReadFile(manifestPath); readErr == nil {
				actorManifest = data
			}
		}
		if actorManifest == nil {
			if p.gcsClient == nil {
				return nil, nil, fmt.Errorf("object storage client is not configured")
			}
			uri, err := resources.ParseSnapshotURI(meta.ObjectSnapshotURI)
			if err != nil {
				return nil, nil, err
			}
			manifestURI, err := uri.ObjectURI(sandboxManifestName)
			if err != nil {
				return nil, nil, err
			}
			actorManifest, err = ategcs.FetchFromGCS(ctx, p.gcsClient, manifestURI)
			if err != nil {
				return nil, nil, fmt.Errorf("while fetching snapshot manifest: %w", err)
			}
		}

	default:
		return nil, nil, fmt.Errorf("unexpected checkpoint type: %v", req.GetType())
	}

	if req.GetScope() == ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN {
		if p.gcsClient == nil {
			return nil, nil, fmt.Errorf("object storage client is not configured")
		}
		goldenURI, err := resources.ParseSnapshotURI(req.GetGoldenSnapshotUri())
		if err != nil {
			return nil, nil, err
		}
		manifestURI, err := goldenURI.ObjectURI(sandboxManifestName)
		if err != nil {
			return nil, nil, err
		}
		goldenManifest, err = ategcs.FetchFromGCS(ctx, p.gcsClient, manifestURI)
		if err != nil {
			return nil, nil, fmt.Errorf("while fetching golden snapshot manifest: %w", err)
		}
	}

	return actorManifest, goldenManifest, nil
}

// PrepareRestoreDir checks whether the local checkpoint directory
// (<baseDir>/actors/<actorUID>/local-checkpoint/<snapshotID>) already exists on
// disk. On a cache hit it returns the path immediately (fetching golden files
// if DATA_ON_GOLDEN); on a cache miss it downloads the ObjectSnapshot from
// object storage using ategcs into the local checkpoint directory and returns
// its path.
func (p *ObjectSnapshotPluginWorkerPlane) PrepareRestoreDir(
	ctx context.Context,
	req *ateletpb.RestoreRequest,
) (string, error) {
	actorUID := req.GetActorUid()
	meta := p.extractRestoreMeta(req)
	if meta.SnapshotID == "" {
		return "", fmt.Errorf("restore request is missing snapshot ID")
	}

	checkpointDir := p.localSnapshotDir(actorUID, meta.SnapshotID)
	if info, err := p.fs.Stat(checkpointDir); err == nil && info.IsDir() {
		if req != nil && req.GetScope() == ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN && req.GetGoldenSnapshotUri() != "" {
			if err := p.ensureGoldenFilesInLocalDir(ctx, checkpointDir, req.GetGoldenSnapshotUri()); err != nil {
				return "", err
			}
		}
		return checkpointDir, nil
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("while checking local checkpoint directory %q: %w", checkpointDir, err)
	}

	if meta.ObjectSnapshotURI == "" {
		return "", fmt.Errorf("local checkpoint %q not found on disk and no object snapshot URI provided", meta.SnapshotID)
	}
	if p.gcsClient == nil {
		return "", fmt.Errorf("object storage client is not configured")
	}

	if err := p.downloadSnapshot(ctx, actorUID, req, meta.ObjectSnapshotURI, checkpointDir); err != nil {
		return "", err
	}
	return checkpointDir, nil
}

func (p *ObjectSnapshotPluginWorkerPlane) ensureGoldenFilesInLocalDir(
	ctx context.Context,
	checkpointDir string,
	goldenSnapshotURI string,
) error {
	if p.gcsClient == nil {
		return fmt.Errorf("object storage client is not configured")
	}
	manifest, err := os.ReadFile(filepath.Join(checkpointDir, sandboxManifestName))
	if err != nil {
		return fmt.Errorf("while reading local snapshot manifest: %w", err)
	}
	sandboxRec, err := unmarshalSandboxRecord(manifest)
	if err != nil {
		return fmt.Errorf("while unmarshalling sandbox record: %w", err)
	}
	goldenURI, err := resources.ParseSnapshotURI(goldenSnapshotURI)
	if err != nil {
		return err
	}
	goldenManifestURI, err := goldenURI.ObjectURI(sandboxManifestName)
	if err != nil {
		return err
	}
	goldenManifest, err := ategcs.FetchFromGCS(ctx, p.gcsClient, goldenManifestURI)
	if err != nil {
		return fmt.Errorf("while fetching golden snapshot manifest: %w", err)
	}
	goldenRec, err := unmarshalSandboxRecord(goldenManifest)
	if err != nil {
		return fmt.Errorf("while unmarshalling golden sandbox record: %w", err)
	}
	if goldenRec.SandboxClass != sandboxRec.SandboxClass {
		return status.Errorf(codes.FailedPrecondition, "golden snapshot sandbox class %q does not match actor snapshot sandbox class %q", goldenRec.SandboxClass, sandboxRec.SandboxClass)
	}
	return p.downloadExternalCheckpoint(ctx, goldenSnapshotURI, checkpointDir, goldenOnlyFiles(sandboxRec.SnapshotFiles, goldenRec.SnapshotFiles))
}

// PrepareCheckpointDir creates and returns the node-local checkpoint directory
// (<baseDir>/actors/<actorUID>/local-checkpoint/<snapshotID>).
func (p *ObjectSnapshotPluginWorkerPlane) PrepareCheckpointDir(
	ctx context.Context,
	req *ateletpb.CheckpointRequest,
) (string, error) {
	actorUID := req.GetActorUid()
	if actorUID == "" {
		return "", fmt.Errorf("checkpoint request is missing actor UID")
	}
	meta := p.extractCheckpointMeta(req)
	if meta.SnapshotID == "" {
		return "", fmt.Errorf("checkpoint request is missing snapshot ID")
	}

	checkpointWriteDir := p.localSnapshotDir(actorUID, meta.SnapshotID)
	if err := p.fs.MkdirAll(checkpointWriteDir, 0o700); err != nil {
		return "", fmt.Errorf("while creating local checkpoint directory %q: %w", checkpointWriteDir, err)
	}
	return checkpointWriteDir, nil
}

// CommitCheckpoint persists a newly written checkpoint according to the
// requested survivability rung:
//   - RESIDENT: returns immediately with pages kept in the host page cache.
//   - LOCAL: runs syncfs() on the local checkpoint directory.
//   - DURABLE: uploads the checkpoint (and any rootfs-delta/ and volumes/
//     archives) to the ObjectSnapshot URI via ategcs.
func (p *ObjectSnapshotPluginWorkerPlane) CommitCheckpoint(
	ctx context.Context,
	req *ateletpb.CheckpointRequest,
	checkpointWriteDir string,
) (ateapipb.SurvivabilityRung, error) {
	meta := p.extractCheckpointMeta(req)
	if checkpointWriteDir == "" {
		checkpointWriteDir = p.localSnapshotDir(req.GetActorUid(), meta.SnapshotID)
	}

	switch meta.Rung {
	case ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_RESIDENT:
		return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_RESIDENT, nil

	case ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_LOCAL:
		if err := p.fs.Syncfs(checkpointWriteDir); err != nil {
			return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED,
				fmt.Errorf("while syncing local checkpoint directory %q: %w", checkpointWriteDir, err)
		}
		return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_LOCAL, nil

	case ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE:
		if meta.ObjectSnapshotURI == "" {
			return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED,
				fmt.Errorf("durable checkpoint requires an object snapshot URI")
		}
		if err := p.persistDurableCheckpoint(ctx, req.GetActorUid(), meta, checkpointWriteDir); err != nil {
			return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED, err
		}
		return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE, nil

	default:
		return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED,
			fmt.Errorf("unsupported survivability rung: %v", meta.Rung)
	}
}

// EscalateCheckpoint elevates an existing node-local checkpoint to a higher
// survivability rung (LOCAL via syncfs, or DURABLE via packaging and upload to
// object storage) without interacting with a running sandbox.
func (p *ObjectSnapshotPluginWorkerPlane) EscalateCheckpoint(
	ctx context.Context,
	req *ateletpb.CheckpointRequest,
) (ateapipb.SurvivabilityRung, error) {
	actorUID := req.GetActorUid()
	meta := p.extractCheckpointMeta(req)
	if override := LocalSnapshotNameFromContext(ctx); override != "" {
		meta.SnapshotID = override
	}
	if meta.SnapshotID == "" {
		return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED,
			fmt.Errorf("escalate checkpoint request is missing snapshot ID")
	}

	localDir := p.localSnapshotDir(actorUID, meta.SnapshotID)

	switch meta.Rung {
	case ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_LOCAL:
		if info, err := p.fs.Stat(localDir); err != nil {
			return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED,
				fmt.Errorf("local checkpoint directory %q is not available for escalation: %w", localDir, err)
		} else if !info.IsDir() {
			return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED,
				fmt.Errorf("local checkpoint path %q is not a directory", localDir)
		}
		if err := p.fs.Syncfs(localDir); err != nil {
			return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED,
				fmt.Errorf("while syncing local checkpoint directory %q: %w", localDir, err)
		}
		return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_LOCAL, nil

	case ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE:
		if meta.ObjectSnapshotURI == "" {
			return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED,
				fmt.Errorf("escalating checkpoint to DURABLE requires an object snapshot URI")
		}
		if err := p.persistDurableCheckpoint(ctx, actorUID, meta, localDir); err != nil {
			return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED, err
		}
		return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE, nil

	default:
		return ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_UNSPECIFIED,
			fmt.Errorf("unsupported target survivability rung for escalation: %v", meta.Rung)
	}
}

// DetachCheckpointDir removes all local checkpoints for the actor from the
// node's local disk (<baseDir>/actors/<actorUID>/local-checkpoint). A missing
// directory is not an error, so retries are safe.
func (p *ObjectSnapshotPluginWorkerPlane) DetachCheckpointDir(
	ctx context.Context,
	actorUID string,
	storageID string,
) error {
	if actorUID == "" {
		return fmt.Errorf("actorUID is required")
	}
	return p.pruneLocalCheckpointDir(ctx, p.localCheckpointsDir(actorUID))
}

func (p *ObjectSnapshotPluginWorkerPlane) pruneLocalCheckpointDir(ctx context.Context, dir string) error {
	entries, err := p.fs.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("while listing local checkpoints in %s: %w", dir, err)
	}
	// Every entry is attempted: one undeletable snapshot must not strand the
	// others on disk.
	var errs []error
	hasStateDirs := false
	for _, entry := range entries {
		name := entry.Name()
		if name == ateompath.CheckpointStateDirName || name == ateompath.RestoreStateDirName {
			hasStateDirs = true
			continue
		}
		path := filepath.Join(dir, name)
		if err := p.fs.RemoveAll(path); err != nil {
			errs = append(errs, fmt.Errorf("while pruning local checkpoint %s: %w", path, err))
			continue
		}
		slog.InfoContext(ctx, "pruned local checkpoint", slog.String("path", path))
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	if !hasStateDirs {
		if err := p.fs.Remove(dir); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("while removing local checkpoints dir %s: %w", dir, err)
		}
	}
	return nil
}

func (p *ObjectSnapshotPluginWorkerPlane) actorDir(actorUID string) string {
	if p.baseDir == ateompath.BasePath {
		return ateompath.ActorPath(actorUID)
	}
	return filepath.Join(p.baseDir, actorsSubdir, actorUID)
}

func (p *ObjectSnapshotPluginWorkerPlane) localCheckpointsDir(actorUID string) string {
	if p.baseDir == ateompath.BasePath {
		return ateompath.LocalCheckpointsDir(actorUID)
	}
	return filepath.Join(p.actorDir(actorUID), localCheckpointSubdir)
}

func (p *ObjectSnapshotPluginWorkerPlane) localSnapshotDir(actorUID, snapshotID string) string {
	if p.baseDir == ateompath.BasePath {
		return ateompath.LocalSnapshotDir(actorUID, snapshotID)
	}
	return filepath.Join(p.localCheckpointsDir(actorUID), snapshotID)
}

func (p *ObjectSnapshotPluginWorkerPlane) rootfsDeltaDir(actorUID string) string {
	return filepath.Join(p.actorDir(actorUID), rootfsDeltaSubdir)
}

func (p *ObjectSnapshotPluginWorkerPlane) volumesDir(actorUID string) string {
	if p.baseDir == ateompath.BasePath {
		return ateompath.VolumesDir(actorUID)
	}
	return filepath.Join(p.actorDir(actorUID), volumesSubdir)
}

type archiveTarget struct {
	archiveName string
	dirPath     string
	optional    bool
}

// persistDurableCheckpoint uploads a local checkpoint to object storage.
// When the checkpoint directory contains a self-describing manifest.json
// written by atelet/ateom (or was already uploaded and pruned on an earlier
// attempt), it uses uploadLocalCheckpointDir to upload the manifest-listed
// files with zstd compression. Otherwise, it packages the checkpoint,
// rootfs-delta/, and volumes/ directories into tarballs and uploads them with
// zstd compression.
func (p *ObjectSnapshotPluginWorkerPlane) persistDurableCheckpoint(
	ctx context.Context,
	actorUID string,
	meta CheckpointRequestMetadata,
	localDir string,
) error {
	if p.gcsClient == nil {
		return fmt.Errorf("object storage client is not configured")
	}
	uri, err := resources.ParseSnapshotURI(meta.ObjectSnapshotURI)
	if err != nil {
		return err
	}

	info, err := p.fs.Stat(localDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			_, upErr := p.uploadLocalCheckpointDir(ctx, meta, localDir, uri)
			return upErr
		}
		return fmt.Errorf("local checkpoint directory %q is not available: %w", localDir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("local checkpoint path %q is not a directory", localDir)
	}

	manifestPath := filepath.Join(localDir, sandboxManifestName)
	if _, statErr := p.fs.Stat(manifestPath); statErr == nil {
		_, upErr := p.uploadLocalCheckpointDir(ctx, meta, localDir, uri)
		return upErr
	}

	return p.packageAndUploadSnapshot(ctx, actorUID, localDir, uri)
}

// uploadLocalCheckpointDir uploads the local checkpoint in localDir to uri,
// converting the captured scope to the requested one where possible.
func (p *ObjectSnapshotPluginWorkerPlane) uploadLocalCheckpointDir(
	ctx context.Context,
	meta CheckpointRequestMetadata,
	localDir string,
	uri resources.SnapshotURI,
) (string, error) {
	manifestURI, err := uri.ObjectURI(sandboxManifestName)
	if err != nil {
		return "", fmt.Errorf("while addressing snapshot manifest in GCS: %w", err)
	}

	manifest, err := os.ReadFile(filepath.Join(localDir, sandboxManifestName))
	if errors.Is(err, os.ErrNotExist) {
		_, fetchErr := ategcs.FetchFromGCS(ctx, p.gcsClient, manifestURI)
		if fetchErr == nil {
			slog.InfoContext(ctx, "Local snapshot already uploaded and pruned; nothing to do", slog.String("snapshot_uri", meta.ObjectSnapshotURI))
			return "", nil
		}
		if errors.Is(fetchErr, ategcs.ErrObjectNotFound) {
			return "", fmt.Errorf("local snapshot %q is gone and no uploaded copy exists: %w", meta.SnapshotID, fetchErr)
		}
		return "", fmt.Errorf("while probing for an already-uploaded snapshot manifest: %w", fetchErr)
	}
	if err != nil {
		return "", fmt.Errorf("while reading local snapshot manifest: %w", err)
	}

	rec, err := unmarshalSandboxRecord(manifest)
	if err != nil {
		return "", err
	}

	capturedScope := rec.Scope
	if capturedScope == "" {
		return rec.SandboxClass, status.Errorf(codes.FailedPrecondition, "local snapshot %q has no scope recorded in its manifest (written by an older atelet); resume and pause the actor again before suspending it", meta.SnapshotID)
	}
	desiredScope := ateattr.SnapshotScopeValue(meta.DesiredScope)
	if meta.DesiredScope == ateletpb.SnapshotScope_SNAPSHOT_SCOPE_UNSPECIFIED {
		desiredScope = capturedScope
	}

	switch {
	case capturedScope == desiredScope:
	case capturedScope == ateattr.SnapshotScopeData && desiredScope == ateattr.SnapshotScopeFull:
		return rec.SandboxClass, status.Errorf(codes.FailedPrecondition, "pause snapshot captured %s; cannot upload it as %s (memory was never captured)", capturedScope, desiredScope)
	default: // captured FULL, DATA wanted
		if err := narrowFullCaptureToData(rec); err != nil {
			return rec.SandboxClass, err
		}
	}

	return rec.SandboxClass, p.uploadSnapshot(ctx, uri, localDir, rec)
}

// narrowFullCaptureToData rewrites rec so a FULL capture uploads as a DATA
// snapshot.
func narrowFullCaptureToData(rec *sandboxAssetsRecord) error {
	switch atev1alpha1.SandboxClass(rec.SandboxClass) {
	case atev1alpha1.SandboxClassMicroVM, atev1alpha1.SandboxClassGvisor:
		if !slices.Contains(rec.SnapshotFiles, ateompath.DurableDirTarFile) {
			return status.Errorf(codes.FailedPrecondition, "full %s capture has no %s; the actor has no durable data to upload as %s", rec.SandboxClass, ateompath.DurableDirTarFile, ateattr.SnapshotScopeData)
		}
		rec.SnapshotFiles = []string{ateompath.DurableDirTarFile}
		rec.Scope = ateattr.SnapshotScopeData
		return nil

	default:
		return status.Errorf(codes.FailedPrecondition, "unknown sandbox class %q in snapshot manifest", rec.SandboxClass)
	}
}

// uploadSnapshot uploads rec's snapshot files from srcDir to uri (each
// zstd-compressed, concurrently), then the marshaled manifest last as the
// commit marker.
func (p *ObjectSnapshotPluginWorkerPlane) uploadSnapshot(
	ctx context.Context,
	uri resources.SnapshotURI,
	srcDir string,
	rec *sandboxAssetsRecord,
) error {
	g, gCtx := errgroup.WithContext(ctx)
	for _, fileName := range rec.SnapshotFiles {
		local := filepath.Join(srcDir, fileName)
		g.Go(func() error {
			objectURI, err := uri.ObjectURI(fileName + ".zstd")
			if err != nil {
				return fmt.Errorf("while addressing %s in GCS: %w", fileName, err)
			}
			if err := ategcs.SendLocalFileToGCSWithZstd(gCtx, p.gcsClient, objectURI, local); err != nil {
				return fmt.Errorf("while uploading %s to GCS: %w", fileName, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	manifest, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("while marshaling snapshot manifest: %w", err)
	}
	manifestURI, err := uri.ObjectURI(sandboxManifestName)
	if err != nil {
		return fmt.Errorf("while addressing snapshot manifest in GCS: %w", err)
	}
	if err := ategcs.SendBytesToGCS(ctx, p.gcsClient, manifestURI, manifest); err != nil {
		return fmt.Errorf("while uploading snapshot manifest: %w", err)
	}
	return nil
}

// packageAndUploadSnapshot archives the checkpoint, rootfs-delta/, and volumes/
// directories into tarballs and uploads them concurrently with zstd compression
// via ategcs.
func (p *ObjectSnapshotPluginWorkerPlane) packageAndUploadSnapshot(
	ctx context.Context,
	actorUID string,
	checkpointDir string,
	uri resources.SnapshotURI,
) error {
	tmpDir, err := p.fs.CreateTempDir("", "substrate-snapshot-upload-")
	if err != nil {
		return fmt.Errorf("while creating temporary staging directory: %w", err)
	}
	defer func() { _ = p.fs.RemoveAll(tmpDir) }()

	targets := []archiveTarget{
		{archiveName: checkpointArchiveName, dirPath: checkpointDir, optional: false},
		{archiveName: rootfsDeltaArchiveName, dirPath: p.rootfsDeltaDir(actorUID), optional: true},
		{archiveName: volumesArchiveName, dirPath: p.volumesDir(actorUID), optional: true},
	}

	g, gCtx := errgroup.WithContext(ctx)
	for _, target := range targets {
		g.Go(func() error {
			if info, err := p.fs.Stat(target.dirPath); err != nil {
				if target.optional && errors.Is(err, os.ErrNotExist) {
					return nil
				}
				return fmt.Errorf("while stating %q for packaging: %w", target.dirPath, err)
			} else if !info.IsDir() {
				return fmt.Errorf("expected directory at %q", target.dirPath)
			}

			tarPath := filepath.Join(tmpDir, target.archiveName)
			if err := p.archiver.Create(gCtx, tarPath, target.dirPath); err != nil {
				return fmt.Errorf("while packaging %q into %s: %w", target.dirPath, target.archiveName, err)
			}

			objectURI, err := uri.ObjectURI(target.archiveName + ".zstd")
			if err != nil {
				return fmt.Errorf("while building object URI for %s: %w", target.archiveName, err)
			}
			if err := ategcs.SendLocalFileToGCSWithZstd(gCtx, p.gcsClient, objectURI, tarPath); err != nil {
				return fmt.Errorf("while uploading %s to %q: %w", target.archiveName, objectURI, err)
			}
			return nil
		})
	}
	return g.Wait()
}

// downloadSnapshot downloads a snapshot from snapshotURI into checkpointDir.
// If a self-describing manifest.json exists at snapshotURI, it downloads the
// manifest-listed files (and merges golden snapshot files on DATA_ON_GOLDEN).
// Otherwise, it downloads and extracts the checkpoint, rootfs-delta, and
// volumes tarballs.
func (p *ObjectSnapshotPluginWorkerPlane) downloadSnapshot(
	ctx context.Context,
	actorUID string,
	req *ateletpb.RestoreRequest,
	snapshotURI string,
	checkpointDir string,
) error {
	uri, err := resources.ParseSnapshotURI(snapshotURI)
	if err != nil {
		return err
	}

	parentDir := p.localCheckpointsDir(actorUID)
	if err := p.fs.MkdirAll(parentDir, 0o700); err != nil {
		return fmt.Errorf("while creating parent local-checkpoint directory %q: %w", parentDir, err)
	}

	stagingCheckpointDir, err := p.fs.CreateTempDir(parentDir, ".staging-checkpoint-")
	if err != nil {
		return fmt.Errorf("while creating staging checkpoint directory: %w", err)
	}
	stagingCommitted := false
	defer func() {
		if !stagingCommitted {
			_ = p.fs.RemoveAll(stagingCheckpointDir)
		}
	}()

	manifestURI, err := uri.ObjectURI(sandboxManifestName)
	if err != nil {
		return err
	}
	manifest, fetchErr := ategcs.FetchFromGCS(ctx, p.gcsClient, manifestURI)
	switch {
	case fetchErr == nil:
		sandboxRec, err := unmarshalSandboxRecord(manifest)
		if err != nil {
			return fmt.Errorf("while unmarshalling sandbox record: %w", err)
		}
		if req != nil && req.GetScope() == ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN && req.GetGoldenSnapshotUri() != "" {
			goldenURI, err := resources.ParseSnapshotURI(req.GetGoldenSnapshotUri())
			if err != nil {
				return err
			}
			goldenManifestURI, err := goldenURI.ObjectURI(sandboxManifestName)
			if err != nil {
				return err
			}
			goldenManifest, err := ategcs.FetchFromGCS(ctx, p.gcsClient, goldenManifestURI)
			if err != nil {
				return fmt.Errorf("while fetching golden snapshot manifest: %w", err)
			}
			goldenRec, err := unmarshalSandboxRecord(goldenManifest)
			if err != nil {
				return fmt.Errorf("while unmarshalling golden sandbox record: %w", err)
			}
			if goldenRec.SandboxClass != sandboxRec.SandboxClass {
				return status.Errorf(codes.FailedPrecondition, "golden snapshot sandbox class %q does not match actor snapshot sandbox class %q", goldenRec.SandboxClass, sandboxRec.SandboxClass)
			}
			if err := p.downloadCombinedCheckpoint(ctx, snapshotURI, req.GetGoldenSnapshotUri(), stagingCheckpointDir, sandboxRec.SnapshotFiles, goldenRec.SnapshotFiles); err != nil {
				return err
			}
		} else {
			if err := p.downloadExternalCheckpoint(ctx, snapshotURI, stagingCheckpointDir, sandboxRec.SnapshotFiles); err != nil {
				return err
			}
		}
		if err := os.WriteFile(filepath.Join(stagingCheckpointDir, sandboxManifestName), manifest, 0o600); err != nil {
			return fmt.Errorf("while writing snapshot manifest: %w", err)
		}

	case errors.Is(fetchErr, ategcs.ErrObjectNotFound):
		if err := p.downloadAndExtractArchives(ctx, actorUID, uri, stagingCheckpointDir); err != nil {
			return err
		}

	default:
		return fmt.Errorf("while fetching snapshot manifest: %w", fetchErr)
	}

	if err := p.fs.Rename(stagingCheckpointDir, checkpointDir); err != nil {
		return fmt.Errorf("while committing restored checkpoint directory %q: %w", checkpointDir, err)
	}
	stagingCommitted = true
	return nil
}

func (p *ObjectSnapshotPluginWorkerPlane) downloadAndExtractArchives(
	ctx context.Context,
	actorUID string,
	uri resources.SnapshotURI,
	stagingCheckpointDir string,
) error {
	tmpDownloadDir, err := p.fs.CreateTempDir("", "substrate-snapshot-download-")
	if err != nil {
		return fmt.Errorf("while creating temporary download directory: %w", err)
	}
	defer func() { _ = p.fs.RemoveAll(tmpDownloadDir) }()

	targets := []archiveTarget{
		{archiveName: checkpointArchiveName, dirPath: stagingCheckpointDir, optional: false},
		{archiveName: rootfsDeltaArchiveName, dirPath: p.rootfsDeltaDir(actorUID), optional: true},
		{archiveName: volumesArchiveName, dirPath: p.volumesDir(actorUID), optional: true},
	}

	g, gCtx := errgroup.WithContext(ctx)
	for _, target := range targets {
		g.Go(func() error {
			objectURI, err := uri.ObjectURI(target.archiveName + ".zstd")
			if err != nil {
				return fmt.Errorf("while building object URI for %s: %w", target.archiveName, err)
			}

			tarPath := filepath.Join(tmpDownloadDir, target.archiveName)
			if err := ategcs.FetchLocalFileFromGCSWithZstd(gCtx, p.gcsClient, objectURI, tarPath); err != nil {
				if target.optional && (errors.Is(err, ategcs.ErrObjectNotFound) || errors.Is(err, os.ErrNotExist)) {
					return nil
				}
				return fmt.Errorf("while downloading %q: %w", objectURI, err)
			}

			if err := p.fs.MkdirAll(target.dirPath, 0o700); err != nil {
				return fmt.Errorf("while creating extraction directory %q: %w", target.dirPath, err)
			}
			if err := p.archiver.Extract(tarPath, target.dirPath); err != nil {
				return fmt.Errorf("while extracting %s into %q: %w", target.archiveName, target.dirPath, err)
			}
			return nil
		})
	}
	return g.Wait()
}

// goldenOnlyFiles returns the golden snapshot files not shadowed by the
// actor's own snapshot: on a DATA_ON_GOLDEN restore the actor's files (the
// durable-dir data) win name collisions, and the golden snapshot supplies
// the rest (guest memory + VM state).
func goldenOnlyFiles(actorFiles, goldenFiles []string) []string {
	shadowed := make(map[string]bool, len(actorFiles))
	for _, f := range actorFiles {
		shadowed[f] = true
	}
	rest := make([]string, 0, len(goldenFiles))
	for _, f := range goldenFiles {
		if !shadowed[f] {
			rest = append(rest, f)
		}
	}
	return rest
}

// downloadCombinedCheckpoint stages a DATA_ON_GOLDEN restore set into dstDir
// as a single folder: every file of the actor's own snapshot (the durable-dir
// data) plus the golden snapshot's files the actor's set does not shadow.
func (p *ObjectSnapshotPluginWorkerPlane) downloadCombinedCheckpoint(
	ctx context.Context,
	actorURI, goldenURI, dstDir string,
	actorFiles, goldenFiles []string,
) error {
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return p.downloadExternalCheckpoint(gctx, actorURI, dstDir, actorFiles)
	})
	g.Go(func() error {
		return p.downloadExternalCheckpoint(gctx, goldenURI, dstDir, goldenOnlyFiles(actorFiles, goldenFiles))
	})
	return g.Wait()
}

func (p *ObjectSnapshotPluginWorkerPlane) downloadExternalCheckpoint(
	ctx context.Context,
	snapshotURI string,
	dstDir string,
	files []string,
) error {
	uri, err := resources.ParseSnapshotURI(snapshotURI)
	if err != nil {
		return err
	}
	g, gCtx := errgroup.WithContext(ctx)
	for _, fileName := range files {
		local := filepath.Join(dstDir, fileName)
		g.Go(func() error {
			objectURI, err := uri.ObjectURI(fileName + ".zstd")
			if err != nil {
				return fmt.Errorf("while addressing %s in GCS: %w", fileName, err)
			}
			if err := ategcs.FetchLocalFileFromGCSWithZstd(gCtx, p.gcsClient, objectURI, local); err != nil {
				return fmt.Errorf("while downloading %s from GCS: %w", fileName, err)
			}
			return nil
		})
	}
	return g.Wait()
}

// defaultCheckpointMeta maps ateletpb.CheckpointRequest fields to CheckpointRequestMetadata.
func defaultCheckpointMeta(req *ateletpb.CheckpointRequest) CheckpointRequestMetadata {
	if req == nil {
		return CheckpointRequestMetadata{}
	}
	meta := CheckpointRequestMetadata{
		SnapshotID:        req.GetLocalConfig().GetSnapshotName(),
		ObjectSnapshotURI: req.GetExternalConfig().GetSnapshotUri(),
		DesiredScope:      req.GetScope(),
	}
	if meta.SnapshotID == "" && meta.ObjectSnapshotURI != "" {
		meta.SnapshotID = filepath.Base(meta.ObjectSnapshotURI)
	}
	switch req.GetType() {
	case ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL:
		meta.Rung = ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_LOCAL
	case ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL:
		meta.Rung = ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE
	}
	return meta
}

// defaultRestoreMeta maps ateletpb.RestoreRequest fields to RestoreRequestMetadata.
func defaultRestoreMeta(req *ateletpb.RestoreRequest) RestoreRequestMetadata {
	if req == nil {
		return RestoreRequestMetadata{}
	}
	meta := RestoreRequestMetadata{
		SnapshotID:        req.GetLocalConfig().GetSnapshotName(),
		ObjectSnapshotURI: req.GetExternalConfig().GetSnapshotUri(),
	}
	if meta.SnapshotID == "" && meta.ObjectSnapshotURI != "" {
		meta.SnapshotID = filepath.Base(meta.ObjectSnapshotURI)
	}
	return meta
}
