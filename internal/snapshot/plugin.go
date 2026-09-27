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

package snapshot

import (
	"context"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// SnapshotPluginControlPlane abstracts control-plane snapshot lifecycle and
// metadata operations without mutating ActorStatus or PlacementState directly.
type SnapshotPluginControlPlane interface {
	PrepareNewActor(ctx context.Context, actor *ateapipb.Actor, tmpl *ateapipb.ActorTemplate) (*ateapipb.SnapshotStorage, *ateapipb.Snapshot, error)
	PrepareNewSnapshot(ctx context.Context, actor *ateapipb.Actor, tmpl *ateapipb.ActorTemplate, snapshotID string, targetRung ateapipb.SurvivabilityRung) (*ateapipb.Snapshot, error)
	AssignToNode(ctx context.Context, actor *ateapipb.Actor, targetNode string) error
	UnassignFromNode(ctx context.Context, actor *ateapipb.Actor, assignedNode string) error
	DeleteSnapshot(ctx context.Context, actor *ateapipb.Actor, snap *ateapipb.Snapshot) error
	DeleteActor(ctx context.Context, actor *ateapipb.Actor) error
	ReserveTagSnapshot(ctx context.Context, tag *ateapipb.Tag, sourceActor *ateapipb.Actor, tmpl *ateapipb.ActorTemplate) (*ateapipb.Snapshot, error)
	CopyToTagSnapshot(ctx context.Context, tag *ateapipb.Tag, sourceSnapshot *ateapipb.Snapshot) error
	DeleteTagSnapshot(ctx context.Context, tag *ateapipb.Tag) error
}

// SnapshotPluginWorkerPlane abstracts node-local and remote snapshot storage
// operations on the worker node without invoking the sandbox runtime directly.
type SnapshotPluginWorkerPlane interface {
	PrepareSnapshotStorage(ctx context.Context, actorUID string, storage *ateapipb.SnapshotStorage) error
	FetchRestoreManifests(ctx context.Context, req *ateletpb.RestoreRequest) (actorManifest, goldenManifest []byte, err error)
	PrepareRestoreDir(ctx context.Context, req *ateletpb.RestoreRequest) (restoreDir string, err error)
	PrepareCheckpointDir(ctx context.Context, req *ateletpb.CheckpointRequest) (checkpointWriteDir string, err error)
	CommitCheckpoint(ctx context.Context, req *ateletpb.CheckpointRequest, checkpointWriteDir string) (ateapipb.SurvivabilityRung, error)
	EscalateCheckpoint(ctx context.Context, req *ateletpb.CheckpointRequest) (ateapipb.SurvivabilityRung, error)
	DetachCheckpointDir(ctx context.Context, actorUID string, storageID string) error
}
