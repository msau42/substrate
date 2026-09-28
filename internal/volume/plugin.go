// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package volume

import (
	"context"
)

// SourceSnapshotIDParameterKey is an optional parameter key passed to
// CreateVolume to clone a new volume from an existing CSI volume snapshot.
const SourceSnapshotIDParameterKey = "substrate.io/source-snapshot-id"

// VolumePluginControlPlane abstracts storage operations performed on the control plane.
type VolumePluginControlPlane interface {
	DriverName(ctx context.Context) (string, error)
	CreateVolume(ctx context.Context, name string, capacity string, driverName string, parameters map[string]string) (volumeID string, volumeContext map[string]string, err error)
	DeleteVolume(ctx context.Context, volumeID string) error
	AttachVolume(ctx context.Context, volumeID string, node string) error
	DetachVolume(ctx context.Context, volumeID string, node string) error
	CreateSnapshot(ctx context.Context, name string, sourceVolumeID string, parameters map[string]string) (snapshotID string, err error)
	DeleteSnapshot(ctx context.Context, snapshotID string) error
}

// VolumePluginWorkerPlane abstracts storage operations performed on worker nodes.
type VolumePluginWorkerPlane interface {
	MountVolume(ctx context.Context, volumeID string, targetPath string, volumeContext map[string]string) error
	UnmountVolume(ctx context.Context, volumeID string, targetPath string) error
}
