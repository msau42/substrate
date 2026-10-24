//go:build linux

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

// The paths ateworker-microvm derives from the ActorDirs atelet sends. The
// directory under root_dir is ateworker-microvm's own: atelet does not know it.

package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.ate.dev/substrate/internal/btrfssubvol"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"golang.org/x/sys/unix"
)

const (
	guestMemoryZoneID        = "mem0"
	btrfsCheckpointTokenFile = ".btrfs-checkpoint-token"
)

// ociBundlePath is the container's OCI bundle.
func ociBundlePath(actorDirs *ateworkerpb.ActorDirs, containerName string) string {
	return filepath.Join(actorDirs.GetOciBundleDir(), containerName)
}

func snapshotRootDir(actorDirs *ateworkerpb.ActorDirs) string {
	if c := actorDirs.GetCheckpointDir(); c != "" {
		return filepath.Dir(c)
	}
	return actorDirs.GetRootDir()
}

func isBtrfsSnapshotDir(actorDirs *ateworkerpb.ActorDirs) bool {
	return btrfssubvol.IsBtrfs(snapshotRootDir(actorDirs))
}

func activeSubvolDir(actorDirs *ateworkerpb.ActorDirs) string {
	return filepath.Join(snapshotRootDir(actorDirs), "active")
}

// guestMemoryFile returns the file-backed shared memory zone path inside the
// active btrfs subvolume when the actor's snapshot directory is a btrfs
// filesystem, or "" when guest RAM is memfd-backed.
func guestMemoryFile(actorDirs *ateworkerpb.ActorDirs) string {
	if isBtrfsSnapshotDir(actorDirs) {
		return filepath.Join(activeSubvolDir(actorDirs), memoryRangesFile)
	}
	return ""
}

func prepareGuestMemoryFile(path string, sizeBytes int64) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("while creating guest memory file %s: %w", path, err)
	}
	if err := f.Truncate(sizeBytes); err != nil {
		_ = f.Close()
		return fmt.Errorf("while sizing guest memory file %s: %w", path, err)
	}
	return f.Close()
}

func stageGuestMemoryFileForRestore(restoreDir, activeDir string) error {
	srcPath := filepath.Join(restoreDir, memoryRangesFile)
	src, err := os.Open(srcPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("while opening restore memory file %s: %w", srcPath, err)
	}
	defer src.Close()

	dstPath := filepath.Join(activeDir, memoryRangesFile)
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("while creating active memory file %s: %w", dstPath, err)
	}
	defer dst.Close()

	if err := unix.IoctlFileClone(int(dst.Fd()), int(src.Fd())); err == nil {
		return nil
	}
	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("while copying restore memory file to %s: %w", dstPath, err)
	}
	return nil
}

// rootfsUpperDir is the host directory backing the actor's rootfs overlay
// uppers: one subdirectory per container (see kata.UpperWorkDirs). When the
// actor's snapshot directory is a btrfs filesystem, it lives inside the active
// subvolume so checkpoints snapshot it via Copy-on-Write without tarring.
func rootfsUpperDir(actorDirs *ateworkerpb.ActorDirs) string {
	if isBtrfsSnapshotDir(actorDirs) {
		return filepath.Join(activeSubvolDir(actorDirs), "rootfs-upper")
	}
	return filepath.Join(actorDirs.GetRootDir(), "rootfs-upper")
}

// durableMountsDir returns the directory backing the actor's durable-dir
// volumes. On btrfs snapshot filesystems it lives inside the active subvolume.
func durableMountsDir(actorDirs *ateworkerpb.ActorDirs) string {
	if isBtrfsSnapshotDir(actorDirs) {
		return filepath.Join(activeSubvolDir(actorDirs), "durable-dir")
	}
	return actorDirs.GetDurableDirVolumeMountsDir()
}

func ensureDurableMountDirs(durableDir string, containers []*ateworkerpb.Container) error {
	names := durableVolumeNames(containers)
	if len(names) == 0 {
		return nil
	}
	if err := os.MkdirAll(durableDir, 0o755); err != nil {
		return fmt.Errorf("while creating durable-dir root %s: %w", durableDir, err)
	}
	for _, name := range names {
		volDir := filepath.Join(durableDir, name)
		if _, err := os.Stat(volDir); err == nil {
			continue
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("while checking durable-dir volume %s: %w", volDir, err)
		}
		if err := os.MkdirAll(volDir, 0o777); err != nil {
			return fmt.Errorf("while creating durable-dir volume %s: %w", volDir, err)
		}
		if err := os.Chmod(volDir, 0o777); err != nil {
			return fmt.Errorf("while setting mode on durable-dir volume %s: %w", volDir, err)
		}
	}
	return nil
}

func prepareActiveSubvolForRun(actorDirs *ateworkerpb.ActorDirs, containers []*ateworkerpb.Container) error {
	if !isBtrfsSnapshotDir(actorDirs) {
		return nil
	}
	if err := btrfssubvol.Create(activeSubvolDir(actorDirs)); err != nil {
		return fmt.Errorf("while creating active btrfs subvolume: %w", err)
	}
	return ensureDurableMountDirs(durableMountsDir(actorDirs), containers)
}

func writeBtrfsCheckpointToken(dir string) error {
	nonce, err := newReseedNonce()
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, btrfsCheckpointTokenFile), fmt.Appendf(nil, "%x\n", nonce), 0o600)
}

func canReuseActiveSubvol(activeDir, restoreDir string) bool {
	if !btrfssubvol.IsSubvolume(activeDir) || !btrfssubvol.IsSubvolume(restoreDir) {
		return false
	}
	activeTok, err := os.ReadFile(filepath.Join(activeDir, btrfsCheckpointTokenFile))
	if err != nil || len(activeTok) == 0 {
		return false
	}
	restoreTok, err := os.ReadFile(filepath.Join(restoreDir, btrfsCheckpointTokenFile))
	if err != nil || len(restoreTok) == 0 {
		return false
	}
	return bytes.Equal(activeTok, restoreTok)
}

func prepareActiveSubvolForRestore(actorDirs *ateworkerpb.ActorDirs, restoreDir string, containers []*ateworkerpb.Container) error {
	if !isBtrfsSnapshotDir(actorDirs) {
		return nil
	}
	activeDir := activeSubvolDir(actorDirs)
	if btrfssubvol.IsSubvolume(restoreDir) {
		if !canReuseActiveSubvol(activeDir, restoreDir) {
			if err := btrfssubvol.Snapshot(restoreDir, activeDir); err != nil {
				return fmt.Errorf("while snapshotting restore subvolume %s to %s: %w", restoreDir, activeDir, err)
			}
		}
		_ = os.Remove(filepath.Join(activeDir, btrfsCheckpointTokenFile))
		_ = os.Remove(filepath.Join(activeDir, "manifest.json"))
	} else {
		if err := btrfssubvol.Create(activeDir); err != nil {
			return fmt.Errorf("while creating active btrfs subvolume: %w", err)
		}
		if err := stageGuestMemoryFileForRestore(restoreDir, activeDir); err != nil {
			return err
		}
	}
	return ensureDurableMountDirs(durableMountsDir(actorDirs), containers)
}

func cleanupActiveOrRootfsUpper(actorDirs *ateworkerpb.ActorDirs) error {
	if isBtrfsSnapshotDir(actorDirs) {
		return btrfssubvol.RemoveAll(activeSubvolDir(actorDirs))
	}
	return os.RemoveAll(rootfsUpperDir(actorDirs))
}
