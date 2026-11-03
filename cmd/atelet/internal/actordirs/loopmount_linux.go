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

//go:build linux

package actordirs

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/nodepath"
	"golang.org/x/sys/unix"
)

const (
	hyperdiskByIDGlob = "/host/dev/disk/by-id/google-ate-actor-disk-*"
	snapshotPoolDir   = "snapshot-pool"
)

var (
	loopPoolMu       sync.Mutex
	loopPoolFree     []string
	loopPoolAssigned = make(map[string]string) // actorUID -> pre-mounted slotDir path
	initLoopPoolOnce sync.Once
)

func hasSnapshotPool() bool {
	matches, err := filepath.Glob(hyperdiskByIDGlob)
	return err == nil && len(matches) > 0
}

func PrewarmLoopPool() {
	if hasSnapshotPool() {
		initLoopPoolOnce.Do(initLoopPool)
	}
}

func wipeSlotDir(slotDir string) {
	entries, err := os.ReadDir(slotDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.Name() == "lost+found" {
			continue
		}
		_ = imagecache.RemoveAllWritable(filepath.Join(slotDir, entry.Name()))
	}
}

func initLoopPool() {
	start := time.Now()

	snapshotsRoot := filepath.Dir(ateletpath.ActorSnapshotPath("dummy"))
	if entries, err := os.ReadDir(snapshotsRoot); err == nil {
		for _, entry := range entries {
			uid := entry.Name()
			if uid == ".cache" || uid == "lost+found" {
				continue
			}
			snapDir := ateletpath.ActorSnapshotPath(uid)
			if mounted, _ := isMountPoint(snapDir); mounted {
				_ = unix.Unmount(snapDir, unix.MNT_DETACH)
			}
			if _, err := os.Stat(ateletpath.ActorPath(uid)); errors.Is(err, os.ErrNotExist) {
				_ = os.RemoveAll(snapDir)
			}
		}
	}

	poolRoot := filepath.Join(nodepath.BasePath, snapshotPoolDir)
	if entries, err := os.ReadDir(poolRoot); err == nil {
		for _, entry := range entries {
			slotDir := filepath.Join(poolRoot, entry.Name())
			if mounted, _ := isMountPoint(slotDir); mounted {
				_ = unix.Unmount(slotDir, unix.MNT_DETACH)
			}
		}
	}
	_ = os.RemoveAll(poolRoot)
	if err := os.MkdirAll(poolRoot, 0o755); err != nil {
		slog.Error("Failed to create snapshot-pool directory", "path", poolRoot, "err", err)
		return
	}

	matches, err := filepath.Glob(hyperdiskByIDGlob)
	if err != nil || len(matches) == 0 {
		slog.Error("No per-actor Hyperdisks found for pool", "glob", hyperdiskByIDGlob, "err", err)
		return
	}
	sort.Sort(sort.Reverse(sort.StringSlice(matches)))

	for _, byID := range matches {
		devPath, err := filepath.EvalSymlinks(byID)
		if err != nil {
			slog.Error("Failed to resolve Hyperdisk symlink", "path", byID, "err", err)
			continue
		}
		if strings.HasPrefix(devPath, "/dev/") {
			devPath = "/host" + devPath
		}
		suffix := strings.TrimPrefix(filepath.Base(byID), "google-ate-actor-disk-")
		slotDir := filepath.Join(poolRoot, "slot-"+suffix)
		if err := os.MkdirAll(slotDir, 0o755); err != nil {
			slog.Error("Failed to create slot mountpoint", "slot", slotDir, "err", err)
			continue
		}
		if err := unix.Mount(devPath, slotDir, "xfs", unix.MS_NOATIME|unix.MS_NODIRATIME, "nodiscard,nouuid"); err != nil {
			slog.Error("Failed to mount per-actor Hyperdisk at pool slot", "device", devPath, "slot", slotDir, "err", err)
			continue
		}
		wipeSlotDir(slotDir)
		loopPoolFree = append(loopPoolFree, slotDir)
	}

	slog.Info("Finished pre-warming snapshot loop image pool",
		"slots", len(loopPoolFree),
		"duration", time.Since(start),
	)
}

func acquireLVMVolume(actorUID string) (string, error) {
	initLoopPoolOnce.Do(initLoopPool)

	loopPoolMu.Lock()
	defer loopPoolMu.Unlock()
	if slotDir, ok := loopPoolAssigned[actorUID]; ok {
		return slotDir, nil
	}
	if n := len(loopPoolFree); n > 0 {
		slotDir := loopPoolFree[n-1]
		loopPoolFree = loopPoolFree[:n-1]
		loopPoolAssigned[actorUID] = slotDir
		return slotDir, nil
	}
	return "", fmt.Errorf("snapshot Hyperdisk pool exhausted for actor %s", actorUID)
}

func releaseLVMVolume(actorUID string) {
	loopPoolMu.Lock()
	slotDir, ok := loopPoolAssigned[actorUID]
	if !ok {
		loopPoolMu.Unlock()
		return
	}
	delete(loopPoolAssigned, actorUID)
	loopPoolMu.Unlock()

	wipeSlotDir(slotDir)

	loopPoolMu.Lock()
	loopPoolFree = append(loopPoolFree, slotDir)
	loopPoolMu.Unlock()
}

func isMountPoint(dir string) (bool, error) {
	var dirStat, parentStat unix.Stat_t
	if err := unix.Stat(dir, &dirStat); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return false, nil
		}
		return false, err
	}
	if err := unix.Stat(filepath.Dir(dir), &parentStat); err != nil {
		return false, err
	}
	return dirStat.Dev != parentStat.Dev, nil
}

func EnsureSnapshotLoopMount(actorUID string) error {
	return ensureSnapshotLoopMount(actorUID)
}

func TeardownSnapshotLoopMount(actorUID string) error {
	return teardownSnapshotLoopMount(actorUID)
}

func ensureSnapshotLoopMount(actorUID string) error {
	if !hasSnapshotPool() {
		return nil
	}
	snapDir := ateletpath.ActorSnapshotPath(actorUID)
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		return fmt.Errorf("creating snapshot mountpoint: %w", err)
	}
	mounted, err := isMountPoint(snapDir)
	if err != nil {
		return fmt.Errorf("checking snapshot mountpoint: %w", err)
	}
	if mounted {
		return nil
	}

	slotDir, err := acquireLVMVolume(actorUID)
	if err != nil {
		return err
	}

	if err := unix.Mount(slotDir, snapDir, "", unix.MS_BIND, ""); err != nil {
		releaseLVMVolume(actorUID)
		return fmt.Errorf("bind-mounting %s on %s: %w", slotDir, snapDir, err)
	}
	return nil
}

func teardownSnapshotLoopMount(actorUID string) error {
	if !hasSnapshotPool() {
		return nil
	}
	snapDir := ateletpath.ActorSnapshotPath(actorUID)
	mounted, err := isMountPoint(snapDir)
	if err != nil {
		return fmt.Errorf("checking snapshot mountpoint %s: %w", snapDir, err)
	}
	if mounted {
		if err := unix.Unmount(snapDir, 0); err != nil {
			if err := unix.Unmount(snapDir, unix.MNT_DETACH); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
				return fmt.Errorf("unmounting snapshot bind mount %s: %w", snapDir, err)
			}
		}
	}
	releaseLVMVolume(actorUID)
	return nil
}
