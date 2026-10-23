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
	"bytes"
	"compress/gzip"
	_ "embed"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"golang.org/x/sys/unix"
)

//go:embed ext4_template.bin.gz
var ext4TemplateGzip []byte

const (
	lvmVGDir            = "/host/dev/vg_snapshots"
	lvmVolumeSize       = 5056 << 20 // 5056 MiB per LV on the dedicated snapshot disk
	loopPoolDefaultSize = 20
	metadataResetSize   = 4 << 20 // 4 MiB covers all flex_bg bitmaps and inode tables
)

type sparseExtent struct {
	offset int64
	data   []byte
}

var (
	loadExt4Template = sync.OnceValues(func() ([]sparseExtent, error) {
		zr, err := gzip.NewReader(bytes.NewReader(ext4TemplateGzip))
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		raw, err := io.ReadAll(zr)
		if err != nil {
			return nil, err
		}
		var extents []sparseExtent
		for len(raw) >= 16 {
			off := int64(binary.LittleEndian.Uint64(raw[0:8]))
			n := int(binary.LittleEndian.Uint64(raw[8:16]))
			raw = raw[16:]
			if len(raw) < n {
				return nil, fmt.Errorf("truncated ext4 template extent at offset %d", off)
			}
			extents = append(extents, sparseExtent{
				offset: off,
				data:   raw[:n],
			})
			raw = raw[n:]
		}
		return extents, nil
	})

	zeroChunk = make([]byte, 4<<20) // 4 MiB zero buffer

	loopPoolMu       sync.Mutex
	loopPoolFree     []string
	loopPoolAssigned = make(map[string]string) // actorUID -> LVM device path
	initLoopPoolOnce sync.Once
)

func PrewarmLoopPool() {
	if _, err := os.Stat(lvmVGDir); err == nil {
		initLoopPoolOnce.Do(initLoopPool)
	}
}

// resetAndWriteTemplate zeroes the first 4 MiB of the LVM block device (wiping
// all flex_bg block/inode bitmaps, inode tables, root dir, and superblock) and
// stamps the fresh journal-less ext4 template onto it.
func resetAndWriteTemplate(f *os.File, extents []sparseExtent) error {
	for off := int64(0); off < metadataResetSize; off += int64(len(zeroChunk)) {
		n := int64(len(zeroChunk))
		if off+n > metadataResetSize {
			n = metadataResetSize - off
		}
		if _, err := f.WriteAt(zeroChunk[:n], off); err != nil {
			return fmt.Errorf("zeroing metadata region at %d: %w", off, err)
		}
	}
	for _, ext := range extents {
		if _, err := f.WriteAt(ext.data, ext.offset); err != nil {
			return fmt.Errorf("writing ext4 template extent at %d: %w", ext.offset, err)
		}
	}
	if err := unix.Fdatasync(int(f.Fd())); err != nil {
		return fmt.Errorf("fdatasync lvm volume: %w", err)
	}
	_ = unix.Fadvise(int(f.Fd()), 0, lvmVolumeSize, unix.FADV_DONTNEED)
	return nil
}

func prewarmLVMVolume(lvPath string, extents []sparseExtent) error {
	f, err := os.OpenFile(lvPath, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	return resetAndWriteTemplate(f, extents)
}

func initLoopPool() {
	start := time.Now()

	// Unmount any orphaned snapshot mounts left over from dead actors.
	snapshotsRoot := filepath.Dir(ateletpath.ActorSnapshotPath("dummy"))
	if entries, err := os.ReadDir(snapshotsRoot); err == nil {
		for _, entry := range entries {
			uid := entry.Name()
			if _, err := os.Stat(ateletpath.ActorPath(uid)); errors.Is(err, os.ErrNotExist) {
				snapDir := ateletpath.ActorSnapshotPath(uid)
				if mounted, _ := isMountPoint(snapDir); mounted {
					_ = unix.Unmount(snapDir, unix.MNT_DETACH)
				}
				_ = os.RemoveAll(snapDir)
			}
		}
	}

	extents, err := loadExt4Template()
	if err != nil {
		slog.Error("Failed to load ext4 template for LVM pool", "err", err)
		return
	}

	slog.Info("Pre-warming snapshot LVM logical volume pool",
		"vg_dir", lvmVGDir,
		"slots", loopPoolDefaultSize,
		"slot_size_mib", lvmVolumeSize>>20,
	)
	for i := 0; i < loopPoolDefaultSize; i++ {
		slotPath := filepath.Join(lvmVGDir, fmt.Sprintf("slot-%02d", i))
		slotStart := time.Now()
		if err := prewarmLVMVolume(slotPath, extents); err != nil {
			slog.Error("Failed to initialize LVM pool slot", "slot", slotPath, "err", err)
			continue
		}
		loopPoolMu.Lock()
		loopPoolFree = append(loopPoolFree, slotPath)
		freeCount := len(loopPoolFree)
		loopPoolMu.Unlock()
		slog.Info("Pre-warmed snapshot loop image slot",
			"slot", slotPath,
			"free_slots", freeCount,
			"duration", time.Since(slotStart),
		)
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
	if lvPath, ok := loopPoolAssigned[actorUID]; ok {
		return lvPath, nil
	}
	if n := len(loopPoolFree); n > 0 {
		lvPath := loopPoolFree[n-1]
		loopPoolFree = loopPoolFree[:n-1]
		loopPoolAssigned[actorUID] = lvPath
		return lvPath, nil
	}
	return "", fmt.Errorf("snapshot LVM pool exhausted for actor %s", actorUID)
}

func releaseLVMVolume(actorUID string) {
	loopPoolMu.Lock()
	lvPath, ok := loopPoolAssigned[actorUID]
	if !ok {
		loopPoolMu.Unlock()
		return
	}
	delete(loopPoolAssigned, actorUID)
	loopPoolMu.Unlock()

	extents, err := loadExt4Template()
	if err == nil {
		if f, err := os.OpenFile(lvPath, os.O_RDWR, 0o600); err == nil {
			_ = resetAndWriteTemplate(f, extents)
			_ = f.Close()
		}
	}

	loopPoolMu.Lock()
	loopPoolFree = append(loopPoolFree, lvPath)
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

func ensureSnapshotLoopMount(actorUID string) error {
	if _, err := os.Stat(lvmVGDir); err != nil {
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

	lvPath, err := acquireLVMVolume(actorUID)
	if err != nil {
		return err
	}

	if err := unix.Mount(lvPath, snapDir, "ext4", unix.MS_NOATIME|unix.MS_NODIRATIME, "nobarrier,nodiscard,noauto_da_alloc"); err != nil {
		releaseLVMVolume(actorUID)
		return fmt.Errorf("mounting %s on %s: %w", lvPath, snapDir, err)
	}
	slog.Info("Mounted per-actor snapshot LVM logical volume",
		"actor_uid", actorUID,
		"device", lvPath,
	)
	return nil
}

func teardownSnapshotLoopMount(actorUID string) error {
	if _, err := os.Stat(lvmVGDir); err != nil {
		return nil
	}
	snapDir := ateletpath.ActorSnapshotPath(actorUID)
	mounted, err := isMountPoint(snapDir)
	if err != nil {
		return fmt.Errorf("checking snapshot mountpoint %s: %w", snapDir, err)
	}
	if mounted {
		if err := unix.Unmount(snapDir, unix.MNT_DETACH); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
			return fmt.Errorf("unmounting snapshot LVM mount %s: %w", snapDir, err)
		}
	}
	releaseLVMVolume(actorUID)
	return nil
}
