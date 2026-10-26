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
var btrfsTemplateGzip []byte

const (
	lvmVGDir            = "/host/dev/vg_snapshots"
	lvmVolumeSize       = 2200 << 20 // 2200 MiB per LV on the dedicated snapshot disk
	loopPoolDefaultSize = 160
)

type sparseExtent struct {
	offset int64
	data   []byte
}

type lvmSlot struct {
	index int
	path  string
}

var (
	loadBtrfsTemplates = sync.OnceValues(func() ([][]sparseExtent, error) {
		zr, err := gzip.NewReader(bytes.NewReader(btrfsTemplateGzip))
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		raw, err := io.ReadAll(zr)
		if err != nil {
			return nil, err
		}
		var slots [][]sparseExtent
		for len(raw) >= 8 {
			count := int(binary.LittleEndian.Uint64(raw[0:8]))
			raw = raw[8:]
			var extents []sparseExtent
			for j := 0; j < count; j++ {
				if len(raw) < 16 {
					return nil, fmt.Errorf("truncated btrfs extent header in slot %d", len(slots))
				}
				off := int64(binary.LittleEndian.Uint64(raw[0:8]))
				n := int(binary.LittleEndian.Uint64(raw[8:16]))
				raw = raw[16:]
				if len(raw) < n {
					return nil, fmt.Errorf("truncated btrfs template extent at offset %d", off)
				}
				extents = append(extents, sparseExtent{
					offset: off,
					data:   raw[:n],
				})
				raw = raw[n:]
			}
			slots = append(slots, extents)
		}
		return slots, nil
	})

	loopPoolMu       sync.Mutex
	loopPoolFree     []lvmSlot
	loopPoolAssigned = make(map[string]lvmSlot) // actorUID -> lvmSlot
	initLoopPoolOnce sync.Once
)

func PrewarmLoopPool() {
	if _, err := os.Stat(lvmVGDir); err == nil {
		initLoopPoolOnce.Do(initLoopPool)
	}
}

// resetAndWriteTemplate stamps the slot's clean btrfs superblocks and initial
// B-tree roots onto the LVM block device.
func resetAndWriteTemplate(f *os.File, extents []sparseExtent) error {
	for _, ext := range extents {
		if _, err := f.WriteAt(ext.data, ext.offset); err != nil {
			return fmt.Errorf("writing btrfs template extent at %d: %w", ext.offset, err)
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

	// Unmount any snapshot mounts left over from a previous atelet run before
	// reformatting all LVM slots into loopPoolFree.
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

	slotTemplates, err := loadBtrfsTemplates()
	if err != nil {
		slog.Error("Failed to load btrfs templates for LVM pool", "err", err)
		return
	}

	slog.Info("Pre-warming snapshot LVM btrfs logical volume pool",
		"vg_dir", lvmVGDir,
		"slots", loopPoolDefaultSize,
		"slot_size_mib", lvmVolumeSize>>20,
	)
	totalSlots := loopPoolDefaultSize
	if len(slotTemplates) < totalSlots {
		totalSlots = len(slotTemplates)
	}
	ch := make(chan int, totalSlots)
	for i := 0; i < totalSlots; i++ {
		ch <- i
	}
	close(ch)

	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range ch {
				slotPath := filepath.Join(lvmVGDir, fmt.Sprintf("slot-%02d", i))
				if err := prewarmLVMVolume(slotPath, slotTemplates[i]); err != nil {
					slog.Error("Failed to initialize LVM btrfs pool slot", "slot", slotPath, "err", err)
					continue
				}
				loopPoolMu.Lock()
				loopPoolFree = append(loopPoolFree, lvmSlot{index: i, path: slotPath})
				loopPoolMu.Unlock()
			}
		}()
	}
	wg.Wait()
	slog.Info("Finished pre-warming snapshot loop image pool",
		"slots", len(loopPoolFree),
		"duration", time.Since(start),
	)
}

func acquireLVMVolume(actorUID string) (lvmSlot, error) {
	initLoopPoolOnce.Do(initLoopPool)

	loopPoolMu.Lock()
	defer loopPoolMu.Unlock()
	if slot, ok := loopPoolAssigned[actorUID]; ok {
		return slot, nil
	}
	if n := len(loopPoolFree); n > 0 {
		slot := loopPoolFree[n-1]
		loopPoolFree = loopPoolFree[:n-1]
		loopPoolAssigned[actorUID] = slot
		return slot, nil
	}
	return lvmSlot{}, fmt.Errorf("snapshot LVM pool exhausted for actor %s", actorUID)
}

func releaseLVMVolume(actorUID string) {
	loopPoolMu.Lock()
	slot, ok := loopPoolAssigned[actorUID]
	if !ok {
		loopPoolMu.Unlock()
		return
	}
	delete(loopPoolAssigned, actorUID)
	loopPoolMu.Unlock()

	if slotTemplates, err := loadBtrfsTemplates(); err == nil && slot.index < len(slotTemplates) {
		if f, err := os.OpenFile(slot.path, os.O_RDWR, 0o600); err == nil {
			_ = resetAndWriteTemplate(f, slotTemplates[slot.index])
			_ = f.Close()
		}
	}

	loopPoolMu.Lock()
	loopPoolFree = append(loopPoolFree, slot)
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

	slot, err := acquireLVMVolume(actorUID)
	if err != nil {
		return err
	}

	if err := unix.Mount(slot.path, snapDir, "btrfs", unix.MS_NOATIME|unix.MS_NODIRATIME, "nodiscard,nodatasum,flushoncommit,commit=5"); err != nil {
		releaseLVMVolume(actorUID)
		return fmt.Errorf("mounting btrfs %s on %s: %w", slot.path, snapDir, err)
	}
	slog.Info("Mounted per-actor snapshot LVM btrfs logical volume",
		"actor_uid", actorUID,
		"device", slot.path,
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
		if err := unix.Unmount(snapDir, 0); err != nil {
			if err := unix.Unmount(snapDir, unix.MNT_DETACH); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
				return fmt.Errorf("unmounting snapshot LVM mount %s: %w", snapDir, err)
			}
		}
	}
	releaseLVMVolume(actorUID)
	return nil
}
