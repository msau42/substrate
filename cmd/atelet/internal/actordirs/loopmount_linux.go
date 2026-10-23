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
	"strings"
	"sync"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"golang.org/x/sys/unix"
)

//go:embed ext4_template.bin.gz
var ext4TemplateGzip []byte

const (
	loopControlPath      = "/host/dev/loop-control"
	loopImageSize        = 16384 << 20 // 16 GiB single-flex_bg fs so free_clusters >> 2*dirty_clusters
	prewarmAllocatedSize = 3300 << 20  // 3300 MiB physically pre-written per slot (66 GiB across 20 slots)
	loopPoolDefaultSize  = 20
	metadataResetSize    = 4 << 20 // 4 MiB covers all 128 flex_bg bitmaps and inode tables (520 blocks)
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
	loopDevAllocMu   sync.Mutex
	loopPoolFree     []string
	loopPoolAssigned = make(map[string]string) // actorUID -> image file path
	initLoopPoolOnce sync.Once
)

func PrewarmLoopPool() {
	if _, err := os.Stat(loopControlPath); err == nil {
		initLoopPoolOnce.Do(initLoopPool)
	}
}

func loopPoolDir() string {
	baseDir := filepath.Dir(filepath.Dir(ateletpath.ActorSnapshotPath("dummy")))
	return filepath.Join(baseDir, "loop-pool")
}

func legacyLoopImagesDir() string {
	baseDir := filepath.Dir(filepath.Dir(ateletpath.ActorSnapshotPath("dummy")))
	return filepath.Join(baseDir, "loop-images")
}

// resetAndWriteTemplate zeroes the first 4 MiB of f (wiping all flex_bg
// block/inode bitmaps, inode tables, root dir, and superblock) and stamps the
// fresh journal-less ext4 template onto the pre-warmed file, then flushes data
// and drops host page cache pages before LO_FLAGS_DIRECT_IO attaches.
func resetAndWriteTemplate(f *os.File, extents []sparseExtent) error {
	if err := f.Truncate(loopImageSize); err != nil {
		return fmt.Errorf("truncating loop image to %d: %w", loopImageSize, err)
	}
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
		return fmt.Errorf("fdatasync loop image: %w", err)
	}
	_ = unix.Fadvise(int(f.Fd()), 0, loopImageSize, unix.FADV_DONTNEED)
	return nil
}

// prewarmImageFile ensures imgPath is a pre-allocated file of logical size
// loopImageSize (with at least prewarmAllocatedSize of written extents) and a
// fresh ext4 template stamped onto it.
func prewarmImageFile(imgPath string, extents []sparseExtent) error {
	var st unix.Stat_t
	needFullWrite := true
	if err := unix.Stat(imgPath, &st); err == nil {
		// st.Blocks is in 512-byte units. Check that allocated blocks match
		// prewarmAllocatedSize.
		if st.Blocks*512 >= prewarmAllocatedSize-(16<<20) {
			needFullWrite = false
		}
	}

	if !needFullWrite {
		f, err := os.OpenFile(imgPath, os.O_RDWR, 0o600)
		if err != nil {
			return err
		}
		defer f.Close()
		return resetAndWriteTemplate(f, extents)
	}

	tmpPath := imgPath + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("creating %s: %w", tmpPath, err)
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(tmpPath)
	}()

	// Write real zero bytes sequentially through buffered I/O so host ext4
	// delayed allocation allocates contiguous written (!EXT4_EXT_UNWRITTEN)
	// extents. Subsequent LO_FLAGS_DIRECT_IO writes take the fast overwrite
	// path in ext4_dio_write_iter without unwritten-extent conversion or host
	// jbd2 transactions.
	for off := int64(0); off < prewarmAllocatedSize; off += int64(len(zeroChunk)) {
		n := int64(len(zeroChunk))
		if off+n > prewarmAllocatedSize {
			n = prewarmAllocatedSize - off
		}
		if _, err := f.Write(zeroChunk[:n]); err != nil {
			return fmt.Errorf("pre-warming %s at %d: %w", tmpPath, off, err)
		}
	}
	if err := resetAndWriteTemplate(f, extents); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, imgPath); err != nil {
		return fmt.Errorf("renaming %s to %s: %w", tmpPath, imgPath, err)
	}
	return nil
}

func initLoopPool() {
	start := time.Now()
	// Remove legacy sparse loop-images directory from earlier prototype runs to
	// reclaim disk space before pre-warming the pool.
	_ = os.RemoveAll(legacyLoopImagesDir())

	// Unmount any orphaned snapshot loop mounts left over from dead actors.
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

	poolDir := loopPoolDir()
	if err := os.MkdirAll(poolDir, 0o700); err != nil {
		slog.Error("Failed to create loop-pool dir", "err", err)
		return
	}
	extents, err := loadExt4Template()
	if err != nil {
		slog.Error("Failed to load ext4 template for loop pool", "err", err)
		return
	}

	// Remove any leftover files in poolDir that aren't slot-00..slot-19.img.
	validSlots := make(map[string]bool, loopPoolDefaultSize)
	for i := 0; i < loopPoolDefaultSize; i++ {
		validSlots[fmt.Sprintf("slot-%02d.img", i)] = true
	}
	if entries, err := os.ReadDir(poolDir); err == nil {
		for _, entry := range entries {
			if !validSlots[entry.Name()] {
				_ = os.RemoveAll(filepath.Join(poolDir, entry.Name()))
			}
		}
	}

	slog.Info("Pre-warming snapshot loop image pool",
		"dir", poolDir,
		"slots", loopPoolDefaultSize,
		"slot_size_mib", loopImageSize>>20,
	)
	for i := 0; i < loopPoolDefaultSize; i++ {
		slotPath := filepath.Join(poolDir, fmt.Sprintf("slot-%02d.img", i))
		slotStart := time.Now()
		if err := prewarmImageFile(slotPath, extents); err != nil {
			slog.Error("Failed to pre-warm loop pool slot", "slot", slotPath, "err", err)
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

func acquireLoopImage(actorUID string, extents []sparseExtent) (string, error) {
	initLoopPoolOnce.Do(initLoopPool)

	loopPoolMu.Lock()
	if imgPath, ok := loopPoolAssigned[actorUID]; ok {
		loopPoolMu.Unlock()
		return imgPath, nil
	}
	if n := len(loopPoolFree); n > 0 {
		imgPath := loopPoolFree[n-1]
		loopPoolFree = loopPoolFree[:n-1]
		loopPoolAssigned[actorUID] = imgPath
		loopPoolMu.Unlock()
		return imgPath, nil
	}
	loopPoolMu.Unlock()

	// Pool exhausted (> loopPoolDefaultSize concurrent actors); pre-warm an overflow image.
	overflowPath := filepath.Join(loopPoolDir(), fmt.Sprintf("overflow-%s.img", actorUID))
	slog.Warn("Snapshot loop pool exhausted; pre-warming overflow image",
		"actor_uid", actorUID,
		"path", overflowPath,
	)
	if err := prewarmImageFile(overflowPath, extents); err != nil {
		return "", err
	}
	loopPoolMu.Lock()
	loopPoolAssigned[actorUID] = overflowPath
	loopPoolMu.Unlock()
	return overflowPath, nil
}

func releaseLoopImage(actorUID string) {
	loopPoolMu.Lock()
	imgPath, ok := loopPoolAssigned[actorUID]
	if !ok {
		loopPoolMu.Unlock()
		return
	}
	delete(loopPoolAssigned, actorUID)
	loopPoolMu.Unlock()

	if strings.HasPrefix(filepath.Base(imgPath), "overflow-") {
		_ = os.Remove(imgPath)
		return
	}

	extents, err := loadExt4Template()
	if err == nil {
		if f, err := os.OpenFile(imgPath, os.O_RDWR, 0o600); err == nil {
			_ = resetAndWriteTemplate(f, extents)
			_ = f.Close()
		}
	}

	loopPoolMu.Lock()
	loopPoolFree = append(loopPoolFree, imgPath)
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
	if _, err := os.Stat(loopControlPath); err != nil {
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

	extents, err := loadExt4Template()
	if err != nil {
		return fmt.Errorf("loading ext4 template: %w", err)
	}

	imgPath, err := acquireLoopImage(actorUID, extents)
	if err != nil {
		return fmt.Errorf("acquiring loop image for %s: %w", actorUID, err)
	}

	imgFile, err := os.OpenFile(imgPath, os.O_RDWR, 0o600)
	if err != nil {
		releaseLoopImage(actorUID)
		return fmt.Errorf("opening loop image %s: %w", imgPath, err)
	}
	defer imgFile.Close()

	loopDevAllocMu.Lock()
	defer loopDevAllocMu.Unlock()

	ctlFile, err := os.OpenFile(loopControlPath, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("opening %s: %w", loopControlPath, err)
	}
	defer ctlFile.Close()

	var (
		loopFile    *os.File
		loopDevPath string
		directIO    bool
	)
	for attempt := 0; attempt < 64; attempt++ {
		loopNum, err := unix.IoctlRetInt(int(ctlFile.Fd()), unix.LOOP_CTL_GET_FREE)
		if err != nil {
			return fmt.Errorf("ioctl LOOP_CTL_GET_FREE: %w", err)
		}
		loopDevPath = fmt.Sprintf("/host/dev/loop%d", loopNum)
		if _, err := os.Stat(loopDevPath); errors.Is(err, os.ErrNotExist) {
			_ = unix.Mknod(loopDevPath, unix.S_IFBLK|0o660, int(unix.Mkdev(7, uint32(loopNum))))
		}
		lf, err := os.OpenFile(loopDevPath, os.O_RDWR, 0)
		if err != nil {
			return fmt.Errorf("opening %s: %w", loopDevPath, err)
		}

		cfg := unix.LoopConfig{
			Fd: uint32(imgFile.Fd()),
			Info: unix.LoopInfo64{
				Flags: unix.LO_FLAGS_AUTOCLEAR | unix.LO_FLAGS_DIRECT_IO,
			},
		}
		copy(cfg.Info.File_name[:], imgPath)
		err = unix.IoctlLoopConfigure(int(lf.Fd()), &cfg)
		if err == nil {
			loopFile = lf
			directIO = true
			break
		}
		if errors.Is(err, unix.EINVAL) {
			cfg.Info.Flags = unix.LO_FLAGS_AUTOCLEAR
			err = unix.IoctlLoopConfigure(int(lf.Fd()), &cfg)
			if err == nil {
				loopFile = lf
				directIO = false
				break
			}
		}
		if errors.Is(err, unix.EBUSY) {
			_ = lf.Close()
			continue
		}
		_ = lf.Close()
		return fmt.Errorf("ioctl LOOP_CONFIGURE on %s: %w", loopDevPath, err)
	}
	if loopFile == nil {
		return fmt.Errorf("failed to allocate free loop device for actor %s after 64 attempts", actorUID)
	}
	defer loopFile.Close()

	if err := unix.Mount(loopDevPath, snapDir, "ext4", unix.MS_NOATIME|unix.MS_NODIRATIME, "nobarrier,nodiscard,noauto_da_alloc"); err != nil {
		return fmt.Errorf("mounting %s on %s: %w", loopDevPath, snapDir, err)
	}
	slog.Info("Mounted per-actor snapshot loopback filesystem",
		"actor_uid", actorUID,
		"device", loopDevPath,
		"image", filepath.Base(imgPath),
		"direct_io", directIO,
	)
	return nil
}

func teardownSnapshotLoopMount(actorUID string) error {
	if _, err := os.Stat(loopControlPath); err != nil {
		return nil
	}
	snapDir := ateletpath.ActorSnapshotPath(actorUID)
	mounted, err := isMountPoint(snapDir)
	if err != nil {
		return fmt.Errorf("checking snapshot mountpoint %s: %w", snapDir, err)
	}
	if mounted {
		if err := unix.Unmount(snapDir, unix.MNT_DETACH); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
			return fmt.Errorf("unmounting snapshot loop mount %s: %w", snapDir, err)
		}
	}
	releaseLoopImage(actorUID)
	return nil
}
