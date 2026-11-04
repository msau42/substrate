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
	"compress/gzip"
	"crypto/rand"
	_ "embed"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"go.ate.dev/substrate/internal/btrfssubvol"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"golang.org/x/sys/unix"
)

const (
	guestMemoryZoneID        = "mem0"
	btrfsCheckpointTokenFile = ".btrfs-checkpoint-token"
)

//go:embed golden_durable_ext4.spz.gz
var goldenDurableExt4SpzGz []byte

var (
	goldenDurableExt4Mu sync.Mutex
	castagnoliTable     = crc32.MakeTable(crc32.Castagnoli)
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

// guestMemoryFile returns "" so guest RAM is backed by anonymous memfd rather
// than a MAP_SHARED file on Btrfs; memory-ranges is written sequentially on
// snapshot and read sequentially on restore.
func guestMemoryFile(_ *ateworkerpb.ActorDirs) string {
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

func cloneFileCoW(srcPath, dstPath string) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("while opening source file %s: %w", srcPath, err)
	}
	defer src.Close()

	srcInfo, err := src.Stat()
	if err != nil {
		return fmt.Errorf("while statting source file %s: %w", srcPath, err)
	}

	_ = os.Remove(dstPath)
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, srcInfo.Mode().Perm())
	if err != nil {
		return fmt.Errorf("while creating destination file %s: %w", dstPath, err)
	}
	defer dst.Close()

	srcFd := int(src.Fd())
	dstFd := int(dst.Fd())
	if err := unix.IoctlFileClone(dstFd, srcFd); err == nil {
		return nil
	}

	size := srcInfo.Size()
	if err := dst.Truncate(size); err != nil {
		return fmt.Errorf("while sizing destination file %s: %w", dstPath, err)
	}
	for off := int64(0); off < size; {
		dataStart, err := unix.Seek(srcFd, off, unix.SEEK_DATA)
		if err != nil {
			if errors.Is(err, unix.ENXIO) {
				break
			}
			if _, serr := src.Seek(0, io.SeekStart); serr != nil {
				return serr
			}
			if _, serr := dst.Seek(0, io.SeekStart); serr != nil {
				return serr
			}
			_, cerr := io.Copy(dst, src)
			return cerr
		}
		holeStart, err := unix.Seek(srcFd, dataStart, unix.SEEK_HOLE)
		if err != nil {
			return fmt.Errorf("while seeking hole in %s: %w", srcPath, err)
		}
		for pos := dataStart; pos < holeStart; {
			chunk := int(min(holeStart-pos, 64<<20))
			rOff := pos
			wOff := pos
			n, cerr := unix.CopyFileRange(srcFd, &rOff, dstFd, &wOff, chunk, 0)
			if cerr != nil {
				if _, serr := src.Seek(pos, io.SeekStart); serr != nil {
					return serr
				}
				if _, serr := dst.Seek(pos, io.SeekStart); serr != nil {
					return serr
				}
				copied, copyErr := io.CopyN(dst, src, holeStart-pos)
				if copyErr != nil {
					return fmt.Errorf("while copying sparse extent in %s: %w", srcPath, copyErr)
				}
				pos += copied
				continue
			}
			if n == 0 {
				break
			}
			pos += int64(n)
		}
		off = holeStart
	}
	return nil
}

// durableMountsDir returns the directory backing the actor's durable-dir
// volumes. On btrfs snapshot filesystems it lives inside the active subvolume.
func durableMountsDir(actorDirs *ateworkerpb.ActorDirs) string {
	if isBtrfsSnapshotDir(actorDirs) {
		return filepath.Join(activeSubvolDir(actorDirs), "durable-dir")
	}
	return actorDirs.GetDurableDirVolumeMountsDir()
}

func durableVolumeImagePath(actorDirs *ateworkerpb.ActorDirs, vol string) string {
	return filepath.Join(durableMountsDir(actorDirs), vol+".ext4")
}

func ensureGoldenDurableExt4Image(durableDir string) (string, error) {
	goldenDir := durableDir
	if filepath.Base(filepath.Dir(durableDir)) == "active" {
		goldenDir = filepath.Dir(filepath.Dir(filepath.Dir(durableDir)))
	}
	goldenPath := filepath.Join(goldenDir, ".golden-durable-ext4-1g-2m.img")

	goldenDurableExt4Mu.Lock()
	defer goldenDurableExt4Mu.Unlock()

	if st, err := os.Stat(goldenPath); err == nil && st.Mode().IsRegular() && st.Size() == 1<<30 {
		return goldenPath, nil
	}
	if err := os.MkdirAll(goldenDir, 0o755); err != nil {
		return "", fmt.Errorf("while creating golden ext4 dir %s: %w", goldenDir, err)
	}

	zr, err := gzip.NewReader(bytes.NewReader(goldenDurableExt4SpzGz))
	if err != nil {
		return "", fmt.Errorf("while opening embedded golden ext4 image: %w", err)
	}
	defer zr.Close()

	var hdr [12]byte
	if _, err := io.ReadFull(zr, hdr[:]); err != nil {
		return "", fmt.Errorf("while reading golden ext4 header: %w", err)
	}
	totalSize := int64(binary.LittleEndian.Uint64(hdr[0:8]))
	numExtents := binary.LittleEndian.Uint32(hdr[8:12])

	tmp, err := os.CreateTemp(goldenDir, ".golden-durable-ext4-*.tmp")
	if err != nil {
		return "", fmt.Errorf("while creating temp golden ext4 image: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	if err := tmp.Truncate(totalSize); err != nil {
		_ = tmp.Close()
		return "", fmt.Errorf("while sizing temp golden ext4 image: %w", err)
	}
	// Write metadata extents through a 2 MiB THP-advised shared mapping so
	// the filesystem allocates 2 MiB-aligned order-9 folios and disk extents
	// for the metadata chunks (chunk 0 and chunk 8) while leaving the rest of
	// the 1 GiB image sparse.
	mapped, mmapErr := unix.Mmap(int(tmp.Fd()), 0, int(totalSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if mmapErr == nil {
		_ = unix.Madvise(mapped, unix.MADV_HUGEPAGE)
	}
	for range numExtents {
		var extHdr [16]byte
		if _, err := io.ReadFull(zr, extHdr[:]); err != nil {
			if mmapErr == nil {
				_ = unix.Munmap(mapped)
			}
			_ = tmp.Close()
			return "", fmt.Errorf("while reading golden ext4 extent header: %w", err)
		}
		off := int64(binary.LittleEndian.Uint64(extHdr[0:8]))
		length := int64(binary.LittleEndian.Uint64(extHdr[8:16]))
		if mmapErr == nil {
			if _, err := io.ReadFull(zr, mapped[off:off+length]); err != nil {
				_ = unix.Munmap(mapped)
				_ = tmp.Close()
				return "", fmt.Errorf("while writing temp golden ext4 extent via mmap: %w", err)
			}
			continue
		}
		if _, err := tmp.Seek(off, io.SeekStart); err != nil {
			_ = tmp.Close()
			return "", fmt.Errorf("while seeking temp golden ext4 image: %w", err)
		}
		if _, err := io.CopyN(tmp, zr, length); err != nil {
			_ = tmp.Close()
			return "", fmt.Errorf("while writing temp golden ext4 extent: %w", err)
		}
	}
	if mmapErr == nil {
		if err := unix.Munmap(mapped); err != nil {
			_ = tmp.Close()
			return "", fmt.Errorf("while unmapping temp golden ext4 image: %w", err)
		}
	}
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmpName, goldenPath); err != nil {
		return "", fmt.Errorf("while renaming golden ext4 image to %s: %w", goldenPath, err)
	}
	return goldenPath, nil
}

func setExt4UUID(path string) error {
	var u [16]byte
	if _, err := rand.Read(u[:]); err != nil {
		return fmt.Errorf("while generating ext4 UUID: %w", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("while opening ext4 image %s for UUID update: %w", path, err)
	}
	defer f.Close()

	// Update the superblock through a 2 MiB MADV_HUGEPAGE mapping so chunk 0
	// (0..2 MiB) is faulted into the page cache as an order-9 (2 MiB) folio
	// rather than an order-0 (4 KiB) folio from pread/pwrite.
	if st, err := f.Stat(); err == nil && st.Size() >= 2<<20 {
		if b, merr := unix.Mmap(int(f.Fd()), 0, 2<<20, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED); merr == nil {
			_ = unix.Madvise(b, unix.MADV_HUGEPAGE)
			sb := b[1024:2048]
			copy(sb[0x68:0x78], u[:])
			csum := ^crc32.Checksum(sb[:0x3fc], castagnoliTable)
			binary.LittleEndian.PutUint32(sb[0x3fc:0x400], csum)
			return unix.Munmap(b)
		}
	}

	var sb [1024]byte
	if _, err := f.ReadAt(sb[:], 1024); err != nil {
		return fmt.Errorf("while reading ext4 superblock from %s: %w", path, err)
	}
	copy(sb[0x68:0x78], u[:])
	csum := ^crc32.Checksum(sb[:0x3fc], castagnoliTable)
	binary.LittleEndian.PutUint32(sb[0x3fc:0x400], csum)
	if _, err := f.WriteAt(sb[:], 1024); err != nil {
		return fmt.Errorf("while writing ext4 superblock to %s: %w", path, err)
	}
	return nil
}

func isExt4Image(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	if st, err := f.Stat(); err == nil && st.Size() >= 2<<20 {
		if b, merr := unix.Mmap(int(f.Fd()), 0, 2<<20, unix.PROT_READ, unix.MAP_SHARED); merr == nil {
			_ = unix.Madvise(b, unix.MADV_HUGEPAGE)
			magic := binary.LittleEndian.Uint16(b[1024+0x38 : 1024+0x3a])
			_ = unix.Munmap(b)
			return magic == 0xef53
		}
	}
	var magic [2]byte
	if _, err := f.ReadAt(magic[:], 1024+0x38); err != nil {
		return false
	}
	return binary.LittleEndian.Uint16(magic[:]) == 0xef53
}

func ensureDurableMountDirs(durableDir string, containers []*ateworkerpb.Container) error {
	names := durableVolumeNames(containers)
	if len(names) == 0 {
		return nil
	}
	if err := os.MkdirAll(durableDir, 0o755); err != nil {
		return fmt.Errorf("while creating durable-dir root %s: %w", durableDir, err)
	}
	var goldenPath string
	for _, name := range names {
		volDir := filepath.Join(durableDir, name)
		if _, err := os.Stat(volDir); os.IsNotExist(err) {
			if err := os.MkdirAll(volDir, 0o777); err != nil {
				return fmt.Errorf("while creating durable-dir volume %s: %w", volDir, err)
			}
			if err := os.Chmod(volDir, 0o777); err != nil {
				return fmt.Errorf("while setting mode on durable-dir volume %s: %w", volDir, err)
			}
		} else if err != nil {
			return fmt.Errorf("while checking durable-dir volume %s: %w", volDir, err)
		}

		imgPath := filepath.Join(durableDir, name+".ext4")
		if _, err := os.Stat(imgPath); err == nil {
			continue
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("while checking durable-dir image %s: %w", imgPath, err)
		}
		if goldenPath == "" {
			var err error
			goldenPath, err = ensureGoldenDurableExt4Image(durableDir)
			if err != nil {
				return err
			}
		}
		if err := cloneFileCoW(goldenPath, imgPath); err != nil {
			return fmt.Errorf("while cloning golden ext4 image to %s: %w", imgPath, err)
		}
		if err := setExt4UUID(imgPath); err != nil {
			return err
		}
	}
	return nil
}

func prepareActiveSubvolForRun(actorDirs *ateworkerpb.ActorDirs, containers []*ateworkerpb.Container) error {
	if isBtrfsSnapshotDir(actorDirs) {
		if err := btrfssubvol.Create(activeSubvolDir(actorDirs)); err != nil {
			return fmt.Errorf("while creating active btrfs subvolume: %w", err)
		}
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

func prepareActiveSubvolForRestore(actorDirs *ateworkerpb.ActorDirs, restoreDir string, _ []*ateworkerpb.Container) error {
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
		_ = os.Remove(filepath.Join(activeDir, memoryRangesFile))
	} else {
		if err := btrfssubvol.Create(activeDir); err != nil {
			return fmt.Errorf("while creating active btrfs subvolume: %w", err)
		}
	}
	return nil
}

func cleanupActiveOrRootfsUpper(actorDirs *ateworkerpb.ActorDirs) error {
	if isBtrfsSnapshotDir(actorDirs) {
		return btrfssubvol.RemoveAll(activeSubvolDir(actorDirs))
	}
	return os.RemoveAll(rootfsUpperDir(actorDirs))
}
