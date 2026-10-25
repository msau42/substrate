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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"golang.org/x/sys/unix"
)

const guestMemoryZoneID = "mem0"

// ociBundlePath is the container's OCI bundle.
func ociBundlePath(actorDirs *ateworkerpb.ActorDirs, containerName string) string {
	return filepath.Join(actorDirs.GetOciBundleDir(), containerName)
}

func hasSnapshotRootDir(actorDirs *ateworkerpb.ActorDirs) bool {
	return actorDirs.GetCheckpointDir() != ""
}

func snapshotRootDir(actorDirs *ateworkerpb.ActorDirs) string {
	if c := actorDirs.GetCheckpointDir(); c != "" {
		return filepath.Dir(c)
	}
	return actorDirs.GetRootDir()
}

func activeDir(actorDirs *ateworkerpb.ActorDirs) string {
	return filepath.Join(snapshotRootDir(actorDirs), "active")
}

// guestMemoryFile returns the file-backed shared memory zone path inside the
// active snapshot directory when a snapshot root directory is configured, or ""
// when guest RAM is memfd-backed.
func guestMemoryFile(actorDirs *ateworkerpb.ActorDirs) string {
	if hasSnapshotRootDir(actorDirs) {
		return filepath.Join(activeDir(actorDirs), memoryRangesFile)
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

func cloneDirTreeCoW(srcDir, dstDir string) error {
	if err := os.RemoveAll(dstDir); err != nil {
		return fmt.Errorf("while clearing destination tree %s: %w", dstDir, err)
	}
	return filepath.WalkDir(srcDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dstDir, rel)
		info, err := d.Info()
		if err != nil {
			return err
		}
		mode := info.Mode()
		switch {
		case mode.IsDir():
			if err := os.MkdirAll(target, mode.Perm()); err != nil {
				return err
			}
			return os.Chmod(target, mode.Perm())
		case mode&os.ModeSymlink != 0:
			linkTarget, err := os.Readlink(path)
			if err != nil {
				return err
			}
			return os.Symlink(linkTarget, target)
		case mode.IsRegular():
			return cloneFileCoW(path, target)
		default:
			// Sockets/FIFOs are skipped just like tarutil.CreateWithRoot.
			return nil
		}
	})
}

func stageGuestMemoryFileForRestore(restoreDir, activeDirPath string, preserveRestoreDir bool) error {
	srcPath := filepath.Join(restoreDir, memoryRangesFile)
	srcInfo, err := os.Stat(srcPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			_ = os.Remove(filepath.Join(activeDirPath, memoryRangesFile))
			return nil
		}
		return fmt.Errorf("while checking restore memory file %s: %w", srcPath, err)
	}

	dstPath := filepath.Join(activeDirPath, memoryRangesFile)
	if dstInfo, derr := os.Stat(dstPath); derr == nil && (preserveRestoreDir || os.SameFile(srcInfo, dstInfo)) {
		return nil
	}

	return cloneFileCoW(srcPath, dstPath)
}

// rootfsUpperDir is the host directory backing the actor's rootfs overlay
// uppers: one subdirectory per container (see kata.UpperWorkDirs). When a
// snapshot root directory is configured, it lives inside active/ on the
// dedicated snapshot disk.
func rootfsUpperDir(actorDirs *ateworkerpb.ActorDirs) string {
	if hasSnapshotRootDir(actorDirs) {
		return filepath.Join(activeDir(actorDirs), "rootfs-upper")
	}
	return filepath.Join(actorDirs.GetRootDir(), "rootfs-upper")
}

// durableMountsDir returns the directory backing the actor's durable-dir
// volumes. When a snapshot root directory is configured, it lives inside
// active/ on the dedicated snapshot disk.
func durableMountsDir(actorDirs *ateworkerpb.ActorDirs) string {
	if hasSnapshotRootDir(actorDirs) {
		return filepath.Join(activeDir(actorDirs), "durable-dir")
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

func prepareActiveForRun(actorDirs *ateworkerpb.ActorDirs, containers []*ateworkerpb.Container) error {
	if !hasSnapshotRootDir(actorDirs) {
		return nil
	}
	dir := activeDir(actorDirs)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("while clearing active snapshot dir %s: %w", dir, err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("while creating active snapshot dir %s: %w", dir, err)
	}
	return ensureDurableMountDirs(durableMountsDir(actorDirs), containers)
}

func prepareActiveForRestore(actorDirs *ateworkerpb.ActorDirs, restoreDir string, containers []*ateworkerpb.Container, preserveRestoreDir bool) error {
	if !hasSnapshotRootDir(actorDirs) {
		return nil
	}
	dir := activeDir(actorDirs)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("while creating active snapshot dir %s: %w", dir, err)
	}
	if err := stageGuestMemoryFileForRestore(restoreDir, dir, preserveRestoreDir); err != nil {
		return err
	}
	return ensureDurableMountDirs(durableMountsDir(actorDirs), containers)
}

func cleanupActiveOrRootfsUpper(actorDirs *ateworkerpb.ActorDirs) error {
	if hasSnapshotRootDir(actorDirs) {
		return os.RemoveAll(activeDir(actorDirs))
	}
	return os.RemoveAll(rootfsUpperDir(actorDirs))
}
