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

// Package btrfssubvol provides btrfs subvolume creation, snapshotting, and
// destruction helpers via Linux btrfs ioctls.
package btrfssubvol

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	btrfsFirstFreeObjectID = 256
	btrfsSubvolNameMax     = 4039

	btrfsIocSnapCreateV2   = 0x50009417
	btrfsIocSubvolCreateV2 = 0x50009418
	btrfsIocSnapDestroyV2  = 0x5000943f
)

// btrfsIoctlVolArgsV2 matches struct btrfs_ioctl_vol_args_v2 in <linux/btrfs.h> (4096 bytes).
type btrfsIoctlVolArgsV2 struct {
	Fd      int64
	Transid uint64
	Flags   uint64
	Unused  [4]uint64
	Name    [btrfsSubvolNameMax + 1]byte
}

// IsBtrfs reports whether path resides on a btrfs filesystem.
func IsBtrfs(path string) bool {
	var sfs unix.Statfs_t
	if err := unix.Statfs(path, &sfs); err != nil {
		return false
	}
	return sfs.Type == unix.BTRFS_SUPER_MAGIC
}

// IsSubvolume reports whether path is the root directory of a btrfs subvolume.
func IsSubvolume(path string) bool {
	var st unix.Stat_t
	if err := unix.Lstat(path, &st); err != nil {
		return false
	}
	if st.Mode&unix.S_IFMT != unix.S_IFDIR || st.Ino != btrfsFirstFreeObjectID {
		return false
	}
	return IsBtrfs(path)
}

func setVolArgsName(args *btrfsIoctlVolArgsV2, name string) error {
	if name == "" || name == "." || name == ".." || len(name) > btrfsSubvolNameMax {
		return fmt.Errorf("invalid btrfs subvolume name %q", name)
	}
	copy(args.Name[:], name)
	return nil
}

// Create creates a new writable btrfs subvolume at dstPath. Any existing
// directory or subvolume at dstPath is removed first.
func Create(dstPath string) error {
	if err := RemoveAll(dstPath); err != nil {
		return fmt.Errorf("removing existing path before subvolume create %s: %w", dstPath, err)
	}
	parentDir := filepath.Dir(dstPath)
	baseName := filepath.Base(dstPath)
	var args btrfsIoctlVolArgsV2
	if err := setVolArgsName(&args, baseName); err != nil {
		return err
	}
	parentFile, err := os.OpenFile(parentDir, os.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return fmt.Errorf("opening parent dir %s: %w", parentDir, err)
	}
	defer parentFile.Close()

	_, _, errno := unix.Syscall(
		unix.SYS_IOCTL,
		parentFile.Fd(),
		uintptr(btrfsIocSubvolCreateV2),
		uintptr(unsafe.Pointer(&args)),
	)
	if errno != 0 {
		return fmt.Errorf("BTRFS_IOC_SUBVOL_CREATE_V2 %s: %w", dstPath, errno)
	}
	return nil
}

// Snapshot creates a writable Copy-on-Write btrfs snapshot of srcSubvolPath at
// dstPath. Any existing directory or subvolume at dstPath is removed first.
func Snapshot(srcSubvolPath, dstPath string) error {
	if err := RemoveAll(dstPath); err != nil {
		return fmt.Errorf("removing existing path before subvolume snapshot %s: %w", dstPath, err)
	}
	parentDir := filepath.Dir(dstPath)
	baseName := filepath.Base(dstPath)
	var args btrfsIoctlVolArgsV2
	if err := setVolArgsName(&args, baseName); err != nil {
		return err
	}
	srcFile, err := os.OpenFile(srcSubvolPath, os.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return fmt.Errorf("opening source subvolume %s: %w", srcSubvolPath, err)
	}
	defer srcFile.Close()
	if err := unix.Syncfs(int(srcFile.Fd())); err != nil {
		return fmt.Errorf("syncing source subvolume %s before snapshot: %w", srcSubvolPath, err)
	}
	args.Fd = int64(srcFile.Fd())

	parentFile, err := os.OpenFile(parentDir, os.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return fmt.Errorf("opening parent dir %s: %w", parentDir, err)
	}
	defer parentFile.Close()

	_, _, errno := unix.Syscall(
		unix.SYS_IOCTL,
		parentFile.Fd(),
		uintptr(btrfsIocSnapCreateV2),
		uintptr(unsafe.Pointer(&args)),
	)
	if errno != 0 {
		return fmt.Errorf("BTRFS_IOC_SNAP_CREATE_V2 %s -> %s: %w", srcSubvolPath, dstPath, errno)
	}
	return nil
}

// Destroy destroys the btrfs subvolume at subvolPath in O(1) using
// BTRFS_IOC_SNAP_DESTROY_V2. Returns nil if subvolPath does not exist.
func Destroy(subvolPath string) error {
	parentDir := filepath.Dir(subvolPath)
	baseName := filepath.Base(subvolPath)
	var args btrfsIoctlVolArgsV2
	if err := setVolArgsName(&args, baseName); err != nil {
		return err
	}
	parentFile, err := os.OpenFile(parentDir, os.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("opening parent dir %s: %w", parentDir, err)
	}
	defer parentFile.Close()

	_, _, errno := unix.Syscall(
		unix.SYS_IOCTL,
		parentFile.Fd(),
		uintptr(btrfsIocSnapDestroyV2),
		uintptr(unsafe.Pointer(&args)),
	)
	if errno != 0 && errno != unix.ENOENT {
		return fmt.Errorf("BTRFS_IOC_SNAP_DESTROY_V2 %s: %w", subvolPath, errno)
	}
	return nil
}

// RemoveAll destroys path via BTRFS_IOC_SNAP_DESTROY_V2 if it is a btrfs
// subvolume, falling back to os.RemoveAll otherwise.
func RemoveAll(path string) error {
	if IsSubvolume(path) {
		if err := Destroy(path); err == nil {
			return nil
		}
	}
	return os.RemoveAll(path)
}
