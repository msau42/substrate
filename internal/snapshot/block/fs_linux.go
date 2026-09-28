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

package block

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"golang.org/x/sys/unix"
)

const (
	// DefaultFilesystemType is the default filesystem formatted onto raw block
	// volumes when an actor first runs.
	DefaultFilesystemType = "ext4"
)

// BlockFileSystem abstracts local filesystem, synchronization, and block-device
// formatting operations on a worker node.
type BlockFileSystem interface {
	Stat(name string) (os.FileInfo, error)
	ReadDir(name string) ([]os.DirEntry, error)
	ReadFile(name string) ([]byte, error)
	MkdirAll(path string, perm os.FileMode) error
	Remove(name string) error
	RemoveAll(path string) error
	Fsync(path string) error
	IsFormatted(ctx context.Context, devicePath string) (bool, error)
	FormatFilesystem(ctx context.Context, devicePath string, fsType string) error
}

// OSBlockFileSystem is the production implementation of BlockFileSystem on Linux.
type OSBlockFileSystem struct{}

var _ BlockFileSystem = OSBlockFileSystem{}

func (OSBlockFileSystem) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (OSBlockFileSystem) ReadDir(name string) ([]os.DirEntry, error) {
	return os.ReadDir(name)
}

func (OSBlockFileSystem) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name)
}

func (OSBlockFileSystem) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (OSBlockFileSystem) Remove(name string) error {
	return os.Remove(name)
}

func (OSBlockFileSystem) RemoveAll(path string) error {
	return os.RemoveAll(path)
}

// Fsync flushes the filesystem and file/directory state at path to the
// underlying block device.
func (OSBlockFileSystem) Fsync(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := unix.Syncfs(int(f.Fd())); err != nil {
		return err
	}
	return unix.Fsync(int(f.Fd()))
}

// IsFormatted reports whether devicePath already contains a filesystem
// signature using blkid. Exit code 2 from blkid indicates no filesystem
// signature was found on the block device.
func (OSBlockFileSystem) IsFormatted(ctx context.Context, devicePath string) (bool, error) {
	if devicePath == "" {
		return false, fmt.Errorf("device path is required")
	}
	cmd := exec.CommandContext(ctx, "blkid", "-p", "-u", "filesystem", devicePath)
	output, err := cmd.CombinedOutput()
	if err == nil {
		return strings.TrimSpace(string(output)) != "", nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 2 {
		return false, nil
	}
	return false, fmt.Errorf("while probing filesystem on %q with blkid: %w (output: %s)", devicePath, err, strings.TrimSpace(string(output)))
}

// FormatFilesystem formats devicePath with the specified filesystem type (defaults to ext4).
func (OSBlockFileSystem) FormatFilesystem(ctx context.Context, devicePath string, fsType string) error {
	if devicePath == "" {
		return fmt.Errorf("device path is required")
	}
	if fsType == "" {
		fsType = DefaultFilesystemType
	}
	mkfsBin := "mkfs." + fsType
	var args []string
	if fsType == "ext4" || fsType == "ext3" || fsType == "ext2" {
		args = append(args, "-F")
	}
	args = append(args, devicePath)

	cmd := exec.CommandContext(ctx, mkfsBin, args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("while formatting %q with %s: %w (output: %s)", devicePath, mkfsBin, err, strings.TrimSpace(string(output)))
	}
	return nil
}

func defaultBlockFileSystem() BlockFileSystem {
	return OSBlockFileSystem{}
}
