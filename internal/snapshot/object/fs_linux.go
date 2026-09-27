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

package object

import (
	"context"
	"os"

	"github.com/agent-substrate/substrate/internal/snapshot/object/ategcs"
	"github.com/agent-substrate/substrate/internal/tarutil"
	"golang.org/x/sys/unix"
)

// FileSystem abstracts local disk operations and filesystem synchronization.
type FileSystem interface {
	Stat(name string) (os.FileInfo, error)
	ReadDir(name string) ([]os.DirEntry, error)
	MkdirAll(path string, perm os.FileMode) error
	Remove(name string) error
	RemoveAll(path string) error
	Rename(oldpath, newpath string) error
	CreateTempDir(dir, pattern string) (string, error)
	Syncfs(dirPath string) error
}

// Archiver abstracts TAR packaging and extraction for directories.
type Archiver interface {
	Create(ctx context.Context, tarPath, srcDir string) error
	Extract(tarPath, dstDir string) error
}

// ObjectStorage is the worker-plane object storage client interface (GCS / S3).
type ObjectStorage = ategcs.ObjectStorage

// OSFileSystem is the production implementation of FileSystem on Linux.
type OSFileSystem struct{}

func (OSFileSystem) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (OSFileSystem) ReadDir(name string) ([]os.DirEntry, error) {
	return os.ReadDir(name)
}

func (OSFileSystem) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (OSFileSystem) Remove(name string) error {
	return os.Remove(name)
}

func (OSFileSystem) RemoveAll(path string) error {
	return os.RemoveAll(path)
}

func (OSFileSystem) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func (OSFileSystem) CreateTempDir(dir, pattern string) (string, error) {
	return os.MkdirTemp(dir, pattern)
}

func (OSFileSystem) Syncfs(dirPath string) error {
	f, err := os.Open(dirPath)
	if err != nil {
		return err
	}
	defer f.Close()
	return unix.Syncfs(int(f.Fd()))
}

// TarutilArchiver implements Archiver using internal/tarutil.
type TarutilArchiver struct{}

func (TarutilArchiver) Create(ctx context.Context, tarPath, srcDir string) error {
	return tarutil.Create(ctx, tarPath, srcDir)
}

func (TarutilArchiver) Extract(tarPath, dstDir string) error {
	return tarutil.Extract(tarPath, dstDir)
}

func defaultFileSystem() FileSystem {
	return OSFileSystem{}
}

func defaultArchiver() Archiver {
	return TarutilArchiver{}
}
