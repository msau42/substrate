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

package btrfssubvol

import (
	"os"
	"path/filepath"
	"testing"
	"unsafe"
)

func TestBtrfsIoctlVolArgsV2Size(t *testing.T) {
	const wantSize = 4096
	if got := unsafe.Sizeof(btrfsIoctlVolArgsV2{}); got != wantSize {
		t.Fatalf("sizeof(btrfsIoctlVolArgsV2) = %d, want %d", got, wantSize)
	}
}

func TestRemoveAllFallback(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sub")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "file"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if IsSubvolume(dir) {
		t.Fatalf("IsSubvolume(%q) = true on temp dir, want false", dir)
	}
	if err := RemoveAll(dir); err != nil {
		t.Fatalf("RemoveAll(%q): %v", dir, err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("Stat(%q) after RemoveAll = %v, want not exist", dir, err)
	}
}
