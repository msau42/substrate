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

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestActorDiskPool_AllocateReuseReleaseRecover(t *testing.T) {
	tmp := t.TempDir()
	poolDir := filepath.Join(tmp, "disk-pool")
	actorsDir := filepath.Join(tmp, "actors")

	disk0 := filepath.Join(poolDir, "disk-0")
	disk1 := filepath.Join(poolDir, "disk-1")
	if err := os.MkdirAll(disk0, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(disk1, 0o700); err != nil {
		t.Fatal(err)
	}

	pool, err := NewActorDiskPool(poolDir, actorsDir)
	if err != nil {
		t.Fatalf("NewActorDiskPool failed: %v", err)
	}

	// 1. Allocate actor-1 -> should get disk-0
	if err := pool.EnsureActorDir("actor-1"); err != nil {
		t.Fatalf("EnsureActorDir(actor-1) failed: %v", err)
	}
	link1 := filepath.Join(actorsDir, "actor-1")
	target1, err := os.Readlink(link1)
	if err != nil {
		t.Fatalf("expected symlink at %s: %v", link1, err)
	}
	if target1 != filepath.Join(disk0, "actor-1") {
		t.Errorf("got target %s, want %s", target1, filepath.Join(disk0, "actor-1"))
	}

	// Write a dummy checkpoint file inside actor-1 to ensure idempotency preserves state
	dummyFile := filepath.Join(link1, "local-checkpoint", "snap-1", "pages.img")
	if err := os.MkdirAll(filepath.Dir(dummyFile), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dummyFile, []byte("checkpoint-data"), 0o600); err != nil {
		t.Fatal(err)
	}

	// 2. Re-calling EnsureActorDir(actor-1) should keep disk-0 and preserve dummyFile
	if err := pool.EnsureActorDir("actor-1"); err != nil {
		t.Fatalf("second EnsureActorDir(actor-1) failed: %v", err)
	}
	if _, err := os.Stat(dummyFile); err != nil {
		t.Errorf("expected dummy checkpoint file to survive EnsureActorDir: %v", err)
	}

	// 3. Allocate actor-2 -> should get disk-1
	if err := pool.EnsureActorDir("actor-2"); err != nil {
		t.Fatalf("EnsureActorDir(actor-2) failed: %v", err)
	}
	target2, err := os.Readlink(filepath.Join(actorsDir, "actor-2"))
	if err != nil {
		t.Fatal(err)
	}
	if target2 != filepath.Join(disk1, "actor-2") {
		t.Errorf("got target %s, want %s", target2, filepath.Join(disk1, "actor-2"))
	}

	// 4. Pool exhausted -> actor-3 should fail
	if err := pool.EnsureActorDir("actor-3"); err == nil {
		t.Errorf("expected error when all disks are allocated, got nil")
	}

	// 5. Simulate atelet restart: NewActorDiskPool should recover existing allocations
	recoveredPool, err := NewActorDiskPool(poolDir, actorsDir)
	if err != nil {
		t.Fatalf("recovery NewActorDiskPool failed: %v", err)
	}
	if err := recoveredPool.EnsureActorDir("actor-3"); err == nil {
		t.Errorf("expected recovered pool to know both disks are busy")
	}

	// 6. Release actor-1 -> frees disk-0
	if err := recoveredPool.ReleaseActor("actor-1"); err != nil {
		t.Fatalf("ReleaseActor(actor-1) failed: %v", err)
	}
	if _, err := os.Lstat(link1); !os.IsNotExist(err) {
		t.Errorf("expected symlink %s to be removed, got err=%v", link1, err)
	}
	if _, err := os.Stat(filepath.Join(disk0, "actor-1")); !os.IsNotExist(err) {
		t.Errorf("expected target dir on disk-0 to be removed")
	}

	// 7. Allocate actor-3 -> should now succeed and get disk-0
	if err := recoveredPool.EnsureActorDir("actor-3"); err != nil {
		t.Fatalf("EnsureActorDir(actor-3) after release failed: %v", err)
	}
	target3, err := os.Readlink(filepath.Join(actorsDir, "actor-3"))
	if err != nil {
		t.Fatal(err)
	}
	if target3 != filepath.Join(disk0, "actor-3") {
		t.Errorf("got target %s, want %s", target3, filepath.Join(disk0, "actor-3"))
	}
}
