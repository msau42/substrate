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
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
)

type fakeWorkerServiceClient struct {
	ateapipb.WorkerServiceClient
	mu       sync.Mutex
	reported []*ateapipb.ReportActorDiskDetachedRequest
}

func (f *fakeWorkerServiceClient) ReportActorDiskDetached(_ context.Context, in *ateapipb.ReportActorDiskDetachedRequest, _ ...grpc.CallOption) (*ateapipb.ReportActorDiskDetachedResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reported = append(f.reported, in)
	return &ateapipb.ReportActorDiskDetachedResponse{}, nil
}

type fakeDiskAttacher struct {
	unmountedPath   string
	detachedDevice  string
	attachedGceDisk string
	attachedDevice  string
	mountedPath     string
}

func (f *fakeDiskAttacher) ResolveDiskMetadata(_ context.Context, mountPath string) (string, string, error) {
	metaPath := filepath.Join(mountPath, diskMetadataFilename)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		base := filepath.Base(mountPath)
		return base, base, nil
	}
	var meta diskMetadataFile
	if err := json.Unmarshal(data, &meta); err != nil {
		return "", "", err
	}
	return meta.GCEDiskName, meta.DeviceName, nil
}

func (f *fakeDiskAttacher) UnmountAndDetach(_ context.Context, mountPath, deviceName string) error {
	f.unmountedPath = mountPath
	f.detachedDevice = deviceName
	return nil
}

func (f *fakeDiskAttacher) AttachAndMount(_ context.Context, gceDiskName, deviceName, mountPath string) error {
	f.attachedGceDisk = gceDiskName
	f.attachedDevice = deviceName
	f.mountedPath = mountPath
	if err := os.MkdirAll(mountPath, 0o755); err != nil {
		return err
	}
	meta := diskMetadataFile{GCEDiskName: gceDiskName, DeviceName: deviceName}
	raw, _ := json.Marshal(meta)
	return os.WriteFile(filepath.Join(mountPath, diskMetadataFilename), raw, 0o644)
}

func TestActorDiskPool_ExportAndImportAcrossNodes(t *testing.T) {
	ctx := context.Background()

	// Setup Node 1 pool with 1 disk: actor-disk-0
	node1Root := t.TempDir()
	node1PoolDir := filepath.Join(node1Root, "disk-pool")
	node1ActorsDir := filepath.Join(node1Root, "actors")
	disk0Path := filepath.Join(node1PoolDir, "actor-disk-0")
	if err := os.MkdirAll(disk0Path, 0o755); err != nil {
		t.Fatal(err)
	}
	meta := diskMetadataFile{GCEDiskName: "actor-disk-0", DeviceName: "actor-disk-0"}
	rawMeta, _ := json.Marshal(meta)
	if err := os.WriteFile(filepath.Join(disk0Path, diskMetadataFilename), rawMeta, 0o644); err != nil {
		t.Fatal(err)
	}

	pool1, err := NewActorDiskPool(node1PoolDir, node1ActorsDir)
	if err != nil {
		t.Fatalf("NewActorDiskPool node1 failed: %v", err)
	}
	fake1 := &fakeDiskAttacher{}
	pool1.attacher = fake1

	actorUID := "actor-1234"
	if err := pool1.EnsureActorDir(actorUID); err != nil {
		t.Fatalf("EnsureActorDir on node1 failed: %v", err)
	}

	// Simulate writing a checkpoint file inside the actor directory on Node 1
	checkpointFile := filepath.Join(node1ActorsDir, actorUID, "local-checkpoint", "snap-1", "state.img")
	if err := os.MkdirAll(filepath.Dir(checkpointFile), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(checkpointFile, []byte("checkpoint-data"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Export actor disk from Node 1
	gceDisk, devName, err := pool1.ExportActorDisk(ctx, actorUID)
	if err != nil {
		t.Fatalf("ExportActorDisk failed: %v", err)
	}
	if gceDisk != "actor-disk-0" || devName != "actor-disk-0" {
		t.Errorf("unexpected exported disk identifiers: gceDisk=%q, devName=%q", gceDisk, devName)
	}
	if fake1.unmountedPath != disk0Path || fake1.detachedDevice != "actor-disk-0" {
		t.Errorf("unexpected UnmountAndDetach args: path=%q, dev=%q", fake1.unmountedPath, fake1.detachedDevice)
	}

	// Verify symlink on Node 1 was removed, but underlying checkpoint data on disk0Path was preserved
	if _, err := os.Lstat(filepath.Join(node1ActorsDir, actorUID)); !os.IsNotExist(err) {
		t.Errorf("expected symlink on node1 to be removed after export, got err=%v", err)
	}
	underlyingCheckpoint := filepath.Join(disk0Path, actorUID, "local-checkpoint", "snap-1", "state.img")
	if data, err := os.ReadFile(underlyingCheckpoint); err != nil || string(data) != "checkpoint-data" {
		t.Errorf("expected checkpoint data on disk to be preserved, got err=%v, data=%q", err, string(data))
	}

	// Setup Node 2 pool (initially empty, simulating all disks busy or dynamic node)
	node2Root := t.TempDir()
	node2PoolDir := filepath.Join(node2Root, "disk-pool")
	node2ActorsDir := filepath.Join(node2Root, "actors")
	pool2, err := NewActorDiskPool(node2PoolDir, node2ActorsDir)
	if err != nil {
		t.Fatalf("NewActorDiskPool node2 failed: %v", err)
	}
	fake2 := &fakeDiskAttacher{}
	pool2.attacher = fake2

	// Import actor disk onto Node 2
	if err := pool2.ImportActorDisk(ctx, actorUID, gceDisk, devName); err != nil {
		t.Fatalf("ImportActorDisk on node2 failed: %v", err)
	}
	expectedMount2 := filepath.Join(node2PoolDir, gceDisk)
	if fake2.attachedGceDisk != gceDisk || fake2.mountedPath != expectedMount2 {
		t.Errorf("unexpected AttachAndMount args on node2: disk=%q, mount=%q", fake2.attachedGceDisk, fake2.mountedPath)
	}

	// Simulate the physical disk contents appearing at expectedMount2 (since fake2 just creates dir)
	importedCheckpoint := filepath.Join(expectedMount2, actorUID, "local-checkpoint", "snap-1", "state.img")
	if err := os.MkdirAll(filepath.Dir(importedCheckpoint), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(importedCheckpoint, []byte("checkpoint-data"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Verify Node 2 symlink resolves to the imported checkpoint
	linkTarget, err := os.Readlink(filepath.Join(node2ActorsDir, actorUID))
	if err != nil {
		t.Fatalf("expected symlink on node2 after import, got err=%v", err)
	}
	if linkTarget != filepath.Join(expectedMount2, actorUID) {
		t.Errorf("unexpected symlink target on node2: got %q, want %q", linkTarget, filepath.Join(expectedMount2, actorUID))
	}
	data, err := os.ReadFile(filepath.Join(node2ActorsDir, actorUID, "local-checkpoint", "snap-1", "state.img"))
	if err != nil || string(data) != "checkpoint-data" {
		t.Errorf("reading checkpoint via node2 symlink failed: err=%v, data=%q", err, string(data))
	}

	// Verify releasing the actor on Node 2 cleans up the actor dir and leaves the disk in Node 2's pool
	if err := pool2.ReleaseActor(actorUID); err != nil {
		t.Fatalf("ReleaseActor on node2 failed: %v", err)
	}
	if err := pool2.EnsureActorDir("actor-5678"); err != nil {
		t.Fatalf("expected imported disk on node2 to be reusable by a new actor after release, got err=%v", err)
	}
}

func TestActorDiskPool_ExportRejectedWhenQueueFull(t *testing.T) {
	ctx := context.Background()

	root := t.TempDir()
	poolDir := filepath.Join(root, "disk-pool")
	actorsDir := filepath.Join(root, "actors")
	disk0Path := filepath.Join(poolDir, "actor-disk-0")
	if err := os.MkdirAll(disk0Path, 0o755); err != nil {
		t.Fatal(err)
	}
	meta := diskMetadataFile{GCEDiskName: "actor-disk-0", DeviceName: "actor-disk-0"}
	rawMeta, _ := json.Marshal(meta)
	if err := os.WriteFile(filepath.Join(disk0Path, diskMetadataFilename), rawMeta, 0o644); err != nil {
		t.Fatal(err)
	}

	pool, err := NewActorDiskPool(poolDir, actorsDir)
	if err != nil {
		t.Fatalf("NewActorDiskPool failed: %v", err)
	}
	pool.attacher = &fakeDiskAttacher{}

	actorUID := "actor-queue-test"
	if err := pool.EnsureActorDir(actorUID); err != nil {
		t.Fatalf("EnsureActorDir failed: %v", err)
	}

	// Simulate 5 pending disk operations in queue
	pool.pendingOps.Store(5)
	_, _, err = pool.ExportActorDisk(ctx, actorUID)
	if err == nil {
		t.Fatal("expected ExportActorDisk to fail when pendingOps >= 5, got nil")
	}
	// Verify symlink and allocation on node remain intact
	if _, err := os.Readlink(filepath.Join(actorsDir, actorUID)); err != nil {
		t.Errorf("expected actor symlink to remain intact after rejected export, got err=%v", err)
	}

	// Simulate queue dropping below 5
	pool.pendingOps.Store(4)
	gceDisk, _, err := pool.ExportActorDisk(ctx, actorUID)
	if err != nil {
		t.Fatalf("expected ExportActorDisk to succeed when pendingOps < 5, got err=%v", err)
	}
	if gceDisk != "actor-disk-0" {
		t.Errorf("gceDisk = %q, want actor-disk-0", gceDisk)
	}
}

func TestActorDiskPool_ProactiveDetachmentAndUnallocatedDetachedPool(t *testing.T) {
	ctx := context.Background()
	t.Setenv("ATELET_PROACTIVE_DISK_LIMIT", "1")

	root := t.TempDir()
	poolDir := filepath.Join(root, "disk-pool")
	actorsDir := filepath.Join(root, "actors")
	disk0Path := filepath.Join(poolDir, "actor-disk-0")
	if err := os.MkdirAll(disk0Path, 0o755); err != nil {
		t.Fatal(err)
	}
	meta := diskMetadataFile{GCEDiskName: "actor-disk-0", DeviceName: "actor-disk-0"}
	rawMeta, _ := json.Marshal(meta)
	if err := os.WriteFile(filepath.Join(disk0Path, diskMetadataFilename), rawMeta, 0o644); err != nil {
		t.Fatal(err)
	}

	// Write .detached-pool.json containing actor-disk-1
	detachedRaw, _ := json.Marshal([]string{"actor-disk-1"})
	if err := os.WriteFile(filepath.Join(poolDir, detachedPoolFilename), detachedRaw, 0o644); err != nil {
		t.Fatal(err)
	}

	pool, err := NewActorDiskPool(poolDir, actorsDir)
	if err != nil {
		t.Fatalf("NewActorDiskPool failed: %v", err)
	}
	fake := &fakeDiskAttacher{}
	pool.attacher = fake
	fakeWorkerClient := &fakeWorkerServiceClient{}
	pool.SetWorkerServiceClient(fakeWorkerClient)

	// Allocate first actor (uses mounted actor-disk-0)
	if err := pool.EnsureActorDir("actor-0"); err != nil {
		t.Fatalf("EnsureActorDir actor-0 failed: %v", err)
	}
	pool.RecordActorRef("actor-0", "default", "actor-0-name")
	pool.NotifyActorPaused("actor-0")

	// Allocate second actor (imports actor-disk-1 from unallocatedDetached)
	if err := pool.EnsureActorDir("actor-1"); err != nil {
		t.Fatalf("EnsureActorDir actor-1 failed: %v", err)
	}

	// Because proactive limit is 1 and actor-0 is paused, proactive eviction should detach actor-disk-0
	if err := pool.evictOneIdleDisk(ctx); err != nil {
		// Wait or check if background goroutine already evicted it
		pool.mu.Lock()
		_, alreadyDetached := pool.detachedDisks["actor-0"]
		pool.mu.Unlock()
		if !alreadyDetached {
			t.Fatalf("expected actor-0 disk to be evicted, got err=%v", err)
		}
	}

	fakeWorkerClient.mu.Lock()
	repCount := len(fakeWorkerClient.reported)
	fakeWorkerClient.mu.Unlock()
	if repCount != 1 {
		t.Fatalf("expected 1 ReportActorDiskDetached call to ateapi, got %d", repCount)
	}
	if got := fakeWorkerClient.reported[0].GetGceDiskName(); got != "actor-disk-0" {
		t.Errorf("reported GceDiskName = %q, want actor-disk-0", got)
	}

	// Now exporting actor-0 should succeed immediately (0ms fast-path) even if queue is full!
	pool.pendingOps.Store(5)
	gceDisk, _, err := pool.ExportActorDisk(ctx, "actor-0")
	if err != nil {
		t.Fatalf("expected ExportActorDisk fast-path to succeed for proactively detached disk even when queue full, got err=%v", err)
	}
	if gceDisk != "actor-disk-0" {
		t.Errorf("gceDisk = %q, want actor-disk-0", gceDisk)
	}
}

func TestActorDiskPool_ConcurrentImportRespectsHyperdiskLimit(t *testing.T) {
	ctx := context.Background()

	root := t.TempDir()
	poolDir := filepath.Join(root, "disk-pool")
	actorsDir := filepath.Join(root, "actors")

	// Pre-create 31 mounted disks (at maxAttachedActorHyperdisks limit)
	for i := 0; i < maxAttachedActorHyperdisks; i++ {
		name := filepath.Join(poolDir, "actor-disk-init-"+string(rune('A'+i)))
		if err := os.MkdirAll(name, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	pool, err := NewActorDiskPool(poolDir, actorsDir)
	if err != nil {
		t.Fatalf("NewActorDiskPool failed: %v", err)
	}
	pool.attacher = &fakeDiskAttacher{}

	// Allocate and pause all 31 initial actors so their disks are eligible for LRU eviction
	for i := 0; i < maxAttachedActorHyperdisks; i++ {
		uid := "actor-init-" + string(rune('A'+i))
		if err := pool.EnsureActorDir(uid); err != nil {
			t.Fatalf("EnsureActorDir %s failed: %v", uid, err)
		}
		pool.NotifyActorPaused(uid)
	}

	// Launch 5 concurrent imports when already at 31 attached disks
	errCh := make(chan error, 5)
	for i := 0; i < 5; i++ {
		go func(idx int) {
			uid := "actor-concurrent-" + string(rune('0'+idx))
			diskName := "actor-disk-new-" + string(rune('0'+idx))
			errCh <- pool.ImportActorDisk(ctx, uid, diskName, diskName)
		}(i)
	}

	for i := 0; i < 5; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent ImportActorDisk failed: %v", err)
		}
	}

	pool.mu.Lock()
	totalMounted := len(pool.disks)
	totalDetached := len(pool.detachedDisks)
	pool.mu.Unlock()

	if totalMounted > maxAttachedActorHyperdisks {
		t.Errorf("totalMounted = %d, exceeds maxAttachedActorHyperdisks (%d)", totalMounted, maxAttachedActorHyperdisks)
	}
	if totalDetached < 5 {
		t.Errorf("expected at least 5 paused disks evicted to detachedDisks, got %d", totalDetached)
	}
}

func TestActorDiskPool_DetachOnPauseParallel(t *testing.T) {
	t.Setenv("ATELET_DETACH_ON_PAUSE", "true")

	root := t.TempDir()
	poolDir := filepath.Join(root, "disk-pool")
	actorsDir := filepath.Join(root, "actors")
	for i := 0; i < 3; i++ {
		d := filepath.Join(poolDir, "actor-disk-"+string(rune('0'+i)))
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	pool, err := NewActorDiskPool(poolDir, actorsDir)
	if err != nil {
		t.Fatalf("NewActorDiskPool failed: %v", err)
	}
	pool.attacher = &fakeDiskAttacher{}

	for i := 0; i < 3; i++ {
		uid := "actor-" + string(rune('0'+i))
		if err := pool.EnsureActorDir(uid); err != nil {
			t.Fatalf("EnsureActorDir %s failed: %v", uid, err)
		}
		pool.NotifyActorPaused(uid)
	}

	// EnsureActorDir immediately after NotifyActorPaused should wait for the background detach and re-import cleanly
	for i := 0; i < 3; i++ {
		uid := "actor-" + string(rune('0'+i))
		if err := pool.EnsureActorDir(uid); err != nil {
			t.Fatalf("EnsureActorDir on resume for %s failed: %v", uid, err)
		}
		if !pool.WasCrossNodeRestore(uid) {
			t.Errorf("expected WasCrossNodeRestore(%s) == true when ATELET_DETACH_ON_PAUSE=true", uid)
		}
		if _, err := os.Readlink(filepath.Join(actorsDir, uid)); err != nil {
			t.Errorf("expected symlink restored after re-import for %s, got err=%v", uid, err)
		}
	}
}

func TestActorDiskPool_SimulatedCrossNodeRestorePct(t *testing.T) {
	root := t.TempDir()
	poolDir := filepath.Join(root, "disk-pool")
	actorsDir := filepath.Join(root, "actors")
	d := filepath.Join(poolDir, "actor-disk-0")
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}

	pool, err := NewActorDiskPool(poolDir, actorsDir)
	if err != nil {
		t.Fatalf("NewActorDiskPool failed: %v", err)
	}
	pool.attacher = &fakeDiskAttacher{}

	uid := "actor-pct-test"
	if err := pool.EnsureActorDir(uid); err != nil {
		t.Fatalf("initial EnsureActorDir failed: %v", err)
	}

	// 1. 0% cross-node restore -> 100% same-node warm reuse
	t.Setenv("ATELET_SIMULATE_CROSS_NODE_PCT", "0")
	for i := 0; i < 10; i++ {
		pool.NotifyActorPaused(uid)
		if err := pool.EnsureActorDir(uid); err != nil {
			t.Fatalf("EnsureActorDir (0%%) failed: %v", err)
		}
		if pool.WasCrossNodeRestore(uid) {
			t.Fatalf("expected WasCrossNodeRestore == false at 0%%, got true on iter %d", i)
		}
	}

	// 2. 50% cross-node restore -> both cross-node and same-node restores occur over 100 cycles
	t.Setenv("ATELET_SIMULATE_CROSS_NODE_PCT", "50")
	crossCount := 0
	for i := 0; i < 100; i++ {
		pool.NotifyActorPaused(uid)
		if err := pool.EnsureActorDir(uid); err != nil {
			t.Fatalf("EnsureActorDir (50%%) failed on iter %d: %v", i, err)
		}
		if pool.WasCrossNodeRestore(uid) {
			crossCount++
		}
	}
	if crossCount < 20 || crossCount > 80 {
		t.Errorf("expected roughly 50/100 cross-node restores at 50%%, got %d", crossCount)
	}
}

