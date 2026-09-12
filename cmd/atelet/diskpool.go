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
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/agent-substrate/substrate/internal/imagecache"
)

// ActorDiskPool allocates dedicated pre-mounted disk directories from a pool
// (e.g., /var/lib/ateom-gvisor/disk-pool/disk-0, disk-1, ...) to actors by
// symlinking /var/lib/ateom-gvisor/actors/<actorUID> -> <diskDir>/<actorUID>.
type ActorDiskPool struct {
	mu          sync.Mutex
	poolDir     string
	actorsDir   string
	disks       []string
	actorToDisk map[string]string
	diskToActor map[string]string
}

var globalActorDiskPool *ActorDiskPool

// NewActorDiskPool initializes the disk pool from poolDir and recovers any
// existing actor-to-disk symlink allocations from actorsDir. Returns nil, nil
// if poolDir is empty.
func NewActorDiskPool(poolDir, actorsDir string) (*ActorDiskPool, error) {
	if poolDir == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(poolDir)
	if err != nil {
		return nil, fmt.Errorf("reading actor disk pool dir %q: %w", poolDir, err)
	}
	var disks []string
	for _, entry := range entries {
		full := filepath.Join(poolDir, entry.Name())
		fi, err := os.Stat(full)
		if err == nil && fi.IsDir() {
			disks = append(disks, full)
		}
	}
	sort.Strings(disks)
	if len(disks) == 0 {
		return nil, fmt.Errorf("no disk directories found in actor disk pool %q", poolDir)
	}

	p := &ActorDiskPool{
		poolDir:     poolDir,
		actorsDir:   actorsDir,
		disks:       disks,
		actorToDisk: make(map[string]string),
		diskToActor: make(map[string]string),
	}

	// Recover existing symlink allocations from actorsDir across restarts.
	if actorEntries, err := os.ReadDir(actorsDir); err == nil {
		for _, entry := range actorEntries {
			actorUID := entry.Name()
			linkPath := filepath.Join(actorsDir, actorUID)
			target, err := os.Readlink(linkPath)
			if err != nil {
				continue
			}
			for _, diskPath := range disks {
				if target == filepath.Join(diskPath, actorUID) || strings.HasPrefix(target, diskPath+string(os.PathSeparator)) {
					p.actorToDisk[actorUID] = diskPath
					p.diskToActor[diskPath] = actorUID
					slog.Info("Recovered actor disk allocation", slog.String("actorUID", actorUID), slog.String("disk", filepath.Base(diskPath)))
					break
				}
			}
		}
	}

	return p, nil
}

// EnsureActorDir allocates a dedicated disk from the pool for actorUID (or
// reuses its existing allocation) and ensures the symlink
// <actorsDir>/<actorUID> -> <diskPath>/<actorUID> is in place.
func (p *ActorDiskPool) EnsureActorDir(actorUID string) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if diskPath, ok := p.actorToDisk[actorUID]; ok {
		targetDir := filepath.Join(diskPath, actorUID)
		if err := os.MkdirAll(targetDir, 0o700); err != nil {
			return fmt.Errorf("ensuring existing target dir %q: %w", targetDir, err)
		}
		linkPath := filepath.Join(p.actorsDir, actorUID)
		if cur, err := os.Readlink(linkPath); err != nil || cur != targetDir {
			_ = os.RemoveAll(linkPath)
			if err := os.MkdirAll(p.actorsDir, 0o700); err != nil {
				return err
			}
			if err := os.Symlink(targetDir, linkPath); err != nil {
				return fmt.Errorf("re-creating actor disk symlink %q -> %q: %w", linkPath, targetDir, err)
			}
		}
		return nil
	}

	var chosenDisk string
	for _, d := range p.disks {
		if _, busy := p.diskToActor[d]; !busy {
			chosenDisk = d
			break
		}
	}
	if chosenDisk == "" {
		return fmt.Errorf("no free disks available in actor disk pool %q (all %d disks allocated)", p.poolDir, len(p.disks))
	}

	targetDir := filepath.Join(chosenDisk, actorUID)
	if err := imagecache.RemoveAllWritable(targetDir); err != nil {
		return fmt.Errorf("cleaning target dir %q: %w", targetDir, err)
	}
	if err := os.MkdirAll(targetDir, 0o700); err != nil {
		return fmt.Errorf("creating target dir %q: %w", targetDir, err)
	}

	if err := os.MkdirAll(p.actorsDir, 0o700); err != nil {
		return fmt.Errorf("creating actors dir %q: %w", p.actorsDir, err)
	}
	linkPath := filepath.Join(p.actorsDir, actorUID)
	if err := imagecache.RemoveAllWritable(linkPath); err != nil {
		return fmt.Errorf("removing old actor path %q: %w", linkPath, err)
	}
	if err := os.Symlink(targetDir, linkPath); err != nil {
		return fmt.Errorf("symlinking actor dir %q -> %q: %w", linkPath, targetDir, err)
	}

	p.actorToDisk[actorUID] = chosenDisk
	p.diskToActor[chosenDisk] = actorUID
	slog.Info("Allocated dedicated disk for actor", slog.String("actorUID", actorUID), slog.String("disk", filepath.Base(chosenDisk)))
	return nil
}

// ReleaseActor removes the actor's symlink and target directory on its
// dedicated disk, returning the disk to the free pool.
func (p *ActorDiskPool) ReleaseActor(actorUID string) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	linkPath := filepath.Join(p.actorsDir, actorUID)
	_ = os.Remove(linkPath)
	_ = imagecache.RemoveAllWritable(linkPath)

	if diskPath, ok := p.actorToDisk[actorUID]; ok {
		delete(p.actorToDisk, actorUID)
		delete(p.diskToActor, diskPath)
		targetDir := filepath.Join(diskPath, actorUID)
		if err := imagecache.RemoveAllWritable(targetDir); err != nil {
			return fmt.Errorf("removing actor directory on dedicated disk %q: %w", targetDir, err)
		}
		slog.Info("Released dedicated disk for actor", slog.String("actorUID", actorUID), slog.String("disk", filepath.Base(diskPath)))
	}
	return nil
}
