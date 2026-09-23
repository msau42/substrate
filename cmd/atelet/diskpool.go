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
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// maxAttachedActorHyperdisks is the hard ceiling of actor Hyperdisks that can
// remain attached to a c3-standard-44 node simultaneously (32 GCE Hyperdisk limit
// minus 1 boot disk = 31 actor disks).
const maxAttachedActorHyperdisks = 31

// defaultProactiveAttachLimit is the target high-water mark of attached actor
// disks per node. When len(p.disks) exceeds this limit, atelet proactively
// detaches idle paused disks in the background so that free Hyperdisk attachment
// slots remain available for incoming resumes and cross-node migrations.
// With 31 max actor Hyperdisks (32 GCE limit - 1 boot disk), 29 leaves 2 free buffer slots.
const defaultProactiveAttachLimit = 29

func proactiveDiskLimit() int {
	if v := os.Getenv("ATELET_PROACTIVE_DISK_LIMIT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return defaultProactiveAttachLimit
}

// defaultMaxDiskOpQueue is the maximum number of concurrent/pending GCE disk
// attach/detach operations allowed on a node before ExportActorDisk rejects new
// cross-node exports so ateapi retries scheduling on the same node.
const defaultMaxDiskOpQueue = 5

func maxDiskOpQueueDepth() int {
	if v := os.Getenv("ATELET_MAX_DISK_OP_QUEUE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return defaultMaxDiskOpQueue
}

const (
	detachedPoolFilename = ".detached-pool.json"
	actorRefFilename     = ".actor-ref.json"
)

// ActorDiskPool allocates dedicated disk directories from a pool
// (e.g., /var/lib/ateom-gvisor/disk-pool/actor-disk-0, ...) to actors by
// symlinking /var/lib/ateom-gvisor/actors/<actorUID> -> <diskDir>/<actorUID>.
type ActorDiskPool struct {
	mu                  sync.Mutex
	poolDir             string
	actorsDir           string
	disks               []string
	actorToDisk         map[string]string
	diskToActor         map[string]string
	detachedDisks       map[string]string
	unallocatedDetached []string
	activeActors        map[string]time.Time
	pausedActors        map[string]time.Time
	actorRefs           map[string]resources.ActorRef
	detachingDisks       map[string]chan struct{}
	attachingDisks       map[string]struct{}
	lastRestoreCrossNode map[string]bool
	activeAttaches       int
	attacher             DiskAttacher
	workerClient         ateapipb.WorkerServiceClient
	pendingOps           atomic.Int32
	proactiveRunning     atomic.Bool
}

var globalActorDiskPool *ActorDiskPool

// NewActorDiskPool initializes the disk pool from poolDir and recovers any
// existing actor-to-disk symlink allocations from actorsDir. Returns nil, nil
// if poolDir is empty.
func NewActorDiskPool(poolDir, actorsDir string) (*ActorDiskPool, error) {
	if poolDir == "" {
		return nil, nil
	}
	if err := os.MkdirAll(poolDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating actor disk pool dir %q: %w", poolDir, err)
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
		slog.Warn("No pre-mounted disk directories found in actor disk pool at startup; disks may be imported dynamically", slog.String("poolDir", poolDir))
	}

	p := &ActorDiskPool{
		poolDir:              poolDir,
		actorsDir:            actorsDir,
		disks:                disks,
		actorToDisk:          make(map[string]string),
		diskToActor:          make(map[string]string),
		detachedDisks:        make(map[string]string),
		activeActors:         make(map[string]time.Time),
		pausedActors:         make(map[string]time.Time),
		actorRefs:            make(map[string]resources.ActorRef),
		detachingDisks:       make(map[string]chan struct{}),
		attachingDisks:       make(map[string]struct{}),
		lastRestoreCrossNode: make(map[string]bool),
		attacher:             NewGCEDiskAttacher(),
	}

	// Load unallocated detached disks from .detached-pool.json if present.
	detachedPoolPath := filepath.Join(poolDir, detachedPoolFilename)
	if data, err := os.ReadFile(detachedPoolPath); err == nil {
		var names []string
		if err := json.Unmarshal(data, &names); err == nil {
			mountedSet := make(map[string]bool, len(disks))
			for _, d := range disks {
				mountedSet[filepath.Base(d)] = true
			}
			for _, name := range names {
				if !mountedSet[name] {
					p.unallocatedDetached = append(p.unallocatedDetached, name)
				}
			}
			slog.Info("Loaded unallocated detached disks from pool manifest",
				slog.Int("detachedCount", len(p.unallocatedDetached)),
				slog.Int("mountedCount", len(disks)))
		}
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
					p.pausedActors[actorUID] = time.Now()
					if raw, err := os.ReadFile(filepath.Join(diskPath, actorUID, actorRefFilename)); err == nil {
						var ref resources.ActorRef
						if json.Unmarshal(raw, &ref) == nil && ref.Name != "" {
							p.actorRefs[actorUID] = ref
						}
					}
					slog.Info("Recovered actor disk allocation", slog.String("actorUID", actorUID), slog.String("disk", filepath.Base(diskPath)))
					break
				}
			}
		}
	}

	return p, nil
}

// SetWorkerServiceClient configures the control-plane WorkerService client used
// to report proactive disk detachments to ateapi.
func (p *ActorDiskPool) SetWorkerServiceClient(client ateapipb.WorkerServiceClient) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.workerClient = client
}

// RecordActorRef associates actorUID with its (atespace, name) reference and
// persists it on the actor's dedicated disk directory so proactive detachment
// can report the detached state back to ateapi.
func (p *ActorDiskPool) RecordActorRef(actorUID, atespace, actorName string) {
	if p == nil || actorUID == "" || atespace == "" || actorName == "" {
		return
	}
	ref := resources.ActorRef{Atespace: atespace, Name: actorName}
	p.mu.Lock()
	p.actorRefs[actorUID] = ref
	diskPath, ok := p.actorToDisk[actorUID]
	p.mu.Unlock()
	if ok && diskPath != "" {
		targetDir := filepath.Join(diskPath, actorUID)
		if raw, err := json.Marshal(ref); err == nil {
			_ = os.WriteFile(filepath.Join(targetDir, actorRefFilename), raw, 0o600)
		}
	}
}

func detachOnPauseEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("ATELET_DETACH_ON_PAUSE")))
	return v == "true" || v == "1"
}

// simulatedCrossNodeRestorePct returns the configured percentage (0..100) of
// Pause->Resume cycles that should simulate a cross-node disk detach/attach.
// Controlled via ATELET_SIMULATE_CROSS_NODE_PCT (0..100). If unset, falls back
// to 100% when ATELET_DETACH_ON_PAUSE=true, or 0% otherwise.
func simulatedCrossNodeRestorePct() float64 {
	if raw := strings.TrimSpace(os.Getenv("ATELET_SIMULATE_CROSS_NODE_PCT")); raw != "" {
		if pct, err := strconv.ParseFloat(raw, 64); err == nil {
			if pct <= 0 {
				return 0
			}
			if pct >= 100 {
				return 100
			}
			return pct
		}
	}
	if detachOnPauseEnabled() {
		return 100
	}
	return 0
}

func shouldSimulateCrossNodeRestore() bool {
	pct := simulatedCrossNodeRestorePct()
	if pct <= 0 {
		return false
	}
	if pct >= 100 {
		return true
	}
	return rand.Float64()*100.0 < pct
}

// WasCrossNodeRestore returns true if the most recent EnsureActorDir call for
// actorUID re-imported a detached disk (simulating a cross-node restore).
func (p *ActorDiskPool) WasCrossNodeRestore(actorUID string) bool {
	if p == nil || actorUID == "" {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastRestoreCrossNode[actorUID]
}

// NotifyActorPaused marks actorUID as paused and triggers background proactive
// detachment if the number of attached disks exceeds the proactive target (or
// immediately in parallel with probability ATELET_SIMULATE_CROSS_NODE_PCT).
func (p *ActorDiskPool) NotifyActorPaused(actorUID string) {
	if p == nil || actorUID == "" {
		return
	}
	p.mu.Lock()
	delete(p.activeActors, actorUID)
	p.pausedActors[actorUID] = time.Now()
	if shouldSimulateCrossNodeRestore() {
		diskPath, ok := p.actorToDisk[actorUID]
		if ok && diskPath != "" {
			gName, dName, _ := p.attacher.ResolveDiskMetadata(context.Background(), diskPath)
			if gName == "" {
				gName = filepath.Base(diskPath)
				dName = gName
			}
			var remaining []string
			for _, d := range p.disks {
				if d != diskPath {
					remaining = append(remaining, d)
				}
			}
			p.disks = remaining
			delete(p.actorToDisk, actorUID)
			delete(p.diskToActor, diskPath)
			p.detachedDisks[actorUID] = gName
			doneCh := make(chan struct{})
			p.detachingDisks[gName] = doneCh
			_ = os.Remove(filepath.Join(p.actorsDir, actorUID))
			p.mu.Unlock()

			go func(uid, dPath, gceName, devName string, ch chan struct{}) {
				t0 := time.Now()
				err := p.attacher.UnmountAndDetach(context.Background(), dPath, devName)
				p.mu.Lock()
				delete(p.detachingDisks, gceName)
				close(ch)
				p.mu.Unlock()
				slog.Info("Completed async parallel detach on pause",
					slog.String("actorUID", uid),
					slog.String("gceDiskName", gceName),
					slog.Bool("crossNodeSimulated", true),
					slog.Duration("elapsed", time.Since(t0)),
					slog.Any("err", err))
			}(actorUID, diskPath, gName, dName, doneCh)
			return
		}
	}
	p.mu.Unlock()
	p.triggerProactiveEviction()
}

func (p *ActorDiskPool) triggerProactiveEviction() {
	if p == nil {
		return
	}
	if !p.proactiveRunning.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer p.proactiveRunning.Store(false)
		time.Sleep(500 * time.Millisecond)
		limit := proactiveDiskLimit()
		for {
			p.mu.Lock()
			curAttached := len(p.disks)
			p.mu.Unlock()
			if curAttached <= limit {
				return
			}
			// Yield completely whenever critical-path attach/detach operations are running.
			if p.pendingOps.Load() > 0 {
				time.Sleep(250 * time.Millisecond)
				continue
			}
			if err := p.evictOneIdleDisk(context.Background()); err != nil {
				return
			}
		}
	}()
}

// evictOneIdleDisk unmounts and detaches one currently idle (paused or unallocated)
// actor disk from this node to free a GCE Hyperdisk attachment slot.
func (p *ActorDiskPool) evictOneIdleDisk(ctx context.Context) error {
	p.mu.Lock()
	var victimDisk string
	var victimActor string
	var victimGceName string
	var victimDevName string

	// 1. Try unallocated mounted disks first.
	var unallocated []string
	type pausedCandidate struct {
		disk     string
		actor    string
		pausedAt time.Time
	}
	var paused []pausedCandidate

	for _, d := range p.disks {
		actor, busy := p.diskToActor[d]
		if !busy {
			unallocated = append(unallocated, d)
			continue
		}
		if _, isActive := p.activeActors[actor]; isActive {
			continue
		}
		paused = append(paused, pausedCandidate{
			disk:     d,
			actor:    actor,
			pausedAt: p.pausedActors[actor],
		})
	}

	// Sort paused candidates by oldest pausedAt timestamp first (LRU eviction).
	sort.Slice(paused, func(i, j int) bool {
		return paused[i].pausedAt.Before(paused[j].pausedAt)
	})

	candidates := make([]string, 0, len(unallocated)+len(paused))
	candidates = append(candidates, unallocated...)
	for _, pc := range paused {
		candidates = append(candidates, pc.disk)
	}

	for _, d := range candidates {
		gName, dName, _ := p.attacher.ResolveDiskMetadata(ctx, d)
		if gName == "" {
			gName = filepath.Base(d)
			dName = gName
		}
		// Non-lazy unmount succeeds only if no active container holds open files/mounts on d.
		// Allow EINVAL/EPERM for unmounted test directories in unit tests.
		err := syscall.Unmount(d, 0)
		if err == nil || err == syscall.EINVAL || err == syscall.EPERM {
			victimDisk = d
			victimActor = p.diskToActor[d]
			victimGceName = gName
			victimDevName = dName
			break
		}
	}
	if victimDisk == "" {
		p.mu.Unlock()
		return fmt.Errorf("no idle disks available to evict on this node (all %d disks busy)", len(p.disks))
	}

	_ = os.Remove(victimDisk)
	var remaining []string
	for _, d := range p.disks {
		if d != victimDisk {
			remaining = append(remaining, d)
		}
	}
	p.disks = remaining

	doneCh := make(chan struct{})
	p.detachingDisks[victimGceName] = doneCh

	if victimActor != "" {
		delete(p.actorToDisk, victimActor)
		delete(p.diskToActor, victimDisk)
		p.detachedDisks[victimActor] = victimGceName
	} else {
		p.unallocatedDetached = append(p.unallocatedDetached, victimGceName)
	}
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		delete(p.detachingDisks, victimGceName)
		close(doneCh)
		p.mu.Unlock()
	}()

	slog.Info("Evicting idle actor disk",
		slog.String("victimActor", victimActor),
		slog.String("gceDiskName", victimGceName),
		slog.String("deviceName", victimDevName))
	if err := p.attacher.UnmountAndDetach(ctx, victimDisk, victimDevName); err != nil {
		return err
	}

	if victimActor != "" {
		p.mu.Lock()
		client := p.workerClient
		actorRef, hasRef := p.actorRefs[victimActor]
		p.mu.Unlock()
		if client != nil && hasRef && actorRef.Name != "" {
			if _, repErr := client.ReportActorDiskDetached(ctx, &ateapipb.ReportActorDiskDetachedRequest{
				Actor:       &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorUid:    victimActor,
				GceDiskName: victimGceName,
			}); repErr != nil {
				slog.Warn("Failed to report proactive actor disk detachment to ateapi",
					slog.String("victimActor", victimActor),
					slog.String("gceDiskName", victimGceName),
					slog.Any("err", repErr))
			} else {
				slog.Info("Reported proactive actor disk detachment to ateapi local_snapshot_info",
					slog.String("victimActor", victimActor),
					slog.String("gceDiskName", victimGceName))
			}
		}
	}
	return nil
}

// EnsureActorDir allocates a dedicated disk from the pool for actorUID (or
// reuses its existing allocation) and ensures the symlink
// <actorsDir>/<actorUID> -> <diskPath>/<actorUID> is in place.
func (p *ActorDiskPool) EnsureActorDir(actorUID string) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	delete(p.pausedActors, actorUID)
	p.activeActors[actorUID] = time.Now()

	// 1. Re-import if actor's disk was previously evicted while paused.
	if gceDiskName, isDetached := p.detachedDisks[actorUID]; isDetached {
		doneCh, isDetaching := p.detachingDisks[gceDiskName]
		delete(p.detachedDisks, actorUID)
		p.lastRestoreCrossNode[actorUID] = true
		p.mu.Unlock()
		tWait := time.Now()
		if isDetaching && doneCh != nil {
			slog.Info("Waiting for in-flight proactive detach before local re-import",
				slog.String("actorUID", actorUID), slog.String("gceDiskName", gceDiskName))
			<-doneCh
		}
		waitDur := time.Since(tWait)
		tImport := time.Now()
		slog.Info("Re-importing previously evicted actor disk on local resume",
			slog.String("actorUID", actorUID), slog.String("gceDiskName", gceDiskName),
			slog.Bool("crossNodeRestore", true),
			slog.Duration("waitDetach", waitDur))
		if err := p.importActorDiskInternal(context.Background(), actorUID, gceDiskName, gceDiskName, false); err != nil {
			p.mu.Lock()
			p.detachedDisks[actorUID] = gceDiskName
			p.mu.Unlock()
			return err
		}
		slog.Info("Completed local re-import on resume",
			slog.String("actorUID", actorUID), slog.String("gceDiskName", gceDiskName),
			slog.Bool("crossNodeRestore", true),
			slog.Duration("waitDetach", waitDur),
			slog.Duration("importDuration", time.Since(tImport)))
		return nil
	}

	// 2. Reuse if already attached and mounted on this node.
	if diskPath, ok := p.actorToDisk[actorUID]; ok {
		p.lastRestoreCrossNode[actorUID] = false
		defer p.mu.Unlock()
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

	p.lastRestoreCrossNode[actorUID] = false

	// 3. Allocate a free mounted disk if one is available.
	var chosenDisk string
	for _, d := range p.disks {
		if _, busy := p.diskToActor[d]; !busy {
			chosenDisk = d
			break
		}
	}
	if chosenDisk != "" {
		targetDir := filepath.Join(chosenDisk, actorUID)
		if err := imagecache.RemoveAllWritable(targetDir); err != nil {
			p.mu.Unlock()
			return fmt.Errorf("cleaning target dir %q: %w", targetDir, err)
		}
		if err := os.MkdirAll(targetDir, 0o700); err != nil {
			p.mu.Unlock()
			return fmt.Errorf("creating target dir %q: %w", targetDir, err)
		}
		if err := os.MkdirAll(p.actorsDir, 0o700); err != nil {
			p.mu.Unlock()
			return fmt.Errorf("creating actors dir %q: %w", p.actorsDir, err)
		}
		linkPath := filepath.Join(p.actorsDir, actorUID)
		if err := imagecache.RemoveAllWritable(linkPath); err != nil {
			p.mu.Unlock()
			return fmt.Errorf("removing old actor path %q: %w", linkPath, err)
		}
		if err := os.Symlink(targetDir, linkPath); err != nil {
			p.mu.Unlock()
			return fmt.Errorf("symlinking actor dir %q -> %q: %w", linkPath, targetDir, err)
		}
		p.actorToDisk[actorUID] = chosenDisk
		p.diskToActor[chosenDisk] = actorUID
		p.mu.Unlock()
		slog.Info("Allocated dedicated disk for actor", slog.String("actorUID", actorUID), slog.String("disk", filepath.Base(chosenDisk)))
		p.triggerProactiveEviction()
		return nil
	}

	// 4. Allocate from unallocated detached pool if available.
	if len(p.unallocatedDetached) > 0 {
		gceDiskName := p.unallocatedDetached[0]
		p.unallocatedDetached = p.unallocatedDetached[1:]
		doneCh, isDetaching := p.detachingDisks[gceDiskName]
		p.mu.Unlock()
		if isDetaching && doneCh != nil {
			<-doneCh
		}
		slog.Info("Importing unallocated detached pool disk for new actor",
			slog.String("actorUID", actorUID), slog.String("gceDiskName", gceDiskName))
		if err := p.importActorDiskInternal(context.Background(), actorUID, gceDiskName, gceDiskName, false); err != nil {
			p.mu.Lock()
			p.unallocatedDetached = append([]string{gceDiskName}, p.unallocatedDetached...)
			p.mu.Unlock()
			return err
		}
		return nil
	}

	p.mu.Unlock()
	return fmt.Errorf("no free disks available in actor disk pool %q (all %d mounted and %d detached disks allocated)",
		p.poolDir, len(p.disks), len(p.detachedDisks))
}

// ReleaseActor removes the actor's symlink and target directory on its
// dedicated disk, returning the disk to the free pool.
func (p *ActorDiskPool) ReleaseActor(actorUID string) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	delete(p.activeActors, actorUID)
	delete(p.pausedActors, actorUID)

	if gceDiskName, isDetached := p.detachedDisks[actorUID]; isDetached {
		delete(p.detachedDisks, actorUID)
		p.unallocatedDetached = append(p.unallocatedDetached, gceDiskName)
	}
	linkPath := filepath.Join(p.actorsDir, actorUID)
	_ = os.Remove(linkPath)
	_ = imagecache.RemoveAllWritable(linkPath)

	if diskPath, ok := p.actorToDisk[actorUID]; ok {
		delete(p.actorToDisk, actorUID)
		delete(p.diskToActor, diskPath)
		targetDir := filepath.Join(diskPath, actorUID)
		if err := imagecache.RemoveAllWritable(targetDir); err != nil {
			p.mu.Unlock()
			return fmt.Errorf("removing actor directory on dedicated disk %q: %w", targetDir, err)
		}
		slog.Info("Released dedicated disk for actor", slog.String("actorUID", actorUID), slog.String("disk", filepath.Base(diskPath)))
	}
	p.mu.Unlock()
	p.triggerProactiveEviction()
	return nil
}

// ExportActorDisk unmounts and detaches the actor's dedicated disk from this
// node so it can be attached to another node.
func (p *ActorDiskPool) ExportActorDisk(ctx context.Context, actorUID string) (string, string, error) {
	if p == nil {
		return "", "", fmt.Errorf("actor disk pool is not enabled on this node")
	}
	p.mu.Lock()
	if gceDiskName, isDetached := p.detachedDisks[actorUID]; isDetached {
		doneCh, isDetaching := p.detachingDisks[gceDiskName]
		delete(p.detachedDisks, actorUID)
		delete(p.pausedActors, actorUID)
		delete(p.activeActors, actorUID)
		linkPath := filepath.Join(p.actorsDir, actorUID)
		_ = os.Remove(linkPath)
		p.mu.Unlock()
		if isDetaching && doneCh != nil {
			slog.Info("Waiting for in-flight proactive detach before exporting disk",
				slog.String("actorUID", actorUID), slog.String("gceDiskName", gceDiskName))
			<-doneCh
		}
		slog.Info("Exported already-detached dedicated disk for actor (0ms detach)",
			slog.String("actorUID", actorUID), slog.String("gceDiskName", gceDiskName))
		return gceDiskName, gceDiskName, nil
	}

	diskPath, ok := p.actorToDisk[actorUID]
	if !ok {
		p.mu.Unlock()
		return "", "", fmt.Errorf("no dedicated disk found for actor %q on this node", actorUID)
	}

	maxQueue := maxDiskOpQueueDepth()
	for {
		cur := p.pendingOps.Load()
		if int(cur) >= maxQueue {
			p.mu.Unlock()
			slog.Info("Rejecting ExportActorDisk because node disk operation queue is full; caller should retry same-node scheduling",
				slog.String("actorUID", actorUID),
				slog.Int("pendingOps", int(cur)),
				slog.Int("maxQueue", maxQueue))
			return "", "", status.Errorf(codes.ResourceExhausted, "disk operation queue full on node (%d pending operations, max %d); retry resume on same node", cur, maxQueue)
		}
		if p.pendingOps.CompareAndSwap(cur, cur+1) {
			break
		}
	}
	defer p.pendingOps.Add(-1)

	gceDiskName, deviceName, err := p.attacher.ResolveDiskMetadata(ctx, diskPath)
	if err != nil {
		p.mu.Unlock()
		return "", "", fmt.Errorf("resolving disk metadata for actor %q at %q: %w", actorUID, diskPath, err)
	}

	// Remove symlink from actorsDir without deleting the checkpoint data on diskPath
	linkPath := filepath.Join(p.actorsDir, actorUID)
	_ = os.Remove(linkPath)

	delete(p.actorToDisk, actorUID)
	delete(p.diskToActor, diskPath)
	delete(p.activeActors, actorUID)
	delete(p.pausedActors, actorUID)
	var remaining []string
	for _, d := range p.disks {
		if d != diskPath {
			remaining = append(remaining, d)
		}
	}
	p.disks = remaining
	doneCh := make(chan struct{})
	p.detachingDisks[gceDiskName] = doneCh
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		delete(p.detachingDisks, gceDiskName)
		close(doneCh)
		p.mu.Unlock()
	}()

	if err := p.attacher.UnmountAndDetach(ctx, diskPath, deviceName); err != nil {
		return "", "", fmt.Errorf("unmounting and detaching disk %q (device %q) for actor %q: %w", gceDiskName, deviceName, actorUID, err)
	}

	slog.Info("Exported dedicated disk for actor", slog.String("actorUID", actorUID), slog.String("gceDiskName", gceDiskName), slog.String("deviceName", deviceName))
	return gceDiskName, deviceName, nil
}

// ImportActorDisk attaches and mounts a dedicated disk from another node onto
// this node, binds it to actorUID in the disk pool, and creates the actor's symlink.
func (p *ActorDiskPool) ImportActorDisk(ctx context.Context, actorUID, gceDiskName, deviceName string) error {
	return p.importActorDiskInternal(ctx, actorUID, gceDiskName, deviceName, true)
}

func (p *ActorDiskPool) importActorDiskInternal(ctx context.Context, actorUID, gceDiskName, deviceName string, enforceQueueLimit bool) error {
	if p == nil {
		return fmt.Errorf("actor disk pool is not enabled on this node")
	}
	if gceDiskName == "" {
		return fmt.Errorf("gceDiskName is required to import actor disk")
	}

	if enforceQueueLimit {
		maxQueue := maxDiskOpQueueDepth()
		for {
			cur := p.pendingOps.Load()
			if int(cur) >= maxQueue {
				slog.Info("Rejecting ImportActorDisk because node disk operation queue is full",
					slog.String("actorUID", actorUID),
					slog.String("gceDiskName", gceDiskName),
					slog.Int("pendingOps", int(cur)),
					slog.Int("maxQueue", maxQueue))
				return status.Errorf(codes.ResourceExhausted, "disk operation queue full on node (%d pending operations, max %d)", cur, maxQueue)
			}
			if p.pendingOps.CompareAndSwap(cur, cur+1) {
				break
			}
		}
	} else {
		p.pendingOps.Add(1)
	}
	defer p.pendingOps.Add(-1)

	p.mu.Lock()
	delete(p.pausedActors, actorUID)
	p.activeActors[actorUID] = time.Now()
	delete(p.detachedDisks, actorUID)
	doneCh, isDetaching := p.detachingDisks[gceDiskName]
	for i, name := range p.unallocatedDetached {
		if name == gceDiskName {
			p.unallocatedDetached = append(p.unallocatedDetached[:i], p.unallocatedDetached[i+1:]...)
			break
		}
	}
	p.mu.Unlock()

	if isDetaching && doneCh != nil {
		slog.Info("Waiting for in-flight detach to finish before importing disk",
			slog.String("actorUID", actorUID), slog.String("gceDiskName", gceDiskName))
		<-doneCh
	}

	p.mu.Lock()
	p.attachingDisks[gceDiskName] = struct{}{}
	slotReserved := true
	defer func() {
		if slotReserved {
			p.mu.Lock()
			delete(p.attachingDisks, gceDiskName)
			p.mu.Unlock()
		}
	}()

	for {
		if len(p.disks)+len(p.attachingDisks) > maxAttachedActorHyperdisks {
			p.mu.Unlock()
			slog.Info("Evicting idle disk to make room for incoming import",
				slog.String("actorUID", actorUID),
				slog.String("gceDiskName", gceDiskName))
			if err := p.evictOneIdleDisk(ctx); err != nil {
				return fmt.Errorf("evicting idle disk before importing %q: %w", gceDiskName, err)
			}
			p.mu.Lock()
			continue
		}

		if len(p.disks)+len(p.detachingDisks)+p.activeAttaches < maxAttachedActorHyperdisks {
			p.activeAttaches++
			p.mu.Unlock()
			break
		}

		if len(p.detachingDisks) > 0 {
			var waitCh chan struct{}
			for _, ch := range p.detachingDisks {
				waitCh = ch
				break
			}
			p.mu.Unlock()
			slog.Info("Waiting for in-flight GCE detach to complete before attaching disk",
				slog.String("actorUID", actorUID),
				slog.String("gceDiskName", gceDiskName))
			<-waitCh
			p.mu.Lock()
			continue
		}

		p.mu.Unlock()
		if err := p.evictOneIdleDisk(ctx); err != nil {
			return fmt.Errorf("evicting idle disk for physical slot before importing %q: %w", gceDiskName, err)
		}
		p.mu.Lock()
	}

	activeAttachReserved := true
	defer func() {
		if activeAttachReserved {
			p.mu.Lock()
			p.activeAttaches--
			p.mu.Unlock()
		}
	}()

	// Always use gceDiskName as the deviceName on the target node to prevent
	// device name collisions with pre-existing disks (e.g. actor-disk-0).
	attachDeviceName := gceDiskName
	mountPath := filepath.Join(p.poolDir, attachDeviceName)

	if err := p.attacher.AttachAndMount(ctx, gceDiskName, attachDeviceName, mountPath); err != nil {
		return fmt.Errorf("attaching and mounting disk %q for actor %q: %w", gceDiskName, actorUID, err)
	}

	if !enforceQueueLimit {
		if entries, err := os.ReadDir(mountPath); err == nil {
			for _, entry := range entries {
				name := entry.Name()
				if name != diskMetadataFilename && name != actorUID && name != "lost+found" {
					_ = imagecache.RemoveAllWritable(filepath.Join(mountPath, name))
				}
			}
		}
	}

	targetDir := filepath.Join(mountPath, actorUID)
	if err := os.MkdirAll(targetDir, 0o700); err != nil {
		return fmt.Errorf("ensuring target dir %q on imported disk: %w", targetDir, err)
	}
	if err := os.MkdirAll(p.actorsDir, 0o700); err != nil {
		return fmt.Errorf("ensuring actors dir %q: %w", p.actorsDir, err)
	}
	linkPath := filepath.Join(p.actorsDir, actorUID)
	_ = os.RemoveAll(linkPath)
	if err := os.Symlink(targetDir, linkPath); err != nil {
		return fmt.Errorf("symlinking imported actor dir %q -> %q: %w", linkPath, targetDir, err)
	}

	p.mu.Lock()
	p.activeAttaches--
	activeAttachReserved = false
	delete(p.attachingDisks, gceDiskName)
	slotReserved = false
	found := false
	for _, d := range p.disks {
		if d == mountPath {
			found = true
			break
		}
	}
	if !found {
		p.disks = append(p.disks, mountPath)
		sort.Strings(p.disks)
	}

	p.actorToDisk[actorUID] = mountPath
	p.diskToActor[mountPath] = actorUID
	if raw, err := os.ReadFile(filepath.Join(targetDir, actorRefFilename)); err == nil {
		var ref resources.ActorRef
		if json.Unmarshal(raw, &ref) == nil && ref.Name != "" {
			p.actorRefs[actorUID] = ref
		}
	}
	p.mu.Unlock()

	slog.Info("Imported dedicated disk for actor", slog.String("actorUID", actorUID), slog.String("gceDiskName", gceDiskName), slog.String("mountPath", mountPath))
	p.triggerProactiveEviction()
	return nil
}
