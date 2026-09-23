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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"cloud.google.com/go/compute/metadata"
	"golang.org/x/oauth2/google"
)

const diskMetadataFilename = ".disk-metadata.json"

type diskMetadataFile struct {
	GCEDiskName string `json:"gceDiskName"`
	DeviceName  string `json:"deviceName"`
}

// DiskAttacher abstracts GCE disk detachment/attachment and host mount/unmount
// operations so that ActorDiskPool can be unit-tested without root or GCP APIs.
type DiskAttacher interface {
	ResolveDiskMetadata(ctx context.Context, mountPath string) (gceDiskName, deviceName string, err error)
	UnmountAndDetach(ctx context.Context, mountPath, deviceName string) error
	AttachAndMount(ctx context.Context, gceDiskName, deviceName, mountPath string) error
}

// GCEDiskAttacher implements DiskAttacher using GCE Compute Engine REST APIs
// and Linux mount/umount operations.
type GCEDiskAttacher struct {
	opMu         sync.Mutex
	project      string
	zone         string
	instanceName string
}

// NewGCEDiskAttacher creates a new GCEDiskAttacher, resolving instance identity
// from environment variables or the GCE metadata server.
func NewGCEDiskAttacher() *GCEDiskAttacher {
	return &GCEDiskAttacher{
		project:      os.Getenv("GCP_PROJECT_ID"),
		zone:         os.Getenv("GCP_ZONE"),
		instanceName: os.Getenv("NODE_NAME"),
	}
}

func (g *GCEDiskAttacher) ensureIdentity(ctx context.Context) error {
	if g.project == "" {
		proj, err := metadata.ProjectIDWithContext(ctx)
		if err != nil {
			return fmt.Errorf("resolving GCE project ID from metadata: %w", err)
		}
		g.project = strings.TrimSpace(proj)
	}
	if g.zone == "" {
		z, err := metadata.ZoneWithContext(ctx)
		if err != nil {
			return fmt.Errorf("resolving GCE zone from metadata: %w", err)
		}
		// metadata.Zone returns "projects/<num>/zones/<zone>" or "<zone>"
		parts := strings.Split(strings.TrimSpace(z), "/")
		g.zone = parts[len(parts)-1]
	}
	if g.instanceName == "" {
		inst, err := metadata.InstanceNameWithContext(ctx)
		if err != nil {
			return fmt.Errorf("resolving GCE instance name from metadata: %w", err)
		}
		g.instanceName = strings.TrimSpace(inst)
	}
	return nil
}

// ResolveDiskMetadata reads or discovers the GCE disk name and device name for
// a mounted disk path.
func (g *GCEDiskAttacher) ResolveDiskMetadata(ctx context.Context, mountPath string) (string, string, error) {
	metaPath := filepath.Join(mountPath, diskMetadataFilename)
	if data, err := os.ReadFile(metaPath); err == nil {
		var meta diskMetadataFile
		if err := json.Unmarshal(data, &meta); err == nil && meta.GCEDiskName != "" && meta.DeviceName != "" {
			return meta.GCEDiskName, meta.DeviceName, nil
		}
	}

	// Fallback: discover device from /proc/self/mountinfo and /dev/disk/by-id/google-*
	devNode, err := findMountSourceDevice(mountPath)
	if err != nil {
		return "", "", fmt.Errorf("discovering device for mount %q: %w", mountPath, err)
	}
	deviceName, err := findGoogleDeviceName(devNode)
	if err != nil {
		return "", "", fmt.Errorf("discovering google device name for %q: %w", devNode, err)
	}

	if err := g.ensureIdentity(ctx); err != nil {
		return "", "", err
	}
	gceDiskName, err := g.lookupDiskNameFromInstance(ctx, deviceName)
	if err != nil {
		return "", "", fmt.Errorf("looking up GCE disk name for device %q on instance %q: %w", deviceName, g.instanceName, err)
	}

	// Persist metadata onto the disk root before it gets unmounted so future
	// nodes have self-describing metadata immediately.
	meta := diskMetadataFile{
		GCEDiskName: gceDiskName,
		DeviceName:  gceDiskName, // Use globally unique gceDiskName on subsequent imports
	}
	if raw, err := json.Marshal(meta); err == nil {
		_ = os.WriteFile(metaPath, raw, 0o644)
	}

	return gceDiskName, deviceName, nil
}

func findMountSourceDevice(mountPath string) (string, error) {
	cleanMount := filepath.Clean(mountPath)
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return "", err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		// mountinfo format:
		// 36 35 98:0 /mnt1 /mnt2 rw,noatime master:1 - ext3 /dev/root rw,errors=continue
		parts := strings.Fields(line)
		if len(parts) < 10 {
			continue
		}
		mountPoint := parts[4]
		if filepath.Clean(mountPoint) != cleanMount {
			continue
		}
		// Find separator "-"
		sepIdx := -1
		for i, field := range parts {
			if field == "-" {
				sepIdx = i
				break
			}
		}
		if sepIdx != -1 && sepIdx+2 < len(parts) {
			return parts[sepIdx+2], nil
		}
	}
	return "", fmt.Errorf("mount point %q not found in /proc/self/mountinfo", mountPath)
}

func findGoogleDeviceName(devNode string) (string, error) {
	realDev, err := filepath.EvalSymlinks(devNode)
	if err != nil {
		realDev = devNode
	}
	for _, dir := range []string{"/host/dev/disk/by-id", "/dev/disk/by-id"} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			name := entry.Name()
			if !strings.HasPrefix(name, "google-") || strings.Contains(name, "-part") {
				continue
			}
			fullLink := filepath.Join(dir, name)
			target, err := filepath.EvalSymlinks(fullLink)
			if err == nil && filepath.Base(target) == filepath.Base(realDev) {
				return strings.TrimPrefix(name, "google-"), nil
			}
		}
	}
	return "", fmt.Errorf("no google-* disk-by-id symlink points to %q", realDev)
}

func (g *GCEDiskAttacher) lookupDiskNameFromInstance(ctx context.Context, deviceName string) (string, error) {
	client, err := google.DefaultClient(ctx, "https://www.googleapis.com/auth/compute")
	if err != nil {
		return "", fmt.Errorf("creating google default client: %w", err)
	}
	url := fmt.Sprintf("https://compute.googleapis.com/compute/v1/projects/%s/zones/%s/instances/%s",
		g.project, g.zone, g.instanceName)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET instance %s returned status %d: %s", g.instanceName, resp.StatusCode, string(body))
	}

	var inst struct {
		Disks []struct {
			DeviceName string `json:"deviceName"`
			Source     string `json:"source"`
		} `json:"disks"`
	}
	if err := json.Unmarshal(body, &inst); err != nil {
		return "", fmt.Errorf("unmarshaling instance response: %w", err)
	}
	for _, d := range inst.Disks {
		if d.DeviceName == deviceName {
			parts := strings.Split(d.Source, "/")
			return parts[len(parts)-1], nil
		}
	}
	return "", fmt.Errorf("device %q not found on GCE instance %q", deviceName, g.instanceName)
}

func simulatedDiskOpDuration() (time.Duration, bool) {
	if v := os.Getenv("ATELET_SIMULATE_DISK_OP_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			return time.Duration(ms) * time.Millisecond, true
		}
	}
	return 0, false
}

// syncAndEvictCache flushes dirty file data under rootDir to the underlying
// block device and advises the kernel to drop cached pages before unmount.
func syncAndEvictCache(rootDir string) {
	_ = filepath.WalkDir(rootDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		f, oerr := os.Open(path)
		if oerr != nil {
			return nil
		}
		_ = f.Sync()
		_, _, _ = syscall.Syscall6(syscall.SYS_FADVISE64, f.Fd(), 0, 0, 4 /* POSIX_FADV_DONTNEED */, 0, 0)
		_ = f.Close()
		return nil
	})
}

// UnmountAndDetach unmounts mountPath on the host and detaches deviceName from
// this GCE VM instance.
func (g *GCEDiskAttacher) UnmountAndDetach(ctx context.Context, mountPath, deviceName string) error {
	simDur, isSimulated := simulatedDiskOpDuration()
	if !isSimulated {
		if err := g.ensureIdentity(ctx); err != nil {
			return err
		}
	}

	slog.Info("Unmounting actor disk before detach", slog.String("mountPath", mountPath), slog.String("deviceName", deviceName), slog.Bool("simulated", isSimulated))
	syncAndEvictCache(mountPath)

	var umountErr error
	for attempt := 0; attempt < 5; attempt++ {
		umountErr = syscall.Unmount(mountPath, 0)
		if umountErr == nil || umountErr == syscall.EINVAL || umountErr == syscall.ENOENT {
			umountErr = nil
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if umountErr != nil {
		// Fallback to lazy unmount or external umount command if busy briefly
		cmd := exec.CommandContext(ctx, "umount", mountPath)
		if out, cmdErr := cmd.CombinedOutput(); cmdErr != nil {
			slog.Warn("Standard umount failed, attempting lazy unmount", slog.String("mountPath", mountPath), slog.String("output", string(out)), slog.Any("err", umountErr))
			if lazyErr := syscall.Unmount(mountPath, syscall.MNT_DETACH); lazyErr != nil && lazyErr != syscall.EINVAL && lazyErr != syscall.ENOENT {
				return fmt.Errorf("unmounting %q: %w (lazy: %v)", mountPath, umountErr, lazyErr)
			}
		}
	}
	_ = os.Remove(mountPath)

	if isSimulated {
		slog.Info("Simulating parallel GCE detachDisk", slog.String("deviceName", deviceName), slog.Duration("duration", simDur))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(simDur):
			return nil
		}
	}

	slog.Info("Detaching GCE disk from node", slog.String("instance", g.instanceName), slog.String("deviceName", deviceName))
	client, err := google.DefaultClient(ctx, "https://www.googleapis.com/auth/compute")
	if err != nil {
		return fmt.Errorf("creating google compute client: %w", err)
	}

	g.opMu.Lock()
	defer g.opMu.Unlock()

	detachURL := fmt.Sprintf("https://compute.googleapis.com/compute/v1/projects/%s/zones/%s/instances/%s/detachDisk?deviceName=%s",
		g.project, g.zone, g.instanceName, deviceName)

	var body []byte
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, detachURL, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("calling GCE detachDisk: %w", err)
		}
		body, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			break
		}
		if attempt < 4 && (resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable) {
			slog.Warn("GCE detachDisk busy, retrying...", slog.Int("status", resp.StatusCode), slog.String("body", string(body)))
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}
		return fmt.Errorf("detachDisk failed with status %d: %s", resp.StatusCode, string(body))
	}

	return g.pollZoneOperation(ctx, client, body)
}

// AttachAndMount attaches gceDiskName to this GCE VM instance as deviceName,
// waits for the block device to appear, and mounts it at mountPath.
func (g *GCEDiskAttacher) AttachAndMount(ctx context.Context, gceDiskName, deviceName, mountPath string) error {
	if simDur, isSimulated := simulatedDiskOpDuration(); isSimulated {
		slog.Info("Simulating parallel GCE attachDisk", slog.String("gceDiskName", gceDiskName), slog.String("deviceName", deviceName), slog.Duration("duration", simDur))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(simDur):
		}
	} else {
		if err := g.ensureIdentity(ctx); err != nil {
			return err
		}

		slog.Info("Attaching GCE disk to node", slog.String("instance", g.instanceName), slog.String("gceDiskName", gceDiskName), slog.String("deviceName", deviceName))
		client, err := google.DefaultClient(ctx, "https://www.googleapis.com/auth/compute")
		if err != nil {
			return fmt.Errorf("creating google compute client: %w", err)
		}

		attachURL := fmt.Sprintf("https://compute.googleapis.com/compute/v1/projects/%s/zones/%s/instances/%s/attachDisk",
			g.project, g.zone, g.instanceName)
		payload := map[string]any{
			"source":     fmt.Sprintf("projects/%s/zones/%s/disks/%s", g.project, g.zone, gceDiskName),
			"deviceName": deviceName,
			"mode":       "READ_WRITE",
		}
		rawPayload, err := json.Marshal(payload)
		if err != nil {
			return err
		}

		if err := func() error {
			g.opMu.Lock()
			defer g.opMu.Unlock()

			var body []byte
			for attempt := 0; attempt < 5; attempt++ {
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, attachURL, bytes.NewReader(rawPayload))
				if err != nil {
					return err
				}
				req.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(req)
				if err != nil {
					return fmt.Errorf("calling GCE attachDisk: %w", err)
				}
				body, _ = io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					break
				}
				if attempt < 4 && (resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable) {
					slog.Warn("GCE attachDisk busy, retrying...", slog.Int("status", resp.StatusCode), slog.String("body", string(body)))
					time.Sleep(time.Duration(attempt+1) * time.Second)
					continue
				}
				return fmt.Errorf("attachDisk failed with status %d: %s", resp.StatusCode, string(body))
			}

			return g.pollZoneOperation(ctx, client, body)
		}(); err != nil {
			return fmt.Errorf("waiting for attachDisk operation: %w", err)
		}
	}

	var devPath string
	deadline := time.Now().Add(25 * time.Second)
	for {
		for _, candidate := range []string{
			filepath.Join("/host/dev/disk/by-id", "google-"+deviceName),
			filepath.Join("/dev/disk/by-id", "google-"+deviceName),
		} {
			if _, err := os.Stat(candidate); err == nil {
				devPath = candidate
				break
			}
		}
		if devPath != "" {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for device google-%s to appear after GCE attachDisk", deviceName)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}

	if err := os.MkdirAll(mountPath, 0o755); err != nil {
		return fmt.Errorf("creating mount directory %q: %w", mountPath, err)
	}

	realDev, err := filepath.EvalSymlinks(devPath)
	if err != nil {
		realDev = devPath
	}

	slog.Info("Mounting newly attached GCE disk", slog.String("devPath", realDev), slog.String("mountPath", mountPath))
	if err := syscall.Mount(realDev, mountPath, "ext4", 0, "discard"); err != nil {
		// Fallback to mount binary if present
		cmd := exec.CommandContext(ctx, "mount", "-o", "discard,defaults", realDev, mountPath)
		if out, cmdErr := cmd.CombinedOutput(); cmdErr != nil {
			return fmt.Errorf("mounting %q to %q failed: syscall err=%w, cmd err=%v (output: %s)", realDev, mountPath, err, cmdErr, string(out))
		}
	}

	// Ensure .disk-metadata.json reflects the current deviceName
	meta := diskMetadataFile{
		GCEDiskName: gceDiskName,
		DeviceName:  deviceName,
	}
	if raw, err := json.Marshal(meta); err == nil {
		_ = os.WriteFile(filepath.Join(mountPath, diskMetadataFilename), raw, 0o644)
	}

	return nil
}

func (g *GCEDiskAttacher) pollZoneOperation(ctx context.Context, client *http.Client, opBody []byte) error {
	var op struct {
		Name   string `json:"name"`
		Status string `json:"status"`
		Error  *struct {
			Errors []struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"errors"`
		} `json:"error"`
	}
	if err := json.Unmarshal(opBody, &op); err != nil {
		return fmt.Errorf("parsing GCE operation response: %w", err)
	}
	if op.Name == "" {
		return fmt.Errorf("GCE operation response missing name: %s", string(opBody))
	}

	opURL := fmt.Sprintf("https://compute.googleapis.com/compute/v1/projects/%s/zones/%s/operations/%s",
		g.project, g.zone, op.Name)

	for {
		if op.Status == "DONE" {
			if op.Error != nil && len(op.Error.Errors) > 0 {
				return fmt.Errorf("GCE zone operation %s failed: %s (%s)",
					op.Name, op.Error.Errors[0].Message, op.Error.Errors[0].Code)
			}
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(400 * time.Millisecond):
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, opURL, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("polling GCE zone operation %s: %w", op.Name, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("polling GCE zone operation %s returned status %d: %s", op.Name, resp.StatusCode, string(body))
		}
		if err := json.Unmarshal(body, &op); err != nil {
			return fmt.Errorf("unmarshaling polled GCE operation: %w", err)
		}
	}
}
