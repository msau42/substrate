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

package e2e

import (
	"slices"
	"strings"
	"testing"

	"go.ate.dev/substrate/internal/nodepath"
	corev1 "k8s.io/api/core/v1"
)

func TestNodeInspectorPod(t *testing.T) {
	pod := nodeInspectorPod("test-namespace", "node-a")

	if pod.Namespace != "test-namespace" {
		t.Errorf("Namespace = %q, want %q", pod.Namespace, "test-namespace")
	}
	if pod.Spec.NodeName != "node-a" {
		t.Errorf("NodeName = %q, want %q", pod.Spec.NodeName, "node-a")
	}
	if !slices.ContainsFunc(pod.Spec.Tolerations, func(tol corev1.Toleration) bool {
		return tol.Key == "" && tol.Operator == corev1.TolerationOpExists
	}) {
		t.Errorf("Tolerations = %v, want one tolerating every taint", pod.Spec.Tolerations)
	}

	wantHostPaths := []string{nodepath.ActorsDir, nodepath.SnapshotsDir}
	if len(pod.Spec.Volumes) != len(wantHostPaths) {
		t.Fatalf("Volumes = %v, want %d hostPath volumes", pod.Spec.Volumes, len(wantHostPaths))
	}
	for i, wantPath := range wantHostPaths {
		if pod.Spec.Volumes[i].HostPath == nil {
			t.Fatalf("Volumes[%d] = %v, want a hostPath volume", i, pod.Spec.Volumes[i])
		}
		if got := pod.Spec.Volumes[i].HostPath.Path; got != wantPath {
			t.Errorf("Volumes[%d] hostPath = %q, want %q", i, got, wantPath)
		}
	}

	if len(pod.Spec.Containers) != 1 {
		t.Fatalf("Containers = %v, want one", pod.Spec.Containers)
	}
	c := pod.Spec.Containers[0]
	if !strings.Contains(c.Image, "@sha256:") {
		t.Errorf("Image = %q, want it pinned by digest", c.Image)
	}
	if c.SecurityContext == nil || c.SecurityContext.RunAsUser == nil || *c.SecurityContext.RunAsUser != 0 {
		t.Errorf("SecurityContext = %v, want RunAsUser 0 to read atelet's root-owned directories", c.SecurityContext)
	}
	if len(c.VolumeMounts) != len(wantHostPaths) {
		t.Fatalf("VolumeMounts = %v, want %d", c.VolumeMounts, len(wantHostPaths))
	}
	for i, wantPath := range wantHostPaths {
		m := c.VolumeMounts[i]
		if m.Name != pod.Spec.Volumes[i].Name {
			t.Errorf("VolumeMounts[%d] names volume %q, want %q", i, m.Name, pod.Spec.Volumes[i].Name)
		}
		if !m.ReadOnly {
			t.Errorf("mount %q is writable, want read-only", m.Name)
		}
		// Mounted at the host path, so a path in the inspector is the path on the node.
		if m.MountPath != wantPath {
			t.Errorf("VolumeMounts[%d].MountPath = %q, want the host path %q", i, m.MountPath, wantPath)
		}
	}
}

func TestActorDirs(t *testing.T) {
	if got, want := actorDir("actor-uid"), nodepath.ActorsDir+"/actor-uid"; got != want {
		t.Errorf("actorDir = %q, want %q", got, want)
	}
	if got, want := actorSnapshotDir("actor-uid"), nodepath.SnapshotsDir+"/actor-uid"; got != want {
		t.Errorf("actorSnapshotDir = %q, want %q", got, want)
	}
	if got, want := localCheckpointsDir("actor-uid"), nodepath.SnapshotsDir+"/actor-uid/local-checkpoint"; got != want {
		t.Errorf("localCheckpointsDir = %q, want %q", got, want)
	}
	if got, want := snapshotStateFile("actor-uid"), nodepath.ActorsDir+"/actor-uid/snapshot-state.json"; got != want {
		t.Errorf("snapshotStateFile = %q, want %q", got, want)
	}
}

func TestParseSnapshotState(t *testing.T) {
	got, err := parseSnapshotState([]byte(`{"latestLocal":{"snapshot":{"snapshotUuid":"local-1"}},"latestDurable":{"snapshot":{"snapshotUuid":"durable-1"}}}`))
	if err != nil {
		t.Fatalf("parseSnapshotState: %v", err)
	}
	want := SnapshotState{LatestLocalUUID: "local-1", LatestDurableUUID: "durable-1"}
	if got != want {
		t.Errorf("parseSnapshotState = %+v, want %+v", got, want)
	}
	if _, err := parseSnapshotState([]byte("not-json")); err == nil {
		t.Error("parseSnapshotState(not-json) succeeded, want error")
	}
}

func TestParseDirListing(t *testing.T) {
	const dir = "/var/lib/ate/snapshots/uid/local-checkpoint"
	tests := []struct {
		name        string
		out         string
		wantEntries []string
		wantExists  bool
		wantErr     bool
	}{
		{name: "missing directory", out: ""},
		{name: "empty directory", out: dir + "\n", wantExists: true},
		{name: "one entry", out: dir + "\nsnap-1\n", wantEntries: []string{"snap-1"}, wantExists: true},
		{name: "sorted", out: dir + "\nsnap-b\nsnap-a\n", wantEntries: []string{"snap-a", "snap-b"}, wantExists: true},
		{name: "blank lines and carriage returns", out: dir + "\r\n\n snap-a \r\n", wantEntries: []string{"snap-a"}, wantExists: true},
		{name: "no trailing newline", out: dir + "\nsnap-a", wantEntries: []string{"snap-a"}, wantExists: true},
		{name: "output from something else", out: "sh: ls: not found\n", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entries, exists, err := parseDirListing([]byte(tt.out), dir)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseDirListing(%q) error = %v, want error %t", tt.out, err, tt.wantErr)
			}
			if !slices.Equal(entries, tt.wantEntries) || exists != tt.wantExists {
				t.Errorf("parseDirListing(%q) = %q, exists %t; want %q, exists %t", tt.out, entries, exists, tt.wantEntries, tt.wantExists)
			}
		})
	}
}

func TestPoll(t *testing.T) {
	calls := 0
	if !poll(0, func() bool { calls++; return true }) {
		t.Error("poll = false for a condition that holds, want true")
	}
	if calls != 1 {
		t.Errorf("poll checked a condition that already holds %d times, want 1", calls)
	}
	if poll(0, func() bool { return false }) {
		t.Error("poll = true for a condition that never holds, want false once the timeout passes")
	}
}
