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

package demo

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const demoAtespace = "demo"

func TestActorLifecycle(t *testing.T) {
	t.Parallel()
	// Create namespace
	nsObj := e2e.CreateNamespace(t)

	ctx := context.Background()
	clients := e2e.GetClients()

	// Create actor template.
	at, err := createActorTemplate(ctx, t, clients, nsObj, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
	if err != nil {
		t.Fatalf("failed to initialize ActorTemplate: %v", err)
	}

	tests := []struct {
		name string
		f    func(ctx context.Context, t *testing.T, clients *e2e.Clients, ns *e2e.Namespace, at *ateapi.ActorTemplate) error
	}{
		{
			name: "CreateActor",
			f:    createActor,
		},
		{
			name: "PauseResumeActor",
			f:    pauseActor,
		},
		{
			name: "SuspendResumeActor",
			f:    suspendActor,
		},
		{
			name: "DeleteRunningActor",
			f:    deleteRunningActor,
		},
		{
			name: "DeletePausedActor",
			f:    deletePausedActor,
		},
		{
			name: "RevertResumeActor",
			f:    revertActor,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := tc.f(ctx, t, clients, nsObj, at); err != nil {
				t.Errorf("Test %q failed: %v", tc.name, err)
			}
		})
	}
}

func TestActorSnapshotLifecycle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	clients := e2e.GetClients()
	nsObj := e2e.CreateNamespace(t)

	at, err := createActorTemplate(ctx, t, clients, nsObj, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
	if err != nil {
		t.Fatalf("failed to initialize ActorTemplate: %v", err)
	}

	goldenStamp := goldenSnapshotRuntime(t, ctx, clients, at)
	wantClass := string(atev1.SandboxClassGvisor)
	if e2e.IsMicroVM() {
		wantClass = string(atev1.SandboxClassMicroVM)
	}
	if got := goldenStamp.GetSandboxClass(); got != wantClass {
		t.Errorf("golden snapshot sandbox_class = %q, want %q", got, wantClass)
	}
	if got := goldenStamp.GetVersion().GetSchemaVersion(); got != hardware.SchemaVersionV1 {
		t.Errorf("golden snapshot schema_version = %q, want %q", got, hardware.SchemaVersionV1)
	}
	if runtimeAttribute(goldenStamp, hardware.AttrArchitecture) == "" {
		t.Errorf("golden snapshot stamp %v has no %q attribute", goldenStamp, hardware.AttrArchitecture)
	}

	sourceName := "snapshot-source-" + nsObj.Name
	cloneName := "snapshot-clone-" + nsObj.Name
	for _, name := range []string{sourceName, cloneName} {
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapi.DeleteActorRequest{
				Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: name},
			})
		})
	}

	sourceCreated, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: sourceName},
		ActorTemplate: e2e.TemplateRef(at),
	}})
	if err != nil {
		t.Fatalf("failed to create source Actor: %v", err)
	}
	if diff := cmp.Diff(goldenStamp, durableSnapshot(sourceCreated.GetStatus()).GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("golden snapshot stamp mismatch (-golden +actor):\n%s", diff)
	}
	sourceResumed, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: sourceName}})
	if err != nil {
		t.Fatalf("failed to resume source Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, sourceName, ateapi.ActorState_ACTOR_STATE_RUNNING)
	assignment := sourceResumed.GetActor().GetStatus().GetWorkerAssignment()
	worker, err := clients.SubstrateAPI.GetWorker(ctx, &ateapi.GetWorkerRequest{Worker: assignment.GetWorker()})
	if err != nil {
		t.Fatalf("failed to get assigned Worker %s: %v", assignment.GetWorker().GetName(), err)
	}
	if diff := cmp.Diff(hardware.ColdBootRuntime(worker.GetStatus()), assignment.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("worker_assignment.sandbox_runtime mismatch (-worker default runtime +assignment):\n%s", diff)
	}
	if !hardware.Matches(assignment.GetSandboxRuntime(), goldenStamp) {
		t.Fatalf("assigned runtime %v cannot restore the golden stamp %v", assignment.GetSandboxRuntime(), goldenStamp)
	}
	response, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: sourceName})
	if err != nil {
		t.Fatalf("failed to call source Actor: %v", err)
	}
	validateCounterResponse(t, response, "source", 1, 1)

	tagRef := &ateapi.ObjectRef{Atespace: demoAtespace, Name: "e2e-" + nsObj.Name}
	t.Cleanup(func() {
		_, _ = clients.SubstrateAPI.DeleteTag(context.Background(), &ateapi.DeleteTagRequest{Tag: tagRef})
	})
	suspended, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor:   &ateapi.ObjectRef{Atespace: demoAtespace, Name: sourceName},
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("failed to suspend source Actor: %v", err)
	}
	suspendedSnap := getLocalSnapshotFromActor(t, suspended.GetActor()).snap
	snapshotURI := suspendedSnap.GetDurableSnapshot().GetObject().GetSnapshotUri()
	if snapshotURI == "" {
		t.Fatal("suspended Actor has no external snapshot URI")
	}
	snapshotStamp := suspendedSnap.GetSandboxRuntime()
	if diff := cmp.Diff(assignment.GetSandboxRuntime(), snapshotStamp, protocmp.Transform()); diff != "" {
		t.Fatalf("suspended snapshot stamp mismatch (-assignment +snapshot):\n%s", diff)
	}
	assertSnapshotUnderLocation(t, snapshotURI, wantSnapshotLocation(t, at))
	// Tagging an asynchronously suspended actor promotes its snapshot to
	// durable storage if it has not finished uploading yet, then copies it to
	// the tag's own URI so it survives the actor being suspended again or
	// deleted.
	tagToUpdate, err := clients.SubstrateAPI.CreateTag(ctx, &ateapi.CreateTagRequest{
		Tag: &ateapi.Tag{
			Metadata:    &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: tagRef.GetName()},
			Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: sourceName},
		},
	})
	if err != nil {
		t.Fatalf("failed to create the Tag: %v", err)
	}
	sourceAfterTag, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: sourceName},
	})
	if err != nil {
		t.Fatalf("failed to get source Actor after CreateTag: %v", err)
	}
	if got := durableSnapshotURI(sourceAfterTag.GetStatus()); got != snapshotURI {
		t.Errorf("source Actor durable snapshot URI after CreateTag = %q, want %q", got, snapshotURI)
	}
	if got := tagSnapshotURI(tagToUpdate); got == "" || got == snapshotURI {
		t.Fatalf("Tag %s snapshot uri = %q, want a copy of its own", tagRef.GetName(), got)
	}
	if _, err := resources.ParseSnapshotURI(tagSnapshotURI(tagToUpdate)); err != nil {
		t.Errorf("ParseSnapshotURI(%q): %v", tagSnapshotURI(tagToUpdate), err)
	}
	if diff := cmp.Diff(snapshotStamp, tagToUpdate.GetStatus().GetSnapshot().GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("tag snapshot stamp mismatch (-actor snapshot +tag):\n%s", diff)
	}
	assertSnapshotUnderLocation(t, tagSnapshotURI(tagToUpdate), wantSnapshotLocation(t, at))
	listed, err := clients.SubstrateAPI.ListTags(ctx, &ateapi.ListTagsRequest{Atespace: demoAtespace})
	if err != nil {
		t.Fatalf("failed to list Tags: %v", err)
	}
	found := false
	for _, candidate := range listed.GetTags() {
		if candidate.GetMetadata().GetName() == tagRef.GetName() {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("tag %q missing from ListTags", tagRef.GetName())
	}

	tagToUpdate.Scope = ateapi.TagScope_TAG_SCOPE_PUBLISHED
	if _, err := clients.SubstrateAPI.UpdateTag(ctx, &ateapi.UpdateTagRequest{
		Tag: tagToUpdate,
	}); err != nil {
		t.Fatalf("failed to publish Tag: %v", err)
	}

	cloned, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{
		Actor: &ateapi.Actor{
			Metadata:      &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: cloneName},
			ActorTemplate: e2e.TemplateRef(at),
			SourceTag:     tagRef,
		},
	})
	if err != nil {
		t.Fatalf("failed to create Actor from Tag: %v", err)
	}
	if diff := cmp.Diff(snapshotStamp, durableSnapshot(cloned.GetStatus()).GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Fatalf("cloned actor snapshot stamp mismatch (-tag +clone):\n%s", diff)
	}
	cloneResumed, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: cloneName}})
	if err != nil {
		t.Fatalf("failed to resume cloned Actor: %v", err)
	}
	if diff := cmp.Diff(snapshotStamp, cloneResumed.GetActor().GetStatus().GetWorkerAssignment().GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Errorf("cloned actor assignment stamp mismatch (-tag +assignment):\n%s", diff)
	}
	waitForActorState(ctx, t, clients, cloneName, ateapi.ActorState_ACTOR_STATE_RUNNING)
	response, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: cloneName})
	if err != nil {
		t.Fatalf("failed to call cloned Actor: %v", err)
	}
	validateCounterResponse(t, response, "clone", 2, 2)

	if _, err := clients.SubstrateAPI.DeleteTag(ctx, &ateapi.DeleteTagRequest{Tag: tagRef}); err != nil {
		t.Fatalf("failed to delete Tag: %v", err)
	}
}

func TestDurableDirLifecycle(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		tc   actorLifecycleTestCase
	}{
		{
			name: "preferredFidelity:MEMORY",
			tc: actorLifecycleTestCase{
				fidelity:               ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
				wantMemoryAfterPause:   2,
				wantFileAfterPause:     2,
				wantRootfsAfterPause:   2,
				wantMemoryAfterSuspend: 3,
				wantFileAfterSuspend:   3,
				wantRootfsAfterSuspend: 3,
			},
		},
		{
			// Async suspend captures the same VOLUMES fidelity as sync suspend,
			// so memory and rootfs writes cold-boot on every resume.
			name: "preferredFidelity:VOLUMES",
			tc: actorLifecycleTestCase{
				fidelity:               ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
				wantMemoryAfterPause:   1,
				wantFileAfterPause:     2,
				wantRootfsAfterPause:   1,
				wantMemoryAfterSuspend: 1,
				wantFileAfterSuspend:   3,
				wantRootfsAfterSuspend: 1,
			},
		},
		{
			// ROOTFS keeps the rootfs writes across a cold boot: the memory
			// counter restarts while the rootfs counter continues.
			name: "preferredFidelity:ROOTFS",
			tc: actorLifecycleTestCase{
				fidelity:               ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
				wantMemoryAfterPause:   1,
				wantFileAfterPause:     2,
				wantRootfsAfterPause:   2,
				wantMemoryAfterSuspend: 1,
				wantFileAfterSuspend:   3,
				wantRootfsAfterSuspend: 3,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			runActorLifecycleTestCase(t, "durabledir-lifecycle", createActorTemplate, test.tc)
		})
	}
}

// TestMultipleDurableDirLifecycle covers an Actor with TWO durable-dir volumes:
// both must survive async suspend/resume and sync suspend/resume independently.
func TestMultipleDurableDirLifecycle(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		tc   actorLifecycleTestCase
	}{
		{
			name: "preferredFidelity:MEMORY",
			tc: actorLifecycleTestCase{
				fidelity:               ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
				wantMemoryAfterPause:   2,
				wantFileAfterPause:     2,
				wantMemoryAfterSuspend: 3,
				wantFileAfterSuspend:   3,
				checkSecondFileCounter: true,
			},
		},
		{
			name: "preferredFidelity:VOLUMES",
			tc: actorLifecycleTestCase{
				fidelity:               ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
				wantMemoryAfterPause:   1,
				wantFileAfterPause:     2,
				wantMemoryAfterSuspend: 1,
				wantFileAfterSuspend:   3,
				checkSecondFileCounter: true,
			},
		},
		{
			name: "preferredFidelity:ROOTFS",
			tc: actorLifecycleTestCase{
				fidelity:               ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
				wantMemoryAfterPause:   1,
				wantFileAfterPause:     2,
				wantRootfsAfterPause:   2,
				wantMemoryAfterSuspend: 1,
				wantFileAfterSuspend:   3,
				wantRootfsAfterSuspend: 3,
				checkSecondFileCounter: true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			runActorLifecycleTestCase(t, "multi-durabledir-lifecycle", createActorTemplateWithTwoDurableDirs, test.tc)
		})
	}
}

func TestExternalVolumeLifecycle(t *testing.T) {
	e2e.RequirePreview(t, preview.GateExternalVolumes)
	t.Parallel()

	tests := []struct {
		name string
		tc   actorLifecycleTestCase
	}{
		{
			name: "preferredFidelity:VOLUMES",
			tc: actorLifecycleTestCase{
				fidelity:               ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
				wantMemoryAfterPause:   1,
				wantFileAfterPause:     2,
				wantMemoryAfterSuspend: 1,
				wantFileAfterSuspend:   3,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			runActorLifecycleTestCase(t, "extvol-lifecycle", createActorTemplateWithExternalVolume, test.tc)
		})
	}
}

func TestDeleteRunningActorWithExternalVolume(t *testing.T) {
	e2e.RequirePreview(t, preview.GateExternalVolumes)
	t.Parallel()
	ctx := context.Background()
	clients := e2e.GetClients()
	nsObj := e2e.CreateNamespace(t)

	at, err := createActorTemplateWithExternalVolume(ctx, t, clients, nsObj, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES)
	if err != nil {
		t.Fatalf("failed to initialize ActorTemplate: %v", err)
	}

	actorName := "delete-running-extvol-" + nsObj.Name

	t.Logf("Creating Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	t.Logf("Resuming Actor %q...", actorName)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	// Verify volume exists in actor (it should be provisioned)
	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
	})
	if err != nil {
		t.Fatalf("failed to get actor: %v", err)
	}
	if len(actor.GetStatus().GetExternalVolumes().GetVolumes()) == 0 {
		t.Fatalf("expected actor to have volumes, got 0")
	}
	for _, vol := range actor.GetStatus().GetExternalVolumes().GetVolumes() {
		if vol.StorageVolumeId == "" {
			t.Fatalf("expected volume %q to be provisioned, got no storage volume ID", vol.Name)
		}
	}

	// Delete the running actor
	t.Logf("Deleting running Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapi.DeleteActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to delete Actor: %v", err)
	}

	// Verify deletion in store
	if _, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("expected actor %q to be NotFound after delete, got err: %v", actorName, err)
	}
}

func TestExternalVolume_NodeMigration(t *testing.T) {
	e2e.RequirePreview(t, preview.GateExternalVolumes)
	t.Parallel()
	if e2e.IsMicroVM() {
		t.Skip("Skipping TestExternalVolume_NodeMigration for microVM environment")
	}

	ctx := context.Background()
	clients := e2e.GetClients()
	nsObj := e2e.CreateNamespace(t)

	at, err := createActorTemplateWithExternalVolume(ctx, t, clients, nsObj, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES)
	if err != nil {
		t.Fatalf("failed to initialize ActorTemplate: %v", err)
	}

	actorName := "extvol-migration-" + nsObj.Name

	t.Logf("Creating Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	t.Logf("Resuming Actor %q...", actorName)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor on initial worker: %v", err)
	}
	validateCounterResponse(t, resp, "initial run", 1, 1)

	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
	})
	if err != nil {
		t.Fatalf("failed to get actor: %v", err)
	}
	initialWorkerPod := actor.GetStatus().GetWorkerAssignment().GetWorkerPod()
	initialWorkerNS := actor.GetStatus().GetWorkerAssignment().GetWorkerNamespace()

	t.Logf("Suspending Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor:   &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	}); err != nil {
		t.Fatalf("failed to suspend Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	// Delete initial worker pod to force migration onto another worker upon resume
	if initialWorkerPod != "" && initialWorkerNS != "" {
		t.Logf("Evicting initial worker pod %s/%s to force migration...", initialWorkerNS, initialWorkerPod)
		_ = clients.K8s.CoreV1().Pods(initialWorkerNS).Delete(ctx, initialWorkerPod, metav1.DeleteOptions{})
	}
	//	Wait for pod to be marked for deletion before resuming the actor to avoid race conditions
	time.Sleep(5 * time.Second)

	t.Logf("Resuming Actor %q after worker pod eviction...", actorName)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor after migration: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor after migration: %v", err)
	}
	validateCounterResponse(t, resp, "after migration", 1, 2)

	// Clean up by deleting the actor
	t.Logf("Deleting Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapi.DeleteActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to delete Actor: %v", err)
	}
}

// Verify that file and memory counters behavior after async and sync suspend, for different snapshot scopes.
// Test case:
//  1. Create actor.
//  2. Call to actor and validate memory and file counters.
//  3. Async Suspend & Resume actor.
//  4. Call to actor and validate memory and file counters.
//  5. Sync Suspend & Resume actor.
//  6. Call to actor and validate memory and file counters.
type actorLifecycleTestCase struct {
	fidelity               ateapi.SnapshotFidelity
	wantMemoryAfterPause   int
	wantFileAfterPause     int
	wantMemoryAfterSuspend int
	wantFileAfterSuspend   int

	// wantRootfsAfterPause and wantRootfsAfterSuspend assert the counter the
	// workload keeps in a file on its root filesystem: ROOTFS and MEMORY
	// snapshots carry it, VOLUMES resets it. Zero skips the check.
	wantRootfsAfterPause   int
	wantRootfsAfterSuspend int

	// checkSecondFileCounter also asserts the counter kept in a SECOND durable
	// volume. Both volumes are written on every request, so it must track the
	// first counter exactly — if one volume were dropped or restored into the
	// wrong place, they would diverge.
	checkSecondFileCounter bool

	// wantSnapshotFidelity, when set, asserts the content scope recorded
	// on the ActorSnapshot the suspend produced.
	wantSnapshotFidelity ateapi.SnapshotFidelity
}

// nodeDiskTimeout bounds the wait for an actor's directories on its node or
// background snapshot state reports to converge.
const nodeDiskTimeout = 30 * time.Second

func runActorLifecycleTestCase(t *testing.T, prefix string, createTemplate func(context.Context, *testing.T, *e2e.Clients, *e2e.Namespace, ateapi.SnapshotFidelity) (*ateapi.ActorTemplate, error), tc actorLifecycleTestCase) {
	// Create namespace
	nsObj := e2e.CreateNamespace(t)

	ctx := context.Background()
	clients := e2e.GetClients()

	// Create actor template.
	at, err := createTemplate(ctx, t, clients, nsObj, tc.fidelity)
	if err != nil {
		t.Fatalf("failed to initialize ActorTemplate: %v", err)
	}

	//
	// Create an Actor.
	//
	actorID := prefix + "-" + nsObj.Name

	t.Logf("Creating Actor %q using Substrate API...", actorID)
	createResp, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: actorID},
		ActorTemplate: e2e.TemplateRef(at),
	}})
	if err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	t.Logf("Successfully created Actor: %s", createResp.GetMetadata().GetName())
	defer func() {
		clients.SubstrateAPI.DeleteActor(ctx, &ateapi.DeleteActorRequest{
			Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorID},
		})
	}()

	// Resuming the actor
	t.Logf("Resuming Actor %q...", actorID)
	resumed1, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorID},
	})
	if err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorID, ateapi.ActorState_ACTOR_STATE_RUNNING)
	assignedRuntime := resumed1.GetActor().GetStatus().GetWorkerAssignment().GetSandboxRuntime()
	if assignedRuntime == nil {
		t.Fatalf("resumed Actor %q has nil WorkerAssignment.SandboxRuntime", actorID)
	}
	inspector := e2e.NewNodeInspector(t, ctx, nsObj.Name, resumed1.GetActor().GetStatus().GetAssignedNode())

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorID})
	if err != nil {
		t.Fatalf("failed to call actor: %v", err)
	}
	validateCounterResponse(t, resp, "after creation", 1, 1)
	if tc.checkSecondFileCounter {
		validateSecondFileCounter(t, resp, "after creation", 1)
	}
	if tc.wantRootfsAfterPause != 0 {
		validateRootfsCounter(t, resp, "after creation", 1)
	}

	//
	// Pausing the actor
	//
	t.Logf("Pausing Actor %q...", actorID)
	paused, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor:   &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorID},
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("failed to pause Actor: %v", err)
	}
	firstPause := getLocalSnapshotFromActor(t, paused.GetActor())
	if diff := cmp.Diff(assignedRuntime, firstPause.snap.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Errorf("local pause snapshot stamp mismatch (-assignment +local):\n%s", diff)
	}

	// Resuming the actor unschedules its background detach while keeping the
	// local checkpoint on disk and letting the background upload finish.
	t.Logf("Resuming Actor %q again...", actorID)
	resumed2, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorID},
	})
	if err != nil {
		t.Fatalf("failed to resume Actor again: %v", err)
	}
	waitForActorState(ctx, t, clients, actorID, ateapi.ActorState_ACTOR_STATE_RUNNING)
	assignedRuntime2 := resumed2.GetActor().GetStatus().GetWorkerAssignment().GetSandboxRuntime()
	if diff := cmp.Diff(assignedRuntime, assignedRuntime2, protocmp.Transform()); diff != "" {
		t.Errorf("resume-from-paused assignment stamp mismatch (-first +second):\n%s", diff)
	}
	inspector = inspectorOn(ctx, t, nsObj.Name, inspector, firstPause.node)
	inspector.WaitForLocalCheckpoints(t, firstPause.actorUID, []string{firstPause.uuid}, nodeDiskTimeout)
	inspector.WaitForSnapshotState(t, firstPause.actorUID, e2e.SnapshotState{
		LatestLocalUUID:   firstPause.uuid,
		LatestDurableUUID: firstPause.uuid,
	}, nodeDiskTimeout)
	waitForSnapshotUploaded(ctx, t, clients, actorID, firstPause.uuid)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorID})
	if err != nil {
		t.Fatalf("failed to call actor again: %v", err)
	}
	validateCounterResponse(t, resp, "after pause", tc.wantMemoryAfterPause, tc.wantFileAfterPause)
	if tc.checkSecondFileCounter {
		validateSecondFileCounter(t, resp, "after pause", tc.wantFileAfterPause)
	}
	if tc.wantRootfsAfterPause != 0 {
		validateRootfsCounter(t, resp, "after pause", tc.wantRootfsAfterPause)
	}

	//
	// Suspending the actor
	//
	t.Logf("Suspending Actor %q...", actorID)
	suspended, err := e2e.RetryAborted(t, ctx, clients.SubstrateAPI.SuspendActor, &ateapi.SuspendActorRequest{
		Actor:   &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorID},
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("failed to suspend Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorID, ateapi.ActorState_ACTOR_STATE_SUSPENDED)
	durableSnap := durableSnapshot(suspended.GetActor().GetStatus())
	if diff := cmp.Diff(assignedRuntime2, durableSnap.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Errorf("durable snapshot stamp after suspend mismatch (-assignment +snapshot):\n%s", diff)
	}

	if tc.wantSnapshotFidelity != ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED {
		validateSnapshotFidelity(ctx, t, clients, actorID, tc.wantSnapshotFidelity)
	}

	// Once the detach delay elapses, atelet deletes the actor's local
	// checkpoint, snapshot-state.json, and actor directory on the node, and
	// reports the detach so the API unpins the actor.
	waitForActorDetached(ctx, t, clients, actorID, durableSnap.GetUuid())
	inspectorOn(ctx, t, nsObj.Name, inspector, resumed2.GetActor().GetStatus().GetAssignedNode()).WaitForActorDirRemoved(t, firstPause.actorUID, nodeDiskTimeout)

	// Resuming the actor after detach restores from the durable snapshot in
	// object storage.
	t.Logf("Resuming Actor %q again...", actorID)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorID},
	}); err != nil {
		t.Fatalf("failed to resume Actor again: %v", err)
	}
	waitForActorState(ctx, t, clients, actorID, ateapi.ActorState_ACTOR_STATE_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorID})
	if err != nil {
		t.Fatalf("failed to call actor again: %v", err)
	}
	validateCounterResponse(t, resp, "after suspend", tc.wantMemoryAfterSuspend, tc.wantFileAfterSuspend)
	if tc.checkSecondFileCounter {
		validateSecondFileCounter(t, resp, "after suspend", tc.wantFileAfterSuspend)
	}
	if tc.wantRootfsAfterSuspend != 0 {
		validateRootfsCounter(t, resp, "after suspend", tc.wantRootfsAfterSuspend)
	}

	// Deleting the running actor terminates it on its node, which reclaims the
	// actor's directory there.
	running := getActorOnNode(ctx, t, clients, actorID)
	t.Logf("Deleting Actor %q...", actorID)
	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapi.DeleteActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorID},
	}); err != nil {
		t.Fatalf("failed to delete Actor: %v", err)
	}
	inspectorOn(ctx, t, nsObj.Name, inspector, running.node).WaitForActorDirRemoved(t, running.actorUID, nodeDiskTimeout)
}

// validateSnapshotFidelity asserts the content scope recorded on the
// suspended actor's external snapshot.
func validateSnapshotFidelity(ctx context.Context, t *testing.T, clients *e2e.Clients, actorID string, want ateapi.SnapshotFidelity) {
	t.Helper()
	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorID},
	})
	if err != nil {
		t.Fatalf("failed to get suspended Actor: %v", err)
	}
	if durableSnapshotURI(actor.GetStatus()) == "" {
		t.Fatal("suspended Actor has no external snapshot")
	}
	if got := durableSnapshot(actor.GetStatus()).GetFidelity(); got != want {
		t.Errorf("snapshot %q content scope = %v, want %v", durableSnapshotURI(actor.GetStatus()), got, want)
	}
}

func durableSnapshot(status *ateapi.ActorStatus) *ateapi.Snapshot {
	var best *ateapi.Snapshot
	for _, snap := range status.GetSnapshots() {
		st := snap.GetDurableSnapshot()
		if st.GetStatus() == ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED &&
			st.GetObject().GetSnapshotUri() != "" {
			if best == nil || snap.GetGeneration() > best.GetGeneration() {
				best = snap
			}
		}
	}
	return best
}

func durableSnapshotStorage(status *ateapi.ActorStatus) *ateapi.SnapshotStorage {
	return durableSnapshot(status).GetDurableSnapshot()
}

func durableSnapshotURI(status *ateapi.ActorStatus) string {
	return durableSnapshotStorage(status).GetObject().GetSnapshotUri()
}

func localSnapshot(status *ateapi.ActorStatus) *ateapi.Snapshot {
	var best *ateapi.Snapshot
	for _, snap := range status.GetSnapshots() {
		if snap.GetLocality() != "" && snap.GetUuid() != "" {
			if best == nil || snap.GetGeneration() > best.GetGeneration() {
				best = snap
			}
		}
	}
	return best
}

func hasLocalSnapshot(status *ateapi.ActorStatus) bool {
	return localSnapshot(status) != nil
}

// localSnapshotRecord is where the API says an asynchronously suspended
// actor's local snapshot lives: the directory name atelet stores it under, and
// the node.
type localSnapshotRecord struct {
	actorUID string
	uuid     string
	node     string
	snap     *ateapi.Snapshot
}

// getLocalSnapshotFromActor extracts the local snapshot record from a
// suspended Actor proto, failing the test unless the actor is SUSPENDED on a
// node with exactly one local snapshot entry.
func getLocalSnapshotFromActor(t *testing.T, actor *ateapi.Actor) localSnapshotRecord {
	t.Helper()
	actorName := actor.GetMetadata().GetName()
	if got := actor.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("Actor %q state = %v, want SUSPENDED", actorName, got)
	}
	if actor.GetStatus().GetAssignedNode() == "" {
		t.Fatalf("Actor %q is SUSPENDED with a local snapshot but not pinned to a node: %v", actorName, actor.GetStatus())
	}
	var local []*ateapi.Snapshot
	for _, snap := range actor.GetStatus().GetSnapshots() {
		if snap.GetLocality() != "" {
			local = append(local, snap)
		}
	}
	if len(local) != 1 || local[0].GetUuid() == "" {
		t.Fatalf("Actor %q local snapshots = %v, want exactly one with a uuid", actorName, local)
	}
	if got, want := local[0].GetLocality(), actor.GetStatus().GetAssignedNode(); got != want {
		t.Fatalf("Actor %q local snapshot locality = %q, want assigned node %q", actorName, got, want)
	}
	return localSnapshotRecord{
		actorUID: actor.GetMetadata().GetUid(),
		uuid:     local[0].GetUuid(),
		node:     local[0].GetLocality(),
		snap:     local[0],
	}
}

// waitForSnapshotUploaded polls GetActor until wantUUID is reported as the
// actor's COMPLETED durable snapshot.
func waitForSnapshotUploaded(ctx context.Context, t *testing.T, clients *e2e.Clients, actorName, wantUUID string) *ateapi.Snapshot {
	t.Helper()
	deadline := time.Now().Add(nodeDiskTimeout)
	var last *ateapi.ActorStatus
	for time.Now().Before(deadline) {
		actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{
			Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
		})
		if err == nil {
			last = actor.GetStatus()
			if snap := durableSnapshot(last); snap != nil && snap.GetUuid() == wantUUID {
				return snap
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for actor %q snapshot %q to finish uploading; last status: %v", actorName, wantUUID, last)
	return nil
}

// waitForActorDetached polls GetActor until wantUUID is durable and background
// detach has cleared the actor's local snapshot and node assignment.
func waitForActorDetached(ctx context.Context, t *testing.T, clients *e2e.Clients, actorName, wantUUID string) *ateapi.Actor {
	t.Helper()
	deadline := time.Now().Add(nodeDiskTimeout)
	var last *ateapi.Actor
	for time.Now().Before(deadline) {
		actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{
			Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
		})
		if err == nil {
			last = actor
			st := actor.GetStatus()
			if durableSnapshot(st).GetUuid() == wantUUID && !hasLocalSnapshot(st) && st.GetAssignedNode() == "" {
				return actor
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for actor %q snapshot %q to detach from node; last status: %v", actorName, wantUUID, last.GetStatus())
	return nil
}

// actorOnNode is where an actor's directory lives: its UID, which names the
// directory, and its node.
type actorOnNode struct {
	actorUID string
	node     string
}

// getActorOnNode reads the actor's UID and assigned node from the API, failing
// the test if it has no node.
func getActorOnNode(ctx context.Context, t *testing.T, clients *e2e.Clients, actorName string) actorOnNode {
	t.Helper()
	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
	})
	if err != nil {
		t.Fatalf("failed to get Actor %q: %v", actorName, err)
	}
	node := actor.GetStatus().GetAssignedNode()
	if node == "" {
		t.Fatalf("Actor %q (state %v) has no assigned node", actorName, actor.GetStatus().GetState())
	}
	return actorOnNode{actorUID: actor.GetMetadata().GetUid(), node: node}
}

// inspectorOn returns inspector if it is already on node, and otherwise starts
// a new one there.
func inspectorOn(ctx context.Context, t *testing.T, namespace string, inspector *e2e.NodeInspector, node string) *e2e.NodeInspector {
	t.Helper()
	if inspector != nil && inspector.Node == node {
		return inspector
	}
	return e2e.NewNodeInspector(t, ctx, namespace, node)
}

func goldenSnapshotRuntime(t *testing.T, ctx context.Context, clients *e2e.Clients, at *ateapi.ActorTemplate) *ateapi.ActorSandboxRuntime {
	t.Helper()
	current, err := clients.SubstrateAPI.GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{ActorTemplate: e2e.TemplateRef(at)})
	if err != nil {
		t.Fatalf("GetActorTemplate: %v", err)
	}
	goldenTag := current.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag()
	if goldenTag.GetName() == "" {
		t.Fatalf("ActorTemplate %s has no golden tag", e2e.TemplateRef(at))
	}
	tag, err := clients.SubstrateAPI.GetTag(ctx, &ateapi.GetTagRequest{Tag: goldenTag})
	if err != nil {
		t.Fatalf("GetTag(golden %s/%s): %v", goldenTag.GetAtespace(), goldenTag.GetName(), err)
	}
	stamp := tag.GetStatus().GetSnapshot().GetSandboxRuntime()
	if stamp == nil {
		t.Fatalf("golden snapshot %v carries no sandbox_runtime", tag.GetStatus().GetSnapshot())
	}
	return stamp
}

func runtimeAttribute(rt *ateapi.ActorSandboxRuntime, key string) string {
	for _, attr := range rt.GetVersion().GetAttributes() {
		if attr.GetKey() == key {
			return attr.GetValue()
		}
	}
	return ""
}

// wantSnapshotLocation is where ate-api-server stores tmpl's snapshots: the
// template's storageLocation with the PerTemplateSnapshotLocation preview gate,
// otherwise $SNAPSHOT_LOCATION, the --snapshot-location ate-setup installs it
// with.
func wantSnapshotLocation(t *testing.T, tmpl *ateapi.ActorTemplate) string {
	t.Helper()
	if location := tmpl.GetSnapshotConfig().GetStorageLocation(); location != "" && preview.IsEnabled(preview.GatePerTemplateSnapshotLocation) {
		return location
	}
	env, err := e2e.CheckEnv("SNAPSHOT_LOCATION")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	return env["SNAPSHOT_LOCATION"]
}

// assertSnapshotUnderLocation fails t unless snapshotURI is stored under
// location. It compares owner prefixes, so location may be spelled with or
// without a trailing slash.
func assertSnapshotUnderLocation(t *testing.T, snapshotURI, location string) {
	t.Helper()
	uri, err := resources.ParseSnapshotURI(snapshotURI)
	if err != nil {
		t.Fatalf("ParseSnapshotURI(%q): %v", snapshotURI, err)
	}
	want, err := uri.Owner().Prefix(location)
	if err != nil {
		t.Fatalf("snapshot owner prefix under %q: %v", location, err)
	}
	if got := uri.OwnerPrefix(); got != want {
		t.Errorf("snapshot %q is stored under %q, want %q (PerTemplateSnapshotLocation preview gate enabled: %t)",
			snapshotURI, got, want, preview.IsEnabled(preview.GatePerTemplateSnapshotLocation))
	}
}

func tagSnapshotURI(tag *ateapi.Tag) string {
	return tag.GetStatus().GetSnapshot().GetDurableSnapshot().GetObject().GetSnapshotUri()
}

// validateSecondFileCounter checks the counter the workload keeps in its second
// durable-dir volume (see createActorTemplateWithTwoDurableDirs).
func validateSecondFileCounter(t *testing.T, resp string, stage string, want int) {
	t.Helper()
	const prefix = "preserved second file counter: "
	if !strings.Contains(resp, prefix+fmt.Sprintf("%d", want)) {
		t.Errorf("[%s] expected second file count %d, got response: %s", stage, want, resp)
	}
}

// validateRootfsCounter asserts the counter the workload keeps in a file on
// its root filesystem, the one layer that tells ROOTFS apart from VOLUMES.
func validateRootfsCounter(t *testing.T, resp string, stage string, want int) {
	t.Helper()
	if !strings.Contains(resp, fmt.Sprintf("preserved rootfs counter: %d", want)) {
		t.Errorf("[%s] expected rootfs counter %d, got response: %s", stage, want, resp)
	}
}

func validateCounterResponse(t *testing.T, resp string, stage string, wantMemory, wantFile int) {
	memoryCounterPrefix := "preserved memory count: "
	fileCounterPrefix := "preserved file counter: "

	if !strings.Contains(resp, memoryCounterPrefix+fmt.Sprintf("%d", wantMemory)) {
		t.Errorf("[%s] expected memory count %d, got response: %s", stage, wantMemory, resp)
	}
	if !strings.Contains(resp, fileCounterPrefix+fmt.Sprintf("%d", wantFile)) {
		t.Errorf("[%s] expected file count %d, got response: %s", stage, wantFile, resp)
	}
}

// validateCountersAtLeast checks that the memory and file counters in resp are
// both at least want.
func validateCountersAtLeast(t *testing.T, resp, stage string, want int) {
	t.Helper()
	for _, prefix := range []string{"preserved memory count: ", "preserved file counter: "} {
		_, rest, _ := strings.Cut(resp, prefix)
		var got int
		if _, err := fmt.Sscanf(rest, "%d", &got); err != nil || got < want {
			t.Errorf("[%s] expected %sat least %d, got response: %s", stage, prefix, want, resp)
		}
	}
}

func createActor(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, at *ateapi.ActorTemplate) error {
	// Create an Actor using the ATE API.
	actorName := "demo-actor-1-" + nsObj.Name

	t.Logf("Creating Actor %q using Substrate API...", actorName)
	createResp, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}})
	if err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	t.Logf("Successfully created Actor: %s", createResp.GetMetadata().GetName())
	defer func() {
		clients.SubstrateAPI.DeleteActor(ctx, &ateapi.DeleteActorRequest{
			Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
		})
	}()

	listResp, err := clients.SubstrateAPI.ListActors(ctx, &ateapi.ListActorsRequest{Atespace: demoAtespace})
	if err != nil {
		t.Fatalf("ListActors RPC failed: %v", err)
	}

	var myActors []*ateapi.Actor
	for _, actor := range listResp.GetActors() {
		if actor.GetActorTemplate().GetName() == at.GetMetadata().GetName() && actor.GetMetadata().GetName() == actorName {
			myActors = append(myActors, actor)
		}
	}

	// Check that we have our Actor created.
	if len(myActors) != 1 {
		t.Fatalf("expected actor %s from template %s, got %d actors: %v", actorName, at.GetMetadata().GetName(), len(myActors), myActors)
	}

	actor := myActors[0]
	if actor.GetMetadata().GetName() != actorName {
		t.Errorf("expected actor name %s, got %s", actorName, actor.GetMetadata().GetName())
	}
	if actor.GetActorTemplate().GetName() != at.GetMetadata().GetName() {
		t.Errorf("expected actor template name %s, got %s", at.GetMetadata().GetName(), actor.GetActorTemplate().GetName())
	}
	if actor.Status.State != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("expected actor state to be SUSPENDED, got %v", actor.Status.State)
	}

	t.Logf("Successfully queried Substrate API. Found %d active actors total, %d from our template %s.",
		len(listResp.GetActors()), len(myActors), at.GetMetadata().GetName())

	return nil
}

func pauseActor(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, at *ateapi.ActorTemplate) error {
	actorName := "pause-actor-" + nsObj.Name
	actorRef := &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapi.DeleteActorRequest{Actor: actorRef})
	})

	// Creating an actor
	t.Logf("Creating Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	// Resuming the actor
	t.Logf("Resuming Actor %q...", actorName)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{
		Actor: actorRef,
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)
	inspector := e2e.NewNodeInspector(t, ctx, nsObj.Name, getActorOnNode(ctx, t, clients, actorName).node)

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor: %v", err)
	}
	validateCounterResponse(t, resp, "after creation", 1, 1)

	// First async suspend: returns immediately with a node-local snapshot whose
	// durable upload is still PENDING. Resuming right away restores from the
	// local checkpoint on the node and unschedules background detach while the
	// background upload finishes.
	t.Logf("Pausing Actor %q (first async suspend)...", actorName)
	paused1, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor:   actorRef,
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("failed to pause Actor: %v", err)
	}
	pause1 := getLocalSnapshotFromActor(t, paused1.GetActor())
	if got := pause1.snap.GetDurableSnapshot().GetStatus(); got != ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING {
		t.Errorf("first async suspend DurableSnapshot.Status = %v, want PENDING", got)
	}
	if pause1.snap.GetDurableSnapshot().GetObject().GetSnapshotUri() == "" {
		t.Error("first async suspend DurableSnapshot.Object.SnapshotUri is empty, want set")
	}

	t.Logf("Resuming Actor %q from first async suspend...", actorName)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{
		Actor: actorRef,
	}); err != nil {
		t.Fatalf("failed to resume Actor after first pause: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	inspector = inspectorOn(ctx, t, nsObj.Name, inspector, pause1.node)
	inspector.WaitForLocalCheckpoints(t, pause1.actorUID, []string{pause1.uuid}, nodeDiskTimeout)
	inspector.WaitForSnapshotState(t, pause1.actorUID, e2e.SnapshotState{
		LatestLocalUUID:   pause1.uuid,
		LatestDurableUUID: pause1.uuid,
	}, nodeDiskTimeout)
	waitForSnapshotUploaded(ctx, t, clients, actorName, pause1.uuid)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor after first pause: %v", err)
	}
	validateCounterResponse(t, resp, "after first pause", 2, 2)

	// Second async suspend: supersedes pause1 on the same node and prunes
	// pause1's local checkpoint directory from disk.
	t.Logf("Pausing Actor %q (second async suspend)...", actorName)
	paused2, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor:   actorRef,
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("failed to pause Actor a second time: %v", err)
	}
	pause2 := getLocalSnapshotFromActor(t, paused2.GetActor())
	if pause2.uuid == pause1.uuid {
		t.Errorf("second async suspend reused local snapshot %q, want a new one", pause2.uuid)
	}
	if pause2.node != pause1.node {
		t.Fatalf("second async suspend stored its local snapshot on node %q, want %q where the actor resumed from", pause2.node, pause1.node)
	}

	t.Logf("Resuming Actor %q from second async suspend...", actorName)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{
		Actor: actorRef,
	}); err != nil {
		t.Fatalf("failed to resume Actor after second pause: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)
	inspector.WaitForLocalCheckpoints(t, pause2.actorUID, []string{pause2.uuid}, nodeDiskTimeout)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor after second pause: %v", err)
	}
	validateCounterResponse(t, resp, "after second pause", 3, 3)

	// Third async suspend: leave the actor SUSPENDED so atelet uploads the
	// snapshot in the background and then detaches the actor from the node,
	// removing its local checkpoint, snapshot-state.json, and actor directory.
	t.Logf("Pausing Actor %q (third async suspend, waiting for upload and detach)...", actorName)
	paused3, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor:   actorRef,
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("failed to pause Actor a third time: %v", err)
	}
	pause3 := getLocalSnapshotFromActor(t, paused3.GetActor())
	uploaded3 := waitForSnapshotUploaded(ctx, t, clients, actorName, pause3.uuid)
	if uploaded3.GetDurableSnapshot().GetObject().GetSnapshotUri() == "" {
		t.Fatal("uploaded async snapshot has no URI")
	}
	waitForActorDetached(ctx, t, clients, actorName, pause3.uuid)
	inspectorOn(ctx, t, nsObj.Name, inspector, pause3.node).WaitForActorDirRemoved(t, pause3.actorUID, nodeDiskTimeout)

	// Resuming after detach restores from the background-uploaded durable
	// snapshot in object storage.
	t.Logf("Resuming Actor %q after background upload and detach...", actorName)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{
		Actor: actorRef,
	}); err != nil {
		t.Fatalf("failed to resume Actor after detach: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor after detach: %v", err)
	}
	validateCounterResponse(t, resp, "after async upload and detach", 4, 4)

	// Deleting the actor
	running := getActorOnNode(ctx, t, clients, actorName)
	t.Logf("Deleting Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapi.DeleteActorRequest{
		Actor: actorRef,
	}); err != nil {
		t.Fatalf("failed to delete Actor: %v", err)
	}
	if _, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{
		Actor: actorRef,
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("expected actor %q to be NotFound after delete, got err: %v", actorName, err)
	}
	inspectorOn(ctx, t, nsObj.Name, inspector, running.node).WaitForActorDirRemoved(t, running.actorUID, nodeDiskTimeout)

	return nil
}

func suspendActor(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, at *ateapi.ActorTemplate) error {
	actorName := "suspend-actor-" + nsObj.Name

	// Creating an actor
	t.Logf("Creating Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	// Resuming the actor
	t.Logf("Resuming Actor %q...", actorName)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor: %v", err)
	}
	validateCounterResponse(t, resp, "after creation", 1, 1)

	// Suspending the actor
	t.Logf("Suspending Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor:   &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	}); err != nil {
		t.Fatalf("failed to suspend Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	// Resuming the actor again
	t.Logf("Resuming Actor %q again...", actorName)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor again: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor again: %v", err)
	}
	validateCounterResponse(t, resp, "after suspend", 2, 2)

	// Suspending the actor before deletion
	t.Logf("Suspending Actor %q before deletion...", actorName)
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor:   &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	}); err != nil {
		t.Fatalf("failed to suspend Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	// Deleting the actor
	t.Logf("Deleting Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapi.DeleteActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to delete Actor: %v", err)
	}
	// Verify deletion
	if _, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err == nil {
		t.Fatalf("expected actor %q to be deleted, but it still exists", actorName)
	}

	return nil
}

func revertActor(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, at *ateapi.ActorTemplate) error {
	actorName := "revert-actor-" + nsObj.Name
	actorRef := &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapi.DeleteActorRequest{Actor: actorRef})
	})

	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor: %v", err)
	}
	validateCounterResponse(t, resp, "before snapshot", 1, 1)

	suspended, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: actorRef, Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS}})
	if err != nil {
		t.Fatalf("failed to suspend Actor: %v", err)
	}
	snapshotURI := durableSnapshotURI(suspended.GetActor().GetStatus())
	if snapshotURI == "" {
		t.Fatal("suspend wrote no external snapshot")
	}

	// Mutate guest state after the snapshot, then revert from RUNNING: the
	// increment to 2 must be discarded and the snapshot left untouched.
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("failed to resume Actor after suspend: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor before revert: %v", err)
	}
	validateCounterResponse(t, resp, "before revert from RUNNING", 2, 2)

	beforeRevert := getActorOnNode(ctx, t, clients, actorName)
	reverted, err := clients.SubstrateAPI.RevertActor(ctx, &ateapi.RevertActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("RevertActor from RUNNING: %v", err)
	}
	if got := reverted.GetActor().GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("state after revert = %v, want SUSPENDED", got)
	}
	if got := durableSnapshotURI(reverted.GetActor().GetStatus()); got != snapshotURI {
		t.Fatalf("snapshot URI after revert = %q, want %q", got, snapshotURI)
	}
	if reverted.GetActor().GetStatus().GetWorkerAssignment() != nil {
		t.Fatal("worker assignment survived revert")
	}
	// The revert terminates the running actor on its node, which reclaims the
	// actor's directory there.
	inspector := e2e.NewNodeInspector(t, ctx, nsObj.Name, beforeRevert.node)
	inspector.WaitForActorDirRemoved(t, beforeRevert.actorUID, nodeDiskTimeout)

	// Resuming proves the guest restored from the count=1 snapshot rather than
	// continuing from count=2: this call increments from 1 to 2, not from 2 to 3.
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("failed to resume Actor after revert: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor after revert: %v", err)
	}
	validateCounterResponse(t, resp, "after revert from RUNNING", 2, 2)

	// Revert from async suspend (local snapshot). The revert drops the local
	// snapshot and keeps the latest durable one: the count=1 snapshot, or this
	// pause's count=2 snapshot if atelet has already reported uploading it.
	paused, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: actorRef, Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS}})
	if err != nil {
		t.Fatalf("failed to pause Actor: %v", err)
	}
	asyncSnap := getLocalSnapshotFromActor(t, paused.GetActor())
	pausedURI := localSnapshot(paused.GetActor().GetStatus()).GetDurableSnapshot().GetObject().GetSnapshotUri()
	inspector = inspectorOn(ctx, t, nsObj.Name, inspector, asyncSnap.node)
	inspector.WaitForLocalCheckpoints(t, asyncSnap.actorUID, []string{asyncSnap.uuid}, nodeDiskTimeout)

	reverted, err = e2e.RetryAborted(t, ctx, clients.SubstrateAPI.RevertActor, &ateapi.RevertActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("RevertActor from paused: %v", err)
	}
	if got := reverted.GetActor().GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("state after revert from paused = %v, want SUSPENDED", got)
	}
	if hasLocalSnapshot(reverted.GetActor().GetStatus()) {
		t.Fatal("local snapshot pointer survived revert from paused")
	}
	if got := durableSnapshotURI(reverted.GetActor().GetStatus()); got != snapshotURI && got != pausedURI {
		t.Fatalf("snapshot URI after revert from paused = %q, want %q or the pause's %q", got, snapshotURI, pausedURI)
	}
	// The revert terminates the actor on its node, reclaiming its directory and
	// local checkpoint there.
	inspector.WaitForActorDirRemoved(t, asyncSnap.actorUID, nodeDiskTimeout)

	// SUSPENDED is rejected with FailedPrecondition rather than no-oping.
	if _, err := clients.SubstrateAPI.RevertActor(ctx, &ateapi.RevertActorRequest{Actor: actorRef}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RevertActor from SUSPENDED: got %v, want FailedPrecondition", err)
	}

	// Final resume restores the snapshot the revert kept, so this call takes
	// the counters to 2 from the count=1 snapshot, or to 3 from the pause's.
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("failed to resume Actor after paused revert: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor after paused revert: %v", err)
	}
	validateCountersAtLeast(t, resp, "after revert from paused", 2)

	beforeDelete := getActorOnNode(ctx, t, clients, actorName)
	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapi.DeleteActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("failed to delete Actor: %v", err)
	}
	inspectorOn(ctx, t, nsObj.Name, inspector, beforeDelete.node).WaitForActorDirRemoved(t, beforeDelete.actorUID, nodeDiskTimeout)
	return nil
}

func deleteRunningActor(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, at *ateapi.ActorTemplate) error {
	actorName := "delete-running-actor-" + nsObj.Name

	// 1. Creating and resuming the actor
	t.Logf("Creating Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	t.Logf("Resuming Actor %q...", actorName)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	// 2. Call the actor to ensure workload is actively serving on worker
	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor: %v", err)
	}
	validateCounterResponse(t, resp, "after creation", 1, 1)

	// 3. Delete the running actor
	running := getActorOnNode(ctx, t, clients, actorName)
	t.Logf("Deleting running Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapi.DeleteActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to delete Actor: %v", err)
	}

	// 4. Verify deletion in store and on node
	if _, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("expected actor %q to be NotFound after delete, got err: %v", actorName, err)
	}
	e2e.NewNodeInspector(t, ctx, nsObj.Name, running.node).WaitForActorDirRemoved(t, running.actorUID, nodeDiskTimeout)

	// 5. Verify actor name can be immediately reused
	t.Logf("Re-creating Actor %q to verify name reuse...", actorName)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to recreate Actor: %v", err)
	}
	defer func() {
		clients.SubstrateAPI.DeleteActor(ctx, &ateapi.DeleteActorRequest{
			Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
		})
	}()
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	return nil
}

func deletePausedActor(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, at *ateapi.ActorTemplate) error {
	actorName := "delete-paused-actor-" + nsObj.Name

	// 1. Creating an actor
	t.Logf("Creating Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	// 2. Resuming the actor
	t.Logf("Resuming Actor %q...", actorName)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)
	inspector := e2e.NewNodeInspector(t, ctx, nsObj.Name, getActorOnNode(ctx, t, clients, actorName).node)

	// 3. Suspending the actor asynchronously
	t.Logf("Pausing Actor %q...", actorName)
	pausedResp, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor:   &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("failed to pause Actor: %v", err)
	}
	paused := getLocalSnapshotFromActor(t, pausedResp.GetActor())
	inspector = inspectorOn(ctx, t, nsObj.Name, inspector, paused.node)
	inspector.WaitForLocalCheckpoints(t, paused.actorUID, []string{paused.uuid}, nodeDiskTimeout)

	// 4. Delete the asynchronously suspended actor
	t.Logf("Deleting paused Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapi.DeleteActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to delete Actor: %v", err)
	}

	// 5. Verify deletion in store
	if _, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("expected actor %q to be NotFound after delete, got err: %v", actorName, err)
	}

	// 6. A paused actor has no worker, so the delete terminates it on the node
	// holding its local snapshot, reclaiming the actor's directory there.
	inspector.WaitForActorDirRemoved(t, paused.actorUID, nodeDiskTimeout)

	return nil
}

func createActorTemplateInternal(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, base string, fidelity ateapi.SnapshotFidelity, modifyTemplate func(*ateapi.ActorTemplate)) (*ateapi.ActorTemplate, error) {
	env, err := e2e.CheckEnv("BUCKET_NAME")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}

	// The whole suite shares demoAtespace, so the per-test suffix keeps
	// template names unique.
	name := base + "-" + nsObj.Name
	at := e2e.CreateSubstrateCounterTemplate(ctx, t, clients, nsObj.Name, e2e.SubstrateTemplateOptions{
		Atespace: demoAtespace,
		Name:     name,
		PoolName: base,
		// One actor at a time, plus a spare for the worker-deletion tests.
		PoolReplicas: 2,
		Labels:       map[string]string{"demo": nsObj.Name},
		SnapshotConfig: &ateapi.SnapshotConfig{
			StorageLocation:   "gs://" + env["BUCKET_NAME"] + "/ate-demo-" + name,
			PreferredFidelity: fidelity,
		},
		Modify: modifyTemplate,
	})
	return at, nil
}

func createActorTemplate(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, fidelity ateapi.SnapshotFidelity) (*ateapi.ActorTemplate, error) {
	return createActorTemplateInternal(ctx, t, clients, nsObj, "counter", fidelity, nil)
}

func createActorTemplateWithExternalVolume(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, fidelity ateapi.SnapshotFidelity) (*ateapi.ActorTemplate, error) {
	scName := e2e.StorageClass
	if !hasStorageClass(ctx, clients, scName) {
		t.Fatalf("StorageClass %q not found in cluster; provide a valid --storage-class or install the CSI driver via --setup-csi", scName)
	}

	modify := func(at *ateapi.ActorTemplate) {
		for _, c := range at.GetContainers() {
			if c.GetName() != "counter" {
				continue
			}
			c.Command = []string{"/ko-app/counter", "--file-counter-directory=/external-data"}

			hasExtMount := false
			for _, vm := range c.GetVolumeMounts() {
				if vm.GetName() == "external-data" {
					hasExtMount = true
					break
				}
			}
			if !hasExtMount {
				c.VolumeMounts = append(c.VolumeMounts, &ateapi.VolumeMount{
					Name:      "external-data",
					MountPath: "/external-data",
				})
			}
		}

		hasExtVol := false
		for _, v := range at.GetVolumes() {
			if v.GetName() == "external-data" {
				hasExtVol = true
				break
			}
		}
		if !hasExtVol {
			at.Volumes = append(at.Volumes, &ateapi.Volume{
				Name: "external-data",
				External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{
					CapacityBytes:    1 << 30,
					StorageClassName: scName,
				}},
			})
		}
	}
	return createActorTemplateInternal(ctx, t, clients, nsObj, "counter-ext-vol", fidelity, modify)
}

// secondDurableDirVolume is the extra durable-dir volume (and where the counter
// container mounts it) added by createActorTemplateWithTwoDurableDirs.
const (
	secondDurableDirVolume    = "data2"
	secondDurableDirMountPath = "/home/counter2"
)

// createActorTemplateWithTwoDurableDirs builds a template with a SECOND
// durable-dir volume alongside the one the demo already declares, and points the
// counter's second file counter at it, so both volumes are written on every
// request.
func createActorTemplateWithTwoDurableDirs(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, fidelity ateapi.SnapshotFidelity) (*ateapi.ActorTemplate, error) {
	modify := func(at *ateapi.ActorTemplate) {
		for _, c := range at.GetContainers() {
			if c.GetName() != "counter" {
				continue
			}
			c.Command = []string{"/ko-app/counter", "--second-file-counter-directory=" + secondDurableDirMountPath}
			c.VolumeMounts = append(c.VolumeMounts, &ateapi.VolumeMount{
				Name:      secondDurableDirVolume,
				MountPath: secondDurableDirMountPath,
			})
		}
		at.Volumes = append(at.Volumes, &ateapi.Volume{
			Name:       secondDurableDirVolume,
			DurableDir: &ateapi.DurableDirVolumeSource{},
		})
	}
	return createActorTemplateInternal(ctx, t, clients, nsObj, "counter-two-durabledirs", fidelity, modify)
}

func hasStorageClass(ctx context.Context, clients *e2e.Clients, name string) bool {
	_, err := clients.K8s.StorageV1().StorageClasses().Get(ctx, name, metav1.GetOptions{})
	return err == nil
}

func waitForActorState(ctx context.Context, t *testing.T, clients *e2e.Clients, actorName string, expectedState ateapi.ActorState) {
	waitForActorStateWithTimeout(ctx, t, clients, actorName, expectedState, 60*time.Second)
}

func waitForActorStateWithTimeout(ctx context.Context, t *testing.T, clients *e2e.Clients, actorName string, expectedState ateapi.ActorState, timeout time.Duration) {
	t.Helper()
	t.Logf("Waiting for Actor %q to be %v...", actorName, expectedState)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{
			Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
		})
		if err == nil {
			if resp.GetStatus().GetState() == expectedState {
				t.Logf("Actor %q reached state %v", actorName, expectedState)
				return
			}
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("timed out waiting for actor %q to reach state %v", actorName, expectedState)
}

func callActor(t *testing.T, actorRef resources.ActorRef) (string, error) {
	return callActorPath(t, actorRef, "POST", "/")
}

func callActorPath(t *testing.T, actorRef resources.ActorRef, method, path string) (string, error) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := callActorPathOnce(t, actorRef, method, path)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		time.Sleep(1 * time.Second)
	}

	return "", fmt.Errorf("timed out waiting for actor response: %w", lastErr)
}

func callActorPathOnce(t *testing.T, actorRef resources.ActorRef, method, path string) (string, error) {
	t.Helper()
	router, err := e2e.NewRouterClient(t, context.Background())
	if err != nil {
		return "", fmt.Errorf("connecting to atenet-router: %w", err)
	}
	defer router.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := router.Do(ctx, method, actorRef, path, nil)
	if err != nil {
		return "", fmt.Errorf("failed to do request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	return string(body), nil
}

func TestWorkerPodDeletion(t *testing.T) {
	t.Parallel()
	// Create namespace
	nsObj := e2e.CreateNamespace(t)

	ctx := context.Background()
	clients := e2e.GetClients()

	// Create actor template.
	at, err := createActorTemplate(ctx, t, clients, nsObj, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
	if err != nil {
		t.Fatalf("failed to initialize ActorTemplate: %v", err)
	}

	actorName := "crash-actor-" + nsObj.Name

	// Creating an actor
	t.Logf("Creating Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	defer func() {
		clients.SubstrateAPI.DeleteActor(ctx, &ateapi.DeleteActorRequest{
			Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
		})
	}()

	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	// Resuming the actor
	t.Logf("Resuming Actor %q...", actorName)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	// Get actor to find the pod details
	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
	})
	if err != nil {
		t.Fatalf("failed to get Actor: %v", err)
	}

	podName := actor.GetStatus().GetWorkerAssignment().GetWorkerPod()
	podNamespace := actor.GetStatus().GetWorkerAssignment().GetWorkerNamespace()
	nodeName := actor.GetStatus().GetWorkerAssignment().GetNodeName()
	if podName == "" || podNamespace == "" || nodeName == "" {
		t.Fatalf("actor is running but pod details are missing: podName=%q, podNamespace=%q, nodeName=%q", podName, podNamespace, nodeName)
	}
	if got := actor.GetStatus().GetAssignedNode(); got != nodeName {
		t.Fatalf("running actor AssignedNode = %q, want %q", got, nodeName)
	}

	// Verify worker is in ListWorkers
	t.Logf("Verifying worker for pod %s/%s is listed...", podNamespace, podName)
	foundWorker := false
	workersResp, err := clients.SubstrateAPI.ListWorkers(ctx, &ateapi.ListWorkersRequest{})
	if err != nil {
		t.Fatalf("failed to list workers: %v", err)
	}
	for _, w := range workersResp.GetWorkers() {
		if w.GetWorkerNamespace() == podNamespace && w.GetWorkerPod() == podName {
			foundWorker = true
			break
		}
	}
	if !foundWorker {
		t.Fatalf("worker for pod %s/%s was not found in ListWorkers response: %v", podNamespace, podName, workersResp.GetWorkers())
	}

	// Delete the worker pod
	t.Logf("Deleting worker pod %s/%s...", podNamespace, podName)
	err = clients.K8s.CoreV1().Pods(podNamespace).Delete(ctx, podName, metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("failed to delete pod %s/%s: %v", podNamespace, podName, err)
	}

	// Wait for the actor to be marked as CRASHED
	t.Logf("Waiting for actor %q to transition to CRASHED...", actorName)
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_CRASHED)

	crashed, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName},
	})
	if err != nil {
		t.Fatalf("failed to get crashed actor %q: %v", actorName, err)
	}
	if got := crashed.GetStatus().GetAssignedNode(); got != nodeName {
		t.Errorf("crashed actor AssignedNode = %q, want %q preserved", got, nodeName)
	}
	crash := crashed.GetStatus().GetCrash()
	// The actor was RUNNING, not mid-operation, so the crash message carries no
	// "<op> failed: " prefix.
	if want := "worker pod went away while hosting the actor"; crash.GetMessage() != want {
		t.Errorf("Crash.Message = %q, want %q", crash.GetMessage(), want)
	}
	if crash.GetCrashTime() == nil {
		t.Errorf("Crash.CrashTime = nil, want set")
	}

	// Verify the worker is cleaned up (deleted) from store
	t.Logf("Verifying worker for pod %s/%s is removed from store...", podNamespace, podName)
	deadline := time.Now().Add(30 * time.Second)
	var lastWorkers []*ateapi.Worker
	for time.Now().Before(deadline) {
		workersResp, err = clients.SubstrateAPI.ListWorkers(ctx, &ateapi.ListWorkersRequest{})
		if err != nil {
			t.Fatalf("failed to list workers: %v", err)
		}
		lastWorkers = workersResp.GetWorkers()
		stillExists := false
		for _, w := range lastWorkers {
			if w.GetWorkerNamespace() == podNamespace && w.GetWorkerPod() == podName {
				stillExists = true
				break
			}
		}
		if !stillExists {
			t.Logf("Worker for pod %s/%s successfully removed.", podNamespace, podName)
			return
		}
		time.Sleep(1 * time.Second)
	}

	t.Errorf("worker for pod %s/%s was not cleaned up within 30s. Current workers: %v", podNamespace, podName, lastWorkers)
}

// A sandbox that dies on its own is reported by the worker: the actor goes
// CRASHED without any operation on it failing, its worker is freed, and it can
// be reverted and resumed.
func TestSandboxCrashIsReported(t *testing.T) {
	t.Parallel()
	nsObj := e2e.CreateNamespace(t)
	ctx := context.Background()
	clients := e2e.GetClients()

	at, err := createActorTemplate(ctx, t, clients, nsObj, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
	if err != nil {
		t.Fatalf("failed to initialize ActorTemplate: %v", err)
	}

	actorName := "sandbox-crash-actor-" + nsObj.Name
	actorRef := &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapi.DeleteActorRequest{Actor: actorRef})
	})

	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("failed to get Actor: %v", err)
	}
	assignment := actor.GetStatus().GetWorkerAssignment()
	if assignment.GetWorkerPod() == "" || assignment.GetWorkerNamespace() == "" {
		t.Fatalf("running actor has no pod recorded: %v", assignment)
	}

	e2e.KillActorSandbox(ctx, t, clients, assignment.GetWorkerNamespace(), assignment.GetWorkerPod(), actor.GetMetadata().GetUid())
	waitForActorStateWithTimeout(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_CRASHED, 2*time.Minute)

	crashed, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("failed to get crashed Actor: %v", err)
	}
	if msg := crashed.GetStatus().GetCrash().GetMessage(); !strings.Contains(msg, "exited") {
		t.Errorf("crash message = %q, want the worker's report of the sandbox exiting", msg)
	}
	if crashed.GetStatus().GetWorkerAssignment() != nil {
		t.Errorf("worker assignment survived the crash: %v", crashed.GetStatus().GetWorkerAssignment())
	}
	// the worker pod survives, unlike TestRevertCrashedActor
	if _, err := clients.K8s.CoreV1().Pods(assignment.GetWorkerNamespace()).Get(ctx, assignment.GetWorkerPod(), metav1.GetOptions{}); err != nil {
		t.Errorf("worker pod %s/%s is gone: %v", assignment.GetWorkerNamespace(), assignment.GetWorkerPod(), err)
	}

	reverted, err := clients.SubstrateAPI.RevertActor(ctx, &ateapi.RevertActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("RevertActor from CRASHED: %v", err)
	}
	if got := reverted.GetActor().GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("state after revert = %v, want SUSPENDED", got)
	}
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("failed to resume the reverted Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)
	if _, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName}); err != nil {
		t.Fatalf("failed to call the reverted Actor: %v", err)
	}
}

func TestRevertCrashedActor(t *testing.T) {
	t.Parallel()
	nsObj := e2e.CreateNamespace(t)
	ctx := context.Background()
	clients := e2e.GetClients()

	at, err := createActorTemplate(ctx, t, clients, nsObj, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
	if err != nil {
		t.Fatalf("failed to initialize ActorTemplate: %v", err)
	}

	actorName := "revert-crash-actor-" + nsObj.Name
	actorRef := &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapi.DeleteActorRequest{Actor: actorRef})
	})

	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor: %v", err)
	}
	validateCounterResponse(t, resp, "before snapshot", 1, 1)

	suspended, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: actorRef, Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS}})
	if err != nil {
		t.Fatalf("failed to suspend Actor: %v", err)
	}
	snapshotURI := durableSnapshotURI(suspended.GetActor().GetStatus())
	if snapshotURI == "" {
		t.Fatal("suspend wrote no external snapshot")
	}

	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("failed to resume Actor after suspend: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor before crash: %v", err)
	}
	validateCounterResponse(t, resp, "before crash", 2, 2)

	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("failed to get Actor: %v", err)
	}
	podName := actor.GetStatus().GetWorkerAssignment().GetWorkerPod()
	podNamespace := actor.GetStatus().GetWorkerAssignment().GetWorkerNamespace()
	nodeName := actor.GetStatus().GetWorkerAssignment().GetNodeName()
	if podName == "" || podNamespace == "" || nodeName == "" {
		t.Fatalf("running actor has no pod recorded: %v", actor.GetStatus().GetWorkerAssignment())
	}

	// Deleting the pod is how a real crash happens: releaseBoundActor marks the
	// actor CRASHED and clears its assignment while keeping AssignedNode so
	// revert can still clean up the node.
	if err := clients.K8s.CoreV1().Pods(podNamespace).Delete(ctx, podName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("failed to delete worker pod %s/%s: %v", podNamespace, podName, err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_CRASHED)

	crashed, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("failed to get crashed Actor: %v", err)
	}
	if got := crashed.GetStatus().GetAssignedNode(); got != nodeName {
		t.Fatalf("crashed actor AssignedNode = %q, want %q", got, nodeName)
	}

	reverted, err := clients.SubstrateAPI.RevertActor(ctx, &ateapi.RevertActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("RevertActor from CRASHED: %v", err)
	}
	if got := reverted.GetActor().GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("state after revert = %v, want SUSPENDED", got)
	}
	if got := durableSnapshotURI(reverted.GetActor().GetStatus()); got != snapshotURI {
		t.Fatalf("snapshot URI after revert = %q, want %q", got, snapshotURI)
	}
	if reverted.GetActor().GetStatus().GetWorkerAssignment() != nil {
		t.Fatal("worker assignment survived revert")
	}
	if got := reverted.GetActor().GetStatus().GetAssignedNode(); got != "" {
		t.Fatalf("assigned node after revert = %q, want empty", got)
	}

	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("failed to resume crashed-and-reverted Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor after recovery: %v", err)
	}
	validateCounterResponse(t, resp, "after recovery from CRASHED", 2, 2)
}
