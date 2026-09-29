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
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/internal/objectstore/objectstoretest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func testActorTemplate() *ateapipb.ActorTemplate {
	return &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "team-a",
			Name:     "tmpl-1",
			Uid:      "tmpl-uid-1",
		},
		SnapshotConfig: &ateapipb.SnapshotConfig{
			Object: &ateapipb.ObjectSnapshotStorage{
				StorageLocation: "gs://test-bucket/root",
			},
			OnPause:  ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			OnCommit: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
		},
	}
}

func testActor() *ateapipb.Actor {
	return &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "team-a",
			Name:     "actor-1",
			Uid:      "actor-uid-1",
		},
		Status: &ateapipb.ActorStatus{
			WorkerAssignment: &ateapipb.WorkerAssignment{
				NodeName: "node-1",
			},
		},
	}
}

func TestPrepareNewActor(t *testing.T) {
	ctx := t.Context()
	store := objectstoretest.New()
	tmpl := testActorTemplate()
	actor := testActor()

	t.Run("no source or golden tag", func(t *testing.T) {
		plugin := NewObjectSnapshotPluginControlPlane(store)
		storage, snap, err := plugin.PrepareNewActor(ctx, actor, tmpl)
		if err != nil {
			t.Fatalf("PrepareNewActor() error = %v", err)
		}
		if storage != nil {
			t.Errorf("PrepareNewActor() storage = %v, want nil", storage)
		}
		if snap != nil {
			t.Errorf("PrepareNewActor() snap = %v, want nil", snap)
		}
	})

	t.Run("cloning from source tag", func(t *testing.T) {
		clonedActor := testActor()
		clonedActor.SourceTag = &ateapipb.ObjectRef{Atespace: "team-a", Name: "tag-1"}
		tagURI := "gs://test-bucket/root/atespaces/team-a/tags/tag-uid-1"

		plugin := NewObjectSnapshotPluginControlPlane(store, WithTagResolver(func(ctx context.Context, a *ateapipb.Actor, ref *ateapipb.ObjectRef, tmpl *ateapipb.ActorTemplate) (*ateapipb.Tag, error) {
			return &ateapipb.Tag{
				Metadata: &ateapipb.ResourceMetadata{Atespace: ref.GetAtespace(), Name: ref.GetName(), Uid: "tag-uid-1"},
				Status: &ateapipb.TagStatus{
					ActorTemplateUid: "tmpl-uid-1",
					Snapshot: &ateapipb.Snapshot{
						SnapshotId:   "tag-uid-1",
						ContentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
						Object: &ateapipb.ObjectSnapshot{
							SnapshotUri: tagURI,
						},
					},
				},
			}, nil
		}))

		storage, snap, err := plugin.PrepareNewActor(ctx, clonedActor, tmpl)
		if err != nil {
			t.Fatalf("PrepareNewActor() error = %v", err)
		}
		if storage != nil {
			t.Errorf("PrepareNewActor() storage = %v, want nil", storage)
		}
		if got := snap.GetObject().GetSnapshotUri(); got != tagURI {
			t.Errorf("SnapshotUri = %q, want %q", got, tagURI)
		}
		if got := snap.GetObject().GetActorTemplateUid(); got != "tmpl-uid-1" {
			t.Errorf("ActorTemplateUid = %q, want %q", got, "tmpl-uid-1")
		}
		if got := snap.GetSurvivability(); got != ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE {
			t.Errorf("Survivability = %v, want DURABLE", got)
		}
	})
}

func TestPrepareNewSnapshot(t *testing.T) {
	ctx := t.Context()
	plugin := NewObjectSnapshotPluginControlPlane(objectstoretest.New())
	tmpl := testActorTemplate()
	actor := testActor()

	t.Run("LOCAL rung", func(t *testing.T) {
		snap, err := plugin.PrepareNewSnapshot(ctx, actor, tmpl, "snap-local-1", ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_LOCAL)
		if err != nil {
			t.Fatalf("PrepareNewSnapshot() error = %v", err)
		}
		if snap.GetSnapshotId() != "snap-local-1" {
			t.Errorf("SnapshotId = %q, want %q", snap.GetSnapshotId(), "snap-local-1")
		}
		if snap.GetLocal() == nil {
			t.Fatal("expected LocalSnapshot to be populated")
		}
		if snap.GetObject() != nil {
			t.Errorf("expected ObjectSnapshot to be nil, got %v", snap.GetObject())
		}
		if !slices.Equal(snap.GetLocal().GetNodeVmsWithLocalSnapshots(), []string{"node-1"}) {
			t.Errorf("NodeVmsWithLocalSnapshots = %v, want [node-1]", snap.GetLocal().GetNodeVmsWithLocalSnapshots())
		}
		if snap.GetSurvivability() != ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_LOCAL {
			t.Errorf("Survivability = %v, want LOCAL", snap.GetSurvivability())
		}
	})

	t.Run("DURABLE rung", func(t *testing.T) {
		snap, err := plugin.PrepareNewSnapshot(ctx, actor, tmpl, "snap-durable-1", ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE)
		if err != nil {
			t.Fatalf("PrepareNewSnapshot() error = %v", err)
		}
		if snap.GetSnapshotId() != "snap-durable-1" {
			t.Errorf("SnapshotId = %q, want %q", snap.GetSnapshotId(), "snap-durable-1")
		}
		wantURI := "gs://test-bucket/root/atespaces/team-a/actors/actor-uid-1/snapshots/snap-durable-1"
		if got := snap.GetObject().GetSnapshotUri(); got != wantURI {
			t.Errorf("SnapshotUri = %q, want %q", got, wantURI)
		}
		if snap.GetLocal() != nil {
			t.Errorf("expected LocalSnapshot to be nil, got %v", snap.GetLocal())
		}
		if snap.GetSurvivability() != ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE {
			t.Errorf("Survivability = %v, want DURABLE", snap.GetSurvivability())
		}
	})
}

func TestDeleteSnapshotAndActor(t *testing.T) {
	ctx := t.Context()
	store := objectstoretest.New()
	plugin := NewObjectSnapshotPluginControlPlane(store)
	actor := testActor()

	snap1URI, err := resources.NewActorSnapshotURI("gs://test-bucket/root", "team-a", "actor-uid-1", "snap-1")
	if err != nil {
		t.Fatal(err)
	}
	snap2URI, err := resources.NewActorSnapshotURI("gs://test-bucket/root", "team-a", "actor-uid-1", "snap-2")
	if err != nil {
		t.Fatal(err)
	}
	tagURI, err := resources.NewTagSnapshotURI("gs://test-bucket/root", "team-a", "tag-uid-1")
	if err != nil {
		t.Fatal(err)
	}

	store.PutSnapshot(t, snap1URI, "checkpoint.tar", "volumes.tar")
	store.PutSnapshot(t, snap2URI, "checkpoint.tar")
	store.PutSnapshot(t, tagURI, "checkpoint.tar")

	// Borrowing a tag snapshot should not delete the tag's objects.
	borrowedSnap := &ateapipb.Snapshot{
		Object: &ateapipb.ObjectSnapshot{SnapshotUri: tagURI.String()},
	}
	if err := plugin.DeleteSnapshot(ctx, actor, borrowedSnap); err != nil {
		t.Fatalf("DeleteSnapshot(borrowed) error = %v", err)
	}
	if got := len(store.Snapshot(t, tagURI)); got != 1 {
		t.Fatalf("expected tag snapshot to remain intact, got %d objects", got)
	}

	// Deleting snap1 removes only snap1's objects.
	snap1 := &ateapipb.Snapshot{
		Object: &ateapipb.ObjectSnapshot{SnapshotUri: snap1URI.String()},
	}
	if err := plugin.DeleteSnapshot(ctx, actor, snap1); err != nil {
		t.Fatalf("DeleteSnapshot(snap1) error = %v", err)
	}
	if got := len(store.Snapshot(t, snap1URI)); got != 0 {
		t.Errorf("expected snap1 to be deleted, got %v", store.Snapshot(t, snap1URI))
	}
	if got := len(store.Snapshot(t, snap2URI)); got != 1 {
		t.Errorf("expected snap2 to remain, got %v", store.Snapshot(t, snap2URI))
	}

	// Deleting the actor removes the entire owner prefix (including snap2).
	actor.Status.DurableSnapshotStatus = &ateapipb.Snapshot{
		Object: &ateapipb.ObjectSnapshot{SnapshotUri: snap2URI.String()},
	}
	if err := plugin.DeleteActor(ctx, actor); err != nil {
		t.Fatalf("DeleteActor() error = %v", err)
	}
	if got := len(store.Snapshot(t, snap2URI)); got != 0 {
		t.Errorf("expected actor snapshots to be deleted, got %v", store.Snapshot(t, snap2URI))
	}
	if got := len(store.Snapshot(t, tagURI)); got != 1 {
		t.Errorf("expected tag snapshot to remain untouched, got %v", store.Snapshot(t, tagURI))
	}
}

func TestTagSnapshotOperations(t *testing.T) {
	ctx := t.Context()
	store := objectstoretest.New()
	plugin := NewObjectSnapshotPluginControlPlane(store)
	tmpl := testActorTemplate()
	actor := testActor()

	srcURI, err := resources.NewActorSnapshotURI("gs://test-bucket/root", "team-a", "actor-uid-1", "snap-1")
	if err != nil {
		t.Fatal(err)
	}
	store.PutSnapshot(t, srcURI, "checkpoint.tar", "rootfs-delta.tar")

	sourceSnap := &ateapipb.Snapshot{
		SnapshotId:    "snap-1",
		Survivability: ateapipb.SurvivabilityRung_SURVIVABILITY_RUNG_DURABLE,
		ContentScope:  ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
		Object: &ateapipb.ObjectSnapshot{
			SnapshotUri:      srcURI.String(),
			ActorTemplateUid: "tmpl-uid-1",
		},
	}
	actor.Status.DurableSnapshotStatus = sourceSnap

	tag := &ateapipb.Tag{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "team-a",
			Name:     "release-v1",
			Uid:      "tag-uid-99",
		},
	}

	reservedSnap, err := plugin.ReserveTagSnapshot(ctx, tag, actor, tmpl)
	if err != nil {
		t.Fatalf("ReserveTagSnapshot() error = %v", err)
	}
	wantTagURI := "gs://test-bucket/root/atespaces/team-a/tags/tag-uid-99"
	if got := reservedSnap.GetObject().GetSnapshotUri(); got != wantTagURI {
		t.Fatalf("ReserveTagSnapshot URI = %q, want %q", got, wantTagURI)
	}
	tag.Status = &ateapipb.TagStatus{Snapshot: reservedSnap}

	if err := plugin.CopyToTagSnapshot(ctx, tag, sourceSnap); err != nil {
		t.Fatalf("CopyToTagSnapshot() error = %v", err)
	}

	parsedTagURI, err := resources.ParseSnapshotURI(wantTagURI)
	if err != nil {
		t.Fatal(err)
	}
	if got := store.Snapshot(t, parsedTagURI); !slices.Equal(got, []string{"checkpoint.tar", "rootfs-delta.tar"}) {
		t.Fatalf("copied tag objects = %v, want [checkpoint.tar rootfs-delta.tar]", got)
	}

	if err := plugin.DeleteTagSnapshot(ctx, tag); err != nil {
		t.Fatalf("DeleteTagSnapshot() error = %v", err)
	}
	if got := store.Snapshot(t, parsedTagURI); len(got) != 0 {
		t.Errorf("expected tag objects to be deleted, got %v", got)
	}
}
