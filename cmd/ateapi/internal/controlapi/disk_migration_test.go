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

package controlapi

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/scheduling"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
)

type fakeDiskMigrationHerder struct {
	ateletpb.UnimplementedAteomHerderServer
	exportActorUID string
	exportCount    int
	exportErr      error
	exportResp     *ateletpb.ExportActorDiskResponse
	importReq      *ateletpb.ImportActorDiskRequest
}

func (f *fakeDiskMigrationHerder) ExportActorDisk(_ context.Context, req *ateletpb.ExportActorDiskRequest) (*ateletpb.ExportActorDiskResponse, error) {
	f.exportActorUID = req.GetActorUid()
	f.exportCount++
	if f.exportErr != nil {
		return nil, f.exportErr
	}
	return f.exportResp, nil
}

func (f *fakeDiskMigrationHerder) ImportActorDisk(_ context.Context, req *ateletpb.ImportActorDiskRequest) (*ateletpb.ImportActorDiskResponse, error) {
	f.importReq = req
	return &ateletpb.ImportActorDiskResponse{}, nil
}

func startFakeAteletServer(t *testing.T, herder ateletpb.AteomHerderServer) *grpc.ClientConn {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := grpc.NewServer()
	ateletpb.RegisterAteomHerderServer(srv, herder)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestAssignWorkerAttempt_CrossNodeFallbackWhenRequiredNodeFull(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)

	// Worker on node-1 has capacity 1 and is already full
	workerNode1 := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("pod-node1")},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod-node1",
		WorkerPodUid:    testWorkerUID("pod-node1"),
		NodeName:        "node-1",
		SandboxClass:    "gvisor",
		Status: &ateapipb.WorkerStatus{
			State:    ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity: &ateapipb.WorkerResources{Actors: 1},
		},
	}
	if _, err := persistence.CreateWorker(ctx, workerNode1); err != nil {
		t.Fatalf("CreateWorker node1: %v", err)
	}
	seedAssignment(t, persistence, testWorkerUID("pod-node1"), &ateapipb.ActorAssignment{
		Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "busy-actor"},
		ActorUid: "busy-uid",
	})

	// Worker on node-2 has capacity 1 and is free
	workerNode2 := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("pod-node2")},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod-node2",
		WorkerPodUid:    testWorkerUID("pod-node2"),
		NodeName:        "node-2",
		SandboxClass:    "gvisor",
		Status: &ateapipb.WorkerStatus{
			State:    ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity: &ateapipb.WorkerResources{Actors: 1},
		},
	}
	if _, err := persistence.CreateWorker(ctx, workerNode2); err != nil {
		t.Fatalf("CreateWorker node2: %v", err)
	}

	// Paused actor whose local snapshot is pinned to node-1
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "paused-actor"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_PAUSED,
			LocalSnapshotInfo: &ateapipb.LocalSnapshotInfo{
				SnapshotName:              "snap-1",
				NodeVmsWithLocalSnapshots: []string{"node-1"},
			},
		},
	})

	cacheCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wc := workercache.New(persistence, time.Minute)
	if err := wc.Start(cacheCtx); err != nil {
		t.Fatalf("workercache.Start: %v", err)
	}

	w := &ActorWorkflow{store: persistence, workerCache: wc, scheduler: scheduling.New(wc)}
	tmpl := &ateapipb.ActorTemplate{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "sub-tmpl"},
		SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR},
	}

	_, assignedWorker, err := w.assignWorkerAttempt(ctx, resources.ActorRef{Atespace: "team-a", Name: "paused-actor"}, actor, tmpl)
	if err != nil {
		t.Fatalf("assignWorkerAttempt failed: %v", err)
	}
	if assignedWorker.GetNodeName() != "node-2" {
		t.Errorf("assignedWorker node = %q, want %q (fallback across nodes)", assignedWorker.GetNodeName(), "node-2")
	}
}

func TestLateWorkerBinding_CrossNodeExportBeforeBindAndImport(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)

	herder1 := &fakeDiskMigrationHerder{
		exportResp: &ateletpb.ExportActorDiskResponse{
			GceDiskName: "actor-disk-7",
			DeviceName:  "actor-disk-7",
		},
	}
	herder2 := &fakeDiskMigrationHerder{}

	conn1 := startFakeAteletServer(t, herder1)
	conn2 := startFakeAteletServer(t, herder2)

	ateletIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		byNode: func(obj any) ([]string, error) {
			pod := obj.(*corev1.Pod)
			return []string{pod.Spec.NodeName}, nil
		},
	})
	_ = ateletIndexer.Add(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ate-system", Name: "atelet-1", UID: types.UID("uid-node-1")},
		Spec:       corev1.PodSpec{NodeName: "node-1"},
	})
	_ = ateletIndexer.Add(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ate-system", Name: "atelet-2", UID: types.UID("uid-node-2")},
		Spec:       corev1.PodSpec{NodeName: "node-2"},
	})

	dialer := NewAteletDialer(nil, ateletIndexer, "", "")
	dialer.ateletConns.Add("uid-node-1", conn1)
	dialer.ateletConns.Add("uid-node-2", conn2)

	workerNode1 := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("pod-node1")},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod-node1",
		WorkerPodUid:    testWorkerUID("pod-node1"),
		NodeName:        "node-1",
		SandboxClass:    "gvisor",
		Status: &ateapipb.WorkerStatus{
			State:    ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity: &ateapipb.WorkerResources{Actors: 1},
		},
	}
	if _, err := persistence.CreateWorker(ctx, workerNode1); err != nil {
		t.Fatalf("CreateWorker node1: %v", err)
	}
	seedAssignment(t, persistence, testWorkerUID("pod-node1"), &ateapipb.ActorAssignment{
		Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "busy-actor"},
		ActorUid: "busy-uid",
	})

	workerNode2 := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("pod-node2")},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod-node2",
		WorkerPodUid:    testWorkerUID("pod-node2"),
		NodeName:        "node-2",
		SandboxClass:    "gvisor",
		Status: &ateapipb.WorkerStatus{
			State:    ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity: &ateapipb.WorkerResources{Actors: 1},
		},
	}
	if _, err := persistence.CreateWorker(ctx, workerNode2); err != nil {
		t.Fatalf("CreateWorker node2: %v", err)
	}

	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "late-bind-actor"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_PAUSED,
			LocalSnapshotInfo: &ateapipb.LocalSnapshotInfo{
				SnapshotName:              "snap-7",
				NodeVmsWithLocalSnapshots: []string{"node-1"},
			},
		},
	})

	cacheCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wc := workercache.New(persistence, time.Minute)
	if err := wc.Start(cacheCtx); err != nil {
		t.Fatalf("workercache.Start: %v", err)
	}

	w := &ActorWorkflow{store: persistence, workerCache: wc, scheduler: scheduling.New(wc), dialer: dialer}
	tmpl := &ateapipb.ActorTemplate{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "sub-tmpl"},
		SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR},
	}
	ref := resources.ActorRef{Atespace: "team-a", Name: "late-bind-actor"}

	assignedActor, assignedWorker, err := w.assignWorkerAttempt(ctx, ref, actor, tmpl)
	if err != nil {
		t.Fatalf("assignWorkerAttempt failed: %v", err)
	}
	if herder1.exportCount != 1 {
		t.Fatalf("expected ExportActorDisk to be called exactly once during assignWorkerAttempt, got %d", herder1.exportCount)
	}
	if herder1.exportActorUID != actor.GetMetadata().GetUid() {
		t.Errorf("exportActorUID = %q, want %q", herder1.exportActorUID, actor.GetMetadata().GetUid())
	}
	if herder2.importReq == nil {
		t.Fatal("expected ImportActorDisk on node-2 to be called before worker binding during assignWorkerAttempt")
	}
	if herder2.importReq.GetGceDiskName() != "actor-disk-7" {
		t.Errorf("ImportActorDisk gceDiskName = %q, want actor-disk-7", herder2.importReq.GetGceDiskName())
	}
	if assignedWorker.GetNodeName() != "node-2" {
		t.Errorf("assignedWorker node = %q, want %q", assignedWorker.GetNodeName(), "node-2")
	}
	nodesAfterAssign := assignedActor.GetStatus().GetLocalSnapshotInfo().GetNodeVmsWithLocalSnapshots()
	if len(nodesAfterAssign) != 1 || nodesAfterAssign[0] != "node-2" {
		t.Errorf("nodesAfterAssign = %v, want [node-2]", nodesAfterAssign)
	}

	// Step 2: ensureLocalSnapshotDiskMigrated should be a no-op since both export and import
	// already completed before worker binding.
	updated, err := w.ensureLocalSnapshotDiskMigrated(ctx, ref, assignedActor, assignedWorker)
	if err != nil {
		t.Fatalf("ensureLocalSnapshotDiskMigrated failed: %v", err)
	}
	if herder1.exportCount != 1 {
		t.Errorf("ExportActorDisk count after migrate = %d, want 1 (should not re-export)", herder1.exportCount)
	}
	nodesFinal := updated.GetStatus().GetLocalSnapshotInfo().GetNodeVmsWithLocalSnapshots()
	if len(nodesFinal) != 1 || nodesFinal[0] != "node-2" {
		t.Errorf("nodesFinal = %v, want [node-2]", nodesFinal)
	}
}

func TestEnsureLocalSnapshotDiskMigrated_CrossNode(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)

	herder1 := &fakeDiskMigrationHerder{
		exportResp: &ateletpb.ExportActorDiskResponse{
			GceDiskName: "actor-disk-0",
			DeviceName:  "actor-disk-0",
		},
	}
	herder2 := &fakeDiskMigrationHerder{}

	conn1 := startFakeAteletServer(t, herder1)
	conn2 := startFakeAteletServer(t, herder2)

	ateletIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		byNode: func(obj any) ([]string, error) {
			pod := obj.(*corev1.Pod)
			return []string{pod.Spec.NodeName}, nil
		},
	})
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ate-system", Name: "atelet-1", UID: types.UID("uid-node-1")},
		Spec:       corev1.PodSpec{NodeName: "node-1"},
	}
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ate-system", Name: "atelet-2", UID: types.UID("uid-node-2")},
		Spec:       corev1.PodSpec{NodeName: "node-2"},
	}
	_ = ateletIndexer.Add(pod1)
	_ = ateletIndexer.Add(pod2)

	dialer := NewAteletDialer(nil, ateletIndexer, "", "")
	dialer.ateletConns.Add("uid-node-1", conn1)
	dialer.ateletConns.Add("uid-node-2", conn2)

	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "migrating-actor"},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RESUMING,
			LocalSnapshotInfo: &ateapipb.LocalSnapshotInfo{
				SnapshotName:              "snap-1",
				NodeVmsWithLocalSnapshots: []string{"node-1"},
			},
		},
	})

	workerNode2 := &ateapipb.Worker{
		Metadata: &ateapipb.ResourceMetadata{Name: "worker-node-2"},
		NodeName: "node-2",
	}

	w := &ActorWorkflow{store: persistence, dialer: dialer}
	ref := resources.ActorRef{Atespace: "team-a", Name: "migrating-actor"}
	updated, err := w.ensureLocalSnapshotDiskMigrated(ctx, ref, actor, workerNode2)
	if err != nil {
		t.Fatalf("ensureLocalSnapshotDiskMigrated failed: %v", err)
	}

	if herder1.exportActorUID != actor.GetMetadata().GetUid() {
		t.Errorf("node-1 ExportActorDisk called with actorUID=%q, want %q", herder1.exportActorUID, actor.GetMetadata().GetUid())
	}
	if herder2.importReq == nil {
		t.Fatal("node-2 ImportActorDisk was not called")
	}
	if herder2.importReq.GetActorUid() != actor.GetMetadata().GetUid() || herder2.importReq.GetGceDiskName() != "actor-disk-0" {
		t.Errorf("unexpected ImportActorDisk request on node-2: %+v", herder2.importReq)
	}
	nodes := updated.GetStatus().GetLocalSnapshotInfo().GetNodeVmsWithLocalSnapshots()
	if len(nodes) != 1 || nodes[0] != "node-2" {
		t.Errorf("updated NodeVmsWithLocalSnapshots = %v, want [node-2]", nodes)
	}
}

func TestLateWorkerBinding_DiskQueueFullRetriesSameNode(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)

	// Herder on node-1 returns ResourceExhausted (disk queue full)
	herder1 := &fakeDiskMigrationHerder{
		exportErr: status.Errorf(codes.ResourceExhausted, "disk operation queue full on node (5 pending operations, max 5); retry resume on same node"),
	}
	herder2 := &fakeDiskMigrationHerder{}

	conn1 := startFakeAteletServer(t, herder1)
	conn2 := startFakeAteletServer(t, herder2)

	ateletIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		byNode: func(obj any) ([]string, error) {
			pod := obj.(*corev1.Pod)
			return []string{pod.Spec.NodeName}, nil
		},
	})
	_ = ateletIndexer.Add(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ate-system", Name: "atelet-1", UID: types.UID("uid-node-1")},
		Spec:       corev1.PodSpec{NodeName: "node-1"},
	})
	_ = ateletIndexer.Add(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ate-system", Name: "atelet-2", UID: types.UID("uid-node-2")},
		Spec:       corev1.PodSpec{NodeName: "node-2"},
	})

	dialer := NewAteletDialer(nil, ateletIndexer, "", "")
	dialer.ateletConns.Add("uid-node-1", conn1)
	dialer.ateletConns.Add("uid-node-2", conn2)

	// Worker on node-1 starts busy
	workerNode1 := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("pod-node1")},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod-node1",
		WorkerPodUid:    testWorkerUID("pod-node1"),
		NodeName:        "node-1",
		SandboxClass:    "gvisor",
		Status: &ateapipb.WorkerStatus{
			State:    ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity: &ateapipb.WorkerResources{Actors: 1},
		},
	}
	if _, err := persistence.CreateWorker(ctx, workerNode1); err != nil {
		t.Fatalf("CreateWorker node1: %v", err)
	}
	seedAssignment(t, persistence, testWorkerUID("pod-node1"), &ateapipb.ActorAssignment{
		Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "busy-actor"},
		ActorUid: "busy-uid",
	})

	// Worker on node-2 is free
	workerNode2 := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("pod-node2")},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod-node2",
		WorkerPodUid:    testWorkerUID("pod-node2"),
		NodeName:        "node-2",
		SandboxClass:    "gvisor",
		Status: &ateapipb.WorkerStatus{
			State:    ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity: &ateapipb.WorkerResources{Actors: 1},
		},
	}
	if _, err := persistence.CreateWorker(ctx, workerNode2); err != nil {
		t.Fatalf("CreateWorker node2: %v", err)
	}

	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "queue-retry-actor"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_PAUSED,
			LocalSnapshotInfo: &ateapipb.LocalSnapshotInfo{
				SnapshotName:              "snap-1",
				NodeVmsWithLocalSnapshots: []string{"node-1"},
			},
		},
	})

	cacheCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wc := workercache.New(persistence, time.Minute)
	if err := wc.Start(cacheCtx); err != nil {
		t.Fatalf("workercache.Start: %v", err)
	}

	// After 50ms, release busy-actor from workerNode1 so node-1 has free capacity
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = persistence.ReleaseActorFromWorker(ctx, testWorkerUID("pod-node1"), "busy-uid")
	}()

	w := &ActorWorkflow{store: persistence, workerCache: wc, scheduler: scheduling.New(wc), dialer: dialer}
	tmpl := &ateapipb.ActorTemplate{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "sub-tmpl"},
		SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR},
	}
	ref := resources.ActorRef{Atespace: "team-a", Name: "queue-retry-actor"}

	assignedActor, assignedWorker, err := w.assignWorkerAttempt(ctx, ref, actor, tmpl)
	if err != nil {
		t.Fatalf("assignWorkerAttempt failed: %v", err)
	}
	if herder1.exportCount == 0 {
		t.Fatal("expected ExportActorDisk to be attempted on node-1")
	}
	// Should have retried scheduling on node-1 and bound workerNode1 (same node!)
	if assignedWorker.GetNodeName() != "node-1" {
		t.Errorf("assignedWorker node = %q, want %q (same-node retry after queue full)", assignedWorker.GetNodeName(), "node-1")
	}
	nodesAfterAssign := assignedActor.GetStatus().GetLocalSnapshotInfo().GetNodeVmsWithLocalSnapshots()
	if len(nodesAfterAssign) != 1 || nodesAfterAssign[0] != "node-1" {
		t.Errorf("nodesAfterAssign = %v, want [node-1]", nodesAfterAssign)
	}
}
