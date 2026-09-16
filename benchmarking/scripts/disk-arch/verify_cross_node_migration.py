#!/usr/bin/env python3
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""End-to-end verification script for Architecture 3 cross-node disk detachment and attachment."""

import json
import os
import subprocess
import sys
import time
import uuid

PROJECT_ID = os.environ.get("PROJECT_ID", "msau-gke-dev")
ZONE = os.environ.get("CLUSTER_LOCATION", "us-central1-a")


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    print(f"+ {' '.join(cmd)}")
    return subprocess.run(cmd, text=True, capture_output=True, check=check)


def get_substrate_nodes() -> list[str]:
    p = run(
        [
            "kubectl",
            "get",
            "nodes",
            "-l",
            "cloud.google.com/gke-nodepool=substrate-node-pool",
            "-o",
            "jsonpath={.items[*].metadata.name}",
        ]
    )
    nodes = [n for n in p.stdout.strip().split() if n]
    if len(nodes) < 2:
        raise RuntimeError(f"Expected at least 2 substrate nodes, got: {nodes}")
    return nodes


def check_disk_attached_to(disk_name: str) -> str:
    p = run(
        [
            "gcloud",
            "compute",
            "disks",
            "describe",
            disk_name,
            f"--zone={ZONE}",
            f"--project={PROJECT_ID}",
            "--format=json",
        ]
    )
    data = json.loads(p.stdout)
    users = data.get("users", [])
    if not users:
        return "NONE"
    return users[0].split("/")[-1]


def cleanup_e2e_actors():
    p = run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "get", "actors", "-o", "json"], check=False)
    if p.returncode != 0:
        return
    data = json.loads(p.stdout)
    for actor in data.get("actors", []):
        name = actor.get("metadata", {}).get("name", "")
        state = actor.get("status", {}).get("state", "")
        if name.startswith("e2e-"):
            print(f"Cleaning up leftover actor {name} (state: {state})...")
            if state == "ACTOR_STATE_PAUSED":
                run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "resume", "actor", name], check=False)
            run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "suspend", "actor", name], check=False)
            run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "delete", "actor", name], check=False)


def wait_for_ready_pod_on_node(node: str):
    print(f"Waiting for worker pod to become Ready on {node}...")
    for _ in range(45):
        p = run(["kubectl", "get", "pods", "-n", "benchmark-workloads", "-l", "ate.dev/worker-pool=benchmark-ateom", "-o", "json"], check=False)
        if p.returncode == 0:
            pods = json.loads(p.stdout).get("items", [])
            ready_pods = [
                pod for pod in pods
                if pod.get("spec", {}).get("nodeName") == node
                and pod.get("status", {}).get("phase") == "Running"
                and pod.get("status", {}).get("deletionTimestamp") is None
                and all(cs.get("ready") for cs in pod.get("status", {}).get("containerStatuses", [{}]))
            ]
            if ready_pods:
                print(f"Worker pod {ready_pods[0]['metadata']['name']} is Ready on {node}")
                time.sleep(3)
                return
        time.sleep(2)
    raise RuntimeError(f"Timed out waiting for Ready worker pod on {node}")


def main():
    nodes = get_substrate_nodes()
    node1, node2 = nodes[0], nodes[1]
    print(f"=== E2E Cross-Node Disk Migration Test ===")
    print(f"Node 1: {node1}")
    print(f"Node 2: {node2}")

    # 1. Ensure both nodes are uncordoned and clean up any leftover test actors
    run(["kubectl", "uncordon", node1, node2])
    cleanup_e2e_actors()

    # 2. Provision 2 disks on node1 (actor-disk-0, actor-disk-1) if not already on node1
    need_setup = "--reset-pool" in sys.argv
    if not need_setup:
        try:
            o0 = check_disk_attached_to("actor-disk-0")
            o1 = check_disk_attached_to("actor-disk-1")
            if o0 != node1 or o1 != node1:
                need_setup = True
        except Exception:
            need_setup = True

    if need_setup:
        print("\n--- Step 1: Provisioning dedicated disks on Node 1 ---")
        run(
            [
                "python3",
                "benchmarking/scripts/disk-arch/setup_disk_pool.py",
                "--cleanup",
            ]
        )
        run(
            [
                "python3",
                "benchmarking/scripts/disk-arch/setup_disk_pool.py",
                "--node",
                node1,
                "--count",
                "2",
                "--throughput",
                "800",
            ]
        )
        print("\n--- Step 2: Restarting atelet DaemonSet ---")
        run(["kubectl", "rollout", "restart", "ds", "-n", "ate-system", "-l", "app=atelet"])
        run(["kubectl", "rollout", "status", "ds", "-n", "ate-system", "-l", "app=atelet", "--timeout=120s"])
    else:
        print("\n--- Step 1 & 2: Disks actor-disk-0 and actor-disk-1 already provisioned on Node 1 ---")

    print("\n--- Step 2b: Waiting for benchmark-workloads/sleep golden snapshot to be ready ---")
    for _ in range(60):
        p = run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "get", "actor-template", "sleep", "-o", "json"], check=False)
        if p.returncode == 0:
            templates = json.loads(p.stdout).get("actorTemplates", [])
            if templates:
                tmpl = templates[0]
                snap = tmpl.get("status", {}).get("goldenSnapshotStatus", {}).get("goldenSnapshot", {}).get("snapshotUri", "")
                if snap:
                    print(f"Golden snapshot ready: {snap}")
                    break
        time.sleep(2)
    else:
        raise RuntimeError("Timed out waiting for sleep golden snapshot")

    # 4. Cordon node2 temporarily so actor-1 and actor-2 start on node1
    print(f"\n--- Step 3: Cordoning {node2} and scaling WorkerPool to 1 worker on {node1} ---")
    run(["kubectl", "cordon", node2])
    run(
        [
            "kubectl",
            "patch",
            "workerpool",
            "benchmark-ateom",
            "-n",
            "benchmark-workloads",
            "--type=merge",
            "-p",
            '{"spec":{"replicas":1}}',
        ]
    )
    run(["kubectl", "delete", "pods", "-n", "benchmark-workloads", "-l", "ate.dev/worker-pool=benchmark-ateom"])
    wait_for_ready_pod_on_node(node1)

    actor_migrating = f"e2e-migrate-{uuid.uuid4().hex[:6]}"
    actor_squatter = f"e2e-squat-{uuid.uuid4().hex[:6]}"

    print(f"\n--- Step 4: Creating and pausing {actor_migrating} on {node1} ---")
    run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "create", "actor", actor_migrating, "--template=sleep"])
    run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "resume", "actor", actor_migrating])
    time.sleep(2)
    run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "pause", "actor", actor_migrating])

    info = run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "get", "actor", actor_migrating, "-o", "json"])
    actor_json = json.loads(info.stdout)["actors"][0]
    actor_uid = actor_json.get("metadata", {}).get("uid", "")
    snap_nodes = actor_json.get("status", {}).get("localSnapshotInfo", {}).get("nodeVmsWithLocalSnapshots", [])
    print(f"Paused {actor_migrating} (UID {actor_uid}) localSnapshotInfo nodes: {snap_nodes}")
    if snap_nodes != [node1]:
        raise RuntimeError(f"Expected paused actor on [{node1}], got {snap_nodes}")

    for disk_name in ["actor-disk-0", "actor-disk-1"]:
        owner = check_disk_attached_to(disk_name)
        print(f"Pre-migration attachment of {disk_name}: {owner}")
        if owner != node1:
            raise RuntimeError(f"Expected {disk_name} on {node1} before migration, got {owner}")

    print(f"\n--- Step 5: Filling {node1}'s single worker with {actor_squatter} ---")
    run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "create", "actor", actor_squatter, "--template=sleep"])
    run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "resume", "actor", actor_squatter])

    print(f"\n--- Step 6: Cordoning {node1}, uncordoning {node2}, and adding a 2nd worker pod on {node2} ---")
    run(["kubectl", "cordon", node1])
    run(["kubectl", "uncordon", node2])
    run(
        [
            "kubectl",
            "patch",
            "workerpool",
            "benchmark-ateom",
            "-n",
            "benchmark-workloads",
            "--type=merge",
            "-p",
            '{"spec":{"replicas":2}}',
        ]
    )
    wait_for_ready_pod_on_node(node2)
    run(["kubectl", "uncordon", node1])
    time.sleep(3)

    print(f"\n--- Step 7: Resuming {actor_migrating} (triggering cross-node disk detach/attach to {node2}) ---")
    t0 = time.time()
    run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "resume", "actor", actor_migrating])
    elapsed = time.time() - t0
    print(f"ResumeActor completed in {elapsed:.2f}s")

    migrated_disks = []
    for disk_name in ["actor-disk-0", "actor-disk-1"]:
        owner_after = check_disk_attached_to(disk_name)
        print(f"Post-resume GCE attachment of {disk_name}: {owner_after}")
        if owner_after == node2:
            migrated_disks.append(disk_name)

    info_after = run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "get", "actor", actor_migrating, "-o", "json"])
    actor_after = json.loads(info_after.stdout)["actors"][0]
    new_snap_nodes = actor_after.get("status", {}).get("localSnapshotInfo", {}).get("nodeVmsWithLocalSnapshots", [])
    print(f"Post-resume {actor_migrating} localSnapshotInfo nodes: {new_snap_nodes}")

    cleanup_e2e_actors()

    if not migrated_disks:
        raise RuntimeError(f"Expected an actor disk to be attached to {node2}, but none were found on {node2}")
    if new_snap_nodes != [node2]:
        raise RuntimeError(f"Expected localSnapshotInfo nodes [{node2}], got {new_snap_nodes}")

    print(f"\n=== SUCCESS: Cross-node disk detachment and attachment verified end-to-end! Migrated disk(s): {migrated_disks} ===")


if __name__ == "__main__":
    main()
