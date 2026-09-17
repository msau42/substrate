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

"""Benchmark for Pause/Resume latency during cross-node actor overcommit scenarios.

Measures:
1. PauseActor latency (local checkpoint to dedicated Hyperdisk)
2. ResumeActor (Same-Node Locality Hit) latency when home node has free capacity
3. ResumeActor (Cross-Node Overcommit Migration) latency when home node is full
   and actor must detach its Hyperdisk from oldNode and attach to newNode
4. Server-side breakdown of Cross-Node Migration:
   - ExportActorDisk (unmount + GCE detachDisk)
   - ImportActorDisk (GCE attachDisk + udev wait + mount)
   - AteomHerder/Restore (gVisor restore from newly attached disk)
5. Stochastic Overcommit Pool simulation (A actors competing for W workers across 2 nodes)
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone

PROJECT_ID = os.environ.get("PROJECT_ID", "msau-gke-dev")
ZONE = os.environ.get("CLUSTER_LOCATION", "us-central1-a")


def run(cmd: list[str], check: bool = True, verbose: bool = True) -> subprocess.CompletedProcess:
    if verbose:
        print(f"+ {' '.join(cmd)}")
    return subprocess.run(cmd, text=True, capture_output=True, check=check)


import concurrent.futures


def get_substrate_nodes(nodepool: str = "") -> list[str]:
    if nodepool:
        run(["kubectl", "uncordon", "-l", f"cloud.google.com/gke-nodepool={nodepool}"], check=False, verbose=False)
    cmd = ["kubectl", "get", "nodes"]
    if nodepool:
        cmd.extend(["-l", f"cloud.google.com/gke-nodepool={nodepool}"])
    cmd.extend(["-o", "json"])
    p = run(cmd, verbose=False)
    items = json.loads(p.stdout).get("items", [])
    nodes = []
    for item in items:
        if item.get("spec", {}).get("unschedulable"):
            continue
        name = item.get("metadata", {}).get("name", "")
        if name:
            nodes.append(name)
    if len(nodes) < 2:
        raise RuntimeError(f"Expected at least 2 schedulable substrate nodes, got: {nodes}")
    return sorted(nodes)


def cleanup_benchmark_actors():
    run(
        [
            "kubectl",
            "exec",
            "-n",
            "ate-system",
            "postgres-0",
            "-c",
            "postgres",
            "--",
            "psql",
            "-U",
            "postgres",
            "-d",
            "atepg",
            "-c",
            "DELETE FROM worker_assignments WHERE actor_uid IN (SELECT uid FROM actors WHERE name LIKE 'overcommit-%' OR name LIKE 'e2e-%'); DELETE FROM actors WHERE name LIKE 'overcommit-%' OR name LIKE 'e2e-%';",
        ],
        check=False,
        verbose=False,
    )


def wait_for_node_proactive_trim(node: str, target_max: int = 19, timeout_sec: int = 75, skip: bool = False):
    """Wait for atelet proactive background eviction to trim attached pool disks to <= target_max."""
    if skip:
        return
    t0 = time.time()
    while time.time() - t0 < timeout_sec:
        res = run(
            [
                "gcloud",
                "compute",
                "instances",
                "describe",
                node,
                f"--zone={ZONE}",
                f"--project={PROJECT_ID}",
                "--format=value(disks[].deviceName)",
            ],
            check=False,
            verbose=False,
        )
        if res.returncode == 0:
            dev_names = [d.strip() for d in res.stdout.strip().split(";") if d.strip()]
            cnt = sum(1 for d in dev_names if d.startswith("actor-disk-"))
            if cnt <= target_max:
                return
        time.sleep(1.5)


def wait_for_ready_pod_on_node(node: str):
    for _ in range(45):
        p = run(
            [
                "kubectl",
                "get",
                "pods",
                "-n",
                "benchmark-workloads",
                "-l",
                "ate.dev/worker-pool=benchmark-ateom",
                "-o",
                "json",
            ],
            check=False,
            verbose=False,
        )
        if p.returncode == 0:
            pods = json.loads(p.stdout).get("items", [])
            ready_pods = [
                pod
                for pod in pods
                if pod.get("spec", {}).get("nodeName") == node
                and pod.get("status", {}).get("phase") == "Running"
                and pod.get("status", {}).get("deletionTimestamp") is None
                and all(cs.get("ready") for cs in pod.get("status", {}).get("containerStatuses", [{}]))
            ]
            if ready_pods:
                time.sleep(2)
                return ready_pods[0]["metadata"]["name"]
        time.sleep(2)
    raise RuntimeError(f"Timed out waiting for Ready worker pod on {node}")


def setup_single_worker_on_node1(node1: str, node2: str):
    """Ensure exactly 1 worker pod on node1 and 0 on node2."""
    run(["kubectl", "cordon", node2], verbose=False)
    run(["kubectl", "uncordon", node1], verbose=False)
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
        ],
        verbose=False,
    )
    run(
        ["kubectl", "delete", "pods", "-n", "benchmark-workloads", "-l", "ate.dev/worker-pool=benchmark-ateom"],
        verbose=False,
    )
    pod1 = wait_for_ready_pod_on_node(node1)
    print(f"Worker 1 Ready on {node1}: {pod1}")


def add_second_worker_on_node2(node1: str, node2: str):
    """Add a 2nd worker pod on node2 while keeping the 1st worker pod on node1."""
    run(["kubectl", "cordon", node1], verbose=False)
    run(["kubectl", "uncordon", node2], verbose=False)
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
        ],
        verbose=False,
    )
    pod2 = wait_for_ready_pod_on_node(node2)
    print(f"Worker 2 Ready on {node2}: {pod2}")
    run(["kubectl", "uncordon", node1], verbose=False)
    time.sleep(2)


def setup_two_node_topology(node1: str, node2: str, disks_per_node: int, throughput: int, skip_disk_setup: bool, nodepool: str = ""):
    """Provision disks across both nodes if needed."""
    run(["kubectl", "uncordon", node1, node2], verbose=False)
    cleanup_benchmark_actors()

    run(
        [
            "kubectl",
            "patch",
            "workerpool",
            "benchmark-ateom",
            "-n",
            "benchmark-workloads",
            "--type=json",
            "-p=[{\"op\": \"remove\", \"path\": \"/spec/template/nodeSelector\"}]",
        ],
        check=False,
        verbose=False,
    )

    if not skip_disk_setup:
        if disks_per_node == 0:
            print("\n--- Configuring Arch 1 Mode (Shared Node Boot Disk, 0 Dedicated Hyperdisks) ---")
            unmount_script = """
set -e
for d in /var/lib/ateom-gvisor/disk-pool/*; do
    if [ -d "$d" ]; then
        umount -lf "$d" || true
        rm -rf "$d" || true
    fi
done
rm -f /var/lib/ateom-gvisor/disk-pool/.detached-pool.json
rm -rf /var/lib/ateom-gvisor/actors/*
"""
            for n in [node1, node2]:
                run(
                    [
                        "kubectl",
                        "debug",
                        f"node/{n}",
                        "--image=alpine",
                        "--profile=sysadmin",
                        "-i",
                        "--",
                        "chroot",
                        "/host",
                        "bash",
                        "-c",
                        unmount_script,
                    ],
                    check=False,
                    verbose=False,
                )
            p_ds = run(
                ["kubectl", "get", "ds", "-n", "ate-system", "-l", "app=atelet", "-o", "jsonpath={.items[0].metadata.name}"],
                check=False,
                verbose=False,
            )
            ds_name = p_ds.stdout.strip()
            if ds_name:
                p_args = run(
                    ["kubectl", "get", "ds", "-n", "ate-system", ds_name, "-o", "jsonpath={.spec.template.spec.containers[0].args}"],
                    check=False,
                    verbose=False,
                )
                if p_args.returncode == 0 and p_args.stdout.strip():
                    args_list = json.loads(p_args.stdout)
                    new_args = [a for a in args_list if not a.startswith("--actor-disk-pool-dir")]
                    new_args.append("--actor-disk-pool-dir=")
                    run(
                        [
                            "kubectl",
                            "patch",
                            "ds",
                            "-n",
                            "ate-system",
                            ds_name,
                            "--type=json",
                            f"-p=[{{\"op\": \"replace\", \"path\": \"/spec/template/spec/containers/0/args\", \"value\": {json.dumps(new_args)}}}]",
                        ]
                    )
        else:
            p_ds = run(
                ["kubectl", "get", "ds", "-n", "ate-system", "-l", "app=atelet", "-o", "jsonpath={.items[0].metadata.name}"],
                check=False,
                verbose=False,
            )
            ds_name = p_ds.stdout.strip()
            if ds_name:
                p_args = run(
                    ["kubectl", "get", "ds", "-n", "ate-system", ds_name, "-o", "jsonpath={.spec.template.spec.containers[0].args}"],
                    check=False,
                    verbose=False,
                )
                if p_args.returncode == 0 and p_args.stdout.strip():
                    args_list = json.loads(p_args.stdout)
                    new_args = [a for a in args_list if not a.startswith("--actor-disk-pool-dir")]
                    new_args.append("--actor-disk-pool-dir=/var/lib/ateom-gvisor/disk-pool")
                    run(
                        [
                            "kubectl",
                            "patch",
                            "ds",
                            "-n",
                            "ate-system",
                            ds_name,
                            "--type=json",
                            f"-p=[{{\"op\": \"replace\", \"path\": \"/spec/template/spec/containers/0/args\", \"value\": {json.dumps(new_args)}}}]",
                        ]
                    )
            print(f"\n--- Provisioning {disks_per_node} dedicated Hyperdisks per node ({throughput} MiB/s) ---")
            pool_cmd = [
                "python3",
                "benchmarking/scripts/disk-arch/setup_disk_pool.py",
                "--count",
                str(disks_per_node),
                "--throughput",
                str(throughput),
            ]
            if nodepool:
                pool_cmd.extend(["--nodepool", nodepool])
            run(pool_cmd)
        print("Restarting atelet DaemonSet...")
        run(["kubectl", "rollout", "restart", "ds", "-n", "ate-system", "-l", "app=atelet"])
        run(["kubectl", "rollout", "status", "ds", "-n", "ate-system", "-l", "app=atelet", "--timeout=120s"])
    elif disks_per_node > 0:
        print("Clearing .actor-uid markers on mounted pool disks and restarting atelet...")
        clear_script = "rm -f /var/lib/ateom-gvisor/disk-pool/*/.actor-uid && rm -rf /var/lib/ateom-gvisor/actors/*"
        for n in [node1, node2]:
            run(
                [
                    "kubectl",
                    "debug",
                    f"node/{n}",
                    "--image=alpine",
                    "--profile=sysadmin",
                    "-i",
                    "--",
                    "chroot",
                    "/host",
                    "bash",
                    "-c",
                    clear_script,
                ],
                check=False,
                verbose=False,
            )
        run(["kubectl", "rollout", "restart", "ds", "-n", "ate-system", "-l", "app=atelet"])
        run(["kubectl", "rollout", "status", "ds", "-n", "ate-system", "-l", "app=atelet", "--timeout=120s"])


def get_actor_snapshot_node(actor_name: str) -> str:
    p = run(
        ["./bin/kubectl-ate", "-a", "benchmark-workloads", "get", "actor", actor_name, "-o", "json"],
        check=False,
        verbose=False,
    )
    if p.returncode != 0 or not p.stdout.strip():
        return ""
    try:
        actor = json.loads(p.stdout)["actors"][0]
        nodes = actor.get("status", {}).get("localSnapshotInfo", {}).get("nodeVmsWithLocalSnapshots", [])
        return nodes[0] if nodes else ""
    except Exception:
        return ""


def get_actor_worker_node(actor_name: str) -> str:
    p = run(
        ["./bin/kubectl-ate", "-a", "benchmark-workloads", "get", "actor", actor_name, "-o", "json"],
        check=False,
        verbose=False,
    )
    if p.returncode != 0 or not p.stdout.strip():
        return ""
    try:
        actor = json.loads(p.stdout)["actors"][0]
        pod_name = actor.get("status", {}).get("workerAssignment", {}).get("workerPod", "")
        if not pod_name:
            return ""
        p2 = run(
            [
                "kubectl",
                "get",
                "pod",
                pod_name,
                "-n",
                "benchmark-workloads",
                "-o",
                "jsonpath={.spec.nodeName}",
            ],
            check=False,
            verbose=False,
        )
        return p2.stdout.strip()
    except Exception:
        return ""


def parse_duration_str(s: str) -> float:
    """Parse Go duration string (e.g. '1m1.351166939s', '5.001752293s', or '863.424161ms') to milliseconds."""
    if not s:
        return 0.0
    if s.endswith("ms"):
        return float(s[:-2])
    if s.endswith("µs") or s.endswith("us"):
        return float(s[:-2]) / 1000.0
    if s.endswith("ns"):
        return float(s[:-2]) / 1e6
    if s.endswith("s"):
        body = s[:-1]
        if "m" in body:
            mins, secs = body.split("m", 1)
            return (float(mins) * 60.0 + float(secs)) * 1000.0
        return float(body) * 1000.0
    return 0.0


def fetch_atelet_migration_breakdown(since_time: str) -> dict[str, list[float]]:
    """Fetch ExportActorDisk, ImportActorDisk, and Restore RPC durations from atelet logs."""
    export_ms = []
    import_ms = []
    restore_ms = []

    # Query Cloud Logging first so container stdout log rotation on 5-minute runs does not truncate samples
    query = f'''
resource.type="k8s_container"
resource.labels.namespace_name="ate-system"
resource.labels.container_name="atelet"
timestamp >= "{since_time}"
jsonPayload.method:("/atelet.AteomHerder/ImportActorDisk" OR "/atelet.AteomHerder/ExportActorDisk" OR "/atelet.AteomHerder/Restore")
-jsonPayload."elapsed-time":"µs"
'''
    p_gcloud = run(["gcloud", "logging", "read", query, "--format=json", "--limit=2000"], check=False, verbose=False)
    if p_gcloud.returncode == 0 and p_gcloud.stdout.strip().startswith("["):
        try:
            entries = json.loads(p_gcloud.stdout)
            for e in entries:
                payload = e.get("jsonPayload", {})
                if payload.get("err") is not None:
                    continue
                method = payload.get("method", "")
                elapsed = parse_duration_str(payload.get("elapsed-time", ""))
                if method == "/atelet.AteomHerder/ExportActorDisk":
                    export_ms.append(elapsed)
                elif method == "/atelet.AteomHerder/ImportActorDisk":
                    import_ms.append(elapsed)
                elif method == "/atelet.AteomHerder/Restore":
                    req = payload.get("req", {})
                    if req.get("type") in (1, 2, "SNAPSHOT_TYPE_LOCAL", "SNAPSHOT_TYPE_EXTERNAL"):
                        restore_ms.append(elapsed)
            if import_ms or restore_ms:
                return {
                    "export_ms": export_ms,
                    "import_ms": import_ms,
                    "restore_ms": restore_ms,
                }
        except Exception:
            pass

    p = run(
        ["kubectl", "logs", "-n", "ate-system", "-l", "app=atelet", "--tail=-1", f"--since-time={since_time}"],
        check=False,
        verbose=False,
    )
    for line in p.stdout.splitlines():
        if not line.startswith("{"):
            continue
        try:
            entry = json.loads(line)
        except Exception:
            continue
        method = entry.get("method", "")
        elapsed = entry.get("elapsed-time", "")
        if method == "/atelet.AteomHerder/ExportActorDisk" and entry.get("err") is None:
            export_ms.append(parse_duration_str(elapsed))
        elif method == "/atelet.AteomHerder/ImportActorDisk" and entry.get("err") is None:
            import_ms.append(parse_duration_str(elapsed))
        elif method == "/atelet.AteomHerder/Restore" and entry.get("err") is None:
            req = entry.get("req", {})
            if req.get("type") in (1, 2, "SNAPSHOT_TYPE_LOCAL", "SNAPSHOT_TYPE_EXTERNAL"):
                restore_ms.append(parse_duration_str(elapsed))
    return {
        "export_ms": export_ms,
        "import_ms": import_ms,
        "restore_ms": restore_ms,
    }


def pct(vals: list[float], q: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    idx = min(int(len(s) * q), len(s) - 1)
    return s[idx]


def format_stats(vals: list[float]) -> str:
    if not vals:
        return "N/A"
    avg = statistics.mean(vals)
    med = statistics.median(vals)
    p90 = pct(vals, 0.90)
    p95 = pct(vals, 0.95)
    return f"**{med:.0f} ms** med / **{avg:.0f} ms** avg / **{p90:.0f} ms** p90 / **{p95:.0f} ms** p95 (n={len(vals)})"


def run_controlled_overcommit_benchmark(node1: str, node2: str, iterations: int) -> dict:
    """Execute controlled overcommit cycles measuring Same-Node Hit vs Cross-Node Migration."""
    print(f"\n=== Running Controlled Overcommit Benchmark ({iterations} cycles) ===")
    setup_single_worker_on_node1(node1, node2)
    start_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    actor_roamer = f"overcommit-roam-{uuid.uuid4().hex[:6]}"
    actor_blocker1 = f"overcommit-block1-{uuid.uuid4().hex[:6]}"
    actor_blocker2 = f"overcommit-block2-{uuid.uuid4().hex[:6]}"

    pause_latencies = []
    same_node_resume_latencies = []
    cross_node_resume_latencies = []

    for a in [actor_roamer, actor_blocker1, actor_blocker2]:
        run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "create", "actor", a, "--template=sleep"], verbose=False)

    # Boot and pause roamer & blocker1 on node1 (only worker pod in cluster is on node1)
    run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "resume", "actor", actor_roamer], verbose=False)
    t0 = time.time()
    run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "pause", "actor", actor_roamer], verbose=False)
    pause_latencies.append((time.time() - t0) * 1000.0)

    # Resume blocker1 on node1 and KEEP IT RUNNING while we add node2's worker and boot blocker2
    run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "resume", "actor", actor_blocker1], verbose=False)
    add_second_worker_on_node2(node1, node2)

    # Since blocker1 occupies node1, blocker2 is guaranteed to schedule onto node2
    run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "resume", "actor", actor_blocker2], verbose=False)
    run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "pause", "actor", actor_blocker2], verbose=False)
    run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "pause", "actor", actor_blocker1], verbose=False)

    print(
        f"Initialized home nodes: {actor_roamer} ({get_actor_snapshot_node(actor_roamer)}), "
        f"{actor_blocker1} ({get_actor_snapshot_node(actor_blocker1)}), "
        f"{actor_blocker2} ({get_actor_snapshot_node(actor_blocker2)})"
    )

    for i in range(iterations):
        print(f"\n--- Cycle {i + 1}/{iterations} ---")
        home_node = get_actor_snapshot_node(actor_roamer)
        other_node = node2 if home_node == node1 else node1

        # 1. Measure Same-Node Locality Hit:
        # Both nodes are currently IDLE. Resume roamer -> prefers home_node!
        t0 = time.time()
        run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "resume", "actor", actor_roamer], verbose=False)
        elapsed_ms = (time.time() - t0) * 1000.0
        actual_node = get_actor_worker_node(actor_roamer)
        print(f"[Same-Node Hit] Roamer resumed on {actual_node} (home={home_node}) in {elapsed_ms:.1f} ms")
        same_node_resume_latencies.append(elapsed_ms)

        # Pause roamer on home_node
        t0 = time.time()
        run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "pause", "actor", actor_roamer], verbose=False)
        p_ms = (time.time() - t0) * 1000.0
        pause_latencies.append(p_ms)
        print(f"[Pause] Roamer paused on {home_node} in {p_ms:.1f} ms")

        # 2. Measure Cross-Node Overcommit Migration:
        # Occupy home_node's worker pod by resuming blocker whose snapshot is on home_node
        blocker = actor_blocker1 if home_node == node1 else actor_blocker2
        run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "resume", "actor", blocker], verbose=False)
        blocker_node = get_actor_worker_node(blocker)
        print(f"Occupied {blocker_node} with {blocker}; {other_node} has 1 free worker.")

        # Now resume roamer -> home_node is full, forcing cross-node disk migration to other_node!
        t0 = time.time()
        run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "resume", "actor", actor_roamer], verbose=False)
        mig_ms = (time.time() - t0) * 1000.0
        new_node = get_actor_worker_node(actor_roamer)
        print(
            f"[Cross-Node Migration] Roamer migrated {home_node} -> {new_node} in {mig_ms:.1f} ms"
        )
        cross_node_resume_latencies.append(mig_ms)

        # Pause roamer (now on new_node) and pause blocker
        t0 = time.time()
        run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "pause", "actor", actor_roamer], verbose=False)
        pause_latencies.append((time.time() - t0) * 1000.0)
        run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "pause", "actor", blocker], verbose=False)

    breakdown = fetch_atelet_migration_breakdown(start_ts)
    cleanup_benchmark_actors()

    return {
        "pause_ms": pause_latencies,
        "same_node_resume_ms": same_node_resume_latencies,
        "cross_node_resume_ms": cross_node_resume_latencies,
        "export_ms": breakdown["export_ms"],
        "import_ms": breakdown["import_ms"],
        "restore_ms": breakdown["restore_ms"],
    }


def wait_for_ready_pods_on_node(node: str, expected_count: int):
    print(f"Waiting for {expected_count} ready worker pods on {node}...")
    for _ in range(90):
        p = run(
            [
                "kubectl",
                "get",
                "pods",
                "-n",
                "benchmark-workloads",
                "-l",
                "ate.dev/worker-pool=benchmark-ateom",
                "-o",
                "json",
            ],
            check=False,
            verbose=False,
        )
        if p.returncode == 0:
            pods = json.loads(p.stdout).get("items", [])
            ready_pods = [
                pod
                for pod in pods
                if pod.get("spec", {}).get("nodeName") == node
                and pod.get("status", {}).get("phase") == "Running"
                and pod.get("status", {}).get("deletionTimestamp") is None
                and all(cs.get("ready") for cs in pod.get("status", {}).get("containerStatuses", [{}]))
            ]
            if len(ready_pods) >= expected_count:
                time.sleep(2)
                return
        time.sleep(2)
    raise RuntimeError(f"Timed out waiting for {expected_count} ready worker pods on {node}")


def scale_worker_pool(replicas: int):
    run(
        [
            "kubectl",
            "patch",
            "workerpool",
            "-n",
            "benchmark-workloads",
            "benchmark-ateom",
            "--type=merge",
            "-p",
            json.dumps({"spec": {"replicas": replicas}}),
        ],
        verbose=False,
    )
def run_stochastic_overcommit_benchmark(
    node1: str,
    node2: str,
    num_workers: int,
    num_actors: int,
    duration_sec: int,
    use_suspend: bool = False,
) -> dict:
    """Run A concurrent actor threads competing for W workers across 2 nodes."""
    workers_per_node = num_workers // 2
    mid = num_actors // 2
    idle_verb = "suspend" if use_suspend else "pause"
    print(
        f"\n=== Running Stochastic Multi-Actor Overcommit Pool Benchmark ({num_actors} Actors on {num_workers} Workers [{workers_per_node}/node] across 2 Nodes, {duration_sec}s, idle={idle_verb}) ==="
    )
    cleanup_benchmark_actors()

    proxy_proc = subprocess.Popen(
        ["./bin/kubectl-ate", "proxy", "--port", "18080"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for _ in range(40):
        try:
            if urllib.request.urlopen("http://127.0.0.1:18080/healthz", timeout=1).status == 200:
                break
        except Exception:
            time.sleep(0.25)

    def _proxy_post(action: str, actor_name: str, timeout: float = 120.0) -> dict:
        data = json.dumps({"atespace": "benchmark-workloads", "name": actor_name}).encode("utf-8")
        req = urllib.request.Request(
            f"http://127.0.0.1:18080/{action}",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _proxy_get_actor(actor_name: str, timeout: float = 15.0) -> dict:
        req = urllib.request.Request(
            f"http://127.0.0.1:18080/actor?atespace=benchmark-workloads&name={actor_name}",
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    try:
        # 1. Create all actors in parallel
        actors = [f"overcommit-pool-{i}-{uuid.uuid4().hex[:4]}" for i in range(num_actors)]
        print(f"Creating {num_actors} actors in parallel...")

        def _create(a: str):
            run(["./bin/kubectl-ate", "-a", "benchmark-workloads", "create", "actor", a, "--template=sleep"], verbose=False)

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
            list(ex.map(_create, actors))

        # 2. Scale to workers_per_node on node1 only
        print(f"Scaling worker pool to {workers_per_node} workers on {node1}...")
        run(["kubectl", "cordon", node2], verbose=False)
        run(["kubectl", "uncordon", node1], verbose=False)
        scale_worker_pool(0)
        time.sleep(4)
        scale_worker_pool(workers_per_node)
        wait_for_ready_pods_on_node(node1, workers_per_node)

        def _resume_retry(a: str):
            for attempt in range(6):
                try:
                    _proxy_post("resume", a)
                    return
                except Exception as exc:
                    time.sleep(1.5)
            raise RuntimeError(f"Failed to resume {a}")

        def _pause_retry(a: str):
            for attempt in range(6):
                try:
                    _proxy_post(idle_verb, a)
                    return
                except Exception as exc:
                    time.sleep(1.5)
            raise RuntimeError(f"Failed to {idle_verb} {a}")

        # 3. Initialize first half of actors (actors[:mid]) on node1 in adaptive waves
        print(f"Initializing {mid} actors on {node1}...")
        wave_idx = 0
        while wave_idx < mid:
            cur_wave_size = workers_per_node if use_suspend else (3 if wave_idx >= 25 else min(workers_per_node, 10))
            wave = actors[wave_idx : min(wave_idx + cur_wave_size, mid)]
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(wave)) as ex:
                list(ex.map(_resume_retry, wave))
            time.sleep(0.5)
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(wave)) as ex:
                list(ex.map(_pause_retry, wave))
            wait_for_node_proactive_trim(node1, target_max=29, skip=use_suspend)
            print(f"  Initialized actors {wave_idx}..{wave_idx + len(wave) - 1} on {node1}")
            wave_idx += len(wave)

        # 4. Occupy all workers on node1 using the most recently paused (warm mounted) actors
        print(f"Occupying {workers_per_node} workers on {node1} to isolate {node2} initialization...")
        blockers_node1 = actors[mid - workers_per_node : mid]
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(blockers_node1)) as ex:
            list(ex.map(_resume_retry, blockers_node1))

        # 5. Scale up to num_workers by adding workers_per_node on node2
        print(f"Adding {workers_per_node} workers on {node2} (total {num_workers} workers)...")
        run(["kubectl", "uncordon", node2], verbose=False)
        run(["kubectl", "cordon", node1], verbose=False)
        scale_worker_pool(num_workers)
        wait_for_ready_pods_on_node(node2, workers_per_node)

        # 6. Initialize second half of actors (actors[mid:]) on node2 in adaptive waves
        print(f"Initializing {num_actors - mid} actors on {node2}...")
        wave_idx = mid
        while wave_idx < num_actors:
            local_idx = wave_idx - mid
            cur_wave_size = workers_per_node if use_suspend else (3 if local_idx >= 25 else min(workers_per_node, 10))
            wave = actors[wave_idx : min(wave_idx + cur_wave_size, num_actors)]
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(wave)) as ex:
                list(ex.map(_resume_retry, wave))
            time.sleep(0.5)
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(wave)) as ex:
                list(ex.map(_pause_retry, wave))
            wait_for_node_proactive_trim(node2, target_max=29, skip=use_suspend)
            print(f"  Initialized actors {wave_idx}..{wave_idx + len(wave) - 1} on {node2}")
            wave_idx += len(wave)

        # 7. Pause node1 blockers and uncordon node1
        print(f"Releasing {node1} blockers and uncordoning all nodes...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(blockers_node1)) as ex:
            list(ex.map(_pause_retry, blockers_node1))
        wait_for_node_proactive_trim(node1, target_max=29, skip=use_suspend)
        wait_for_node_proactive_trim(node2, target_max=29, skip=use_suspend)
        run(["kubectl", "uncordon", node1], verbose=False)
        time.sleep(1.0)

        # Build static pod_to_node map so we never call kubectl get pod during steady-state
        pod_to_node = {}
        p_pods = run(
            ["kubectl", "get", "pods", "-n", "benchmark-workloads", "-l", "ate.dev/worker-pool=benchmark-ateom", "-o", "json"],
            check=False,
            verbose=False,
        )
        if p_pods.returncode == 0:
            for item in json.loads(p_pods.stdout).get("items", []):
                pname = item.get("metadata", {}).get("name", "")
                nname = item.get("spec", {}).get("nodeName", "")
                if pname and nname:
                    pod_to_node[pname] = nname

        print(
            f"Initialization complete: {mid} actors {idle_verb}ed on {node1}, {num_actors - mid} actors {idle_verb}ed on {node2}; {num_workers} idle workers ready."
        )
        start_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        lock = threading.Lock()
        pause_ms = []
        same_node_ms = []
        detached_import_ms = []
        cross_node_ms = []
        stop_event = threading.Event()
        last_worker_node = {a: (node1 if i < mid else node2) for i, a in enumerate(actors)}

        # Gate concurrency to at most num_workers active actors at a time
        worker_sem = threading.Semaphore(num_workers)

        def actor_loop(actor_name: str, idx: int):
            # Stagger initial start slightly across actors
            time.sleep((idx % 20) * 0.12)
            while not stop_event.is_set():
                if not worker_sem.acquire(timeout=1.0):
                    continue
                try:
                    if stop_event.is_set():
                        break
                    prev_node = ""
                    if not use_suspend:
                        try:
                            info = _proxy_get_actor(actor_name)
                            snodes = info.get("snapshot_nodes") or []
                            prev_node = snodes[0] if snodes else ""
                        except Exception:
                            pass

                    try:
                        res_data = _proxy_post("resume", actor_name)
                        r_ms = res_data.get("elapsed_ms", 0.0)
                        pod_name = res_data.get("worker_pod", "")
                        cur_node = pod_to_node.get(pod_name, "")
                        if not cur_node and pod_name:
                            cur_node = get_actor_worker_node(actor_name)
                            if cur_node:
                                pod_to_node[pod_name] = cur_node
                        prev_host = last_worker_node.get(actor_name, "")
                        if cur_node:
                            last_worker_node[actor_name] = cur_node
                        with lock:
                            if use_suspend:
                                if prev_host != "" and cur_node != "" and prev_host != cur_node:
                                    cross_node_ms.append(r_ms)
                                    print(f"  [Actor {idx:02d}] CROSS-NODE (GCS) {prev_host[-4:]} -> {cur_node[-4:]}: {r_ms:.0f} ms")
                                else:
                                    same_node_ms.append(r_ms)
                                    if len(same_node_ms) % 10 == 1:
                                        print(f"  [Actor {idx:02d}] SAME-NODE (GCS) on {cur_node[-4:]}: {r_ms:.0f} ms (total same-node={len(same_node_ms)})")
                            elif prev_node.startswith("detached."):
                                detached_import_ms.append(r_ms)
                                loc_tag = "same-node" if prev_host == cur_node else f"{prev_host[-4:]}->{cur_node[-4:]}"
                                print(f"  [Actor {idx:02d}] DETACHED-IMPORT ({loc_tag}) on {cur_node[-4:]}: {r_ms:.0f} ms")
                            elif prev_node != "" and cur_node != "" and prev_node != cur_node:
                                cross_node_ms.append(r_ms)
                                print(f"  [Actor {idx:02d}] LIVE-MIGRATED {prev_node[-4:]} -> {cur_node[-4:]}: {r_ms:.0f} ms")
                            else:
                                same_node_ms.append(r_ms)
                                if len(same_node_ms) % 10 == 1:
                                    print(f"  [Actor {idx:02d}] WARM SAME-NODE on {cur_node[-4:]}: {r_ms:.0f} ms (total hits={len(same_node_ms)})")

                        # Active work duration while holding worker
                        time.sleep(0.6)

                        p_data = _proxy_post(idle_verb, actor_name)
                        p_dur = p_data.get("elapsed_ms", 0.0)
                        with lock:
                            pause_ms.append(p_dur)
                    except Exception as exc:
                        pass
                finally:
                    worker_sem.release()

                # Idle think time before next wake-up
                time.sleep(0.5 + ((idx % 10) * 0.15))

        threads = [threading.Thread(target=actor_loop, args=(a, idx)) for idx, a in enumerate(actors)]
        for t in threads:
            t.start()

        time.sleep(duration_sec)
        print("Stopping stochastic overcommit pool benchmark...")
        stop_event.set()
        for t in threads:
            t.join()

        breakdown = fetch_atelet_migration_breakdown(start_ts)
        cleanup_benchmark_actors()
        return {
            "pause_ms": pause_ms,
            "same_node_resume_ms": same_node_ms,
            "detached_import_ms": detached_import_ms,
            "cross_node_resume_ms": cross_node_ms,
            "export_ms": breakdown["export_ms"],
            "import_ms": breakdown["import_ms"],
            "restore_ms": breakdown["restore_ms"],
        }
    finally:
        proxy_proc.terminate()
        try:
            proxy_proc.wait(timeout=5)
        except Exception:
            proxy_proc.kill()


def print_summary_report(ctrl: dict, stoch: dict | None):
    print("\n" + "=" * 80)
    print("OVERCOMMIT CROSS-NODE DISK MIGRATION BENCHMARK RESULTS")
    print("=" * 80)

    if ctrl and ctrl.get("same_node_resume_ms"):
        print("\n### 1. Controlled Overcommit Latency & Server Breakdown")
        print(f"- **PauseActor (Local Hyperdisk Checkpoint)**: {format_stats(ctrl['pause_ms'])}")
        print(f"- **ResumeActor — Same-Node Locality Hit**:   {format_stats(ctrl['same_node_resume_ms'])}")
        print(f"- **ResumeActor — Cross-Node Migration**:     {format_stats(ctrl['cross_node_resume_ms'])}")
        print("\n#### Cross-Node Migration Server-Side Sub-Step Breakdown (`atelet` RPCs):")
        print(f"  1. **`ExportActorDisk` (Unmount + GCE `detachDisk`)**: {format_stats(ctrl['export_ms'])}")
        print(f"  2. **`ImportActorDisk` (GCE `attachDisk` + Mount)**:   {format_stats(ctrl['import_ms'])}")
        print(f"  3. **`AteomHerder/Restore` (gVisor Checkpoint Restore)**: {format_stats(ctrl['restore_ms'])}")

    if stoch:
        warm_hits = len(stoch["same_node_resume_ms"])
        detached_imports = len(stoch.get("detached_import_ms", []))
        live_migs = len(stoch["cross_node_resume_ms"])
        total_resumes = warm_hits + detached_imports + live_migs
        cold_or_mig = stoch.get("detached_import_ms", []) + stoch["cross_node_resume_ms"]
        all_resumes = stoch["same_node_resume_ms"] + cold_or_mig
        print("\n### 2. Stochastic Multi-Actor Overcommit Pool Results")
        print(
            f"- **Total Completed Resume Cycles**: {total_resumes} "
            f"({warm_hits} Same-Node Resumes, {detached_imports} Detached Cold Imports [0 Export RPCs], {live_migs} Cross-Node Resumes)"
        )
        print(f"- **Same-Node `ResumeActor` Latency**:  {format_stats(stoch['same_node_resume_ms'])}")
        if detached_imports > 0:
            print(f"- **Detached Cold Import `ResumeActor` Latency**:    {format_stats(stoch['detached_import_ms'])}")
        if live_migs > 0:
            print(f"- **Cross-Node `ResumeActor` Latency**: {format_stats(stoch['cross_node_resume_ms'])}")
        print(f"- **Combined Cold/Migrated `ResumeActor` Latency**:  {format_stats(cold_or_mig)}")
        print(f"- **Blended Overall `ResumeActor` Latency**:         {format_stats(all_resumes)}")
        print(f"- **Blended `PauseActor`/`SuspendActor` Latency**:   {format_stats(stoch['pause_ms'])}")
        if stoch.get("export_ms") or stoch.get("import_ms") or stoch.get("restore_ms"):
            print("\n#### Server-Side Breakdown of Operations in Stochastic Pool:")
            print(f"  1. **`ExportActorDisk` (Unmount + GCE `detachDisk`)**: {format_stats(stoch['export_ms'])}")
            print(f"  2. **`ImportActorDisk` (GCE `attachDisk` + Mount)**:   {format_stats(stoch['import_ms'])}")
            print(f"  3. **`AteomHerder/Restore` (gVisor Checkpoint Restore)**: {format_stats(stoch['restore_ms'])}")

    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(description="Run overcommit cross-node disk migration benchmark.")
    parser.add_argument("--iterations", type=int, default=3, help="Number of controlled ping-pong migration cycles")
    parser.add_argument("--pool-workers", type=int, default=2, help="Number of worker pods across the 2 nodes")
    parser.add_argument("--pool-actors", type=int, default=4, help="Number of actors in stochastic pool")
    parser.add_argument("--pool-duration", type=int, default=45, help="Duration in seconds for stochastic pool run")
    parser.add_argument("--disks-per-node", type=int, default=3, help="Dedicated disks to provision per node")
    parser.add_argument("--throughput", type=int, default=800, help="Hyperdisk throughput MiB/s")
    parser.add_argument("--nodepool", type=str, default="", help="Optional GKE nodepool label filter")
    parser.add_argument("--skip-setup", action="store_true", help="Skip disk pool re-provisioning")
    parser.add_argument("--use-suspend", action="store_true", help="Use SuspendActor (GCS snapshot) instead of PauseActor")
    parser.add_argument("--mode", choices=["controlled", "stochastic", "both"], default="both")
    args = parser.parse_args()

    nodes = get_substrate_nodes(args.nodepool)
    node1, node2 = nodes[0], nodes[1]
    setup_two_node_topology(node1, node2, args.disks_per_node, args.throughput, args.skip_setup, args.nodepool)

    ctrl_res = {"pause_ms": [], "same_node_resume_ms": [], "cross_node_resume_ms": [], "export_ms": [], "import_ms": [], "restore_ms": []}
    stoch_res = None

    if args.mode in ("controlled", "both"):
        ctrl_res = run_controlled_overcommit_benchmark(node1, node2, args.iterations)

    if args.mode in ("stochastic", "both"):
        stoch_res = run_stochastic_overcommit_benchmark(
            node1, node2, args.pool_workers, args.pool_actors, args.pool_duration, args.use_suspend
        )

    print_summary_report(ctrl_res, stoch_res)


if __name__ == "__main__":
    main()
