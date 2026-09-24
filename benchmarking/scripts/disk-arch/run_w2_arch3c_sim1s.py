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

"""Run W2 Single-Node Benchmark at N=15 (W=17 clean workers) for Arch 3c (Parallel 1s Attach/Detach Simulation)."""

import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[3]
TMPL_PATH = ROOT / "benchmarking/automation/manifests/runner-job.yaml.tmpl"
PROJECT_ID = os.environ.get("PROJECT_ID", "msau-gke-dev")
IMAGE = os.environ.get("BENCHMARK_LOCUST_IMAGE", f"us-docker.pkg.dev/{PROJECT_ID}/gcr.io/ate-images/locust-test:latest")
BUCKET = os.environ.get("BENCHMARK_RESULTS_BUCKET", f"gs://snapshot-substrate-test-{PROJECT_ID}/results")
TAG = os.environ.get("ATE_TAG", "5c49070e")

NODE1 = os.environ.get("ACTIVE_NODE", "gke-ate-bench-substrate-node-pool-c3--75e0a69e-xt9d")
NODE2 = os.environ.get("LOADGEN_NODE", "gke-ate-bench-substrate-node-pool-c3--75e0a69e-0trs")



def run_cmd(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, text=True, capture_output=True, check=check)


def _run_privileged_host_script(script: str):
    pod_name = "node-prep-helper"
    run_cmd(["kubectl", "delete", "pod", pod_name, "-n", "default", "--ignore-not-found=true"], check=False)
    manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": pod_name, "namespace": "default"},
        "spec": {
            "nodeName": NODE1,
            "restartPolicy": "Never",
            "hostPID": True,
            "containers": [{
                "name": "prep",
                "image": "ubuntu:22.04",
                "securityContext": {"privileged": True},
                "command": ["nsenter", "-t", "1", "-m", "-u", "-i", "-n", "--", "bash", "-c", script],
            }],
        },
    }
    subprocess.run(["kubectl", "apply", "-f", "-"], input=json.dumps(manifest), text=True, capture_output=True)
    for _ in range(60):
        res = run_cmd(["kubectl", "get", "pod", pod_name, "-n", "default", "-o", "jsonpath={.status.phase}"], check=False)
        if res.stdout.strip() in ("Succeeded", "Failed"):
            break
        time.sleep(1)
    logs = run_cmd(["kubectl", "logs", pod_name, "-n", "default"], check=False).stdout
    print("Host prep output:\n" + logs.strip())
    run_cmd(["kubectl", "delete", "pod", pod_name, "-n", "default", "--ignore-not-found=true"], check=False)


def _get_ready_workers_on_node1() -> int:
    res = run_cmd(["kubectl", "get", "pods", "-n", "benchmark-workloads", "-l", "ate.dev/worker-pool=benchmark-ateom", "-o", "json"], check=False)
    if res.returncode == 0 and res.stdout.strip():
        items = json.loads(res.stdout).get("items", [])
        ready = [
            i for i in items
            if i.get("spec", {}).get("nodeName") == NODE1
            and i.get("status", {}).get("phase") == "Running"
            and i.get("status", {}).get("deletionTimestamp") is None
            and all(cs.get("ready") for cs in i.get("status", {}).get("containerStatuses", []))
        ]
        return len(ready)
    return 0


def prepare_node1_arch3c_and_workers(atelet_image: str, workers: int, cross_node_pct: float = 100.0):
    """Ensure 17 workers on NODE1, mount 17 attached google-actor-disk-* volumes on NODE1, and restart atelet with cross_node_pct."""
    print(f"Preparing step (cross_node_pct={cross_node_pct}%)...")
    run_cmd(["kubectl", "delete", "jobs", "-n", "benchmarking", "-l", "app=substrate-benchmark-runner"], check=False)

    # Clean up any leftover actors and stale worker assignments in PostgreSQL
    run_cmd([
        "kubectl", "exec", "-n", "ate-system", "postgres-0", "-c", "postgres", "--",
        "psql", "-U", "postgres", "-d", "atepg", "-c",
        "DELETE FROM worker_assignments WHERE actor_uid IN (SELECT uid FROM actors WHERE atespace != 'ate-golden'); DELETE FROM actors WHERE atespace != 'ate-golden';"
    ], check=False)

    print(f"Ensuring all 17 dedicated Hyperdisks are mounted and clean on {NODE1}...")
    script = """
set -e
mkdir -p /var/lib/ateom-gvisor
if ! findmnt -n -o PROPAGATION /var/lib/ateom-gvisor | grep -q shared; then
    mount --bind /var/lib/ateom-gvisor /var/lib/ateom-gvisor || true
    mount --make-rshared /var/lib/ateom-gvisor || true
fi
rm -rf /var/lib/ateom-gvisor/actors/* 2>/dev/null || true
mkdir -p /var/lib/ateom-gvisor/actors
mkdir -p /var/lib/ateom-gvisor/disk-pool
rm -f /var/lib/ateom-gvisor/disk-pool/.detached-pool.json

COUNT=0
for dev in $(ls /dev/disk/by-id/google-actor-disk-* | grep -v part | sort -V); do
    base=$(basename "$dev")
    dname=${base#google-}
    mnt="/var/lib/ateom-gvisor/disk-pool/${dname}"
    mkdir -p "$mnt"
    if ! findmnt -rn "$mnt" >/dev/null 2>&1; then
        mount -o discard,defaults "$dev" "$mnt" 2>/dev/null || true
    fi
    rm -rf "$mnt"/* "$mnt"/.actor-uid "$mnt"/.actor-ref.json 2>/dev/null || true
    cat > "$mnt/.disk-metadata.json" <<EOF
{"gceDiskName": "${dname}", "deviceName": "${dname}"}
EOF
    chmod 755 "$mnt"
    COUNT=$((COUNT + 1))
    if [ "$COUNT" -ge 17 ]; then
        break
    fi
done
echo "Total mounted pool disks: $(df -h | grep -c disk-pool)"
"""
    _run_privileged_host_script(script)

    p_ds = run_cmd(["kubectl", "get", "ds", "-n", "ate-system", "-l", "app=atelet", "-o", "jsonpath={.items[0].metadata.name}"])
    ds_name = p_ds.stdout.strip()
    p_args = run_cmd(["kubectl", "get", "ds", "-n", "ate-system", ds_name, "-o", "jsonpath={.spec.template.spec.containers[0].args}"])
    args_list = json.loads(p_args.stdout)
    new_args = [a for a in args_list if not a.startswith("--actor-disk-pool-dir")]
    new_args.append("--actor-disk-pool-dir=/var/lib/ateom-gvisor/disk-pool")
    patch = [
        {"op": "replace", "path": "/spec/template/spec/containers/0/args", "value": new_args},
        {"op": "replace", "path": "/spec/template/spec/containers/0/image", "value": atelet_image},
    ]
    run_cmd(["kubectl", "patch", "ds", "-n", "ate-system", ds_name, "--type=json", f"-p={json.dumps(patch)}"])
    run_cmd([
        "kubectl", "set", "env", f"ds/{ds_name}", "-n", "ate-system",
        f"ATELET_SIMULATE_CROSS_NODE_PCT={cross_node_pct}",
        f"ATELET_SIMULATE_DISK_OP_MS={1000 if cross_node_pct > 0 else 0}",
        f"ATELET_DETACH_ON_PAUSE={'true' if cross_node_pct >= 100.0 else 'false'}",
        "ATELET_MAX_DISK_OP_QUEUE=64",
    ])
    run_cmd(["kubectl", "rollout", "restart", "ds", "-n", "ate-system", ds_name])
    run_cmd(["kubectl", "rollout", "status", "ds", "-n", "ate-system", ds_name, "--timeout=90s"])

    run_cmd(["kubectl", "taint", "nodes", NODE2, "dedicated=loadgen:NoSchedule-"], check=False)
    run_cmd(["kubectl", "uncordon", NODE1], check=False)
    run_cmd(["kubectl", "uncordon", NODE2], check=False)

    patch_wp = {
        "spec": {
            "replicas": workers,
            "template": {
                "nodeSelector": {
                    "kubernetes.io/hostname": NODE1,
                }
            },
        }
    }
    run_cmd(["kubectl", "patch", "workerpool", "benchmark-ateom", "-n", "benchmark-workloads", "--type=merge", "-p", json.dumps(patch_wp)])
    print(f"Fast-resetting {workers} benchmark-ateom worker pods on {NODE1}...")
    run_cmd(["kubectl", "delete", "pod", "-n", "benchmark-workloads", "-l", "ate.dev/worker-pool=benchmark-ateom", "--grace-period=0", "--force"], check=False)
    for _ in range(90):
        if _get_ready_workers_on_node1() == workers:
            break
        time.sleep(1)
    time.sleep(5)
    print(f"Verified {_get_ready_workers_on_node1()}/{workers} ACTIVE workers on {NODE1}.")


TELEMETRY_SCRIPT = '''
import time, json, sys

def read_all_diskstats():
    boot = {}
    pool_write_sectors = 0
    pool_read_sectors = 0
    pool_writes = 0
    pool_reads = 0
    pool_write_ticks = 0
    pool_read_ticks = 0
    pool_in_flight = 0
    pool_io_ticks = 0
    pool_time_in_queue = 0

    with open("/proc/diskstats") as f:
        for line in f:
            parts = line.split()
            if len(parts) < 14:
                continue
            dev = parts[2]
            if dev == "nvme0n1":
                boot = {
                    "reads": int(parts[3]),
                    "read_sectors": int(parts[5]),
                    "read_ticks": int(parts[6]),
                    "writes": int(parts[7]),
                    "write_sectors": int(parts[9]),
                    "write_ticks": int(parts[10]),
                    "in_flight": int(parts[11]),
                    "io_ticks": int(parts[12]),
                    "time_in_queue": int(parts[13]),
                }
            elif dev.startswith("nvme0n") and "p" not in dev and dev not in ("nvme0n1", "nvme0n2"):
                pool_reads += int(parts[3])
                pool_read_sectors += int(parts[5])
                pool_read_ticks += int(parts[6])
                pool_writes += int(parts[7])
                pool_write_sectors += int(parts[9])
                pool_write_ticks += int(parts[10])
                pool_in_flight += int(parts[11])
                pool_io_ticks += int(parts[12])
                pool_time_in_queue += int(parts[13])

    pool = {
        "reads": pool_reads,
        "read_sectors": pool_read_sectors,
        "read_ticks": pool_read_ticks,
        "writes": pool_writes,
        "write_sectors": pool_write_sectors,
        "write_ticks": pool_write_ticks,
        "in_flight": pool_in_flight,
        "io_ticks": pool_io_ticks,
        "time_in_queue": pool_time_in_queue,
    }
    return boot, pool

def read_stat():
    with open("/proc/stat") as f:
        for line in f:
            if line.startswith("cpu "):
                parts = [int(x) for x in line.split()[1:]]
                total = sum(parts)
                idle = parts[3]
                iowait = parts[4] if len(parts) > 4 else 0
                return {"total": total, "idle": idle, "iowait": iowait}
    return None

def read_net():
    rx, tx = 0, 0
    with open("/proc/net/dev") as f:
        for line in f:
            if ":" in line:
                iface, rest = line.split(":", 1)
                iface = iface.strip()
                if iface.startswith("eth") or iface.startswith("ens"):
                    parts = rest.split()
                    rx += int(parts[0])
                    tx += int(parts[8])
    return {"rx": rx, "tx": tx}

duration = int(sys.argv[1])
prev_b, prev_p = read_all_diskstats()
prev_s = read_stat()
prev_n = read_net()
prev_t = time.time()

samples = []
for _ in range(duration):
    time.sleep(1.0)
    cur_t = time.time()
    dt = cur_t - prev_t
    cur_b, cur_p = read_all_diskstats()
    cur_s = read_stat()
    cur_n = read_net()
    if cur_b and prev_b and dt > 0:
        b_w_mib = ((cur_b["write_sectors"] - prev_b["write_sectors"]) * 512) / (dt * 1024 * 1024)
        b_r_mib = ((cur_b["read_sectors"] - prev_b["read_sectors"]) * 512) / (dt * 1024 * 1024)
        p_w_mib = ((cur_p["write_sectors"] - prev_p["write_sectors"]) * 512) / (dt * 1024 * 1024)
        p_r_mib = ((cur_p["read_sectors"] - prev_p["read_sectors"]) * 512) / (dt * 1024 * 1024)

        tot_w_mib = b_w_mib + p_w_mib
        tot_r_mib = b_r_mib + p_r_mib

        dw_b = cur_b["writes"] - prev_b["writes"]
        dw_p = cur_p["writes"] - prev_p["writes"]
        dr_p = cur_p["reads"] - prev_p["reads"]
        b_await = (cur_b["write_ticks"] - prev_b["write_ticks"]) / dw_b if dw_b > 0 else 0.0
        p_await = (cur_p["write_ticks"] - prev_p["write_ticks"]) / dw_p if dw_p > 0 else 0.0
        p_r_await = (cur_p["read_ticks"] - prev_p["read_ticks"]) / dr_p if dr_p > 0 else 0.0

        b_aqu = (cur_b["time_in_queue"] - prev_b["time_in_queue"]) / (dt * 1000.0)
        p_aqu = (cur_p["time_in_queue"] - prev_p["time_in_queue"]) / (dt * 1000.0)
        tot_aqu = b_aqu + p_aqu

        dtot = cur_s["total"] - prev_s["total"]
        cpu_util = 100.0 * (dtot - (cur_s["idle"] - prev_s["idle"]) - (cur_s["iowait"] - prev_s["iowait"])) / dtot if dtot > 0 else 0.0
        iowait_pct = 100.0 * (cur_s["iowait"] - prev_s["iowait"]) / dtot if dtot > 0 else 0.0

        rx_mib_s = (cur_n["rx"] - prev_n["rx"]) / (dt * 1024 * 1024)
        tx_mib_s = (cur_n["tx"] - prev_n["tx"]) / (dt * 1024 * 1024)

        samples.append({
            "ts": cur_t,
            "tot_write_mib_s": tot_w_mib,
            "boot_write_mib_s": b_w_mib,
            "pool_write_mib_s": p_w_mib,
            "pool_read_mib_s": p_r_mib,
            "tot_read_mib_s": tot_r_mib,
            "boot_w_await_ms": b_await,
            "pool_w_await_ms": p_await,
            "pool_r_await_ms": p_r_await,
            "tot_aqu_sz": tot_aqu,
            "boot_aqu_sz": b_aqu,
            "pool_aqu_sz": p_aqu,
            "cpu_util_pct": cpu_util,
            "iowait_pct": iowait_pct,
            "rx_mib_s": rx_mib_s,
            "tx_mib_s": tx_mib_s,
        })
    prev_b, prev_p, prev_s, prev_n, prev_t = cur_b, cur_p, cur_s, cur_n, cur_t

print("TELEMETRY_JSON_START")
print(json.dumps(samples))
print("TELEMETRY_JSON_END")
'''


def start_node1_telemetry_pod(duration_sec: int) -> str:
    pod_name = "arch-telemetry-sampler"
    run_cmd(["kubectl", "delete", "pod", pod_name, "-n", "default", "--ignore-not-found=true"], check=False)
    manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": pod_name, "namespace": "default"},
        "spec": {
            "nodeName": NODE1,
            "restartPolicy": "Never",
            "hostPID": True,
            "hostNetwork": True,
            "containers": [{
                "name": "sampler",
                "image": "python:3.11-slim",
                "securityContext": {"privileged": True},
                "command": ["python3", "-c", TELEMETRY_SCRIPT, str(duration_sec)],
            }],
        },
    }
    subprocess.run(["kubectl", "apply", "-f", "-"], input=json.dumps(manifest), text=True, capture_output=True)
    return pod_name


def parse_duration_ms(val) -> float:
    if isinstance(val, (int, float)):
        return float(val) / 1e6  # slog serializes time.Duration as int64 nanoseconds
    s = str(val).strip()
    if not s:
        return 0.0
    if s.endswith("µs") or s.endswith("us"):
        return float(s[:-2]) / 1000.0
    if s.endswith("ns"):
        return float(s[:-2]) / 1e6
    if s.endswith("ms"):
        return float(s[:-2])
    if s.endswith("s"):
        body = s[:-1]
        if "m" in body:
            m, sec = body.split("m", 1)
            return (float(m) * 60.0 + float(sec)) * 1000.0
        return float(body) * 1000.0
    return 0.0


def percentile(arr: list[float], p: float) -> float:
    if not arr:
        return 0.0
    s = sorted(arr)
    idx = min(len(s) - 1, max(0, int(len(s) * p / 100.0)))
    return s[idx]


def summary(arr: list[float]) -> dict:
    if not arr:
        return {"count": 0, "avg": 0.0, "p50": 0.0, "p90": 0.0, "p95": 0.0}
    return {
        "count": len(arr),
        "avg": sum(arr) / len(arr),
        "p50": percentile(arr, 50),
        "p90": percentile(arr, 90),
        "p95": percentile(arr, 95),
    }


def parse_server_and_atelet_logs(start_iso: str) -> dict:
    ateapi_hibernate = []
    ateapi_resume_cold = []
    ateapi_resume_warm = []
    seen_actors = set()

    pods = run_cmd(["kubectl", "get", "pods", "-n", "ate-system", "-l", "app=ate-api-server", "-o", "jsonpath={.items[*].metadata.name}"]).stdout.split()
    lines = []
    for pod in pods:
        out = run_cmd(["kubectl", "logs", "-n", "ate-system", pod, f"--since-time={start_iso}"], check=False).stdout
        for line in out.splitlines():
            if "Handle RPC" in line:
                try:
                    d = json.loads(line)
                    if d.get("err") is None:
                        lines.append(d)
                except Exception:
                    pass

    lines.sort(key=lambda x: x.get("time", ""))
    for d in lines:
        method = d.get("method", "")
        el_str = d.get("elapsed-time", "")
        principal = d.get("principal", {}).get("ID", "")
        if not el_str:
            continue
        ms = parse_duration_ms(el_str)
        if "SuspendActor" in method or "PauseActor" in method:
            ateapi_hibernate.append(ms)
        elif "ResumeActor" in method and "atenet-router" not in principal:
            name = d.get("req", {}).get("name", "")
            if name and name not in seen_actors:
                seen_actors.add(name)
                ateapi_resume_cold.append(ms)
            else:
                seen_actors.add(name)
                ateapi_resume_warm.append(ms)

    wait_detach_ms = []
    import_dur_ms = []
    restore_mount_ms = []
    restore_download_ms = []
    restore_ateom_ms = []
    restore_total_ms = []
    async_detach_ms = []
    checkpoint_total_ms = []

    restore_same_node_ms = []
    restore_same_node_download_ms = []
    restore_same_node_ateom_ms = []
    restore_cross_node_ms = []
    restore_cross_node_mount_ms = []
    restore_cross_node_download_ms = []
    restore_cross_node_ateom_ms = []

    atelet_pods = run_cmd(["kubectl", "get", "pods", "-n", "ate-system", "-l", "app=atelet", "-o", "jsonpath={.items[*].metadata.name}"]).stdout.split()
    for apod in atelet_pods:
        out = run_cmd(["kubectl", "logs", "-n", "ate-system", apod, f"--since-time={start_iso}"], check=False).stdout
        for line in out.splitlines():
            if not line.startswith("{"):
                continue
            try:
                d = json.loads(line)
            except Exception:
                continue
            msg = d.get("msg", "")
            if msg == "Completed local re-import on resume":
                wait_detach_ms.append(parse_duration_ms(d.get("waitDetach", 0)))
                import_dur_ms.append(parse_duration_ms(d.get("importDuration", 0)))
            elif msg == "Completed async parallel detach on pause":
                async_detach_ms.append(parse_duration_ms(d.get("elapsed", 0)))
            elif msg == "Restore timing breakdown" and d.get("ate.actor.restore.error") is None:
                snap_kind = d.get("ate.snapshot.kind", "")
                if snap_kind == "golden":
                    continue
                tot = float(d.get("ate.actor.restore.duration.total", 0.0)) * 1000.0
                mnt = float(d.get("ate.actor.restore.duration.volume_mount", 0.0)) * 1000.0
                dl = float(d.get("ate.actor.restore.duration.download", 0.0)) * 1000.0
                ateom = float(d.get("ate.actor.restore.duration.ateom_restore", 0.0)) * 1000.0
                is_xnode = d.get("ate.actor.restore.cross_node_simulated")

                restore_mount_ms.append(mnt)
                restore_download_ms.append(dl)
                restore_ateom_ms.append(ateom)
                restore_total_ms.append(tot)
                if is_xnode is True or mnt >= 500.0:
                    restore_cross_node_ms.append(tot)
                    restore_cross_node_mount_ms.append(mnt)
                    restore_cross_node_download_ms.append(dl)
                    restore_cross_node_ateom_ms.append(ateom)
                else:
                    restore_same_node_ms.append(tot)
                    restore_same_node_download_ms.append(dl)
                    restore_same_node_ateom_ms.append(ateom)
            elif d.get("method") == "/atelet.AteomHerder/Checkpoint" and d.get("err") is None:
                checkpoint_total_ms.append(parse_duration_ms(d.get("elapsed-time", "0s")))

    obs_pct = (len(restore_cross_node_ms) * 100.0 / len(restore_total_ms)) if restore_total_ms else 0.0
    return {
        "observed_cross_node_restore_pct": obs_pct,
        "ateapi_hibernate": summary(ateapi_hibernate),
        "ateapi_resume_cold": summary(ateapi_resume_cold),
        "ateapi_resume_warm": summary(ateapi_resume_warm),
        "atelet_wait_detach_ms": summary(wait_detach_ms),
        "atelet_import_dur_ms": summary(import_dur_ms),
        "atelet_restore_mount_ms": summary(restore_mount_ms),
        "atelet_restore_download_ms": summary(restore_download_ms),
        "atelet_restore_ateom_ms": summary(restore_ateom_ms),
        "atelet_restore_total_ms": summary(restore_total_ms),
        "atelet_restore_same_node_ms": summary(restore_same_node_ms),
        "atelet_restore_same_node_download_ms": summary(restore_same_node_download_ms),
        "atelet_restore_same_node_ateom_ms": summary(restore_same_node_ateom_ms),
        "atelet_restore_cross_node_ms": summary(restore_cross_node_ms),
        "atelet_restore_cross_node_mount_ms": summary(restore_cross_node_mount_ms),
        "atelet_restore_cross_node_download_ms": summary(restore_cross_node_download_ms),
        "atelet_restore_cross_node_ateom_ms": summary(restore_cross_node_ateom_ms),
        "atelet_async_detach_ms": summary(async_detach_ms),
        "atelet_checkpoint_total_ms": summary(checkpoint_total_ms),
    }


def run_step(atelet_image: str, wait_sec: str = "1.0", duration_sec: int = 180, cross_node_pct: float = 100.0):
    users = 15
    workers = 17  # 2 spare workers prevent async ReportWorkerCapacity race
    prepare_node1_arch3c_and_workers(atelet_image, workers, cross_node_pct=cross_node_pct)

    pct_tag = str(int(cross_node_pct)) if float(cross_node_pct).is_integer() else str(cross_node_pct).replace(".", "p")
    label = f"arch3c_sim1s_xnode{pct_tag}pct_wait{wait_sec.replace('.', 'p')}s"
    job_id = uuid.uuid4().hex[:6]
    run_name = f"{label}_w2_w{users}"
    job_name = f"runner-{label.replace('_', '-')}-{job_id}"

    subs = {
        "JOB_NAME": job_name,
        "IMAGE": IMAGE,
        "TAG": TAG,
        "NAME": run_name,
        "DEST": BUCKET,
        "TEST_FILE": "/app/tests/glutton.py",
        "DURATION": f"{duration_sec}s",
        "USERS": str(users),
    }
    tmpl = TMPL_PATH.read_text()
    for k, v in subs.items():
        tmpl = tmpl.replace(f"${{{k}}}", str(v))

    extra_flags = [
        "--lifecycle-mode", "pause",
        "--mem-target", "512Mi",
        "--mem-churn", "32Mi",
        "--mem-read", "all",
        "--min-wait-time", wait_sec,
        "--max-wait-time", wait_sec,
    ]
    docs = list(yaml.safe_load_all(tmpl))
    for doc in docs:
        if doc and doc.get("kind") == "Job":
            doc["spec"]["template"]["spec"]["nodeSelector"] = {"kubernetes.io/hostname": NODE2}
            doc["spec"]["template"]["spec"]["tolerations"] = [
                {"key": "dedicated", "operator": "Equal", "value": "loadgen", "effect": "NoSchedule"}
            ]
            doc["spec"]["template"]["spec"]["containers"][0]["args"].extend(extra_flags)

    manifest_yaml = yaml.safe_dump_all(docs)

    print(f"\n========================================================")
    print(f"Launching W2 {label.upper()}: W={workers} workers / N={users} actors, cross_node_pct={cross_node_pct}%, wait={wait_sec}s ({duration_sec}s)")
    print(f"Active Node: {NODE1} | Loadgen Node: {NODE2}")
    print(f"========================================================")

    start_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    sampler_pod = start_node1_telemetry_pod(duration_sec + 15)

    subprocess.run(["kubectl", "apply", "-f", "-"], input=manifest_yaml, text=True, check=True)

    run_cmd(["kubectl", "wait", "--for=condition=complete", f"job/{job_name}", "-n", "benchmarking", f"--timeout={duration_sec + 180}s"], check=False)

    job_logs = run_cmd(["kubectl", "logs", f"job/{job_name}", "-n", "benchmarking"], check=False).stdout

    for _ in range(30):
        res = run_cmd(["kubectl", "get", "pod", sampler_pod, "-n", "default", "-o", "jsonpath={.status.phase}"], check=False)
        if res.stdout.strip() in ("Succeeded", "Failed"):
            break
        time.sleep(1)

    sampler_logs = run_cmd(["kubectl", "logs", sampler_pod, "-n", "default"], check=False).stdout
    run_cmd(["kubectl", "delete", "pod", sampler_pod, "-n", "default", "--ignore-not-found=true"], check=False)

    samples = []
    if "TELEMETRY_JSON_START" in sampler_logs:
        json_part = sampler_logs.split("TELEMETRY_JSON_START")[1].split("TELEMETRY_JSON_END")[0].strip()
        samples = json.loads(json_part)

    steady = samples[15:-10] if len(samples) > 30 else samples
    active_write = [s for s in steady if s["tot_write_mib_s"] > 50.0]

    telemetry_summary = {
        "peak_tot_write_mib_s": max((s["tot_write_mib_s"] for s in steady), default=0.0),
        "peak_boot_write_mib_s": max((s["boot_write_mib_s"] for s in steady), default=0.0),
        "peak_pool_write_mib_s": max((s["pool_write_mib_s"] for s in steady), default=0.0),
        "peak_pool_read_mib_s": max((s.get("pool_read_mib_s", 0.0) for s in steady), default=0.0),
        "active_mean_tot_write_mib_s": sum(s["tot_write_mib_s"] for s in active_write) / len(active_write) if active_write else 0.0,
        "peak_tot_aqu_sz": max((s["tot_aqu_sz"] for s in steady), default=0.0),
        "peak_boot_w_await_ms": max((s["boot_w_await_ms"] for s in steady), default=0.0),
        "peak_pool_w_await_ms": max((s["pool_w_await_ms"] for s in steady), default=0.0),
        "active_mean_pool_w_await_ms": sum(s["pool_w_await_ms"] for s in active_write) / len(active_write) if active_write else 0.0,
        "mean_cpu_util_pct": sum(s["cpu_util_pct"] for s in steady) / len(steady) if steady else 0.0,
        "peak_cpu_util_pct": max((s["cpu_util_pct"] for s in steady), default=0.0),
        "peak_iowait_pct": max((s["iowait_pct"] for s in steady), default=0.0),
        "mean_iowait_pct": sum(s["iowait_pct"] for s in steady) / len(steady) if steady else 0.0,
        "peak_tx_mib_s": max((s["tx_mib_s"] for s in steady), default=0.0),
        "peak_rx_mib_s": max((s["rx_mib_s"] for s in steady), default=0.0),
    }

    server_metrics = parse_server_and_atelet_logs(start_iso)

    result = {
        "label": label,
        "cross_node_pct": cross_node_pct,
        "wait_sec": wait_sec,
        "duration_sec": duration_sec,
        "workers": workers,
        "users": users,
        "job_name": job_name,
        "telemetry": telemetry_summary,
        "server_metrics": server_metrics,
        "job_logs_tail": "\n".join(job_logs.splitlines()[-55:]),
    }

    out_dir = ROOT / "benchmarking/results/w2-single-node"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"result_{label}.json"
    out_file.write_text(json.dumps(result, indent=2))
    print(f"\n=== RESULT SUMMARY ({label.upper()}) ===")
    print(json.dumps({
        "telemetry": telemetry_summary,
        "server_metrics": server_metrics,
    }, indent=2))
    print("\n--- Boomer Client Logs Tail ---")
    print(result["job_logs_tail"])


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("atelet_image", help="ko-built atelet image URI")
    parser.add_argument("wait_sec", nargs="?", default="1.0", help="Wait time between Pause and Resume (seconds)")
    parser.add_argument("duration_sec", nargs="?", type=int, default=120, help="Benchmark duration in seconds")
    parser.add_argument("--cross-node-restore-pct", type=float, default=100.0, help="Percentage (0..100) of restores that simulate a cross-node detach/attach")
    parser.add_argument("--sweep-pcts", type=str, default="", help="Comma-separated list of percentages to sweep (e.g. 5,10,15,20,25,30,35,40)")
    args = parser.parse_args()
    if args.sweep_pcts:
        pcts = [float(x.strip()) for x in args.sweep_pcts.split(",") if x.strip()]
        for p in pcts:
            run_step(args.atelet_image, args.wait_sec, args.duration_sec, cross_node_pct=p)
    else:
        run_step(args.atelet_image, args.wait_sec, args.duration_sec, cross_node_pct=args.cross_node_restore_pct)


