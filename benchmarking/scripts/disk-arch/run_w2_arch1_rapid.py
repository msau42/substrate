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

"""Run W2 Single-Node Benchmark at N=15 (W=17 clean workers) for Arch 1 with GCS Rapid Bucket (SuspendActor + Boot Disk)."""

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


def prepare_node1_arch1_rapid(atelet_image: str, workers: int = 17):
    """Configure NODE1 for Arch 1 (shared boot disk /dev/nvme0n1, no dedicated Hyperdisk pool) + GCS Rapid Bucket."""
    print("Preparing NODE1 for Arch 1 (Shared Boot Disk + GCS Rapid Bucket)...")
    run_cmd(["kubectl", "delete", "jobs", "-n", "benchmarking", "-l", "app=substrate-benchmark-runner"], check=False)

    # Clean up any leftover actors and stale worker assignments in PostgreSQL
    run_cmd([
        "kubectl", "exec", "-n", "ate-system", "postgres-0", "-c", "postgres", "--",
        "psql", "-U", "postgres", "-d", "atepg", "-c",
        "DELETE FROM worker_assignments WHERE actor_uid IN (SELECT uid FROM actors WHERE atespace != 'ate-golden'); DELETE FROM actors WHERE atespace != 'ate-golden';"
    ], check=False)

    script = """
set -e
mkdir -p /var/lib/ateom-gvisor
if findmnt -rn /var/lib/ateom-gvisor/actors >/dev/null 2>&1; then
    umount -l /var/lib/ateom-gvisor/actors || true
fi
rm -rf /var/lib/ateom-gvisor/actors/* 2>/dev/null || true
mkdir -p /var/lib/ateom-gvisor/actors
df -h /var/lib/ateom-gvisor/actors
"""
    _run_privileged_host_script(script)

    p_ds = run_cmd(["kubectl", "get", "ds", "-n", "ate-system", "-l", "app=atelet", "-o", "jsonpath={.items[0].metadata.name}"])
    ds_name = p_ds.stdout.strip()
    p_args = run_cmd(["kubectl", "get", "ds", "-n", "ate-system", ds_name, "-o", "jsonpath={.spec.template.spec.containers[0].args}"])
    args_list = json.loads(p_args.stdout)
    new_args = [a for a in args_list if not a.startswith("--actor-disk-pool-dir")]
    patch = [
        {"op": "replace", "path": "/spec/template/spec/containers/0/args", "value": new_args},
        {"op": "replace", "path": "/spec/template/spec/containers/0/image", "value": atelet_image},
    ]
    run_cmd(["kubectl", "patch", "ds", "-n", "ate-system", ds_name, "--type=json", f"-p={json.dumps(patch)}"])
    run_cmd([
        "kubectl", "set", "env", f"ds/{ds_name}", "-n", "ate-system",
        "ATE_GCS_RAPID=true",
        "ATELET_SIMULATE_CROSS_NODE_PCT=0",
        "ATELET_SIMULATE_DISK_OP_MS=0",
        "ATELET_DETACH_ON_PAUSE=false",
    ])
    run_cmd(["kubectl", "rollout", "restart", "ds", "-n", "ate-system", ds_name])
    run_cmd(["kubectl", "rollout", "status", "ds", "-n", "ate-system", ds_name, "--timeout=90s"])

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
    return boot

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
prev_b = read_all_diskstats()
prev_s = read_stat()
prev_n = read_net()
prev_t = time.time()

samples = []
for _ in range(duration):
    time.sleep(1.0)
    cur_t = time.time()
    dt = cur_t - prev_t
    cur_b = read_all_diskstats()
    cur_s = read_stat()
    cur_n = read_net()

    b_w_mib = ((cur_b["write_sectors"] - prev_b["write_sectors"]) * 512) / (dt * 1024 * 1024)
    b_r_mib = ((cur_b["read_sectors"] - prev_b["read_sectors"]) * 512) / (dt * 1024 * 1024)
    b_w_ios = cur_b["writes"] - prev_b["writes"]
    b_w_await = (cur_b["write_ticks"] - prev_b["write_ticks"]) / b_w_ios if b_w_ios > 0 else 0.0
    b_aqu_sz = (cur_b["time_in_queue"] - prev_b["time_in_queue"]) / (dt * 1000.0)

    d_tot = cur_s["total"] - prev_s["total"]
    d_idle = cur_s["idle"] - prev_s["idle"]
    d_iow = cur_s["iowait"] - prev_s["iowait"]
    cpu_util = 100.0 * (d_tot - d_idle) / d_tot if d_tot > 0 else 0.0
    iowait_pct = 100.0 * d_iow / d_tot if d_tot > 0 else 0.0

    rx_mib = (cur_n["rx"] - prev_n["rx"]) / (dt * 1024 * 1024)
    tx_mib = (cur_n["tx"] - prev_n["tx"]) / (dt * 1024 * 1024)

    samples.append({
        "boot_write_mib_s": round(b_w_mib, 2),
        "boot_read_mib_s": round(b_r_mib, 2),
        "boot_w_await_ms": round(b_w_await, 2),
        "boot_aqu_sz": round(b_aqu_sz, 2),
        "cpu_util_pct": round(cpu_util, 2),
        "iowait_pct": round(iowait_pct, 2),
        "rx_mib_s": round(rx_mib, 2),
        "tx_mib_s": round(tx_mib, 2),
    })
    prev_b, prev_s, prev_n, prev_t = cur_b, cur_s, cur_n, cur_t

print("TELEMETRY_JSON_START")
print(json.dumps(samples))
print("TELEMETRY_JSON_END")
'''


def start_node1_telemetry_pod(duration_sec: int) -> str:
    pod_name = "node1-telemetry-sampler"
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
        return float(val) / 1e6
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
    ateapi_suspend = []
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
        if "SuspendActor" in method:
            ateapi_suspend.append(ms)
        elif "ResumeActor" in method and "atenet-router" not in principal:
            name = d.get("req", {}).get("name", "")
            if name and name not in seen_actors:
                seen_actors.add(name)
                ateapi_resume_cold.append(ms)
            else:
                seen_actors.add(name)
                ateapi_resume_warm.append(ms)

    checkpoint_ateom_ms = []
    checkpoint_upload_ms = []
    checkpoint_total_ms = []
    restore_download_ms = []
    restore_ateom_ms = []
    restore_total_ms = []

    atelet_pods = run_cmd([
        "kubectl", "get", "pods", "-n", "ate-system", "-l", "app=atelet",
        "--field-selector", f"spec.nodeName={NODE1}",
        "-o", "jsonpath={.items[*].metadata.name}"
    ]).stdout.split()

    for apod in atelet_pods:
        out = run_cmd(["kubectl", "logs", "-n", "ate-system", apod, f"--since-time={start_iso}"], check=False).stdout
        for line in out.splitlines():
            if "Checkpoint timing breakdown" in line:
                try:
                    d = json.loads(line)
                    if d.get("ate.snapshot.outcome") == "success" and d.get("ate.actor.operation") == "suspend":
                        checkpoint_ateom_ms.append(parse_duration_ms(d.get("ate.snapshot.phase.duration.ateom", 0)))
                        checkpoint_upload_ms.append(parse_duration_ms(d.get("ate.snapshot.phase.duration.upload", 0)))
                        checkpoint_total_ms.append(parse_duration_ms(d.get("ate.snapshot.phase.duration.total", 0)))
                except Exception:
                    pass
            elif "Restore timing breakdown" in line:
                try:
                    d = json.loads(line)
                    if d.get("ate.snapshot.outcome") == "success" and d.get("ate.actor.operation") == "resume_suspended":
                        restore_download_ms.append(parse_duration_ms(d.get("ate.snapshot.phase.duration.download", 0)))
                        restore_ateom_ms.append(parse_duration_ms(d.get("ate.snapshot.phase.duration.ateom", 0)))
                        restore_total_ms.append(parse_duration_ms(d.get("ate.snapshot.phase.duration.total", 0)))
                except Exception:
                    pass

    return {
        "ateapi_suspend_ms": summary(ateapi_suspend),
        "ateapi_resume_cold_ms": summary(ateapi_resume_cold),
        "ateapi_resume_warm_ms": summary(ateapi_resume_warm),
        "atelet_checkpoint_ateom_ms": summary(checkpoint_ateom_ms),
        "atelet_checkpoint_upload_ms": summary(checkpoint_upload_ms),
        "atelet_checkpoint_total_ms": summary(checkpoint_total_ms),
        "atelet_restore_download_ms": summary(restore_download_ms),
        "atelet_restore_ateom_ms": summary(restore_ateom_ms),
        "atelet_restore_total_ms": summary(restore_total_ms),
    }


def run_benchmark(atelet_image: str, wait_sec: str = "1.0", duration_sec: int = 180):
    workers = 17
    users = 15
    label = f"arch1_rapid_w2_wait{wait_sec.replace('.', 'p')}s"
    prepare_node1_arch1_rapid(atelet_image, workers)

    run_id = uuid.uuid4().hex[:6]
    run_name = f"{label}_w2_w{users}"
    job_name = f"runner-{label.replace('_', '-')}-{run_id}"[:62].rstrip("-")

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
        "--lifecycle-mode", "suspend",
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
    print(f"Launching W2 {label.upper()}: W={workers} workers / N={users} actors, wait={wait_sec}s ({duration_sec}s)")
    print(f"Active Node: {NODE1} | Loadgen Node: {NODE2} | Bucket: gs://snapshot-substrate-rapid-msau-gke-dev")
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
    active_write = [s for s in steady if s["boot_write_mib_s"] > 50.0]

    telemetry_summary = {
        "peak_boot_write_mib_s": max((s["boot_write_mib_s"] for s in steady), default=0.0),
        "peak_boot_read_mib_s": max((s["boot_read_mib_s"] for s in steady), default=0.0),
        "active_mean_boot_write_mib_s": sum(s["boot_write_mib_s"] for s in active_write) / len(active_write) if active_write else 0.0,
        "peak_boot_aqu_sz": max((s["boot_aqu_sz"] for s in steady), default=0.0),
        "peak_boot_w_await_ms": max((s["boot_w_await_ms"] for s in steady), default=0.0),
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
    atelet_img = sys.argv[1]
    dur = int(sys.argv[2]) if len(sys.argv) > 2 else 180
    run_benchmark(atelet_img, "1.0", dur)
