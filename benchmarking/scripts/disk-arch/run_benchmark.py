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

"""Executes repeatable 1-active-node W1 and W2 disk architecture benchmarks for gVisor and microVM."""

import argparse
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parent.parent.parent.parent
TMPL_PATH = ROOT / "benchmarking/automation/manifests/runner-job.yaml.tmpl"
PROJECT_ID = os.environ.get("PROJECT_ID", "msau-gke-dev")
IMAGE = f"us-docker.pkg.dev/{PROJECT_ID}/gcr.io/ate-images/locust-test:latest"
BUCKET = f"gs://snapshot-substrate-test-{PROJECT_ID}/results"
TAG = "latest"


def get_nodes() -> tuple[str, str]:
    """Return (active_node, loadgen_node)."""
    p = subprocess.run(
        [
            "kubectl",
            "get",
            "nodes",
            "-l",
            "cloud.google.com/gke-nodepool=substrate-node-pool",
            "-o",
            "jsonpath={.items[*].metadata.name}",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    all_nodes = p.stdout.strip().split()
    wp = subprocess.run(
        [
            "kubectl",
            "get",
            "pods",
            "-n",
            "benchmark-workloads",
            "-l",
            "ate.dev/worker-pool=benchmark-ateom",
            "-o",
            "jsonpath={.items[0].spec.nodeName}",
        ],
        capture_output=True,
        text=True,
    )
    active_node = (
        wp.stdout.strip()
        if wp.returncode == 0 and wp.stdout.strip()
        else all_nodes[0]
    )
    loadgen_nodes = [n for n in all_nodes if n != active_node]
    loadgen_node = loadgen_nodes[0] if loadgen_nodes else active_node
    return active_node, loadgen_node


def deploy_and_pin_workloads(
    sandbox_class: str, worker_count: int = 6, actor_memory: str = "1536Mi"
):
    """Deploy benchmark workloads for gvisor or microvm and pin worker pool to active_node."""
    if sandbox_class == "microvm":
        print("Installing microVM dependencies (hack/install-microvm-deps.sh --install)...")
        subprocess.run(
            ["./hack/install-microvm-deps.sh", "--install"],
            cwd=ROOT,
            check=True,
        )

    print(
        f"Deploying benchmark workloads (sandbox_class={sandbox_class}, workers={worker_count}, actor_memory={actor_memory})..."
    )
    subprocess.run(
        [
            "./benchmarking/workloads/deploy.sh",
            "--deploy",
            "--sandbox-class",
            sandbox_class,
            "--worker-count",
            str(worker_count),
            "--actor-memory",
            actor_memory,
        ],
        cwd=ROOT,
        check=True,
    )

    active_node, loadgen_node = get_nodes()
    print(f"Pinning benchmark-ateom WorkerPool to active node {active_node}...")
    patch = {
        "spec": {
            "template": {
                "spec": {
                    "nodeSelector": {"kubernetes.io/hostname": active_node}
                }
            }
        }
    }
    subprocess.run(
        [
            "kubectl",
            "patch",
            "workerpool",
            "benchmark-ateom",
            "-n",
            "benchmark-workloads",
            "--type=merge",
            "-p",
            json.dumps(patch),
        ],
        check=True,
    )
    subprocess.run(
        [
            "kubectl",
            "rollout",
            "status",
            "deployment/benchmark-ateom",
            "-n",
            "benchmark-workloads",
            "--timeout=180s",
        ],
        check=True,
    )
    print(
        f"Workloads ready on active node {active_node} (loadgen node: {loadgen_node})."
    )


def reset_active_node_disk_pool(active_node: str):
    """Clear /var/lib/ateom-gvisor/actors/* and disk-pool/disk-*/* and restart atelet on active_node."""
    print(f"Resetting disk pool and restarting atelet on {active_node}...")
    pod_name = "disk-cleaner"
    subprocess.run(
        [
            "kubectl",
            "delete",
            "pod",
            pod_name,
            "-n",
            "default",
            "--ignore-not-found=true",
        ],
        capture_output=True,
    )
    manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": pod_name, "namespace": "default"},
        "spec": {
            "nodeName": active_node,
            "restartPolicy": "Never",
            "hostPID": True,
            "containers": [
                {
                    "name": "cleaner",
                    "image": "ubuntu:22.04",
                    "securityContext": {"privileged": True},
                    "command": [
                        "nsenter",
                        "-t",
                        "1",
                        "-m",
                        "-u",
                        "-i",
                        "-n",
                        "--",
                        "bash",
                        "-c",
                        "rm -rf /var/lib/ateom-gvisor/actors/* /var/lib/ateom-gvisor/disk-pool/disk-*/* 2>/dev/null || true",
                    ],
                }
            ],
        },
    }
    subprocess.run(
        ["kubectl", "apply", "-f", "-"],
        input=json.dumps(manifest),
        text=True,
        capture_output=True,
    )
    for _ in range(30):
        res = subprocess.run(
            [
                "kubectl",
                "get",
                "pod",
                pod_name,
                "-n",
                "default",
                "-o",
                "jsonpath={.status.phase}",
            ],
            capture_output=True,
            text=True,
        )
        if res.stdout.strip() in ("Succeeded", "Failed"):
            break
        time.sleep(1)
    subprocess.run(
        [
            "kubectl",
            "delete",
            "pod",
            pod_name,
            "-n",
            "default",
            "--ignore-not-found=true",
        ],
        capture_output=True,
    )

    p = subprocess.run(
        [
            "kubectl",
            "get",
            "pods",
            "-n",
            "ate-system",
            "-l",
            "app=atelet",
            "--field-selector",
            f"spec.nodeName={active_node}",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ],
        capture_output=True,
        text=True,
    )
    atelet_pod = p.stdout.strip()
    if atelet_pod:
        subprocess.run(
            [
                "kubectl",
                "delete",
                "pod",
                atelet_pod,
                "-n",
                "ate-system",
                "--wait=true",
            ],
            capture_output=True,
        )
        for _ in range(30):
            r = subprocess.run(
                [
                    "kubectl",
                    "get",
                    "pods",
                    "-n",
                    "ate-system",
                    "-l",
                    "app=atelet",
                    "--field-selector",
                    f"spec.nodeName={active_node}",
                    "-o",
                    "jsonpath={.items[0].status.containerStatuses[0].ready}",
                ],
                capture_output=True,
                text=True,
            )
            if r.stdout.strip() == "true":
                break
            time.sleep(1)
    print(f"Active node {active_node} disk pool and atelet ready.")


def cleanup():
    """Ensure no leftover test actors or jobs exist."""
    print("Running cleanup...")
    subprocess.run(
        [
            "kubectl",
            "delete",
            "jobs",
            "-n",
            "benchmarking",
            "-l",
            "app=substrate-benchmark-runner",
        ],
        capture_output=True,
    )

    p = subprocess.run(
        ["./bin/kubectl-ate", "get", "actors", "-A", "-o", "json"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if p.returncode == 0 and p.stdout.strip():
        try:
            data = json.loads(p.stdout)
            actors = data.get("actors", [])
            non_golden = [
                a
                for a in actors
                if a.get("metadata", {}).get("atespace") != "ate-golden"
            ]
            if non_golden:
                print(f"Found {len(non_golden)} non-golden actors to clean up.")
                needs_worker_restart = any(
                    a.get("status", {}).get("state")
                    in (
                        3,
                        "ACTOR_STATE_SUSPENDING",
                        5,
                        "ACTOR_STATE_PAUSING",
                        2,
                        "ACTOR_STATE_RESUMING",
                    )
                    for a in non_golden
                )
                if needs_worker_restart:
                    print(
                        "Restarting benchmark-ateom worker pods to crash stuck actors..."
                    )
                    subprocess.run(
                        [
                            "kubectl",
                            "delete",
                            "pod",
                            "-n",
                            "benchmark-workloads",
                            "-l",
                            "ate.dev/worker-pool=benchmark-ateom",
                        ],
                        capture_output=True,
                    )
                    subprocess.run(
                        [
                            "kubectl",
                            "rollout",
                            "status",
                            "deployment/benchmark-ateom",
                            "-n",
                            "benchmark-workloads",
                            "--timeout=60s",
                        ],
                        capture_output=True,
                    )
                    time.sleep(2)

                for a in non_golden:
                    ns = a["metadata"]["atespace"]
                    name = a["metadata"]["name"]
                    print(f"Deleting actor {ns}/{name}...")
                    subprocess.run(
                        ["./bin/kubectl-ate", "delete", "actor", name, "-a", ns],
                        cwd=ROOT,
                        capture_output=True,
                    )
                    subprocess.run(
                        ["./bin/kubectl-ate", "delete", "atespace", ns],
                        cwd=ROOT,
                        capture_output=True,
                    )
        except Exception as e:
            print(f"Cleanup error parsing actors: {e}")

    active_node, _ = get_nodes()
    reset_active_node_disk_pool(active_node)
    print("Cleanup complete.")


def render_job(
    name: str,
    extra_flags: list[str],
    loadgen_node: str,
    users: int = 1,
    duration: str = "1m",
    test_file: str = "/app/tests/glutton.py",
) -> tuple[str, str]:
    job_id = uuid.uuid4().hex[:6]
    job_name = f"runner-{name.replace('_', '-')}-{job_id}"
    subs = {
        "JOB_NAME": job_name,
        "IMAGE": IMAGE,
        "TAG": TAG,
        "NAME": name,
        "DEST": BUCKET,
        "TEST_FILE": test_file,
        "DURATION": duration,
        "USERS": str(users),
    }
    tmpl = TMPL_PATH.read_text()
    for k, v in subs.items():
        tmpl = tmpl.replace(f"${{{k}}}", str(v))

    docs = list(yaml.safe_load_all(tmpl))
    for doc in docs:
        if doc and doc.get("kind") == "Job":
            doc["spec"]["template"]["spec"]["nodeSelector"] = {
                "kubernetes.io/hostname": loadgen_node
            }
            doc["spec"]["template"]["spec"]["containers"][0]["args"].extend(
                extra_flags
            )
    return job_name, yaml.safe_dump_all(docs)


def run_job(
    name: str,
    extra_flags: list[str],
    users: int = 1,
    duration: str = "1m",
    test_file: str = "/app/tests/glutton.py",
) -> dict:
    cleanup()
    active_node, loadgen_node = get_nodes()
    job_name, manifest = render_job(
        name,
        extra_flags,
        loadgen_node=loadgen_node,
        users=users,
        duration=duration,
        test_file=test_file,
    )
    print("\n========================================================")
    print(
        f"Starting test: {name} (file={test_file}, users={users}, duration={duration})"
    )
    print(f"Active Node: {active_node} | Loadgen Node: {loadgen_node}")
    print(f"Job Name: {job_name}")
    print(f"Extra Flags: {extra_flags}")
    print("========================================================")

    p = subprocess.run(
        ["kubectl", "apply", "-f", "-"],
        input=manifest,
        text=True,
        capture_output=True,
    )
    if p.returncode != 0:
        print(f"Failed to create Job: {p.stderr}", file=sys.stderr)
        return {"name": name, "status": "failed", "error": p.stderr}

    print(f"Job {job_name} submitted. Waiting for pod to start...")

    pod_name = None
    for _ in range(60):
        res = subprocess.run(
            [
                "kubectl",
                "get",
                "pods",
                "-n",
                "benchmarking",
                "-l",
                f"job-name={job_name}",
                "-o",
                "jsonpath={.items[0].metadata.name}",
            ],
            text=True,
            capture_output=True,
        )
        if res.returncode == 0 and res.stdout.strip():
            pod_name = res.stdout.strip()
            break
        time.sleep(2)

    if not pod_name:
        print("Timed out waiting for pod creation", file=sys.stderr)
        return {"name": name, "status": "failed", "error": "pod never created"}

    print(f"Pod created: {pod_name}. Waiting for job completion...")

    timeout_sec = 300
    start = time.time()

    wait_cmd = [
        "kubectl",
        "wait",
        "--for=condition=complete",
        f"job/{job_name}",
        "-n",
        "benchmarking",
        f"--timeout={timeout_sec}s",
    ]
    res = subprocess.run(wait_cmd, text=True, capture_output=True)

    status = "completed" if res.returncode == 0 else "failed"
    print(
        f"Job finished with status: {status} (in {time.time() - start:.1f}s)"
    )

    log_res = subprocess.run(
        ["kubectl", "logs", f"job/{job_name}", "-n", "benchmarking"],
        text=True,
        capture_output=True,
    )
    logs = log_res.stdout
    print("\n--- Pod Logs Summary ---")
    lines = logs.splitlines()
    for line in lines[-55:]:
        print(line)

    cleanup()
    return {
        "name": name,
        "job_name": job_name,
        "status": status,
        "logs": logs,
    }


BASE_WORKLOADS = {
    "arch1_w1": (
        "arch1_w1_1node_suspend_1gi",
        [
            "--mem-target",
            "1Gi",
            "--mem-churn",
            "64Mi",
            "--mem-read",
            "all",
            "--min-wait-time",
            "1.0",
            "--max-wait-time",
            "1.0",
        ],
        1,
    ),
    "arch1_w2": (
        "arch1_w2_1node_suspend_5x512mi",
        [
            "--mem-target",
            "512Mi",
            "--mem-churn",
            "32Mi",
            "--mem-read",
            "all",
            "--min-wait-time",
            "1.0",
            "--max-wait-time",
            "1.0",
        ],
        5,
    ),
    "arch2_w1": (
        "arch2_w1_1node_nvme_suspend_1gi",
        [
            "--mem-target",
            "1Gi",
            "--mem-churn",
            "64Mi",
            "--mem-read",
            "all",
            "--min-wait-time",
            "1.0",
            "--max-wait-time",
            "1.0",
        ],
        1,
    ),
    "arch2_w2": (
        "arch2_w2_1node_nvme_suspend_5x512mi",
        [
            "--mem-target",
            "512Mi",
            "--mem-churn",
            "32Mi",
            "--mem-read",
            "all",
            "--min-wait-time",
            "1.0",
            "--max-wait-time",
            "1.0",
        ],
        5,
    ),
    "arch3a_w1": (
        "arch3a_w1_1node_pause_hd_c3_8_1gi",
        [
            "--lifecycle-mode",
            "pause",
            "--mem-target",
            "1Gi",
            "--mem-churn",
            "64Mi",
            "--mem-read",
            "all",
            "--min-wait-time",
            "1.0",
            "--max-wait-time",
            "1.0",
        ],
        1,
    ),
    "arch3a_w2": (
        "arch3a_w2_1node_pause_hd_c3_8_5x512mi",
        [
            "--lifecycle-mode",
            "pause",
            "--mem-target",
            "512Mi",
            "--mem-churn",
            "32Mi",
            "--mem-read",
            "all",
            "--min-wait-time",
            "1.0",
            "--max-wait-time",
            "1.0",
        ],
        5,
    ),
    "arch3b_w1": (
        "arch3b_w1_1node_pause_hd_c3_44_1gi",
        [
            "--lifecycle-mode",
            "pause",
            "--mem-target",
            "1Gi",
            "--mem-churn",
            "64Mi",
            "--mem-read",
            "all",
            "--min-wait-time",
            "1.0",
            "--max-wait-time",
            "1.0",
        ],
        1,
    ),
    "arch3b_w2": (
        "arch3b_w2_1node_pause_hd_c3_44_5x512mi",
        [
            "--lifecycle-mode",
            "pause",
            "--mem-target",
            "512Mi",
            "--mem-churn",
            "32Mi",
            "--mem-read",
            "all",
            "--min-wait-time",
            "1.0",
            "--max-wait-time",
            "1.0",
        ],
        5,
    ),
}

WORKLOADS = {}
for k, (run_name, flags, users) in BASE_WORKLOADS.items():
    WORKLOADS[k] = (run_name, flags, users, "gvisor")
    WORKLOADS[f"{k}_microvm"] = (f"{run_name}_microvm", flags, users, "microvm")


def main():
    parser = argparse.ArgumentParser(
        description="Execute repeatable 1-active-node W1 and W2 benchmarks for gVisor or microVM."
    )
    parser.add_argument(
        "--sandbox-class",
        choices=["gvisor", "microvm"],
        default="",
        help="Override sandbox class for specified targets (gvisor or microvm)",
    )
    parser.add_argument(
        "--deploy-workloads",
        action="store_true",
        help="Deploy benchmark WorkerPool/ActorTemplates (with --actor-memory 1536Mi) and pin to active node before running",
    )
    parser.add_argument(
        "targets",
        nargs="*",
        help=f"Workload targets to run ({', '.join(BASE_WORKLOADS.keys())}, or *_microvm variants)",
    )
    args = parser.parse_args()

    if not args.targets and not args.deploy_workloads:
        parser.print_help()
        sys.exit(1)

    inferred_sandbox = args.sandbox_class
    if not inferred_sandbox and args.targets:
        if any(t.endswith("_microvm") for t in args.targets):
            inferred_sandbox = "microvm"
        else:
            inferred_sandbox = "gvisor"

    if args.deploy_workloads:
        deploy_and_pin_workloads(
            sandbox_class=inferred_sandbox or "gvisor",
            worker_count=6,
            actor_memory="1536Mi",
        )

    for t in args.targets:
        lookup = t
        if args.sandbox_class == "microvm" and not t.endswith("_microvm"):
            lookup = f"{t}_microvm"
        if lookup in WORKLOADS:
            name, flags, users, _ = WORKLOADS[lookup]
            run_job(
                name,
                flags,
                users=users,
                duration="1m",
                test_file="/app/tests/glutton.py",
            )
        else:
            print(f"Unknown target: {t}")


if __name__ == "__main__":
    main()
