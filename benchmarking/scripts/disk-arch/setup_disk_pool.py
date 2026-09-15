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

"""Provisions, attaches, formats, and mounts dedicated Hyperdisk Balanced volumes for Architecture 3."""

import argparse
import json
import os
import subprocess
import time

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
    nodes = p.stdout.strip().split()
    return [n for n in nodes if n]


def cleanup_disks():
    print("Cleaning up existing actor-disk-* disks...")
    p = run(
        [
            "gcloud",
            "compute",
            "disks",
            "list",
            f"--project={PROJECT_ID}",
            f"--filter=name~'^actor-disk-' AND zone:({ZONE})",
            "--format=json",
        ],
        check=False,
    )
    if p.returncode != 0 or not p.stdout.strip():
        return
    disks = json.loads(p.stdout)
    for d in disks:
        disk_name = d["name"]
        users = d.get("users", [])
        for u in users:
            inst_name = u.split("/")[-1]
            print(f"Detaching {disk_name} from {inst_name}...")
            run(
                [
                    "gcloud",
                    "compute",
                    "instances",
                    "detach-disk",
                    inst_name,
                    f"--disk={disk_name}",
                    f"--zone={ZONE}",
                    f"--project={PROJECT_ID}",
                ],
                check=False,
            )
        print(f"Deleting disk {disk_name}...")
        run(
            [
                "gcloud",
                "compute",
                "disks",
                "delete",
                disk_name,
                f"--zone={ZONE}",
                f"--project={PROJECT_ID}",
                "--quiet",
            ],
            check=False,
        )


def setup_disks_for_node(node: str, count: int, throughput_mbps: int):
    suffix = node[-8:]
    for i in range(count):
        disk_name = f"actor-disk-{suffix}-{i}"
        device_name = f"actor-disk-{i}"
        p = run(
            [
                "gcloud",
                "compute",
                "disks",
                "describe",
                disk_name,
                f"--zone={ZONE}",
                f"--project={PROJECT_ID}",
            ],
            check=False,
        )
        if p.returncode != 0:
            print(
                f"Creating disk {disk_name} (100GB, 50000 IOPS, {throughput_mbps} MiB/s)..."
            )
            run(
                [
                    "gcloud",
                    "compute",
                    "disks",
                    "create",
                    disk_name,
                    f"--project={PROJECT_ID}",
                    f"--zone={ZONE}",
                    "--size=100GB",
                    "--type=hyperdisk-balanced",
                    "--provisioned-iops=50000",
                    f"--provisioned-throughput={throughput_mbps}",
                ]
            )
        else:
            print(f"Disk {disk_name} already exists.")

        inst_desc = run(
            [
                "gcloud",
                "compute",
                "instances",
                "describe",
                node,
                f"--zone={ZONE}",
                f"--project={PROJECT_ID}",
                "--format=json",
            ]
        )
        inst_data = json.loads(inst_desc.stdout)
        attached = any(
            d.get("deviceName") == device_name
            for d in inst_data.get("disks", [])
        )
        if not attached:
            print(f"Attaching {disk_name} to {node} as device {device_name}...")
            run(
                [
                    "gcloud",
                    "compute",
                    "instances",
                    "attach-disk",
                    node,
                    f"--project={PROJECT_ID}",
                    f"--zone={ZONE}",
                    f"--disk={disk_name}",
                    f"--device-name={device_name}",
                ]
            )
        else:
            print(f"Disk {disk_name} already attached to {node}.")

    mount_script = (
        """
set -e
mkdir -p /var/lib/ateom-gvisor/disk-pool
if ! findmnt -n -o PROPAGATION /var/lib/ateom-gvisor | grep -q shared; then
    mount --bind /var/lib/ateom-gvisor /var/lib/ateom-gvisor || true
    mount --make-shared /var/lib/ateom-gvisor || true
fi
for i in $(seq 0 """
        + str(count - 1)
        + """); do
    DEV="/dev/disk/by-id/google-actor-disk-${i}"
    MNT="/var/lib/ateom-gvisor/disk-pool/disk-${i}"
    mkdir -p "${MNT}"
    if ! mountpoint -q "${MNT}"; then
        echo "Formatting ${DEV}..."
        mkfs.ext4 -F -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard "${DEV}"
        echo "Mounting ${DEV} to ${MNT}..."
        mount -o discard,defaults "${DEV}" "${MNT}"
    else
        echo "${MNT} is already mounted."
    fi
    rm -rf "${MNT}"/*
    chmod 755 "${MNT}"
done
rm -rf /var/lib/ateom-gvisor/actors/*
df -h | grep disk-pool
"""
    )
    pod_name = f"disk-pool-mounter-{suffix}"
    manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": pod_name, "namespace": "default"},
        "spec": {
            "nodeName": node,
            "restartPolicy": "Never",
            "hostPID": True,
            "containers": [
                {
                    "name": "mounter",
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
                        mount_script,
                    ],
                }
            ],
        },
    }
    run(
        [
            "kubectl",
            "delete",
            "pod",
            pod_name,
            "-n",
            "default",
            "--ignore-not-found=true",
        ]
    )
    subprocess.run(
        ["kubectl", "apply", "-f", "-"],
        input=json.dumps(manifest),
        text=True,
        capture_output=True,
        check=True,
    )
    for _ in range(60):
        res = run(
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
            check=False,
        )
        phase = res.stdout.strip()
        if phase in ("Succeeded", "Failed"):
            break
        time.sleep(2)
    logs = run(["kubectl", "logs", pod_name, "-n", "default"], check=False)
    print(logs.stdout)
    run(
        [
            "kubectl",
            "delete",
            "pod",
            pod_name,
            "-n",
            "default",
            "--ignore-not-found=true",
        ]
    )
    if phase != "Succeeded":
        raise RuntimeError(f"Mounter pod on {node} failed: {logs.stderr}")


def main():
    parser = argparse.ArgumentParser(
        description="Provision and mount per-actor Hyperdisk pool for Arch 3 benchmarks."
    )
    parser.add_argument(
        "--node",
        type=str,
        default="",
        help="Specific node name to setup (defaults to all substrate nodes)",
    )
    parser.add_argument(
        "--count", type=int, default=5, help="Number of dedicated disks per node"
    )
    parser.add_argument(
        "--throughput",
        type=int,
        default=2400,
        help="Provisioned throughput MiB/s per disk (800 for Arch 3a, 2400 for Arch 3b)",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Detach and delete all actor-disk-* volumes",
    )
    args = parser.parse_args()

    if args.cleanup:
        cleanup_disks()
        return

    nodes = [args.node] if args.node else get_substrate_nodes()
    print(f"Target nodes: {nodes}")
    for node in nodes:
        setup_disks_for_node(node, args.count, args.throughput)


if __name__ == "__main__":
    main()
