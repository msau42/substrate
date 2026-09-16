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


import concurrent.futures


def get_substrate_nodes(nodepool: str = "") -> list[str]:
    cmd = ["kubectl", "get", "nodes"]
    if nodepool:
        cmd.extend(["-l", f"cloud.google.com/gke-nodepool={nodepool}"])
    cmd.extend(["-o", "jsonpath={.items[*].metadata.name}"])
    p = run(cmd)
    nodes = p.stdout.strip().split()
    return sorted([n for n in nodes if n])


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
    # Group by instance to detach
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
    if disks:
        disk_names = [d["name"] for d in disks]
        for i in range(0, len(disk_names), 25):
            chunk = disk_names[i : i + 25]
            print(f"Batch deleting disks: {chunk[0]}..{chunk[-1]}...")
            run(
                [
                    "gcloud",
                    "compute",
                    "disks",
                    "delete",
                    *chunk,
                    f"--zone={ZONE}",
                    f"--project={PROJECT_ID}",
                    "--quiet",
                ],
                check=False,
            )


def run_node_script(node: str, script: str, suffix: str = "mounter"):
    pod_name = f"disk-pool-{suffix}-{node[-8:]}"
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
                        script,
                    ],
                }
            ],
        },
    }
    run(["kubectl", "delete", "pod", pod_name, "-n", "default", "--ignore-not-found=true"], check=False)
    subprocess.run(
        ["kubectl", "apply", "-f", "-"],
        input=json.dumps(manifest),
        text=True,
        capture_output=True,
        check=True,
    )
    phase = ""
    for _ in range(90):
        res = run(
            ["kubectl", "get", "pod", pod_name, "-n", "default", "-o", "jsonpath={.status.phase}"],
            check=False,
        )
        phase = res.stdout.strip()
        if phase in ("Succeeded", "Failed"):
            break
        time.sleep(2)
    logs = run(["kubectl", "logs", pod_name, "-n", "default"], check=False)
    print(f"[{node}] {suffix} output:\n{logs.stdout.strip()}")
    run(["kubectl", "delete", "pod", pod_name, "-n", "default", "--ignore-not-found=true"], check=False)
    if phase != "Succeeded":
        raise RuntimeError(f"Pod {pod_name} on {node} failed: {logs.stderr}")


def setup_disks_for_node(
    node: str,
    start_idx: int,
    count: int,
    throughput_mbps: int,
    size_gb: int = 10,
    iops: int = 3000,
):
    print(f"[{node}] Checking existing disks for range {start_idx}..{start_idx + count - 1}...")
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
    existing_disks = {}
    if p.returncode == 0 and p.stdout.strip():
        for d in json.loads(p.stdout):
            existing_disks[d["name"]] = d

    # Do not mix disk types: all actor disks must be hyperdisk-balanced.
    to_delete = []
    for i in range(count):
        disk_name = f"actor-disk-{start_idx + i}"
        if disk_name in existing_disks:
            cur_type = existing_disks[disk_name].get("type", "").split("/")[-1]
            if cur_type != "hyperdisk-balanced":
                to_delete.append(disk_name)

    if to_delete:
        print(f"[{node}] Detaching and deleting {len(to_delete)} non-hyperdisk volumes...")
        for dname in to_delete:
            users = existing_disks[dname].get("users", [])
            for u in users:
                inst = u.split("/")[-1]
                run(
                    [
                        "gcloud",
                        "compute",
                        "instances",
                        "detach-disk",
                        inst,
                        f"--disk={dname}",
                        f"--zone={ZONE}",
                        f"--project={PROJECT_ID}",
                    ],
                    check=False,
                )
        run(
            [
                "gcloud",
                "compute",
                "disks",
                "delete",
                *to_delete,
                f"--zone={ZONE}",
                f"--project={PROJECT_ID}",
                "--quiet",
            ],
            check=False,
        )
        for dname in to_delete:
            existing_disks.pop(dname, None)

    to_create_hd = []
    for i in range(count):
        disk_name = f"actor-disk-{start_idx + i}"
        if disk_name not in existing_disks:
            to_create_hd.append(disk_name)

    for i in range(0, len(to_create_hd), 25):
        chunk = to_create_hd[i : i + 25]
        print(
            f"[{node}] Batch creating {len(chunk)} hyperdisk-balanced disks ({chunk[0]}..{chunk[-1]}): {size_gb}GB, {iops} IOPS, {throughput_mbps} MiB/s..."
        )
        run(
            [
                "gcloud",
                "compute",
                "disks",
                "create",
                *chunk,
                f"--project={PROJECT_ID}",
                f"--zone={ZONE}",
                f"--size={size_gb}GB",
                "--type=hyperdisk-balanced",
                f"--provisioned-iops={iops}",
                f"--provisioned-throughput={throughput_mbps}",
            ]
        )

    # 1. Unmount all existing pool mounts on the host first
    unmount_script = """
set -e
mkdir -p /var/lib/ateom-gvisor/disk-pool
for d in /var/lib/ateom-gvisor/disk-pool/*; do
    if [ -d "$d" ]; then
        umount -lf "$d" || true
        rm -rf "$d" || true
    fi
done
rm -f /var/lib/ateom-gvisor/disk-pool/.detached-pool.json
rm -rf /var/lib/ateom-gvisor/actors/*
echo "Unmounted all pool disks."
"""
    run_node_script(node, unmount_script, "unmount")

    # Keep at most 29 disks attached initially (29 actor disks + 1 boot = 30 <= 32 limit, 2 free slots).
    init_attached = min(count, 29)
    attached_indices = list(range(start_idx, start_idx + init_attached))
    detached_indices = list(range(start_idx + init_attached, start_idx + count))
    detached_names = [f"actor-disk-{idx}" for idx in detached_indices]
    desired_attached_set = {f"actor-disk-{idx}" for idx in attached_indices}

    # 2. Detach actor-disk-* currently attached to this node
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
    currently_attached = [
        d.get("deviceName")
        for d in inst_data.get("disks", [])
        if d.get("deviceName", "").startswith("actor-disk-")
    ]
    to_detach = currently_attached if len(to_create_hd) > 0 else [d for d in currently_attached if d not in desired_attached_set]
    if to_detach:
        print(f"[{node}] Detaching {len(to_detach)} actor disks...")
        for dev in to_detach:
            run(
                [
                    "gcloud",
                    "compute",
                    "instances",
                    "detach-disk",
                    node,
                    f"--device-name={dev}",
                    f"--zone={ZONE}",
                    f"--project={PROJECT_ID}",
                ],
                check=False,
            )
    currently_attached_set = set(currently_attached) - set(to_detach)

    # 3. Format detached_indices in batches of <= 25 disks only if newly created
    if len(to_create_hd) > 0:
        for chunk_offset in range(0, len(detached_indices), 25):
            chunk = detached_indices[chunk_offset : chunk_offset + 25]
            print(f"[{node}] Attaching batch of {len(chunk)} detached-pool disks ({chunk[0]}..{chunk[-1]}) for formatting...")
            for idx in chunk:
                dname = f"actor-disk-{idx}"
                run(
                    [
                        "gcloud",
                        "compute",
                        "instances",
                        "attach-disk",
                        node,
                        f"--project={PROJECT_ID}",
                        f"--zone={ZONE}",
                        f"--disk={dname}",
                        f"--device-name={dname}",
                    ]
                )
            chunk_str = " ".join(str(x) for x in chunk)
            fmt_script = f"""
set -e
for IDX in {chunk_str}; do
    DISK_NAME="actor-disk-${{IDX}}"
    DEV="/dev/disk/by-id/google-${{DISK_NAME}}"
    mkfs.ext4 -F -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard "${{DEV}}" >/dev/null 2>&1 &
done
wait
for IDX in {chunk_str}; do
    DISK_NAME="actor-disk-${{IDX}}"
    DEV="/dev/disk/by-id/google-${{DISK_NAME}}"
    MNT="/var/lib/ateom-gvisor/disk-pool/${{DISK_NAME}}"
    mkdir -p "${{MNT}}"
    mount -o discard,defaults "${{DEV}}" "${{MNT}}"
    rm -rf "${{MNT}}"/*
    cat > "${{MNT}}/.disk-metadata.json" <<EOF
{{"gceDiskName": "${{DISK_NAME}}", "deviceName": "${{DISK_NAME}}"}}
EOF
    chmod 755 "${{MNT}}"
    umount "${{MNT}}"
    rm -rf "${{MNT}}"
done
echo "Formatted and unmounted {len(chunk)} detached-pool disks."
"""
            run_node_script(node, fmt_script, "fmt-detached")
            print(f"[{node}] Detaching formatted batch ({chunk[0]}..{chunk[-1]})...")
            for idx in chunk:
                dname = f"actor-disk-{idx}"
                run(
                    [
                        "gcloud",
                        "compute",
                        "instances",
                        "detach-disk",
                        node,
                        f"--device-name={dname}",
                        f"--zone={ZONE}",
                        f"--project={PROJECT_ID}",
                    ]
                )

    # 4. Attach and mount the initial attached disks (init_attached = 29) and write .detached-pool.json
    to_attach = [idx for idx in attached_indices if f"actor-disk-{idx}" not in currently_attached_set]
    if to_attach:
        print(f"[{node}] Attaching {len(to_attach)} active pool disks...")
        for idx in to_attach:
            dname = f"actor-disk-{idx}"
            run(
                [
                    "gcloud",
                    "compute",
                    "instances",
                    "attach-disk",
                    node,
                    f"--project={PROJECT_ID}",
                    f"--zone={ZONE}",
                    f"--disk={dname}",
                    f"--device-name={dname}",
                ]
            )

    attached_str = " ".join(str(x) for x in attached_indices)
    detached_json = json.dumps(detached_names)
    mount_script = f"""
set -e
mkdir -p /var/lib/ateom-gvisor/disk-pool
if ! findmnt -n -o PROPAGATION /var/lib/ateom-gvisor | grep -q shared; then
    mount --bind /var/lib/ateom-gvisor /var/lib/ateom-gvisor || true
    mount --make-rshared /var/lib/ateom-gvisor || true
fi
mkdir -p /var/lib/ateom-gvisor/disk-pool

for IDX in {attached_str}; do
    DISK_NAME="actor-disk-${{IDX}}"
    DEV="/dev/disk/by-id/google-${{DISK_NAME}}"
    MNT="/var/lib/ateom-gvisor/disk-pool/${{DISK_NAME}}"
    mkdir -p "${{MNT}}"
    if ! mount -o discard,defaults "${{DEV}}" "${{MNT}}" 2>/dev/null; then
        mkfs.ext4 -F -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard "${{DEV}}" >/dev/null 2>&1
        mount -o discard,defaults "${{DEV}}" "${{MNT}}"
    fi
    rm -rf "${{MNT}}"/*
    cat > "${{MNT}}/.disk-metadata.json" <<EOF
{{"gceDiskName": "${{DISK_NAME}}", "deviceName": "${{DISK_NAME}}"}}
EOF
    chmod 755 "${{MNT}}"
done

cat > /var/lib/ateom-gvisor/disk-pool/.detached-pool.json <<'EOF'
{detached_json}
EOF

rm -rf /var/lib/ateom-gvisor/actors/*
echo "Mounted $(df -h | grep -c disk-pool) disks on {node}, with {len(detached_names)} detached disks in .detached-pool.json."
"""
    run_node_script(node, mount_script, "mount-active")


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
        "--nodepool",
        type=str,
        default="",
        help="Optional GKE nodepool label filter",
    )
    parser.add_argument(
        "--count", type=int, default=5, help="Number of dedicated disks per node"
    )
    parser.add_argument(
        "--size", type=int, default=20, help="Size in GB per disk (default 20GB)"
    )
    parser.add_argument(
        "--iops", type=int, default=10000, help="Provisioned IOPS per disk"
    )
    parser.add_argument(
        "--throughput",
        type=int,
        default=800,
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

    nodes = [args.node] if args.node else get_substrate_nodes(args.nodepool)
    print(f"Target nodes: {nodes}")

    unmount_all_script = """
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
    for n in nodes:
        run_node_script(n, unmount_all_script, "pre-unmount")

    # First, detach any disks that migrated to a different node during a previous run
    res = run(
        [
            "gcloud",
            "compute",
            "disks",
            "list",
            f"--project={PROJECT_ID}",
            f"--filter=name~'^actor-disk-' AND zone:({ZONE})",
            "--format=json",
        ]
    )
    all_disks = {d["name"]: d for d in json.loads(res.stdout)}
    for idx, node in enumerate(nodes):
        for i in range(args.count):
            dname = f"actor-disk-{idx * args.count + i}"
            dinfo = all_disks.get(dname)
            if dinfo:
                for u in dinfo.get("users", []):
                    attached_node = u.split("/")[-1]
                    if attached_node != node:
                        print(f"Detaching migrated disk {dname} from {attached_node} (belongs to {node})...")
                        run(
                            [
                                "gcloud",
                                "compute",
                                "instances",
                                "detach-disk",
                                attached_node,
                                f"--disk={dname}",
                                f"--zone={ZONE}",
                                f"--project={PROJECT_ID}",
                            ],
                            check=False,
                        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(nodes)) as executor:
        futures = []
        for idx, node in enumerate(nodes):
            futures.append(
                executor.submit(
                    setup_disks_for_node,
                    node,
                    idx * args.count,
                    args.count,
                    args.throughput,
                    args.size,
                    args.iops,
                )
            )
        for f in concurrent.futures.as_completed(futures):
            f.result()


if __name__ == "__main__":
    main()
