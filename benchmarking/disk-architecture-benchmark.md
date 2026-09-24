# Storage Architecture Benchmarking Plan & Empirical Study: Boot Disk vs. Local NVMe vs. Per-Actor Hyperdisk

## 1. Executive Summary & Objectives

In our initial `PauseActor` vs. `SuspendActor` benchmarks on `c3-standard-8` nodes (`500 GB hyperdisk-balanced`), we identified two critical storage bottlenecks:
1. **`SuspendActor` Local Staging Contention**: Even though `SuspendActor` targets GCS, gVisor first writes the entire checkpoint and `durable-dir.tar` to node-local storage (`/var/lib/ateom-gvisor/actors/<actorUID>/checkpoint-state`) before uploading to GCS. During 1 GiB memory scaling runs, `SuspendActor` drove the shared boot disk to **745.4 MiB/s (87.8% of limit)** and **10.90 ms disk latency**.
2. **VM Aggregate Hyperdisk Bus Saturation**: On `c3-standard-8`, the VM itself imposes an **800 MiB/s (`839 MB/s`) aggregate Hyperdisk ceiling** across all attached Hyperdisk volumes. During 1 GiB `PauseActor` runs, write bursts hit **800.4 MiB/s (100% of the VM limit)**, spiking disk queue depth to `15.70` and latency to `13.20 ms`.

This document defines the repeatable benchmarking plan, empirical results on 1 active worker node, single-node density limit study, and step-by-step runbook evaluating **four storage configurations** (`Arch 1`, `Arch 2`, `Arch 3a`, `Arch 3b`).

---

## 2. The Four Target Storage Architectures

| Architecture | Hibernation Verb | Local Storage Topology | Primary Bottleneck Being Tested | Key Architectural Benefit |
| :--- | :--- | :--- | :--- | :--- |
| **Arch 1: Baseline**<br>*(Single Tuned Boot Disk)* | **`SuspendActor`**<br>(to GCS) | Single shared `100 GB hyperdisk-balanced` boot disk mounted at `/var/lib/ateom-gvisor`<br>*(Tuned to VM max: `800 MiB/s`, `50k IOPS` on C3-8)* | Shared filesystem journal (`ext4`), single block queue, VM Hyperdisk bus cap (`800 MiB/s`), + GCS upload/download | Zero extra infrastructure or disk management; standard GKE node configuration. |
| **Arch 2: Local NVMe Staging**<br>*(Suspend + Local SSDs)* | **`SuspendActor`**<br>(to GCS) | Node pool with `c3-standard-8-lssd` (GKE-managed NVMe RAID-0 array `/dev/md0`) backing `/var/lib/ateom-gvisor` | Pure **GCS network upload/download** and gVisor CPU compression (local disk bottleneck eliminated) | Bypasses the 800 MiB/s Hyperdisk VM bus limit completely; delivers **~2,600 MiB/s write** and **<0.1 ms** local staging latency. |
| **Arch 3a: Per-Actor Hyperdisk (C3-8)**<br>*(Pause + Dedicated Disk/Actor)* | **`PauseActor`**<br>(Local Block) | Dedicated `hyperdisk-balanced` volume (`100 GB`, `50k IOPS`, `800 MiB/s`) assigned per actor by `atelet` on `c3-standard-8` | Multi-disk VM bus scaling under `c3-standard-8`'s **800 MiB/s (`839 MB/s`)** aggregate Hyperdisk ceiling | Eliminates GCS network transfer and isolates per-actor filesystems/journals (`ext4`) and block queues. |
| **Arch 3b: Per-Actor Hyperdisk (C3-44)**<br>*(Pause + Dedicated Disk/Actor)* | **`PauseActor`**<br>(Local Block) | Dedicated `hyperdisk-balanced` volume (`100 GB`, `50k IOPS`, `2,400 MiB/s`) assigned per actor by `atelet` on `c3-standard-44` | Unthrottled parallel multi-disk throughput under `c3-standard-44`'s **2,400 MiB/s (`2,517 MB/s`)** VM bus ceiling | Eliminates both GCS network transfer and VM Hyperdisk bus throttling, allowing full-speed concurrent checkpointing. |

---

## 3. Architectural Deep Dive & Implementation Mechanics

### 3.1 Architecture 1: `Suspend` with Single Tuned Boot Disk (Baseline)
- **Provisioning**:
  ```bash
  go run ./tools/setup-gcp bootstrap \
    --cluster-name ate-bench \
    --machine-type c3-standard-8 \
    --boot-disk-size 100 \
    --boot-disk-type hyperdisk-balanced \
    --boot-disk-iops 50000 \
    --boot-disk-throughput 800
  ```
- **Data Path**:
  All actors share `/var/lib/ateom-gvisor` on `/dev/nvme0n1` (the boot Hyperdisk). Checkpoints are staged locally, uploaded to GCS, and deleted locally upon completion.

### 3.2 Architecture 2: `Suspend` with Local NVMe SSDs (`c3-standard-8-lssd`)
- **GKE `lssd` Mount Behavior & Verification**:
  - When provisioning `c3-standard-8-lssd`, GKE automatically formats and builds a RAID-0 array across the local NVMe SSDs (`2x 375 GB = 750 GB`) and mounts it on the host at `/mnt/stateful_partition/kube-ephemeral-disks`.
  - To ensure `/var/lib/ateom-gvisor` is backed by the local SSD RAID device (`/dev/md0`), bind-mount `/mnt/stateful_partition/kube-ephemeral-disks/ateom-gvisor` to `/var/lib/ateom-gvisor` on the host before starting `atelet`.

### 3.3 Architecture 3: `Pause` with One Dedicated Hyperdisk per Actor (`atelet` Integration & Cross-Node Migration)
- **Modifying `atelet` for Per-Actor Disk Assignment**:
  - Because `/var/lib/ateom-gvisor` is mounted into `atelet` (`mountPropagation: Bidirectional`) and all `ateom` worker pods (`mountPropagation: HostToContainer`), any disk mounted under `/var/lib/ateom-gvisor/disk-pool/<diskName>` is visible at the identical path inside both `atelet` and `ateom` containers.
  - When `--actor-disk-pool-dir=/var/lib/ateom-gvisor/disk-pool` is enabled in `cmd/atelet/main.go`:
    1. Pre-attach and mount $N$ dedicated `hyperdisk-balanced` volumes (`actor-disk-<idx>`) under `/var/lib/ateom-gvisor/disk-pool/` on the host using `benchmarking/scripts/disk-arch/setup_disk_pool.py`, which assigns globally unique, node-agnostic disk names (`actor-disk-0`, `actor-disk-1`, ...) across the cluster and writes self-describing `.disk-metadata.json` (`gceDiskName` and `deviceName`) onto each disk root.
    2. In `atelet`, `ActorDiskPool` maps `actorUID` $\leftrightarrow$ `diskPath` and symlinks `/var/lib/ateom-gvisor/actors/<actorUID>` $\to$ `<diskPath>/<actorUID>`.
- **Cross-Node Disk Migration when Previous Node is Out of Capacity**:
  - When a paused actor resumes (`ResumeActor`) and its previous node (`NodeVmsWithLocalSnapshots`) has no free worker capacity (`scheduling.ErrNoCapacity`), `assignWorkerAttempt` in `cmd/ateapi/internal/controlapi/workflow_resume.go` automatically falls back to scheduling the actor onto another node with available worker capacity.
  - Before invoking `Restore` on the target node's `atelet`, `ensureLocalSnapshotDiskMigrated` orchestrates cross-node block volume migration:
    1. Calls `ExportActorDisk` on the old node's `atelet`: unmounts the actor's dedicated Hyperdisk, removes it from the old node's `ActorDiskPool`, and calls GCE `instances.detachDisk`.
    2. Calls `ImportActorDisk` on the new node's `atelet`: calls GCE `instances.attachDisk`, mounts the block device under `/var/lib/ateom-gvisor/disk-pool/<gceDiskName>`, registers the disk in the new node's `ActorDiskPool`, and creates `/var/lib/ateom-gvisor/actors/<actorUID>` symlink.
    3. Updates the actor's `NodeVmsWithLocalSnapshots` in PostgreSQL to `[newNode]` and proceeds with local checkpoint restore on `newNode` without requiring object storage upload/download.

---

## 4. Benchmark Workload Matrix

All benchmarks are executed with **100% of `benchmark-ateom` worker pods and actor state pinned to 1 single active worker node**, while the Locust load generator runs isolated on the second node.

### Workload W1: High-Churn Single-Actor Memory Scaling (1 GiB RAM + 64 MiB Churn, 1 User)
- **Goal**: Measure single-actor peak write burst and determine:
  1. How Local NVMe (Arch 2) compares to a single tuned boot disk (Arch 1) for `SuspendActor`.
  2. How much faster `PauseActor` runs when the VM bus cap is raised from **800 MiB/s** (`c3-standard-8`, Arch 3a) to **2,400 MiB/s** (`c3-standard-44`, Arch 3b) on a dedicated Hyperdisk.

### Workload W2: Multi-Actor Concurrent Memory Burst (5 Users x 512 MiB RAM, ALL 5 Users on 1 Active Node)
- **Goal**: Stress-test concurrent disk write contention (`5 x 512 MiB = 2.5 GiB` concurrent state on 1 node).
- **What it Reveals**:
  - **Arch 1 (`Suspend + Shared Boot Disk`)**: Severe journal & queue contention on the single disk + GCS upload contention.
  - **Arch 2 (`Suspend + Local NVMe`)**: Eliminates local disk contention; measures pure concurrent GCS upload scaling.
  - **Arch 3a (`Pause + Per-Actor Hyperdisk on c3-standard-8`)**: Tests per-filesystem isolation under the 800 MiB/s VM bus cap when 5 disks flush concurrently.
  - **Arch 3b (`Pause + Per-Actor Hyperdisk on c3-standard-44`)**: Tests true parallel multi-disk throughput under the **2,400 MiB/s** VM bus ceiling.

---

## 5. Comparison & Empirical Results Tables (1 Active Node, Fresh Build `5784f12b`)

All workloads below were executed with **100% of `benchmark-ateom` worker pods and actor state pinned to 1 single active worker node** (while the Locust load generator ran isolated on the second node), using freshly built Substrate and benchmark binaries (`5784f12b`). Only **`p90` percentage differences** relative to the `Arch 1` baseline are shown for Hibernate and Resume latencies.

### 5.1 Workload W1: 1 GiB Memory Scaling (1 User, 1 GiB RAM + 64 MiB Churn, 1 Active Node)

| Metric | Arch 1: `Suspend` + 100 GB Tuned Boot Disk (`c3-standard-8`, 800 MiB/s Cap) *(BASELINE)* | Arch 2: `Suspend` + Local NVMe SSD (`c3-standard-8-lssd`, 750 GB RAID-0) *(MEASURED)* | Arch 3a: `Pause` + 1 HD/Actor (`c3-standard-8`, 800 MiB/s Cap) *(MEASURED)* | Arch 3b: `Pause` + 1 HD/Actor (`c3-standard-44`, 2,400 MiB/s Cap) *(MEASURED)* |
| :--- | :--- | :--- | :--- | :--- |
| **Hibernate Latency**<br>*(Avg / Med / p90 / p95)* | **4,224 / 4,300 / 5,000 / 5,000 ms** | **5,055 / 5,100 / 6,100 / 6,100 ms**<br>*(**+22% p90**)* | **2,277 / 2,300 / 3,300 / 4,500 ms**<br>*(**-34% p90**)* | **1,255 / 1,100 / 1,700 / 3,000 ms**<br>*(**-66% p90**)* |
| **Resume Latency**<br>*(Avg / Med / p90 / p95)* | **2,310 / 2,100 / 2,700 / 2,700 ms** | **2,435 / 2,400 / 2,900 / 2,900 ms**<br>*(**+7% p90**)* | **1,072 / 1,100 / 1,100 / 1,100 ms**<br>*(**-59% p90**)* | **1,060 / 1,100 / 1,100 / 1,100 ms**<br>*(**-59% p90**)* |
| **Total Cycle Overhead**<br>*(Avg / Med)* | **6,534 ms avg / 6,400 ms med** | **7,490 ms avg / 7,500 ms med** | **3,349 ms avg / 3,400 ms med** | **2,315 ms avg / 2,200 ms med** |
| **1s Peak Disk Write**<br>*(60s Avg Write)* | **228.3 MiB/s**<br>*(80.6 MiB/s 60s avg)* | **PCIe NVMe Passthrough (`/dev/md0`)** | **800+ MiB/s VM cap**<br>*(148.7 MiB/s 60s avg)* | **1,761.4 MiB/s** *(2.20x c3-8 cap)*<br>*(274.4 MiB/s 60s avg)* |
| **Disk Latency / Queue Depth** | **16.16 ms / QDepth 12.09** | **Boot disk idle: `< 0.40 ms` / QDepth `< 0.05`** | **67.44 ms / QDepth 41.78**<br>*(Boot disk idle: 0.44 ms)* | **12.37 ms / QDepth 9.18**<br>*(Boot disk idle: 0.44 ms)* |
| **Throughput (1-Min Run)** | **7 Suspend / 7 Resume cycles/min**<br>*(0% errors)* | **6 Suspend / 6 Resume cycles/min**<br>*(**-14% throughput**, 0% errors)* | **12 Pause / 12 Resume cycles/min**<br>*(**+71% throughput**, 0% errors)* | **16 Pause / 16 Resume cycles/min**<br>*(**+129% throughput**, 0% errors)* |

---

### 5.2 Workload W2: Concurrent Memory Burst (5 Users x 512 MiB RAM + 32 MiB Churn, ALL 5 Users on 1 Active Node)

| Metric | Arch 1: `Suspend` + 100 GB Tuned Boot Disk (`c3-standard-8`, 800 MiB/s Cap) *(BASELINE)* | Arch 2: `Suspend` + Local NVMe SSD (`c3-standard-8-lssd`, 750 GB RAID-0) *(MEASURED)* | Arch 3a: `Pause` + 1 HD/Actor (`c3-standard-8`, 800 MiB/s Cap) *(MEASURED)* | Arch 3b: `Pause` + 1 HD/Actor (`c3-standard-44`, 2,400 MiB/s Cap) *(MEASURED)* |
| :--- | :--- | :--- | :--- | :--- |
| **Hibernate Latency**<br>*(Avg / Med / p90 / p95)* | **3,607 / 3,400 / 5,100 / 5,200 ms** | **3,162 / 3,000 / 3,800 / 4,700 ms**<br>*(**-25% p90**)* | **982 / 740 / 2,200 / 2,600 ms**<br>*(**-57% p90**)* | **701 / 640 / 770 / 1,000 ms**<br>*(**-85% p90**)* |
| **Resume Latency**<br>*(Avg / Med / p90 / p95)* | **2,031 / 1,900 / 3,000 / 3,400 ms** | **2,054 / 1,800 / 2,900 / 3,800 ms**<br>*(**-3% p90**)* | **1,523 / 1,500 / 2,500 / 2,900 ms**<br>*(**-17% p90**)* | **439 / 430 / 480 / 490 ms**<br>*(**-84% p90**)* |
| **Total Cycle Overhead**<br>*(Avg / Med)* | **5,638 ms avg / 5,300 ms med** | **5,216 ms avg / 4,800 ms med** | **2,505 ms avg / 2,240 ms med** | **1,140 ms avg / 1,070 ms med** |
| **1s Peak Disk Write**<br>*(60s Avg Write)* | **804.4 MiB/s** *(100.6% of VM cap)*<br>*(506.4 MiB/s 60s avg)* | **PCIe NVMe Passthrough (`/dev/md0`)** | **826.8 MiB/s combined 1s peak** *(103.4% VM cap)*<br>*(233.6 MiB/s 60s avg)* | **1,708.6 MiB/s combined 1s peak** *(71.2% VM cap)*<br>*(0% bus throttling)* |
| **Disk Latency / Queue Depth** | **20.39 ms / QDepth 40.19** | **Boot disk idle: `< 0.40 ms` / QDepth `< 0.05`** | **127.88 ms max / Sum QDepth 70.55**<br>*(Boot disk collateral latency: 6.70 ms)* | **11.34 ms max (`3.27–4.73 ms` disks 1–4) / Sum QDepth 3.57**<br>*(Boot disk idle: `0.47 ms`)* |
| **Throughput (1-Min Run)** | **42 Suspend / 38 Resume cycles/min**<br>*(0% errors)* | **45 Suspend / 42 Resume cycles/min**<br>*(**+7% throughput**, 0% errors)* | **77 Pause / 74 Resume cycles/min**<br>*(**+83% throughput**, 0% errors)* | **124 Pause / 122 Resume cycles/min**<br>*(**+195% throughput — 3.0x Arch 1**, 0% errors)* |

---

## 6. Key Architectural Findings & Production Recommendations

### 1. Local NVMe Staging (`Arch 2`) vs. Tuned Boot Disk (`Arch 1`) for `SuspendActor`
- **Multi-Actor Contention Elimination on 1 Active Node (`W2`)**: When all 5 concurrent users run on **1 single active node**, staging snapshots on a shared 100 GB tuned boot disk (`Arch 1`) saturates the `800 MiB/s` VM storage bus (`804.4 MiB/s` peak — `100.6%` of VM cap, Queue Depth `40.19`, Latency `20.39 ms`). Switching `/var/lib/ateom-gvisor` to local NVMe RAID-0 (`Arch 2`) bypasses the Persistent Disk/Hyperdisk network storage bus entirely, reducing `SuspendActor` p90 latency by **25%** (`5,100 ms` $\to$ `3,800 ms`) while leaving the boot disk **100% idle** (`< 0.40 ms`, `QDepth < 0.05`).
- **Single-Actor GCS Network Variability (`W1`)**: For a single 1 GiB actor (`W1`), local NVMe provides no net improvement (`6,100 ms` p90 vs `5,000 ms` p90) because single-actor `SuspendActor` time is dominated by sequential GCS network upload/download variability rather than local disk queue contention.

### 2. `PauseActor` with Dedicated Hyperdisk per Actor (`Arch 3a` vs. `Arch 3b`)
- **Massive Latency & Throughput Leap over `SuspendActor`**: By eliminating object storage transfers and persisting checkpoints directly to dedicated per-actor Hyperdisks (`--actor-disk-pool-dir`), `PauseActor` on `Arch 3b` reduces p90 hibernate latency for 5 concurrent `512 MiB` actors (`W2`) from `5,100 ms` (`Arch 1`) to **`770 ms`** (**`-85% p90`, 6.6x faster**) and p90 resume latency from `3,000 ms` to **`480 ms`** (**`-84% p90`, 6.25x faster**). Total cycle overhead drops from `5,638 ms` to **`1,140 ms`** (**4.9x faster**), driving a **+195% increase in single-node system throughput** (`42` $\to$ **124 cycles/min**, **3.0x Arch 1**).
- **Why Pinning All 5 Users to 1 Active Node Amplifies the VM Bus Ceiling Difference (`Arch 3a` vs `Arch 3b`)**:
  - When **all 5 concurrent W2 actors (`2.5 GiB` total state)** are forced onto **1 single active node**:
    - On **`c3-standard-8` (`Arch 3a`, `800 MiB/s` cap)**, the 5 dedicated Hyperdisks severely contend for the `800 MiB/s` VM-wide storage shaper (`826.8 MiB/s` combined peak — `103.4%` of VM cap, `Sum QDepth 70.55`, `Max Latency 127.88 ms`, idle boot disk collateral latency `6.70 ms`). This bus saturation pushes `PauseActor` p90 to **`2,200 ms`** (`-57% p90` vs Arch 1) and `ResumeActor` p90 to **`2,500 ms`** (`-17% p90` vs Arch 1), yielding **`77 Pause / 74 Resume cycles/min`** (`+83%` vs Arch 1).
    - On **`c3-standard-44` (`Arch 3b`, `2,400 MiB/s` cap)**, the 5 dedicated Hyperdisks burst concurrently to **`1,708.6 MiB/s`** (`71.2%` of the `2,400 MiB/s` VM ceiling) with **zero VM bus throttling** (`Sum QDepth 3.57`, `Max Latency 11.34 ms`, idle boot disk `0.47 ms`). Consequently, `PauseActor` p90 stays at **`770 ms`** (**`-85% p90` vs Arch 1**, **`-65% p90` vs Arch 3a**) and `ResumeActor` p90 stays at **`480 ms`** (**`-84% p90` vs Arch 1**, **`-81% p90` vs Arch 3a**), achieving **`124 Pause / 122 Resume cycles/min`** (**`+195%` throughput over Arch 1** and **`+61%` higher single-node throughput than Arch 3a**).

---

## 7. Single-Node Density Limit Study (`W2`, Arch 3b on `c3-standard-44`)

To find the single-node density limit for `Arch 3b` (`PauseActor` with one dedicated `100 GB, 50k IOPS, 2,400 MiB/s hyperdisk-balanced` volume per actor on `c3-standard-44`), we isolated a single node (`m1xc`: `44 vCPUs, 176 GiB RAM, 2,400 MiB/s (2,516.6 MB/s) aggregate Hyperdisk bus ceiling`), provisioned 25 dedicated Hyperdisks (`actor-disk-0..24`) and 25 `benchmark-ateom` worker pods on `m1xc`, and pinned the Locust load generator to the second node (`hz9x`). We swept concurrency $N \in \{3, 5, 10, 12, 15, 20, 25\}$ concurrent `W2` actors (`512 MiB RAM working set + 32 MiB churn/cycle`) while capturing both **1-second kernel `/proc/diskstats` aggregate telemetry** and 60-second Cloud Monitoring metrics.

### 7.1 Empirical Single-Node Concurrency Sweep Table (with 1-Second Kernel Aggregate Telemetry)

| Single-Node Concurrency ($N$ Users) | `PauseActor` Latency (Med / Avg / p95 / p99) | `ResumeActor` Latency (Med / Avg / p95 / p99) | Single-Node Throughput (Cycles/Min) | 1s Aggregate Write Throughput (Peak / Active Mean) | 1s Aggregate IO Queue Depth (`aqu-sz` Peak / `in_flight` Peak) | 1s Aggregate Write Latency (Actor Peak / Active Mean / Boot Disk Peak) | 60s Cloud Mon Avg Write | Node CPU Util / Peak `iowait%` |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **$N = 3$ Users** *(Baseline)* | **580 / 626 / 800 / — ms** | **420 / 418 / 450 / — ms** | **78 Pause / 76 Resume** | **1,049.2 MiB/s** (`1,100.1 MB/s`)<br>*(Active Mean: `278.2 MiB/s`)* | **24.03 `aqu-sz`**<br>*(Peak In-Flight: `98`)* | **8.81 ms / 5.43 ms**<br>*(Boot Disk Peak: `2.30 ms`)* | `292 MiB/s` | **~12% CPU**<br>*(1.9% iowait)* |
| **$N = 5$ Users** | **660 / 785 / 1,800 / 2,800 ms** | **430 / 443 / 510 / 540 ms** | **120 Pause / 117 Resume** | **1,033.2 MiB/s** (`1,083.4 MB/s`)<br>*(Active Mean: `376.6 MiB/s`)* | **52.07 `aqu-sz`**<br>*(Peak In-Flight: `85`)* | **10.91 ms / 4.81 ms**<br>*(Boot Disk Peak: `2.35 ms`)* | `203 MiB/s` | **19.5% CPU**<br>*(4.0% iowait)* |
| **$N = 10$ Users** | **700 / 793 / 1,400 / 2,900 ms** | **480 / 477 / 540 / 570 ms** | **226 Pause / 218 Resume** | **1,700.2 MiB/s** (`1,782.7 MB/s`)<br>*(Active Mean: `585.1 MiB/s`)* | **70.82 `aqu-sz`**<br>*(Peak In-Flight: `189`)* | **10.67 ms / 4.80 ms**<br>*(Boot Disk Peak: `13.24 ms`)* | `250 MiB/s` | **25.1% CPU**<br>*(5.2% iowait)* |
| **$N = 12$ Users** | **740 / 854 / 1,800 / 2,800 ms** | **500 / 501 / 580 / 610 ms** | **257 Pause / 250 Resume** | **2,326.2 MiB/s** (`2,439.2 MB/s`)<br>*(96.9% VM cap; Mean: `889.4 MiB/s`)* | **193.26 `aqu-sz`**<br>*(Peak In-Flight: `401`)* | **19.89 ms / 6.37 ms**<br>*(Boot Disk Peak: `27.58 ms`)* | `290 MiB/s` | **35.8% CPU**<br>*(14.0% iowait)* |
| **$N = 15$ Users** *(Latency Sweet Spot)* | **760 / 950 / 2,300 / 2,900 ms** | **520 / 524 / 620 / 670 ms** | **297 Pause / 292 Resume** | **2,401.1 MiB/s** (`2,517.8 MB/s`)<br>*(**100.0% VM cap**; Mean: `668.4 MiB/s`)* | **187.92 `aqu-sz`**<br>*(Peak In-Flight: `111`)* | **18.70 ms / 3.96 ms**<br>*(Boot Disk Peak: `17.82 ms`)* | `385 MiB/s` | **39.2% CPU**<br>*(19.7% iowait)* |
| **$N = 20$ Users** *(Peak Throughput)* | **820 / 1,066 / 2,900 / 3,200 ms** | **620 / 668 / 1,000 / 1,100 ms** | **341 Pause / 329 Resume** *(11.36 transitions/sec)* | **2,407.0 MiB/s** (`2,523.9 MB/s`)<br>*(**100.3% VM cap**; Mean: `1,137.4 MiB/s`)* | **516.79 `aqu-sz`**<br>*(Peak In-Flight: `548`)* | **49.81 ms / 11.08 ms**<br>*(Boot Disk Peak: `48.55 ms`)* | `749 MiB/s` | **42.9% CPU**<br>*(20.2% iowait)* |
| **$N = 25$ Users** *(Beyond Knee)* | **1,100 / 1,803 / 6,900 / 8,900 ms** | **1,600 / 1,390 / 2,300 / 2,600 ms** | **264 Pause / 249 Resume** *(-23% retrograde drop)* | **2,405.4 MiB/s** (`2,522.2 MB/s`)<br>*(**Clipped 27s straight**; Mean: `1,676.6 MiB/s`)* | **1,282.16 `aqu-sz`**<br>*(Peak In-Flight: `1,618`)* | **119.15–131.9 ms / 47.07 ms**<br>*(Boot Disk Peak: **`204.00 ms`**)* | `977 MiB/s` | **29.0% CPU**<br>*(**48.6–61.4% iowait**)* |

---

## 8. Repeatable Runbook (Step-by-Step Execution Guide)

To reproduce any of the 4 storage architectures (`Arch 1`, `Arch 2`, `Arch 3a`, `Arch 3b`) on 1 active worker node, follow the step-by-step instructions below. All helper scripts are checked into `benchmarking/scripts/disk-arch/`.

### Step 1: Build Fresh Binaries & Push Images
```bash
source .ate-dev-env.sh
make build
./benchmarking/locust/build_and_push.sh
```

### Step 2: Provision Node Pool for the Target Architecture

Choose the machine type and boot disk settings in `.ate-dev-env.sh`:
- **Arch 1 (`c3-standard-8` Tuned Boot Disk)**:
  ```bash
  export GVISOR_NODE_MACHINE_TYPE=c3-standard-8
  export BOOT_DISK_THROUGHPUT_MBPS=800
  ```
- **Arch 2 (`c3-standard-8-lssd` Local NVMe RAID-0)**:
  ```bash
  export GVISOR_NODE_MACHINE_TYPE=c3-standard-8-lssd
  export BOOT_DISK_THROUGHPUT_MBPS=800
  ```
- **Arch 3a (`c3-standard-8` + Dedicated Hyperdisks)**:
  ```bash
  export GVISOR_NODE_MACHINE_TYPE=c3-standard-8
  export BOOT_DISK_THROUGHPUT_MBPS=800
  ```
- **Arch 3b (`c3-standard-44` + Dedicated Hyperdisks)**:
  ```bash
  export GVISOR_NODE_MACHINE_TYPE=c3-standard-44
  export BOOT_DISK_THROUGHPUT_MBPS=2400
  ```

Provision/update the GKE node pool and deploy Substrate + benchmarks:
```bash
go run ./tools/setup-gcp bootstrap
./hack/install-ate.sh --deploy-atelet --deploy-benchmarks --benchmark-worker-count 6
```

### Step 3: Pin All `benchmark-ateom` Workers to 1 Single Active Node

Identify the two substrate nodes (`ACTIVE_NODE` and `LOADGEN_NODE`):
```bash
NODES=($(kubectl get nodes -l cloud.google.com/gke-nodepool=substrate-node-pool -o jsonpath='{.items[*].metadata.name}'))
export ACTIVE_NODE=${NODES[0]}
export LOADGEN_NODE=${NODES[1]}
echo "Active Node: $ACTIVE_NODE | Loadgen Node: $LOADGEN_NODE"
```

Pin `benchmark-ateom` worker pods to `$ACTIVE_NODE` so 100% of actor state and disk I/O run on a single node:
```bash
kubectl patch workerpool benchmark-ateom -n benchmark-workloads --type=merge \
  -p "{\"spec\":{\"template\":{\"spec\":{\"nodeSelector\":{\"kubernetes.io/hostname\":\"$ACTIVE_NODE\"}}}}}"
kubectl rollout status deployment/benchmark-ateom -n benchmark-workloads
```

### Step 4: Architecture-Specific Disk Setup on `$ACTIVE_NODE`

- **For Arch 1**: No extra setup required (uses `/var/lib/ateom-gvisor` on the boot disk). Ensure `--actor-disk-pool-dir` is removed from `manifests/ate-install/atelet.yaml` if previously enabled.
- **For Arch 2 (`c3-standard-8-lssd`)**: Bind-mount the GKE local NVMe RAID-0 array (`/dev/md0` at `/mnt/stateful_partition/kube-ephemeral-disks`) onto `/var/lib/ateom-gvisor` on `$ACTIVE_NODE`, then restart `atelet`.
- **For Arch 3a / Arch 3b (`Pause` + Dedicated Hyperdisk Pool)**:
  1. Ensure `manifests/ate-install/atelet.yaml` includes `- --actor-disk-pool-dir=/var/lib/ateom-gvisor/disk-pool` and redeploy `atelet` (`./hack/install-ate.sh --deploy-atelet`).
  2. Provision, attach, format (`ext4`), and mount 5 dedicated `hyperdisk-balanced` volumes (`actor-disk-0..4`) onto `$ACTIVE_NODE`:
     ```bash
     # For Arch 3a (800 MiB/s per disk):
     python3 benchmarking/scripts/disk-arch/setup_disk_pool.py --node "$ACTIVE_NODE" --count 5 --throughput 800

     # For Arch 3b (2400 MiB/s per disk):
     python3 benchmarking/scripts/disk-arch/setup_disk_pool.py --node "$ACTIVE_NODE" --count 5 --throughput 2400
     ```

### Step 5: Execute W1 and W2 Benchmarks

Run the automated benchmark runner script (`benchmarking/scripts/disk-arch/run_benchmark.py`), which automatically cleans up prior actors, clears the disk pool, restarts `atelet` on `$ACTIVE_NODE`, pins the Locust runner Job to `$LOADGEN_NODE`, and streams the Locust summary table (`Avg / Med / p90 / p95` and `reqs/s`):

```bash
# Architecture 1 (Suspend + Boot Disk)
python3 benchmarking/scripts/disk-arch/run_benchmark.py arch1_w1 arch1_w2

# Architecture 2 (Suspend + Local NVMe)
python3 benchmarking/scripts/disk-arch/run_benchmark.py arch2_w1 arch2_w2

# Architecture 3a (Pause + Dedicated Hyperdisks on c3-standard-8)
python3 benchmarking/scripts/disk-arch/run_benchmark.py arch3a_w1 arch3a_w2

# Architecture 3b (Pause + Dedicated Hyperdisks on c3-standard-44)
python3 benchmarking/scripts/disk-arch/run_benchmark.py arch3b_w1 arch3b_w2
```

### Step 6: Query Cloud Monitoring Per-Disk Telemetry

After a run completes, pass the UTC start and end timestamps to `check_disk_telemetry.py` to inspect 1-second peak write bursts (`ALIGN_MAX`), 60-second average write rates (`ALIGN_RATE`), per-disk IO latency, and node-aggregate queue depth:

```bash
python3 benchmarking/scripts/disk-arch/check_disk_telemetry.py 2026-09-15T02:55:00Z 2026-09-15T03:02:00Z
```

### Step 7: Cleanup Dedicated Hyperdisks
When finished benchmarking Architecture 3, detach and delete all `actor-disk-*` volumes:
```bash
python3 benchmarking/scripts/disk-arch/setup_disk_pool.py --cleanup
```

---

## 9. Extending the Disk Architecture Benchmark to MicroVM (`ateom-microvm`)

The disk architecture benchmark harness and `ActorDiskPool` (`--actor-disk-pool-dir=/var/lib/ateom-gvisor/disk-pool`) have been extended to run seamlessly on **microVM (`ateom-microvm` + Cloud Hypervisor + `virtiofsd`)** across all 4 storage architectures.

### 9.1 How `ateom-microvm` Interacts with Per-Actor Disk Pools (`Arch 3a / 3b`)
- **Identical Symlink Data Path**: Like `ateom-gvisor`, `ateom-microvm` mounts `ateompath.BasePath` (`/var/lib/ateom-gvisor`) with `MountPropagationHostToContainer` and resolves actor state paths via `ateompath.ActorPath(actorUID)`.
- **Transparent Dedicated Volume Routing**: When `atelet` allocates a dedicated Hyperdisk (`disk-i`) and creates the symlink `/var/lib/ateom-gvisor/actors/<actorUID>` $\to$ `/var/lib/ateom-gvisor/disk-pool/disk-i/<actorUID>`, `ateom-microvm`'s `CheckpointWorkload` automatically writes:
  1. Cloud Hypervisor guest memory ranges (`memory-ranges`, sparse guest physical memory dump)
  2. Device and VMM state (`state.json`, `config.json`)
  3. Container rootfs upper layer (`rootfs-upper.tar`)
  4. Durable directory volumes (`durable-dir.tar`)
  directly onto the actor's dedicated Hyperdisk volume (`disk-i`).

### 9.2 GKE Node Pool Requirements for MicroVM (`/dev/kvm` Nested Virtualization)
MicroVM requires hardware nested virtualization (`/dev/kvm`) on GKE worker nodes, which requires:
1. **`EnableNestedVirtualization = true`** on the GKE node pool (`AdvancedMachineFeatures`).
2. **`ImageType = UBUNTU_CONTAINERD`** (Container-Optimized OS does not expose `/dev/kvm`).
3. **`ActorMemory = 1536Mi`** on `ActorTemplate` specs for `W1` (`1 GiB` working set) to accommodate the `128 MiB` Cloud Hypervisor VMM reserve + guest kernel floor alongside the `1 GiB` application working set.

`tools/setup-gcp` supports this directly via `--enable-nested-virtualization` (`ENABLE_NESTED_VIRTUALIZATION=true`):

```bash
export ENABLE_NESTED_VIRTUALIZATION=true
export NODE_IMAGE_TYPE=UBUNTU_CONTAINERD
export GVISOR_NODE_MACHINE_TYPE=c3-standard-44  # or c3-standard-8 / c3-standard-8-lssd
export BOOT_DISK_THROUGHPUT_MBPS=2400

go run ./tools/setup-gcp bootstrap
```

### 9.3 Running W1 & W2 on MicroVM (`run_benchmark.py --sandbox-class microvm`)

Use `--sandbox-class microvm --deploy-workloads` with `run_benchmark.py`. The runner automatically:
1. Executes `./hack/install-microvm-deps.sh --install` (builds/stages Cloud Hypervisor, `virtiofsd`, guest kernel/rootfs to GCS and applies the `microvm` `SandboxConfig`).
2. Deploys the `benchmark-ateom` WorkerPool and ActorTemplates with `--sandbox-class microvm --actor-memory 1536Mi`.
3. Pins all `benchmark-ateom` microVM worker pods to `$ACTIVE_NODE` and isolates the Locust load generator on `$LOADGEN_NODE`.

```bash
# Deploy microVM WorkerPool + ActorTemplates (1536Mi memory limit) and pin to 1 active node, then run Arch 3b W1 & W2:
python3 benchmarking/scripts/disk-arch/run_benchmark.py \
  --sandbox-class microvm \
  --deploy-workloads \
  arch3b_w1 arch3b_w2

# Or invoke explicit *_microvm targets directly once deployed:
python3 benchmarking/scripts/disk-arch/run_benchmark.py arch1_w1_microvm arch1_w2_microvm
python3 benchmarking/scripts/disk-arch/run_benchmark.py arch2_w1_microvm arch2_w2_microvm
python3 benchmarking/scripts/disk-arch/run_benchmark.py arch3a_w1_microvm arch3a_w2_microvm
python3 benchmarking/scripts/disk-arch/run_benchmark.py arch3b_w1_microvm arch3b_w2_microvm
```

---

## 10. Single-Node Benchmark Extension: Simulating `Arch 3` with Parallel 1-Second Per-VM Hyperdisk Attach/Detach (`Arch 3c`)

### 10.1 Objectives & Simulation Model (`Async Detach on Pause + Sync Attach on Resume`, Parallel Across Disks)

In standard `Arch 3b` on a single `c3-standard-44` node ($W = 15$ workers, $N = 15$ concurrent W2 actors), all 15 actor Hyperdisks (`actor-disk-0..14`) remain permanently attached to `node1`, incurring **0 ms** of GCE disk attach/detach latency (`448.0 cycles/min`, `870 ms` median `PauseActor`, `690 ms` median `ResumeActor`).

To evaluate `Arch 3` when actor disks are dynamically detached on every `PauseActor` and re-attached on every `ResumeActor` with a **simulated 1-second (`1,000 ms`) Hyperdisk attach/detach latency executing in parallel per disk on the VM** (no per-VM `opMu` serialization):
1. **`PauseActor` (Async Parallel Detach in Background)**:
   - Writes the `544 MiB` checkpoint (`memory-ranges`, `state.json`, `durable-dir.tar`) to `/var/lib/ateom-gvisor/disk-pool/disk-i/<actorUID>` (`~870 ms` median) and **returns immediately** on the critical path, freeing the worker pod.
   - Spawns a per-actor background goroutine (`NotifyActorPaused`) that executes a **real kernel `syscall.Unmount`** (forcing `syncfs` writeback to the NVMe queue and evicting the filesystem's Linux page cache) and sleeps **`1,000 ms` (`1.0s`) in parallel** (without holding `g.opMu`) to simulate a parallel `detachDisk`.
2. **`ResumeActor` (Sync Parallel Attach on Critical Path)**:
   - Waits on `<-detachingDisks[disk-i]` only if `disk-i`'s own background `1.0s` detach has not yet finished (`0 ms` wait when actor idle time $\ge 1.0\text{s}$; `1,000 ms` wait under `0.0s` back-to-back stress).
   - Sleeps **`1,000 ms` (`1.0s`) in parallel** (without holding `g.opMu`) to simulate a parallel `attachDisk`.
   - Executes a **real kernel `syscall.Mount`** of `/dev/disk/by-id/google-actor-disk-i` (`~15 ms`) and runs `runsc restore` reading the `544 MiB` checkpoint **cold from the physical Hyperdisk** (`~690 ms` median).

### 10.2 Measured Single-Node W2 Performance (`c3-standard-44`, $W = 17$ Workers / $N = 15$ Actors, `180s`)

| Metric (`c3-standard-44`, W2 `544 MiB` State, $N=15$) | **Arch 1 (`Suspend` to Standard Regional GCS + Boot Disk)** *(Measured)* | **Arch 1 (`Suspend` to `RAPID` Zonal GCS Bucket + Boot Disk)** *(Measured, DirectPath gRPC)* | **Arch 2 (`Suspend` to GCS + `tmpfs`)** *(Measured)* | **Arch 3 (`0s` Pre-Attached Hyperdisks)** *(Measured)* | **Arch 3c (`1s` Parallel Attach/Detach, `1.0s` Wait)** *(Measured, n=301)* | **Arch 3c (`1s` Parallel Attach/Detach, `0.0s` Wait)** *(Measured, n=256)* |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **`PauseActor` / `SuspendActor` Latency** *(Med / Avg / p90)* | `3,900 / 4,328 / 6,500 ms` | **`2,400 / 2,659 / 3,800 ms`** *(**`1.63x` faster Med vs Standard GCS**)* | `5,700 / 5,564 / 7,500 ms` | `870 / 1,061 / 1,800 ms` | **`640 / 810 / 1,500 ms`** *(**`6.09x` faster Med vs Arch 1**; `0 ms` sync detach)* | **`620 / 767 / 1,300 ms`** *(**`6.29x` faster Med vs Arch 1**; `0 ms` sync detach)* |
| **Background `UnmountAndDetach` (`atelet_async_detach_ms`)** | `N/A` | `N/A` | `N/A` | `N/A` | **`1,707.5 ms` med / `1,652.0 ms` avg** *(`652 ms` NVMe `umount` flush + `1,000 ms` detach)* | **`1,699.9 ms` med / `1,674.0 ms` avg** *(`674 ms` NVMe `umount` flush + `1,000 ms` detach)* |
| **Wait for In-Flight Detach (`atelet_wait_detach_ms`)** | `0 ms` | `0 ms` | `0 ms` | `0 ms` | **`0.0 ms` med / `211.6 ms` avg** *(p90: `697.1 ms`)* | **`1,691.8 ms` med / `1,646.5 ms` avg** *(p90: `1,912.7 ms`)* |
| **Parallel `AttachAndMount` (`atelet_import_dur_ms`)** | `0 ms` | `0 ms` | `0 ms` | `0 ms` | **`1,020.6 ms` med / `1,027.4 ms` avg** *(p90: `1,038.2 ms` — zero `opMu` queue)* | **`1,021.4 ms` med / `1,040.3 ms` avg** *(p90: `1,125.0 ms` — zero `opMu` queue)* |
| **Checkpoint Stage / Cold NVMe Read (`atelet_restore_download_ms`)** | `2,430 ms` med *(Standard GCS)* | **`1,468.6 ms` med / `1,500.6 ms` avg** *(`RAPID` GCS; `953.5 ms` med upload)* | `4,510 ms` med *(GCS)* | `~480 ms` med *(Warm DRAM)* | **`2,584.6 ms` med / `2,663.5 ms` avg** *(`1,892.3 MiB/s` peak physical NVMe read!)* | **`2,305.2 ms` med / `2,381.8 ms` avg** *(`1,526.8 MiB/s` peak physical NVMe read!)* |
| **Server Warm Restore (`atelet_restore_total_ms` / `ateapi`)** | `2,700 / 2,851 / 4,200 ms` | **`1,857.9 / 1,877.4 / 2,262.1 ms`** | `4,900 / 4,747 / 6,800 ms` | `675 / 704 / 920 ms` | **`4,014.9 / 4,111.3 / 4,800.7 ms`** *(`ateapi`: `4,090.5 ms` med / `4,196.5 ms` avg)* | **`5,259.8 / 5,288.4 / 5,869.0 ms`** *(`ateapi`: `5,241.3 ms` med / `5,089.5 ms` avg)* |
| **Client Warm `ResumeActor` Latency** *(Med / Avg / p90)* | `2,700 / 2,851 / 4,200 ms` | **`1,900 / 1,890 / 2,300 ms`** *(**`-29.6%` Med / `-45.2%` p90**)* | `4,900 / 4,747 / 6,800 ms` | `690 / 719 / 940 ms` | **`5,200 / 6,319 / 11,000 ms`** | **`5,400 / 6,181 / 12,000 ms`** |
| **Server Cycle Overhead (`Hibernate + Server Restore` Med)** | `6,600 ms` | **`4,300 ms` (`1.53x` faster vs Standard GCS)** | `10,600 ms` | `1,545 ms` | **`4,655 ms` (`1.42x` faster vs Arch 1)** | **`5,880 ms` (`1.12x` faster vs Arch 1)** |
| **Single-Node System Throughput (`cycles/min`)** | **`103.3 cycles/min`** | **`149.3 cycles/min` (`+44.5%`, `1.45x`)** | **`80.3 cycles/min`** | **`448.0 cycles/min`** | **`102.7 cycles/min`** (`308` cycles in `180s`, `0` GCS traffic) | **`86.7 cycles/min`** (`260` cycles in `180s`, `0` GCS traffic) |
| **Peak Physical NVMe Write / Read Bandwidth (`MiB/s`)** | `1,000.6 / 0.0 MiB/s` | **`2,182.6 / 0.0 MiB/s`** *(`4,562.4 MiB/s` RX / `3,593.2 MiB/s` TX)* | `1.4 / 0.0 MiB/s` | `2,383.0 / 0.0 MiB/s` | **`2,340.4 MiB/s` Write / `1,892.3 MiB/s` Read** | **`2,328.8 MiB/s` Write / `1,526.8 MiB/s` Read** |

### 10.3 Cross-Node Restore Percentage Sweep (`0%` to `100%`)

Using `--cross-node-restore-pct` (`ATELET_SIMULATE_CROSS_NODE_PCT`), `atelet` probabilistically detaches a paused actor's disk on $P\%$ of `PauseActor` calls (triggering a parallel `1.0s` `AttachAndMount` + cold NVMe read on the next `ResumeActor`) and leaves the remaining $(100 - P)\%$ mounted on the same node (`0 ms` attach + warm Linux DRAM page-cache read).

#### Summary Comparison vs. Architecture 1 (`SuspendActor` to GCS + Boot Disk at $N=15$)

| Target Cross-Node % | Observed Cross-Node Restore % | Sustained Throughput (`cycles/min` & vs. Arch 1) | Client `PauseActor` (`p50 / avg / p90`) | `Pause` vs. Arch 1 `Suspend` (`p50 / avg / p90`) | Client `ResumeActor` (`p50 / avg / p90`) | `Resume` vs. Arch 1 `Resume` (`p50 / avg / p90`) | Total Client Cycle (`Pause+Resume` `p50 / avg / p90`) | `Cycle` vs. Arch 1 (`p50 / avg / p90`) |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Arch 1 Baseline** *(Standard Regional GCS)* | `N/A` *(100% GCS)* | **`103.3`** *(Baseline)* | `3,900 / 4,328 / 6,500 ms` | *Baseline (`0.0%`)* | `2,700 / 2,851 / 4,200 ms` | *Baseline (`0.0%`)* | `6,600 / 7,179 / 10,700 ms` | *Baseline (`0.0%`)* |
| **Arch 1 (`RAPID` Zonal GCS Bucket)** *(us-central1-a)* | `N/A` *(100% Rapid GCS)* | **`149.3` (`+44.5%`, `1.45x`)** | `2,400 / 2,659 / 3,800 ms` | **`-38.5% / -38.6% / -41.5%`** | `1,900 / 1,890 / 2,300 ms` | **`-29.6% / -33.7% / -45.2%`** | `4,300 / 4,549 / 6,100 ms` | **`-34.8% / -36.6% / -43.0%`** |
| **`0%`** *(Pre-Attached)* | **`0.0%`** | **`448.0` (`+333.7%`, `4.34x`)** | `870 / 1,061 / 1,800 ms` | **`-77.7% / -75.5% / -72.3%`** | `690 / 719 / 940 ms` | **`-74.4% / -74.8% / -77.6%`** | `1,560 / 1,780 / 2,740 ms` | **`-76.4% / -75.2% / -74.4%`** |
| **`5%`** | **`7.8%`** (`30/383`) | **`264.7` (`+156.2%`, `2.56x`)** | `730 / 885 / 1,500 ms` | **`-81.3% / -79.6% / -76.9%`** | `640 / 1,110 / 2,700 ms` | **`-76.3% / -61.1% / -35.7%`** | `1,370 / 1,995 / 4,200 ms` | **`-79.2% / -72.2% / -60.7%`** |
| **`10%`** | **`12.9%`** (`46/356`) | **`242.0` (`+134.3%`, `2.34x`)** | `730 / 871 / 1,500 ms` | **`-81.3% / -79.9% / -76.9%`** | `670 / 1,356 / 4,200 ms` | **`-75.2% / -52.4% / 0.0%`** | `1,400 / 2,227 / 5,700 ms` | **`-78.8% / -69.0% / -46.7%`** |
| **`20%`** | **`19.8%`** (`73/368`) | **`250.7` (`+142.7%`, `2.43x`)** | `740 / 902 / 1,600 ms` | **`-81.0% / -79.2% / -75.4%`** | `540 / 1,274 / 4,300 ms` | **`-80.0% / -55.3% / +2.4%`** | `1,280 / 2,176 / 5,900 ms` | **`-80.6% / -69.7% / -44.9%`** |
| **`30%`** | **`27.4%`** (`90/328`) | **`220.7` (`+113.6%`, `2.14x`)** | `740 / 942 / 1,800 ms` | **`-81.0% / -78.2% / -72.3%`** | `530 / 1,612 / 4,900 ms` | **`-80.4% / -43.5% / +16.7%`** | `1,270 / 2,554 / 6,700 ms` | **`-80.8% / -64.4% / -37.4%`** |
| **`40%`** | **`39.1%`** (`117/299`) | **`201.3` (`+94.9%`, `1.95x`)** | `720 / 856 / 1,400 ms` | **`-81.5% / -80.2% / -78.5%`** | `560 / 2,098 / 5,200 ms` | **`-79.3% / -26.4% / +23.8%`** | `1,280 / 2,954 / 6,600 ms` | **`-80.6% / -58.9% / -38.3%`** |
| **`50%`** | **`49.4%`** (`124/251`) | **`173.3` (`+67.8%`, `1.68x`)** | `710 / 919 / 1,800 ms` | **`-81.8% / -78.8% / -72.3%`** | `660 / 2,776 / 5,800 ms` | **`-75.6% / -2.6% / +38.1%`** | `1,370 / 3,695 / 7,600 ms` | **`-79.2% / -48.5% / -29.0%`** |
| **`60%`** | **`63.1%`** (`140/222`) | **`152.0` (`+47.1%`, `1.47x`)** | `690 / 855 / 1,600 ms` | **`-82.3% / -80.2% / -75.4%`** | `4,400 / 3,500 / 6,300 ms` | **`+63.0% / +22.8% / +50.0%`** | `5,090 / 4,355 / 7,900 ms` | **`-22.9% / -39.3% / -26.2%`** |
| **`70%`** | **`70.0%`** (`142/203`) | **`138.0` (`+33.6%`, `1.34x`)** | `690 / 937 / 1,800 ms` | **`-82.3% / -78.4% / -72.3%`** | `4,800 / 3,943 / 6,600 ms` | **`+77.8% / +38.3% / +57.1%`** | `5,490 / 4,880 / 8,400 ms` | **`-16.8% / -32.0% / -21.5%`** |
| **`80%`** | **`84.2%`** (`154/183`) | **`123.3` (`+19.4%`, `1.19x`)** | `660 / 844 / 1,500 ms` | **`-83.1% / -80.5% / -76.9%`** | `5,400 / 4,765 / 6,700 ms` | **`+100.0% / +67.1% / +59.5%`** | `6,060 / 5,609 / 8,200 ms` | **`-8.2% / -21.9% / -23.4%`** |
| **`90%`** | **`90.2%`** (`156/173`) | **`118.7` (`+14.9%`, `1.15x`)** | `660 / 798 / 1,500 ms` | **`-83.1% / -81.6% / -76.9%`** | `5,900 / 5,267 / 6,700 ms` | **`+118.5% / +84.7% / +59.5%`** | `6,560 / 6,065 / 8,200 ms` | **`-0.6% / -15.5% / -23.4%`** |
| **`100%`** *(Every Cycle)* | **`100.0%`** (`287/287`) | **`102.7` (`-0.6%`, `0.99x`)** | `640 / 810 / 1,500 ms` | **`-83.6% / -81.3% / -76.9%`** | `5,200 / 6,319 / 11,000 ms` | **`+92.6% / +121.6% / +161.9%`** | `5,840 / 7,129 / 12,500 ms` | **`-11.5% / -0.7% / +16.8%`** |

#### Server Sub-Stage & Node Telemetry Breakdown (`0%` to `100%`)

| Target Cross-Node % (`--cross-node-restore-pct`) | Observed Cross-Node Restore % | Sustained Throughput (`cycles/min`) | Client `Pause` / `Suspend` (`p50 / avg / p90`) | Client `ResumeActor` (`p50 / avg / p90 / p95`) | Server Blended Restore (`p50 / avg / p90 / p95`) | Same-Node Warm Restore (`p50 / avg`, Warm DRAM Copy `p50`) | Cross-Node / GCS Cold Restore (`p50 / avg / p90`, `dMount` + Cold Read / Download `p50`) | Peak NVMe Write / Read (`MiB/s`) & Mean `iowait` |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Arch 1 (`STANDARD` GCS)** | `100.0%` *(GCS)* | **`103.3`** | `3,900 / 4,328 / 6,500 ms` | **`2,700 / 2,851 / 4,200 / 5,800 ms`** | **`2,612 / 2,758 / 4,043 / 5,620 ms`** | `N/A` (`0` local hits) | **`2,612 / 2,758 / 4,043 ms`** (`0 ms` mnt + `2,430 ms` GCS dl) | `2,203 W / 0 R` (`2.1%` iowait) |
| **Arch 1 (`RAPID` Zonal GCS)** | `100.0%` *(Zonal GCS)* | **`149.3`** | `2,400 / 2,659 / 3,800 ms` | **`1,900 / 1,890 / 2,300 / 2,600 ms`** | **`1,858 / 1,877 / 2,262 / 2,424 ms`** | `N/A` (`0` local hits) | **`1,858 / 1,877 / 2,262 ms`** (`0 ms` mnt + `1,469 ms` DirectPath dl) | `2,183 W / 0 R` (`1.4%` iowait) |
| **`0%`** *(Pre-Attached)* | `0.0%` | **`448.0`** | `870 / 1,061 / 1,800 ms` | **`690 / 719 / 940 / 1,100 ms`** | **`675 / 704 / 920 / 1,080 ms`** | `675 / 704 ms` (`480 ms` copy) | `N/A` (`0` cold restores) | `2,390 W / 0 R` (`8.5%` iowait) |
| **`5%`** | `7.8%` (`30/383`) | **`264.7`** | `730 / 885 / 1,500 ms` | **`640 / 1,110 / 2,700 / 4,200 ms`** | **`495 / 779 / 637 / 3,953 ms`** | **`491 / 498 ms`** (`275 ms` copy) | **`4,089 / 4,088 / 4,795 ms`** (`1,735 ms` mnt + `2,175 ms` read) | `2,433 W / 926 R` (`4.6%` iowait) |
| **`10%`** | `12.9%` (`46/356`) | **`242.0`** | `730 / 871 / 1,500 ms` | **`670 / 1,356 / 4,200 / 4,800 ms`** | **`493 / 941 / 3,564 / 4,113 ms`** | **`483 / 495 ms`** (`277 ms` copy) | **`3,952 / 3,954 / 4,691 ms`** (`1,586 ms` mnt + `2,245 ms` read) | `2,399 W / 960 R` (`5.5%` iowait) |
| **`20%`** | `19.8%` (`73/368`) | **`250.7`** | `740 / 902 / 1,600 ms` | **`540 / 1,274 / 4,300 / 4,800 ms`** | **`529 / 1,289 / 4,328 / 4,719 ms`** | **`509 / 517 ms`** (`280 ms` copy) | **`4,328 / 4,408 / 5,007 ms`** (`1,741 ms` mnt + `2,340 ms` read) | `2,404 W / 1,433 R` (`7.1%` iowait) |
| **`30%`** | `27.4%` (`90/328`) | **`220.7`** | `740 / 942 / 1,800 ms` | **`530 / 1,612 / 4,900 / 5,400 ms`** | **`529 / 1,645 / 4,866 / 5,370 ms`** | **`508 / 510 ms`** (`281 ms` copy) | **`4,583 / 4,645 / 5,669 ms`** (`1,761 ms` mnt + `2,550 ms` read) | `2,377 W / 1,377 R` (`9.6%` iowait) |
| **`40%`** | `39.1%` (`117/299`) | **`201.3`** | `720 / 856 / 1,400 ms` | **`560 / 2,098 / 5,200 / 5,700 ms`** | **`557 / 2,120 / 5,215 / 5,680 ms`** | **`501 / 505 ms`** (`272 ms` copy) | **`4,478 / 4,632 / 5,976 ms`** (`1,784 ms` mnt + `2,436 ms` read) | `2,393 W / 1,968 R` (`10.1%` iowait) |
| **`50%`** | `49.4%` (`124/251`) | **`173.3`** | `710 / 919 / 1,800 ms` | **`660 / 2,776 / 5,800 / 6,400 ms`** | **`699 / 2,788 / 5,749 / 6,414 ms`** | **`496 / 508 ms`** (`274 ms` copy) | **`5,049 / 5,122 / 6,414 ms`** (`1,775 ms` mnt + `3,093 ms` read) | `2,362 W / 1,913 R` (`12.9%` iowait) |
| **`60%`** | `63.1%` (`140/222`) | **`152.0`** | `690 / 855 / 1,600 ms` | **`4,400 / 3,500 / 6,300 / 6,700 ms`** | **`4,437 / 3,512 / 6,309 / 6,717 ms`** | **`492 / 503 ms`** (`273 ms` copy) | **`5,412 / 5,275 / 6,566 ms`** (`1,841 ms` mnt + `3,249 ms` read) | `2,375 W / 1,982 R` (`14.4%` iowait) |
| **`70%`** | `70.0%` (`142/203`) | **`138.0`** | `690 / 937 / 1,800 ms` | **`4,800 / 3,943 / 6,600 / 6,800 ms`** | **`4,773 / 3,998 / 6,600 / 6,814 ms`** | **`482 / 503 ms`** (`273 ms` copy) | **`5,642 / 5,500 / 6,726 ms`** (`1,891 ms` mnt + `3,439 ms` read) | `2,380 W / 2,302 R` (`16.3%` iowait) |
| **`80%`** | `84.2%` (`154/183`) | **`123.3`** | `660 / 844 / 1,500 ms` | **`5,400 / 4,765 / 6,700 / 6,900 ms`** | **`5,345 / 4,738 / 6,676 / 6,879 ms`** | **`468 / 492 ms`** (`266 ms` copy) | **`5,781 / 5,538 / 6,716 ms`** (`2,021 ms` mnt + `3,343 ms` read) | `2,374 W / 1,814 R` (`16.8%` iowait) |
| **`90%`** | `90.2%` (`156/173`) | **`118.7`** | `660 / 798 / 1,500 ms` | **`5,900 / 5,267 / 6,700 / 6,900 ms`** | **`5,791 / 5,199 / 6,692 / 6,961 ms`** | **`518 / 504 ms`** (`271 ms` copy) | **`5,956 / 5,711 / 6,709 ms`** (`2,125 ms` mnt + `3,308 ms` read) | `2,372 W / 2,389 R` (`17.2%` iowait) |
| **`100%`** *(Every Cycle)* | `100.0%` (`287/287`) | **`102.7`** | `640 / 810 / 1,500 ms` | **`5,200 / 6,319 / 11,000 / 11,000 ms`** | **`4,015 / 4,111 / 4,801 / 5,021 ms`** | `N/A` (`0` warm restores) | **`4,015 / 4,111 / 4,801 ms`** (`1,042 ms` mnt + `2,585 ms` read) | `2,340 W / 1,892 R` (`4.7%` iowait) |

### 10.4 Reproducing Single-Node `Arch 1` (`RAPID` Zonal GCS Bucket) & `Arch 3c` (`0%`–`100%` Cross-Node Sweep)

1. **Build `atelet` and `ate-api-server` images**:
   ```bash
   source .ate-dev-env.sh
   export KO_DOCKER_REPO="gcr.io/${PROJECT_ID}/ate-images"
   ATELET_IMG=$(ko build ./cmd/atelet)
   ATEAPI_IMG=$(ko build ./cmd/ateapi)
   kubectl set image -n ate-system deploy/ate-api-server api-server="$ATEAPI_IMG"
   ```
2. **Reproduce `Arch 1` (`RAPID` Zonal GCS Bucket in `us-central1-a`)**:
   ```bash
   export RAPID_BUCKET="gs://snapshot-substrate-rapid-${PROJECT_ID}"
   gcloud storage buckets create "$RAPID_BUCKET" \
     --project="$PROJECT_ID" \
     --location=us-central1 \
     --placement=us-central1-a \
     --default-storage-class=RAPID \
     --enable-hierarchical-namespace \
     --uniform-bucket-level-access

   python3 benchmarking/scripts/disk-arch/run_w2_arch1_rapid.py "$ATELET_IMG" 180
   ```
3. **Reproduce `Arch 3c` (`1s` Parallel Simulated Hyperdisk Attach/Detach `0%`–`100%` Sweep)**:
   ```bash
   python3 benchmarking/scripts/disk-arch/setup_disk_pool.py \
     --node "$ACTIVE_NODE" --count 17 --size-gb 20 --iops 3000 --throughput 140

   python3 benchmarking/scripts/disk-arch/run_w2_arch3c_sim1s.py "$ATELET_IMG" 1.0 180 \
     --sweep-pcts 5,10,20,30,40,50,60,70,80,90,100
   ```

---

## 11. Multi-Node Actor Overcommit Benchmark (`1.0x` to `5.0x`: `30` to `150` Actors across `2x c3-standard-44` Nodes)

We evaluated **Workload W2 (`544 MiB` dirty state per transition)** across **`2x c3-standard-44` worker nodes** at **`30` ready workers (`15` workers/node)** across five overcommit tiers (`1.0x = 30` actors, `2.0x = 60` actors, `3.0x = 90` actors, `4.0x = 120` actors, `5.0x = 150` actors) comparing **Architecture 1 (`SuspendActor` to GCS)** vs. **Architecture 3 (`PauseActor` + Dedicated GCE Hyperdisk Pool with `maxQueue=2` Bounded Disk Admission)**:

| Overcommit Tier | Architecture | Cluster Throughput (`cycles/min`) | Warm Resume (`avg / p90 / p95`) | Hibernate (`avg / p90 / p95`) | Same-Node Disk Hit Rate (%) |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **`1.0x` (`30` Actors)** | **Arch 1 (`SuspendActor`)** | `159.0 / min` | `3,645 / 6,100 / 7,000 ms` | `5,566 / 8,800 / 18,000 ms` | `N/A` (GCS transfer on 100% of cycles) |
| | **Arch 3 (`PauseActor`)** | **`673.7 / min` (`+323.7%`)** | **`563 / 670 / 740 ms` (`-84.6%` avg)** | **`952 / 1,500 / 2,100 ms` (`-82.9%` avg)** | **`100.0%`** (`2,049 / 2,049` local hits) |
| **`2.0x` (`60` Actors)** | **Arch 1 (`SuspendActor`)** | `178.7 / min` | `4,115 / 6,000 / 7,500 ms` | `4,842 / 7,700 / 9,100 ms` | `N/A` (GCS transfer on 100% of cycles) |
| | **Arch 3 (`PauseActor`)** | **`502.3 / min` (`+181.1%`)** | **`1,149 / 1,300 / 1,600 ms` (`-72.1%` avg)** | **`1,221 / 2,300 / 3,000 ms` (`-74.8%` avg)** | **`98.8%`** (`1,513 / 1,532` local hits) |
| **`3.0x` (`90` Actors)** | **Arch 1 (`SuspendActor`)** | `199.7 / min` | `3,769 / 5,400 / 6,000 ms` | `4,181 / 6,100 / 7,500 ms` | `N/A` (GCS transfer on 100% of cycles) |
| | **Arch 3 (`PauseActor`)** | **`363.0 / min` (`+81.8%`)** | **`2,232 / 4,000 / 13,000 ms` (`-40.8%` avg)** | **`947 / 1,400 / 2,200 ms` (`-77.3%` avg)** | **`98.1%`** (`1,085 / 1,106` local hits) |
| **`4.0x` (`120` Actors)** | **Arch 1 (`SuspendActor`)** | **`178.0 / min`** | **`4,351 / 6,200 / 6,900 ms`** | `4,953 / 6,700 / 8,800 ms` | `N/A` (GCS transfer on 100% of cycles) |
| | **Arch 3 (`PauseActor`)** | `120.3 / min` (`-32.4%`) | `7,511 / 25,000 / 41,000 ms` | **`833 / 1,700 / 2,600 ms` (`-83.2%` avg)** | **`98.9%`** of completed restores (`60`-disk VM slot cap reached) |
| **`5.0x` (`150` Actors)** | **Arch 1 (`SuspendActor`)** | **`181.7 / min`** | **`4,455 / 6,100 / 6,500 ms`** | `4,803 / 7,800 / 8,100 ms` | `N/A` (GCS transfer on 100% of cycles) |
| | **Arch 3 (`PauseActor`)** | `40.7 / min` (`-77.6%`) | `12,611 / 13,000 / 13,000 ms` | **`849 / 1,000 / 2,900 ms` (`-82.3%` avg)** | **`99.2%`** of completed restores (LRU disk thrashing) |

### 11.1 Reproducing the Multi-Node Overcommit Benchmark (`1.0x` to `5.0x`)

```bash
# 1. Provision 30 attached Hyperdisks on NODE1, 30 attached on NODE2, and 90 formatted detached pool disks:
python3 benchmarking/scripts/disk-arch/setup_disk_pool.py \
  --node "$NODE1" --start-index 0 --count 30 --size-gb 20 --iops 10000 --throughput 800
python3 benchmarking/scripts/disk-arch/setup_disk_pool.py \
  --node "$NODE2" --start-index 30 --count 30 --size-gb 20 --iops 10000 --throughput 800
python3 benchmarking/scripts/disk-arch/setup_disk_pool.py \
  --node "$NODE1" --start-index 60 --count 90 --size-gb 20 --iops 10000 --throughput 800 --detach-after-format

# 2. Run Arch 1 (SuspendActor to GCS) across 30, 60, 90, 120, 150 actors (30 workers):
for ACTOR_COUNT in 30 60 90 120 150; do
  python3 benchmarking/scripts/disk-arch/run_overcommit_benchmark.py \
    --mode arch1 --workers 30 --actors "$ACTOR_COUNT" --duration-sec 180 --wait-sec 1.0
done

# 3. Run Arch 3 (PauseActor + Hyperdisk Pool) across 30, 60, 90, 120, 150 actors (30 workers):
for ACTOR_COUNT in 30 60 90 120 150; do
  python3 benchmarking/scripts/disk-arch/run_overcommit_benchmark.py \
    --mode arch3 --workers 30 --actors "$ACTOR_COUNT" --duration-sec 180 --wait-sec 1.0
done
```




