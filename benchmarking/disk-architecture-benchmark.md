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

### 3.3 Architecture 3: `Pause` with One Dedicated Hyperdisk per Actor (`atelet` Integration)
- **Modifying `atelet` for Per-Actor Disk Assignment**:
  - Because `/var/lib/ateom-gvisor` is mounted into both `atelet` and all `ateom` worker pods at `/var/lib/ateom-gvisor`, any subdirectory under `/var/lib/ateom-gvisor/disk-pool/disk-i` is visible at the identical path inside both `atelet` and `ateom` containers.
  - Because `atelet` drops `CAP_SYS_ADMIN`, it cannot call `mount(2)`, **but it can create symlinks (`os.Symlink`) as root**.
  - When `--actor-disk-pool-dir=/var/lib/ateom-gvisor/disk-pool` is enabled in `cmd/atelet/main.go`:
    1. Pre-attach and mount $N$ dedicated `hyperdisk-balanced` volumes (`disk-0`, `disk-1`, ..., `disk-N`) under `/var/lib/ateom-gvisor/disk-pool/disk-i` on the host using `benchmarking/scripts/disk-arch/setup_disk_pool.py`.
    2. In `atelet`, `ActorDiskPool` maps `actorUID` $\leftrightarrow$ `disk-i` and symlinks `/var/lib/ateom-gvisor/actors/<actorUID>` $\to$ `/var/lib/ateom-gvisor/disk-pool/disk-i/<actorUID>`.

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
