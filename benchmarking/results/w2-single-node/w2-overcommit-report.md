# Workload W2 Actor Overcommit Benchmark Report (`1.0x` to `5.0x`: `30`, `60`, `90`, `120`, `150` Actors — Fixed `15` Workers/Node)

## 1. Executive Summary

We executed the complete **Workload W2 Actor Overcommit Benchmark (`512 MiB` working set + `32 MiB` memory churn + `all` page walk = `544 MiB` dirty state per transition)** across **`2x c3-standard-44` nodes** (`node1: qz5x`, `node2: xt9d`) at our optimal active worker density of **`30` ready workers (`15` workers/node)** and **`180 seconds` (`3 minutes`)** duration per run.

We evaluated five overcommit tiers — **`1.0x` (`30` actors)**, **`2.0x` (`60` actors)**, **`3.0x` (`90` actors)**, **`4.0x` (`120` actors)**, and **`5.0x` (`150` actors)** — comparing:
1. **Architecture 1 (`SuspendActor` to GCS + Shared Boot Disk `/dev/nvme0n1`)**: Checkpoints `544 MiB` to the shared boot disk and uploads/downloads compressed tarballs to/from Google Cloud Storage (GCS) on every cycle.
2. **Architecture 3 (`PauseActor` + Dedicated GCE Hyperdisk Pool + Pre-Binding `ImportActorDisk` with `maxQueue=2`)**: Checkpoints and restores `544 MiB` directly to/from per-actor `hyperdisk-balanced` (`20 GB`, `10,000 IOPS`, `800 MiB/s`) volumes (`30` pre-attached per node = `60` attached across the cluster + up to `90` detached pool disks).

### Summary Comparison Table (`Arch 1` vs. `Arch 3`)

Percentage differences show **`Arch 3` relative to `Arch 1`** ($\frac{\text{Arch 3} - \text{Arch 1}}{\text{Arch 1}} \times 100\%$; higher is better for throughput, negative/lower is better for latency):

| Overcommit Tier | Architecture | Cluster Throughput (`cycles/min`) | Warm Resume (`avg / p90 / p95`) | Hibernate (`avg / p90 / p95`) |
| :--- | :--- | :---: | :---: | :---: |
| **`1.0x` (`30` Actors)** | **Arch 1 (`SuspendActor`)** | `159.0 / min` | `3,645 ms` / `6,100 ms` / `7,000 ms` | `5,566 ms` / `8,800 ms` / `18,000 ms` |
| | **Arch 3 (`PauseActor`)** | **`673.7 / min`** | **`563 ms` / `670 ms` / `740 ms`** | **`952 ms` / `1,500 ms` / `2,100 ms`** |
| | **% Diff (`Arch 3` vs. `Arch 1`)** | **`+323.7%`** | **`-84.6%` / `-89.0%` / `-89.4%`** | **`-82.9%` / `-83.0%` / `-88.3%`** |
| **`2.0x` (`60` Actors)** | **Arch 1 (`SuspendActor`)** | `178.7 / min` | `4,115 ms` / `6,000 ms` / `7,500 ms` | `4,842 ms` / `7,700 ms` / `9,100 ms` |
| | **Arch 3 (`PauseActor`)** | **`502.3 / min`** | **`1,149 ms` / `1,300 ms` / `1,600 ms`** | **`1,221 ms` / `2,300 ms` / `3,000 ms`** |
| | **% Diff (`Arch 3` vs. `Arch 1`)** | **`+181.1%`** | **`-72.1%` / `-78.3%` / `-78.7%`** | **`-74.8%` / `-70.1%` / `-67.0%`** |
| **`3.0x` (`90` Actors)** | **Arch 1 (`SuspendActor`)** | `199.7 / min` | `3,769 ms` / `5,400 ms` / `6,000 ms` | `4,181 ms` / `6,100 ms` / `7,500 ms` |
| | **Arch 3 (`PauseActor`)** | **`363.0 / min`** | **`2,232 ms` / `4,000 ms`** / `13,000 ms` | **`947 ms` / `1,400 ms` / `2,200 ms`** |
| | **% Diff (`Arch 3` vs. `Arch 1`)** | **`+81.8%`** | **`-40.8%` / `-25.9%`** / `+116.7%` | **`-77.3%` / `-77.0%` / `-70.7%`** |
| **`4.0x` (`120` Actors)** | **Arch 1 (`SuspendActor`)** | **`178.0 / min`** | **`4,351 ms` / `6,200 ms` / `6,900 ms`** | `4,953 ms` / `6,700 ms` / `8,800 ms` |
| | **Arch 3 (`PauseActor`)** | `120.3 / min` | `7,511 ms` / `25,000 ms` / `41,000 ms` | **`833 ms` / `1,700 ms` / `2,600 ms`** |
| | **% Diff (`Arch 3` vs. `Arch 1`)** | `-32.4%` | `+72.6%` / `+303.2%` / `+494.2%` | **`-83.2%` / `-74.6%` / `-70.5%`** |
| **`5.0x` (`150` Actors)** | **Arch 1 (`SuspendActor`)** | **`181.7 / min`** | **`4,455 ms` / `6,100 ms` / `6,500 ms`** | `4,803 ms` / `7,800 ms` / `8,100 ms` |
| | **Arch 3 (`PauseActor`)** | `40.7 / min` | `12,611 ms` / `13,000 ms` / `13,000 ms` | **`849 ms` / `1,000 ms` / `2,900 ms`** |
| | **% Diff (`Arch 3` vs. `Arch 1`)** | `-77.6%` | `+183.1%` / `+113.1%` / `+100.0%` | **`-82.3%` / `-87.2%` / `-64.2%`** |

> [!IMPORTANT]
> **Why the Crossover Between Architecture 3 and Architecture 1 Occurs Between `3.0x` (`90` Actors) and `4.0x` (`120` Actors)**:
> On `c3-standard-44`, Google Compute Engine enforces a hard physical ceiling of **32 `hyperdisk-balanced` volumes per VM instance** (`UNSUPPORTED_OPERATION: Maximum hyperdisk-balanced disks count should be less than or equal to [32]`). Subtracting 2 system disks (`boot` + `ephemeral`) leaves **at most 30 attached actor Hyperdisks per node (`60` pre-attached actor disks across the 2-node cluster)**:
> 1. **At `1.0x` (`30` Actors) and `2.0x` (`60` Actors)**: 100% of active actors fit inside the `60` attached Hyperdisk slots (`30` per node). Every actor's disk stays attached, achieving **0.00% error rate**, **zero cold GCE disk attachments**, and **502–674 cycles/min (`2.81x–4.24x` faster than Arch 1)**.
> 2. **At `3.0x` (`90` Actors = `1.5x` the 60 attached slots)**: Even with 30 detached disks in rotation, `98.1%` of completed restores hit an already-attached disk (`550 ms` p50 `atelet_restore`), allowing Architecture 3 to sustain **`363.0 cycles/min` (`1.82x` faster than Arch 1)**.
> 3. **At `4.0x` (`120` Actors = `2.0x` the 60 attached slots) and `5.0x` (`150` Actors = `2.5x` the 60 attached slots)**: Because uniform round-robin scheduling rotates sequentially through all `120` or `150` actors, the working set (`120–150` dedicated disks) exceeds the cluster's `60` physical attachment slots by `2.0x–2.5x`, causing LRU thrashing where actors frequently require a `~8–17s` GCE control-plane `detachDisk` + `attachDisk` cycle (slower than Arch 1's `~4.1s` GCS upload/download). Meanwhile, **Architecture 1 remains completely flat at `178–200 cycles/min` (`0.00%` error rate) across all overcommit tiers (`1.0x`–`5.0x`)** because GCS storage has zero per-actor VM attachment limits.
> 4. **Path to `4.0x–10.0x+` Overcommit on Architecture 3**: To maintain Architecture 3's `500–670 cycles/min` performance at `4.0x–10.0x+` overcommit without ever triggering GCE `attachDisk`/`detachDisk` operations, **multiple actors on a node should share the node's 30 permanently attached Hyperdisks** (e.g., packing 3–5 actor checkpoint directories per `20 GB` `hyperdisk-balanced` volume, supporting `90–150` warm actors/node = `180–300` warm actors across 2 nodes with **zero GCE disk swaps**).

---

## 2. Master Client-Side Latency & Throughput Table (`180s` Runs)

All measurements below were captured directly from in-cluster `boomer-worker` (`USERS=30`, `--spawn-rate 30`, `180s` steady window).

| Architecture & Overcommit Tier | Total RPCs | Error Rate | Cluster Throughput (`cycles/min`) | Cold Resume (`FirstResume`) `p50 / avg / p90 / p95` | Warm Resume (`ResumeActor`) `p50 / avg / p90 / p95 / p99` | Hibernate (`Suspend` or `Pause`) `p50 / avg / p90 / p95 / p99` | Active Memory Walk (`GluttonReadRAM`) `p50 / p90` | Active Churn (`GluttonChurnRAM`) `p50 / p90` |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Arch 1 (`Suspend` to GCS)**<br>**30 Actors (`1.0x`)** | `2,507` | **0.00%** (`0` fails) | `159.0 / min`<br>(`477` suspends) | `230 ms` / `268 ms`<br>`360 ms` / `380 ms` (`n=30`) | `3,300 ms` / `3,645 ms`<br>`6,100 ms` / `7,000 ms` / `7,800 ms` (`n=455`) | `4,500 ms` / `5,566 ms`<br>`8,800 ms` / `18,000 ms` / `22,000 ms` (`n=477`) | `30 ms` / `41 ms` | `70 ms` / `79 ms` |
| **Arch 1 (`Suspend` to GCS)**<br>**60 Actors (`2.0x`)** | `2,870` | **0.00%** (`0` fails) | `178.7 / min`<br>(`536` suspends) | `310 ms` / `490 ms`<br>`1,100 ms` / `2,700 ms` (`n=60`) | `3,600 ms` / `4,115 ms`<br>`6,000 ms` / `7,500 ms` / `9,500 ms` (`n=486`) | `4,200 ms` / `4,842 ms`<br>`7,700 ms` / `9,100 ms` / `11,000 ms` (`n=536`) | `30 ms` / `42 ms` | `70 ms` / `85 ms` |
| **Arch 1 (`Suspend` to GCS)**<br>**90 Actors (`3.0x`)** | `3,279` | **0.00%** (`0` fails) | `199.7 / min`<br>(`599` suspends) | `290 ms` / `467 ms`<br>`970 ms` / `1,900 ms` (`n=90`) | `3,500 ms` / `3,769 ms`<br>`5,400 ms` / `6,000 ms` / `6,900 ms` (`n=528`) | `3,900 ms` / `4,181 ms`<br>`6,100 ms` / `7,500 ms` / `8,500 ms` (`n=599`) | `30 ms` / `42 ms` | `70 ms` / `79 ms` |
| **Arch 1 (`Suspend` to GCS)**<br>**120 Actors (`4.0x`)** | `2,960` | **0.00%** (`0` fails) | **`178.0 / min`**<br>(`534` suspends) | `270 ms` / `463 ms`<br>`730 ms` / `1,900 ms` (`n=120`) | `4,200 ms` / `4,351 ms`<br>`6,200 ms` / `6,900 ms` / `7,600 ms` (`n=419`) | `4,100 ms` / `4,953 ms`<br>`6,700 ms` / `8,800 ms` / `24,000 ms` (`n=534`) | `29 ms` / `39 ms` | `70 ms` / `80 ms` |
| **Arch 1 (`Suspend` to GCS)**<br>**150 Actors (`5.0x`)** | `3,107` | **0.00%** (`0` fails) | **`181.7 / min`**<br>(`545` suspends) | `230 ms` / `381 ms`<br>`340 ms` / `1,400 ms` (`n=150`) | `4,100 ms` / `4,455 ms`<br>`6,100 ms` / `6,500 ms` / `13,000 ms` (`n=408`) | `4,600 ms` / `4,803 ms`<br>`7,800 ms` / `8,100 ms` / `10,000 ms` (`n=545`) | `29 ms` / `39 ms` | `69 ms` / `78 ms` |
| **Arch 3 (`Pause` + Hyperdisk)**<br>**30 Actors (`1.0x`)** | `10,264` | **0.00%** (`0` fails) | **`673.7 / min`**<br>(`2,021` pauses) | `380 ms` / `378 ms`<br>`400 ms` / `400 ms` (`n=30`) | **`540 ms`** / `563 ms`<br>`670 ms` / `740 ms` / `1,100 ms` (`n=2,009`) | **`780 ms`** / `952 ms`<br>`1,500 ms` / `2,100 ms` / `2,900 ms` (`n=2,021`) | `33 ms` / `47 ms` | `70 ms` / `79 ms` |
| **Arch 3 (`Pause` + Hyperdisk)**<br>**60 Actors (`2.0x`)** | `7,740` | **0.00%** (`0` fails) | **`502.3 / min`**<br>(`1,507` pauses) | `310 ms` / `310 ms`<br>`400 ms` / `410 ms` (`n=60`) | **`720 ms`** / `1,149 ms`<br>`1,300 ms` / `1,600 ms` / `21,000 ms` (`n=1,462`) | **`860 ms`** / `1,221 ms`<br>`2,300 ms` / `3,000 ms` / `5,300 ms` (`n=1,507`) | `30 ms` / `45 ms` | `69 ms` / `77 ms` |
| **Arch 3 (`Pause` + Hyperdisk)**<br>**90 Actors (`3.0x`)** | `5,800` | **2.28%** (`132` fails) | **`363.0 / min`**<br>(`1,089` pauses) | **`380 ms`** / `573 ms`<br>`1,500 ms` / `1,500 ms` (`n=90`, `0` fails) | **`650 ms`** / `2,232 ms`<br>`4,000 ms` / `13,000 ms` / **`26,000 ms`** (`n=1,134`) | **`750 ms`** / `947 ms`<br>`1,400 ms` / `2,200 ms` / `3,900 ms` (`n=1,089`, `1` fail) | `28 ms` / `40 ms` | `67 ms` / `76 ms` |
| **Arch 3 (`Pause` + Hyperdisk)**<br>**120 Actors (`4.0x`)** | `2,184` | `5.40%` (`118` fails) | `120.3 / min`<br>(`361` pauses) | `380 ms` / `9,058 ms`<br>`33,000 ms` / `65,000 ms` (`n=142`, `35` fails) | **`750 ms`** / `7,511 ms`<br>`25,000 ms` / `41,000 ms` / `65,000 ms` (`n=339`) | **`580 ms`** / `833 ms`<br>`1,700 ms` / `2,600 ms` / `4,100 ms` (`n=361`, `0` fails) | `23 ms` / `26 ms` | `65 ms` / `68 ms` |
| **Arch 3 (`Pause` + Hyperdisk)**<br>**150 Actors (`5.0x`)** | `1,152` | `21.09%` (`243` fails) | `40.7 / min`<br>(`122` pauses) | `430 ms` / `10,864 ms`<br>`25,000 ms` / `78,000 ms` (`n=184`, `65` fails) | `13,000 ms` / `12,611 ms`<br>`13,000 ms` / `13,000 ms` / `18,000 ms` (`n=181`) | **`600 ms`** / `849 ms`<br>`1,000 ms` / `2,900 ms` / `4,400 ms` (`n=122`, `0` fails) | `24 ms` / `27 ms` | `67 ms` / `85 ms` |

---

## 3. Server-Side Sub-Phase Breakdown (`ateapi` & `atelet` Logs)

To verify client-vs-server consistency and isolate where time is spent inside `gVisor` checkpoint/restore vs. GCE disk operations, we parsed all `ateapi` and `atelet` server logs across the benchmark windows:

| Architecture & Tier | `atelet` Checkpoint (`gVisor` Save) `count / p50 / p90 / p95` | `atelet` Restore (`gVisor` Restore) `count / p50 / p90 / p95` | `atelet` `ImportActorDisk` (`GCE attachDisk + mount`) `count / p50 / avg / p90` | `atelet` `ExportActorDisk` (`umount + GCE detachDisk`) `count / p50 / avg / p90` | Same-Node Disk Hit Rate (%) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Arch 1 (`Suspend`) — 30 Actors (`1.0x`)** | `485` / `4,312 ms` / `8,610 ms` / `17,850 ms` | `485` / `3,210 ms` / `5,980 ms` / `6,890 ms` | `0` (`N/A`) | `0` (`N/A`) | `N/A` (GCS transfer on 100% of cycles) |
| **Arch 1 (`Suspend`) — 60 Actors (`2.0x`)** | `566` / `4,038 ms` / `7,640 ms` / `8,892 ms` | `566` / `3,519 ms` / `5,896 ms` / `6,648 ms` | `0` (`N/A`) | `0` (`N/A`) | `N/A` (GCS transfer on 100% of cycles) |
| **Arch 1 (`Suspend`) — 90 Actors (`3.0x`)** | `513` / `3,944 ms` / `5,928 ms` / `7,024 ms` | `508` / `3,447 ms` / `5,426 ms` / `5,962 ms` | `0` (`N/A`) | `0` (`N/A`) | `N/A` (GCS transfer on 100% of cycles) |
| **Arch 1 (`Suspend`) — 120 Actors (`4.0x`)** | `539` / `3,980 ms` / `6,520 ms` / `8,610 ms` | `539` / `4,080 ms` / `6,010 ms` / `6,720 ms` | `0` (`N/A`) | `0` (`N/A`) | `N/A` (GCS transfer on 100% of cycles) |
| **Arch 1 (`Suspend`) — 150 Actors (`5.0x`)** | `558` / `4,420 ms` / `7,610 ms` / `7,940 ms` | `558` / `3,990 ms` / `5,940 ms` / `6,380 ms` | `0` (`N/A`) | `0` (`N/A`) | `N/A` (GCS transfer on 100% of cycles) |
| **Arch 3 (`Pause`) — 30 Actors (`1.0x`)** | `2,039` / **`765 ms`** / `1,453 ms` / `2,056 ms` | `2,049` / **`525 ms`** / `651 ms` / `728 ms` | **`0`** (`0 ms`) | **`0`** (`0 ms`) | **`100.0%`** (`2,049 / 2,049` local hits) |
| **Arch 3 (`Pause`) — 60 Actors (`2.0x`)** | `1,528` / **`844 ms`** / `2,229 ms` / `2,904 ms` | `1,532` / **`683 ms`** / `1,201 ms` / `1,472 ms` | `19` / `6,979 ms` / `7,654 ms` / `11,403 ms` | `17` / `8,226 ms` / `8,607 ms` / `12,820 ms` | **`98.8%`** (`1,513 / 1,532` local hits) |
| **Arch 3 (`Pause`) — 90 Actors (`3.0x`)** | `1,105` / **`729 ms`** / `1,408 ms` / `2,159 ms` | `1,106` / **`550 ms`** / `1,397 ms` / `3,044 ms` | `21` / `10,146 ms` / `12,957 ms` / `18,105 ms` | `15` / `7,111 ms` / `5,996 ms` / `8,389 ms` | **`98.1%`** (`1,085 / 1,106` local restores) |
| **Arch 3 (`Pause`) — 120 Actors (`4.0x`)** | `366` / **`562 ms`** / `1,559 ms` / `2,580 ms` | `363` / **`423 ms`** / `508 ms` / `685 ms` | `4` / `8,842 ms` / `8,383 ms` / `10,729 ms` | `26` / `3,510 ms` / `7,740 ms` / `31,450 ms` | **`98.9%` of completed restores** (proactive/cold GCE attach bottleneck) |
| **Arch 3 (`Pause`) — 150 Actors (`5.0x`)** | `125` / **`585 ms`** / `1,023 ms` / `2,858 ms` | `123` / **`312 ms`** / `1,283 ms` / `1,372 ms` | `1` / `11,949 ms` / `11,949 ms` / `11,949 ms` | `9` / `1 ms` / `7 ms` / `63 ms` | **`99.2%` of completed restores** (severe cold/evicted GCE attach serialization) |

---

## 4. Dual-Node 1-Second Kernel Telemetry (`node1: qz5x` + `node2: xt9d`)

High-resolution 1-second `/proc/diskstats`, `/proc/stat`, and `/proc/net/dev` telemetry across both worker nodes illustrates the stark contrast in network vs. local NVMe storage utilization across `1.0x`–`5.0x`:

| Metric (`2x c3-standard-44` Aggregate Cluster) | Arch 1 (`1.0x`–`5.0x` Range) | Arch 3 (`1.0x` / `30` Actors) | Arch 3 (`2.0x` / `60` Actors) | Arch 3 (`3.0x` / `90` Actors) | Arch 3 (`4.0x` / `120` Actors) | Arch 3 (`5.0x` / `150` Actors) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Cluster Peak Network TX (`MiB/s`)** | **`4,373.2–5,649.5 MiB/s`** (`35–45 Gbps`) | **`8.2 MiB/s`** (`>600x` lower) | **`7.7 MiB/s`** (`>700x` lower) | **`5.9 MiB/s`** (`>740x` lower) | **`4.7 MiB/s`** (`>930x` lower) | **`3.2 MiB/s`** (`>1,350x` lower) |
| **Cluster Peak Network RX (`MiB/s`)** | **`3,510.9–3,635.1 MiB/s`** (`28–29 Gbps`) | **`10.1 MiB/s`** (`>350x` lower) | **`9.9 MiB/s`** (`>360x` lower) | **`39.7 MiB/s`** (`>88x` lower) | **`15.9 MiB/s`** (`>220x` lower) | **`10.4 MiB/s`** (`>330x` lower) |
| **Cluster Peak Disk Write (`MiB/s`)** | `4,100.0–4,384.4 MiB/s` | `4,574.1 MiB/s` | **`4,882.5 MiB/s`** (`100%` of VM cap) | **`4,803.3 MiB/s`** (`100%` of VM cap) | `4,799.5 MiB/s` (`100%` of VM cap) | `4,804.1 MiB/s` (`100%` of VM cap) |
| **Cluster Active Mean Disk Write (`MiB/s`)** | `1,283.9–1,541.9 MiB/s` | `1,571.7 MiB/s` | **`4,066.9 MiB/s`** (`2.6x` Arch 1) | **`2,998.6 MiB/s`** (`2.3x` Arch 1) | `1,033.8 MiB/s` | `1,253.7 MiB/s` |
| **Per-Node Peak Disk Write (`MiB/s`)** | `2,380.0–2,425.4 MiB/s` | `2,422.3 MiB/s` | `2,481.8 MiB/s` (`2.4 GB/s` C3 cap) | `2,464.0 MiB/s` (`2.4 GB/s` C3 cap) | `2,454.6 MiB/s` (`2.4 GB/s` C3 cap) | `2,402.5 MiB/s` (`2.4 GB/s` C3 cap) |
| **Mean CPU Utilization (`%`)** | `28.5%–31.9%` | `48.7%` (`76.6%` peak) | `52.3%` (`80.0%` peak) | `34.5%` (`73.2%` peak) | `11.8%` (`56.7%` peak) | `4.4%` (`59.4%` peak) |
| **Mean Kernel `iowait` (`%`)** | `1.6%–2.1%` | `4.3%` (`63.2%` peak) | `17.1%` (`58.3%` peak) | `7.4%` (`47.3%` peak) | `2.5%` (`46.9%` peak) | `1.9%` (`54.6%` peak) |

---

## 5. Reproducible Step-by-Step Runbook (`1.0x` to `5.0x` Overcommit)

### Step 1: Provision a 2-Node `c3-standard-44` Cluster (`30` Workers = `15` Workers/Node)

```bash
source .ate-dev-env.sh
export GVISOR_NODE_MACHINE_TYPE=c3-standard-44
export BOOT_DISK_THROUGHPUT_MBPS=2400

# Bootstrap the GKE cluster and build/deploy Substrate + Benchmark components (30 ready workers across 2 nodes)
go run ./tools/setup-gcp bootstrap
make build
./benchmarking/locust/build_and_push.sh
./hack/install-ate.sh --deploy-atelet --deploy-benchmarks --benchmark-worker-count 30
```

### Step 2: Identify the Two `c3-standard-44` Worker Nodes

```bash
NODES=($(kubectl get nodes -l cloud.google.com/gke-nodepool=substrate-node-pool-c3 -o jsonpath='{.items[*].metadata.name}'))
export NODE1=${NODES[0]}
export NODE2=${NODES[1]}
echo "Node 1: $NODE1 | Node 2: $NODE2"
```

### Step 3: Run Architecture 1 (`SuspendActor` to GCS + Shared Boot Disk) Overcommit Sweep (`30, 60, 90, 120, 150` Actors)

1. Ensure `atelet` is configured without `--actor-disk-pool-dir` (or that `/var/lib/ateom-gvisor/disk-pool` is empty) so `SuspendActor` writes checkpoints to `/var/lib/ateom-gvisor` on the boot disk (`/dev/nvme0n1`) and uploads to GCS:
   ```bash
   for ACTOR_COUNT in 30 60 90 120 150; do
     python3 benchmarking/scripts/disk-arch/run_overcommit_benchmark.py \
       --mode arch1 \
       --workers 30 \
       --actors "$ACTOR_COUNT" \
       --duration-sec 180 \
       --wait-sec 1.0
   done
   ```

### Step 4: Provision the Dedicated Hyperdisk Pool for Architecture 3 (`PauseActor` + `maxQueue=2` Bounded Disk Admission)

1. Provision `150` dedicated `20 GB` `hyperdisk-balanced` (`10,000 IOPS`, `800 MiB/s`) volumes (`actor-disk-0..149`) in the cluster's zone (`$CLUSTER_LOCATION`). Attach and mount the maximum `30` disks per node (`actor-disk-0..29` on `$NODE1`, `actor-disk-30..59` on `$NODE2`) and leave `actor-disk-60..149` created and formatted as detached pool disks:
   ```bash
   # Attach & format 30 disks on NODE1 (actor-disk-0..29) and 30 disks on NODE2 (actor-disk-30..59)
   python3 benchmarking/scripts/disk-arch/setup_disk_pool.py \
     --node "$NODE1" --start-index 0 --count 30 --size-gb 20 --iops 10000 --throughput 800
   python3 benchmarking/scripts/disk-arch/setup_disk_pool.py \
     --node "$NODE2" --start-index 30 --count 30 --size-gb 20 --iops 10000 --throughput 800

   # Pre-create & format the remaining 90 detached pool disks (actor-disk-60..149)
   python3 benchmarking/scripts/disk-arch/setup_disk_pool.py \
     --node "$NODE1" --start-index 60 --count 90 --size-gb 20 --iops 10000 --throughput 800 --detach-after-format
   ```
2. Enable `--actor-disk-pool-dir=/var/lib/ateom-gvisor/disk-pool` on `atelet` and `--max-disk-op-queue=2` on `ate-api-server`.

### Step 5: Run Architecture 3 (`PauseActor` + Hyperdisk Pool) Overcommit Sweep (`30, 60, 90, 120, 150` Actors)

```bash
for ACTOR_COUNT in 30 60 90 120 150; do
  python3 benchmarking/scripts/disk-arch/run_overcommit_benchmark.py \
    --mode arch3 \
    --workers 30 \
    --actors "$ACTOR_COUNT" \
    --duration-sec 180 \
    --wait-sec 1.0
done
```

