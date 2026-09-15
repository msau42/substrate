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

"""Queries Cloud Monitoring per-disk hypervisor metrics for Arch 1, Arch 2, Arch 3a, and Arch 3b."""

import os
import sys

sys.path.insert(0, "/usr/lib/google-cloud-sdk/lib/third_party")
sys.path.insert(0, "/usr/lib/google-cloud-sdk/lib")
from googlecloudsdk.core import properties
from googlecloudsdk.core.credentials import requests as creds_requests

PROJECT_ID = os.environ.get("PROJECT_ID", "msau-gke-dev")
properties.VALUES.core.project.Set(PROJECT_ID)
session = creds_requests.GetSession()
url = f"https://monitoring.mtls.googleapis.com/v3/projects/{PROJECT_ID}/timeSeries"

if len(sys.argv) < 3:
    print(
        f"Usage: {sys.argv[0]} <start_iso8601_utc> <end_iso8601_utc>\n"
        f"Example: {sys.argv[0]} 2026-09-15T02:55:00Z 2026-09-15T03:02:00Z"
    )
    sys.exit(1)

start_t = sys.argv[1]
end_t = sys.argv[2]


def get_series(metric_type: str, aligner: str) -> list[dict]:
    params = {
        "filter": f'metric.type = "{metric_type}"',
        "interval.startTime": start_t,
        "interval.endTime": end_t,
        "aggregation.alignmentPeriod": "60s",
        "aggregation.perSeriesAligner": aligner,
    }
    return session.get(url, params=params).json().get("timeSeries", [])


write_burst = get_series(
    "compute.googleapis.com/instance/disk/max_write_bytes_count", "ALIGN_MAX"
)
write_avg = get_series(
    "compute.googleapis.com/instance/disk/write_bytes_count", "ALIGN_RATE"
)
read_burst = get_series(
    "compute.googleapis.com/instance/disk/max_read_bytes_count", "ALIGN_MAX"
)
read_avg = get_series(
    "compute.googleapis.com/instance/disk/read_bytes_count", "ALIGN_RATE"
)
lat = get_series(
    "compute.googleapis.com/instance/disk/average_io_latency", "ALIGN_MEAN"
)
qdepth = get_series(
    "compute.googleapis.com/instance/disk/average_io_queue_depth", "ALIGN_MEAN"
)

data_by_ts = {}
for name, series, scale in [
    ("peak_w_mb_s", write_burst, 1 / (1024 * 1024)),
    ("avg_w_mb_s", write_avg, 1 / (1024 * 1024)),
    ("peak_r_mb_s", read_burst, 1 / (1024 * 1024)),
    ("avg_r_mb_s", read_avg, 1 / (1024 * 1024)),
    ("lat_ms", lat, 1 / 1000.0),
    ("qdepth", qdepth, 1.0),
]:
    for s in series:
        dev = s["metric"]["labels"]["device_name"]
        inst = s["resource"]["labels"]["instance_id"][-6:]
        for p in s["points"]:
            ts = p["interval"]["endTime"][:16]
            val = (
                float(
                    p["value"].get("doubleValue", p["value"].get("int64Value", 0))
                )
                * scale
            )
            cur = data_by_ts.setdefault((ts, inst, dev), {})
            cur[name] = max(cur.get(name, 0), val)

print("=== PER DEVICE TELEMETRY ===")
node_agg = {}
for (ts, inst, dev), d in sorted(data_by_ts.items()):
    pw = d.get("peak_w_mb_s", 0)
    aw = d.get("avg_w_mb_s", 0)
    pr = d.get("peak_r_mb_s", 0)
    ar = d.get("avg_r_mb_s", 0)
    lm = d.get("lat_ms", 0)
    qd = d.get("qdepth", 0)
    if pw > 1.0 or pr > 1.0 or "gke-ate-bench" in dev:
        print(
            f"{ts} | Node:{inst} | Dev:{dev:18s} | PeakW:{pw:6.1f} MiB/s (60s:{aw:5.1f}) | "
            f"PeakR:{pr:6.1f} MiB/s (60s:{ar:5.1f}) | Lat:{lm:5.2f}ms | QD:{qd:5.2f}"
        )
    if dev.startswith("actor-disk-"):
        agg = node_agg.setdefault(
            (ts, inst),
            {
                "avg_w": 0.0,
                "avg_r": 0.0,
                "sum_peak_w": 0.0,
                "sum_peak_r": 0.0,
                "max_lat": 0.0,
                "sum_qd": 0.0,
            },
        )
        agg["avg_w"] += aw
        agg["avg_r"] += ar
        agg["sum_peak_w"] += pw
        agg["sum_peak_r"] += pr
        agg["max_lat"] = max(agg["max_lat"], lm)
        agg["sum_qd"] += qd

print("\n=== NODE AGGREGATE (ACTOR DISKS) ===")
for (ts, inst), agg in sorted(node_agg.items()):
    if agg["sum_peak_w"] > 1.0:
        print(
            f"{ts} | Node:{inst} | SumPeakW:{agg['sum_peak_w']:6.1f} MiB/s | "
            f"Sum60sAvgW:{agg['avg_w']:5.1f} MiB/s | MaxLat:{agg['max_lat']:5.2f}ms | SumQD:{agg['sum_qd']:5.2f}"
        )
