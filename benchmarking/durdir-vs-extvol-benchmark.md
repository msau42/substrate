# Reproducing the Storage Architecture Benchmarks (`durableDir` vs. External Volumes)

This guide provides step-by-step instructions to reproduce the three storage architecture benchmarks:

1. **`durdir_full_512mb`**: `durableDir` + GCS Object Storage (`SuspendActor` / `ResumeActor`)
2. **`durdir_full_512mb_pause`**: `durableDir` + Local Node Storage (`PauseActor` / `ResumeActor`)
3. **`durdir_full_512mb_extvol_pd`**: `externalVolumeTemplate` + Hyperdisk Balanced with Same-Node Retention (`SuspendActor` / `ResumeActor`)

---

## 1. Prerequisites & Environment Configuration

1. Check out the benchmark branch (`benchmark2`):
   ```bash
   git checkout benchmark2
   ```
2. Source your environment configuration file (see [`../hack/ate-dev-env.sh.example`](../hack/ate-dev-env.sh.example)):
   ```bash
   source .ate-dev-env.sh
   ```
   Ensure the following variables are exported:
   - `PROJECT_ID`: GCP project ID
   - `CLUSTER_NAME`: GKE cluster name (e.g., `ate-bench`)
   - `ZONE`: GCP zone (e.g., `us-central1-a`)
   - `BUCKET_NAME`: GCS bucket used by `ateapi` / `atelet` for snapshots
   - `KO_DOCKER_REPO`: Artifact Registry repository for container images

   > [!IMPORTANT]
   > `hyperdisk-balanced` requires a compatible GCE machine series such as **C3** (e.g., `c3-standard-44`). Ensure your GKE worker node pool uses a machine type that supports Hyperdisk Balanced.

---

## 2. Build and Deploy Agent Substrate

Build and deploy the Agent Substrate control plane (`ateapi`) and data plane (`atelet`, `atenet`, `envoy-dataplane`):

```bash
./hack/install-ate.sh --deploy-ate
```

Wait for all pods in `ate-system` to become ready:

```bash
kubectl get pods -n ate-system
```

---

## 3. Configure GCE PD CSI Driver & Hyperdisk StorageClass

Run [`../hack/setup-csi-pd-gke.sh`](../hack/setup-csi-pd-gke.sh) to deploy:
- The in-cluster `csi-gce-pd-controller` Deployment and Service (`tcp://csi-gce-pd-controller.kube-system.svc.cluster.local:50053`)
- The `csi-gce-pd-node-substrate` DaemonSet with bidirectional mount propagation on `/var/lib/ate`
- The `CSIDriverConfig` (`pd.csi.storage.gke.io`)
- The `StorageClass` (`csi-hyperdisk-balanced-sc`: `type=hyperdisk-balanced`, `provisioned-iops-on-create=5000`, `provisioned-throughput-on-create=1250Mi`)

```bash
./hack/setup-csi-pd-gke.sh
```

> [!NOTE]
> If your GKE cluster nodes use the default Compute Engine service account instead of Workload Identity, grant the node service account permissions to create and attach disks:
> ```bash
> PROJECT_NUMBER="$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')"
> for role in roles/compute.storageAdmin roles/compute.instanceAdmin.v1; do
>   gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
>     --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
>     --role="${role}" \
>     --condition=None
> done
> ```

Verify the CSI components and `StorageClass` are ready:

```bash
kubectl get csidriverconfig pd.csi.storage.gke.io
kubectl get storageclass csi-hyperdisk-balanced-sc
kubectl get pods -n kube-system -l app=csi-gce-pd-controller
kubectl get pods -n kube-system -l app=csi-gce-pd-node-substrate
```

---

## 4. Deploy Benchmark Workloads and Locust (`durdir` Boomer Worker)

1. Deploy the `glutton` workload image, `WorkerPool` (`1` replica), and `ActorTemplate` manifests ([`workloads/manifests/glutton-durdir-full-template.yaml.tmpl`](workloads/manifests/glutton-durdir-full-template.yaml.tmpl) and [`workloads/manifests/glutton-extvol-full-template.yaml.tmpl`](workloads/manifests/glutton-extvol-full-template.yaml.tmpl)):
   ```bash
   ./benchmarking/workloads/deploy.sh --deploy --worker-count 1
   ```

2. Build and deploy Locust with the `durdir` boomer worker:
   ```bash
   ./benchmarking/locust/deploy.sh --deploy --user-class durdir
   ```

3. Verify the `WorkerPool` and `ActorTemplate`s are registered in `ateapi`:
   ```bash
   make build-atectl
   ./bin/kubectl-ate get actortemplates -n default
   kubectl get pods -n benchmarking
   ```
   You should see both `glutton-durdir-full` and `glutton-extvol-full` listed.

---

## 5. Run the Benchmark Scenarios

All three scenarios are defined in [`automation/tests.yaml`](automation/tests.yaml) and can be executed directly inside the running `locust` master pod via `runner.py`.

### 5.1 Scenario 1: `durdir_full_512mb` (`durableDir` + GCS Object Storage)

1. Configure the `locust-boomer` worker with the `durdir_full_512mb` flags:
   ```bash
   kubectl set env deployment/locust-boomer -n benchmarking \
     BOOMER_FLAGS="--durdir-template glutton-durdir-full --lifecycle-mode suspend --resume-mode explicit --durdir-read-mode digest --durdir-file-size-bytes 536870912 --durdir-overwrite-size-bytes 16777216 --min-wait-time 1.0 --max-wait-time 1.0"
   kubectl rollout status deployment/locust-boomer -n benchmarking --timeout=120s
   ```

2. Run the 3-minute test from the `locust` master pod:
   ```bash
   kubectl exec -n benchmarking deploy/locust -c locust -- \
     python3 /app/runner.py \
       -f /app/tests/durdir.py \
       -t 3m \
       -u 1 \
       --name durdir_full_512mb \
       --dest /tmp/bench_durdir_full_512mb \
       --durdir-template glutton-durdir-full \
       --lifecycle-mode suspend \
       --resume-mode explicit \
       --durdir-read-mode digest \
       --durdir-file-size-bytes 536870912 \
       --durdir-overwrite-size-bytes 16777216 \
       --min-wait-time 1.0 \
       --max-wait-time 1.0
   ```

3. View or copy the results:
   ```bash
   kubectl exec -n benchmarking deploy/locust -c locust -- cat /tmp/bench_durdir_full_512mb/stats.csv
   kubectl exec -n benchmarking deploy/locust -c locust -- cat /tmp/bench_durdir_full_512mb/stats.jsonl
   ```

---

### 5.2 Scenario 2: `durdir_full_512mb_pause` (`durableDir` + Local Node Storage)

1. Configure the `locust-boomer` worker with `--lifecycle-mode pause`:
   ```bash
   kubectl set env deployment/locust-boomer -n benchmarking \
     BOOMER_FLAGS="--durdir-template glutton-durdir-full --lifecycle-mode pause --resume-mode explicit --durdir-read-mode digest --durdir-file-size-bytes 536870912 --durdir-overwrite-size-bytes 16777216 --min-wait-time 1.0 --max-wait-time 1.0"
   kubectl rollout status deployment/locust-boomer -n benchmarking --timeout=120s
   ```

2. Run the 3-minute test from the `locust` master pod:
   ```bash
   kubectl exec -n benchmarking deploy/locust -c locust -- \
     python3 /app/runner.py \
       -f /app/tests/durdir.py \
       -t 3m \
       -u 1 \
       --name durdir_full_512mb_pause \
       --dest /tmp/bench_durdir_full_512mb_pause \
       --durdir-template glutton-durdir-full \
       --lifecycle-mode pause \
       --resume-mode explicit \
       --durdir-read-mode digest \
       --durdir-file-size-bytes 536870912 \
       --durdir-overwrite-size-bytes 16777216 \
       --min-wait-time 1.0 \
       --max-wait-time 1.0
   ```

3. View or copy the results:
   ```bash
   kubectl exec -n benchmarking deploy/locust -c locust -- cat /tmp/bench_durdir_full_512mb_pause/stats.csv
   kubectl exec -n benchmarking deploy/locust -c locust -- cat /tmp/bench_durdir_full_512mb_pause/stats.jsonl
   ```

---

### 5.3 Scenario 3: `durdir_full_512mb_extvol_pd` (`externalVolumeTemplate` + Hyperdisk Balanced Same-Node)

1. Configure the `locust-boomer` worker with `--durdir-template glutton-extvol-full` and `--lifecycle-mode suspend`:
   ```bash
   kubectl set env deployment/locust-boomer -n benchmarking \
     BOOMER_FLAGS="--durdir-template glutton-extvol-full --lifecycle-mode suspend --resume-mode explicit --durdir-read-mode digest --durdir-file-size-bytes 536870912 --durdir-overwrite-size-bytes 16777216 --min-wait-time 1.0 --max-wait-time 1.0"
   kubectl rollout status deployment/locust-boomer -n benchmarking --timeout=120s
   ```

2. Run the 3-minute test from the `locust` master pod:
   ```bash
   kubectl exec -n benchmarking deploy/locust -c locust -- \
     python3 /app/runner.py \
       -f /app/tests/durdir.py \
       -t 3m \
       -u 1 \
       --name durdir_full_512mb_extvol_pd \
       --dest /tmp/bench_durdir_full_512mb_extvol_pd \
       --durdir-template glutton-extvol-full \
       --lifecycle-mode suspend \
       --resume-mode explicit \
       --durdir-read-mode digest \
       --durdir-file-size-bytes 536870912 \
       --durdir-overwrite-size-bytes 16777216 \
       --min-wait-time 1.0 \
       --max-wait-time 1.0
   ```

3. View or copy the results:
   ```bash
   kubectl exec -n benchmarking deploy/locust -c locust -- cat /tmp/bench_durdir_full_512mb_extvol_pd/stats.csv
   kubectl exec -n benchmarking deploy/locust -c locust -- cat /tmp/bench_durdir_full_512mb_extvol_pd/stats.jsonl
   ```

---

## 6. Teardown & Cleanup

To tear down the Locust deployment and benchmark workloads:

```bash
./benchmarking/deploy_locust.sh --delete
```

To verify or clean up any dynamically provisioned GCE Hyperdisks left from `externalVolumeTemplate` runs:

```bash
gcloud compute disks list --project="${PROJECT_ID}" --filter="zone:(${ZONE}) AND name~'^pvc-'"
```
