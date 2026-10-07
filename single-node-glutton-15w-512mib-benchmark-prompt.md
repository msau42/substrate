# Glutton Benchmark Prompt (512MiB Memory, 32MiB Churn, 15 Actors, 17 Workers — Suspend & Pause)

## Agent Prompt

```markdown
Deploy and run two `glutton` benchmarks (`suspend` and `pause` lifecycle modes) on the current cluster using `.ate-dev-env.sh` with the following configuration:

**Execution & Output Guardrails (Preventing UI/Session Hangs)**:
- Do NOT fetch external web documentation (`read_url_content` / `search_web`); all required hardware thresholds and flags are specified below.
- Always set `BUILDKIT_PROGRESS=plain` and redirect `docker build` / `runner.py` stdout/stderr to a log file, printing only the tail and final `stats.csv`, so background PTY logs stay small.
- Do NOT use `benchmarking/deploy_locust.sh` or `benchmarking/locust/deploy.sh` for headless `runner.py` runs: `deployment/locust` lacks the `ate.dev/benchmarking=true:NoSchedule` toleration (causing `kubectl rollout status` to hang) and starts interactive `locust-master` + `boomer-worker` processes that collide with `runner.py` on ports `5557` and `8001`. Instead, deploy workloads with `benchmarking/workloads/deploy.sh` and run `runner.py` in a dedicated headless `benchmark-runner` Pod on `NODE_LOCUST`.

0. **Prerequisites (Cluster Hardware Verification, Control Plane Readiness & Node Isolation)**:
   - Source `.ate-dev-env.sh` and inspect all nodes in the cluster (via `kubectl get nodes` and `gcloud compute disks describe`):
     - Identify two nodes (`NODE_WORKER` and `NODE_LOCUST`) that meet all of the following hardware requirements:
       - **Machine type**: `c3-standard-44` (`node.kubernetes.io/instance-type=c3-standard-44`)
       - **Boot disk type**: `hyperdisk-balanced` (`type: .../diskTypes/hyperdisk-balanced`)
       - **Boot disk size & maxed-out performance**: `sizeGb: 100`, `provisionedIops: 50000`, `provisionedThroughput: 2400` MiB/s
     - Stop and report an error if fewer than 2 nodes meet these exact specifications.
   - **Isolate WorkerPool onto `NODE_WORKER` and the Benchmark Runner onto `NODE_LOCUST`**:
     - If the cluster has more than 2 nodes, **cordon all other nodes** (`kubectl cordon`) so no worker or runner pods can schedule onto them; ensure `NODE_WORKER` and `NODE_LOCUST` are uncordoned.
     - Ensure `ate-system` (`ate-api-server`, `ate-controller`, `atenet-router`, `atenet-egress`, `postgres`) and the `atelet` DaemonSet are `Running` and `Ready` (ensuring `NODE_WORKER` carries the `ate.dev/substrate-version` label required by the `atelet` DaemonSet, and `NODE_LOCUST` carries any `nodeSelector` label required by `ate-system` deployments such as `ate.dev/workload-role=benchmark-runner`).
     - Taint `NODE_WORKER` with `ate.dev/sandboxClass=gvisor:NoSchedule` (tolerated by `WorkerPool` `benchmark-ateom` pods and `atelet`, repelling the runner and `ate-system` deployments).
     - Taint `NODE_LOCUST` with `ate.dev/benchmarking=true:NoSchedule` (repelling `WorkerPool` `benchmark-ateom` pods).

1. **Workloads, WorkerPool & Headless Runner Pod**:
   - Deploy the `WorkerPool` and `ActorTemplate` resources using `benchmarking/workloads/deploy.sh --deploy --worker-count 17 --actor-memory 1024Mi` (providing 512MiB headroom above the 512MiB working set for the guest/runtime).
   - Build and push the runner image (`us-docker.pkg.dev/${PROJECT_ID}/gcr.io/ate-images/locust-test:latest`) with `BUILDKIT_PROGRESS=plain ./benchmarking/locust/build_and_push.sh >/tmp/locust-build.log 2>&1`.
   - Deploy a dedicated headless `benchmark-runner` Pod in namespace `benchmarking` with the `ate.dev/benchmarking=true:NoSchedule` toleration and `servicedns-ca` / `podidentity` projected volumes (matching `benchmarking/automation/manifests/runner-job.yaml.tmpl`).
   - Verify via `kubectl get pods -o wide` that all 17 `benchmark-ateom` worker pods are `Running`/`Ready` on `NODE_WORKER`, the `glutton` `ActorTemplate` golden snapshot is ready, and `benchmark-runner` is `Running` on `NODE_LOCUST`.

2. **Load & Memory Parameters (shared by both runs)**:
   - User class: `glutton` (`-f /app/tests/glutton.py`)
   - Duration: `3m` (`-t 3m`) per benchmark
   - Concurrent actors (users): `15` (`-u 15`, with default `--actors-per-user 1`)
   - Resident memory working set: `512MiB` (`--mem-target 512Mi`)
   - Per-cycle memory churn: `32MiB` (`--mem-churn 32Mi`)
   - Inter-cycle delay (wait time between suspend/pause and next resume): `1s` (`--min-wait-time 1.0 --max-wait-time 1.0`)
   - Runner flags: `--atelet-lag-s 0 --no-cluster-facts` (skips the 70s post-run Prometheus wait and cluster RBAC discovery)

3. **Benchmark Runs**:
   - Execute `python3 /app/runner.py` inside `pod/benchmark-runner` (redirecting container stdout/stderr to `/tmp/bench/<name>.out` and printing only the resulting `stats.csv`) for:
     - **Benchmark 1 (Suspend)**: Default durable suspend (`--lifecycle-mode suspend`), named `glutton_mem_512mi_15u_17w_suspend`.
     - **Benchmark 2 (Pause)**: Node-local pause (`--lifecycle-mode pause`), named `glutton_mem_512mi_15u_17w_pause`.
   - For each run, report and compare:
     - **Cycle throughput** (completed cycles/sec, i.e. `Requests/s` and total `Request Count` on `SuspendActor` / `PauseActor` / `GluttonPing`)
     - **Latency percentiles** (p50, p90, p99) and **failure counts** across `ResumeActorFirstResume`, `ResumeActor`, `SuspendActor` (or `PauseActor`), `GluttonFillRAM`, `GluttonChurnRAM`, and `GluttonPing`.
```

---

## Reference Commands

### 0. Verify Hardware, Cordon Extra Nodes, and Taint `NODE_WORKER` / `NODE_LOCUST`

```bash
source .ate-dev-env.sh

# Find nodes matching c3-standard-44 + hyperdisk-balanced (100 GiB, 50000 IOPS, 2400 MiB/s)
QUALIFIED_NODES=()
OTHER_NODES=()
for node in $(kubectl get nodes -o jsonpath='{.items[*].metadata.name}'); do
  mtype=$(kubectl get node "${node}" -o jsonpath='{.metadata.labels.node\.kubernetes\.io/instance-type}')
  zone=$(kubectl get node "${node}" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}')
  read -r dtype size iops tput <<<"$(gcloud compute disks describe "${node}" \
    --zone="${zone}" --project="${PROJECT_ID}" \
    --format="value(type.basename(),sizeGb,provisionedIops,provisionedThroughput)")"
  echo "${node}: machineType=${mtype} diskType=${dtype} sizeGb=${size} IOPS=${iops} throughput=${tput}"
  if [[ "${mtype}" == "c3-standard-44" && "${dtype}" == "hyperdisk-balanced" && "${size}" == "100" && "${iops}" == "50000" && "${tput}" == "2400" ]]; then
    QUALIFIED_NODES+=("${node}")
  else
    OTHER_NODES+=("${node}")
  fi
done

if (( ${#QUALIFIED_NODES[@]} < 2 )); then
  echo "ERROR: Expected at least 2 maxed-out c3-standard-44 + hyperdisk-balanced nodes, found ${#QUALIFIED_NODES[@]}" >&2
  exit 1
fi

NODE_WORKER="${QUALIFIED_NODES[0]}"
NODE_LOCUST="${QUALIFIED_NODES[1]}"
EXTRA_NODES=("${QUALIFIED_NODES[@]:2}" "${OTHER_NODES[@]}")

# Uncordon the 2 target nodes and cordon all other nodes
kubectl uncordon "${NODE_WORKER}" "${NODE_LOCUST}"
if (( ${#EXTRA_NODES[@]} > 0 )); then
  kubectl cordon "${EXTRA_NODES[@]}"
fi

# Taint NODE_WORKER for WorkerPool pods and NODE_LOCUST for the benchmark runner
kubectl taint nodes "${NODE_WORKER}" ate.dev/sandboxClass=gvisor:NoSchedule --overwrite
kubectl taint nodes "${NODE_LOCUST}" ate.dev/benchmarking=true:NoSchedule --overwrite
```

### 1. Deploy Workloads and Headless `benchmark-runner` Pod

```bash
source .ate-dev-env.sh

# Deploy 17 WorkerPool pods (on NODE_WORKER) and ActorTemplates with 1024Mi memory limit
./benchmarking/workloads/deploy.sh --deploy \
  --worker-count 17 \
  --actor-memory 1024Mi

# Build and push the runner image with plain progress redirected to a log file
BUILDKIT_PROGRESS=plain ./benchmarking/locust/build_and_push.sh >/tmp/locust-build.log 2>&1
tail -n 20 /tmp/locust-build.log

# Remove any interactive deployment/locust and deploy an idle headless benchmark-runner pod on NODE_LOCUST
kubectl create namespace benchmarking --dry-run=client -o yaml | kubectl apply -f -
kubectl delete deployment locust -n benchmarking --ignore-not-found
kubectl delete pod benchmark-runner -n benchmarking --ignore-not-found

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: benchmark-runner
  namespace: benchmarking
spec:
  tolerations:
  - key: ate.dev/benchmarking
    operator: Equal
    value: "true"
    effect: NoSchedule
  containers:
  - name: runner
    image: us-docker.pkg.dev/${PROJECT_ID}/gcr.io/ate-images/locust-test:latest
    imagePullPolicy: Always
    command: ["python3", "-c", "import time; time.sleep(86400)"]
    volumeMounts:
    - name: servicedns-ca
      mountPath: /run/servicedns-ca
      readOnly: true
    - name: podidentity
      mountPath: /run/podidentity.podcert.ate.dev
      readOnly: true
  volumes:
  - name: servicedns-ca
    projected:
      sources:
      - clusterTrustBundle:
          signerName: servicedns.podcert.ate.dev/identity
          labelSelector:
            matchLabels:
              podcert.ate.dev/canarying: live
          path: ca.crt
  - name: podidentity
    projected:
      sources:
      - podCertificate:
          signerName: podidentity.podcert.ate.dev/identity
          keyType: ECDSAP256
          credentialBundlePath: credential-bundle.pem
EOF

kubectl wait --for=condition=Ready pod/benchmark-runner -n benchmarking --timeout=60s
kubectl get pods -n benchmark-workloads -o wide
kubectl get pod benchmark-runner -n benchmarking -o wide
```

### 2. Headless Runner Invocations

#### Benchmark 1: Suspend (`--lifecycle-mode suspend`)

```bash
kubectl exec -n benchmarking pod/benchmark-runner -- \
  python3 /app/runner.py \
    -f /app/tests/glutton.py \
    -t 3m \
    -u 15 \
    --tag manual \
    --name glutton_mem_512mi_15u_17w_suspend \
    --dest /tmp/bench \
    --atelet-lag-s 0 \
    --no-cluster-facts \
    --lifecycle-mode suspend \
    --mem-target 512Mi \
    --mem-churn 32Mi \
    --min-wait-time 1.0 \
    --max-wait-time 1.0 >/tmp/glutton_suspend.log 2>&1

tail -n 15 /tmp/glutton_suspend.log
kubectl exec -n benchmarking pod/benchmark-runner -- \
  python3 -c "import glob, pathlib; [print(p, '\n' + pathlib.Path(p).read_text()) for p in sorted(glob.glob('/tmp/bench/runs/glutton_mem_512mi_15u_17w_suspend/*/*/run_tag=manual/stats.csv'))]"
```

#### Benchmark 2: Pause (`--lifecycle-mode pause`)

```bash
kubectl exec -n benchmarking pod/benchmark-runner -- \
  python3 /app/runner.py \
    -f /app/tests/glutton.py \
    -t 3m \
    -u 15 \
    --tag manual \
    --name glutton_mem_512mi_15u_17w_pause \
    --dest /tmp/bench \
    --atelet-lag-s 0 \
    --no-cluster-facts \
    --lifecycle-mode pause \
    --mem-target 512Mi \
    --mem-churn 32Mi \
    --min-wait-time 1.0 \
    --max-wait-time 1.0 >/tmp/glutton_pause.log 2>&1

tail -n 15 /tmp/glutton_pause.log
kubectl exec -n benchmarking pod/benchmark-runner -- \
  python3 -c "import glob, pathlib; [print(p, '\n' + pathlib.Path(p).read_text()) for p in sorted(glob.glob('/tmp/bench/runs/glutton_mem_512mi_15u_17w_pause/*/*/run_tag=manual/stats.csv'))]"
```
