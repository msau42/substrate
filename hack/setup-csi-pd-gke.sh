#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Deploys an in-cluster GCE PD CSI controller, a Substrate-aware PD CSI node
# DaemonSet with /var/lib/ate bidirectional mount propagation, a
# CSIDriverConfig for pd.csi.storage.gke.io, and the csi-hyperdisk-balanced-sc
# StorageClass on a GKE cluster.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

if [[ -f .ate-dev-env.sh ]]; then
  source .ate-dev-env.sh
fi

if [[ -z "${PROJECT_ID:-}" ]]; then
  echo "Error: PROJECT_ID environment variable is not set." >&2
  exit 1
fi

GSA_NAME="csi-gce-pd-controller"
GSA_EMAIL="${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
KSA_NAMESPACE="kube-system"
KSA_NAME="csi-gce-pd-controller-sa"

echo "Ensuring GCP Service Account ${GSA_EMAIL} exists..."
if ! gcloud iam service-accounts describe "${GSA_EMAIL}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud iam service-accounts create "${GSA_NAME}" \
    --project="${PROJECT_ID}" \
    --display-name="GCE PD CSI Controller for Agent Substrate"
fi

for role in roles/compute.storageAdmin roles/compute.instanceAdmin.v1 roles/iam.serviceAccountUser; do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${GSA_EMAIL}" \
    --role="${role}" \
    --condition=None \
    --quiet >/dev/null
done

gcloud iam service-accounts add-iam-policy-binding "${GSA_EMAIL}" \
  --project="${PROJECT_ID}" \
  --role="roles/iam.workloadIdentityUser" \
  --member="serviceAccount:${PROJECT_ID}.svc.id.goog[${KSA_NAMESPACE}/${KSA_NAME}]" \
  --quiet >/dev/null

# Discover the PD CSI driver image from the cluster's built-in pdcsi-node DaemonSet
# if available, falling back to a known GKE release image.
PDCSI_IMAGE="$(kubectl get daemonset pdcsi-node -n kube-system \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="gce-pd-driver")].image}' 2>/dev/null || true)"
if [[ -z "${PDCSI_IMAGE}" ]]; then
  PDCSI_IMAGE="us-central1-artifactregistry.gcr.io/gke-release/gke-release/gcp-compute-persistent-disk-csi-driver:v1.23.1-gke.20"
fi
echo "Using GCE PD CSI driver image: ${PDCSI_IMAGE}"

if kubectl get daemonset csi-gce-pd-node-substrate -n "${KSA_NAMESPACE}" >/dev/null 2>&1; then
  CURRENT_DS_SA="$(kubectl get daemonset csi-gce-pd-node-substrate -n "${KSA_NAMESPACE}" \
    -o jsonpath='{.spec.template.spec.serviceAccountName}' 2>/dev/null || true)"
  if [[ "${CURRENT_DS_SA}" != "${KSA_NAME}" ]]; then
    kubectl delete daemonset csi-gce-pd-node-substrate -n "${KSA_NAMESPACE}" --ignore-not-found
  fi
fi

echo "Applying GCE PD CSI controller, node DaemonSet, CSIDriverConfig, and StorageClass..."
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${KSA_NAME}
  namespace: ${KSA_NAMESPACE}
  annotations:
    iam.gke.io/gcp-service-account: ${GSA_EMAIL}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: csi-gce-pd-controller-role
rules:
- apiGroups: [""]
  resources: ["nodes", "persistentvolumes", "persistentvolumeclaims", "events", "configmaps"]
  verbs: ["get", "list", "watch", "create", "update", "patch"]
- apiGroups: ["storage.k8s.io"]
  resources: ["storageclasses", "volumeattachments", "csinodes", "volumeattributesclasses"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["Snapshot.storage.k8s.io", "snapshot.storage.k8s.io"]
  resources: ["volumesnapshots", "volumesnapshotcontents", "volumesnapshotclasses"]
  verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: csi-gce-pd-controller-binding
subjects:
- kind: ServiceAccount
  name: ${KSA_NAME}
  namespace: ${KSA_NAMESPACE}
roleRef:
  kind: ClusterRole
  name: csi-gce-pd-controller-role
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: v1
kind: Service
metadata:
  name: csi-gce-pd-controller
  namespace: ${KSA_NAMESPACE}
spec:
  selector:
    app: csi-gce-pd-controller
  ports:
  - name: grpc
    port: 50053
    targetPort: 10000
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: csi-gce-pd-controller
  namespace: ${KSA_NAMESPACE}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: csi-gce-pd-controller
  template:
    metadata:
      labels:
        app: csi-gce-pd-controller
    spec:
      serviceAccountName: ${KSA_NAME}
      containers:
      - name: gce-pd-driver
        image: ${PDCSI_IMAGE}
        args:
        - --v=5
        - --endpoint=unix:/csi/csi.sock
        - --run-controller-service=true
        - --run-node-service=false
        volumeMounts:
        - name: socket-dir
          mountPath: /csi
      - name: socat
        image: docker.io/alpine/socat:1.7.4.3-r0
        args:
        - tcp-listen:10000,fork,reuseaddr
        - unix-connect:/csi/csi.sock
        volumeMounts:
        - name: socket-dir
          mountPath: /csi
      volumes:
      - name: socket-dir
        emptyDir: {}
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: csi-gce-pd-node-substrate
  namespace: ${KSA_NAMESPACE}
spec:
  selector:
    matchLabels:
      app: csi-gce-pd-node-substrate
  template:
    metadata:
      labels:
        app: csi-gce-pd-node-substrate
    spec:
      serviceAccountName: ${KSA_NAME}
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet
      tolerations:
      - operator: Exists
      containers:
      - name: gce-pd-driver
        image: ${PDCSI_IMAGE}
        args:
        - --v=5
        - --endpoint=unix:/csi/csi.sock
        - --run-controller-service=false
        - --run-node-service=true
        - --enable-data-cache=true
        - --node-name=\$(KUBE_NODE_NAME)
        - --http-endpoint=:9931
        - --dynamic-volumes=true
        env:
        - name: KUBE_NODE_NAME
          valueFrom:
            fieldRef:
              apiVersion: v1
              fieldPath: spec.nodeName
        securityContext:
          privileged: true
          readOnlyRootFilesystem: true
        volumeMounts:
        - name: ateom-dir
          mountPath: /var/lib/ate
          mountPropagation: Bidirectional
        - name: kubelet-plugin-dir
          mountPath: /var/lib/kubelet/plugins/kubernetes.io/csi
          mountPropagation: Bidirectional
        - name: kubelet-pods
          mountPath: /var/lib/kubelet/pods
          mountPropagation: Bidirectional
        - name: plugin-dir
          mountPath: /csi
        - name: device-dir
          mountPath: /dev
        - name: udev-rules-etc
          mountPath: /etc/udev
        - name: udev-rules-lib
          mountPath: /lib/udev
        - name: udev-socket
          mountPath: /run/udev
        - name: sys
          mountPath: /sys
        - name: tmp
          mountPath: /tmp
        - name: modules
          mountPath: /lib/modules
          readOnly: true
        - name: lvm-dir
          mountPath: /etc/lvm
      volumes:
      - name: ateom-dir
        hostPath:
          path: /var/lib/ate
          type: DirectoryOrCreate
      - name: kubelet-pods
        hostPath:
          path: /var/lib/kubelet/pods
          type: Directory
      - name: plugin-dir
        hostPath:
          path: /var/lib/kubelet/plugins/pd.csi.storage.gke.io-substrate/
          type: DirectoryOrCreate
      - name: kubelet-plugin-dir
        hostPath:
          path: /var/lib/kubelet/plugins/kubernetes.io/csi
          type: DirectoryOrCreate
      - name: device-dir
        hostPath:
          path: /dev
          type: Directory
      - name: udev-rules-etc
        hostPath:
          path: /etc/udev
          type: Directory
      - name: udev-rules-lib
        hostPath:
          path: /lib/udev
          type: Directory
      - name: udev-socket
        hostPath:
          path: /run/udev
          type: Directory
      - name: sys
        hostPath:
          path: /sys
          type: Directory
      - name: tmp
        emptyDir:
          sizeLimit: 5Mi
      - name: lvm-dir
        emptyDir:
          sizeLimit: 5Mi
      - name: modules
        hostPath:
          path: /lib/modules
          type: Directory
---
apiVersion: ate.dev/v1alpha1
kind: CSIDriverConfig
metadata:
  name: pd.csi.storage.gke.io
spec:
  driverName: pd.csi.storage.gke.io
  controllerEndpoint: tcp://csi-gce-pd-controller.kube-system.svc.cluster.local:50053
  nodeSocketOverride: unix:///var/lib/kubelet/plugins/pd.csi.storage.gke.io-substrate/csi.sock
---
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: csi-hyperdisk-balanced-sc
provisioner: pd.csi.storage.gke.io
parameters:
  type: hyperdisk-balanced
  provisioned-iops-on-create: "5000"
  provisioned-throughput-on-create: "1250Mi"
reclaimPolicy: Delete
volumeBindingMode: Immediate
EOF

echo "Waiting for csi-gce-pd-controller and csi-gce-pd-node-substrate to be ready..."
kubectl rollout status deployment/csi-gce-pd-controller -n "${KSA_NAMESPACE}" --timeout=180s
kubectl rollout status daemonset/csi-gce-pd-node-substrate -n "${KSA_NAMESPACE}" --timeout=180s
echo "GCE PD CSI setup complete."
