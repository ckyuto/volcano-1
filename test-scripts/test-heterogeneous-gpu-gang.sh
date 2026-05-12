#!/bin/bash
# Heterogeneous GPU gang test: a single Volcano Job whose tasks mix full
# nvidia.com/gpu pods with volcano.sh/vgpu-* pods. Pre-v0.0.6, the
# Ghost-Reservation Bug (shallow CloneOthers + LinkedIn collocate
# simulation leaking UsedMem onto non-vGPU nodes via untyped-nil device
# entries) caused gangs like this to get stuck Pending. v0.0.6 ships
# #5235's deep CloneOthers + #5290's typed-nil preservation, which should
# let the gang schedule cleanly with each task landing on the right node
# type.
set -e

source "$(dirname "$0")/_lib.sh"

FULL_GPU_NODE=""
VGPU_NODE=""

echo "============================================================"
echo "  HETEROGENEOUS GANG TEST: full GPU + vGPU in one Job"
echo "============================================================"

######################################################################
echo "=== PHASE 0: Build and load v0.0.6 scheduler image ==="
docker buildx build --no-cache \
  --output=type=docker \
  -t volcanosh/vc-scheduler:v0.0.6-volcano1.14.0 \
  -f installer/dockerfile/scheduler/Dockerfile . 2>&1 | tail -5
echo "  Loading image into minikube (multi-node)..."
minikube image load volcanosh/vc-scheduler:v0.0.6-volcano1.14.0 2>&1 | tail -5
echo ""

######################################################################
echo "=== PHASE 1: Apply prod-lor1-k8s-2 scheduler config ==="
kubectl apply -f "$(dirname "$0")/scheduler-config.prod-like.yaml"

echo ""
echo "=== PHASE 1: point scheduler deployment at v0.0.6 image ==="
kubectl set image -n volcano-system deployment/volcano-scheduler \
  volcano-scheduler=volcanosh/vc-scheduler:v0.0.6-volcano1.14.0
kubectl patch deployment volcano-scheduler -n volcano-system \
  -p '{"spec":{"template":{"spec":{"containers":[{"name":"volcano-scheduler","imagePullPolicy":"Never"}]}}}}'

echo ""
echo "=== PHASE 1: restart scheduler ==="
restart_scheduler

echo ""
echo "=== PHASE 1: label nodes with maintenance-zone (colocation MZ plugin) ==="
setup_mz_labels

echo ""
echo "=== PHASE 1: pick a full-GPU node and a vGPU node ==="
ALL_NODES=($(kubectl get nodes -o jsonpath='{.items[*].metadata.name}'))
FULL_GPU_NODE="${ALL_NODES[0]}"
VGPU_NODE="${ALL_NODES[1]:-${ALL_NODES[0]}}"
echo "  full GPU node: ${FULL_GPU_NODE}"
echo "  vGPU node:     ${VGPU_NODE}"

if [ "$FULL_GPU_NODE" = "$VGPU_NODE" ]; then
  echo -e "  ${RED}ABORT${NC}: cluster has only one node — heterogeneous test needs at least two"
  exit 2
fi

echo ""
echo "=== PHASE 1: configure ${FULL_GPU_NODE} with nvidia.com/gpu=4 (full GPU) ==="
setup_full_gpu_node "${FULL_GPU_NODE}" 4

echo ""
echo "=== PHASE 1: configure ${VGPU_NODE} with vGPU annotations (8 GPUs × 2 slots) ==="
NODE="${VGPU_NODE}"
kubectl proxy --port=8202 &
PROXY_PID=$!
sleep 2
curl -s -X PATCH \
  -H "Content-Type: application/json-patch+json" \
  --data '[
    {"op": "add", "path": "/status/capacity/volcano.sh~1vgpu-number", "value": "16"},
    {"op": "add", "path": "/status/capacity/volcano.sh~1vgpu-memory", "value": "131072"},
    {"op": "add", "path": "/status/capacity/volcano.sh~1vgpu-cores", "value": "1600"}
  ]' \
  http://localhost:8202/api/v1/nodes/${VGPU_NODE}/status > /dev/null
kill $PROXY_PID 2>/dev/null; wait $PROXY_PID 2>/dev/null || true

kubectl annotate node ${VGPU_NODE} --overwrite \
  volcano.sh/node-vgpu-register="GPU-0,2,16384,NVIDIA,true,hami-core:GPU-1,2,16384,NVIDIA,true,hami-core:GPU-2,2,16384,NVIDIA,true,hami-core:GPU-3,2,16384,NVIDIA,true,hami-core:GPU-4,2,16384,NVIDIA,true,hami-core:GPU-5,2,16384,NVIDIA,true,hami-core:GPU-6,2,16384,NVIDIA,true,hami-core:GPU-7,2,16384,NVIDIA,true,hami-core" > /dev/null

(
  while true; do
    kubectl annotate node ${VGPU_NODE} --overwrite volcano.sh/node-vgpu-handshake="Active" 2>/dev/null
    sleep 30
  done
) &
KEEPALIVE_PID=$!
trap "kill $KEEPALIVE_PID 2>/dev/null" EXIT

echo "  waiting for scheduler to discover vGPU node..."
deadline=$((SECONDS + 30))
while [ $SECONDS -lt $deadline ]; do
  handshake=$(kubectl get node ${VGPU_NODE} -o jsonpath='{.metadata.annotations.volcano\.sh/node-vgpu-handshake}' 2>/dev/null)
  if [[ "$handshake" == "Active" ]]; then
    sleep 3
    break
  fi
  sleep 2
done

echo ""
echo "=== PHASE 1: pre-flight — confirm deviceshare Allocate path is engaged ==="
require_deviceshare_allocate_engaged

######################################################################
echo ""
echo "======================================================"
echo "  TEST 1: Heterogeneous gang — 1 full GPU task + 2 vGPU tasks"
echo "======================================================"
echo "  Volcano Job with minAvailable=3 must gang-schedule cleanly."
echo "  Pre-v0.0.6 (Ghost-Reservation Bug): such gangs got stuck Pending"
echo "  because shallow CloneOthers + simulation leaked UsedMem onto"
echo "  the full-GPU (non-vGPU) node."
cleanup

cat <<EOF | kubectl apply -f -
apiVersion: batch.volcano.sh/v1alpha1
kind: Job
metadata:
  name: hetero-gang
spec:
  minAvailable: 3
  schedulerName: volcano
  policies:
  - event: PodEvicted
    action: RestartJob
  tasks:
  - replicas: 1
    name: head
    template:
      metadata:
        labels:
          role: head-fullgpu
      spec:
        schedulerName: volcano
        nodeSelector:
          kubernetes.io/hostname: ${FULL_GPU_NODE}
        containers:
        - name: main
          image: busybox
          command: ["sleep", "3600"]
          resources:
            limits:
              nvidia.com/gpu: "1"
  - replicas: 2
    name: worker
    template:
      metadata:
        labels:
          role: worker-vgpu
      spec:
        schedulerName: volcano
        nodeSelector:
          kubernetes.io/hostname: ${VGPU_NODE}
        containers:
        - name: main
          image: busybox
          command: ["sleep", "3600"]
          resources:
            limits:
              volcano.sh/vgpu-number: "1"
              volcano.sh/vgpu-memory: "2000"
EOF
sleep 45

HEAD_POD="hetero-gang-head-0"
WORKER_0="hetero-gang-worker-0"
WORKER_1="hetero-gang-worker-1"

check "head pod is Running" "Running" "$(get_status $HEAD_POD)"
check "worker-0 is Running"  "Running" "$(get_status $WORKER_0)"
check "worker-1 is Running"  "Running" "$(get_status $WORKER_1)"

HEAD_NODE=$(kubectl get pod $HEAD_POD -o jsonpath='{.spec.nodeName}' 2>/dev/null)
W0_NODE=$(kubectl get pod $WORKER_0 -o jsonpath='{.spec.nodeName}' 2>/dev/null)
W1_NODE=$(kubectl get pod $WORKER_1 -o jsonpath='{.spec.nodeName}' 2>/dev/null)

echo "  head pod node:    $HEAD_NODE"
echo "  worker-0 node:    $W0_NODE"
echo "  worker-1 node:    $W1_NODE"

check "head pod landed on full-GPU node" "${FULL_GPU_NODE}" "${HEAD_NODE}"
check "worker-0 landed on vGPU node"     "${VGPU_NODE}"     "${W0_NODE}"
check "worker-1 landed on vGPU node"     "${VGPU_NODE}"     "${W1_NODE}"

# The Ghost-Reservation Bug signature: vGPU UsedMem appearing on a node that
# was never assigned a vGPU pod. Inspect scheduler logs for the
# "Devices ... assertion conversion failed" pattern that #5290 fixed —
# if we still see it on the full-GPU node, the fix is not effective.
SCHED_LOGS=$(kubectl logs -n volcano-system -l app=volcano-scheduler --tail=500 2>&1)
if echo "$SCHED_LOGS" | grep -q "assertion conversion failed"; then
  echo -e "  ${RED}FAIL${NC}: scheduler logs show \"assertion conversion failed\" (#5290 not effective)"
  FAIL=$((FAIL+1))
else
  echo -e "  ${GREEN}PASS${NC}: no \"assertion conversion failed\" in scheduler logs"
  PASS=$((PASS+1))
fi

######################################################################
echo ""
echo "======================================================"
echo "  TEST 2: Repeated submit/delete (stresses CloneOthers)"
echo "======================================================"
echo "  Submit + delete the heterogeneous gang 3 times in a row to"
echo "  exercise the alloc / release rollback path (which is where"
echo "  #5235's tentativeAlloc/rollback machinery and the DeepCopy"
echo "  guard live)."

for run in 1 2 3; do
  kubectl delete vcjob hetero-gang --force --grace-period=0 2>/dev/null || true
  # Wait for stale pods from the previous iteration to clear, otherwise the
  # new gang job can collide on pod names and stay Pending.
  for _ in $(seq 1 15); do
    if ! kubectl get pods --no-headers 2>/dev/null | grep -q "^hetero-gang-"; then
      break
    fi
    sleep 2
  done
  sleep 5
  cat <<EOF | kubectl apply -f - >/dev/null
apiVersion: batch.volcano.sh/v1alpha1
kind: Job
metadata:
  name: hetero-gang
spec:
  minAvailable: 3
  schedulerName: volcano
  tasks:
  - replicas: 1
    name: head
    template:
      spec:
        schedulerName: volcano
        nodeSelector:
          kubernetes.io/hostname: ${FULL_GPU_NODE}
        containers:
        - name: main
          image: busybox
          command: ["sleep", "3600"]
          resources:
            limits:
              nvidia.com/gpu: "1"
  - replicas: 2
    name: worker
    template:
      spec:
        schedulerName: volcano
        nodeSelector:
          kubernetes.io/hostname: ${VGPU_NODE}
        containers:
        - name: main
          image: busybox
          command: ["sleep", "3600"]
          resources:
            limits:
              volcano.sh/vgpu-number: "1"
              volcano.sh/vgpu-memory: "1000"
EOF
  # Poll up to 60s for all 3 pods to reach Running.
  for _ in $(seq 1 30); do
    RUN_OK=true
    for p in hetero-gang-head-0 hetero-gang-worker-0 hetero-gang-worker-1; do
      [ "$(get_status $p)" = "Running" ] || RUN_OK=false
    done
    $RUN_OK && break
    sleep 2
  done
  check "iteration ${run}: all 3 gang tasks Running" "true" "$RUN_OK"
  if [ "$RUN_OK" != "true" ]; then
    for p in hetero-gang-head-0 hetero-gang-worker-0 hetero-gang-worker-1; do
      echo "    $p: $(get_status $p) on $(kubectl get pod $p -o jsonpath='{.spec.nodeName}' 2>/dev/null)"
    done
  fi
done

kubectl delete vcjob hetero-gang --force --grace-period=0 2>/dev/null || true
cleanup
print_results

echo ""
echo "=== Scheduler logs (heterogeneous gang) ==="
kubectl logs -n volcano-system -l app=volcano-scheduler --tail=200 2>&1 | grep -iE "hetero-gang|assertion conversion|CloneOthers|gpuexclusive" | tail -30
