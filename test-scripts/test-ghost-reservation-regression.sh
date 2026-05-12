#!/bin/bash
# Ghost-Reservation Bug regression test
# =====================================
# Reproduces the conditions that caused heterogeneous Ray gangs to get stuck
# Pending for days on prod-lor1-k8s-2, then verifies v0.0.6 (with #5235's
# DeepCopy() in CloneOthers + #5290's typed-nil preservation) doesn't leak
# UsedMem across MZ ranking simulations.
#
# Bug summary:
#   - NodeInfo.CloneOthers did a SHALLOW copy of `Others map[string]interface{}`.
#   - LinkedIn collocate plugin's `simulateScheduling` (called from MZ ranking
#     at mz.go:95) cloned each candidate NodeInfo and ran AddTask on the clone,
#     which routes through addResource → vgpu.AddResource(pod) and mutates the
#     SHARED *vgpu.GPUDevices snapshot (UsedMem += pod.vgpu-memory).
#   - No SubResource to undo. Every ranking pass leaked vgpu-memory onto a GPU.
#   - After several passes, every GPU on every empty hami node had UsedMem
#     poisoned, predicate (Memory - UsedMem >= MemReq) failed forever.
#
# Trigger conditions (all required):
#   1. PodGroup.minAvailable >= 2
#   2. At least one pod requests volcano.sh/vgpu-number
#   3. Nodes labeled with node.linkedin.com/maintenance-zone
#   4. colocation plugin configured with mz: true
#
# This test sets all four up exactly, runs many gang cycles, then submits a
# probe pod that requests almost a full GPU's worth of memory. Pre-fix, the
# leak would have poisoned the GPUs and the probe would stay Pending.
# Post-fix, the probe schedules cleanly.

set -e

source "$(dirname "$0")/_lib.sh"

FULL_GPU_NODE=""
VGPU_NODE=""

echo "============================================================"
echo "  GHOST-RESERVATION BUG REGRESSION TEST"
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
echo "=== PHASE 1: Apply prod-lor1-k8s-2 scheduler config (colocation mz=true) ==="
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
echo "=== PHASE 1: label nodes with maintenance-zone (colocation MZ trigger) ==="
setup_mz_labels
kubectl get nodes -L node.linkedin.com/maintenance-zone --no-headers

echo ""
echo "=== PHASE 1: pick a full-GPU node and a vGPU node ==="
ALL_NODES=($(kubectl get nodes -o jsonpath='{.items[*].metadata.name}'))
FULL_GPU_NODE="${ALL_NODES[0]}"
VGPU_NODE="${ALL_NODES[1]:-${ALL_NODES[0]}}"
echo "  full GPU node: ${FULL_GPU_NODE} (MZ=$(kubectl get node ${FULL_GPU_NODE} -o jsonpath='{.metadata.labels.node\.linkedin\.com/maintenance-zone}'))"
echo "  vGPU node:     ${VGPU_NODE} (MZ=$(kubectl get node ${VGPU_NODE} -o jsonpath='{.metadata.labels.node\.linkedin\.com/maintenance-zone}'))"

if [ "$FULL_GPU_NODE" = "$VGPU_NODE" ]; then
  echo -e "  ${RED}ABORT${NC}: cluster has only one node — ghost-reservation test needs at least two"
  exit 2
fi

echo ""
echo "=== PHASE 1: configure ${FULL_GPU_NODE} with nvidia.com/gpu=4 ==="
setup_full_gpu_node "${FULL_GPU_NODE}" 4

echo ""
echo "=== PHASE 1: configure ${VGPU_NODE} with vGPU annotations (8 GPUs × 16 GB) ==="
NODE="${VGPU_NODE}"
kubectl proxy --port=8203 &
PROXY_PID=$!
sleep 2
curl -s -X PATCH \
  -H "Content-Type: application/json-patch+json" \
  --data '[
    {"op": "add", "path": "/status/capacity/volcano.sh~1vgpu-number", "value": "16"},
    {"op": "add", "path": "/status/capacity/volcano.sh~1vgpu-memory", "value": "131072"},
    {"op": "add", "path": "/status/capacity/volcano.sh~1vgpu-cores", "value": "1600"}
  ]' \
  http://localhost:8203/api/v1/nodes/${VGPU_NODE}/status > /dev/null
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

GANG_ROUNDS=5
PROBE_VGPU_MEM=14336   # 14 GB out of 16 GB — fits only if GPU is clean
WORKER_VGPU_MEM=4096   # 4 GB per worker — leaks 8 GB per session pre-fix

heterogeneous_gang_yaml() {
  cat <<EOF
apiVersion: batch.volcano.sh/v1alpha1
kind: Job
metadata:
  name: $1
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
              volcano.sh/vgpu-memory: "${WORKER_VGPU_MEM}"
EOF
}

######################################################################
echo ""
echo "======================================================"
echo "  TEST 1: Baseline — 14 GB probe schedules on clean GPUs"
echo "======================================================"
cleanup

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: baseline-probe
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
        volcano.sh/vgpu-memory: "${PROBE_VGPU_MEM}"
EOF
sleep 25
check "baseline 14 GB probe Running on clean GPU" "Running" "$(get_status baseline-probe)"
echo "  baseline GPU: $(get_gpus baseline-probe)"
kubectl delete pod baseline-probe --force --grace-period=0 2>/dev/null
sleep 5

######################################################################
echo ""
echo "======================================================"
echo "  TEST 2: Run ${GANG_ROUNDS} heterogeneous gang cycles"
echo "  (each cycle triggers BatchNodeOrder → MZ simulation"
echo "   → AddTask on cloned NodeInfo. Pre-fix this would leak"
echo "   ${WORKER_VGPU_MEM} MB × 2 workers = $((WORKER_VGPU_MEM * 2)) MB per"
echo "   cycle onto a GPU's UsedMem.)"
echo "======================================================"

for round in $(seq 1 $GANG_ROUNDS); do
  echo ""
  echo "  --- gang round ${round}/${GANG_ROUNDS} ---"
  heterogeneous_gang_yaml "ghost-gang-r${round}" | kubectl apply -f -

  # poll up to 60s for all 3 pods Running
  PODS=("ghost-gang-r${round}-head-0" "ghost-gang-r${round}-worker-0" "ghost-gang-r${round}-worker-1")
  RUN_OK=false
  for _ in $(seq 1 30); do
    ALL_RUNNING=true
    for p in "${PODS[@]}"; do
      [ "$(get_status $p)" = "Running" ] || ALL_RUNNING=false
    done
    if $ALL_RUNNING; then RUN_OK=true; break; fi
    sleep 2
  done
  check "round ${round}: all 3 gang tasks Running" "true" "$RUN_OK"
  for p in "${PODS[@]}"; do
    [ "$(get_status $p)" = "Running" ] || echo "    $p: $(get_status $p)"
  done

  # Clean up to free capacity for the next round
  kubectl delete vcjob "ghost-gang-r${round}" --force --grace-period=0 2>/dev/null || true
  for _ in $(seq 1 15); do
    kubectl get pods --no-headers 2>/dev/null | grep -q "^ghost-gang-r${round}-" || break
    sleep 2
  done
done

######################################################################
echo ""
echo "======================================================"
echo "  TEST 3: Post-cycles probe — 14 GB probe must STILL fit"
echo "======================================================"
echo "  Pre-fix: after ${GANG_ROUNDS} MZ simulation passes, UsedMem on every"
echo "  GPU on ${VGPU_NODE} would have been inflated by leaked AddResource"
echo "  calls, and a 14 GB probe could not find a GPU with enough memory."
echo "  Post-fix: deep CloneOthers isolates the simulation. UsedMem on the"
echo "  real *GPUDevices stays accurate, probe still schedules."
cleanup

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: postcycle-probe
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
        volcano.sh/vgpu-memory: "${PROBE_VGPU_MEM}"
EOF
sleep 30
PROBE_STATUS=$(get_status postcycle-probe)
check "post-cycle 14 GB probe Running (no ghost reservation)" "Running" "$PROBE_STATUS"
if [ "$PROBE_STATUS" = "Running" ]; then
  echo "  post-cycle GPU: $(get_gpus postcycle-probe)"
else
  echo "  ghost reservation symptom — probe Pending. Scheduler events:"
  kubectl describe pod postcycle-probe | grep -A1 "Events:" | tail -5
  echo "  scheduler log tail:"
  kubectl logs -n volcano-system -l app=volcano-scheduler --tail=80 2>&1 | grep -iE "vgpu|UsedMem|not enough|deviceshare" | tail -15
fi

######################################################################
echo ""
echo "======================================================"
echo "  TEST 4: Sanity — clone-mutation isolation evidence in logs"
echo "======================================================"
SCHED_LOGS=$(kubectl logs -n volcano-system -l app=volcano-scheduler --tail=500 2>&1)
if echo "$SCHED_LOGS" | grep -q "assertion conversion failed"; then
  echo -e "  ${RED}FAIL${NC}: \"assertion conversion failed\" appears — #5290 not effective"
  FAIL=$((FAIL+1))
else
  echo -e "  ${GREEN}PASS${NC}: no \"assertion conversion failed\" warnings"
  PASS=$((PASS+1))
fi

if echo "$SCHED_LOGS" | grep -qE "Found .* MZs in the cluster"; then
  echo -e "  ${GREEN}PASS${NC}: colocation MZ plugin loaded and discovered MZs"
  PASS=$((PASS+1))
else
  echo -e "  ${RED}FAIL${NC}: colocation MZ plugin did not announce MZ discovery — trigger not exercised"
  FAIL=$((FAIL+1))
fi

cleanup
print_results
