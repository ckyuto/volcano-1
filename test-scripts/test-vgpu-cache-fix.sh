#!/bin/bash
set -e

source "$(dirname "$0")/_lib.sh"

echo "=============================================="
echo "  vGPU Cache Double-Count Fix — E2E Tests"
echo "=============================================="

echo ""
echo "=== SETUP: scheduler config (prod-like, see scheduler-config.prod-like.yaml) ==="
kubectl apply -f "$(dirname "$0")/scheduler-config.prod-like.yaml"

echo ""
echo "=== SETUP: restart scheduler ==="
restart_scheduler

echo ""
echo "=== SETUP: fake 8 GPUs × 2 sharing slots = 16 ==="
setup_fake_node_gpus

# Abort the suite up front if the deviceshare Allocate path isn't engaged.
# Without this guard, all the assertions below pass vacuously off k8s extended-
# resource accounting alone, and the cache double-count / preemption code paths
# are never actually exercised — exactly the gap that let v0.0.5-volcano1.14.0
# ship with a panic-on-pod-removal regression.
require_deviceshare_allocate_engaged

######################################################################
echo ""
echo "=========================================================================="
echo "  TEST 1: Binpack consolidation (the cache double-count signature)"
echo "  5 single-GPU pods on a node with 8 GPUs × 2 sharing slots each."
echo "  WITH BUG: each pod sees prior GPU as full → spreads across 5 GPUs."
echo "  WITH FIX: binpack consolidates → uses ⌈5/2⌉ = 3 GPUs."
echo "=========================================================================="
cleanup

for i in 1 2 3 4 5; do
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: bp-w$i
spec:
  schedulerName: volcano
  containers:
  - name: w
    image: busybox
    command: ["sleep", "3600"]
    resources:
      limits:
        volcano.sh/vgpu-number: "1"
        volcano.sh/vgpu-memory: "4096"
        volcano.sh/vgpu-cores: "25"
EOF
done
sleep 30

echo "--- pod statuses ---"
ALL_RUNNING=true
for i in 1 2 3 4 5; do
  st=$(get_status bp-w$i)
  echo "  bp-w$i: $st  GPU=$(get_gpus bp-w$i)"
  [ "$st" = "Running" ] || ALL_RUNNING=false
done
check "all 5 binpack pods Running" "true" "$ALL_RUNNING"

ALL_GPUS=""
for i in 1 2 3 4 5; do ALL_GPUS="$ALL_GPUS $(get_gpus bp-w$i)"; done
UNIQUE=$(count_unique_gpus "$ALL_GPUS")
echo "  unique GPUs used: $UNIQUE"
if [ "$UNIQUE" -le 3 ]; then
  echo -e "  ${GREEN}PASS${NC}: binpack consolidates to ≤3 GPUs (got=$UNIQUE)"
  PASS=$((PASS+1))
else
  echo -e "  ${RED}FAIL${NC}: binpack should consolidate to ≤3 GPUs, got=$UNIQUE — likely cache double-count"
  FAIL=$((FAIL+1))
fi

######################################################################
echo ""
echo "=========================================================================="
echo "  TEST 2: Sub-then-Add — same pod removed and re-scheduled"
echo "  Validates that SubResource cleans up PodMap so re-Add re-increments."
echo "=========================================================================="
cleanup

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: sub-add-1
spec:
  schedulerName: volcano
  containers:
  - name: w
    image: busybox
    command: ["sleep", "3600"]
    resources:
      limits:
        volcano.sh/vgpu-number: "1"
        volcano.sh/vgpu-memory: "4096"
        volcano.sh/vgpu-cores: "25"
EOF
sleep 20
check "sub-add-1 is Running" "Running" "$(get_status sub-add-1)"
echo "  sub-add-1 GPU: $(get_gpus sub-add-1)"

kubectl delete pod sub-add-1 --force --grace-period=0 2>/dev/null
sleep 10

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: sub-add-2
spec:
  schedulerName: volcano
  containers:
  - name: w
    image: busybox
    command: ["sleep", "3600"]
    resources:
      limits:
        volcano.sh/vgpu-number: "1"
        volcano.sh/vgpu-memory: "4096"
        volcano.sh/vgpu-cores: "25"
EOF
sleep 20
check "sub-add-2 is Running" "Running" "$(get_status sub-add-2)"
echo "  sub-add-2 GPU: $(get_gpus sub-add-2)"

######################################################################
echo ""
echo "=========================================================================="
echo "  TEST 3: Burst submission — 16 single-GPU pods saturate 16 slots"
echo "  WITH BUG: cache double-count would falsely reject some pods."
echo "  WITH FIX: all 16 pods schedule successfully (binpack + double-share)."
echo "=========================================================================="
cleanup

for i in $(seq 1 16); do
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: burst-w$i
spec:
  schedulerName: volcano
  containers:
  - name: w
    image: busybox
    command: ["sleep", "3600"]
    resources:
      limits:
        volcano.sh/vgpu-number: "1"
        volcano.sh/vgpu-memory: "4096"
        volcano.sh/vgpu-cores: "25"
EOF
done
sleep 60

RUNNING=0
for i in $(seq 1 16); do
  st=$(get_status burst-w$i)
  [ "$st" = "Running" ] && RUNNING=$((RUNNING+1))
done
echo "  $RUNNING/16 pods Running"
check "all 16 pods Running (16 slots, no double-count rejection)" "16" "$RUNNING"

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: burst-w17
spec:
  schedulerName: volcano
  containers:
  - name: w
    image: busybox
    command: ["sleep", "3600"]
    resources:
      limits:
        volcano.sh/vgpu-number: "1"
        volcano.sh/vgpu-memory: "4096"
        volcano.sh/vgpu-cores: "25"
EOF
sleep 15
check "burst-w17 is Pending (over capacity)" "Pending" "$(get_status burst-w17)"

cleanup
print_results
