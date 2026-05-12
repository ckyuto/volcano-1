#!/bin/bash
set -e

source "$(dirname "$0")/_lib.sh"

echo "=============================================="
echo "  vGPU PodGroup Device Spread E2E Tests"
echo "=============================================="

echo ""
echo "=== SETUP: scheduler config (deviceshare, no exclusivity rules) ==="
cat <<'PATCH' | kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: volcano-scheduler-configmap
  namespace: volcano-system
data:
  volcano-scheduler.conf: |
    actions: "enqueue, allocate, backfill, reclaim, preempt"
    tiers:
    - plugins:
      - name: priority
      - name: gang
        enablePreemptable: false
      - name: conformance
      - name: sla
    - plugins:
      - name: overcommit
      - name: drf
        enablePreemptable: false
      - name: predicates
      - name: proportion
      - name: nodeorder
      - name: binpack
      - name: deviceshare
        arguments:
          deviceshare.VGPUEnable: true
PATCH

echo ""
echo "=== SETUP: restart scheduler ==="
restart_scheduler

echo ""
echo "=== SETUP: fake 8 GPUs × 2 sharing slots = 16 ==="
setup_fake_node_gpus

# Abort if the deviceshare Allocate path isn't engaged — the spread feature
# lives in that exact path, so without it the spread assertions would all
# pass vacuously.
require_deviceshare_allocate_engaged

echo ""
echo "=== SETUP: verify scheduler started ==="
ERRORS=$(kubectl logs -n volcano-system -l app=volcano-scheduler --tail=30 2>&1 | grep "Failed to get plugin" || true)
if [ -z "$ERRORS" ]; then
  echo -e "  ${GREEN}Scheduler loaded successfully${NC}"
else
  echo -e "  ${RED}Scheduler errors: $ERRORS${NC}"
fi

######################################################################
echo ""
echo "========================================================================="
echo "  TEST 1: Multi-worker VCJob with spread — 4 workers in same PodGroup"
echo "           each requesting 1 GPU, all with spread annotation."
echo "           Expect: each worker on a DIFFERENT GPU device."
echo "========================================================================="
cleanup

cat <<'EOF' | kubectl apply -f -
apiVersion: scheduling.volcano.sh/v1beta1
kind: PodGroup
metadata:
  name: spread-job-pg
spec:
  minMember: 4
  queue: default
---
apiVersion: v1
kind: Pod
metadata:
  name: spread-w0
  annotations:
    scheduling.k8s.io/group-name: "spread-job-pg"
    volcano.sh/vgpu-podgroup-policy: "spread"
spec:
  schedulerName: volcano
  containers:
  - name: worker
    image: busybox
    command: ["sleep", "3600"]
    resources:
      limits:
        volcano.sh/vgpu-number: "1"
        volcano.sh/vgpu-memory: "4096"
        volcano.sh/vgpu-cores: "25"
---
apiVersion: v1
kind: Pod
metadata:
  name: spread-w1
  annotations:
    scheduling.k8s.io/group-name: "spread-job-pg"
    volcano.sh/vgpu-podgroup-policy: "spread"
spec:
  schedulerName: volcano
  containers:
  - name: worker
    image: busybox
    command: ["sleep", "3600"]
    resources:
      limits:
        volcano.sh/vgpu-number: "1"
        volcano.sh/vgpu-memory: "4096"
        volcano.sh/vgpu-cores: "25"
---
apiVersion: v1
kind: Pod
metadata:
  name: spread-w2
  annotations:
    scheduling.k8s.io/group-name: "spread-job-pg"
    volcano.sh/vgpu-podgroup-policy: "spread"
spec:
  schedulerName: volcano
  containers:
  - name: worker
    image: busybox
    command: ["sleep", "3600"]
    resources:
      limits:
        volcano.sh/vgpu-number: "1"
        volcano.sh/vgpu-memory: "4096"
        volcano.sh/vgpu-cores: "25"
---
apiVersion: v1
kind: Pod
metadata:
  name: spread-w3
  annotations:
    scheduling.k8s.io/group-name: "spread-job-pg"
    volcano.sh/vgpu-podgroup-policy: "spread"
spec:
  schedulerName: volcano
  containers:
  - name: worker
    image: busybox
    command: ["sleep", "3600"]
    resources:
      limits:
        volcano.sh/vgpu-number: "1"
        volcano.sh/vgpu-memory: "4096"
        volcano.sh/vgpu-cores: "25"
EOF
sleep 30

echo "  --- Pod statuses ---"
for i in 0 1 2 3; do
  check "spread-w$i is Running" "Running" "$(get_status spread-w$i)"
done

echo "  --- GPU assignments ---"
for i in 0 1 2 3; do
  echo "  spread-w$i GPU: $(get_gpus spread-w$i)"
done

echo "  --- Cross-check all pairs for no overlap ---"
for i in 0 1 2; do
  for j in $(seq $((i+1)) 3); do
    check_no_overlap "spread-w$i vs spread-w$j" "$(get_gpus spread-w$i)" "$(get_gpus spread-w$j)"
  done
done

UNIQUE_COUNT=$(for i in 0 1 2 3; do get_gpus spread-w$i | tr ':' '\n' | grep GPU | cut -d, -f1; done | sort -u | wc -l)
check "4 workers on 4 distinct GPUs" "4" "$UNIQUE_COUNT"

######################################################################
echo ""
echo "========================================================================="
echo "  TEST 2: Mixed scenario — two PodGroups, spread + binpack coexistence"
echo "           PodGroup A: 3 workers with spread (must get different GPUs)"
echo "           PodGroup B: 3 workers WITHOUT spread (can share GPUs)"
echo "           Plus: PodGroup C with spread on a DIFFERENT group"
echo "                 (can reuse GPUs from PodGroup A)"
echo "========================================================================="
cleanup

cat <<'EOF' | kubectl apply -f -
apiVersion: scheduling.volcano.sh/v1beta1
kind: PodGroup
metadata:
  name: job-spread-a
spec:
  minMember: 3
  queue: default
---
apiVersion: scheduling.volcano.sh/v1beta1
kind: PodGroup
metadata:
  name: job-binpack-b
spec:
  minMember: 3
  queue: default
---
apiVersion: scheduling.volcano.sh/v1beta1
kind: PodGroup
metadata:
  name: job-spread-c
spec:
  minMember: 2
  queue: default
---
# === PodGroup A: 3 workers, spread ===
apiVersion: v1
kind: Pod
metadata:
  name: a-w0
  annotations:
    scheduling.k8s.io/group-name: "job-spread-a"
    volcano.sh/vgpu-podgroup-policy: "spread"
spec:
  schedulerName: volcano
  containers:
  - name: worker
    image: busybox
    command: ["sleep", "3600"]
    resources:
      limits:
        volcano.sh/vgpu-number: "1"
        volcano.sh/vgpu-memory: "4096"
        volcano.sh/vgpu-cores: "25"
---
apiVersion: v1
kind: Pod
metadata:
  name: a-w1
  annotations:
    scheduling.k8s.io/group-name: "job-spread-a"
    volcano.sh/vgpu-podgroup-policy: "spread"
spec:
  schedulerName: volcano
  containers:
  - name: worker
    image: busybox
    command: ["sleep", "3600"]
    resources:
      limits:
        volcano.sh/vgpu-number: "1"
        volcano.sh/vgpu-memory: "4096"
        volcano.sh/vgpu-cores: "25"
---
apiVersion: v1
kind: Pod
metadata:
  name: a-w2
  annotations:
    scheduling.k8s.io/group-name: "job-spread-a"
    volcano.sh/vgpu-podgroup-policy: "spread"
spec:
  schedulerName: volcano
  containers:
  - name: worker
    image: busybox
    command: ["sleep", "3600"]
    resources:
      limits:
        volcano.sh/vgpu-number: "1"
        volcano.sh/vgpu-memory: "4096"
        volcano.sh/vgpu-cores: "25"
---
# === PodGroup B: 3 workers, NO spread (binpack) ===
apiVersion: v1
kind: Pod
metadata:
  name: b-w0
  annotations:
    scheduling.k8s.io/group-name: "job-binpack-b"
spec:
  schedulerName: volcano
  containers:
  - name: worker
    image: busybox
    command: ["sleep", "3600"]
    resources:
      limits:
        volcano.sh/vgpu-number: "1"
        volcano.sh/vgpu-memory: "4096"
        volcano.sh/vgpu-cores: "25"
---
apiVersion: v1
kind: Pod
metadata:
  name: b-w1
  annotations:
    scheduling.k8s.io/group-name: "job-binpack-b"
spec:
  schedulerName: volcano
  containers:
  - name: worker
    image: busybox
    command: ["sleep", "3600"]
    resources:
      limits:
        volcano.sh/vgpu-number: "1"
        volcano.sh/vgpu-memory: "4096"
        volcano.sh/vgpu-cores: "25"
---
apiVersion: v1
kind: Pod
metadata:
  name: b-w2
  annotations:
    scheduling.k8s.io/group-name: "job-binpack-b"
spec:
  schedulerName: volcano
  containers:
  - name: worker
    image: busybox
    command: ["sleep", "3600"]
    resources:
      limits:
        volcano.sh/vgpu-number: "1"
        volcano.sh/vgpu-memory: "4096"
        volcano.sh/vgpu-cores: "25"
---
# === PodGroup C: 2 workers with spread, DIFFERENT group ===
apiVersion: v1
kind: Pod
metadata:
  name: c-w0
  annotations:
    scheduling.k8s.io/group-name: "job-spread-c"
    volcano.sh/vgpu-podgroup-policy: "spread"
spec:
  schedulerName: volcano
  containers:
  - name: worker
    image: busybox
    command: ["sleep", "3600"]
    resources:
      limits:
        volcano.sh/vgpu-number: "1"
        volcano.sh/vgpu-memory: "4096"
        volcano.sh/vgpu-cores: "25"
---
apiVersion: v1
kind: Pod
metadata:
  name: c-w1
  annotations:
    scheduling.k8s.io/group-name: "job-spread-c"
    volcano.sh/vgpu-podgroup-policy: "spread"
spec:
  schedulerName: volcano
  containers:
  - name: worker
    image: busybox
    command: ["sleep", "3600"]
    resources:
      limits:
        volcano.sh/vgpu-number: "1"
        volcano.sh/vgpu-memory: "4096"
        volcano.sh/vgpu-cores: "25"
EOF
sleep 40

echo "  --- All pods should be Running ---"
for pod in a-w0 a-w1 a-w2 b-w0 b-w1 b-w2 c-w0 c-w1; do
  check "$pod is Running" "Running" "$(get_status $pod)"
done

echo ""
echo "  --- GPU assignments ---"
for pod in a-w0 a-w1 a-w2 b-w0 b-w1 b-w2 c-w0 c-w1; do
  echo "  $pod GPU: $(get_gpus $pod)"
done

echo ""
echo "  --- TEST 2a: PodGroup A (spread) — all workers must have DIFFERENT GPUs ---"
for i in 0 1; do
  for j in $(seq $((i+1)) 2); do
    check_no_overlap "a-w$i vs a-w$j" "$(get_gpus a-w$i)" "$(get_gpus a-w$j)"
  done
done
A_UNIQUE=$(for i in 0 1 2; do get_gpus a-w$i | tr ':' '\n' | grep GPU | cut -d, -f1; done | sort -u | wc -l)
check "Group A: 3 workers on 3 distinct GPUs" "3" "$A_UNIQUE"

echo ""
echo "  --- TEST 2b: PodGroup B (binpack) — workers CAN share GPUs ---"
B_UNIQUE=$(for i in 0 1 2; do get_gpus b-w$i | tr ':' '\n' | grep GPU | cut -d, -f1; done | sort -u | wc -l)
echo "  Group B: $B_UNIQUE unique GPU(s) used across 3 workers"
check "Group B: at least 2 GPUs used (max 2 sharing per GPU)" "true" "$([ $B_UNIQUE -ge 2 ] && echo true || echo false)"

echo ""
echo "  --- TEST 2c: PodGroup C (spread, different group) — can reuse A's GPUs ---"
check_no_overlap "c-w0 vs c-w1 (within group C)" "$(get_gpus c-w0)" "$(get_gpus c-w1)"
C_UNIQUE=$(for i in 0 1; do get_gpus c-w$i | tr ':' '\n' | grep GPU | cut -d, -f1; done | sort -u | wc -l)
check "Group C: 2 workers on 2 distinct GPUs" "2" "$C_UNIQUE"
echo "  (Cross-group GPU sharing is allowed — spread only applies within a PodGroup)"

echo ""
echo "  --- TEST 2d: Verify total GPU usage fits in 8 GPUs with 2 slots each ---"
ALL_RUNNING=true
for pod in a-w0 a-w1 a-w2 b-w0 b-w1 b-w2 c-w0 c-w1; do
  if [ "$(get_status $pod)" != "Running" ]; then
    ALL_RUNNING=false
  fi
done
check "All 8 pods running within 16 GPU slots" "true" "$ALL_RUNNING"

cleanup
print_results
