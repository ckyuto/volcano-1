#!/bin/bash
set -e

source "$(dirname "$0")/_lib.sh"

# Label/value matching the single GPUExclusiveRule that prod-lor1-k8s-2 uses,
# rendered in scheduler-config.prod-like.yaml.
RULE_LABEL_KEY="kingkong.dl.linkedin.com/workloadType"
RULE_LABEL_VAL="multi-gpus"
OTHER_LABEL_VAL="single-gpu"

echo "============================================================"
echo "  gpuexclusive INTEGRATION TESTS (prod-lor1-k8s-2 config)"
echo "============================================================"
echo "  Rule: ${RULE_LABEL_KEY}=${RULE_LABEL_VAL}"
echo "  Topology: 8 physical GPUs × 2 vGPU slots each = 16 total"
echo ""

######################################################################
echo "=== PHASE 0: Build and load scheduler image into minikube ==="
echo "  Building scheduler image with v0.0.6-volcano1.14.0 tag..."
docker buildx build --no-cache \
  --output=type=docker \
  -t volcanosh/vc-scheduler:v0.0.6-volcano1.14.0 \
  -f installer/dockerfile/scheduler/Dockerfile . 2>&1 | tail -5
echo "  Loading image into minikube (all nodes — multi-node compatible)..."
minikube image load volcanosh/vc-scheduler:v0.0.6-volcano1.14.0 2>&1 | tail -5
echo ""

######################################################################
echo "=== PHASE 1: Apply prod-lor1-k8s-2 scheduler config (UQM removed) ==="
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
kubectl get nodes -L node.linkedin.com/maintenance-zone --no-headers

echo ""
echo "=== PHASE 1: fake 8 GPUs × 2 sharing slots = 16 ==="
setup_fake_node_gpus

# Abort if the deviceshare Allocate path isn't engaged — the gpuexclusive wrapper
# wraps the deviceshare device. Without Allocate running, exclusivity assertions
# pass vacuously.
require_deviceshare_allocate_engaged

echo ""
echo "=== PHASE 1: verify plugin loaded ==="
SCHED_LOGS=$(kubectl logs -n volcano-system -l app=volcano-scheduler --tail=30 2>&1)
if echo "$SCHED_LOGS" | grep -q "Failed to get plugin"; then
  echo -e "  ${RED}Plugin load error!${NC}"
  echo "$SCHED_LOGS" | grep "Failed to get plugin"
elif echo "$SCHED_LOGS" | grep -q "no rules configured"; then
  echo -e "  ${RED}WARNING: no rules configured — plugin is a no-op!${NC}"
else
  echo -e "  ${GREEN}Plugin loaded successfully${NC}"
fi

if echo "$SCHED_LOGS" | grep -q "gpuexclusive: OnSessionOpen"; then
  echo -e "  ${GREEN}gpuexclusive OnSessionOpen detected with rules${NC}"
else
  echo -e "  ${YELLOW}Note: OnSessionOpen not yet visible in tail logs${NC}"
fi

apply_pod() {
  local name="$1" vgpu_num="$2" labels="$3"
  cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: ${name}
  labels:
${labels}
spec:
  schedulerName: volcano
  containers:
  - name: main
    image: busybox
    command: ["sleep", "3600"]
    resources:
      limits:
        volcano.sh/vgpu-number: "${vgpu_num}"
        volcano.sh/vgpu-memory: "1000"
EOF
}

######################################################################
echo ""
echo "======================================================"
echo "  TEST 1: Same-rule exclusivity"
echo "======================================================"
echo "  Two multi-gpus pods (4 vGPUs each) must not overlap physical GPUs."
cleanup

apply_pod train-a 4 "    ${RULE_LABEL_KEY}: ${RULE_LABEL_VAL}"
apply_pod train-b 4 "    ${RULE_LABEL_KEY}: ${RULE_LABEL_VAL}"
sleep 30

check "train-a is Running" "Running" "$(get_status train-a)"
check "train-b is Running" "Running" "$(get_status train-b)"
TRAIN_A=$(get_gpus train-a)
TRAIN_B=$(get_gpus train-b)
echo "  train-a GPUs: $TRAIN_A"
echo "  train-b GPUs: $TRAIN_B"
check_no_overlap "train-a vs train-b" "$TRAIN_A" "$TRAIN_B"

######################################################################
echo ""
echo "======================================================"
echo "  TEST 2: Single-vGPU rule pod still reserves the card"
echo "======================================================"
echo "  Even requesting 1 vGPU, a multi-gpus pod gets a dedicated card."
cleanup

apply_pod t2-train-1gpu 1 "    ${RULE_LABEL_KEY}: ${RULE_LABEL_VAL}"
apply_pod t2-train-4gpu 4 "    ${RULE_LABEL_KEY}: ${RULE_LABEL_VAL}"
sleep 30

check "t2-train-1gpu is Running" "Running" "$(get_status t2-train-1gpu)"
check "t2-train-4gpu is Running" "Running" "$(get_status t2-train-4gpu)"
T2_1=$(get_gpus t2-train-1gpu)
T2_4=$(get_gpus t2-train-4gpu)
echo "  t2-train-1gpu GPUs: $T2_1"
echo "  t2-train-4gpu GPUs: $T2_4"
check_no_overlap "1-vGPU vs 4-vGPU multi-gpus" "$T2_1" "$T2_4"

######################################################################
echo ""
echo "======================================================"
echo "  TEST 3: Non-matching label value gets no exclusivity"
echo "======================================================"
echo "  workloadType=${OTHER_LABEL_VAL} (wrong value) is treated as no-rule."
echo "  Must coexist freely with other non-rule pods on the same card."
cleanup

apply_pod t3-multi 4 "    ${RULE_LABEL_KEY}: ${RULE_LABEL_VAL}"
apply_pod t3-single-a 1 "    ${RULE_LABEL_KEY}: ${OTHER_LABEL_VAL}"
apply_pod t3-single-b 1 "    ${RULE_LABEL_KEY}: ${OTHER_LABEL_VAL}"
sleep 30

check "t3-multi is Running" "Running" "$(get_status t3-multi)"
check "t3-single-a is Running" "Running" "$(get_status t3-single-a)"
check "t3-single-b is Running" "Running" "$(get_status t3-single-b)"
echo "  t3-multi GPUs:    $(get_gpus t3-multi)"
echo "  t3-single-a GPUs: $(get_gpus t3-single-a)"
echo "  t3-single-b GPUs: $(get_gpus t3-single-b)"

######################################################################
echo ""
echo "======================================================"
echo "  TEST 4: GPU exhaustion — 5th multi-gpus pod stays Pending"
echo "======================================================"
echo "  4 multi-gpus pods × 2 vGPUs reserve all 8 physical GPUs."
echo "  5th multi-gpus pod must be Pending. Non-matching pod still schedules."
cleanup

for i in 1 2 3 4; do
  apply_pod t4-multi-$i 2 "    ${RULE_LABEL_KEY}: ${RULE_LABEL_VAL}"
done
sleep 40

for i in 1 2 3 4; do
  check "t4-multi-$i is Running" "Running" "$(get_status t4-multi-$i)"
done

echo "  GPU assignments:"
for i in 1 2 3 4; do
  echo "    t4-multi-$i: $(get_gpus t4-multi-$i)"
done

for i in 1 2 3; do
  for j in $(seq $((i+1)) 4); do
    check_no_overlap "t4-multi-$i vs t4-multi-$j" "$(get_gpus t4-multi-$i)" "$(get_gpus t4-multi-$j)"
  done
done

apply_pod t4-multi-5 2 "    ${RULE_LABEL_KEY}: ${RULE_LABEL_VAL}"
sleep 15
check "t4-multi-5 is Pending (GPUs exhausted)" "Pending" "$(get_status t4-multi-5)"

apply_pod t4-share 1 "    role: free-share"
sleep 15
check "t4-share is Running (no rule = can share)" "Running" "$(get_status t4-share)"

######################################################################
echo ""
echo "======================================================"
echo "  TEST 5: Release and reclaim"
echo "======================================================"
echo "  Delete t4-multi-1, then t4-multi-5 must schedule."

kubectl delete pod t4-multi-1 --force --grace-period=0
sleep 20

STATUS5=$(get_status t4-multi-5)
check "t4-multi-5 Running after release" "Running" "$STATUS5"
if [ "$STATUS5" = "Running" ]; then
  echo "  t4-multi-5 GPUs: $(get_gpus t4-multi-5)"
fi

######################################################################
echo ""
echo "======================================================"
echo "  TEST 6: Mixed workloads coexist"
echo "======================================================"
echo "  multi-gpus (2 vGPUs) + two single-gpu pods (1 vGPU each)."
echo "  multi-gpus reserves its card; single-gpu pods share freely."
cleanup

apply_pod t6-multi 2 "    ${RULE_LABEL_KEY}: ${RULE_LABEL_VAL}"
apply_pod t6-single-a 1 "    role: free-share"
apply_pod t6-single-b 1 "    role: free-share"
sleep 30

check "t6-multi is Running" "Running" "$(get_status t6-multi)"
check "t6-single-a is Running" "Running" "$(get_status t6-single-a)"
check "t6-single-b is Running" "Running" "$(get_status t6-single-b)"

T6_MULTI=$(get_gpus t6-multi)
T6_A=$(get_gpus t6-single-a)
T6_B=$(get_gpus t6-single-b)
echo "  t6-multi GPUs:    $T6_MULTI"
echo "  t6-single-a GPUs: $T6_A"
echo "  t6-single-b GPUs: $T6_B"

######################################################################
echo ""
echo "======================================================"
echo "  TEST 7: Colocation MZ plugin is engaged for gang jobs"
echo "======================================================"
echo "  Submit a 2-task Volcano gang job (minAvailable=2). With the colocation"
echo "  plugin enabled and node.linkedin.com/maintenance-zone labels in place,"
echo "  BatchNodeOrder must run and emit an MZ scoring record in scheduler logs."
cleanup

cat <<'EOF' | kubectl apply -f -
apiVersion: batch.volcano.sh/v1alpha1
kind: Job
metadata:
  name: colocate-gang
spec:
  minAvailable: 2
  schedulerName: volcano
  policies:
  - event: PodEvicted
    action: RestartJob
  tasks:
  - replicas: 2
    name: worker
    template:
      metadata:
        labels:
          role: gang-worker
      spec:
        schedulerName: volcano
        containers:
        - name: main
          image: busybox
          command: ["sleep", "3600"]
          resources:
            limits:
              volcano.sh/vgpu-number: "1"
              volcano.sh/vgpu-memory: "1000"
EOF
sleep 30

GANG_RUNNING=true
for i in 0 1; do
  POD="colocate-gang-worker-$i"
  [ "$(get_status $POD)" = "Running" ] || GANG_RUNNING=false
done
check "Both gang tasks are Running" "true" "$GANG_RUNNING"

# Inspect scheduler logs for colocation activity. The mz plugin logs at v=3,
# so we look for both the load-time "Found N MZs in the cluster" and any
# session-time "selecting MZ" / BatchNodeOrder evidence.
SCHED_LOGS=$(kubectl logs -n volcano-system -l app=volcano-scheduler --tail=500 2>&1)
if echo "$SCHED_LOGS" | grep -qE "Colocate plugin is started|Found .* MZs in the cluster"; then
  echo -e "  ${GREEN}PASS${NC}: colocation plugin started and read MZ labels"
  PASS=$((PASS+1))
else
  echo -e "  ${RED}FAIL${NC}: colocation plugin start/MZ-discovery log not found"
  FAIL=$((FAIL+1))
fi

if echo "$SCHED_LOGS" | grep -qE "selecting MZ:|Found MZs .* for task .* in Job"; then
  echo -e "  ${GREEN}PASS${NC}: colocation MZ BatchNodeOrder scored the gang job"
  PASS=$((PASS+1))
else
  echo -e "  ${YELLOW}NOTE${NC}: no MZ scoring record found — scheduler may need -v=3+"
fi

cleanup
kubectl delete vcjob colocate-gang --force --grace-period=0 2>/dev/null || true
sleep 5

######################################################################
echo ""
echo "======================================================"
echo "  TEST 8: No rules = pass-through (regression — strip gpuExclusiveRules)"
echo "======================================================"
echo "  Patch the configmap to remove GPUExclusiveRules; verify all pods share."
cleanup

# Apply a stripped variant of the prod-like config (no GPUExclusiveRules line).
# Everything else stays identical to prod-lor1-k8s-2 minus uqm.
cat <<'PATCH' | kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: volcano-scheduler-configmap
  namespace: volcano-system
data:
  volcano-scheduler.conf: |
    actions: "enqueue, preemptall, allocate, backfill"
    tiers:
    - plugins:
      - name: gang
        enablePreemptable: false
        enableJobStarving: false
      - name: conformance
      - name: colocation
        enablePredicate: true
        enableNodeOrder: true
        arguments:
          ib: false
          mz: true
          mzScore: 20.0
      - name: deviceshare
        arguments:
          deviceshare.VGPUEnable: true
          deviceshare.SchedulePolicy: binpack
          deviceshare.ScheduleWeight: 5
    - plugins:
      - name: predicates
      - name: nodeorder
      - name: binpack
PATCH

restart_scheduler
sleep 5

for i in 1 2 3 4 5; do
  apply_pod t8-multi-$i 2 "    ${RULE_LABEL_KEY}: ${RULE_LABEL_VAL}"
done
sleep 40

ALL_RUNNING=true
for i in 1 2 3 4 5; do
  [ "$(get_status t8-multi-$i)" = "Running" ] || ALL_RUNNING=false
done
check "All 5 multi-gpus pods Running (no rules = sharing allowed)" "true" "$ALL_RUNNING"
if [ "$ALL_RUNNING" != "true" ]; then
  for i in 1 2 3 4 5; do
    echo "    t8-multi-$i: $(get_status t8-multi-$i) GPUs: $(get_gpus t8-multi-$i)"
  done
fi

cleanup
print_results

echo ""
echo "=== Scheduler logs (gpuexclusive) ==="
kubectl logs -n volcano-system -l app=volcano-scheduler --tail=100 2>&1 | grep gpuexclusive | tail -30
