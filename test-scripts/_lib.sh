#!/bin/bash
# Shared helpers for vGPU/GPU integration tests on minikube.
# Source with:    source "$(dirname "$0")/_lib.sh"
#
# Conventions used by callers:
#   $NODE   — the node to attach fake GPUs to (set automatically)
#   $PASS / $FAIL — counters incremented by check helpers
#   $KEEPALIVE_PID — handshake keepalive background pid (cleaned by trap)

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASS=0
FAIL=0

check() {
  local desc="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    echo -e "  ${GREEN}PASS${NC}: $desc (expected=$expected, got=$actual)"
    PASS=$((PASS+1))
  else
    echo -e "  ${RED}FAIL${NC}: $desc (expected=$expected, got=$actual)"
    FAIL=$((FAIL+1))
  fi
}

check_no_overlap() {
  local desc="$1" gpus_a="$2" gpus_b="$3"
  local ids_a=$(echo "$gpus_a" | tr ':' '\n' | grep GPU | cut -d, -f1 | sort)
  local ids_b=$(echo "$gpus_b" | tr ':' '\n' | grep GPU | cut -d, -f1 | sort)
  local overlap=$(comm -12 <(echo "$ids_a") <(echo "$ids_b"))
  if [ -z "$overlap" ]; then
    echo -e "  ${GREEN}PASS${NC}: $desc (no GPU overlap)"
    PASS=$((PASS+1))
  else
    echo -e "  ${RED}FAIL${NC}: $desc (overlapping GPUs: $overlap)"
    FAIL=$((FAIL+1))
  fi
}

check_has_overlap() {
  local desc="$1" gpus_a="$2" gpus_b="$3"
  local ids_a=$(echo "$gpus_a" | tr ':' '\n' | grep GPU | cut -d, -f1 | sort)
  local ids_b=$(echo "$gpus_b" | tr ':' '\n' | grep GPU | cut -d, -f1 | sort)
  local overlap=$(comm -12 <(echo "$ids_a") <(echo "$ids_b"))
  if [ -n "$overlap" ]; then
    echo -e "  ${GREEN}PASS${NC}: $desc (GPU overlap found: $overlap)"
    PASS=$((PASS+1))
  else
    echo -e "  ${RED}FAIL${NC}: $desc (expected overlap but none found)"
    FAIL=$((FAIL+1))
  fi
}

count_unique_gpus() {
  echo "$1" | tr ':' '\n' | grep GPU | cut -d, -f1 | sort -u | wc -l
}

get_gpu_ids() {
  echo "$1" | tr ':' '\n' | grep GPU | cut -d, -f1 | sort -u
}

get_status() { kubectl get pod "$1" -o jsonpath='{.status.phase}' 2>/dev/null; }
get_gpus()   { kubectl get pod "$1" -o jsonpath='{.metadata.annotations.volcano\.sh/vgpu-ids-new}' 2>/dev/null; }

cleanup() {
  echo "--- cleanup ---"
  kubectl delete vcjob --all --force --grace-period=0 2>/dev/null || true
  kubectl delete pod --all --force --grace-period=0 2>/dev/null || true
  kubectl delete podgroup --all 2>/dev/null || true
  sleep 3
}

# Restart the volcano scheduler and wait for readiness.
restart_scheduler() {
  kubectl delete pod -n volcano-system -l app=volcano-scheduler 2>&1 | tail -2
  sleep 5
  kubectl wait --for=condition=ready pod -l app=volcano-scheduler -n volcano-system --timeout=120s
}

# Patch the first node with fake vGPU capacity (16 vGPU slots, 128GB), annotate
# 8 fake GPUs (2 sharing slots each = 16 total), and start the handshake
# keepalive in the background. Sets $NODE and $KEEPALIVE_PID.
setup_fake_node_gpus() {
  NODE=$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')

  kubectl proxy --port=8199 &
  local proxy_pid=$!
  sleep 2
  curl -s -X PATCH \
    -H "Content-Type: application/json-patch+json" \
    --data '[
      {"op": "add", "path": "/status/capacity/volcano.sh~1vgpu-number", "value": "16"},
      {"op": "add", "path": "/status/capacity/volcano.sh~1vgpu-memory", "value": "131072"},
      {"op": "add", "path": "/status/capacity/volcano.sh~1vgpu-cores", "value": "1600"}
    ]' \
    http://localhost:8199/api/v1/nodes/$NODE/status > /dev/null
  kill $proxy_pid 2>/dev/null; wait $proxy_pid 2>/dev/null || true

  kubectl annotate node $NODE --overwrite \
    volcano.sh/node-vgpu-register="GPU-0,2,16384,NVIDIA,true,hami-core:GPU-1,2,16384,NVIDIA,true,hami-core:GPU-2,2,16384,NVIDIA,true,hami-core:GPU-3,2,16384,NVIDIA,true,hami-core:GPU-4,2,16384,NVIDIA,true,hami-core:GPU-5,2,16384,NVIDIA,true,hami-core:GPU-6,2,16384,NVIDIA,true,hami-core:GPU-7,2,16384,NVIDIA,true,hami-core" > /dev/null

  (
    while true; do
      kubectl annotate node $NODE --overwrite volcano.sh/node-vgpu-handshake="Active" 2>/dev/null
      sleep 30
    done
  ) &
  KEEPALIVE_PID=$!
  trap "kill $KEEPALIVE_PID 2>/dev/null" EXIT

  # Wait for the scheduler to actually pick up the fake GPUs. After a scheduler
  # restart, NewGPUDevices() reads the volcano.sh/node-vgpu-handshake annotation
  # — if it sees "Requesting_<time>" (a transient state the scheduler itself
  # writes), it returns nil for up to 60s. The keepalive above sets it back to
  # "Active", but it can take a few session cycles for SetNode to refresh
  # node.Others with the real *GPUDevices. We poll for any node with a real
  # vgpu-cores allocatable (means the device discovery completed) up to 30s.
  echo "  waiting for scheduler to discover fake GPUs..."
  local deadline=$((SECONDS + 30))
  while [ $SECONDS -lt $deadline ]; do
    local handshake
    handshake=$(kubectl get node $NODE -o jsonpath='{.metadata.annotations.volcano\.sh/node-vgpu-handshake}' 2>/dev/null)
    if [[ "$handshake" == "Active" ]]; then
      sleep 3  # one more session cycle
      break
    fi
    sleep 2
  done
}

# Label every node with a `node.linkedin.com/maintenance-zone` value (the label
# the colocation MZ plugin reads at session-open) and uncordon worker nodes so
# scoring sees them as real candidates. Without this, the MZ plugin loads but
# its mzNodes map only ever holds an empty-string key, so BatchNodeOrder
# returns no scores and the plugin is effectively a no-op.
setup_mz_labels() {
  local zones=("zone-a" "zone-b" "zone-c")
  local i=0
  local n
  for n in $(kubectl get nodes -o jsonpath='{.items[*].metadata.name}'); do
    kubectl uncordon "$n" 2>/dev/null | tail -1 || true
    kubectl label node "$n" --overwrite \
      "node.linkedin.com/maintenance-zone=${zones[$((i % ${#zones[@]}))]}" >/dev/null
    i=$((i+1))
  done
}

# Set up fake vGPU capacity on a SECOND node (different MZ) so the colocation
# plugin has multiple maintenance zones to score against. Both this node and
# the primary $NODE will each have 8 fake GPUs × 2 vGPU slots = 16 vGPUs.
# Returns the second node name in $NODE2.
setup_fake_node_gpus_second() {
  NODE2=$(kubectl get nodes -o jsonpath='{.items[1].metadata.name}')
  if [ -z "$NODE2" ] || [ "$NODE2" = "$NODE" ]; then
    echo "  WARN: no second node available; colocation multi-MZ test will be skipped"
    return 1
  fi

  kubectl proxy --port=8200 &
  local proxy_pid=$!
  sleep 2
  curl -s -X PATCH \
    -H "Content-Type: application/json-patch+json" \
    --data '[
      {"op": "add", "path": "/status/capacity/volcano.sh~1vgpu-number", "value": "16"},
      {"op": "add", "path": "/status/capacity/volcano.sh~1vgpu-memory", "value": "131072"},
      {"op": "add", "path": "/status/capacity/volcano.sh~1vgpu-cores", "value": "1600"}
    ]' \
    http://localhost:8200/api/v1/nodes/$NODE2/status > /dev/null
  kill $proxy_pid 2>/dev/null; wait $proxy_pid 2>/dev/null || true

  kubectl annotate node $NODE2 --overwrite \
    volcano.sh/node-vgpu-register="GPU-0,2,16384,NVIDIA,true,hami-core:GPU-1,2,16384,NVIDIA,true,hami-core:GPU-2,2,16384,NVIDIA,true,hami-core:GPU-3,2,16384,NVIDIA,true,hami-core:GPU-4,2,16384,NVIDIA,true,hami-core:GPU-5,2,16384,NVIDIA,true,hami-core:GPU-6,2,16384,NVIDIA,true,hami-core:GPU-7,2,16384,NVIDIA,true,hami-core" > /dev/null

  (
    while true; do
      kubectl annotate node $NODE2 --overwrite volcano.sh/node-vgpu-handshake="Active" 2>/dev/null
      sleep 30
    done
  ) &
  KEEPALIVE_PID2=$!
  trap "kill $KEEPALIVE_PID 2>/dev/null; kill $KEEPALIVE_PID2 2>/dev/null" EXIT

  echo "  waiting for scheduler to discover second-node fake GPUs..."
  local deadline=$((SECONDS + 30))
  while [ $SECONDS -lt $deadline ]; do
    local handshake
    handshake=$(kubectl get node $NODE2 -o jsonpath='{.metadata.annotations.volcano\.sh/node-vgpu-handshake}' 2>/dev/null)
    if [[ "$handshake" == "Active" ]]; then
      sleep 3
      break
    fi
    sleep 2
  done
}

# Set up a node with full physical nvidia.com/gpu capacity (no vGPU). Used to
# build a heterogeneous cluster where some nodes expose full GPUs and others
# expose vGPUs — the topology that the v0.0.5 Ghost-Reservation Bug used to
# break (shallow CloneOthers leaked vGPU UsedMem onto non-vGPU nodes).
#
# Usage: setup_full_gpu_node <node-name> <num-gpus>
setup_full_gpu_node() {
  local node="$1" count="$2"

  kubectl proxy --port=8201 &
  local proxy_pid=$!
  sleep 2
  curl -s -X PATCH \
    -H "Content-Type: application/json-patch+json" \
    --data "[
      {\"op\": \"add\", \"path\": \"/status/capacity/nvidia.com~1gpu\", \"value\": \"${count}\"}
    ]" \
    http://localhost:8201/api/v1/nodes/${node}/status > /dev/null
  kill $proxy_pid 2>/dev/null; wait $proxy_pid 2>/dev/null || true
}

# Pre-flight check: verify the deviceshare Allocate path is actually engaged.
# Schedules a single vGPU pod and asserts that the volcano scheduler patches the
# `volcano.sh/vgpu-ids-new` annotation onto it. If the annotation is absent, the
# deviceshare cache code path is NOT being exercised — the test would pass
# vacuously (capacity-only checks succeed because k8s extended-resource accounting
# still runs). Aborts the suite with a loud message rather than reporting a fake
# pass.
#
# Background: in 1.14+ the deviceshare Allocate path requires the pod to carry a
# `volcano.sh/devices-to-allocate` annotation, which is set by volcano-admission's
# vGPU mutation webhook. If admission isn't configured for vGPU mutation, Allocate
# bails out early and never patches `vgpu-ids-new`. Tests that don't check this
# annotation will pass without exercising the code paths under test.
require_deviceshare_allocate_engaged() {
  echo ""
  echo "=== PRE-FLIGHT: verifying deviceshare Allocate path is engaged ==="

  # The scheduler may have just restarted; node device discovery and the
  # handshake transition out of "Requesting" can take a few session cycles.
  # Try the probe up to 3 times before declaring failure, with a fresh pod
  # each attempt (a pod that landed without the annotation will never get
  # it retro-actively patched).
  local attempt
  for attempt in 1 2 3; do
    local probe_pod="preflight-vgpu-probe-$attempt"
    kubectl delete pod "$probe_pod" --force --grace-period=0 2>/dev/null || true
    cat <<EOF | kubectl apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata:
  name: $probe_pod
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

    local deadline=$((SECONDS + 30))
    local phase=""
    while [ $SECONDS -lt $deadline ]; do
      phase=$(get_status "$probe_pod")
      [ "$phase" = "Running" ] && break
      sleep 3
    done

    local ann
    ann=$(get_gpus "$probe_pod")
    kubectl delete pod "$probe_pod" --force --grace-period=0 2>/dev/null || true

    if [ "$phase" = "Running" ] && [ -n "$ann" ]; then
      echo -e "  ${GREEN}PRE-FLIGHT PASS${NC}: probe pod scheduled with annotation: $ann (attempt $attempt)"
      return 0
    fi
    echo -e "  ${YELLOW}attempt $attempt: phase=$phase, ann=${ann:-<empty>}${NC}"
    sleep 5
  done

  echo -e "  ${RED}PRE-FLIGHT FAIL${NC}: 3 probe attempts all landed without volcano.sh/vgpu-ids-new annotation."
  echo -e "  ${YELLOW}This means the deviceshare Allocate code path is NOT being exercised.${NC}"
  echo -e "  ${YELLOW}Pods are scheduling via standard k8s extended-resource accounting only,${NC}"
  echo -e "  ${YELLOW}so any test based on capacity (e.g. 16 pods on 16 slots) will pass vacuously${NC}"
  echo -e "  ${YELLOW}without actually testing the vGPU code.${NC}"
  echo ""
  echo -e "  Likely causes:"
  echo -e "    - the scheduler cannot discover the fake GPUs (handshake stuck in 'Requesting')"
  echo -e "    - the volcano-admission webhook is not configured for vGPU mutation, so pods"
  echo -e "      never receive the volcano.sh/devices-to-allocate annotation that"
  echo -e "      deviceshare.Allocate gates on"
  echo ""
  echo -e "  Fix the test environment, then re-run. Aborting suite."
  exit 2
}

# Print the final results block and exit non-zero if any tests failed.
print_results() {
  echo ""
  echo "=============================================="
  echo "  RESULTS"
  echo "=============================================="
  echo -e "  ${GREEN}PASSED: $PASS${NC}"
  echo -e "  ${RED}FAILED: $FAIL${NC}"
  local total=$((PASS+FAIL))
  echo "  TOTAL:  $total"
  echo ""
  if [ $FAIL -eq 0 ]; then
    echo -e "  ${GREEN}ALL TESTS PASSED${NC}"
  else
    echo -e "  ${RED}SOME TESTS FAILED${NC}"
    exit 1
  fi
}
