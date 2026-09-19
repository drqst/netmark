#!/bin/sh
# Tests for the Kubernetes lifecycle scripts: init.sh (sets up all nodes and
# pods), stop.sh (stops and removes all pods but keeps the PostgreSQL data
# volume) and persistence of that volume between runs.
#
#   ./test.sh                        # static checks, plus live checks if a
#                                    # cluster is reachable
#   NETMARK_TEST_STOP=1 ./test.sh    # additionally run stop.sh against the
#                                    # live cluster and verify the pods are
#                                    # gone while the data volume survives
#
# Every case prints "ok" or "FAIL"; the script exits non-zero if any case
# fails, so it can be used in a pipeline.
set -u

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
namespace=${NETMARK_NAMESPACE:-netmark}
failures=0
cases=0

case_ok() {
  cases=$((cases + 1))
  printf '%-58s ok\n' "$1"
}

case_fail() {
  cases=$((cases + 1))
  failures=$((failures + 1))
  printf '%-58s FAIL: %s\n' "$1" "$(printf '%s' "$2" | tr '\n' ' ' | tail -c 160)"
}

check_contains() {
  name=$1
  file=$2
  needle=$3
  if grep -q -- "$needle" "$file" 2>/dev/null; then
    case_ok "$name"
  else
    case_fail "$name" "$file does not contain: $needle"
  fi
}

check_not_contains() {
  name=$1
  file=$2
  needle=$3
  if grep -q -- "$needle" "$file" 2>/dev/null; then
    case_fail "$name" "$file must not contain: $needle"
  else
    case_ok "$name"
  fi
}

# --- Static checks: the scripts themselves -------------------------------

for script in init.sh stop.sh; do
  if [ -x "$ROOT/$script" ]; then
    case_ok "$script exists and is executable"
  else
    case_fail "$script exists and is executable" "missing or not executable"
  fi
  if sh -n "$ROOT/$script" 2>/dev/null; then
    case_ok "$script parses as POSIX shell"
  else
    case_fail "$script parses as POSIX shell" "$(sh -n "$ROOT/$script" 2>&1)"
  fi
done

# init.sh must set up every node and every pod: create the kind cluster from
# k8s/kind-config.yaml, wait for all nodes, apply all three manifests and wait
# for every pod to be Ready.
check_contains "init.sh creates the kind cluster" \
  "$ROOT/init.sh" "kind create cluster"
check_contains "init.sh waits for all nodes to be Ready" \
  "$ROOT/init.sh" "wait --for=condition=Ready node --all"
check_contains "init.sh deploys postgres" "$ROOT/init.sh" 'kubectl apply -f "$MANIFEST"'
check_contains "init.sh deploys the app service" \
  "$ROOT/init.sh" 'kubectl apply -f "$APP_MANIFEST"'
check_contains "init.sh deploys grafana" \
  "$ROOT/init.sh" 'kubectl apply -f "$GRAFANA_MANIFEST"'
check_contains "init.sh waits for the postgres rollout" \
  "$ROOT/init.sh" "rollout status deployment/netmark-postgres"
check_contains "init.sh waits for the grafana rollout" \
  "$ROOT/init.sh" "rollout status deployment/netmark-grafana"
check_contains "init.sh waits for all pods to be Ready" \
  "$ROOT/init.sh" "wait --for=condition=Ready pod --all"
check_contains "init.sh reports the phase of a failed command" \
  "$ROOT/init.sh" "init.sh: failed during"

# stop.sh must stop and remove every pod, but never the data volume: it may
# only delete the deployments/services/configmaps, not the PVC, not the
# namespace and not the manifests wholesale (which would take the PVC along).
check_contains "stop.sh deletes the postgres and grafana deployments" \
  "$ROOT/stop.sh" "delete deployment netmark-postgres netmark-grafana"
check_contains "stop.sh deletes the services" "$ROOT/stop.sh" "delete service"
check_contains "stop.sh waits for pods to be gone" \
  "$ROOT/stop.sh" "wait --for=delete pod --all"
check_contains "stop.sh also removes standalone pods" \
  "$ROOT/stop.sh" "delete pod --all"
check_not_contains "stop.sh never deletes the data volume claim" \
  "$ROOT/stop.sh" "delete pvc"
check_not_contains "stop.sh never deletes persistentvolumeclaims" \
  "$ROOT/stop.sh" "delete persistentvolumeclaim"
check_not_contains "stop.sh never deletes the namespace" \
  "$ROOT/stop.sh" "delete namespace"
check_not_contains "stop.sh never deletes whole manifests" \
  "$ROOT/stop.sh" "delete -f"

# The data volume must live in a PVC of its own (not the deployment manifest's
# pod spec alone), so deleting the workloads cannot delete the data.
check_contains "postgres data lives on a PersistentVolumeClaim" \
  "$ROOT/k8s/postgres.yaml" "kind: PersistentVolumeClaim"
check_contains "the postgres pod mounts the data volume claim" \
  "$ROOT/k8s/postgres.yaml" "claimName: netmark-postgres-data"
check_contains "postgres initializes in a dedicated persistent directory" \
  "$ROOT/k8s/postgres.yaml" "value: /var/lib/postgresql/data/pgdata"
check_contains "postgres reuses existing databases at the volume root" \
  "$ROOT/k8s/postgres.yaml" 'if \[ -s /var/lib/postgresql/data/PG_VERSION \]'
check_contains "postgres never rolls out two writers on the volume" \
  "$ROOT/k8s/postgres.yaml" "type: Recreate"

# --- Live checks: a reachable cluster ------------------------------------

if command -v kubectl >/dev/null 2>&1 && kubectl cluster-info >/dev/null 2>&1; then
  nodes_total=$(kubectl get nodes --no-headers 2>/dev/null | wc -l | tr -d ' ')
  nodes_ready=$(kubectl get nodes --no-headers 2>/dev/null | awk '$2 == "Ready"' | wc -l | tr -d ' ')
  if [ "${nodes_total:-0}" -ge 1 ] && [ "$nodes_ready" = "$nodes_total" ]; then
    case_ok "all $nodes_total cluster nodes are Ready"
  else
    case_fail "all cluster nodes are Ready" "$nodes_ready of ${nodes_total:-0} Ready"
  fi

  if kubectl get namespace "$namespace" >/dev/null 2>&1; then
    if kubectl -n "$namespace" get deployment netmark-postgres >/dev/null 2>&1; then
      for deployment in netmark-postgres netmark-grafana; do
        ready=$(kubectl -n "$namespace" get deployment "$deployment" \
          -o jsonpath='{.status.readyReplicas}' 2>/dev/null)
        if [ "${ready:-0}" -ge 1 ] 2>/dev/null; then
          case_ok "deployment $deployment has a ready pod"
        else
          case_fail "deployment $deployment has a ready pod" "ready replicas: ${ready:-0}"
        fi
      done

      not_running=$(kubectl -n "$namespace" get pods --no-headers 2>/dev/null \
        | awk '$3 != "Running"' | wc -l | tr -d ' ')
      if [ "$not_running" = "0" ]; then
        case_ok "every pod in namespace $namespace is Running"
      else
        case_fail "every pod in namespace $namespace is Running" "$not_running pod(s) not Running"
      fi

      pvc=$(kubectl -n "$namespace" get pvc netmark-postgres-data \
        -o jsonpath='{.status.phase}' 2>/dev/null)
      if [ "$pvc" = "Bound" ]; then
        case_ok "data volume netmark-postgres-data is Bound"
      else
        case_fail "data volume netmark-postgres-data is Bound" "phase: ${pvc:-missing}"
      fi

      # Credentials expand inside the postgres container, not in the host shell.
      # shellcheck disable=SC2016
      if settings=$(kubectl -n "$namespace" exec deployment/netmark-postgres -c postgres -- \
        sh -ec 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "
          SELECT setting FROM pg_settings
          WHERE name IN ('\''data_directory'\'', '\''config_file'\'',
                         '\''hba_file'\'', '\''ident_file'\'') ORDER BY name;"' 2>&1) \
        && [ "$(printf '%s\n' "$settings" \
        | grep -c '^/var/lib/postgresql/data\(/\|$\)')" -eq 4 ]; then
        case_ok "postgres data and settings are on the persistent volume"
      else
        case_fail "postgres data and settings are on the persistent volume" "$settings"
      fi

      if [ "${NETMARK_TEST_STOP:-0}" = "1" ]; then
        # Destructive: run stop.sh, then verify every pod is gone while the
        # data volume claim survives, so PostgreSQL data persists between runs.
        output=$(NAMESPACE="$namespace" "$ROOT/stop.sh" 2>&1)
        if [ $? -eq 0 ]; then
          case_ok "stop.sh runs successfully"
        else
          case_fail "stop.sh runs successfully" "$output"
        fi

        pods_left=$(kubectl -n "$namespace" get pods --no-headers 2>/dev/null | wc -l | tr -d ' ')
        if [ "$pods_left" = "0" ]; then
          case_ok "stop.sh removed every pod"
        else
          case_fail "stop.sh removed every pod" "$pods_left pod(s) remain"
        fi

        pvc=$(kubectl -n "$namespace" get pvc netmark-postgres-data \
          -o jsonpath='{.status.phase}' 2>/dev/null)
        if [ "$pvc" = "Bound" ]; then
          case_ok "data volume survives stop.sh (postgres data persists)"
        else
          case_fail "data volume survives stop.sh (postgres data persists)" "phase: ${pvc:-missing}"
        fi
      else
        printf '%-58s skipped (set NETMARK_TEST_STOP=1)\n' "stop.sh live teardown test"
      fi
    else
      # The stopped state left behind by stop.sh: no workloads, no pods, but
      # the data volume claim (and the postgres data on it) still there.
      pods_left=$(kubectl -n "$namespace" get pods --no-headers 2>/dev/null | wc -l | tr -d ' ')
      if [ "$pods_left" = "0" ]; then
        case_ok "stopped: no pods remain in namespace $namespace"
      else
        case_fail "stopped: no pods remain in namespace $namespace" "$pods_left pod(s) remain"
      fi
      if kubectl -n "$namespace" get pvc netmark-postgres-data >/dev/null 2>&1; then
        case_ok "stopped: data volume still exists (postgres data persists)"
      else
        case_fail "stopped: data volume still exists (postgres data persists)" "pvc missing"
      fi
      printf '%-58s skipped (deployment stopped; run init.sh)\n' "running-state checks"
    fi
  else
    printf '%-58s skipped (run init.sh first)\n' "live pod/volume checks"
  fi
else
  printf '%-58s skipped (no cluster reachable)\n' "live cluster checks"
fi

printf '\n%d cases, %d failed\n' "$cases" "$failures"
[ "$failures" -eq 0 ] || exit 1
