#!/bin/sh
# Stops the netmark Kubernetes deployment brought up by init.sh: every pod is
# deleted (the postgres/volume/netmark pod and the Grafana pod, plus their
# Services), but the persistent volume claim the volume container mounts and
# PostgreSQL writes its data to is kept, so the data persists between runs and
# the next init.sh starts with the same database. The namespace and the
# postgres secret stay too, since removing the namespace would take the volume
# with it.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
NAMESPACE=${NAMESPACE:-netmark}

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl is required" >&2
  exit 1
fi

if ! kubectl cluster-info >/dev/null 2>&1; then
  echo "No Kubernetes cluster is reachable - cannot confirm shutdown." >&2
  exit 1
fi

namespace=$(kubectl get namespace "$NAMESPACE" --ignore-not-found -o name)
if [ -z "$namespace" ]; then
  echo "Namespace $NAMESPACE does not exist - nothing to stop."
  exit 0
fi

# End the PostgreSQL port-forward init.sh left running on 127.0.0.1:5433.
pkill -f "port-forward service/netmark-postgres" 2>/dev/null || true

# Delete the workloads and their Services, one by one, so the persistent
# volume claim (netmark-postgres-data) is never touched. Deleting the
# manifests wholesale or the namespace itself would remove the PVC and lose
# the PostgreSQL data.
kubectl -n "$NAMESPACE" delete deployment netmark-postgres netmark-grafana \
  --ignore-not-found --cascade=foreground --timeout=180s
kubectl -n "$NAMESPACE" delete pod --all --ignore-not-found --timeout=180s
kubectl -n "$NAMESPACE" delete service netmark-postgres netmark-app netmark-grafana \
  --ignore-not-found
kubectl -n "$NAMESPACE" delete configmap netmark-grafana-provisioning \
  netmark-grafana-dashboards --ignore-not-found

# Wait until every pod is actually gone, not just marked for deletion.
kubectl -n "$NAMESPACE" wait --for=delete pod --all --timeout=180s

remaining=$(kubectl -n "$NAMESPACE" get pods -o name)
if [ -n "$remaining" ]; then
  echo "Pods remain in namespace $NAMESPACE; shutdown is incomplete." >&2
  exit 1
fi

pvc_phase=$(kubectl -n "$NAMESPACE" get pvc netmark-postgres-data \
  -o jsonpath='{.status.phase}')
echo "All netmark pods stopped."
echo "Data volume netmark-postgres-data kept (phase: ${pvc_phase:-missing}) - PostgreSQL data persists for the next $ROOT/init.sh run."
