#!/bin/sh
set -eu

STEP='checking prerequisites'

on_exit() {
  status=${1:-$?}
  if [ "$status" -ne 0 ]; then
    printf 'init.sh: failed during %s (exit status %s)\n' "$STEP" "$status" >&2
  fi
}

trap 'status=$?; on_exit "$status"' 0

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT="$ROOT"
if [ ! -d "$PROJECT_ROOT/k8s" ]; then PROJECT_ROOT=$(CDPATH= cd -- "$ROOT/../.." && pwd); fi
NAMESPACE=${NAMESPACE:-netmark}
CLUSTER_NAME=${CLUSTER_NAME:-netmark}
MANIFEST="$PROJECT_ROOT/k8s/postgres.yaml"
APP_MANIFEST="$PROJECT_ROOT/k8s/netmark.yaml"
GRAFANA_MANIFEST="$PROJECT_ROOT/k8s/grafana.yaml"
POSTGRES_USER=admin
POSTGRES_PASSWORD=password
POSTGRES_DB=netmark
POSTGRES_TIMEOUT_SECONDS=360
CONFIG="$ROOT/netmark.config"

STEP='checking for kubectl'
if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl is required" >&2
  exit 1
fi

install_kind() {
  if command -v kind >/dev/null 2>&1; then
    return
  fi
  if [ "$(uname -m)" != x86_64 ]; then
    echo "kind installation is supported only on x86_64; install kind manually for $(uname -m)." >&2
    exit 1
  fi
  if ! command -v curl >/dev/null 2>&1; then
    echo "curl is required to install kind" >&2
    exit 1
  fi
  if ! command -v sudo >/dev/null 2>&1; then
    echo "sudo is required to install kind to /usr/local/bin" >&2
    exit 1
  fi
  STEP='installing kind'
  curl -fL -o ./kind https://kind.sigs.k8s.io/dl/v0.33.0/kind-linux-amd64
  chmod +x ./kind
  sudo mv ./kind /usr/local/bin/kind
}

install_kind

STEP='building the netmark tools image'
docker build -t netmark-tools:local -f "$PROJECT_ROOT/k8s/Dockerfile" "$PROJECT_ROOT/k8s"
STEP='building the netmark application image'
docker build -t netmark-app:local -f "$PROJECT_ROOT/k8s/Dockerfile.netmark" "$PROJECT_ROOT"

STEP='checking Kubernetes cluster availability'
if ! kubectl cluster-info >/dev/null 2>&1; then
  if command -v kind >/dev/null 2>&1; then
    if ! kind get clusters | grep -qx "$CLUSTER_NAME"; then
      STEP="creating kind cluster $CLUSTER_NAME"
      kind create cluster --name "$CLUSTER_NAME" --config "$PROJECT_ROOT/k8s/kind-config.yaml"
    fi
    STEP="selecting kind cluster $CLUSTER_NAME"
    kubectl config use-context "kind-$CLUSTER_NAME" >/dev/null
  else
    echo "No Kubernetes cluster is reachable. Enable Docker Desktop Kubernetes or install kind, then rerun init.sh." >&2
    exit 1
  fi
fi

# All nodes must be up before anything is scheduled: the control plane, the
# database node and the app node from k8s/kind-config.yaml (or whatever nodes
# the reachable cluster has).
STEP='waiting for all Kubernetes nodes to become Ready'
kubectl wait --for=condition=Ready node --all --timeout=180s

# The Kubernetes deployment always bootstraps the same local account. Add any
# application-specific database accounts manually after init.sh completes.
STEP="creating namespace $NAMESPACE"
kubectl -n "$NAMESPACE" create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
STEP='creating PostgreSQL credentials secret'
kubectl -n "$NAMESPACE" create secret generic netmark-postgres \
  --from-literal=POSTGRES_USER="$POSTGRES_USER" \
  --from-literal=POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  --from-literal=POSTGRES_DB="$POSTGRES_DB" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# The app image is local, so hand it to the kind nodes before applying the
# workload. A kind-backed current cluster needs the kind CLI even when the
# cluster already exists; otherwise imagePullPolicy: Never leaves the app pod
# permanently in ErrImageNeverPull.
if kubectl get nodes -o jsonpath='{.items[*].spec.providerID}' 2>/dev/null | grep -q 'kind://'; then
  if ! command -v kind >/dev/null 2>&1; then
    echo "The current cluster is kind, but the kind CLI is required to load netmark-app:local. Install kind and rerun init.sh." >&2
    exit 1
  fi
  STEP='loading the application image into kind'
  kind load docker-image netmark-app:local --name "$CLUSTER_NAME"
else
  echo "netmark-app:local requires a kind cluster; set up kind with init.sh or publish the image to a registry." >&2
  exit 1
fi

STEP='applying the PostgreSQL manifest'
kubectl apply -f "$MANIFEST"

STEP='waiting for PostgreSQL to accept connections'
postgres_ready=false
attempt=0
while [ "$attempt" -lt $((POSTGRES_TIMEOUT_SECONDS / 2)) ]; do
  if kubectl -n "$NAMESPACE" exec deployment/netmark-postgres -c postgres -- \
    pg_isready -h 127.0.0.1 -d postgres >/dev/null 2>&1; then
    postgres_ready=true
    break
  fi
  attempt=$((attempt + 1))
  sleep 2
done
if [ "$postgres_ready" != true ]; then
  echo "PostgreSQL did not accept connections within ${POSTGRES_TIMEOUT_SECONDS} seconds" >&2
  exit 1
fi

STEP='configuring PostgreSQL credentials'
kubectl -n "$NAMESPACE" exec deployment/netmark-postgres -c postgres -- \
  sh -ec '
    for bootstrap_user in "$POSTGRES_USER" postgres netmark; do
      if psql --username="$bootstrap_user" --dbname=postgres -c "SELECT 1" >/dev/null 2>&1; then
        export BOOTSTRAP_USER=$bootstrap_user
        break
      fi
    done
    : "${BOOTSTRAP_USER:?unable to connect to PostgreSQL as the configured, postgres, or legacy netmark user}"
    psql --username="$BOOTSTRAP_USER" --dbname=postgres -v ON_ERROR_STOP=1 \
      -c "DO \$\$ BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '\''$POSTGRES_USER'\'') THEN
              CREATE ROLE \"$POSTGRES_USER\" LOGIN SUPERUSER PASSWORD '\''$POSTGRES_PASSWORD'\'';
            ELSE
              ALTER ROLE \"$POSTGRES_USER\" LOGIN SUPERUSER PASSWORD '\''$POSTGRES_PASSWORD'\'';
            END IF;
          END \$\$;"
    psql --username="$BOOTSTRAP_USER" --dbname=postgres -v ON_ERROR_STOP=1 \
      -c "ALTER DATABASE \"$POSTGRES_DB\" OWNER TO \"$POSTGRES_USER\";"
  '

STEP='restarting netmark with PostgreSQL credentials'
kubectl -n "$NAMESPACE" rollout restart deployment/netmark-postgres
STEP='waiting for the PostgreSQL deployment rollout'
kubectl -n "$NAMESPACE" rollout status deployment/netmark-postgres --timeout="${POSTGRES_TIMEOUT_SECONDS}s"

# Only the Service is left to apply for the web interface: the netmark
# container itself is already up as part of the netmark-postgres pod above, so
# its always-on local log/netmark.sqlite lives on the same pod/volume as the
# external PostgreSQL it complements.
STEP='applying the netmark service manifest'
kubectl apply -f "$APP_MANIFEST"

# Grafana reads the same external metrics database the CLI writes to.
STEP='applying the Grafana manifest'
kubectl apply -f "$GRAFANA_MANIFEST"
STEP='waiting for the Grafana deployment rollout'
kubectl -n "$NAMESPACE" rollout status deployment/netmark-grafana --timeout="${POSTGRES_TIMEOUT_SECONDS}s"

# Every pod in the namespace must be Ready before init.sh declares success:
# the postgres pod (postgres + volume + netmark containers) and the Grafana pod.
STEP="waiting for all pods in namespace $NAMESPACE to become Ready"
kubectl -n "$NAMESPACE" wait --for=condition=Ready pod --all --timeout="${POSTGRES_TIMEOUT_SECONDS}s"

# Port-forward PostgreSQL and the web service to the host so the generated
# metrics configuration and netmarkctl work even when this cluster was not
# created with the kind host-port mappings.
STEP='starting the PostgreSQL port-forward'
nohup kubectl -n "$NAMESPACE" port-forward service/netmark-postgres 5433:5432 \
  >/tmp/netmark-postgres-port-forward.log 2>&1 &
STEP='starting the netmark web port-forward'
nohup kubectl -n "$NAMESPACE" port-forward service/netmark-app 8080:8080 \
  >/tmp/netmark-app-port-forward.log 2>&1 &

STEP='waiting for the netmark web port-forward'
web_ready=false
attempt=0
while [ "$attempt" -lt 15 ]; do
  if curl -fsS --max-time 2 http://127.0.0.1:8080/api/v1/health >/dev/null 2>&1; then
    web_ready=true
    break
  fi
  attempt=$((attempt + 1))
  sleep 1
done
if [ "$web_ready" != true ]; then
  echo "The netmark web port-forward did not become ready; see /tmp/netmark-app-port-forward.log" >&2
  exit 1
fi

cat > "$CONFIG" <<EOF
# Generated by init.sh. Override with: configure metrics <connection>
metrics:
  sql: "postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@127.0.0.1:5433/${POSTGRES_DB}"
EOF
echo "PostgreSQL/Timescale is ready on 127.0.0.1:5433"
echo "netmark web interface (web CLI): http://127.0.0.1:8080"
echo "Grafana (external metrics graphs): http://127.0.0.1:3000"
echo "Host CLI: $PROJECT_ROOT/netmarkctl help"
echo "Config written to $CONFIG"
echo "Use: metrics enable"
