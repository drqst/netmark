#!/bin/sh
# Test cases for a running netmark Kubernetes cluster. Run it after init.sh:
#
#   ./k8s/cluster-test.sh
#
# Every case prints "ok" or "FAIL"; the script exits non-zero if any case fails,
# so it can be used as a smoke test in a pipeline.
set -u

namespace=${NETMARK_NAMESPACE:-netmark}
web=${NETMARK_WEB:-http://127.0.0.1:8080}
failures=0
cases=0

case_ok() {
  cases=$((cases + 1))
  printf '%-46s ok\n' "$1"
}

case_fail() {
  cases=$((cases + 1))
  failures=$((failures + 1))
  # Kubernetes errors are long and repetitive, so only the tail of the first
  # line is kept: that is the part that names the actual problem.
  printf '%-46s FAIL: %s\n' "$1" "$(printf '%s' "$2" | tr "\n" " " | tail -c 160)"
}

check() {
  name=$1
  shift
  output=$("$@" 2>&1)
  if [ $? -eq 0 ]; then
    case_ok "$name"
  else
    case_fail "$name" "$(printf '%s' "$output" | tr '\n' ' ')"
  fi
}

if ! command -v kubectl >/dev/null 2>&1; then
  printf 'kubectl is missing - install it and run init.sh first\n' >&2
  exit 1
fi

check "kubernetes api reachable" kubectl cluster-info
check "namespace $namespace exists" kubectl get namespace "$namespace"

# The cluster is three containers: postgres and its volume container in one pod,
# and the netmark web server in another.
containers=$(kubectl -n "$namespace" get deployment netmark-postgres \
  -o jsonpath='{.spec.template.spec.containers[*].name}' 2>/dev/null)
case " $containers " in
  *" postgres "*) case_ok "postgres container is deployed" ;;
  *) case_fail "postgres container is deployed" "containers: ${containers:-none}" ;;
esac
case " $containers " in
  *" volume "*) case_ok "volume container is deployed" ;;
  *) case_fail "volume container is deployed" "containers: ${containers:-none}" ;;
esac

for deployment in netmark-postgres netmark; do
  ready=$(kubectl -n "$namespace" get deployment "$deployment" \
    -o jsonpath='{.status.readyReplicas}' 2>/dev/null)
  if [ "${ready:-0}" -ge 1 ] 2>/dev/null; then
    case_ok "deployment $deployment is ready"
  else
    case_fail "deployment $deployment is ready" "ready replicas: ${ready:-0}"
  fi
done

bound=$(kubectl -n "$namespace" get pvc netmark-postgres-data \
  -o jsonpath='{.status.phase}' 2>/dev/null)
if [ "$bound" = "Bound" ]; then
  case_ok "data volume is bound"
else
  case_fail "data volume is bound" "phase: ${bound:-missing}"
fi

check "postgres accepts connections" kubectl -n "$namespace" exec \
  deployment/netmark-postgres -c postgres -- pg_isready -U netmark

check "volume container owns the data volume" kubectl -n "$namespace" exec \
  deployment/netmark-postgres -c volume -- test -d /data

if command -v curl >/dev/null 2>&1; then
  status=$(curl -fsS --max-time 5 "$web/api/v1/status" 2>&1)
  if [ $? -eq 0 ] && printf '%s' "$status" | grep -q '"running"'; then
    case_ok "web server reports live status"
  else
    case_fail "web server reports live status" "$(printf '%s' "$status" | tr '\n' ' ')"
  fi

  reply=$(curl -fsS --max-time 5 -X POST "$web/api/v1/cli" \
    -H 'Content-Type: application/json' -d '{"command":"status"}' 2>&1)
  if [ $? -eq 0 ] && printf '%s' "$reply" | grep -q 'Web server'; then
    case_ok "web CLI answers the status command"
  else
    case_fail "web CLI answers the status command" "$(printf '%s' "$reply" | tr '\n' ' ')"
  fi
else
  case_fail "web server is reachable" "curl is missing"
fi

printf '\n%d cases, %d failed\n' "$cases" "$failures"
[ "$failures" -eq 0 ] || exit 1
