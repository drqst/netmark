#!/bin/sh
set -u
ok=0
for tool in docker kubectl; do
  if command -v "$tool" >/dev/null 2>&1; then printf '%s: ok\n' "$tool"; else printf '%s: missing - install %s\n' "$tool" "$tool"; ok=1; fi
done
if docker info >/dev/null 2>&1; then printf 'docker daemon: ok\n'; else printf 'docker daemon: unavailable - start Docker Desktop\n'; ok=1; fi
if command -v kind >/dev/null 2>&1; then printf 'kind: ok\n'; elif kubectl cluster-info >/dev/null 2>&1; then printf 'kind: not required (cluster already reachable)\n'; else printf 'kind: missing - install from https://kind.sigs.k8s.io/docs/user/quick-start/\n'; ok=1; fi
if kubectl cluster-info >/dev/null 2>&1; then printf 'kubernetes: ok\n'; else printf 'kubernetes: unavailable - run init.sh or enable Docker Desktop Kubernetes\n'; fi
exit "$ok"
