#!/usr/bin/env bash
# Tears down the manual-test environment: port-forward, kind cluster, and
# (optionally, with --images) the locally built images.
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-hyperfleet}"
IMAGE_TAG="${IMAGE_TAG:-manual-test}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export KIND_EXPERIMENTAL_PROVIDER=podman

PF_PID_FILE="${SCRIPT_DIR}/.port-forward.pid"
if [[ -f "${PF_PID_FILE}" ]]; then
  kill "$(cat "${PF_PID_FILE}")" 2>/dev/null || true
  rm -f "${PF_PID_FILE}"
  echo "stopped port-forward"
fi

if kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"; then
  kind delete cluster --name "${CLUSTER_NAME}"
fi

# Host postgres from `make db/setup`, if present (not used by the kind flow).
podman rm -f psql-hyperfleet 2>/dev/null || true

if [[ "${1:-}" == "--images" ]]; then
  podman rmi -f \
    "localhost/hyperfleet-api:${IMAGE_TAG}" \
    "localhost/facade-watch-controller:${IMAGE_TAG}" 2>/dev/null || true
  echo "removed local images"
fi

rm -f "${SCRIPT_DIR}/facade-kubeconfig"
echo "done"
