#!/usr/bin/env bash
# Brings up the full manual-test environment in a kind cluster:
#   kind cluster -> API image + controller image -> helm install (facade
#   enabled, JWT off, embedded postgres) -> watch controller -> port-forward.
#
# Requirements: podman, kind, kubectl, helm, go.
# Re-runnable: helm upgrade --install and kubectl apply are idempotent.
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-hyperfleet}"
NAMESPACE="${NAMESPACE:-hyperfleet}"
IMAGE_TAG="${IMAGE_TAG:-manual-test}"
PF_PORT="${PF_PORT:-18000}"

REPO_ROOT="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
CONTROLLER_DIR="${REPO_ROOT}/examples/facade-watch-controller"
export KIND_EXPERIMENTAL_PROVIDER=podman

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

step "Checking prerequisites"
for tool in podman kind kubectl helm go; do
  command -v "$tool" >/dev/null || { echo "missing: $tool"; exit 1; }
done
systemctl --user is-active --quiet podman.socket || systemctl --user start podman.socket

step "Creating kind cluster '${CLUSTER_NAME}' (skipped if it exists)"
if ! kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"; then
  kind create cluster --name "${CLUSTER_NAME}" --wait 120s
fi
kubectl config use-context "kind-${CLUSTER_NAME}"

step "Building API image (localhost/hyperfleet-api:${IMAGE_TAG})"
make -C "${REPO_ROOT}" image \
  IMAGE_REGISTRY=localhost IMAGE_NAME=hyperfleet-api IMAGE_TAG="${IMAGE_TAG}"

step "Building controller image (localhost/facade-watch-controller:${IMAGE_TAG})"
(cd "${CONTROLLER_DIR}" && CGO_ENABLED=0 go build -o controller . &&
  podman build -t "localhost/facade-watch-controller:${IMAGE_TAG}" .)

step "Loading images into kind"
ARCHIVE="$(mktemp --suffix=.tar)"
trap 'rm -f "${ARCHIVE}"' EXIT
podman save -m \
  "localhost/hyperfleet-api:${IMAGE_TAG}" \
  "localhost/facade-watch-controller:${IMAGE_TAG}" \
  -o "${ARCHIVE}"
kind load image-archive "${ARCHIVE}" --name "${CLUSTER_NAME}"

step "Installing hyperfleet-api chart (facade enabled, JWT off)"
helm upgrade --install hyperfleet-api "${REPO_ROOT}/charts" \
  --namespace "${NAMESPACE}" --create-namespace \
  --set image.registry=localhost \
  --set image.repository=hyperfleet-api \
  --set "image.tag=${IMAGE_TAG}" \
  --set image.pullPolicy=Never \
  --set config.k8s_facade.enabled=true
kubectl -n "${NAMESPACE}" rollout status deploy/hyperfleet-api --timeout=180s

step "Deploying facade-watch-controller"
kubectl apply -f "${CONTROLLER_DIR}/deploy.yaml"
kubectl -n "${NAMESPACE}" rollout status deploy/facade-watch-controller --timeout=120s

step "Starting port-forward localhost:${PF_PORT} -> hyperfleet-api:8000"
PF_PID_FILE="${CONTROLLER_DIR}/scripts/.port-forward.pid"
if [[ -f "${PF_PID_FILE}" ]] && kill -0 "$(cat "${PF_PID_FILE}")" 2>/dev/null; then
  echo "port-forward already running (pid $(cat "${PF_PID_FILE}"))"
else
  nohup kubectl -n "${NAMESPACE}" port-forward svc/hyperfleet-api "${PF_PORT}:8000" \
    >/dev/null 2>&1 &
  echo $! > "${PF_PID_FILE}"
  sleep 2
fi

step "Sanity check"
curl -sf "http://localhost:${PF_PORT}/apis/hyperfleet.openshift.io/v1alpha1" \
  | python3 -m json.tool | head -8

cat <<EOF

Environment is up. Next:
  ./10-lifecycle-test.sh                                # drive CRUD, then check controller logs
  kubectl -n ${NAMESPACE} logs deploy/facade-watch-controller -f   # watch events live
  ./20-watch-raw.sh clusters                            # raw watch stream via curl
  ./30-kubectl-facade.sh                                # use kubectl against the facade
  ./99-teardown.sh                                      # tear everything down
EOF
