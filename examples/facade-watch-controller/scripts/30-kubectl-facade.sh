#!/usr/bin/env bash
# Generates a kubeconfig pointing kubectl at the facade (via the
# port-forward) and runs a few demo commands. The kubeconfig is left at
# scripts/facade-kubeconfig for your own use:
#
#   kubectl --kubeconfig scripts/facade-kubeconfig get clusters -w
set -euo pipefail

PF_PORT="${PF_PORT:-18000}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KUBECONFIG_FILE="${SCRIPT_DIR}/facade-kubeconfig"

cat > "${KUBECONFIG_FILE}" <<EOF
apiVersion: v1
kind: Config
clusters:
  - name: hyperfleet-facade
    cluster: {server: "http://localhost:${PF_PORT}"}
contexts:
  - name: facade
    context: {cluster: hyperfleet-facade}
current-context: facade
EOF
echo "wrote ${KUBECONFIG_FILE}"

step() { printf '\n\033[1m$ kubectl --kubeconfig facade-kubeconfig %s\033[0m\n' "$*"; }

step api-resources
kubectl --kubeconfig "${KUBECONFIG_FILE}" api-resources

step get clusters
kubectl --kubeconfig "${KUBECONFIG_FILE}" get clusters

step get nodepools -A
kubectl --kubeconfig "${KUBECONFIG_FILE}" get nodepools -A

step "get clusters -o wide (first cluster as yaml metadata)"
FIRST=$(kubectl --kubeconfig "${KUBECONFIG_FILE}" get clusters \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [[ -n "${FIRST}" ]]; then
  kubectl --kubeconfig "${KUBECONFIG_FILE}" get cluster "${FIRST}" \
    -o jsonpath='{.metadata.name}{"  uid="}{.metadata.uid}{"  rv="}{.metadata.resourceVersion}{"  gen="}{.metadata.generation}{"\n"}'
else
  echo "(no clusters exist — create one with 10-lifecycle-test.sh or curl)"
fi

cat <<EOF

Try a live watch (Ctrl-C to stop):
  kubectl --kubeconfig ${KUBECONFIG_FILE} get clusters -w
EOF
