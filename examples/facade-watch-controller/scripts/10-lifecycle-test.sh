#!/usr/bin/env bash
# Drives a full resource lifecycle through the NATIVE HyperFleet REST API,
# then prints what the watch controller observed through the K8s facade.
#
# Expected controller log sequence (resource versions strictly increasing):
#   ADD    clusters   <name>            (create)
#   ADD    nodepools  workers  ns=<cluster-id>
#   UPDATE clusters   generation=2      (spec patch)
#   UPDATE clusters   generation=2      (adapter status: rv moves, gen does not)
#   UPDATE nodepools  deletionTimestamp (cascade soft delete)
#   UPDATE clusters   deletionTimestamp (soft delete)
#   DELETE nodepools / DELETE clusters  (force-delete finalization)
#   ADD    channels  + DELETE channels  (hard-delete kind)
set -euo pipefail

NAMESPACE="${NAMESPACE:-hyperfleet}"
PF_PORT="${PF_PORT:-18000}"
API="http://localhost:${PF_PORT}/api/hyperfleet/v1"
NAME="lifecycle-$(date +%s)"

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
json_field() { python3 -c "import json,sys; print(json.load(sys.stdin)['$1'])"; }

step "Mark current end of controller log"
BEFORE_LINES=$(kubectl -n "${NAMESPACE}" logs deploy/facade-watch-controller | wc -l)

step "1. POST cluster '${NAME}'"
CID=$(curl -sf -X POST "${API}/clusters" -H 'Content-Type: application/json' \
  -d "{\"kind\":\"Cluster\",\"name\":\"${NAME}\",\"spec\":{\"provider\":\"gcp\",\"region\":\"us-east1\"}}" \
  | json_field id)
echo "cluster id: ${CID}"

step "2. POST nodepool 'workers' under it"
curl -sf -X POST "${API}/clusters/${CID}/nodepools" -H 'Content-Type: application/json' \
  -d '{"kind":"NodePool","name":"workers","spec":{"machine_type":"n1-standard-4","replicas":3}}' \
  | json_field id

step "3. PATCH cluster spec (generation should bump)"
curl -sf -X PATCH "${API}/clusters/${CID}" -H 'Content-Type: application/json' \
  -d '{"spec":{"provider":"gcp","region":"us-east1","patched":true}}' \
  | json_field generation

step "4. PUT adapter status (condition-only: rv moves, generation must NOT)"
curl -sf -X PUT "${API}/clusters/${CID}/statuses" -H 'Content-Type: application/json' \
  -d "{\"adapter\":\"validation\",\"observed_generation\":2,\"observed_time\":\"$(date -u +%FT%TZ)\",
       \"conditions\":[{\"type\":\"Available\",\"status\":\"True\"},
                       {\"type\":\"Applied\",\"status\":\"True\"},
                       {\"type\":\"Health\",\"status\":\"True\"}]}" \
  -o /dev/null -w 'HTTP %{http_code}\n'

step "5. DELETE cluster (soft delete + nodepool cascade)"
curl -sf -X DELETE "${API}/clusters/${CID}" -o /dev/null -w 'HTTP %{http_code}\n'

step "6. Force-delete (finalization -> DELETED events)"
curl -sf -X POST "${API}/clusters/${CID}/force-delete" -H 'Content-Type: application/json' \
  -d '{"reason":"manual lifecycle test"}' -o /dev/null -w 'HTTP %{http_code}\n'

step "7. Channel create + delete (hard-delete kind, tombstone event)"
CHID=$(curl -sf -X POST "${API}/channels" -H 'Content-Type: application/json' \
  -d "{\"kind\":\"Channel\",\"name\":\"${NAME}-ch\",\"spec\":{\"is_default\":false,\"enabled_regex\":\".*\"}}" \
  | json_field id)
curl -sf -X DELETE "${API}/channels/${CHID}" -o /dev/null -w 'HTTP %{http_code}\n'

step "Waiting for events to propagate"
sleep 3

step "Controller observed (new lines only)"
kubectl -n "${NAMESPACE}" logs deploy/facade-watch-controller \
  | tail -n "+$((BEFORE_LINES + 1))" \
  | sed 's/time=[^ ]* //'
