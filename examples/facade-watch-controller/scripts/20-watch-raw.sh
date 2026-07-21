#!/usr/bin/env bash
# Opens a raw facade watch stream with curl so you can see the actual
# newline-delimited watch frames (ADDED/MODIFIED/DELETED/BOOKMARK).
#
# Usage:
#   ./20-watch-raw.sh                     # watch clusters from "now"
#   ./20-watch-raw.sh nodepools           # watch another resource
#   ./20-watch-raw.sh clusters 0          # replay current state first (RV=0)
#   ./20-watch-raw.sh clusters 42         # replay history after seq 42 (410 if compacted)
#
# Drive changes from another terminal (e.g. ./10-lifecycle-test.sh) and the
# frames appear here. Ctrl-C to stop.
set -euo pipefail

PF_PORT="${PF_PORT:-18000}"
RESOURCE="${1:-clusters}"
RV="${2:-}"

BASE="http://localhost:${PF_PORT}/apis/hyperfleet.openshift.io/v1alpha1"
if [[ -z "${RV}" ]]; then
  # Start from the current list head: no replay, live events only.
  RV=$(curl -sf "${BASE}/${RESOURCE}" \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['metadata']['resourceVersion'])")
fi
URL="${BASE}/${RESOURCE}?watch=true&allowWatchBookmarks=true&resourceVersion=${RV}"

FORMATTER="$(mktemp --suffix=.py)"
trap 'rm -f "${FORMATTER}"' EXIT
cat > "${FORMATTER}" <<'PYEOF'
import json, sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        frame = json.loads(line)
    except json.JSONDecodeError:
        print(line)
        continue
    meta = frame.get("object", {}).get("metadata", {})
    print(f'{frame.get("type", "?"):9s} '
          f'ns={meta.get("namespace", "-"):38s} '
          f'{meta.get("name", "-"):30s} '
          f'rv={meta.get("resourceVersion", "-")} '
          f'gen={meta.get("generation", "-")} '
          f'deleting={"yes" if meta.get("deletionTimestamp") else "no"}')
PYEOF

echo "watching ${RESOURCE} from resourceVersion=${RV}"
echo "-> ${URL}"
echo
# -N disables curl buffering so frames appear as they arrive.
curl -sN "${URL}" | python3 -u "${FORMATTER}"
