# facade-watch-controller

A minimal Kubernetes-style controller for manually testing the HyperFleet
API's [Kubernetes-compatible facade](../../docs/k8s-facade.md). It runs
unmodified client-go shared informers (LIST + WATCH) against the facade for
`clusters`, `nodepools`, and `channels`, and logs every add/update/delete —
including namespace (parent ID), resourceVersion, generation,
deletionTimestamp, and the aggregated `Reconciled` condition.

## Quick start

```bash
cd scripts/
./00-setup.sh            # kind cluster + API (helm, facade on) + controller + port-forward
./10-lifecycle-test.sh   # drive a full CRUD lifecycle, print what the controller saw
./99-teardown.sh         # tear it all down (add --images to also remove images)
```

Prerequisites: `podman`, `kind`, `kubectl`, `helm`, `go`. On Fedora the
podman user socket is started automatically by the setup script.

## What the lifecycle test proves

`10-lifecycle-test.sh` drives the **native** REST API and prints the
controller's informer events, demonstrating:

| Native API action | Facade watch event |
|---|---|
| `POST /clusters` | `ADD clusters` |
| `POST /clusters/{id}/nodepools` | `ADD nodepools` with `namespace=<cluster id>` |
| `PATCH /clusters/{id}` | `UPDATE`, generation bumps |
| `PUT /clusters/{id}/statuses` | `UPDATE`, resourceVersion moves but generation does **not** (condition-only change) |
| `DELETE /clusters/{id}` | `UPDATE` with `deletionTimestamp` on the cluster **and** the cascaded nodepool (soft delete) |
| `POST /clusters/{id}/force-delete` | `DELETE` for nodepool then cluster (finalization) |
| `POST` + `DELETE /channels` | `ADD` then `DELETE` (hard-delete kind, tombstone event) |

Resource versions are globally, strictly increasing across all kinds.

## Other scripts

- `20-watch-raw.sh [resource] [resourceVersion]` — raw watch frames via
  `curl -N`, formatted one per line. Use `0` to replay current state, an old
  seq to test history replay, or a compacted seq to see `410 Gone`.
- `30-kubectl-facade.sh` — writes `scripts/facade-kubeconfig` and runs
  `kubectl api-resources` / `get clusters` / `get nodepools -A` against the
  facade. Then try `kubectl --kubeconfig scripts/facade-kubeconfig get clusters -w`.

Watch the controller live while driving changes from another terminal:

```bash
kubectl -n hyperfleet logs deploy/facade-watch-controller -f
```

## Knobs

All scripts accept env overrides: `CLUSTER_NAME` (kind cluster, default
`hyperfleet`), `NAMESPACE` (default `hyperfleet`), `IMAGE_TAG` (default
`manual-test`), `PF_PORT` (port-forward, default `18000`).
