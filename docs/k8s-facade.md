# Kubernetes-Compatible Facade

The facade exposes HyperFleet resources through a read-only Kubernetes API so
that controllers built on client-go / controller-runtime can **list and
watch** them unmodified. It is served at:

```
/apis/hyperfleet.openshift.io/v1alpha1
```

Enable it with `k8s_facade.enabled: true` (see `configs/config.yaml.example`).
It is off by default.

## What is served

| Endpoint | Purpose |
|---|---|
| `GET /apis`, `/apis/hyperfleet.openshift.io[/v1alpha1]` | Discovery (APIGroupList, APIGroup, APIResourceList) |
| `GET /api`, `GET /version` | Legacy discovery + version stubs for kubectl/RESTMapper |
| `GET /apis/.../<plural>` | LIST (cluster scope / all namespaces) |
| `GET /apis/.../namespaces/{ns}/<plural>` | LIST scoped to one parent |
| `GET .../<plural>/{name}`, `.../namespaces/{ns}/<plural>/{name}` | GET |
| `GET .../<plural>?watch=true&resourceVersion=N` | WATCH (streaming JSON frames) |

Every entity kind registered in `config.yaml` (`entities:`) is served
automatically. The facade is **read-only** — writes go through the native
REST API.

## Mapping semantics

- **Scope**: kinds with a `parent_kind` (NodePool, Version) are *namespaced*;
  the namespace is the **parent resource's ID**. Top-level kinds (Cluster,
  Channel, WifConfig) are cluster-scoped.
- **`metadata.name`** is the native resource name; **`metadata.uid`** is the
  native ID (also in the `hyperfleet.openshift.io/id` annotation, alongside
  `hyperfleet.openshift.io/href`).
- **`metadata.resourceVersion`** is a global, monotonically increasing
  sequence from the `resource_events` outbox. It advances on *every* change,
  including adapter condition updates — unlike `generation`, which only
  advances on spec/label/reference changes.
- **Deletion**: soft-deleted resources (kinds with `required_adapters`)
  appear with `metadata.deletionTimestamp` and the synthetic finalizer
  `hyperfleet.openshift.io/adapters`; the watch emits `MODIFIED`. When
  adapters finish finalization (or on force-delete / hard-delete kinds), the
  watch emits `DELETED` with the final state.
- **`status.conditions`** follows the `metav1.Condition` shape (missing
  native reasons default to `Unknown`).
- **Selectors**: `labelSelector` (equality and set-based) and
  `fieldSelector` (`metadata.name`, `metadata.namespace`) are supported on
  list and watch. An object whose labels change out of a watch's selector is
  delivered as `DELETED` to that watch.
- **Pagination**: `limit`/`continue` are ignored and full lists returned —
  explicitly allowed by the Kubernetes list contract; client-go handles it.

## Watch semantics

- `resourceVersion=N` replays committed events with seq > N, then streams
  live. History is retained per `k8s_facade.retention`; a request below the
  compaction floor gets **410 Gone** (reason `Expired`), which makes
  reflectors relist — exactly like a kube-apiserver.
- `resourceVersion` empty or `"0"` sends synthetic `ADDED` events for the
  current state, then streams live.
- `allowWatchBookmarks=true` gets periodic `BOOKMARK` frames
  (`k8s_facade.bookmark_interval`).
- `timeoutSeconds` is honored; otherwise watches end with a clean EOF after
  5 minutes and clients re-establish. `sendInitialEvents` /
  `resourceVersionMatch` are not supported (400).
- Only JSON is served (no protobuf) — dynamic/unstructured clients and
  controller-runtime's unstructured mode work as-is.

## Pointing a controller at the facade

The facade sits behind the same JWT auth as the native API. A kubeconfig
looks like:

```yaml
apiVersion: v1
kind: Config
clusters:
  - name: hyperfleet
    cluster:
      server: https://hyperfleet-api.example.com   # API base URL (no path)
      certificate-authority: /etc/hyperfleet/ca.crt
contexts:
  - name: hyperfleet
    context: {cluster: hyperfleet, user: controller}
current-context: hyperfleet
users:
  - name: controller
    user:
      token: <JWT bearer token>
```

With client-go directly:

```go
cfg := &rest.Config{Host: "https://hyperfleet-api.example.com", BearerToken: token}
client, _ := dynamic.NewForConfig(cfg)
gvr := schema.GroupVersionResource{
    Group: "hyperfleet.openshift.io", Version: "v1alpha1", Resource: "clusters",
}
factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(client, 0, metav1.NamespaceAll, nil)
informer := factory.ForResource(gvr)
```

Long-lived watch connections re-authenticate on every re-establishment
(watches end cleanly after at most a few minutes), so normal token rotation
works.

## How it works

Every mutation appends one event (full resource snapshot) to the
`resource_events` table **in the same transaction**, with a `BIGSERIAL` seq
allocated under `pg_advisory_xact_lock` so seq order equals commit-visibility
order. `resources.rv` denormalizes each resource's latest seq. Each API
replica runs a feed that tails the table (poll + `LISTEN/NOTIFY` wake-ups)
and fans events out to watch connections; seq values are global, so
resourceVersions are portable across replicas. A janitor compacts events
older than `retention` (keeping at least `ring_size`), which defines the 410
floor.

## Known limitations

- Read-only: no create/update/delete/patch through the facade.
- No protobuf negotiation, no OpenAPI discovery documents, no
  `sendInitialEvents`, no `limit`/`continue` chunking.
- Two rare selector-transition gaps on watches started from a specific
  `resourceVersion` (not `0`): an object whose labels left the selector
  *before* the watch started, or that was deleted while unmatched, may not
  produce a `DELETED` frame; the client's next relist reconciles.
- HyperFleet label keys/values are looser than Kubernetes label syntax
  (up to 255 chars, arbitrary characters). Clients don't validate on read,
  and selectors still work, but such labels are not round-trippable through
  Kubernetes tooling.
