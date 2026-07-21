// Package k8sfacade serves a read-only Kubernetes-compatible API
// (discovery, LIST, GET, WATCH) over the generic resource model, so that
// controllers built on client-go/controller-runtime can list and watch
// HyperFleet resources unmodified.
//
// Mapping rules:
//   - Group/version: hyperfleet.openshift.io/v1alpha1.
//   - Kinds with a ParentKind are namespaced; the namespace is the parent
//     resource's ID. Top-level kinds are cluster-scoped.
//   - metadata.name is the native resource name (unique per scope thanks to
//     the partial unique indexes on the resources table); metadata.uid is the
//     native ID, also exposed in the hyperfleet.openshift.io/id annotation.
//   - metadata.resourceVersion is the resource_events seq of the resource's
//     latest event (globally ordered, etcd-revision style).
//   - Soft-deleted resources carry deletionTimestamp plus a synthetic
//     finalizer; hard deletion is the K8s object's disappearance.
package k8sfacade

import (
	"encoding/json"
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/api"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/registry"
)

const (
	// Group is the API group the facade serves.
	Group = "hyperfleet.openshift.io"
	// Version is the facade's sole API version.
	Version = "v1alpha1"
	// GroupVersion is the apiVersion value stamped on served objects.
	GroupVersion = Group + "/" + Version

	// AnnotationID carries the native HyperFleet resource ID.
	AnnotationID = Group + "/id"
	// AnnotationHref carries the native REST href of the resource.
	AnnotationHref = Group + "/href"
	// FinalizerAdapters marks resources awaiting adapter finalization
	// (HyperFleet's soft-delete state) in K8s terms.
	FinalizerAdapters = Group + "/adapters"
)

// IsNamespaced reports whether a kind is served as a namespaced resource
// (child kinds use their parent's ID as namespace).
func IsNamespaced(d registry.EntityDescriptor) bool {
	return d.ParentKind != ""
}

// ToK8sObject converts a native resource into an unstructured Kubernetes
// object of the facade's group/version.
func ToK8sObject(d registry.EntityDescriptor, r *api.Resource) map[string]interface{} {
	metadata := map[string]interface{}{
		"name":              r.Name,
		"uid":               r.ID,
		"resourceVersion":   strconv.FormatInt(r.Rv, 10),
		"generation":        int64(r.Generation),
		"creationTimestamp": metav1.NewTime(r.CreatedTime),
		"annotations": map[string]interface{}{
			AnnotationID:   r.ID,
			AnnotationHref: r.Href,
		},
	}
	if IsNamespaced(d) && r.OwnerID != nil {
		metadata["namespace"] = *r.OwnerID
	}
	if len(r.Labels) > 0 {
		labels := make(map[string]interface{}, len(r.Labels))
		for _, l := range r.Labels {
			labels[l.Key] = l.Value
		}
		metadata["labels"] = labels
	}
	if r.DeletedTime != nil {
		metadata["deletionTimestamp"] = metav1.NewTime(*r.DeletedTime)
		metadata["finalizers"] = []interface{}{FinalizerAdapters}
	}
	if r.OwnerID != nil && r.OwnerKind != nil {
		metadata["ownerReferences"] = []interface{}{
			map[string]interface{}{
				"apiVersion": GroupVersion,
				"kind":       *r.OwnerKind,
				// The parent object's metadata.name is its native name, but the
				// only identifier stored on the child is the parent ID — which
				// is also the parent's uid and the child's namespace.
				"name":       *r.OwnerID,
				"uid":        *r.OwnerID,
				"controller": true,
			},
		}
	}

	obj := map[string]interface{}{
		"apiVersion": GroupVersion,
		"kind":       d.Kind,
		"metadata":   metadata,
	}

	var spec interface{}
	if len(r.Spec) > 0 {
		if err := json.Unmarshal(r.Spec, &spec); err != nil {
			spec = map[string]interface{}{}
		}
	} else {
		spec = map[string]interface{}{}
	}
	obj["spec"] = spec

	if len(r.Conditions) > 0 {
		conditions := make([]interface{}, 0, len(r.Conditions))
		for i := range r.Conditions {
			conditions = append(conditions, toK8sCondition(&r.Conditions[i]))
		}
		obj["status"] = map[string]interface{}{"conditions": conditions}
	} else {
		obj["status"] = map[string]interface{}{}
	}

	return obj
}

// toK8sCondition maps a native condition to the metav1.Condition wire shape.
func toK8sCondition(c *api.ResourceCondition) map[string]interface{} {
	// metav1.Condition requires a non-empty reason; native conditions may
	// omit it.
	reason := "Unknown"
	if c.Reason != nil && *c.Reason != "" {
		reason = *c.Reason
	}
	message := ""
	if c.Message != nil {
		message = *c.Message
	}
	return map[string]interface{}{
		"type":               c.Type,
		"status":             string(c.Status),
		"reason":             reason,
		"message":            message,
		"observedGeneration": int64(c.ObservedGeneration),
		"lastTransitionTime": metav1.NewTime(c.LastTransitionTime),
	}
}

// ToK8sList wraps mapped objects into a K8s List object for the kind.
func ToK8sList(d registry.EntityDescriptor, items []map[string]interface{}, listRv int64) map[string]interface{} {
	if items == nil {
		items = []map[string]interface{}{}
	}
	return map[string]interface{}{
		"apiVersion": GroupVersion,
		"kind":       d.Kind + "List",
		"metadata": map[string]interface{}{
			"resourceVersion": strconv.FormatInt(listRv, 10),
		},
		"items": items,
	}
}

// dedupeByScopeName suppresses soft-deleted "ghost" rows whose (namespace,
// name) has been reclaimed by a live row. Kubernetes clients key objects by
// namespace/name, so at most one object per key may be served; the live row
// wins, otherwise the newest ghost.
func dedupeByScopeName(resources api.ResourceList) api.ResourceList {
	type key struct {
		owner string
		name  string
	}
	best := make(map[key]*api.Resource, len(resources))
	order := make([]key, 0, len(resources))
	for _, r := range resources {
		k := key{name: r.Name}
		if r.OwnerID != nil {
			k.owner = *r.OwnerID
		}
		current, seen := best[k]
		if !seen {
			best[k] = r
			order = append(order, k)
			continue
		}
		if betterListEntry(r, current) {
			best[k] = r
		}
	}
	result := make(api.ResourceList, 0, len(order))
	for _, k := range order {
		result = append(result, best[k])
	}
	return result
}

func betterListEntry(candidate, current *api.Resource) bool {
	candidateLive := candidate.DeletedTime == nil
	currentLive := current.DeletedTime == nil
	if candidateLive != currentLive {
		return candidateLive
	}
	return candidate.CreatedTime.After(current.CreatedTime)
}
