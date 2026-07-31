package services

import (
	"context"

	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/api"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/errors"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/tenant"
)

// verifyTenantOwnership checks that the resource belongs to the calling tenant.
// Returns a 404 (not 403) to avoid leaking resource existence to other tenants.
// Returns nil if tenant enforcement is not active.
func verifyTenantOwnership(ctx context.Context, kind, id string, resource *api.Resource) *errors.ServiceError {
	t := tenant.FromContext(ctx)
	if t == nil || len(t.Dimensions) == 0 {
		return nil
	}

	// Build a lookup map from the resource's labels.
	labelMap := make(map[string]string, len(resource.Labels))
	for _, l := range resource.Labels {
		labelMap[l.Key] = l.Value
	}

	// Every tenant dimension must match the resource's labels.
	for labelKey, tenantValue := range t.Dimensions {
		if labelMap[labelKey] != tenantValue {
			// Return 404 — do not reveal that the resource exists to another tenant.
			return errors.NotFound("%s with id '%s' not found", kind, id)
		}
	}
	return nil
}

// injectTenantLabels appends tenant labels from the request context to the resource's labels.
// Existing labels are preserved; tenant labels are added (or overwritten if already present).
func injectTenantLabels(ctx context.Context, labels []api.ResourceLabel) []api.ResourceLabel {
	t := tenant.FromContext(ctx)
	if t == nil || len(t.Dimensions) == 0 {
		return labels
	}

	tenantLabels := t.Labels()

	// Build a set of tenant label keys for dedup.
	tenantKeys := make(map[string]bool, len(tenantLabels))
	for _, tl := range tenantLabels {
		tenantKeys[tl.Key] = true
	}

	// Keep all non-tenant labels, then append tenant labels.
	result := make([]api.ResourceLabel, 0, len(labels)+len(tenantLabels))
	for _, l := range labels {
		if !tenantKeys[l.Key] {
			result = append(result, l)
		}
	}
	result = append(result, tenantLabels...)
	return result
}

// preserveTenantLabels ensures tenant labels from the old labels are preserved
// in the new labels after a patch. User-supplied labels are kept, but tenant
// labels are restored from the pre-patch state.
func preserveTenantLabels(ctx context.Context, oldLabels, newLabels []api.ResourceLabel) []api.ResourceLabel {
	t := tenant.FromContext(ctx)
	if t == nil || len(t.Dimensions) == 0 {
		return newLabels
	}

	// Build a set of tenant label keys.
	tenantKeys := make(map[string]bool, len(t.Dimensions))
	for labelKey := range t.Dimensions {
		tenantKeys[labelKey] = true
	}

	// Collect old tenant labels.
	var oldTenantLabels []api.ResourceLabel
	for _, l := range oldLabels {
		if tenantKeys[l.Key] {
			oldTenantLabels = append(oldTenantLabels, l)
		}
	}

	// Remove any tenant labels from the new set (user shouldn't be able to override them).
	result := make([]api.ResourceLabel, 0, len(newLabels)+len(oldTenantLabels))
	for _, l := range newLabels {
		if !tenantKeys[l.Key] {
			result = append(result, l)
		}
	}

	// Re-add the old tenant labels.
	result = append(result, oldTenantLabels...)
	return result
}
