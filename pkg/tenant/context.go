package tenant

import (
	"context"

	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/api"
)

type contextKey struct{}

// ResolvedTenant holds the tenant dimensions resolved from request headers.
type ResolvedTenant struct {
	// Dimensions maps label keys to their resolved values (e.g., "hyperfleet.io/org" -> "acme").
	Dimensions map[string]string
}

// WithTenant stores resolved tenant information in the request context.
func WithTenant(ctx context.Context, t *ResolvedTenant) context.Context {
	return context.WithValue(ctx, contextKey{}, t)
}

// FromContext retrieves the resolved tenant from the context.
// Returns nil if tenant enforcement is not active for this request.
func FromContext(ctx context.Context) *ResolvedTenant {
	t, _ := ctx.Value(contextKey{}).(*ResolvedTenant)
	return t
}

// LabelFilters returns TSL-compatible label filter expressions for the resolved tenant.
// Each filter is in the form: labels.hyperfleet.io/org='acme'
// These are AND-combined with any user-supplied search filters.
func (t *ResolvedTenant) LabelFilters() []string {
	if t == nil {
		return nil
	}
	filters := make([]string, 0, len(t.Dimensions))
	for labelKey, value := range t.Dimensions {
		// TSL syntax: labels.<key>='<value>'
		filters = append(filters, "labels."+labelKey+"='"+value+"'")
	}
	return filters
}

// Labels returns the tenant dimensions as ResourceLabel entries, suitable for
// injection into a resource's labels on create.
func (t *ResolvedTenant) Labels() []api.ResourceLabel {
	if t == nil {
		return nil
	}
	labels := make([]api.ResourceLabel, 0, len(t.Dimensions))
	for labelKey, value := range t.Dimensions {
		labels = append(labels, api.ResourceLabel{Key: labelKey, Value: value})
	}
	return labels
}
