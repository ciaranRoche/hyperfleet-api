package tenant

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/logger"
)

// Middleware extracts tenant identity from request headers (injected by the
// authorization gateway) and stores it in the request context for downstream
// consumption by the service layer.
type Middleware struct {
	config TenantConfig
}

// NewMiddleware creates a tenant enforcement middleware from config.
func NewMiddleware(cfg TenantConfig) *Middleware {
	return &Middleware{config: cfg}
}

// EnforceTenant is the middleware handler. It reads configured headers, rejects
// requests missing required dimensions, and stores the resolved tenant in context.
func (m *Middleware) EnforceTenant(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		resolved := &ResolvedTenant{
			Dimensions: make(map[string]string, len(m.config.Dimensions)),
		}

		for _, dim := range m.config.Dimensions {
			value := strings.TrimSpace(r.Header.Get(dim.Header))
			if value == "" && dim.Required {
				logger.With(ctx,
					"header", dim.Header,
					"label_key", dim.LabelKey,
				).Warn("Missing required tenant header")

				writeForbiddenResponse(w, dim.Header)
				return
			}
			if value != "" {
				resolved.Dimensions[dim.LabelKey] = value
			}
		}

		if len(resolved.Dimensions) > 0 {
			logger.With(ctx, "tenant_dimensions", resolved.Dimensions).
				Debug("Tenant identity resolved from headers")
		}

		ctx = WithTenant(ctx, resolved)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// writeForbiddenResponse writes a 403 response in RFC 9457 Problem Details format.
func writeForbiddenResponse(w http.ResponseWriter, header string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(http.StatusForbidden)
	resp := map[string]interface{}{
		"type":   "https://api.hyperfleet.io/errors/forbidden",
		"title":  "Forbidden",
		"status": http.StatusForbidden,
		"detail": "missing required tenant identity: header " + header + " is required",
		"code":   "HYPERFLEET-AUT-004",
	}
	json.NewEncoder(w).Encode(resp) //nolint:errcheck
}

// InjectSearchFilters prepends tenant label filters to the existing search string.
// Called by the service layer before executing LIST queries.
func InjectSearchFilters(ctx context.Context, search string) string {
	t := FromContext(ctx)
	if t == nil {
		return search
	}
	filters := t.LabelFilters()
	if len(filters) == 0 {
		return search
	}

	tenantFilter := strings.Join(filters, " AND ")
	if search == "" {
		return tenantFilter
	}
	return "(" + search + ") AND " + tenantFilter
}
