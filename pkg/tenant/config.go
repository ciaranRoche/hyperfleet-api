package tenant

import "fmt"

// TenantConfig holds tenant enforcement configuration.
// Loaded from YAML config only (complex struct list, not bindable to env vars).
type TenantConfig struct {
	Dimensions []DimensionConfig `mapstructure:"dimensions" json:"dimensions"`
	Enabled    bool              `mapstructure:"enabled" json:"enabled"`
}

// DimensionConfig maps a gateway-injected HTTP header to a resource label key.
// Each deployment defines its own set of dimensions — the tenant model is not
// compiled into application code.
type DimensionConfig struct {
	// Header is the HTTP header name injected by the authorization gateway (e.g., "X-Tenant-Org").
	Header string `mapstructure:"header" json:"header"`
	// LabelKey is the resource label key used for storage and filtering (e.g., "hyperfleet.io/org").
	LabelKey string `mapstructure:"label_key" json:"label_key"`
	// Required means the header must be present and non-empty; requests without it are rejected.
	Required bool `mapstructure:"required" json:"required"`
}

// Validate checks that the config is internally consistent.
func (c *TenantConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if len(c.Dimensions) == 0 {
		return fmt.Errorf("tenant.dimensions requires at least one entry when tenant enforcement is enabled")
	}
	seen := make(map[string]bool)
	for i, dim := range c.Dimensions {
		if dim.Header == "" {
			return fmt.Errorf("tenant.dimensions[%d].header is required", i)
		}
		if dim.LabelKey == "" {
			return fmt.Errorf("tenant.dimensions[%d].label_key is required", i)
		}
		if seen[dim.Header] {
			return fmt.Errorf("tenant.dimensions[%d].header %q is duplicated", i, dim.Header)
		}
		seen[dim.Header] = true
	}
	return nil
}
