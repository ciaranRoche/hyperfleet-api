package config

import (
	"fmt"
	"time"
)

// K8sFacadeConfig configures the Kubernetes-compatible read facade served at
// /apis/hyperfleet.openshift.io. Disabled by default.
type K8sFacadeConfig struct {
	Enabled bool `mapstructure:"enabled" json:"enabled"`
	// PollInterval is how often the watch-cache feed polls resource_events
	// (LISTEN/NOTIFY only shortens the wait; correctness comes from polling).
	PollInterval time.Duration `mapstructure:"poll_interval" json:"poll_interval"`
	// RingSize is the number of recent events kept per kind for watch replay.
	RingSize int `mapstructure:"ring_size" json:"ring_size"`
	// Retention bounds how long compacted history is kept in resource_events.
	Retention time.Duration `mapstructure:"retention" json:"retention"`
	// BookmarkInterval is the cadence of BOOKMARK events on watches that
	// request them.
	BookmarkInterval time.Duration `mapstructure:"bookmark_interval" json:"bookmark_interval"`
}

func NewK8sFacadeConfig() *K8sFacadeConfig {
	return &K8sFacadeConfig{
		Enabled:          false,
		PollInterval:     time.Second,
		RingSize:         4096,
		Retention:        time.Hour,
		BookmarkInterval: time.Minute,
	}
}

func (c *K8sFacadeConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.PollInterval < 50*time.Millisecond {
		return fmt.Errorf("k8s_facade.poll_interval must be at least 50ms, got %v", c.PollInterval)
	}
	if c.RingSize < 16 {
		return fmt.Errorf("k8s_facade.ring_size must be at least 16, got %d", c.RingSize)
	}
	if c.Retention < time.Minute {
		return fmt.Errorf("k8s_facade.retention must be at least 1 minute, got %v", c.Retention)
	}
	if c.BookmarkInterval < time.Second {
		return fmt.Errorf("k8s_facade.bookmark_interval must be at least 1 second, got %v", c.BookmarkInterval)
	}
	return nil
}
