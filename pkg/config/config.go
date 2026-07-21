package config

import "github.com/openshift-hyperfleet/hyperfleet-api/pkg/registry"

// ApplicationConfig holds all application configuration
// Follows HyperFleet Configuration Standard with validation and structured marshaling
type ApplicationConfig struct {
	Server    *ServerConfig               `mapstructure:"server" json:"server" validate:"required"`
	Metrics   *MetricsConfig              `mapstructure:"metrics" json:"metrics" validate:"required"`
	Health    *HealthConfig               `mapstructure:"health" json:"health" validate:"required"`
	Database  *DatabaseConfig             `mapstructure:"database" json:"database" validate:"required"`
	Logging   *LoggingConfig              `mapstructure:"logging" json:"logging" validate:"required"`
	K8sFacade *K8sFacadeConfig            `mapstructure:"k8s_facade" json:"k8s_facade"`
	Entities  []registry.EntityDescriptor `mapstructure:"entities" json:"entities"`
}

// NewApplicationConfig returns default ApplicationConfig with all sub-configs initialized
// These defaults can be overridden by config file, env vars, or CLI flags
func NewApplicationConfig() *ApplicationConfig {
	return &ApplicationConfig{
		Server:    NewServerConfig(),
		Metrics:   NewMetricsConfig(),
		Health:    NewHealthConfig(),
		Database:  NewDatabaseConfig(),
		Logging:   NewLoggingConfig(),
		K8sFacade: NewK8sFacadeConfig(),
	}
}
