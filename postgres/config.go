package postgres

import (
	"errors"
	"strings"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Config configures a PostgreSQL database/sql pool. Zero pool values use defaults.
type Config struct {
	// DSN is required. PostgreSQL URLs and keyword/value connection strings are
	// supported, including sslmode, sslrootcert, and application_name.
	DSN string
	// MaxOpenConns defaults to 25. Each service instance owns its own pool.
	MaxOpenConns int
	// MaxIdleConns defaults to 5, capped at MaxOpenConns. Set -1 to disable idle
	// connections. Positive values above MaxOpenConns are rejected.
	MaxIdleConns int
	// ConnMaxLifetime and ConnMaxIdleTime default to 30m and 5m, respectively.
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
	// ConnectTimeout defaults to 5s and bounds each complete connection attempt.
	// Fx also uses it for the startup ping. Query deadlines belong to callers.
	ConnectTimeout time.Duration
	// Providers default to the global providers installed by telemetry.New.
	// These providers are borrowed and are never shut down by this package.
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
}

func normalizeConfig(cfg Config) (Config, error) {
	if strings.TrimSpace(cfg.DSN) == "" {
		return Config{}, errors.New("postgres: DSN is required")
	}

	if cfg.MaxOpenConns < 0 || cfg.MaxIdleConns < -1 {
		return Config{}, errors.New("postgres: invalid connection limits")
	}

	if cfg.MaxOpenConns == 0 {
		cfg.MaxOpenConns = 25
	}

	if cfg.MaxIdleConns == 0 {
		cfg.MaxIdleConns = min(5, cfg.MaxOpenConns)
	} else if cfg.MaxIdleConns == -1 {
		cfg.MaxIdleConns = 0
	}

	if cfg.MaxIdleConns > cfg.MaxOpenConns {
		return Config{}, errors.New("postgres: MaxIdleConns must not exceed MaxOpenConns")
	}

	durations := []struct {
		value    *time.Duration
		fallback time.Duration
	}{
		{&cfg.ConnMaxLifetime, 30 * time.Minute},
		{&cfg.ConnMaxIdleTime, 5 * time.Minute},
		{&cfg.ConnectTimeout, 5 * time.Second},
	}
	for _, duration := range durations {
		if *duration.value < 0 {
			return Config{}, errors.New("postgres: durations must not be negative")
		}

		if *duration.value == 0 {
			*duration.value = duration.fallback
		}
	}

	return cfg, nil
}
