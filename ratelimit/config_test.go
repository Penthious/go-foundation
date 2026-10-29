package ratelimit

import (
	"crypto/tls"
	"strings"
	"testing"
	"time"
)

func TestNormalizeConfig_ValidationAndSnapshots(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*Config)
	}{
		{"logger", func(c *Config) { c.Logger = nil }},
		{"namespace", func(c *Config) { c.Namespace = "" }},
		{"namespace characters", func(c *Config) { c.Namespace = "private/secret" }},
		{"namespace length", func(c *Config) { c.Namespace = strings.Repeat("x", 129) }},
		{"backend", func(c *Config) { c.Backend = "other" }},
		{"limit", func(c *Config) { c.Limit = -1 }},
		{"window", func(c *Config) { c.Window = -time.Second }},
		{"small window", func(c *Config) { c.Window = time.Microsecond }},
		{"large window", func(c *Config) { c.Window = 25 * time.Hour }},
		{"fractional microsecond", func(c *Config) { c.Window = time.Second + time.Nanosecond }},
		{"excessive rate", func(c *Config) { c.Window, c.Limit = time.Millisecond, 1001 }},
		{"burst", func(c *Config) { c.Burst = -1 }},
		{"burst debt", func(c *Config) { c.Limit, c.Window, c.Burst = 1, 24*time.Hour, 2 }},
		{"capacity", func(c *Config) { c.MaxKeys = -1 }},
		{"timeout", func(c *Config) { c.Timeout = -time.Second }},
		{"Redis for memory", func(c *Config) { c.Redis = &RedisConfig{} }},
		{"missing Redis", func(c *Config) { c.Backend = Redis }},
		{"Redis URL", func(c *Config) {
			c.Backend, c.Redis = Redis, &RedisConfig{Address: "redis://user:private-secret@host:6379"}
		}},
		{"Redis credentials", func(c *Config) { c.Backend, c.Redis = Redis, &RedisConfig{Address: "private-secret@host:6379"} }},
		{"Redis port", func(c *Config) { c.Backend, c.Redis = Redis, &RedisConfig{Address: "host:0"} }},
		{"Redis host", func(c *Config) { c.Backend, c.Redis = Redis, &RedisConfig{Address: ":6379"} }},
		{"Redis DB", func(c *Config) { c.Backend, c.Redis = Redis, &RedisConfig{DB: -1} }},
		{"Redis pool", func(c *Config) { c.Backend, c.Redis = Redis, &RedisConfig{PoolSize: -1} }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(t, Memory)
			tt.change(&cfg)
			_, _, err := normalizeConfig(cfg)

			if err == nil || strings.Contains(err.Error(), "private-secret") {
				t.Fatal("invalid configuration accepted or leaked:", err)
			}
		})
	}

	cfg := testConfig(t, Memory)
	cfg.Backend, cfg.Redis = Redis, &RedisConfig{TLS: &tls.Config{ServerName: "redis.internal"}}
	normalized, interval, err := normalizeConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if interval != 10*time.Millisecond || normalized.Limit != 100 || normalized.Burst != 100 || normalized.MaxKeys != 10_000 || normalized.Timeout != 2*time.Second || normalized.Redis.Address != "localhost:6379" || normalized.Redis.PoolSize != 10 {
		t.Fatal("unexpected defaults")
	}

	cfg.Redis.TLS.ServerName = "changed"
	if normalized.Redis == cfg.Redis || normalized.Redis.TLS == cfg.Redis.TLS || normalized.Redis.TLS.ServerName != "redis.internal" || normalized.Redis.TLS.MinVersion != tls.VersionTLS12 || cfg.Redis.TLS.MinVersion != 0 {
		t.Fatal("Redis/TLS config was not independently snapshotted")
	}

	cfg = testConfig(t, Memory)
	cfg.Limit = 3
	_, interval, err = normalizeConfig(cfg)
	if err != nil || interval != 333334*time.Microsecond {
		t.Fatal("refill intervals were not conservatively rounded:", interval, err)
	}
}
