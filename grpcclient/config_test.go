package grpcclient

import (
	"context"
	"crypto/tls"
	"strings"
	"testing"
	"time"
)

func TestNew_InvalidConfigAndDefaults(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*Config)
	}{
		{"target", func(c *Config) { c.Target = "" }},
		{"logger", func(c *Config) { c.Logger = nil }},
		{"target credentials", func(c *Config) { c.Target = "dns://user:private-secret@host/service" }},
		{"target query", func(c *Config) { c.Target += "?token=private-secret" }},
		{"target whitespace", func(c *Config) { c.Target = " bad " }},
		{"target control", func(c *Config) { c.Target = "host\nname:9090" }},
		{"bad URI", func(c *Config) { c.Target = "dns://[bad/service" }},
		{"TLS conflict", func(c *Config) { c.TLS = &tls.Config{} }},
		{"unary timeout", func(c *Config) { c.Timeout = -time.Second }},
		{"stream timeout", func(c *Config) { c.StreamTimeout = -time.Second }},
		{"breaker threshold", func(c *Config) { c.CircuitBreaker = &CircuitBreakerConfig{FailureThreshold: -1} }},
		{"breaker timeout", func(c *Config) { c.CircuitBreaker = &CircuitBreakerConfig{OpenTimeout: -time.Second} }},
		{"receive limit", func(c *Config) { c.MaxRecvMessageBytes = -1 }},
		{"send limit", func(c *Config) { c.MaxSendMessageBytes = -1 }},
		{"credential conflict", func(c *Config) { c.APIKey, c.BearerToken = "key", "token" }},
		{"provider conflict", func(c *Config) {
			c.APIKey = "key"
			c.TokenProvider = func(context.Context) (string, error) { return "token", nil }
		}},
		{"invalid key", func(c *Config) { c.APIKey = "private-secret\n" }},
		{"prefixed token", func(c *Config) { c.BearerToken = "Bearer private-secret" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(t, "localhost:9090")
			tt.change(&cfg)
			client, err := New(cfg)

			if err == nil || client != nil || strings.Contains(err.Error(), "private-secret") {
				t.Fatalf("invalid config accepted or leaked: %v", err)
			}
		})
	}

	cfg := testConfig(t, "localhost:9090")
	cfg.Insecure = false
	client := testClient(t, cfg)
	if client.cfg.Timeout != 30*time.Second || client.cfg.StreamTimeout != 5*time.Minute || client.cfg.TLS == nil || client.cfg.TLS.InsecureSkipVerify || client.cfg.TLS.MinVersion != tls.VersionTLS12 || client.cfg.MaxRecvMessageBytes != 4<<20 || client.cfg.MaxSendMessageBytes != 4<<20 {
		t.Fatal("unexpected client defaults")
	}

	cfg.TLS = &tls.Config{ServerName: "orders.internal"}
	copy, err := normalizeConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	cfg.TLS.ServerName = "changed"
	if copy.TLS.ServerName != "orders.internal" || cfg.TLS.MinVersion != 0 {
		t.Fatal("TLS config was not independently cloned")
	}
}
