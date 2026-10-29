package cache

import (
	"crypto/tls"
	"math"
	"strings"
	"testing"
	"time"
)

func TestNew_ConfigValidationAndDefaults(t *testing.T) {
	for _, tt := range []struct {
		name string
		cfg  Config
	}{
		{"namespace required", Config{}},
		{"namespace length", Config{Namespace: strings.Repeat("x", 129)}},
		{"namespace separator", Config{Namespace: "orders:private"}},
		{"namespace unicode", Config{Namespace: "订单"}},
		{"backend", Config{Namespace: "orders", Backend: "disk"}},
		{"negative timeout", Config{Namespace: "orders", Timeout: -time.Second}},
		{"negative TTL", Config{Namespace: "orders", DefaultTTL: -time.Second}},
		{"tiny TTL", Config{Namespace: "orders", DefaultTTL: time.Nanosecond}},
		{"overflow TTL", Config{Namespace: "orders", DefaultTTL: math.MaxInt64}},
		{"negative value limit", Config{Namespace: "orders", MaxValueBytes: -1}},
		{"large value limit", Config{Namespace: "orders", MaxValueBytes: (512 << 20) + 1}},
		{"negative entries", Config{Namespace: "orders", Memory: MemoryConfig{MaxEntries: -1}}},
		{"negative bytes", Config{Namespace: "orders", Memory: MemoryConfig{MaxBytes: -1}}},
		{"missing Redis config", Config{Namespace: "orders", Backend: Redis}},
		{"unselected Redis", Config{Namespace: "orders", Redis: &RedisConfig{}}},
		{"bad Redis address", Config{Namespace: "orders", Backend: Redis, Redis: &RedisConfig{Address: "redis://user:secret@localhost:6379"}}},
		{"missing Redis host", Config{Namespace: "orders", Backend: Redis, Redis: &RedisConfig{Address: ":6379"}}},
		{"bad Redis port", Config{Namespace: "orders", Backend: Redis, Redis: &RedisConfig{Address: "localhost:0"}}},
		{"bad Redis database", Config{Namespace: "orders", Backend: Redis, Redis: &RedisConfig{DB: -1}}},
		{"negative Redis pool", Config{Namespace: "orders", Backend: Redis, Redis: &RedisConfig{PoolSize: -1}}},
		{"large Redis pool", Config{Namespace: "orders", Backend: Redis, Redis: &RedisConfig{PoolSize: math.MaxInt32 + 1}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, err := New(tt.cfg)

			if err == nil || client != nil || strings.Contains(err.Error(), "secret") {
				t.Fatalf("invalid configuration accepted or disclosed: %v", err)
			}
		})
	}

	client, err := New(Config{Namespace: "orders"})
	if err != nil {
		t.Fatal(err)
	}

	defer client.Close()
	if client.cfg.Backend != Memory || client.cfg.DefaultTTL != 5*time.Minute || client.cfg.Timeout != 2*time.Second || client.cfg.MaxValueBytes != 1<<20 || client.cfg.Memory.MaxEntries != 10_000 || client.cfg.Memory.MaxBytes != 64<<20 {
		t.Fatal("unexpected default limits")
	}
}

func TestNormalizeConfig_RedisCopiesAndTTLPrecision(t *testing.T) {
	input := Config{Namespace: "orders", Backend: Redis, DefaultTTL: 1500 * time.Microsecond, Redis: &RedisConfig{TLS: &tls.Config{ServerName: "cache.internal"}}}
	cfg, err := normalizeConfig(input)
	if err != nil {
		t.Fatal(err)
	}

	input.Redis.Address = "changed:99"
	input.Redis.TLS.ServerName = "changed"
	if cfg.DefaultTTL != 2*time.Millisecond || cfg.Redis.Address != "localhost:6379" || cfg.Redis.PoolSize != 10 || cfg.Redis.TLS.ServerName != "cache.internal" || cfg.Redis.TLS.MinVersion != tls.VersionTLS12 {
		t.Fatal("config copy, Redis defaults or TTL precision are incorrect")
	}
}
