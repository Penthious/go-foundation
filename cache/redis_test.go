package cache

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

func TestClient_RedisSharingIsolationAndTTL(t *testing.T) {
	cfg := testConfig(t, Redis)
	cfg.DefaultTTL = time.Minute
	one, two := testClient(t, cfg), testClient(t, cfg)
	if err := one.Set(t.Context(), "order", []byte("shared"), 0); err != nil {
		t.Fatal(err)
	}

	value, err := two.Get(t.Context(), "order")
	if err != nil || string(value) != "shared" {
		t.Fatal("clients did not share the namespace")
	}

	key, err := one.key(t.Context(), "order")
	if err != nil {
		t.Fatal(err)
	}

	ttl, err := one.redis.PTTL(t.Context(), key).Result()
	if err != nil || ttl <= 0 || ttl > time.Minute {
		t.Fatalf("default TTL not applied: %v, %v", ttl, err)
	}

	if err := two.Set(t.Context(), "order", []byte("updated"), 10*time.Minute); err != nil {
		t.Fatal(err)
	}

	ttl, err = one.redis.PTTL(t.Context(), key).Result()
	if err != nil || ttl < 9*time.Minute || ttl > 10*time.Minute {
		t.Fatalf("Set did not replace TTL: %v, %v", ttl, err)
	}

	isolationCfg := cfg
	isolationCfg.Namespace += "-other"
	isolated := testClient(t, isolationCfg)
	if _, err := isolated.Get(t.Context(), "order"); !errors.Is(err, ErrMiss) {
		t.Fatal("entry leaked into another namespace")
	}

	if err := one.Close(); err != nil {
		t.Fatal(err)
	}

	value, err = two.Get(t.Context(), "order")
	if err != nil || string(value) != "updated" {
		t.Fatal("Close deleted shared Redis data")
	}

	if err := two.Set(t.Context(), "expiring", nil, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		_, err := two.Get(ctx, "expiring")
		if errors.Is(err, ErrMiss) {
			break
		}

		if err != nil {
			t.Fatalf("expiry check failed: %v", err)
		}

		select {
		case <-ctx.Done():
			t.Fatal("Redis entry did not expire within bounded wait")
		case <-ticker.C:
		}
	}
}

func TestClient_RedisExternalValuesAndAuthentication(t *testing.T) {
	cfg := testConfig(t, Redis)
	cfg.MaxValueBytes = 8
	client := testClient(t, cfg)
	key, err := client.key(t.Context(), "sensitive-key")
	if err != nil {
		t.Fatal(err)
	}

	if err := client.redis.Set(t.Context(), key, strings.Repeat("x", 100), time.Minute).Err(); err != nil {
		t.Fatal(err)
	}

	if _, err := client.Get(t.Context(), "sensitive-key"); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("oversized external value accepted: %v", err)
	}

	if err := client.redis.Del(t.Context(), key).Err(); err != nil {
		t.Fatal(err)
	}

	if err := client.redis.HSet(t.Context(), key, "field", "value").Err(); err != nil {
		t.Fatal(err)
	}

	if _, err := client.Get(t.Context(), "sensitive-key"); !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), "sensitive") {
		t.Fatalf("driver error not sanitized: %v", err)
	}

	badCfg := cfg
	badConfig := *cfg.Redis
	badConfig.Password = "incorrect-sensitive-password"
	badCfg.Redis = &badConfig
	bad := testClient(t, badCfg)
	if err := bad.Ping(t.Context()); !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), badConfig.Password) {
		t.Fatalf("authentication error not sanitized: %v", err)
	}
}

func TestClient_RedisLazyConnectionAndDeadline(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	defer listener.Close()
	client := testClient(t, Config{Backend: Redis, Namespace: "deadline", Timeout: time.Second, Redis: &RedisConfig{Address: listener.Addr().String(), PoolSize: 1}})
	if client.redis.PoolStats().TotalConns != 0 {
		t.Fatal("New performed network I/O")
	}

	accepted := make(chan net.Conn, 1)
	go func() {
		connection, err := listener.Accept()
		if err == nil {
			accepted <- connection
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	result := make(chan error, 1)
	go func() { result <- client.Ping(ctx) }()
	select {
	case connection := <-accepted:
		defer connection.Close()
	case <-time.After(2 * time.Second):
		t.Fatal("Redis client did not connect")
	}

	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("deadline was not preserved: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Redis I/O exceeded caller deadline")
	}
}
