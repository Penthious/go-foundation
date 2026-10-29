package ratelimit

import (
	"context"
	"crypto/sha256"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRedis_SharedQuotasIsolationAndPolicyMismatch(t *testing.T) {
	cfg := testConfig(t, Redis)
	cfg.Limit, cfg.Window, cfg.Burst = 1, time.Hour, 3
	one, two := testLimiter(t, cfg), testLimiter(t, cfg)
	identity := "sensitive-user-identity"
	for i, limiter := range []*Limiter{one, two} {
		decision, err := limiter.Allow(t.Context(), identity)
		if err != nil || !decision.Allowed || decision.Remaining != 2-i {
			t.Fatal("replicas did not share a quota:", decision, err)
		}
	}

	key := two.redisKey(sha256.Sum256([]byte(identity)))
	if strings.Contains(key, identity) {
		t.Fatal("Redis key exposed raw identity")
	}

	if err := one.Close(); err != nil {
		t.Fatal(err)
	}

	decision, err := two.Allow(t.Context(), identity)
	if err != nil || !decision.Allowed || decision.Remaining != 0 {
		t.Fatal("Close reset the shared quota:", decision, err)
	}

	decision, err = two.Allow(t.Context(), identity)
	if err != nil || decision.Allowed || decision.RetryAfter <= 0 || decision.RetryAfter > time.Hour {
		t.Fatal("shared quota did not deny with refill delay:", decision, err)
	}

	ttl, err := two.redis.PTTL(t.Context(), key).Result()
	if err != nil || ttl <= 2*time.Hour || ttl > 3*time.Hour+time.Millisecond {
		t.Fatal("quota does not expire after a full refill:", ttl, err)
	}

	different := cfg
	different.Burst = 4
	if _, err := testLimiter(t, different).Allow(t.Context(), identity); !errors.Is(err, ErrPolicyMismatch) {
		t.Fatal("mismatched replica policy silently reset/shared the quota:", err)
	}

	isolated := cfg
	isolated.Namespace += "-isolated"
	decision, err = testLimiter(t, isolated).Allow(t.Context(), identity)
	if err != nil || !decision.Allowed || decision.Remaining != 2 {
		t.Fatal("namespaces were not isolated:", decision, err)
	}
}

func TestRedis_ConcurrentReplicas(t *testing.T) {
	cfg := testConfig(t, Redis)
	cfg.Limit, cfg.Window, cfg.Burst, cfg.Timeout = 1, time.Hour, 9, 5*time.Second
	clients := []*Limiter{testLimiter(t, cfg), testLimiter(t, cfg)}
	var admitted atomic.Int32
	var callers sync.WaitGroup
	results := make(chan error, 60)
	for i := range 60 {
		callers.Go(func() {
			decision, err := clients[i%2].Allow(t.Context(), "shared-user")
			if err != nil {
				results <- err
				return
			}

			if decision.Allowed {
				admitted.Add(1)
			}

			results <- nil
		})
	}

	callers.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}

	if admitted.Load() != 9 {
		t.Fatalf("cross-replica admissions = %d, want nine", admitted.Load())
	}
}

func TestRedis_ExpiryAndDenialsDoNotExtendQuota(t *testing.T) {
	cfg := testConfig(t, Redis)
	cfg.Limit, cfg.Window, cfg.Burst = 1, 250*time.Millisecond, 1
	limiter := testLimiter(t, cfg)
	decision, err := limiter.Allow(t.Context(), "expiring")
	if err != nil || !decision.Allowed {
		t.Fatal(err)
	}

	key := limiter.redisKey(sha256.Sum256([]byte("expiring")))
	before, err := limiter.redis.Get(t.Context(), key).Result()
	if err != nil {
		t.Fatal(err)
	}

	decision, err = limiter.Allow(t.Context(), "expiring")
	if err != nil || decision.Allowed || decision.RetryAfter <= 0 {
		t.Fatal("expected quota denial:", decision, err)
	}

	after, err := limiter.redis.Get(t.Context(), key).Result()
	if err != nil || after != before {
		t.Fatal("denied check advanced the quota timestamp:", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		count, err := limiter.redis.Exists(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}

		if count == 0 {
			break
		}

		select {
		case <-ctx.Done():
			t.Fatal("idle quota did not expire")
		case <-ticker.C:
		}
	}

	decision, err = limiter.Allow(t.Context(), "expiring")
	if err != nil || !decision.Allowed {
		t.Fatal("expired quota did not refill:", decision, err)
	}
}

func TestRedis_CorruptStateAndAuthenticationFailClosed(t *testing.T) {
	cfg := testConfig(t, Redis)
	limiter := testLimiter(t, cfg)
	key := limiter.redisKey(sha256.Sum256([]byte("sensitive-user")))
	for _, value := range []string{"", "private-sensitive-value", strings.Repeat("9", 4096), "10000,100,99999999999999999999"} {
		if err := limiter.redis.Set(t.Context(), key, value, time.Minute).Err(); err != nil {
			t.Fatal(err)
		}

		if decision, err := limiter.Allow(t.Context(), "sensitive-user"); decision.Allowed || !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), "sensitive") {
			t.Fatal("corrupt quota was accepted or driver data leaked:", decision, err)
		}
	}

	if err := limiter.redis.Del(t.Context(), key).Err(); err != nil {
		t.Fatal(err)
	}

	if err := limiter.redis.HSet(t.Context(), key, "field", "sensitive-value").Err(); err != nil {
		t.Fatal(err)
	}

	if _, err := limiter.Allow(t.Context(), "sensitive-user"); !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), "sensitive") {
		t.Fatal("wrong-type quota leaked driver error:", err)
	}

	badCfg := cfg
	badRedis := *cfg.Redis
	badRedis.Password = "incorrect-sensitive-password"
	badCfg.Redis = &badRedis
	if err := testLimiter(t, badCfg).Ping(t.Context()); !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), "sensitive") {
		t.Fatal("Redis authentication failure was not sanitized:", err)
	}
}

func TestRedis_LazyConstructionAndDeadline(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	defer listener.Close()

	cfg := testConfig(t, Memory)
	cfg.Backend, cfg.Redis = Redis, &RedisConfig{Address: listener.Addr().String(), PoolSize: 1}
	limiter := testLimiter(t, cfg)
	if limiter.redis.PoolStats().TotalConns != 0 {
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
	go func() {
		_, err := limiter.Allow(ctx, "user")
		result <- err
	}()
	select {
	case connection := <-accepted:
		defer connection.Close()
	case <-time.After(2 * time.Second):
		t.Fatal("Redis client did not connect")
	}

	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("Redis I/O escaped the request deadline:", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Redis check did not terminate")
	}
}
