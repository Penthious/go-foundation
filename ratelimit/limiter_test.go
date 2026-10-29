package ratelimit

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/logger"
)

func TestLimiter_KeysCancellationAndClose(t *testing.T) {
	for _, backend := range []Backend{Memory, Redis} {
		t.Run(string(backend), func(t *testing.T) {
			limiter := testLimiter(t, testConfig(t, backend))
			for _, identity := range []string{"", " ", strings.Repeat("s", 4097)} {
				if _, err := limiter.Allow(t.Context(), identity); !errors.Is(err, ErrInvalidKey) || strings.Contains(err.Error(), strings.Repeat("s", 20)) {
					t.Fatal("invalid identity accepted or leaked:", err)
				}
			}

			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			if _, err := limiter.Allow(ctx, "user"); !errors.Is(err, context.Canceled) {
				t.Fatal("canceled check accepted:", err)
			}

			if err := limiter.Ping(t.Context()); err != nil {
				t.Fatal(err)
			}

			for range 2 {
				if err := limiter.Close(); err != nil {
					t.Fatal(err)
				}
			}

			if _, err := limiter.Allow(t.Context(), "user"); !errors.Is(err, ErrClosed) {
				t.Fatal("closed limiter admitted check:", err)
			}

			if err := limiter.Ping(t.Context()); !errors.Is(err, ErrClosed) {
				t.Fatal("closed limiter reported healthy:", err)
			}
		})
	}
}

func TestLimiter_ConcurrentQuota(t *testing.T) {
	for _, backend := range []Backend{Memory, Redis} {
		t.Run(string(backend), func(t *testing.T) {
			cfg := testConfig(t, backend)
			cfg.Limit, cfg.Window, cfg.Burst, cfg.Timeout = 1, time.Hour, 7, 5*time.Second
			limiter := testLimiter(t, cfg)
			var admitted atomic.Int32
			results := make(chan error, 100)
			var callers sync.WaitGroup
			for range 100 {
				callers.Go(func() {
					decision, err := limiter.Allow(t.Context(), "shared-user")
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

			if admitted.Load() != 7 {
				t.Fatalf("concurrent admissions = %d, want seven", admitted.Load())
			}
		})
	}
}

func TestLimiter_CloseRacesChecks(t *testing.T) {
	for _, backend := range []Backend{Memory, Redis} {
		t.Run(string(backend), func(t *testing.T) {
			limiter := testLimiter(t, testConfig(t, backend))
			var callers sync.WaitGroup
			results := make(chan error, 20)
			for range 20 {
				callers.Go(func() {
					for range 20 {
						_, err := limiter.Allow(t.Context(), "shared-user")
						if err != nil && !errors.Is(err, ErrClosed) {
							results <- err
							return
						}
					}

					results <- nil
				})
			}

			if err := limiter.Close(); err != nil {
				t.Fatal(err)
			}

			callers.Wait()
			close(results)
			for err := range results {
				if err != nil {
					t.Fatal("unexpected close-race error:", err)
				}
			}
		})
	}
}

func testConfig(t *testing.T, backend Backend) Config {
	t.Helper()

	log, err := logger.New(logger.Config{Service: "ratelimit-test", Output: io.Discard})
	if err != nil {
		t.Fatal(err)
	}

	cfg := Config{Backend: backend, Namespace: "test-" + rand.Text(), Logger: log}
	if backend == Redis {
		address := os.Getenv("GO_FOUNDATION_REDIS_TEST_ADDRESS")
		if address == "" {
			t.Skip("set GO_FOUNDATION_REDIS_TEST_ADDRESS to test against Redis")
		}

		cfg.Redis = &RedisConfig{Address: address, Password: os.Getenv("GO_FOUNDATION_REDIS_TEST_PASSWORD")}
	}

	return cfg
}

func testLimiter(t *testing.T, cfg Config) *Limiter {
	t.Helper()

	limiter, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = limiter.Close() })

	return limiter
}
