package cache

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestClient_BackendContract(t *testing.T) {
	for _, backend := range []Backend{Memory, Redis} {
		t.Run(string(backend), func(t *testing.T) {
			cfg := testConfig(t, backend)
			cfg.MaxValueBytes = 16
			client := testClient(t, cfg)
			ctx := t.Context()

			if _, err := client.Get(ctx, "absent"); !errors.Is(err, ErrMiss) {
				t.Fatalf("missing entry: %v", err)
			}

			input := []byte{'b', 0, 255, 'y'}
			want := bytes.Clone(input)
			if err := client.Set(ctx, "binary", input, 0); err != nil {
				t.Fatal(err)
			}

			input[0] = 'x'
			got, err := client.Get(ctx, "binary")
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("binary value = %v, %v", got, err)
			}

			got[0] = 'z'
			got, err = client.Get(ctx, "binary")
			if err != nil || !bytes.Equal(got, want) {
				t.Fatal("Get did not return an independent copy")
			}

			for _, value := range [][]byte{nil, {}} {
				if err := client.Set(ctx, "empty", value, time.Minute); err != nil {
					t.Fatal(err)
				}

				got, err := client.Get(ctx, "empty")
				if err != nil || len(got) != 0 {
					t.Fatalf("empty value was not a hit: %v", err)
				}
			}

			if err := client.Set(ctx, "binary", bytes.Repeat([]byte("x"), 17), time.Minute); !errors.Is(err, ErrValueTooLarge) {
				t.Fatalf("oversized value: %v", err)
			}

			for _, ttl := range []time.Duration{-time.Second, time.Nanosecond} {
				if err := client.Set(ctx, "binary", []byte("changed"), ttl); err == nil {
					t.Fatal("invalid TTL accepted")
				}
			}

			got, err = client.Get(ctx, "binary")
			if err != nil || !bytes.Equal(got, want) {
				t.Fatal("failed Set changed the existing entry")
			}

			for _, key := range []string{"", " ", "secret\x00", "secret\n", string([]byte{255}), strings.Repeat("x", 1025)} {
				if _, err := client.Get(ctx, key); err == nil || strings.Contains(err.Error(), "secret") {
					t.Fatal("invalid key accepted or disclosed")
				}

				if err := client.Set(ctx, key, nil, 0); err == nil {
					t.Fatal("invalid Set key accepted")
				}

				if err := client.Delete(ctx, key); err == nil {
					t.Fatal("invalid Delete key accepted")
				}
			}

			for range 2 {
				if err := client.Delete(ctx, "binary"); err != nil {
					t.Fatal(err)
				}
			}

			if _, err := client.Get(ctx, "binary"); !errors.Is(err, ErrMiss) {
				t.Fatal("deleted entry still present")
			}

			if err := client.Ping(ctx); err != nil {
				t.Fatal(err)
			}

			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if _, err := client.Get(canceled, "empty"); !errors.Is(err, context.Canceled) {
				t.Fatalf("Get cancellation: %v", err)
			}

			if err := client.Set(canceled, "empty", nil, 0); !errors.Is(err, context.Canceled) {
				t.Fatalf("Set cancellation: %v", err)
			}

			if err := client.Delete(canceled, "empty"); !errors.Is(err, context.Canceled) {
				t.Fatalf("Delete cancellation: %v", err)
			}

			if err := client.Ping(canceled); !errors.Is(err, context.Canceled) {
				t.Fatalf("Ping cancellation: %v", err)
			}

			for range 2 {
				if err := client.Close(); err != nil {
					t.Fatal(err)
				}
			}

			if _, err := client.Get(ctx, "empty"); !errors.Is(err, ErrClosed) {
				t.Fatalf("Get after Close: %v", err)
			}

			for _, err := range []error{client.Set(ctx, "empty", nil, 0), client.Delete(ctx, "empty"), client.Ping(ctx)} {
				if !errors.Is(err, ErrClosed) {
					t.Fatalf("operation after Close: %v", err)
				}
			}
		})
	}
}

func TestClient_ConcurrentUseAndClose(t *testing.T) {
	for _, backend := range []Backend{Memory, Redis} {
		t.Run(string(backend), func(t *testing.T) {
			client := testClient(t, testConfig(t, backend))
			var wg sync.WaitGroup
			for range 20 {
				wg.Go(func() {
					key := rand.Text()
					for range 10 {
						if err := client.Set(t.Context(), key, []byte(key), 0); err != nil {
							t.Error(err)
							return
						}

						if _, err := client.Get(t.Context(), key); err != nil {
							t.Error(err)
							return
						}

						if err := client.Delete(t.Context(), key); err != nil {
							t.Error(err)
							return
						}
					}
				})
			}

			wg.Wait()
			for range 20 {
				wg.Go(func() {
					if err := client.Close(); err != nil {
						t.Error(err)
					}
				})
			}

			wg.Wait()
		})
	}
}

func TestClient_OperationsRacingWithClose(t *testing.T) {
	for _, backend := range []Backend{Memory, Redis} {
		t.Run(string(backend), func(t *testing.T) {
			client := testClient(t, testConfig(t, backend))
			started := make(chan struct{})
			result := make(chan error, 1)
			go func() {
				close(started)
				for range 100 {
					err := client.Set(t.Context(), "racing", []byte("value"), 0)
					if err != nil && !errors.Is(err, ErrClosed) {
						result <- err
						return
					}

					_, err = client.Get(t.Context(), "racing")
					if err != nil && !errors.Is(err, ErrClosed) {
						result <- err
						return
					}
				}

				result <- nil
			}()
			<-started

			if err := client.Close(); err != nil {
				t.Fatal(err)
			}

			select {
			case err := <-result:
				if err != nil {
					t.Fatalf("operation racing with Close: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("operations did not terminate after Close")
			}
		})
	}
}

func testConfig(t *testing.T, backend Backend) Config {
	t.Helper()

	cfg := Config{Backend: backend, Namespace: "test-" + rand.Text()}
	if backend == Redis {
		address := os.Getenv("GO_FOUNDATION_REDIS_TEST_ADDRESS")
		if address == "" {
			t.Skip("set GO_FOUNDATION_REDIS_TEST_ADDRESS to test against Redis")
		}

		cfg.Redis = &RedisConfig{Address: address, Password: os.Getenv("GO_FOUNDATION_REDIS_TEST_PASSWORD")}
	}

	return cfg
}

func testClient(t *testing.T, cfg Config) *Client {
	t.Helper()

	client, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = client.Close() })

	return client
}
