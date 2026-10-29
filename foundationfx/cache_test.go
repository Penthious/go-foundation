package foundationfx_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/cache"
	"github.com/Penthious/go-foundation/foundationfx"

	"go.uber.org/fx"
)

func TestCacheModule_LazyAndInvalidConfig(t *testing.T) {
	app := fx.New(foundationfx.CacheModule(), fx.NopLogger)
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}

	if err := app.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := app.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}

	for _, options := range [][]fx.Option{nil, {fx.Supply(cache.Config{})}} {
		var client *cache.Client
		base := []fx.Option{foundationfx.CacheModule(), fx.NopLogger, fx.Populate(&client)}
		app := fx.New(append(base, options...)...)

		if app.Err() == nil || client != nil {
			t.Fatal("missing/invalid cache config accepted")
		}
	}
}

func TestCacheModule_LifecycleAndFailureCleanup(t *testing.T) {
	for _, scenario := range []string{"normal", "graph error", "earlier hook error", "later hook error", "ping error"} {
		t.Run(scenario, func(t *testing.T) {
			var client, same *cache.Client
			cfg := cache.Config{Namespace: "orders"}
			if scenario == "ping error" {
				cfg.Backend = cache.Redis
				cfg.Redis = &cache.RedisConfig{Address: "127.0.0.1:1"}
				cfg.Timeout = 100 * time.Millisecond
			}

			options := []fx.Option{foundationfx.CacheModule(), fx.NopLogger, fx.Supply(cfg)}
			if scenario == "earlier hook error" {
				options = append(options, fx.Invoke(func(lifecycle fx.Lifecycle) {
					lifecycle.Append(fx.Hook{OnStart: func(context.Context) error {
						return errors.New("earlier startup error")
					}})
				}))
			}

			options = append(options, fx.Populate(&client, &same))
			if scenario == "graph error" {
				options = append(options, fx.Invoke(func() error { return errors.New("graph error") }))
			}

			if scenario == "later hook error" {
				options = append(options, fx.Invoke(func(lifecycle fx.Lifecycle) {
					lifecycle.Append(fx.Hook{OnStart: func(context.Context) error {
						return errors.New("later startup error")
					}})
				}))
			}

			app := fx.New(options...)
			if scenario == "graph error" {
				if app.Err() == nil || client == nil || !errors.Is(client.Ping(t.Context()), cache.ErrClosed) {
					t.Fatal("graph failure did not close constructed cache")
				}

				return
			}

			if err := app.Err(); err != nil {
				t.Fatal(err)
			}

			if client != same {
				t.Fatal("cache not shared within app")
			}

			err := app.Start(t.Context())
			if scenario == "normal" {
				if err != nil {
					t.Fatal(err)
				}

				if err := client.Set(t.Context(), "ready", nil, 0); err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("startup failure was accepted")
			}

			if err := app.Stop(t.Context()); err != nil {
				t.Fatal(err)
			}

			if !errors.Is(client.Ping(t.Context()), cache.ErrClosed) {
				t.Fatal("cache not closed on shutdown/startup rollback")
			}
		})
	}
}

func TestCacheModule_RedisStartup(t *testing.T) {
	address := os.Getenv("GO_FOUNDATION_REDIS_TEST_ADDRESS")
	if address == "" {
		t.Skip("set GO_FOUNDATION_REDIS_TEST_ADDRESS to test against Redis")
	}

	var client *cache.Client
	app := fx.New(foundationfx.CacheModule(), fx.NopLogger,
		fx.Supply(cache.Config{Backend: cache.Redis, Namespace: "fx-startup", Redis: &cache.RedisConfig{Address: address, Password: os.Getenv("GO_FOUNDATION_REDIS_TEST_PASSWORD")}}),
		fx.Populate(&client),
	)
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}

	if err := app.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = app.Stop(t.Context()) })
	if err := client.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := app.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}

	if !errors.Is(client.Ping(t.Context()), cache.ErrClosed) {
		t.Fatal("Redis cache not closed by Fx")
	}
}
