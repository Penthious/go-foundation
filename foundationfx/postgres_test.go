package foundationfx_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/foundationfx"
	"github.com/Penthious/go-foundation/postgres"

	"go.uber.org/fx"
)

func TestPostgresModule_ConfigurationAndLazyConstruction(t *testing.T) {
	t.Run("unused needs no config", func(t *testing.T) {
		app := fx.New(foundationfx.PostgresModule(), fx.NopLogger)
		if err := app.Err(); err != nil {
			t.Fatal(err)
		}

		if err := app.Start(t.Context()); err != nil {
			t.Fatal(err)
		}

		if err := app.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}
	})

	for _, tt := range []struct {
		name string
		opts []fx.Option
	}{
		{name: "missing config"},
		{name: "invalid config", opts: []fx.Option{fx.Supply(postgres.Config{})}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var db *sql.DB
			opts := []fx.Option{foundationfx.PostgresModule(), fx.NopLogger, fx.Populate(&db)}
			app := fx.New(append(opts, tt.opts...)...)

			if app.Err() == nil || db != nil {
				t.Fatal("missing/invalid configuration was accepted")
			}
		})
	}
}

func TestPostgresModule_GraphFailureClosesPool(t *testing.T) {
	var db *sql.DB
	app := fx.New(
		foundationfx.PostgresModule(), fx.NopLogger,
		fx.Supply(postgres.Config{DSN: "postgres://localhost/unused"}),
		fx.Populate(&db),
		fx.Invoke(func() error {
			return errors.New("service graph failed")
		}),
	)

	if app.Err() == nil || db == nil {
		t.Fatal("expected a graph failure after constructing the pool")
	}

	assertClosedDB(t, db)
}

func TestPostgresModule_EarlierStartupFailureClosesPool(t *testing.T) {
	var db *sql.DB
	app := fx.New(
		foundationfx.PostgresModule(), fx.NopLogger,
		fx.Supply(postgres.Config{DSN: "postgres://localhost/unused"}),
		fx.Invoke(func(lifecycle fx.Lifecycle) {
			lifecycle.Append(fx.Hook{OnStart: func(context.Context) error {
				return errors.New("earlier service startup failed")
			}})
		}),
		fx.Populate(&db),
	)
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}

	if err := app.Start(t.Context()); err == nil {
		t.Fatal("expected startup failure")
	}

	assertClosedDB(t, db)
}

func TestPostgresModule_Integration(t *testing.T) {
	dsn := os.Getenv("GO_FOUNDATION_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set GO_FOUNDATION_POSTGRES_TEST_DSN to test against PostgreSQL")
	}

	t.Run("singleton startup and shutdown", func(t *testing.T) {
		var db, sameDB *sql.DB
		app := fx.New(
			foundationfx.Module, foundationfx.PostgresModule(), fx.NopLogger,
			fx.Supply(postgres.Config{DSN: dsn}),
			fx.Populate(&db, &sameDB),
		)
		if err := app.Err(); err != nil {
			t.Fatal(err)
		}

		if db != sameDB || db.Stats().OpenConnections != 0 {
			t.Fatal("pool is not a lazy singleton")
		}

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		if err := app.Start(ctx); err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() { _ = app.Stop(context.Background()) })
		var value int
		if err := db.QueryRowContext(ctx, "SELECT 42").Scan(&value); err != nil || value != 42 {
			t.Fatalf("query after startup = %d, %v", value, err)
		}

		if err := app.Stop(ctx); err != nil {
			t.Fatal(err)
		}

		assertClosedDB(t, db)
	})

	t.Run("later startup rollback", func(t *testing.T) {
		var db *sql.DB
		app := fx.New(
			foundationfx.PostgresModule(), fx.NopLogger,
			fx.Supply(postgres.Config{DSN: dsn}),
			fx.Populate(&db),
			fx.Invoke(func(lifecycle fx.Lifecycle, db *sql.DB) {
				lifecycle.Append(fx.Hook{OnStart: func(ctx context.Context) error {
					if err := db.PingContext(ctx); err != nil {
						return err
					}

					return errors.New("later service startup failed")
				}})
			}),
		)
		if err := app.Err(); err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		if err := app.Start(ctx); err == nil || !strings.Contains(err.Error(), "later service startup failed") {
			t.Fatalf("expected later startup failure: %v", err)
		}

		assertClosedDB(t, db)
	})

	t.Run("failed database startup", func(t *testing.T) {
		var db *sql.DB
		app := fx.New(
			foundationfx.PostgresModule(), fx.NopLogger,
			fx.Supply(postgres.Config{DSN: dsn, ConnectTimeout: time.Nanosecond}),
			fx.Populate(&db),
		)
		if err := app.Err(); err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		if err := app.Start(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected startup ping deadline: %v", err)
		}

		assertClosedDB(t, db)
	})
}

func assertClosedDB(t *testing.T, db *sql.DB) {
	t.Helper()

	if err := db.PingContext(t.Context()); err == nil || !strings.Contains(err.Error(), "database is closed") {
		t.Fatalf("pool was not closed: %v", err)
	}
}
