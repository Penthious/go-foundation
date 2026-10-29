package postgres_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/health"
	"github.com/Penthious/go-foundation/postgres"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestNew_Config(t *testing.T) {
	for _, tt := range []struct {
		name string
		cfg  postgres.Config
		max  int
	}{
		{name: "URL defaults", cfg: postgres.Config{DSN: "postgres://localhost/example"}, max: 25},
		{name: "keyword DSN", cfg: postgres.Config{DSN: "host=localhost dbname=example"}, max: 25},
		{name: "small pool", cfg: postgres.Config{DSN: "postgres://localhost/example", MaxOpenConns: 1}, max: 1},
		{name: "explicit pool", cfg: postgres.Config{
			DSN: "postgres://localhost/example", MaxOpenConns: 8, MaxIdleConns: 3,
			ConnMaxLifetime: time.Hour, ConnMaxIdleTime: time.Minute, ConnectTimeout: time.Second,
		}, max: 8},
		{name: "no idle connections", cfg: postgres.Config{DSN: "postgres://localhost/example", MaxIdleConns: -1}, max: 25},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var db *sql.DB
			db, err := postgres.New(tt.cfg)
			if err != nil {
				t.Fatal(err)
			}

			t.Cleanup(func() { _ = db.Close() })
			stats := db.Stats()
			if stats.MaxOpenConnections != tt.max || stats.OpenConnections != 0 {
				t.Fatalf("unexpected lazy pool: %+v", stats)
			}
		})
	}
}

func TestNew_InvalidConfig(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*postgres.Config)
	}{
		{name: "missing DSN", change: func(c *postgres.Config) { c.DSN = "" }},
		{name: "blank DSN", change: func(c *postgres.Config) { c.DSN = "  " }},
		{name: "invalid secret DSN", change: func(c *postgres.Config) {
			c.DSN = "postgres://user:secret-password@localhost/db?connect_timeout=invalid"
		}},
		{name: "negative open limit", change: func(c *postgres.Config) { c.MaxOpenConns = -1 }},
		{name: "negative idle limit", change: func(c *postgres.Config) { c.MaxIdleConns = -2 }},
		{name: "idle exceeds open", change: func(c *postgres.Config) { c.MaxOpenConns, c.MaxIdleConns = 1, 2 }},
		{name: "negative lifetime", change: func(c *postgres.Config) { c.ConnMaxLifetime = -time.Second }},
		{name: "negative idle time", change: func(c *postgres.Config) { c.ConnMaxIdleTime = -time.Second }},
		{name: "negative connect timeout", change: func(c *postgres.Config) { c.ConnectTimeout = -time.Second }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := postgres.Config{DSN: "postgres://localhost/example"}
			tt.change(&cfg)

			db, err := postgres.New(cfg)
			if err == nil || db != nil {
				if db != nil {
					_ = db.Close()
				}

				t.Fatal("invalid config accepted")
			}

			if strings.Contains(err.Error(), "secret-password") {
				t.Fatal("error exposed a DSN password")
			}
		})
	}
}

func TestNew_CloseUnregistersPoolMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	db, err := postgres.New(postgres.Config{DSN: "postgres://localhost/example", MeterProvider: provider})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = db.Close() })
	if value, ok := poolLimit(t, reader); !ok || value != 25 {
		t.Fatalf("pool metric = %d, found = %v", value, ok)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if _, ok := poolLimit(t, reader); ok {
		t.Fatal("closed pool still has a metric callback")
	}
}

func TestNew_ConnectTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}

		close(accepted)
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		if conn := <-accepted; conn != nil {
			_ = conn.Close()
		}
	})
	db, err := postgres.New(postgres.Config{
		DSN:            "postgres://foundation@" + listener.Addr().String() + "/example?sslmode=disable",
		ConnectTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err = db.PingContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		t.Fatalf("connection timeout did not bound the stalled handshake: %v", err)
	}
}

func TestNew_Integration(t *testing.T) {
	dsn := os.Getenv("GO_FOUNDATION_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set GO_FOUNDATION_POSTGRES_TEST_DSN to test against PostgreSQL")
	}

	t.Run("queries transactions and health", func(t *testing.T) {
		db := integrationDB(t, postgres.Config{DSN: dsn})
		conn, err := db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}

		defer conn.Close()
		if _, err := conn.ExecContext(t.Context(), "CREATE TEMP TABLE foundation_items (id integer PRIMARY KEY, name text)"); err != nil {
			t.Fatal(err)
		}

		for _, operation := range []struct {
			id     int
			commit bool
		}{{id: 1, commit: true}, {id: 2, commit: false}} {
			tx, err := conn.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}

			defer tx.Rollback()
			if _, err := tx.ExecContext(t.Context(), "INSERT INTO foundation_items VALUES ($1, $2)", operation.id, "order"); err != nil {
				t.Fatal(err)
			}

			if operation.commit {
				err = tx.Commit()
			} else {
				err = tx.Rollback()
			}

			if err != nil {
				t.Fatal(err)
			}
		}

		stmt, err := conn.PrepareContext(t.Context(), "SELECT count(*) FROM foundation_items WHERE name = $1")
		if err != nil {
			t.Fatal(err)
		}

		defer stmt.Close()
		var count int
		if err := stmt.QueryRowContext(t.Context(), "order").Scan(&count); err != nil {
			t.Fatal(err)
		}

		if count != 1 {
			t.Fatalf("commit/rollback persisted %d rows", count)
		}

		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}

		checks, err := health.New(health.Required("postgres", db.PingContext))
		if err != nil {
			t.Fatal(err)
		}

		if !checks.Readiness(t.Context()).Ready {
			t.Fatal("healthy database failed readiness")
		}

		if err := db.Close(); err != nil {
			t.Fatal(err)
		}

		if checks.Readiness(t.Context()).Ready {
			t.Fatal("closed database passed readiness")
		}
	})

	t.Run("pool reuse limits and cancellation", func(t *testing.T) {
		db := integrationDB(t, postgres.Config{DSN: dsn, MaxOpenConns: 1, MaxIdleConns: 1})
		conn, err := db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}

		defer conn.Close()
		var firstPID, secondPID int
		if err := conn.QueryRowContext(t.Context(), "SELECT pg_backend_pid()").Scan(&firstPID); err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()

		if err := db.PingContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("pool wait ignored its context: %v", err)
		}

		if stats := db.Stats(); stats.OpenConnections != 1 || stats.WaitCount == 0 {
			t.Fatalf("pool limit not enforced: %+v", stats)
		}

		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}

		if err := db.QueryRowContext(t.Context(), "SELECT pg_backend_pid()").Scan(&secondPID); err != nil {
			t.Fatal(err)
		}

		if firstPID != secondPID || db.Stats().Idle != 1 {
			t.Fatal("idle connection was not reused")
		}

		ctx, cancelQuery := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancelQuery()

		if _, err := db.ExecContext(ctx, "SELECT pg_sleep(10)"); err == nil || ctx.Err() == nil {
			t.Fatalf("query ignored cancellation: %v", err)
		}
	})

	t.Run("disabled idle pooling", func(t *testing.T) {
		db := integrationDB(t, postgres.Config{DSN: dsn, MaxIdleConns: -1})
		if stats := db.Stats(); stats.OpenConnections != 0 || stats.Idle != 0 {
			t.Fatalf("disabled idle pool retained connections: %+v", stats)
		}
	})

	t.Run("traces and metrics", func(t *testing.T) {
		recorder := tracetest.NewSpanRecorder()
		traces := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
		reader := sdkmetric.NewManualReader()
		metrics := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		t.Cleanup(func() {
			_ = traces.Shutdown(context.Background())
			_ = metrics.Shutdown(context.Background())
		})
		db := integrationDB(t, postgres.Config{DSN: dsn, TracerProvider: traces, MeterProvider: metrics})
		ctx, parent := traces.Tracer("test").Start(t.Context(), "service operation")
		var value string
		if err := db.QueryRowContext(ctx, "SELECT $1::text /* secret-query-text */", "secret-parameter").Scan(&value); err != nil {
			t.Fatal(err)
		}

		if _, err := db.ExecContext(ctx, "SELECT 1 / 0"); err == nil {
			t.Fatal("expected SQL failure")
		}

		parent.End()
		foundChild, foundFailure := false, false
		for _, span := range recorder.Ended() {
			if span.Parent().SpanID() == parent.SpanContext().SpanID() {
				foundChild = true
			}

			if span.Status().Code == codes.Error {
				foundFailure = true
			}

			encoded, err := json.Marshal(span.Attributes())
			if err != nil {
				t.Fatal(err)
			}

			if strings.Contains(string(encoded), "secret-query-text") || strings.Contains(string(encoded), "secret-parameter") || strings.Contains(string(encoded), "db.query.text") {
				t.Fatal("SQL text or parameters were recorded as span attributes")
			}
		}

		if !foundChild || !foundFailure {
			t.Fatalf("child span = %v, failure span = %v", foundChild, foundFailure)
		}

		var data metricdata.ResourceMetrics
		if err := reader.Collect(t.Context(), &data); err != nil {
			t.Fatal(err)
		}

		foundDuration := false
		for _, scope := range data.ScopeMetrics {
			for _, measurement := range scope.Metrics {
				if measurement.Name == "db.client.operation.duration" {
					histogram := measurement.Data.(metricdata.Histogram[float64])
					for _, point := range histogram.DataPoints {
						if system, ok := point.Attributes.Value(attribute.Key("db.system.name")); ok && system.AsString() == "postgresql" && point.Count > 0 {
							foundDuration = true
						}
					}
				}
			}
		}

		if !foundDuration {
			t.Fatal("database operation duration metric missing")
		}
	})
}

func integrationDB(t *testing.T, cfg postgres.Config) *sql.DB {
	t.Helper()

	db, err := postgres.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	return db
}

func poolLimit(t *testing.T, reader *sdkmetric.ManualReader) (int64, bool) {
	t.Helper()

	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}

	for _, scope := range data.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			if measurement.Name == "db.sql.connection.max_open" {
				gauge := measurement.Data.(metricdata.Gauge[int64])
				if len(gauge.DataPoints) > 0 {
					return gauge.DataPoints[0].Value, true
				}
			}
		}
	}

	return 0, false
}
