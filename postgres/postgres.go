// Package postgres provides instrumented database/sql pools with a private pgx driver.
package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/XSAM/otelsql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

// New validates configuration and creates a lazy pool without connecting. Reuse
// the returned *sql.DB and call Close when done; Close also unregisters pool
// metrics. Call PingContext at startup to verify connectivity, and register it
// with health.Required for readiness. Pass contexts to SQL operations to bound
// queries, transactions, and waits for a connection.
func New(cfg Config) (*sql.DB, error) {
	cfg, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}

	connConfig, err := pgx.ParseConfig(cfg.DSN)
	if err != nil {
		// Parser errors can include the entire DSN, including its password.
		return nil, errors.New("postgres: invalid DSN")
	}

	options := []otelsql.Option{
		otelsql.WithAttributes(semconv.DBSystemNamePostgreSQL),
		otelsql.WithSpanOptions(otelsql.SpanOptions{Ping: true, DisableQuery: true, DisableErrSkip: true}),
		otelsql.WithDisableSkipErrMeasurement(true),
	}
	if cfg.TracerProvider != nil {
		options = append(options, otelsql.WithTracerProvider(cfg.TracerProvider))
	}

	if cfg.MeterProvider != nil {
		options = append(options, otelsql.WithMeterProvider(cfg.MeterProvider))
	}

	c := &connector{Connector: stdlib.GetConnector(*connConfig), timeout: cfg.ConnectTimeout}
	db := otelsql.OpenDB(c, options...)
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)

	registration, err := otelsql.RegisterDBStatsMetrics(db, options...)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("postgres: register pool metrics: %w", err), db.Close())
	}

	c.registration = registration

	return db, nil
}

// database/sql closes its connector when DB.Close is called; otelsql forwards
// that call so pool metric callbacks have exactly the same lifetime as the pool.
type connector struct {
	driver.Connector
	timeout      time.Duration
	registration metric.Registration
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	return c.Connector.Connect(ctx)
}

func (c *connector) Close() error {
	if c.registration != nil {
		return c.registration.Unregister()
	}

	return nil
}
