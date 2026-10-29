package foundationfx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Penthious/go-foundation/postgres"

	"go.uber.org/fx"
)

// PostgresModule provides a lazy *sql.DB from a required postgres.Config. When
// consumed, it verifies connectivity during startup and closes the pool during
// shutdown or startup rollback. Call once per app to give graph-error cleanup its
// own state. This module works with or without the base Module.
func PostgresModule() fx.Option {
	pool := &postgresPool{}

	return fx.Options(
		fx.ErrorHook(pool),
		fx.Module("go-foundation/postgres",
			fx.Invoke(func(lifecycle fx.Lifecycle) {
				// Cover parent-app hooks that fail before the startup ping runs.
				lifecycle.Append(fx.Hook{OnStop: pool.close})
			}),
			fx.Provide(pool.new),
		),
	)
}

type postgresPool struct {
	db      *sql.DB
	timeout time.Duration
}

func (p *postgresPool) new(lifecycle fx.Lifecycle, cfg postgres.Config) (*sql.DB, error) {
	db, err := postgres.New(cfg)
	if err != nil {
		return nil, err
	}

	p.db, p.timeout = db, cfg.ConnectTimeout
	if p.timeout == 0 {
		p.timeout = 5 * time.Second
	}

	lifecycle.Append(fx.Hook{OnStart: p.start, OnStop: p.close})

	return db, nil
}

func (p *postgresPool) start(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	if err := p.db.PingContext(ctx); err != nil {
		return errors.Join(fmt.Errorf("postgres: startup ping: %w", err), p.db.Close())
	}

	return nil
}

func (p *postgresPool) close(context.Context) error {
	if p.db == nil {
		return nil
	}

	return p.db.Close()
}

func (p *postgresPool) HandleError(error) {
	_ = p.close(context.Background())
}
