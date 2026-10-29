package foundationfx

import (
	"context"
	"database/sql"
	"errors"

	"github.com/Penthious/go-foundation/logger"
	"github.com/Penthious/go-foundation/outbox"
	"github.com/Penthious/go-foundation/pubsub"

	"go.uber.org/fx"
)

// OutboxModule starts a relay automatically. Include Module and PostgresModule,
// configure pubsub topics, and apply outbox.Schema through service migrations.
// The relay stops before its injected database and pubsub dependencies.
var OutboxModule = fx.Module("go-foundation/outbox",
	fx.Provide(newOutbox),
	fx.Invoke(func(*outbox.Relay) {}),
)

type outboxParams struct {
	fx.In

	Lifecycle fx.Lifecycle
	Shutdown  fx.Shutdowner
	DB        *sql.DB
	PubSub    *pubsub.Client
	Logger    *logger.Logger
	Config    outbox.Config `optional:"true"`
}

func newOutbox(p outboxParams) (*outbox.Relay, error) {
	cfg := p.Config
	if cfg.Logger == nil {
		cfg.Logger = p.Logger
	}

	relay, err := outbox.New(p.DB, p.PubSub.Publish, cfg)
	if err != nil {
		return nil, err
	}

	var cancel context.CancelFunc
	var done chan struct{}
	p.Lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if err := relay.Ping(ctx); err != nil {
				return err
			}

			var runCtx context.Context
			runCtx, cancel = context.WithCancel(context.Background())
			done = make(chan struct{})
			go func() {
				defer close(done)

				if err := relay.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
					cfg.Logger.Error(runCtx, "outbox stopped", logger.Err(err))
					if err := p.Shutdown.Shutdown(fx.ExitCode(1)); err != nil {
						cfg.Logger.Error(runCtx, "outbox could not stop app", logger.Err(err))
					}
				}
			}()

			return nil
		},
		OnStop: func(ctx context.Context) error {
			if cancel == nil {
				return nil
			}

			cancel()
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})

	return relay, nil
}
