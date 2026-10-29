package foundationfx

import (
	"context"
	"errors"
	"fmt"

	"github.com/Penthious/go-foundation/logger"
	"github.com/Penthious/go-foundation/ratelimit"

	"go.uber.org/fx"
)

// RateLimitModule provides a lazy limiter, injecting the app logger unless the
// required ratelimit.Config supplies one. It checks health on startup and closes
// on shutdown, graph errors or startup rollback. Call afresh once per app.
func RateLimitModule() fx.Option {
	owner := &rateLimitOwner{}

	return fx.Options(
		fx.ErrorHook(owner),
		fx.Module("go-foundation/ratelimit",
			fx.Invoke(func(lifecycle fx.Lifecycle) {
				lifecycle.Append(fx.Hook{OnStop: owner.close})
			}),
			fx.Provide(owner.new),
		),
	)
}

type rateLimitParams struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *logger.Logger
	Config    ratelimit.Config
}

type rateLimitOwner struct {
	limiter *ratelimit.Limiter
}

func (o *rateLimitOwner) new(p rateLimitParams) (*ratelimit.Limiter, error) {
	cfg := p.Config
	if cfg.Logger == nil {
		cfg.Logger = p.Logger
	}

	limiter, err := ratelimit.New(cfg)
	if err != nil {
		return nil, err
	}

	o.limiter = limiter
	p.Lifecycle.Append(fx.Hook{OnStart: o.start, OnStop: o.close})

	return limiter, nil
}

func (o *rateLimitOwner) start(ctx context.Context) error {
	if err := o.limiter.Ping(ctx); err != nil {
		return errors.Join(fmt.Errorf("ratelimit: startup ping: %w", err), o.limiter.Close())
	}

	return nil
}

func (o *rateLimitOwner) close(context.Context) error {
	if o.limiter == nil {
		return nil
	}

	return o.limiter.Close()
}

func (o *rateLimitOwner) HandleError(error) {
	_ = o.close(context.Background())
}
