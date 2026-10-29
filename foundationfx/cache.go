package foundationfx

import (
	"context"
	"errors"
	"fmt"

	"github.com/Penthious/go-foundation/cache"

	"go.uber.org/fx"
)

// CacheModule provides a lazy *cache.Client from a required cache.Config. When
// consumed it checks health on startup and closes on shutdown or graph/startup
// failure. Call once per app so cleanup state is never shared between apps.
func CacheModule() fx.Option {
	owner := &cacheOwner{}

	return fx.Options(
		fx.ErrorHook(owner),
		fx.Module("go-foundation/cache",
			fx.Invoke(func(lifecycle fx.Lifecycle) {
				lifecycle.Append(fx.Hook{OnStop: owner.close})
			}),
			fx.Provide(owner.new),
		),
	)
}

type cacheOwner struct {
	client *cache.Client
}

func (o *cacheOwner) new(lifecycle fx.Lifecycle, cfg cache.Config) (*cache.Client, error) {
	client, err := cache.New(cfg)
	if err != nil {
		return nil, err
	}

	o.client = client
	lifecycle.Append(fx.Hook{OnStart: o.start, OnStop: o.close})

	return client, nil
}

func (o *cacheOwner) start(ctx context.Context) error {
	if err := o.client.Ping(ctx); err != nil {
		return errors.Join(fmt.Errorf("cache: startup ping: %w", err), o.client.Close())
	}

	return nil
}

func (o *cacheOwner) close(context.Context) error {
	if o.client == nil {
		return nil
	}

	return o.client.Close()
}

func (o *cacheOwner) HandleError(error) {
	_ = o.close(context.Background())
}
