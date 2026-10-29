package foundationfx

import (
	"github.com/Penthious/go-foundation/logger"
	"github.com/Penthious/go-foundation/pubsub"

	"go.uber.org/fx"
)

type pubsubParams struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *logger.Logger
	Config    pubsub.Config `optional:"true"`
}

func newPubSub(p pubsubParams) (*pubsub.Client, error) {
	cfg := p.Config
	if cfg.Logger == nil {
		cfg.Logger = p.Logger
	}

	client, err := pubsub.New(cfg)
	if err != nil {
		return nil, err
	}

	p.Lifecycle.Append(fx.Hook{OnStart: client.Start, OnStop: client.Shutdown})

	return client, nil
}
