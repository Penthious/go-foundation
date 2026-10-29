package foundationfx

import (
	"github.com/Penthious/go-foundation/logger"
	"github.com/Penthious/go-foundation/telemetry"

	"go.uber.org/fx"
)

type loggerParams struct {
	fx.In

	Config logger.Config
	SDK    *telemetry.SDK `optional:"true"`
}

func newLogger(p loggerParams) (*logger.Logger, error) {
	cfg := p.Config
	if cfg.LoggerProvider == nil && p.SDK != nil {
		cfg.LoggerProvider = p.SDK.LoggerProvider()
	}

	return logger.New(cfg)
}
