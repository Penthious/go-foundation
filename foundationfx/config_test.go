package foundationfx_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Penthious/go-foundation/buildinfo"
	"github.com/Penthious/go-foundation/config"
	"github.com/Penthious/go-foundation/foundationfx"
	"github.com/Penthious/go-foundation/logger"

	"go.uber.org/fx"
)

type serviceConfig struct {
	Service     string `env:"SERVICE,required,notEmpty"`
	Environment string `env:"ENVIRONMENT" envDefault:"local"`
	Port        int    `env:"PORT" envDefault:"8080"`
	Validations int    `env:"-"`
}

func (c *serviceConfig) Validate() error {
	c.Validations++
	if c.Port <= 0 || c.Port > 65535 {
		return errors.New("port must be between 1 and 65535")
	}

	return nil
}

func TestConfig_LoadsAndValidatesOnce(t *testing.T) {
	var first, second serviceConfig
	app := fx.New(
		foundationfx.Config[serviceConfig](config.Options{Prefix: "ORDERS_", Environment: map[string]string{"ORDERS_SERVICE": "orders"}}),
		fx.NopLogger,
		fx.Populate(&first, &second),
	)
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}

	if first != second || first.Service != "orders" || first.Port != 8080 || first.Environment != "local" || first.Validations != 1 {
		t.Errorf("config was not shared or validated exactly once: %+v %+v", first, second)
	}
}

func TestConfig_InvalidConfigFailsBeforeLifecycleStartup(t *testing.T) {
	for _, environment := range []map[string]string{
		{},
		{"SERVICE": "orders", "PORT": "private-value"},
		{"SERVICE": "orders", "PORT": "0"},
	} {
		started := false
		app := fx.New(
			foundationfx.Config[serviceConfig](config.Options{Environment: environment}),
			fx.NopLogger,
			fx.Invoke(func(lifecycle fx.Lifecycle) {
				lifecycle.Append(fx.Hook{OnStart: func(context.Context) error {
					started = true
					return nil
				}})
			}),
		)

		if app.Err() == nil || strings.Contains(app.Err().Error(), "private-value") {
			t.Fatalf("configuration was accepted or its value leaked: %v", app.Err())
		}

		if err := app.Start(t.Context()); err == nil || started {
			t.Error("lifecycle started with invalid config")
		}
	}
}

func TestConfig_ComposesWithLoggerAndBuildMetadata(t *testing.T) {
	var output bytes.Buffer
	var log *logger.Logger
	app := fx.New(
		foundationfx.Module,
		foundationfx.Config[serviceConfig](config.Options{Environment: map[string]string{"SERVICE": "orders", "ENVIRONMENT": "staging"}}),
		fx.NopLogger,
		fx.Provide(func(cfg serviceConfig, build buildinfo.Info) logger.Config {
			return logger.Config{Service: cfg.Service, Environment: cfg.Environment, Version: build.Version, Output: &output}
		}),
		fx.Populate(&log),
	)
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}

	log.Info(t.Context(), "service started")

	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}

	if record["service"] != "orders" || record["environment"] != "staging" || record["version"] != buildinfo.Current().Version {
		t.Errorf("runtime and build metadata were not wired into the logger: %v", record)
	}
}
