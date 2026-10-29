package foundationfx_test

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/buildinfo"
	"github.com/Penthious/go-foundation/foundationfx"
	"github.com/Penthious/go-foundation/httpclient"
	"github.com/Penthious/go-foundation/logger"

	"go.uber.org/fx"
)

func TestModule_ConstructorErrors(t *testing.T) {
	tests := []struct {
		name string
		opts []fx.Option
	}{
		{name: "missing logger config"},
		{name: "invalid logger config", opts: []fx.Option{fx.Supply(logger.Config{})}},
		{name: "invalid HTTP config", opts: []fx.Option{
			fx.Supply(logger.Config{Service: "orders", Output: io.Discard}),
			fx.Supply(httpclient.Config{Timeout: -time.Second}),
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var client *http.Client
			opts := []fx.Option{foundationfx.Module, fx.NopLogger, fx.Populate(&client)}
			app := fx.New(append(opts, tt.opts...)...)

			if app.Err() == nil {
				t.Fatal("invalid dependency configuration was accepted")
			}
		})
	}
}

func TestModule_BuildInfo(t *testing.T) {
	var info buildinfo.Info
	app := fx.New(foundationfx.Module, fx.NopLogger, fx.Populate(&info))
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}

	if info != buildinfo.Current() {
		t.Errorf("injected build info = %+v", info)
	}
}

func startApp(t *testing.T, options ...fx.Option) *fx.App {
	t.Helper()

	opts := []fx.Option{foundationfx.Module, fx.NopLogger}
	app := fx.New(append(opts, options...)...)
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if err := app.Start(ctx); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := app.Stop(ctx); err != nil {
			t.Error(err)
		}
	})

	return app
}
