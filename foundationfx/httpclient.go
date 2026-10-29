package foundationfx

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/Penthious/go-foundation/httpclient"
	"github.com/Penthious/go-foundation/logger"

	"go.uber.org/fx"
)

// NamedHTTPClientModule provides a lazy *http.Client under an Fx value name,
// consuming httpclient.Config with the same name. Each binding owns its client
// and idle connection cleanup. Config.Logger defaults to the shared app logger.
// Names must be nonblank without backquotes. Call once per name per app, with
// fresh module options for each app. Module continues to provide the unnamed client.
func NamedHTTPClientModule(name string) fx.Option {
	if strings.TrimSpace(name) == "" || strings.ContainsRune(name, '`') {
		return fx.Error(errors.New("foundationfx: HTTP client name must be nonblank and cannot contain backquotes"))
	}

	owner := &httpClientOwner{name: name}
	tag := "name:" + strconv.Quote(name)

	return fx.Options(
		fx.ErrorHook(owner),
		fx.Module("go-foundation/httpclient/"+name,
			fx.Invoke(func(lifecycle fx.Lifecycle) {
				lifecycle.Append(fx.Hook{OnStop: owner.close})
			}),
			fx.Provide(fx.Annotate(owner.new, fx.ParamTags("", "", tag), fx.ResultTags(tag))),
		),
	)
}

type httpClientParams struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *logger.Logger
	Config    httpclient.Config `optional:"true"`
}

func newHTTPClient(p httpClientParams) (*http.Client, error) {
	owner := &httpClientOwner{}

	return owner.new(p.Lifecycle, p.Logger, p.Config)
}

type httpClientOwner struct {
	client *http.Client
	once   sync.Once
	name   string
}

func (o *httpClientOwner) new(lifecycle fx.Lifecycle, log *logger.Logger, cfg httpclient.Config) (*http.Client, error) {
	if o.name != "" {
		cfg.Name = o.name
	}

	if cfg.Logger == nil {
		cfg.Logger = log
	}

	client, err := httpclient.New(cfg)
	if err != nil {
		return nil, err
	}

	o.client = client
	lifecycle.Append(fx.Hook{OnStop: o.close})

	return client, nil
}

func (o *httpClientOwner) close(context.Context) error {
	o.once.Do(func() {
		if o.client != nil {
			o.client.CloseIdleConnections()
		}
	})

	return nil
}

func (o *httpClientOwner) HandleError(error) {
	_ = o.close(context.Background())
}
