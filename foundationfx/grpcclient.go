package foundationfx

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/Penthious/go-foundation/grpcclient"
	"github.com/Penthious/go-foundation/logger"

	"go.uber.org/fx"
)

// GRPCClientModule provides a lazy *grpcclient.Client from a required Config,
// injecting the application logger. It performs no startup network check and
// closes the channel on shutdown, graph errors or startup rollback. Call once
// per app so its cleanup state is not shared between apps. Use
// NamedGRPCClientModule for additional independently configured upstreams.
func GRPCClientModule() fx.Option {
	return grpcClientModule("")
}

// NamedGRPCClientModule provides a lazy *grpcclient.Client under an Fx value
// name, consuming grpcclient.Config with the same name. Each binding owns its
// channel and cleanup; the application logger remains unnamed/shared unless
// Config.Logger supplies an override. Names must be nonblank without backquotes.
// Call once per name per app, with fresh module options for each app.
func NamedGRPCClientModule(name string) fx.Option {
	if strings.TrimSpace(name) == "" || strings.ContainsRune(name, '`') {
		return fx.Error(errors.New("foundationfx: gRPC client name must be nonblank and cannot contain backquotes"))
	}

	return grpcClientModule(name)
}

func grpcClientModule(name string) fx.Option {
	owner := &grpcClientOwner{name: name}
	moduleName := "go-foundation/grpcclient"
	var provider any = owner.new
	if name != "" {
		tag := "name:" + strconv.Quote(name)
		provider = fx.Annotate(owner.new, fx.ParamTags("", "", tag), fx.ResultTags(tag))
		moduleName += "/" + name
	}

	return fx.Options(
		fx.ErrorHook(owner),
		fx.Module(moduleName,
			fx.Invoke(func(lifecycle fx.Lifecycle) {
				lifecycle.Append(fx.Hook{OnStop: owner.close})
			}),
			fx.Provide(provider),
		),
	)
}

type grpcClientOwner struct {
	client *grpcclient.Client
	name   string
}

func (o *grpcClientOwner) new(lifecycle fx.Lifecycle, log *logger.Logger, cfg grpcclient.Config) (*grpcclient.Client, error) {
	if o.name != "" {
		cfg.Name = o.name
	}

	if cfg.Logger == nil {
		cfg.Logger = log
	}

	client, err := grpcclient.New(cfg)
	if err != nil {
		return nil, err
	}

	o.client = client
	lifecycle.Append(fx.Hook{OnStop: o.close})

	return client, nil
}

func (o *grpcClientOwner) close(context.Context) error {
	if o.client == nil {
		return nil
	}

	return o.client.Close()
}

func (o *grpcClientOwner) HandleError(error) {
	_ = o.close(context.Background())
}
