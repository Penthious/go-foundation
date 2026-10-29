// Package foundationfx supplies optional Fx adapters for go-foundation.
// Core packages remain usable without Fx.
package foundationfx

import (
	"github.com/Penthious/go-foundation/buildinfo"

	"go.uber.org/fx"
)

// Module provides a *logger.Logger, *http.Client, *pubsub.Client, buildinfo.Info,
// trace.Tracer, and metric.Meter. Pubsub connects only if consumed, during app.Start.
// Supply logger.Config; httpclient.Config is optional. To use telemetry, create
// and supply a *telemetry.SDK before constructing the app. The service owns that
// SDK and must shut it down after stopping the app, including on startup failure.
// Providers are lazy, so applications that only need logging need no SDK.
// HTTP listeners require the separate HTTPServerModule.
var Module = fx.Module("go-foundation",
	fx.Provide(newLogger, newHTTPClient, newInstruments, newPubSub, buildinfo.Current),
)
