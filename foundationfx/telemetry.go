package foundationfx

import (
	"github.com/Penthious/go-foundation/telemetry"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
)

type instruments struct {
	fx.Out

	Tracer trace.Tracer
	Meter  metric.Meter
}

func newInstruments(sdk *telemetry.SDK) instruments {
	return instruments{Tracer: sdk.Tracer, Meter: sdk.Meter}
}
