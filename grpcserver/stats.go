package grpcserver

import (
	"context"
	"time"

	"github.com/Penthious/go-foundation/logger"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

type rpcStateKey struct{}

type rpcState struct {
	method  string
	started time.Time
}

type rpcStats struct {
	base    stats.Handler
	log     *logger.Logger
	methods map[string]bool
}

func newStats(log *logger.Logger, methods map[string]bool) *rpcStats {
	return &rpcStats{base: otelgrpc.NewServerHandler(otelgrpc.WithFilter(func(info *stats.RPCTagInfo) bool {
		return methods[info.FullMethodName]
	})), log: log, methods: methods}
}

func (s *rpcStats) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	ctx = s.base.TagRPC(ctx, info)
	method := info.FullMethodName
	if !s.methods[method] {
		method = "unknown"
	}

	return context.WithValue(ctx, rpcStateKey{}, rpcState{method: method, started: time.Now()})
}

func (s *rpcStats) HandleRPC(ctx context.Context, event stats.RPCStats) {
	if end, ok := event.(*stats.End); ok {
		state, _ := ctx.Value(rpcStateKey{}).(rpcState)
		code := status.Code(end.Error)
		fields := []logger.Field{logger.String("rpc_method", state.method), logger.String("status", code.String()), logger.Duration("duration", time.Since(state.started))}
		if code == codes.Internal || code == codes.Unknown || code == codes.DataLoss {
			s.log.Error(ctx, "grpc request completed", fields...)
		} else {
			s.log.Info(ctx, "grpc request completed", fields...)
		}
	}

	s.base.HandleRPC(ctx, event)
}

func (s *rpcStats) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return s.base.TagConn(ctx, info)
}

func (s *rpcStats) HandleConn(ctx context.Context, event stats.ConnStats) {
	s.base.HandleConn(ctx, event)
}
