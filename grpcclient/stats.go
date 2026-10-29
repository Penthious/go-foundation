package grpcclient

import (
	"context"
	"time"

	"github.com/Penthious/go-foundation/logger"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

type rpcStateKey struct{}

type rpcState struct {
	method  string
	started time.Time
}

type rpcStats struct {
	base   stats.Handler
	log    *logger.Logger
	target string
}

func newStats(log *logger.Logger, target string) *rpcStats {
	return &rpcStats{base: otelgrpc.NewClientHandler(), log: log, target: target}
}

func (s *rpcStats) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	ctx = s.base.TagRPC(ctx, info)

	return context.WithValue(ctx, rpcStateKey{}, rpcState{method: info.FullMethodName, started: time.Now()})
}

func (s *rpcStats) HandleRPC(ctx context.Context, event stats.RPCStats) {
	if end, ok := event.(*stats.End); ok {
		state, _ := ctx.Value(rpcStateKey{}).(rpcState)
		s.log.Info(ctx, "grpc client request completed", logger.String("target", s.target), logger.String("rpc_method", state.method), logger.String("status", status.Code(end.Error).String()), logger.Duration("duration", time.Since(state.started)))
	}

	s.base.HandleRPC(ctx, event)
}

func (s *rpcStats) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return s.base.TagConn(ctx, info)
}

func (s *rpcStats) HandleConn(ctx context.Context, event stats.ConnStats) {
	s.base.HandleConn(ctx, event)
}
