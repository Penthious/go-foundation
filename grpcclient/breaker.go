package grpcclient

import (
	"context"
	"errors"
	"time"

	"github.com/Penthious/go-foundation/internal/circuitbreaker"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrCircuitOpen means the client rejected an RPC without making an attempt.
// It is an Unavailable status and can also be inspected with errors.Is.
var ErrCircuitOpen = status.Error(codes.Unavailable, "grpcclient: circuit breaker is open")

// CircuitBreakerConfig enables an independent circuit for this client. Nil
// Config.CircuitBreaker disables it. A non-nil zero config uses the defaults.
type CircuitBreakerConfig struct {
	// FailureThreshold defaults to five consecutive completed failed RPCs.
	// Unavailable, Internal, Unknown, DataLoss and DeadlineExceeded are failures;
	// application statuses reset the count. ResourceExhausted, local preparation
	// errors, cancellations and client closure are ignored.
	FailureThreshold int
	// OpenTimeout defaults to 30s, then permits one concurrent recovery probe.
	// A stream holds the probe until completion, cancellation or its deadline.
	OpenTimeout time.Duration
}

func (c *Client) allowCall(ctx context.Context) (*circuitbreaker.Permit, error) {
	permit, allowed := c.breaker.Allow(ctx)
	if !allowed {
		return nil, ErrCircuitOpen
	}

	return permit, nil
}

func (c *Client) recordCall(permit *circuitbreaker.Permit, err error) {
	if c.closed.Load() || status.Code(err) == codes.Canceled || errors.Is(err, context.Canceled) {
		permit.Done(circuitbreaker.Ignored)
		return
	}

	switch status.Code(err) {
	case codes.ResourceExhausted:
		// Quotas and message limits do not establish upstream unavailability.
		permit.Done(circuitbreaker.Ignored)
	case codes.Unavailable, codes.Internal, codes.Unknown, codes.DataLoss, codes.DeadlineExceeded:
		permit.Done(circuitbreaker.Failure)
	default:
		permit.Done(circuitbreaker.Success)
	}
}
