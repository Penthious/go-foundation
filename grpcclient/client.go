// Package grpcclient supplies reusable channels for generated gRPC clients, with
// GOF telemetry, credential metadata, deadlines and connection ownership.
package grpcclient

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Penthious/go-foundation/internal/circuitbreaker"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ErrClosed is returned by operations begun after Close. It is a gRPC
// Unavailable status and can also be inspected with errors.Is.
var ErrClosed = status.Error(codes.Unavailable, "grpcclient: client is closed")

// Client implements the interface generated protobuf clients require. The native
// connection is private. Reuse one Client per upstream and close it after callers
// stop; concurrent RPCs and Close are supported. Its zero value is not usable.
type Client struct {
	cfg        Config
	connection *grpc.ClientConn
	lifetime   context.Context
	cancel     context.CancelFunc
	closed     atomic.Bool
	closeOnce  sync.Once
	closeErr   error
	breaker    *circuitbreaker.Breaker
}

var _ grpc.ClientConnInterface = (*Client)(nil)

// New creates a lazy gRPC channel without network I/O. Initialize telemetry
// before New, then pass the result to generated NewXClient functions. TLS is
// verified by default; plaintext requires Insecure. No application retries or
// hedging are enabled, including retries supplied by resolver service config.
func New(cfg Config) (*Client, error) {
	cfg, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}

	var breaker *circuitbreaker.Breaker
	if cfg.CircuitBreaker != nil {
		breaker, err = circuitbreaker.New(circuitbreaker.Config{
			FailureThreshold: cfg.CircuitBreaker.FailureThreshold, OpenTimeout: cfg.CircuitBreaker.OpenTimeout,
			Name: cfg.Name, Protocol: "grpc", Logger: cfg.Logger,
		})
		if err != nil {
			return nil, fmt.Errorf("grpcclient: %w", err)
		}
	}

	var transport credentials.TransportCredentials
	if cfg.Insecure {
		transport = insecure.NewCredentials()
	} else {
		transport = credentials.NewTLS(cfg.TLS)
	}

	options := []grpc.DialOption{
		grpc.WithTransportCredentials(transport),
		grpc.WithStatsHandler(newStats(cfg.Logger, cfg.Target)),
		grpc.WithDisableRetry(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(cfg.MaxRecvMessageBytes), grpc.MaxCallSendMsgSize(cfg.MaxSendMessageBytes)),
	}
	if cfg.Dialer != nil {
		options = append(options, grpc.WithContextDialer(cfg.Dialer))
	}

	connection, err := grpc.NewClient(cfg.Target, options...)
	if err != nil {
		return nil, fmt.Errorf("grpcclient: create channel: %w", err)
	}

	lifetime, cancel := context.WithCancel(context.Background())

	return &Client{cfg: cfg, connection: connection, lifetime: lifetime, cancel: cancel, breaker: breaker}, nil
}

// Invoke implements grpc.ClientConnInterface for generated unary methods. It
// preserves gRPC statuses and caller options while applying a bounded deadline.
func (c *Client) Invoke(ctx context.Context, method string, request, response any, opts ...grpc.CallOption) error {
	ctx, finish := c.callContext(ctx, c.cfg.Timeout)
	defer finish()

	if err := c.contextError(ctx); err != nil {
		return err
	}

	permit, err := c.allowCall(ctx)
	if err != nil {
		return err
	}

	defer permit.Done(circuitbreaker.Ignored)

	ctx, err = c.prepare(ctx, method)
	if err != nil {
		return err
	}

	err = c.connection.Invoke(ctx, method, request, response, opts...)
	c.recordCall(permit, err)

	return err
}

// NewStream implements grpc.ClientConnInterface for every generated stream type.
// StreamTimeout bounds establishment and the complete stream, including blocked
// sends/receives. Callers must receive to completion or cancel their context;
// CloseSend alone does not finish a server/bidirectional response stream.
func (c *Client) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	ctx, finish := c.callContext(ctx, c.cfg.StreamTimeout)
	if err := c.contextError(ctx); err != nil {
		finish()
		return nil, err
	}

	permit, err := c.allowCall(ctx)
	if err != nil {
		finish()
		return nil, err
	}

	established := false
	defer func() {
		if !established {
			permit.Done(circuitbreaker.Ignored)
			finish()
		}
	}()

	ctx, err = c.prepare(ctx, method)
	if err != nil {
		return nil, err
	}

	if desc == nil {
		return nil, status.Error(codes.InvalidArgument, "grpcclient: stream descriptor is required")
	}

	// Use gRPC's completion hook so timers are released on success, remote errors,
	// deadline, cancellation and connection closure, without a goroutine per stream.
	options := append([]grpc.CallOption{}, opts...)
	options = append(options, grpc.OnFinish(func(err error) {
		c.recordCall(permit, err)
		finish()
	}))
	stream, err := c.connection.NewStream(ctx, desc, method, options...)
	if err != nil {
		c.recordCall(permit, err)
	}

	established = err == nil

	return stream, err
}

// Ping waits for a ready transport under Timeout and the caller's deadline. It
// verifies connection/TLS establishment, not upstream application health or RPC
// authorization. It is suitable for an optional health.Checker callback. Native
// shared connection attempts may continue after this wait ends, until Close.
func (c *Client) Ping(ctx context.Context) error {
	ctx, finish := c.callContext(ctx, c.cfg.Timeout)
	defer finish()

	for {
		if c.closed.Load() {
			return ErrClosed
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		state := c.connection.GetState()
		if state == connectivity.Ready {
			return nil
		}

		if state == connectivity.Shutdown {
			return ErrClosed
		}

		if state == connectivity.Idle {
			c.connection.Connect()
		}

		c.connection.WaitForStateChange(ctx, state)
	}
}

// Close is idempotent and closes the owned channel, canceling active RPCs and
// credential lookups. Stop/join application callers first; providers/dialers must
// honor cancellation. Borrowed logger and OTel providers are never shut down.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.cancel()
		c.closeErr = c.connection.Close()
	})

	return c.closeErr
}

func (c *Client) callContext(parent context.Context, timeout time.Duration) (context.Context, func()) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	stop := context.AfterFunc(c.lifetime, cancel)

	return ctx, func() {
		stop()
		cancel()
	}
}
