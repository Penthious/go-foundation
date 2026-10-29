// Package pubsub provides durable messaging without exposing broker client types.
package pubsub

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Penthious/go-foundation/logger"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ErrNotStarted indicates that Start has not completed successfully.
var ErrNotStarted = errors.New("pubsub: client has not started")

// ErrClosed indicates that shutdown has begun; a closed client cannot restart.
var ErrClosed = errors.New("pubsub: client is closed")

// Client owns a broker connection and active operations. It is safe for concurrent
// use. Construct it with New, then call Start before publishing or consuming.
type Client struct {
	cfg     Config
	mu      sync.Mutex
	startMu sync.Mutex
	conn    *nats.Conn
	js      jetstream.JetStream
	ctx     context.Context
	cancel  context.CancelFunc
	closed  bool
	wg      sync.WaitGroup
	done    chan struct{}
}

// New validates and copies config without opening connections or goroutines.
func New(cfg Config) (*Client, error) {
	cfg, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Client{cfg: cfg, ctx: ctx, cancel: cancel, done: make(chan struct{})}, nil
}

// Start connects and provisions configured topics. It is idempotent after
// success. Startup failures close the connection and can be retried.
func (c *Client) Start(ctx context.Context) error {
	c.startMu.Lock()
	defer c.startMu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}

	if c.conn != nil {
		c.mu.Unlock()
		return nil
	}

	c.wg.Add(1)
	c.mu.Unlock()
	defer c.wg.Done()

	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	stop := context.AfterFunc(c.ctx, cancel)
	defer stop()

	conn, err := c.connect(ctx)
	if err != nil {
		return fmt.Errorf("pubsub: connect: %w", err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return fmt.Errorf("pubsub: initialize messaging: %w", err)
	}

	if _, err := js.AccountInfo(ctx); err != nil {
		conn.Close()
		return fmt.Errorf("pubsub: durable messaging unavailable: %w", err)
	}

	for _, topic := range c.cfg.Topics {
		if err := createTopic(ctx, js, topic); err != nil {
			conn.Close()
			return err
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		conn.Close()
		return ErrClosed
	}

	c.conn, c.js = conn, js
	c.cfg.Logger.Info(ctx, "pubsub connected")

	return nil
}

// Ping verifies that durable messaging is reachable with a read-only broker
// round trip. It publishes no messages and uses the caller's deadline or Timeout.
func (c *Client) Ping(ctx context.Context) error {
	ctx, js, finish, err := c.operation(ctx)
	if err != nil {
		return err
	}

	defer finish()
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	if _, err := js.AccountInfo(ctx); err != nil {
		return fmt.Errorf("pubsub: ping: %w", err)
	}

	return nil
}

func (c *Client) connect(ctx context.Context) (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name(c.cfg.Name), nats.Timeout(c.cfg.Timeout),
		nats.MaxReconnects(-1), nats.ReconnectBufSize(0),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if c.ctx.Err() == nil {
				c.cfg.Logger.Warn(c.ctx, "pubsub disconnected", logger.Err(err))
			}
		}),
		nats.ReconnectHandler(func(*nats.Conn) {
			c.cfg.Logger.Info(c.ctx, "pubsub reconnected")
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			c.cfg.Logger.Error(c.ctx, "pubsub broker error", logger.Err(err))
		}),
	}
	if c.cfg.Token != "" {
		opts = append(opts, nats.Token(c.cfg.Token))
	} else if c.cfg.Username != "" {
		opts = append(opts, nats.UserInfo(c.cfg.Username, c.cfg.Password))
	}

	if c.cfg.TLSConfig != nil {
		opts = append(opts, nats.Secure(c.cfg.TLSConfig))
	}

	type result struct {
		conn *nats.Conn
		err  error
	}
	done := make(chan result)
	go func() {
		conn, err := nats.Connect(strings.Join(c.cfg.Endpoints, ","), opts...)
		select {
		case done <- result{conn, err}:
		case <-ctx.Done():
			if conn != nil {
				conn.Close()
			}
		}
	}()

	select {
	case result := <-done:
		return result.conn, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Shutdown cancels consumption and in-flight publications, waits for handlers,
// and closes the connection. Handlers must observe cancellation. If ctx expires,
// the connection is closed immediately; handlers that ignore cancellation can
// outlive this call. Repeated calls may wait for that remaining work to finish.
// Do not call Shutdown from a handler running on this client.
func (c *Client) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	conn := c.conn
	if !c.closed {
		c.closed = true
		c.cancel()
		go func() {
			c.wg.Wait()
			if conn != nil {
				conn.Close()
			}

			close(c.done)
		}()
	}
	c.mu.Unlock()

	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		if conn != nil {
			conn.Close()
		}

		return ctx.Err()
	}
}

func (c *Client) operation(ctx context.Context) (context.Context, jetstream.JetStream, func(), error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, nil, nil, ErrClosed
	}

	if c.js == nil {
		return nil, nil, nil, ErrNotStarted
	}

	c.wg.Add(1)
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	finish := func() {
		stop()
		cancel()
		c.wg.Done()
	}

	return ctx, c.js, finish, nil
}
