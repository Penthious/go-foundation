// Package httpclient provides instrumented HTTP clients for service calls.
package httpclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Penthious/go-foundation/internal/circuitbreaker"
	"github.com/Penthious/go-foundation/logger"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// Config configures a reusable client for an internal or external service.
type Config struct {
	// Name identifies this client in circuit-breaker telemetry; empty means
	// "default". Named Fx bindings set it to their binding name automatically.
	Name string
	// Logger is required. Full curl commands, including credentials and bodies,
	// are logged only when its DEBUG level is enabled.
	Logger *logger.Logger
	// Client supplies base settings, including TLS transport, redirect policy,
	// cookie jar, and timeout. It is copied, not modified. If nil, defaults apply.
	Client *http.Client
	// Timeout covers the entire request, including reading the response body.
	// It includes retries and backoff, overrides Client.Timeout when positive,
	// and defaults to Client.Timeout or 30 seconds. Negative values are rejected.
	Timeout time.Duration
	// Transport overrides Client.Transport when non-nil. Standard transports are
	// cloned; custom transports from Client are reused. Configure TLSClientConfig
	// for private CAs or mutual TLS. Defaults verify certificates.
	Transport *http.Transport
	// Retry defaults to the retryablehttp base settings when nil.
	Retry *RetryConfig
	// CircuitBreaker optionally fails fast during an upstream outage. Its state
	// belongs to this client and is shared across its destinations and methods.
	CircuitBreaker *CircuitBreakerConfig
	// APIKey sends X-API-Key. BearerToken sends a raw token as Authorization:
	// Bearer. TokenProvider obtains a raw bearer token per request/redirect hop,
	// before retries, and must be concurrency-safe and honor context cancellation.
	// Configure at most one source. Requests must not also supply credentials.
	// Configured auth restricts redirects to the same scheme, host, and port.
	APIKey        string
	BearerToken   string
	TokenProvider func(context.Context) (string, error)
	// PropagateTraceContext opts into outgoing trace-context/baggage injection.
	// Enable it for trusted internal services. Local spans and metrics are always
	// recorded when OTel is configured, including when propagation is disabled.
	PropagateTraceContext bool
}

// New returns a standard HTTP client with pooling, OTel instrumentation, and
// DEBUG curl logging. Reuse it and close every response body. It retains standard
// redirect and HTTP-status behavior. Safe, replayable requests retry transient
// failures by default. Call CloseIdleConnections when the client is no longer needed.
func New(cfg Config) (*http.Client, error) {
	if cfg.Logger == nil {
		return nil, errors.New("httpclient: logger is required")
	}

	if cfg.Timeout < 0 {
		return nil, errors.New("httpclient: timeout must not be negative")
	}

	auth, err := newAuthTransport(cfg)
	if err != nil {
		return nil, err
	}

	var breaker *circuitbreaker.Breaker
	if cfg.CircuitBreaker != nil {
		breaker, err = circuitbreaker.New(circuitbreaker.Config{
			FailureThreshold: cfg.CircuitBreaker.FailureThreshold, OpenTimeout: cfg.CircuitBreaker.OpenTimeout,
			Name: cfg.Name, Protocol: "http", Logger: cfg.Logger,
		})
		if err != nil {
			return nil, fmt.Errorf("httpclient: %w", err)
		}
	}

	client := http.Client{}
	if cfg.Client != nil {
		client = *cfg.Client
	}

	if cfg.Timeout > 0 {
		client.Timeout = cfg.Timeout
	} else if client.Timeout < 0 {
		return nil, errors.New("httpclient: client timeout must not be negative")
	} else if client.Timeout == 0 {
		client.Timeout = 30 * time.Second
	}

	base := client.Transport
	if cfg.Transport != nil {
		base = cfg.Transport
	} else if base == nil {
		base = http.DefaultTransport
	}

	disableCompression := false
	if standard, ok := base.(*http.Transport); ok {
		base = standard.Clone()
		disableCompression = standard.DisableCompression
	}

	propagator := propagation.NewCompositeTextMapPropagator()
	if cfg.PropagateTraceContext {
		propagator = otel.GetTextMapPropagator()
	}

	observed := otelhttp.NewTransport(&curlTransport{
		base: base, log: cfg.Logger, disableCompression: disableCompression,
	}, otelhttp.WithPropagators(propagator))

	attempts := &transport{base: base, observed: observed, log: cfg.Logger}
	retrying, err := newRetryTransport(attempts, cfg.Retry)
	if err != nil {
		return nil, err
	}

	client.Transport = retrying
	if auth != nil {
		auth.base = retrying
		client.Transport = auth
		client.CheckRedirect = authRedirectPolicy(client.CheckRedirect)
	}

	if breaker != nil {
		client.Transport = &breakerTransport{base: client.Transport, breaker: breaker}
	}

	return &client, nil
}

// Preserve the original replay function before otelhttp wraps it for metrics.
type replayBodyKey struct{}

type transport struct {
	base     http.RoundTripper
	observed http.RoundTripper
	log      *logger.Logger
}

func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.log.Enabled(req.Context(), logger.LevelDebug) {
		req = req.Clone(context.WithValue(req.Context(), replayBodyKey{}, req.GetBody))
	}

	resp, err := t.observed.RoundTrip(req)
	if last, ok := req.Context().Value(breakerAttemptKey{}).(*breakerAttempt); ok {
		last.rateLimited = err == nil && resp != nil && resp.StatusCode == http.StatusTooManyRequests
	}

	return resp, err
}

func (t *transport) CloseIdleConnections() {
	if closer, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

type curlTransport struct {
	base               http.RoundTripper
	log                *logger.Logger
	disableCompression bool
}

func (t *curlTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !t.log.Enabled(req.Context(), logger.LevelDebug) {
		return t.base.RoundTrip(req)
	}

	var body []byte
	var bodyErr error
	var captured *capturedBody
	if req.Body != nil && req.Body != http.NoBody {
		getBody, _ := req.Context().Value(replayBodyKey{}).(func() (io.ReadCloser, error))
		if getBody != nil {
			body, bodyErr = readReplayBody(req.Context(), getBody)
		} else {
			req = req.Clone(req.Context())
			captured = &capturedBody{source: req.Body, expected: req.ContentLength}
			req.Body = captured
		}
	}

	resp, err := t.base.RoundTrip(req)
	protocol := 1
	if resp != nil {
		protocol = resp.ProtoMajor
	}

	emit := func(data []byte, captureErr error) {
		if len(req.Trailer) != 0 {
			captureErr = errors.Join(captureErr, errors.New("request trailers cannot be represented by this curl command"))
		}

		fields := []logger.Field{
			logger.String("method", req.Method), logger.String("url", req.URL.String()),
		}
		if captureErr == nil {
			fields = append(fields, logger.String("curl", curlCommand(req, data, protocol, t.disableCompression)))
		} else {
			fields = append(fields, logger.String("curl_error", captureErr.Error()))
		}

		t.log.Debug(req.Context(), "http request", fields...)
	}
	if captured != nil {
		captured.publish(emit)
	} else {
		emit(body, bodyErr)
	}

	return resp, err
}
