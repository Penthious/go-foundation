package httpclient

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/Penthious/go-foundation/internal/circuitbreaker"
)

// ErrCircuitOpen means the client rejected a request without making an attempt.
// Inspect it with errors.Is; http.Client wraps it in a *url.Error.
var ErrCircuitOpen = errors.New("httpclient: circuit breaker is open")

// CircuitBreakerConfig enables an independent circuit for this client. Nil
// Config.CircuitBreaker disables it. A non-nil zero config uses the defaults.
type CircuitBreakerConfig struct {
	// FailureThreshold defaults to five consecutive failed request hops, each
	// counted once after retries. Transport errors and 5xx are failures.
	// Quotas (429), cancellations and local auth errors are ignored;
	// other statuses reset the count.
	FailureThreshold int
	// OpenTimeout defaults to 30s, then permits one concurrent recovery probe.
	// A successful probe closes the circuit; a failure starts another timeout.
	OpenTimeout time.Duration
}

type breakerTransport struct {
	base    http.RoundTripper
	breaker *circuitbreaker.Breaker
}

type breakerAttemptKey struct{}

// Each request's retry attempts update this synchronously before returning.
type breakerAttempt struct {
	rateLimited bool
}

func (t *breakerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	err := req.Context().Err()
	var permit *circuitbreaker.Permit
	if err == nil {
		var allowed bool
		permit, allowed = t.breaker.Allow(req.Context())
		if !allowed {
			err = ErrCircuitOpen
		}
	}

	if err != nil {
		if req.Body != nil {
			_ = req.Body.Close()
		}

		return nil, err
	}

	// Panic unwinding must also release an admitted recovery probe.
	defer permit.Done(circuitbreaker.Ignored)

	last := &breakerAttempt{}
	req = req.WithContext(context.WithValue(req.Context(), breakerAttemptKey{}, last))
	resp, err := t.base.RoundTrip(req)
	outcome := httpOutcome(resp, err)
	if last.rateLimited && errors.Is(err, context.DeadlineExceeded) {
		// Retry backoff can time out after 429 without returning its response.
		outcome = circuitbreaker.Ignored
	}

	permit.Done(outcome)

	return resp, err
}

func (t *breakerTransport) CloseIdleConnections() {
	if closer, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

func httpOutcome(resp *http.Response, err error) circuitbreaker.Outcome {
	if errors.Is(err, context.Canceled) || errors.Is(err, errCredentials) {
		return circuitbreaker.Ignored
	}

	if err == nil && resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		// One identity's quota must not change the shared upstream circuit.
		return circuitbreaker.Ignored
	}

	if err != nil || resp != nil && resp.StatusCode >= 500 {
		return circuitbreaker.Failure
	}

	return circuitbreaker.Success
}
