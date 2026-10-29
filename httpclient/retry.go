package httpclient

import (
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

// RetryConfig overrides retries per client. A nil Config.Retry retains all
// retryablehttp base retry limits and backoff settings.
type RetryConfig struct {
	// MaxRetries counts retries after the initial attempt; zero disables retries.
	MaxRetries int
	// WaitMin and WaitMax retain the library defaults when zero. Positive values
	// override them; negative values and an effective maximum below minimum fail.
	WaitMin time.Duration
	WaitMax time.Duration
}

type retryTransport struct {
	attempts *transport
	client   *retryablehttp.Client
}

func newRetryTransport(attempts *transport, cfg *RetryConfig) (*retryTransport, error) {
	client := retryablehttp.NewClient()
	if cfg != nil {
		if cfg.MaxRetries < 0 || cfg.WaitMin < 0 || cfg.WaitMax < 0 {
			return nil, errors.New("httpclient: retry limits and waits must not be negative")
		}

		client.RetryMax = cfg.MaxRetries
		if cfg.WaitMin > 0 {
			client.RetryWaitMin = cfg.WaitMin
		}

		if cfg.WaitMax > 0 {
			client.RetryWaitMax = cfg.WaitMax
		}

		if client.RetryWaitMax < client.RetryWaitMin {
			return nil, errors.New("httpclient: retry maximum wait must be at least the minimum wait")
		}
	}

	// The outer client owns redirects, cookies, and the overall timeout budget.
	client.HTTPClient = &http.Client{
		Transport: attempts,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	client.Logger = nil
	client.ErrorHandler = retryablehttp.PassthroughErrorHandler
	client.PrepareRetry = rewindBody

	return &retryTransport{attempts: attempts, client: client}, nil
}

func (t *retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.client.RetryMax == 0 || !safeToRetry(req) {
		return t.attempts.RoundTrip(req)
	}

	// FromRequest pre-buffers bodies. Keep the first body intact and rewind only
	// subsequent attempts with PrepareRetry, preserving streaming and ownership.
	resp, err := t.client.Do(&retryablehttp.Request{Request: req})
	if err != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}

		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}

		return nil, err
	}

	return resp, nil
}

func (t *retryTransport) CloseIdleConnections() {
	t.attempts.CloseIdleConnections()
}

func safeToRetry(req *http.Request) bool {
	if len(req.Trailer) != 0 {
		return false
	}

	switch req.Method {
	case "", http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return req.Body == nil || req.Body == http.NoBody || req.GetBody != nil
	default:
		return false
	}
}

func rewindBody(req *http.Request) error {
	if req.Body == nil || req.Body == http.NoBody {
		return nil
	}

	body, err := req.GetBody()
	if err != nil {
		return err
	}

	if body == nil {
		return errors.New("httpclient: GetBody returned a nil body for retry")
	}

	req.Body = body

	return nil
}
