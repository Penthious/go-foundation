package httpclient

import (
	"context"
	"errors"
	"net/http"
	"strings"
)

var errCredentials = errors.New("httpclient: missing, invalid or conflicting credentials")

type authTransport struct {
	base          *retryTransport
	apiKey        string
	bearerToken   string
	tokenProvider func(context.Context) (string, error)
}

func newAuthTransport(cfg Config) (*authTransport, error) {
	count := 0
	if cfg.APIKey != "" {
		count++
	}

	if cfg.BearerToken != "" {
		count++
	}

	if cfg.TokenProvider != nil {
		count++
	}

	if count == 0 {
		return nil, nil
	}

	if count > 1 || cfg.APIKey != "" && !validCredential(cfg.APIKey, 4096) || cfg.BearerToken != "" && !validCredential(cfg.BearerToken, 16<<10) {
		return nil, credentialError()
	}

	return &authTransport{apiKey: cfg.APIKey, bearerToken: cfg.BearerToken, tokenProvider: cfg.TokenProvider}, nil
}

func (t *authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	prepared, err := t.prepare(req)
	if err != nil {
		if req.Body != nil {
			_ = req.Body.Close()
		}

		if !errors.Is(err, errCredentials) {
			err = errors.Join(errCredentials, err)
		}

		return nil, err
	}

	return t.base.RoundTrip(prepared)
}

func (t *authTransport) prepare(req *http.Request) (*http.Request, error) {
	if err := req.Context().Err(); err != nil {
		return nil, err
	}

	if req.URL.User != nil {
		return nil, credentialError()
	}

	for key := range req.Header {
		if strings.EqualFold(key, "Authorization") || strings.EqualFold(key, "X-API-Key") {
			return nil, credentialError()
		}
	}

	token := t.bearerToken
	if t.tokenProvider != nil {
		value, ok := fetchToken(req.Context(), t.tokenProvider)
		if err := req.Context().Err(); err != nil {
			return nil, err
		}

		if !ok {
			return nil, credentialError()
		}

		token = value
	}

	prepared := req.Clone(req.Context())
	if t.apiKey != "" {
		prepared.Header.Set("X-API-Key", t.apiKey)
	} else {
		prepared.Header.Set("Authorization", "Bearer "+token)
	}

	return prepared, nil
}

func (t *authTransport) CloseIdleConnections() {
	t.base.CloseIdleConnections()
}

func authRedirectPolicy(previous func(*http.Request, []*http.Request) error) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if previous != nil {
			if err := previous(req, via); err != nil {
				return err
			}
		} else if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}

		if !strings.EqualFold(req.URL.Scheme, via[0].URL.Scheme) || !strings.EqualFold(req.URL.Host, via[0].URL.Host) {
			return http.ErrUseLastResponse
		}

		return nil
	}
}

func credentialError() error {
	return errCredentials
}

func validCredential(value string, limit int) bool {
	if len(value) == 0 || len(value) > limit {
		return false
	}

	for _, char := range value {
		if char < '!' || char > '~' || char == ',' {
			return false
		}
	}

	return true
}

func fetchToken(ctx context.Context, provider func(context.Context) (string, error)) (token string, ok bool) {
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			token, ok = "", false
		}
	}()
	token, err := provider(ctx)
	returned = true

	return token, err == nil && validCredential(token, 16<<10)
}
