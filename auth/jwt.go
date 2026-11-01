package auth

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	jose "github.com/go-jose/go-jose/v4"
)

// JWTConfig trusts one issuer and one resource-server audience. JWKSURL is an
// explicit trusted URL; token-supplied key URLs and discovery are never used.
type JWTConfig struct {
	Issuer   string
	Audience string
	JWKSURL  string
	// Algorithms defaults to RS256. Only asymmetric signing algorithms are allowed.
	Algorithms []string
	// TokenType defaults to at+jwt, the JWT access-token profile. Set JWT only for
	// providers using legacy access tokens, with a dedicated API audience that is
	// never used for ID tokens. The protected typ header must match exactly.
	TokenType string
	// AllowMissingTokenType accepts an absent typ header from trusted legacy
	// providers. A present header must still match TokenType. Use only with a
	// dedicated access-token audience that is never used for ID tokens.
	AllowMissingTokenType bool
	// Timeout defaults to 5s, bounding verification and each JWKS HTTP operation.
	Timeout time.Duration
	// HTTPClient is copied. Its transport is borrowed; configure it before New
	// and close idle connections after requests stop. Redirects are rejected.
	HTTPClient *http.Client
	// AllowInsecureHTTP permits plaintext issuer/JWKS URLs for local development.
	// Production issuer and JWKS URLs must use HTTPS.
	AllowInsecureHTTP bool
}

type jwtVerifier struct {
	verifier              *oidc.IDTokenVerifier
	issuer                string
	tokenType             string
	allowMissingTokenType bool
	algorithms            []jose.SignatureAlgorithm
	timeout               time.Duration
}

func newJWTVerifier(cfg JWTConfig) (*jwtVerifier, error) {
	if !trustedURL(cfg.Issuer, cfg.AllowInsecureHTTP) || !trustedURL(cfg.JWKSURL, cfg.AllowInsecureHTTP) {
		return nil, errors.New("auth: issuer and JWKS URL must be absolute HTTPS URLs without credentials or fragments")
	}

	if strings.TrimSpace(cfg.Audience) == "" || cfg.Timeout < 0 {
		return nil, errors.New("auth: JWT audience is required and timeout must not be negative")
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}

	if cfg.TokenType == "" {
		cfg.TokenType = "at+jwt"
	}

	if cfg.TokenType != "at+jwt" && cfg.TokenType != "JWT" {
		return nil, errors.New("auth: JWT token type must be at+jwt or JWT")
	}

	if len(cfg.Algorithms) == 0 {
		cfg.Algorithms = []string{"RS256"}
	}

	algorithms := make([]jose.SignatureAlgorithm, 0, len(cfg.Algorithms))
	for _, alg := range cfg.Algorithms {
		if !slices.Contains([]string{"RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512", "EdDSA"}, alg) {
			return nil, errors.New("auth: unsupported JWT signing algorithm")
		}

		algorithms = append(algorithms, jose.SignatureAlgorithm(alg))
	}

	client := http.Client{}
	if cfg.HTTPClient != nil {
		client = *cfg.HTTPClient
	}

	if client.Timeout == 0 || client.Timeout > cfg.Timeout {
		client.Timeout = cfg.Timeout
	}

	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("auth: JWKS redirects are disabled")
	}
	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	client.Transport = &jwksTransport{base: transport}
	keySet := oidc.NewRemoteKeySet(oidc.ClientContext(context.Background(), &client), cfg.JWKSURL)
	verifier := oidc.NewVerifier(cfg.Issuer, keySet, &oidc.Config{
		ClientID: cfg.Audience, SupportedSigningAlgs: slices.Clone(cfg.Algorithms),
	})

	return &jwtVerifier{
		verifier: verifier, issuer: cfg.Issuer, tokenType: cfg.TokenType,
		allowMissingTokenType: cfg.AllowMissingTokenType,
		algorithms:            algorithms, timeout: cfg.Timeout,
	}, nil
}

func (v *jwtVerifier) verify(ctx context.Context, raw string) (Principal, bool) {
	if len(raw) > 16<<10 {
		return Principal{}, false
	}

	jws, err := jose.ParseSignedCompact(raw, v.algorithms)
	if err != nil || len(jws.Signatures) != 1 {
		return Principal{}, false
	}

	tokenType, present := jws.Signatures[0].Protected.ExtraHeaders[jose.HeaderType]
	if (!present && !v.allowMissingTokenType) || (present && tokenType != v.tokenType) {
		return Principal{}, false
	}

	ctx, cancel := context.WithTimeout(ctx, v.timeout)
	defer cancel()

	token, err := v.verifier.Verify(ctx, raw)
	if err != nil || token.Issuer != v.issuer || strings.TrimSpace(token.Subject) == "" {
		return Principal{}, false
	}

	var claims struct {
		Scope     string   `json:"scope"`
		SCP       []string `json:"scp"`
		NotBefore float64  `json:"nbf"`
		IssuedAt  float64  `json:"iat"`
	}
	if err := token.Claims(&claims); err != nil {
		return Principal{}, false
	}

	// Enforce a small, explicit skew instead of the OIDC library's 5-minute nbf allowance.
	if claims.NotBefore > float64(time.Now().Add(30*time.Second).Unix()) || claims.IssuedAt > float64(time.Now().Add(30*time.Second).Unix()) || ctx.Err() != nil {
		return Principal{}, false
	}

	scopes := claims.SCP
	if claims.Scope != "" {
		scopes = append(strings.Split(claims.Scope, " "), scopes...)
	}

	for _, scope := range scopes {
		if !validScope(scope) {
			return Principal{}, false
		}
	}

	return Principal{Subject: token.Subject, Issuer: token.Issuer, Method: "jwt", Scopes: scopes}, true
}

func trustedURL(raw string, insecure bool) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}

	return u.Hostname() != "" && u.User == nil && u.Fragment == "" &&
		(u.Scheme == "https" || (insecure && u.Scheme == "http"))
}

// Bound payload size and refresh traffic, including requests with fabricated key IDs.
type jwksTransport struct {
	base      http.RoundTripper
	mu        sync.Mutex
	lastFetch time.Time
}

func (t *jwksTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	t.mu.Lock()
	allowed := time.Since(t.lastFetch) >= time.Second
	if allowed {
		t.lastFetch = time.Now()
	}

	t.mu.Unlock()
	if !allowed {
		return nil, errors.New("auth: JWKS refresh throttled")
	}

	response, err := t.base.RoundTrip(r)
	if err != nil {
		return nil, err
	}

	response.Body = http.MaxBytesReader(nil, response.Body, 1<<20)

	return response, nil
}
