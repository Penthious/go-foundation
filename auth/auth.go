// Package auth authenticates HTTP requests using JWT access tokens or API keys.
// Cryptographic libraries and identity-provider clients remain private.
package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"slices"
	"strings"
)

// ErrUnauthenticated means a credential is absent, ambiguous or invalid.
var ErrUnauthenticated = errors.New("auth: unauthenticated")

// Config enables one or both authentication methods. It is a startup snapshot.
type Config struct {
	JWT     *JWTConfig
	APIKeys []APIKey
}

// APIKey grants scopes to a named service identity. Secret must be a random,
// printable ASCII value of at least 32 bytes, without whitespace or commas.
// Load it from a secret store or environment; never commit it to source control.
type APIKey struct {
	ID     string
	Secret string
	Scopes []string
}

// Principal is the authenticated identity. JWT identities are identified by the
// pair Issuer and Subject; API-key identities use Subject equal to the key ID.
type Principal struct {
	Subject string
	Issuer  string
	Method  string // "jwt" or "api_key"
	Scopes  []string
}

// HasScopes reports whether every required scope is granted, with exact,
// case-sensitive matching. Empty requirements allow any authenticated identity.
func (p Principal) HasScopes(required ...string) bool {
	for _, scope := range required {
		if !validScope(scope) || !slices.Contains(p.Scopes, scope) {
			return false
		}
	}

	return true
}

type principalKey struct{}

// FromContext returns the identity installed by Protect. Its scope slice is an
// independent copy. A missing identity must never be treated as authenticated.
func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	p.Scopes = slices.Clone(p.Scopes)

	return p, ok
}

type apiCredential struct {
	digest    [sha256.Size]byte
	principal Principal
}

// Authenticator is safe for concurrent requests. Construct it with New; it owns
// no background workers. API keys are retained only as SHA-256 digests.
type Authenticator struct {
	jwt  *jwtVerifier
	keys []apiCredential
}

// New validates and copies config without contacting the identity provider.
// JWT keys are fetched lazily and cached; a missing key triggers a refresh.
func New(cfg Config) (*Authenticator, error) {
	if cfg.JWT == nil && len(cfg.APIKeys) == 0 {
		return nil, errors.New("auth: at least one authentication method is required")
	}

	a := &Authenticator{}
	if cfg.JWT != nil {
		verifier, err := newJWTVerifier(*cfg.JWT)
		if err != nil {
			return nil, err
		}

		a.jwt = verifier
	}

	ids := make(map[string]bool)
	digests := make(map[[sha256.Size]byte]bool)
	for _, key := range cfg.APIKeys {
		if strings.TrimSpace(key.ID) == "" || len(key.ID) > 256 || strings.ContainsAny(key.ID, "\r\n\x00") {
			return nil, errors.New("auth: API keys require a nonblank ID of at most 256 bytes without control delimiters")
		}

		if len(key.Secret) < 32 || len(key.Secret) > 4096 || !validCredential(key.Secret) {
			return nil, errors.New("auth: API key secrets must contain 32–4096 printable ASCII bytes without whitespace or commas")
		}

		for _, scope := range key.Scopes {
			if !validScope(scope) {
				return nil, errors.New("auth: API key scopes must be nonempty OAuth scope tokens")
			}
		}

		digest := sha256.Sum256([]byte(key.Secret))
		if ids[key.ID] || digests[digest] {
			return nil, errors.New("auth: duplicate API key ID or secret")
		}

		ids[key.ID], digests[digest] = true, true
		a.keys = append(a.keys, apiCredential{
			digest:    digest,
			principal: Principal{Subject: key.ID, Method: "api_key", Scopes: slices.Clone(key.Scopes)},
		})
	}

	return a, nil
}

// Protect authenticates one header credential, then requires every listed scope.
// Missing, invalid, duplicate, or mixed credentials receive 401; an authenticated
// identity lacking a scope receives 403. Invalid scope requirements deny access.
// Wrap every protected route explicitly; unwrapped routes remain public.
func (a *Authenticator) Protect(next http.Handler, scopes ...string) http.Handler {
	required := slices.Clone(scopes)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		ctx, err := a.Authenticate(r.Context(), r.Header)
		if err != nil {
			if a.jwt != nil {
				w.Header().Add("WWW-Authenticate", "Bearer")
			}

			if len(a.keys) > 0 {
				w.Header().Add("WWW-Authenticate", "ApiKey")
			}

			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}

		p, _ := FromContext(ctx)
		if !p.HasScopes(required...) {
			if p.Method == "jwt" {
				w.Header().Set("WWW-Authenticate", `Bearer error="insufficient_scope"`)
			}

			writeError(w, http.StatusForbidden, "forbidden")
			return
		}

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Authenticate validates header credentials and returns a context containing the
// principal. Other transports can map their authorization/x-api-key metadata to
// standard HTTP headers. It applies the same rules as Protect, without scope
// checks or writing a response. Use Header.Set/Add to canonicalize header names.
func (a *Authenticator) Authenticate(ctx context.Context, headers http.Header) (context.Context, error) {
	p, ok := a.authenticate(ctx, headers)
	if !ok {
		return ctx, ErrUnauthenticated
	}

	return context.WithValue(ctx, principalKey{}, p), nil
}

func (a *Authenticator) authenticate(ctx context.Context, headers http.Header) (Principal, bool) {
	if ctx.Err() != nil {
		return Principal{}, false
	}

	bearer, keys := headers.Values("Authorization"), headers.Values("X-API-Key")
	if len(bearer)+len(keys) != 1 {
		return Principal{}, false
	}

	if len(bearer) == 1 && a.jwt != nil {
		scheme, token, ok := strings.Cut(bearer[0], " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") || !validCredential(token) {
			return Principal{}, false
		}

		return a.jwt.verify(ctx, token)
	}

	if len(keys) != 1 || len(keys[0]) > 4096 || !validCredential(keys[0]) {
		return Principal{}, false
	}

	digest := sha256.Sum256([]byte(keys[0]))
	var principal Principal
	matched := false
	for _, key := range a.keys {
		if subtle.ConstantTimeCompare(digest[:], key.digest[:]) == 1 {
			principal, matched = key.principal, true
		}
	}

	return principal, matched
}

func validCredential(value string) bool {
	if value == "" {
		return false
	}

	for _, c := range value {
		if c < '!' || c > '~' || c == ',' {
			return false
		}
	}

	return true
}

func validScope(value string) bool {
	if value == "" {
		return false
	}

	for _, c := range value {
		if c < '!' || c > '~' || c == '"' || c == '\\' {
			return false
		}
	}

	return true
}

func writeError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte("{\"error\":\"" + code + "\"}\n"))
}
