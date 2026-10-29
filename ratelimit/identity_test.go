package ratelimit

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/auth"

	jose "github.com/go-jose/go-jose/v4"
)

func TestIdentity_ValidatedPrincipalsAndCredentialRotation(t *testing.T) {
	cfg := testConfig(t, Memory)
	cfg.Limit, cfg.Window, cfg.Burst = 1, time.Hour, 1
	limiter := testLimiter(t, cfg)
	if _, err := limiter.allowPrincipal(t.Context()); !errors.Is(err, auth.ErrUnauthenticated) || len(limiter.memory.entries) != 0 {
		t.Fatal("missing principal was treated as authenticated:", err)
	}

	first := apiContext(t, "alice", strings.Repeat("a", 32))
	rotated := apiContext(t, "alice", strings.Repeat("b", 32))
	decision, err := limiter.allowPrincipal(first)
	if err != nil || !decision.Allowed {
		t.Fatal(err)
	}

	decision, err = limiter.allowPrincipal(rotated)
	if err != nil || decision.Allowed {
		t.Fatal("rotating a key secret reset the key ID quota:", decision, err)
	}

	one, tokenOne := jwtGate(t)
	two, tokenTwo := jwtGate(t)
	jwtContext := func(gate *auth.Authenticator, token string) context.Context {
		ctx, err := gate.Authenticate(t.Context(), http.Header{"Authorization": {"Bearer " + token}})
		if err != nil {
			t.Fatal(err)
		}

		return ctx
	}

	for i, ctx := range []context.Context{
		jwtContext(one, tokenOne("alice")),
		jwtContext(one, tokenOne("alice")),
		jwtContext(one, tokenOne("bob")),
		jwtContext(two, tokenTwo("alice")),
	} {
		decision, err := limiter.allowPrincipal(ctx)
		if err != nil || decision.Allowed != (i != 1) {
			t.Fatal("JWT subject/issuer isolation or token rotation failed:", i, decision, err)
		}
	}
}

func apiContext(t *testing.T, id, secret string) context.Context {
	t.Helper()

	gate, err := auth.New(auth.Config{APIKeys: []auth.APIKey{{ID: id, Secret: secret}}})
	if err != nil {
		t.Fatal(err)
	}

	ctx, err := gate.Authenticate(t.Context(), http.Header{"X-Api-Key": {secret}})
	if err != nil {
		t.Fatal(err)
	}

	return ctx
}

func jwtGate(t *testing.T) (*auth.Authenticator, func(string) string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	keys := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: "test", Algorithm: "ES256", Use: "sig"}}})
	}))
	t.Cleanup(keys.Close)

	gate, err := auth.New(auth.Config{JWT: &auth.JWTConfig{Issuer: keys.URL, Audience: "rate-test", JWKSURL: keys.URL, Algorithms: []string{"ES256"}, HTTPClient: keys.Client()}})
	if err != nil {
		t.Fatal(err)
	}

	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.ES256, Key: jose.JSONWebKey{Key: key, KeyID: "test"}}, (&jose.SignerOptions{}).WithType("at+jwt"))
	if err != nil {
		t.Fatal(err)
	}

	return gate, func(subject string) string {
		claims, err := json.Marshal(map[string]any{"iss": keys.URL, "aud": "rate-test", "sub": subject, "exp": time.Now().Add(time.Hour).Unix()})
		if err != nil {
			t.Fatal(err)
		}

		signed, err := signer.Sign(claims)
		if err != nil {
			t.Fatal(err)
		}

		token, err := signed.CompactSerialize()
		if err != nil {
			t.Fatal(err)
		}

		return token
	}
}
