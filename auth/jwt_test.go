package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
)

func TestNew_JWTValidation(t *testing.T) {
	base := JWTConfig{Issuer: "https://issuer.example", Audience: "orders", JWKSURL: "https://issuer.example/keys"}
	for _, tt := range []struct {
		name   string
		change func(*JWTConfig)
	}{
		{"missing issuer", func(c *JWTConfig) { c.Issuer = "" }},
		{"relative JWKS", func(c *JWTConfig) { c.JWKSURL = "/keys" }},
		{"plaintext", func(c *JWTConfig) { c.JWKSURL = "http://issuer.example/keys" }},
		{"credentials", func(c *JWTConfig) { c.JWKSURL = "https://secret@issuer.example/keys" }},
		{"fragment", func(c *JWTConfig) { c.JWKSURL += "#keys" }},
		{"missing audience", func(c *JWTConfig) { c.Audience = " " }},
		{"timeout", func(c *JWTConfig) { c.Timeout = -time.Second }},
		{"HMAC", func(c *JWTConfig) { c.Algorithms = []string{"HS256"} }},
		{"unsigned", func(c *JWTConfig) { c.Algorithms = []string{"none"} }},
		{"unsupported type", func(c *JWTConfig) { c.TokenType = "id_token" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			tt.change(&cfg)

			if a, err := New(Config{JWT: &cfg}); err == nil || a != nil {
				t.Fatal("invalid JWT config accepted")
			}
		})
	}
}

func TestJWT_ClaimsSignaturesAndHTTPContract(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	var fetches atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: "one", Use: "sig", Algorithm: "RS256"}}})
	}))
	defer server.Close()

	client := server.Client()
	cfg := JWTConfig{Issuer: server.URL, Audience: "orders", JWKSURL: server.URL + "/keys", HTTPClient: client}
	apiSecret := strings.Repeat("k", 32)
	a, err := New(Config{JWT: &cfg, APIKeys: []APIKey{{ID: "worker", Secret: apiSecret}}})
	if err != nil {
		t.Fatal(err)
	}

	if fetches.Load() != 0 || client.Timeout != 0 || client.CheckRedirect != nil {
		t.Fatal("New fetched keys or mutated the provided client")
	}

	claims := func() map[string]any {
		return map[string]any{"iss": server.URL, "aud": "orders", "sub": "alice", "exp": time.Now().Add(time.Hour).Unix(), "scope": "orders:read profile", "iat": time.Now().Unix()}
	}
	for _, tt := range []struct {
		name      string
		change    func(map[string]any)
		tokenType string
		status    int
	}{
		{"valid", nil, "at+jwt", 204},
		{"audience array", func(c map[string]any) { c["aud"] = []string{"other", "orders"} }, "at+jwt", 204},
		{"scp permissions", func(c map[string]any) { delete(c, "scope"); c["scp"] = []string{"orders:read"} }, "at+jwt", 204},
		{"wrong issuer", func(c map[string]any) { c["iss"] = "https://wrong.example" }, "at+jwt", 401},
		{"wrong audience", func(c map[string]any) { c["aud"] = "browser-client" }, "at+jwt", 401},
		{"missing audience", func(c map[string]any) { delete(c, "aud") }, "at+jwt", 401},
		{"expired", func(c map[string]any) { c["exp"] = time.Now().Add(-time.Second).Unix() }, "at+jwt", 401},
		{"missing expiry", func(c map[string]any) { delete(c, "exp") }, "at+jwt", 401},
		{"missing subject", func(c map[string]any) { delete(c, "sub") }, "at+jwt", 401},
		{"blank subject", func(c map[string]any) { c["sub"] = " " }, "at+jwt", 401},
		{"future nbf", func(c map[string]any) { c["nbf"] = time.Now().Add(time.Minute).Unix() }, "at+jwt", 401},
		{"future iat", func(c map[string]any) { c["iat"] = time.Now().Add(time.Minute).Unix() }, "at+jwt", 401},
		{"nbf skew", func(c map[string]any) { c["nbf"] = time.Now().Add(10 * time.Second).Unix() }, "at+jwt", 204},
		{"malformed scopes", func(c map[string]any) { c["scope"] = []string{"orders:read"} }, "at+jwt", 401},
		{"invalid scope separator", func(c map[string]any) { c["scope"] = "orders:read\tprofile" }, "at+jwt", 401},
		{"malformed scp", func(c map[string]any) { c["scp"] = "orders:read" }, "at+jwt", 401},
		{"missing permission", func(c map[string]any) { c["scope"] = "orders:write" }, "at+jwt", 403},
		{"invalid scope token", func(c map[string]any) { c["scp"] = []string{"bad scope"} }, "at+jwt", 401},
		{"ID token type", nil, "JWT", 401},
		{"missing type", nil, "", 401},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := claims()
			if tt.change != nil {
				tt.change(c)
			}

			token := signJWT(t, key, jose.RS256, "one", tt.tokenType, c)
			called := false
			handler := a.Protect(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				p, ok := FromContext(r.Context())
				if !ok || p.Subject != "alice" || p.Issuer != server.URL || p.Method != "jwt" || !p.HasScopes("orders:read") {
					t.Errorf("wrong JWT principal: %+v", p)
				}

				w.WriteHeader(204)
			}), "orders:read")
			req := httptest.NewRequest("GET", "/orders", nil)
			req.Header.Set("Authorization", "Bearer "+token)
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, req)

			if response.Code != tt.status || called != (tt.status == 204) {
				t.Fatalf("status = %d, called = %v; want %d", response.Code, called, tt.status)
			}

			if tt.status == 401 && len(response.Header().Values("WWW-Authenticate")) != 2 {
				t.Fatal("missing enabled method challenges")
			}

			if tt.status == 403 && !strings.Contains(response.Header().Get("WWW-Authenticate"), "insufficient_scope") {
				t.Fatal("missing insufficient_scope challenge")
			}

			if strings.Contains(response.Body.String(), token) {
				t.Fatal("token exposed in response")
			}
		})
	}

	valid := signJWT(t, key, jose.RS256, "one", "at+jwt", claims())
	for _, value := range []string{
		"Bearer ", "Basic " + valid, "Bearer " + valid + "," + valid,
		"Bearer " + strings.Repeat("a", 17000),
		"Bearer " + valid[:strings.LastIndex(valid, ".")+1] + "invalid",
		"Bearer " + signJWT(t, []byte(strings.Repeat("s", 32)), jose.HS256, "one", "at+jwt", claims()),
		"Bearer " + base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"at+jwt"}`)) + ".e30.",
	} {
		req := httptest.NewRequest("GET", "/orders", nil)
		req.Header.Set("Authorization", value)
		response := httptest.NewRecorder()

		a.Protect(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("invalid credential reached handler") })).ServeHTTP(response, req)

		if response.Code != 401 {
			t.Errorf("invalid credential accepted: status %d", response.Code)
		}
	}

	if fetches.Load() != 1 {
		t.Fatalf("cached keys or refresh throttle failed: %d fetches", fetches.Load())
	}

	cfg.TokenType = "JWT"
	legacy, err := New(Config{JWT: &cfg})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest("GET", "/orders", nil)
	req.Header.Set("Authorization", "bearer "+signJWT(t, key, jose.RS256, "one", "JWT", claims()))
	response := httptest.NewRecorder()
	legacy.Protect(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	})).ServeHTTP(response, req)

	if response.Code != 204 {
		t.Fatal("explicit legacy JWT setting rejected")
	}
}

func TestJWT_RotationOutageAndConcurrentCaching(t *testing.T) {
	first, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	second, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	var rotated, outage atomic.Bool
	var fetches atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		if outage.Load() {
			http.Error(w, "provider unavailable", 503)
			return
		}

		key, id := &first.PublicKey, "first"
		if rotated.Load() {
			key, id = &second.PublicKey, "second"
		}

		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: key, KeyID: id, Use: "sig", Algorithm: "ES256"}}})
	}))
	defer server.Close()

	cfg := JWTConfig{Issuer: server.URL, JWKSURL: server.URL, Audience: "orders", Algorithms: []string{"ES256"}, HTTPClient: server.Client()}
	v, err := newJWTVerifier(cfg)
	if err != nil {
		t.Fatal(err)
	}

	claims := map[string]any{"iss": server.URL, "aud": "orders", "sub": "service", "exp": time.Now().Add(time.Hour).Unix()}
	one := signJWT(t, first, jose.ES256, "first", "at+jwt", claims)
	two := signJWT(t, second, jose.ES256, "second", "at+jwt", claims)
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if _, ok := v.verify(t.Context(), one); !ok {
				t.Error("concurrent verification failed")
			}
		})
	}

	wg.Wait()
	if fetches.Load() != 1 {
		t.Fatalf("concurrent fetches = %d, want 1", fetches.Load())
	}

	rotated.Store(true)
	verifyEventually(t, v, two)

	outage.Store(true)
	if _, ok := v.verify(t.Context(), one); ok {
		t.Fatal("unavailable unknown signing key accepted")
	}

	if _, ok := v.verify(t.Context(), two); !ok {
		t.Fatal("cached key should work during provider outage")
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, ok := v.verify(ctx, two); ok {
		t.Fatal("canceled verification succeeded")
	}
}

func TestJWT_JWKSBoundaries(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	for _, mode := range []string{"oversized", "redirect", "timeout", "malformed", "unavailable"} {
		t.Run(mode, func(t *testing.T) {
			var redirects atomic.Int64
			var entered atomic.Bool
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				entered.Store(true)
				if r.URL.Path == "/redirected" {
					redirects.Add(1)
					return
				}

				switch mode {
				case "oversized":
					_, _ = io.WriteString(w, strings.Repeat(" ", (1<<20)+1))
				case "redirect":
					http.Redirect(w, r, "/redirected", 302)
				case "timeout":
					<-r.Context().Done()
				case "malformed":
					_, _ = io.WriteString(w, "not json")
				case "unavailable":
					http.Error(w, "provider unavailable", http.StatusServiceUnavailable)
				}
			}))
			defer server.Close()

			cfg := JWTConfig{Issuer: server.URL, JWKSURL: server.URL, Audience: "orders", Algorithms: []string{"ES256"}, HTTPClient: server.Client(), Timeout: time.Second}
			if mode == "timeout" {
				cfg.Timeout = 100 * time.Millisecond
			}

			v, err := newJWTVerifier(cfg)
			if err != nil {
				t.Fatal(err)
			}

			token := signJWT(t, key, jose.ES256, "one", "at+jwt", map[string]any{"iss": server.URL, "aud": "orders", "sub": "service", "exp": time.Now().Add(time.Hour).Unix()})
			_, ok := v.verify(t.Context(), token)

			if ok || redirects.Load() != 0 || !entered.Load() {
				t.Fatalf("JWKS boundary failed: authenticated=%v, redirects=%d, reached=%v", ok, redirects.Load(), entered.Load())
			}
		})
	}
}

func signJWT(t *testing.T, key any, alg jose.SignatureAlgorithm, id, typ string, claims map[string]any) string {
	t.Helper()

	options := &jose.SignerOptions{}
	if typ != "" {
		options.WithType(jose.ContentType(typ))
	}

	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: alg, Key: jose.JSONWebKey{Key: key, KeyID: id}}, options)
	if err != nil {
		t.Fatal(err)
	}

	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}

	signed, err := signer.Sign(payload)
	if err != nil {
		t.Fatal(err)
	}

	raw, err := signed.CompactSerialize()
	if err != nil {
		t.Fatal(err)
	}

	return raw
}

func verifyEventually(t *testing.T, v *jwtVerifier, token string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if _, ok := v.verify(ctx, token); ok {
			return
		}

		select {
		case <-ctx.Done():
			t.Fatal("rotated signing key was not refreshed within the bounded wait")
		case <-ticker.C:
		}
	}
}
