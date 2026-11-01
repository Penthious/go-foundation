# auth

[Overview](../README.md) · [Development and testing](../testsupport/README.md)

## Authentication and authorization

Services create an `auth.Authenticator` and explicitly wrap protected handlers.
JWT and API-key backends stay private; handlers receive a GOF `Principal` from
the request context. Either configured method can authenticate a wrapped route;
the identity must have **every** scope required by that route.

```go
gate, err := auth.New(auth.Config{
	JWT: &auth.JWTConfig{
		Issuer:   cfg.AuthIssuer,
		Audience: "orders-api",
		JWKSURL:  cfg.AuthJWKSURL,
	},
	APIKeys: []auth.APIKey{{
		ID: "inventory-service", Secret: cfg.InventoryAPIKey,
		Scopes: []string{"orders:read"},
	}},
})
if err != nil {
	return err
}

orderHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	principal, ok := auth.FromContext(r.Context())
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	// Apply resource ownership/tenant checks before accessing this order.
	log.Info(r.Context(), "fetching order", logger.String("subject", principal.Subject))
	w.WriteHeader(http.StatusNoContent)
})

server, err := httpserver.New(httpserver.Config{
	Logger: log,
	Routes: []httpserver.Route{{
		Pattern: "GET /orders/{id}",
		Handler: gate.Protect(orderHandler, "orders:read"),
	}},
})
if err != nil {
	return err
}
```

This uses `net/http` and GOF's `auth`, `httpserver`, and `logger` packages.
`cfg` is the service's loaded configuration. Run the server using its normal
startup/shutdown lifecycle. Omit `JWT` or `APIKeys` to enable just one method;
at least one method is required. `New` copies configuration and does no network
I/O. `Protect(handler)` without scopes still requires authentication.
Scope checks are exact and case-sensitive; invalid/empty required scope strings
deny access. Route requirements are copied when the wrapper is constructed.

Clients send **one** of:

```http
Authorization: Bearer <JWT-access-token>
```

```http
X-API-Key: <random-secret>
```

Missing/invalid credentials, duplicate authentication headers, or both methods
in the same request return `401 {"error":"unauthorized"}` with challenges for
the configured methods. API keys use the `ApiKey` challenge. Credentials never
fall back to a different method after verification fails. Query strings, cookies,
and form fields are not credential sources. An authenticated identity missing a
required scope returns `403 {"error":"forbidden"}`; JWT responses also include
`Bearer error="insufficient_scope"`. Responses default to `Cache-Control: no-store`.
Failures never invoke the protected handler. Auth adds no credential logging;
existing HTTP request logs/traces/metrics still capture the response status.

### JWT access tokens

Configure a trusted, absolute HTTPS issuer and JWKS URL plus the API's audience.
The implementation uses private `go-oidc`/`go-jose` verification with additional
access-token checks: signature, exact issuer, audience membership, required
expiration, nonblank subject, protected token type, and optional `nbf`/`iat`.
Expiration is strict; future `nbf`/`iat` allow 30 seconds of clock skew. Keep
service and identity-provider clocks synchronized. The default allowed algorithm
is `RS256`; explicitly set `Algorithms` for other supported RSA, RSA-PSS, ECDSA,
or EdDSA algorithms. HMAC and unsigned tokens cannot be enabled.

The default protected `typ` header is **`at+jwt`**. For a provider whose access
tokens use `typ: JWT`, explicitly set `TokenType: "JWT"` and use a dedicated
resource-server audience **never used for ID tokens**. Do not use a browser
login client ID as the API audience. GOF does not issue tokens, perform login,
or introspect opaque tokens. Other provider-specific token-purpose checks remain
service-owned. Scopes come from OAuth's space-separated `scope` string and/or
the `scp` string array; role/group claims do not automatically grant scopes.

Some trusted legacy providers, including Cloudflare Access, issue signed access
assertions without a protected `typ` header. Set `AllowMissingTokenType: true`
explicitly for that provider and use a dedicated application audience that is
never used for ID tokens. The default remains false. This accepts only an absent
header; a present value (including an empty or mismatched value) must still
match `TokenType`. Signature, algorithm, issuer, audience, expiration, subject,
time and scope checks remain enforced. The service must retain any additional
provider-specific token-purpose checks.

JWKS keys are fetched lazily and cached by a shared verifier. A missing key or
failed signature triggers a refresh, limited to one HTTP fetch per second per
authenticator, with concurrent fetches coalesced. During that cooldown a new key
can receive 401 until a subsequent request refreshes it; publish replacement
keys before using them. Cached keys continue working during provider outages;
uncached/unverifiable keys deny access. Cached keys have no periodic eviction,
so removing a key from the provider alone is **not immediate revocation**.
Use short-lived access tokens and replace/restart the authenticator when urgent
cache invalidation is required. No token revocation list is maintained.

`JWTConfig.Timeout` defaults to 5 seconds for verification and JWKS requests.
Requests honor cancellation; a shared key fetch may finish after one caller
cancels, within the HTTP timeout. Tokens are limited to 16 KiB and JWKS responses
to 1 MiB. Redirects are rejected. Token-supplied key URLs are never followed;
GOF does not perform OIDC discovery. An optional standard `HTTPClient` is copied
and its transport borrowed, allowing private CAs and local TLS test servers;
the shorter of its existing timeout and the configured timeout applies. Callers
own custom transport cleanup. Transports must honor contexts. Plaintext URLs
require explicit `AllowInsecureHTTP: true`, intended only for development.

### API keys and identities

Load API-key secrets from your service's environment/secret store. Generate at
least 32 random bytes (using `crypto/rand.Read`, then hex-encoding); the SDK requires
32–4096 printable ASCII bytes without spaces or commas. Length validation does
not establish randomness. IDs must be unique, nonblank, and at most 256 bytes;
secrets must be unique too. GOF retains SHA-256 digests and compares digests in
constant time. Caller-owned configuration still contains the original secret.

API-key principals have `Method: "api_key"`, `Subject` equal to the configured
key ID, and the configured scopes. JWT principals have `Method: "jwt"`, issuer,
subject, and validated scopes. Identify JWT users by **issuer plus subject**;
different issuers can use the same subject. `auth.FromContext(ctx)` returns an
independent scope slice; `principal.HasScopes(...)` can check additional
permissions in service code. The service owns resource/tenant authorization.

Configuration is a startup snapshot: rotate keys by deploying overlapping old
and new entries under distinct IDs, migrating callers, then removing the old
entry and redeploying. API keys have no built-in expiry or persistent registry.
Use HTTPS between callers and the ingress, and protect ingress-to-service
traffic. Existing HTTP-client DEBUG curl logging includes full credentials by
design, including these headers; use it only with appropriately protected logs.

### Auth in Fx

```go
func protectedHTTPConfig(log *logger.Logger, gate *auth.Authenticator) httpserver.Config {
	return httpserver.Config{
		Logger: log,
		Routes: []httpserver.Route{{
			Pattern: "GET /orders",
			Handler: gate.Protect(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}), "orders:read"),
		}},
	}
}

app := fx.New(
	foundationfx.Module,
	foundationfx.AuthModule,
	foundationfx.HTTPServerModule,
	fx.Supply(logger.Config{Service: "orders"}),
	fx.Supply(auth.Config{APIKeys: []auth.APIKey{{
		ID: "inventory-service", Secret: cfg.InventoryAPIKey,
		Scopes: []string{"orders:read"},
	}}}),
	fx.Provide(protectedHTTPConfig),
)
```

`AuthModule` is opt-in and lazy; consuming its authenticator validates config
during graph construction. It owns no lifecycle hooks and changes no routes.
Wrap each protected route, or mount a protected standard router to cover a whole
application. Unwrapped routes remain public. The server's reserved health
endpoints remain public for probes; restrict their network exposure at ingress
when needed, especially `/health` and `/health/ui`.
