package grpcserver

import (
	"crypto/tls"
	"strings"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/auth"
	"github.com/Penthious/go-foundation/ratelimit"

	"google.golang.org/grpc"
	testpb "google.golang.org/grpc/interop/grpc_testing"
)

func TestNew_ConfigValidation(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*Config)
	}{
		{"logger", func(c *Config) { c.Logger = nil }},
		{"registrations", func(c *Config) { c.Register = nil }},
		{"receive limit", func(c *Config) { c.MaxRecvMessageBytes = -1 }},
		{"send limit", func(c *Config) { c.MaxSendMessageBytes = -1 }},
		{"unary timeout", func(c *Config) { c.UnaryTimeout = -time.Second }},
		{"handshake timeout", func(c *Config) { c.ConnectionTimeout = -time.Second }},
		{"health timeout", func(c *Config) { c.HealthTimeout = -time.Second }},
		{"health interval", func(c *Config) { c.HealthWatchInterval = -time.Second }},
		{"TLS certificate", func(c *Config) { c.TLS = &tls.Config{} }},
		{"unconfigured auth", func(c *Config) { c.PublicMethods = []string{testpb.TestService_UnaryCall_FullMethodName} }},
		{"rate limiting without auth", func(c *Config) { c.RateLimiter = &ratelimit.Limiter{} }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(t, &testService{})
			tt.change(&cfg)

			if server, err := New(cfg); err == nil || server != nil {
				t.Fatal("invalid server config accepted")
			}
		})
	}
}

func TestNew_PoliciesAndSnapshots(t *testing.T) {
	a, err := auth.New(auth.Config{APIKeys: []auth.APIKey{{ID: "caller", Secret: strings.Repeat("s", 32)}}})
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct {
		name   string
		public []string
		scopes map[string][]string
	}{
		{"unknown public", []string{"/unknown/Method"}, nil},
		{"unknown scoped", nil, map[string][]string{"/unknown/Method": {"read"}}},
		{"conflict", []string{testpb.TestService_UnaryCall_FullMethodName}, map[string][]string{testpb.TestService_UnaryCall_FullMethodName: {"read"}}},
		{"invalid scope", nil, map[string][]string{testpb.TestService_UnaryCall_FullMethodName: {"read write"}}},
		{"reserved probe", nil, map[string][]string{"/grpc.health.v1.Health/Check": {"read"}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(t, &testService{})
			cfg.Auth, cfg.PublicMethods, cfg.RequireScopes = a, tt.public, tt.scopes

			if server, err := New(cfg); err == nil || server != nil {
				t.Fatal("invalid method policy accepted")
			}
		})
	}

	cfg := testConfig(t, &testService{})
	cfg.Auth = a
	cfg.RequireScopes = map[string][]string{testpb.TestService_UnaryCall_FullMethodName: {"read"}}
	cfg.PublicMethods = []string{testpb.TestService_EmptyCall_FullMethodName}
	server, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	cfg.RequireScopes[testpb.TestService_UnaryCall_FullMethodName][0] = "changed"
	cfg.PublicMethods[0] = "changed"
	cfg.Register[0] = func(grpc.ServiceRegistrar) { panic("changed") }
	if server.cfg.RequireScopes[testpb.TestService_UnaryCall_FullMethodName][0] != "read" || !server.public[testpb.TestService_EmptyCall_FullMethodName] || server.grpc != nil {
		t.Fatal("config was not copied or runtime was constructed eagerly")
	}

	if server.cfg.UnaryTimeout != 30*time.Second || server.cfg.MaxRecvMessageBytes != 4<<20 || server.cfg.MaxSendMessageBytes != 4<<20 || server.cfg.MaxHeaderBytes != 64<<10 || server.cfg.MaxConcurrentStreams != 100 {
		t.Fatal("unexpected server defaults")
	}
}
