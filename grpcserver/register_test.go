package grpcserver

import (
	"strings"
	"testing"

	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	testpb "google.golang.org/grpc/interop/grpc_testing"
)

func TestNew_InvalidRegistrationsReturnErrors(t *testing.T) {
	for _, tt := range []struct {
		name     string
		register func(grpc.ServiceRegistrar)
	}{
		{"nil callback", nil},
		{"no services", func(grpc.ServiceRegistrar) {}},
		{"panic", func(grpc.ServiceRegistrar) { panic("sensitive registration") }},
		{"nil panic", func(grpc.ServiceRegistrar) { panic(nil) }},
		{"nil descriptor", func(r grpc.ServiceRegistrar) { r.RegisterService(nil, &testService{}) }},
		{"wrong implementation", func(r grpc.ServiceRegistrar) { r.RegisterService(&testpb.TestService_ServiceDesc, &struct{}{}) }},
		{"typed nil", func(r grpc.ServiceRegistrar) { r.RegisterService(&testpb.TestService_ServiceDesc, (*testService)(nil)) }},
		{"duplicate service", func(r grpc.ServiceRegistrar) {
			testpb.RegisterTestServiceServer(r, &testService{})
			testpb.RegisterTestServiceServer(r, &testService{})
		}},
		{"reserved", func(r grpc.ServiceRegistrar) {
			r.RegisterService(&healthpb.Health_ServiceDesc, &healthpb.UnimplementedHealthServer{})
		}},
		{"bad method", func(r grpc.ServiceRegistrar) {
			desc := testpb.TestService_ServiceDesc
			desc.Methods = []grpc.MethodDesc{{MethodName: "bad/name"}}
			r.RegisterService(&desc, &testService{})
		}},
		{"duplicate method", func(r grpc.ServiceRegistrar) {
			desc := testpb.TestService_ServiceDesc
			desc.Methods = []grpc.MethodDesc{desc.Methods[0], desc.Methods[0]}
			r.RegisterService(&desc, &testService{})
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(t, &testService{})
			cfg.Register = []func(grpc.ServiceRegistrar){tt.register}
			server, err := New(cfg)

			if err == nil || server != nil || strings.Contains(err.Error(), "sensitive") {
				t.Fatalf("registration did not fail safely: %v", err)
			}
		})
	}
}
