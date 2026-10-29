package grpcserver

import (
	"errors"
	"reflect"
	"slices"
	"strings"

	"google.golang.org/grpc"
)

type registration struct {
	desc           grpc.ServiceDesc
	implementation any
}

// Collect and validate before touching grpc.Server: its duplicate/type checks
// terminate the process instead of returning errors to the constructor.
type registrar struct {
	services []registration
	err      error
}

func (r *registrar) RegisterService(desc *grpc.ServiceDesc, implementation any) {
	if r.err != nil {
		return
	}

	if desc == nil || !validService(desc.ServiceName) || desc.ServiceName == "grpc.health.v1.Health" || strings.HasPrefix(desc.ServiceName, "grpc.reflection.") {
		r.err = errors.New("grpcserver: invalid or reserved service registration")
		return
	}

	contract := reflect.TypeOf(desc.HandlerType)
	handler := reflect.ValueOf(implementation)
	if contract == nil || contract.Kind() != reflect.Pointer || contract.Elem().Kind() != reflect.Interface || !handler.IsValid() || !handler.Type().Implements(contract.Elem()) || handler.Kind() == reflect.Pointer && handler.IsNil() {
		r.err = errors.New("grpcserver: service handler does not implement its generated contract")
		return
	}

	for _, existing := range r.services {
		if existing.desc.ServiceName == desc.ServiceName {
			r.err = errors.New("grpcserver: duplicate service registration")
			return
		}
	}

	methods := make(map[string]bool)
	for _, method := range desc.Methods {
		if !validIdentifier(method.MethodName) || method.Handler == nil || methods[method.MethodName] {
			r.err = errors.New("grpcserver: invalid or duplicate method registration")
			return
		}

		methods[method.MethodName] = true
	}

	for _, stream := range desc.Streams {
		if !validIdentifier(stream.StreamName) || stream.Handler == nil || methods[stream.StreamName] {
			r.err = errors.New("grpcserver: invalid or duplicate stream registration")
			return
		}

		methods[stream.StreamName] = true
	}

	copy := *desc
	copy.Methods, copy.Streams = slices.Clone(desc.Methods), slices.Clone(desc.Streams)
	r.services = append(r.services, registration{desc: copy, implementation: implementation})
}

func collect(callbacks []func(grpc.ServiceRegistrar)) (result []registration, err error) {
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			err = errors.New("grpcserver: service registration panicked")
		}
	}()
	r := &registrar{}
	for _, callback := range callbacks {
		if callback == nil {
			returned = true
			return nil, errors.New("grpcserver: nil service registration")
		}

		callback(r)
		if r.err != nil {
			returned = true
			return nil, r.err
		}
	}

	returned = true
	if len(r.services) == 0 {
		return nil, errors.New("grpcserver: no application services registered")
	}

	return r.services, nil
}

func validService(name string) bool {
	for _, part := range strings.Split(name, ".") {
		if !validIdentifier(part) {
			return false
		}
	}

	return true
}

func validIdentifier(name string) bool {
	if name == "" {
		return false
	}

	for i, c := range name {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' || i > 0 && c >= '0' && c <= '9') {
			return false
		}
	}

	return true
}
