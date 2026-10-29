package grpcclient

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func (c *Client) prepare(ctx context.Context, method string) (context.Context, error) {
	if err := c.contextError(ctx); err != nil {
		return ctx, err
	}

	if !validMethod(method) {
		return ctx, status.Error(codes.InvalidArgument, "grpcclient: invalid full RPC method")
	}

	values, _ := metadata.FromOutgoingContext(ctx)
	if values == nil {
		values = make(metadata.MD)
	}

	count := len(values.Get("authorization")) + len(values.Get("x-api-key"))
	configured := c.cfg.APIKey != "" || c.cfg.BearerToken != "" || c.cfg.TokenProvider != nil
	if count > 1 || configured && count > 0 {
		return ctx, credentialError()
	}

	if c.cfg.APIKey != "" {
		values.Set("x-api-key", c.cfg.APIKey)
	} else if c.cfg.BearerToken != "" || c.cfg.TokenProvider != nil {
		token := c.cfg.BearerToken
		if c.cfg.TokenProvider != nil {
			var ok bool
			token, ok = fetchToken(ctx, c.cfg.TokenProvider)
			if err := c.contextError(ctx); err != nil {
				return ctx, err
			}

			if !ok {
				return ctx, credentialError()
			}
		}

		values.Set("authorization", "Bearer "+token)
	}

	if keys := values.Get("x-api-key"); len(keys) == 1 && !validCredential(keys[0], 4096) {
		return ctx, credentialError()
	}

	if tokens := values.Get("authorization"); len(tokens) == 1 {
		scheme, token, ok := strings.Cut(tokens[0], " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") || !validCredential(token, 16<<10) {
			return ctx, credentialError()
		}
	}

	return metadata.NewOutgoingContext(ctx, values), nil
}

func (c *Client) contextError(ctx context.Context) error {
	if c.closed.Load() {
		return ErrClosed
	}

	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}

	return nil
}

func credentialError() error {
	return status.Error(codes.Unauthenticated, "grpcclient: missing, invalid or conflicting credentials")
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

func validMethod(method string) bool {
	parts := strings.Split(method, "/")
	if len(parts) != 3 || parts[0] != "" || len(method) > 1024 || parts[1] == "" || parts[2] == "" {
		return false
	}

	for _, part := range append(strings.Split(parts[1], "."), parts[2]) {
		if part == "" {
			return false
		}

		for i, char := range part {
			if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char == '_' || i > 0 && char >= '0' && char <= '9') {
				return false
			}
		}
	}

	return true
}
