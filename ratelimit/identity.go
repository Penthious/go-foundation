package ratelimit

import (
	"context"
	"encoding/binary"

	"github.com/Penthious/go-foundation/auth"
)

func (l *Limiter) allowPrincipal(ctx context.Context) (Decision, error) {
	if err := ctx.Err(); err != nil {
		return Decision{}, err
	}

	p, ok := auth.FromContext(ctx)
	if !ok || p.Subject == "" || len(p.Method)+len(p.Issuer)+len(p.Subject)+12 > 4096 {
		return Decision{}, auth.ErrUnauthenticated
	}

	if p.Method != "api_key" && p.Method != "jwt" || p.Method == "jwt" && p.Issuer == "" {
		return Decision{}, auth.ErrUnauthenticated
	}

	// Length prefixes isolate methods/issuers without delimiter ambiguities.
	var identity []byte
	for _, part := range []string{p.Method, p.Issuer, p.Subject} {
		identity = binary.BigEndian.AppendUint32(identity, uint32(len(part)))
		identity = append(identity, part...)
	}

	return l.Allow(ctx, string(identity))
}
