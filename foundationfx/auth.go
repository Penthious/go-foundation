package foundationfx

import (
	"github.com/Penthious/go-foundation/auth"

	"go.uber.org/fx"
)

// AuthModule provides a lazy *auth.Authenticator from a supplied auth.Config.
// Services wrap their own handlers with Protect; this module changes no routes
// and starts no background workers. JWT keys are fetched on the first request.
var AuthModule = fx.Module("go-foundation/auth", fx.Provide(auth.New))
