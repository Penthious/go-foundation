package foundationfx

import (
	"github.com/Penthious/go-foundation/config"

	"go.uber.org/fx"
)

// Config loads and validates one service-owned config exactly once during Fx
// graph construction, even if no other constructor consumes it. A failure is
// available through app.Err() before lifecycle startup. Registered constructors
// receive T, not a pointer or a global config holder. Use fx.Supply instead when
// configuration was already loaded before constructing the app.
func Config[T any](options ...config.Options) fx.Option {
	return fx.Options(
		fx.Provide(func() (T, error) {
			return config.Load[T](options...)
		}),
		fx.Invoke(func(T) {}),
	)
}
