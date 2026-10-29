package foundationfx

import (
	"database/sql"

	"github.com/Penthious/go-foundation/pubsub"

	"go.uber.org/fx"
)

// FailedMessagesModule provides a lazy *pubsub.FailedStore and checks its schema
// at startup when consumed. Services pass it through ConsumerConfig.Failures.
var FailedMessagesModule = fx.Module("go-foundation/failed-messages", fx.Provide(newFailedStore))

type failedStoreParams struct {
	fx.In

	Lifecycle fx.Lifecycle
	DB        *sql.DB
	Config    pubsub.FailedStoreConfig `optional:"true"`
}

func newFailedStore(p failedStoreParams) (*pubsub.FailedStore, error) {
	store, err := pubsub.NewFailedStore(p.DB, p.Config)
	if err != nil {
		return nil, err
	}

	p.Lifecycle.Append(fx.Hook{OnStart: store.Ping})

	return store, nil
}
