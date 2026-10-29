package foundationfx

import (
	"database/sql"

	"github.com/Penthious/go-foundation/inbox"

	"go.uber.org/fx"
)

// InboxModule provides a lazy *inbox.Store and checks its schema on startup when
// consumed. Include PostgresModule or provide *sql.DB; inbox.Config is optional.
// It starts no workers and borrows the database's lifecycle. Services supervise
// ConsumeTransactional with a service-lifetime context and join it at shutdown.
var InboxModule = fx.Module("go-foundation/inbox", fx.Provide(newInbox))

type inboxParams struct {
	fx.In

	Lifecycle fx.Lifecycle
	DB        *sql.DB
	Config    inbox.Config `optional:"true"`
}

func newInbox(p inboxParams) (*inbox.Store, error) {
	store, err := inbox.New(p.DB, p.Config)
	if err != nil {
		return nil, err
	}

	p.Lifecycle.Append(fx.Hook{OnStart: store.Ping})

	return store, nil
}
