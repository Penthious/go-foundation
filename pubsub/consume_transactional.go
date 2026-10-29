package pubsub

import (
	"context"
	"database/sql"
	"errors"

	"github.com/Penthious/go-foundation/inbox"
)

// TransactionalHandler writes through the inbox-owned transaction. Do not commit,
// roll back, retain it, or use a separate database connection for protected writes.
// External side effects need their own idempotency; use outbox for outgoing events.
type TransactionalHandler func(context.Context, *sql.Tx, Message) error

// ConsumeTransactional wraps Consume with atomic inbox deduplication. Its stable
// consumer namespace is Group + ":" + Topic, independent of service instances.
// A committed duplicate skips the handler and is acknowledged normally. Errors
// and panics roll back and cause redelivery. Apply inbox.Schema and check
// store.Ping at startup (or use foundationfx.InboxModule) before consuming.
func (c *Client) ConsumeTransactional(ctx context.Context, cfg ConsumerConfig, store *inbox.Store, handler TransactionalHandler) error {
	if store == nil || handler == nil {
		return errors.New("pubsub: inbox store and transactional handler are required")
	}

	return c.Consume(ctx, cfg, func(ctx context.Context, message Message) error {
		_, err := store.Process(ctx, inbox.Key{Consumer: cfg.Group + ":" + cfg.Topic, MessageID: message.ID}, func(ctx context.Context, tx *sql.Tx) error {
			return handler(ctx, tx, message)
		})

		return err
	})
}
