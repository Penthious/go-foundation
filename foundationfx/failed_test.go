package foundationfx_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/foundationfx"
	"github.com/Penthious/go-foundation/postgres"
	"github.com/Penthious/go-foundation/pubsub"

	"go.uber.org/fx"
)

func TestFailedMessagesModule_LazyAndInvalidConfig(t *testing.T) {
	app := fx.New(foundationfx.FailedMessagesModule, fx.NopLogger)
	if err := app.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := app.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}

	var store *pubsub.FailedStore
	invalid := fx.New(foundationfx.FailedMessagesModule, fx.NopLogger,
		fx.Supply(&sql.DB{}, pubsub.FailedStoreConfig{Timeout: -time.Second}), fx.Populate(&store))
	if invalid.Err() == nil || store != nil {
		t.Fatal("invalid failed-message config accepted")
	}
}

func TestFailedMessagesModule_SchemaAndLifecycle(t *testing.T) {
	for _, migrate := range []bool{true, false} {
		t.Run(map[bool]string{true: "schema available", false: "missing schema"}[migrate], func(t *testing.T) {
			dsn := outboxDatabase(t, false)
			if migrate {
				db, err := postgres.New(postgres.Config{DSN: dsn})
				if err != nil {
					t.Fatal(err)
				}

				defer db.Close()
				if _, err := db.ExecContext(t.Context(), pubsub.FailedSchema()); err != nil {
					t.Fatal(err)
				}
			}

			var db *sql.DB
			var store *pubsub.FailedStore
			app := fx.New(foundationfx.PostgresModule(), foundationfx.FailedMessagesModule, fx.NopLogger,
				fx.Supply(postgres.Config{DSN: dsn}), fx.Populate(&db, &store))
			if err := app.Err(); err != nil {
				t.Fatal(err)
			}

			if db.Stats().OpenConnections != 0 {
				t.Fatal("construction opened a database connection")
			}

			err := app.Start(t.Context())
			if !migrate {
				if err == nil {
					t.Fatal("startup accepted missing failed-message schema")
				}

				assertClosedDB(t, db)
				return
			}

			if err != nil {
				t.Fatal(err)
			}

			if err := store.Ping(t.Context()); err != nil {
				t.Fatal(err)
			}

			if err := app.Stop(t.Context()); err != nil {
				t.Fatal(err)
			}

			assertClosedDB(t, db)
		})
	}
}
