package foundationfx_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/foundationfx"
	"github.com/Penthious/go-foundation/inbox"
	"github.com/Penthious/go-foundation/postgres"

	"go.uber.org/fx"
)

func TestInboxModule_LazyAndInvalidConfig(t *testing.T) {
	app := fx.New(foundationfx.InboxModule, fx.NopLogger)
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}

	if err := app.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := app.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}

	var store *inbox.Store
	invalid := fx.New(
		foundationfx.InboxModule, fx.NopLogger,
		fx.Supply(&sql.DB{}, inbox.Config{Timeout: -time.Second}),
		fx.Populate(&store),
	)
	if invalid.Err() == nil || store != nil {
		t.Fatal("invalid inbox config accepted")
	}
}

func TestInboxModule_StartupChecksAndBorrowedDatabase(t *testing.T) {
	for _, migrate := range []bool{true, false} {
		t.Run(map[bool]string{true: "schema available", false: "missing schema"}[migrate], func(t *testing.T) {
			dsn := outboxDatabase(t, false)
			if migrate {
				db, err := postgres.New(postgres.Config{DSN: dsn})
				if err != nil {
					t.Fatal(err)
				}

				defer db.Close()
				if _, err := db.ExecContext(t.Context(), inbox.Schema()); err != nil {
					t.Fatal(err)
				}
			}

			var db *sql.DB
			var store, sameStore *inbox.Store
			app := fx.New(
				foundationfx.PostgresModule(), foundationfx.InboxModule, fx.NopLogger,
				fx.Supply(postgres.Config{DSN: dsn}),
				fx.Populate(&db, &store, &sameStore),
			)
			if err := app.Err(); err != nil {
				t.Fatal(err)
			}

			if store != sameStore || db.Stats().OpenConnections != 0 {
				t.Fatal("inbox is not a lazy singleton")
			}

			err := app.Start(t.Context())
			if !migrate {
				if err == nil {
					t.Fatal("startup accepted missing inbox schema")
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
