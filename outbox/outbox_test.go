package outbox

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/postgres"
	"github.com/Penthious/go-foundation/pubsub"
)

func TestEnqueue_RequiresTransaction(t *testing.T) {
	if err := Enqueue(t.Context(), nil, pubsub.Message{ID: "event-1", Topic: "orders.created"}); err == nil {
		t.Fatal("nil transaction accepted")
	}
}

func TestEnqueue_AtomicCommitAndRollback(t *testing.T) {
	db := testDB(t, true)
	if _, err := db.ExecContext(t.Context(), "CREATE TABLE orders (id integer PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}

	for _, commit := range []bool{false, true} {
		tx, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}

		defer tx.Rollback()
		if _, err := tx.ExecContext(t.Context(), "INSERT INTO orders VALUES (1)"); err != nil {
			t.Fatal(err)
		}

		message := pubsub.Message{ID: "order-created-1", Topic: "orders.created", Data: []byte{0, 1, 255}}
		if err := Enqueue(t.Context(), tx, message); err != nil {
			t.Fatal(err)
		}

		if count(t, db, "public.gof_outbox") != 0 || count(t, db, "orders") != 0 {
			t.Fatal("uncommitted changes became visible")
		}

		if commit {
			err = tx.Commit()
		} else {
			err = tx.Rollback()
		}

		if err != nil {
			t.Fatal(err)
		}

		want := 0
		if commit {
			want = 1
		}

		if count(t, db, "public.gof_outbox") != want || count(t, db, "orders") != want {
			t.Fatal("business write and event did not commit/rollback together")
		}
	}

	var payload []byte
	if err := db.QueryRowContext(t.Context(), "SELECT data FROM public.gof_outbox").Scan(&payload); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(payload, []byte{0, 1, 255}) {
		t.Fatal("binary payload changed")
	}
}

func TestEnqueue_ErrorsPreserveTransactionOwnership(t *testing.T) {
	db := testDB(t, true)
	queue(t, db, "duplicate")

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}

	defer tx.Rollback()
	if err := Enqueue(t.Context(), tx, pubsub.Message{ID: "invalid", Topic: "orders.*"}); err == nil {
		t.Fatal("invalid topic was queued")
	}

	// Validation leaves the transaction usable; the service still decides its outcome.
	if err := Enqueue(t.Context(), tx, pubsub.Message{ID: "valid", Topic: "orders.created"}); err != nil {
		t.Fatal(err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if err := Enqueue(t.Context(), tx, pubsub.Message{ID: "closed", Topic: "orders.created"}); !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("closed transaction error = %v", err)
	}

	tx, err = db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}

	defer tx.Rollback()
	if err := Enqueue(t.Context(), tx, pubsub.Message{ID: "duplicate", Topic: "orders.created", Data: []byte("different")}); err == nil {
		t.Fatal("duplicate ID silently overwrote or discarded an event")
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if count(t, db, "public.gof_outbox") != 1 {
		t.Fatal("failed enqueue changed committed events")
	}
}

func testDB(t *testing.T, migrate bool) *sql.DB {
	t.Helper()

	dsn := os.Getenv("GO_FOUNDATION_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set GO_FOUNDATION_POSTGRES_TEST_DSN to a PostgreSQL URL with CREATEDB permission")
	}

	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		t.Fatal("integration DSN must be a PostgreSQL URL")
	}

	admin, err := postgres.New(postgres.Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = admin.Close() })
	name := "gof_outbox_" + strings.ToLower(rand.Text())
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}

	u.Path, u.RawPath = "/"+name, ""
	db, err := postgres.New(postgres.Config{DSN: u.String()})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if _, err := admin.ExecContext(ctx, "DROP DATABASE "+name+" WITH (FORCE)"); err != nil {
			t.Error(err)
		}
	})
	if migrate {
		if _, err := db.ExecContext(ctx, Schema()); err != nil {
			t.Fatal(err)
		}
	}

	return db
}

func queue(t *testing.T, db *sql.DB, id string) {
	t.Helper()

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}

	defer tx.Rollback()
	if err := Enqueue(t.Context(), tx, pubsub.Message{ID: id, Topic: "orders.created"}); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func count(t *testing.T, db *sql.DB, table string) int {
	t.Helper()

	var count int
	if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM "+table).Scan(&count); err != nil {
		t.Fatal(err)
	}

	return count
}
