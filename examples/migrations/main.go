// This command demonstrates a service-owned migration binary. Copy it and its
// embedded SQL directory into a service, adapting the schemas and version choices.
package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Penthious/go-foundation/migrate"
	"github.com/Penthious/go-foundation/postgres"
)

//go:embed sql/*.sql
var migrations embed.FS

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) != 1 || (args[0] != "up" && args[0] != "down" && args[0] != "status" && args[0] != "version") {
		return errors.New("usage: migrate up|down|status|version")
	}

	db, err := postgres.New(postgres.Config{DSN: os.Getenv("DATABASE_URL")})
	if err != nil {
		return err
	}

	defer db.Close()

	runner, err := migrate.New(db, migrate.Config{FS: migrations, Dir: "sql", InboxVersion: 1, OutboxVersion: 2})
	if err != nil {
		return err
	}

	switch args[0] {
	case "up":
		return runner.Up(ctx)
	case "down":
		return runner.Down(ctx)
	case "version":
		version, err := runner.Version(ctx)
		if err != nil {
			return err
		}

		fmt.Println(version)
	case "status":
		statuses, err := runner.Status(ctx)
		if err != nil {
			return err
		}

		for _, status := range statuses {
			state := "pending"
			if status.Applied {
				state = "applied"
			}

			fmt.Printf("%d\t%s\t%s\n", status.Version, state, status.Name)
		}
	}

	return nil
}
