package migrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"time"

	"github.com/pressly/goose/v3/lock"
)

const migrationLockID int64 = 0x474f465f4d4947

type sessionLocker struct {
	delegate lock.SessionLocker
	runner   *Runner
}

func (l *sessionLocker) SessionLock(ctx context.Context, conn *sql.Conn) error {
	if err := l.delegate.SessionLock(ctx, conn); err != nil {
		// An interrupted lock response is ambiguous. Discard the physical session
		// so an acquired advisory lock cannot leak back into the database pool.
		discard(conn)
		return err
	}

	if err := l.runner.validateHistory(ctx, conn); err != nil {
		return errors.Join(err, l.SessionUnlock(context.WithoutCancel(ctx), conn))
	}

	return nil
}

func (l *sessionLocker) SessionUnlock(ctx context.Context, conn *sql.Conn) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := l.delegate.SessionUnlock(ctx, conn); err != nil {
		discard(conn)
		return err
	}

	return nil
}

func discard(conn *sql.Conn) {
	_ = conn.Raw(func(any) error { return driver.ErrBadConn })
}
