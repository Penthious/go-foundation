package inbox

import (
	"database/sql"
	"errors"
	"time"
)

// Config controls inbox transactions. Zero values use defaults.
type Config struct {
	// Timeout defaults to 30s, covering pool waits, duplicate-key waits, the
	// handler, and commit. Handlers must honor cancellation.
	Timeout time.Duration
	// Isolation defaults to sql.LevelReadCommitted. RepeatableRead and
	// Serializable are also supported; serialization failures return for retry.
	Isolation sql.IsolationLevel
}

func normalizeConfig(cfg Config) (Config, error) {
	if cfg.Timeout < 0 {
		return Config{}, errors.New("inbox: timeout must not be negative")
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}

	switch cfg.Isolation {
	case sql.LevelDefault:
		cfg.Isolation = sql.LevelReadCommitted
	case sql.LevelReadCommitted, sql.LevelRepeatableRead, sql.LevelSerializable:
	default:
		return Config{}, errors.New("inbox: unsupported isolation level")
	}

	return cfg, nil
}
