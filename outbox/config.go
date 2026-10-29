package outbox

import (
	"errors"
	"time"

	"github.com/Penthious/go-foundation/logger"
)

// Config controls one relay. Each Run invocation publishes one event at a time.
type Config struct {
	// Logger is required. GOF logs event metadata and errors without logging Data.
	Logger *logger.Logger
	// PollInterval defaults to 1s after an empty queue or database error.
	PollInterval time.Duration
	// RetryDelay defaults to 5s after a failed publication. Retries never discard
	// an event; persistent failures require operator intervention.
	RetryDelay time.Duration
	// Timeout defaults to 10s for one claim, publish, and acknowledgment cycle.
	// A claim's lease lasts twice this value. Ambiguous/expired operations leave
	// their lease to expire, then another relay can retry the same event ID.
	Timeout time.Duration
}

func normalizeConfig(cfg Config) (Config, error) {
	if cfg.Logger == nil {
		return Config{}, errors.New("outbox: logger is required")
	}

	for _, duration := range []struct {
		value    *time.Duration
		fallback time.Duration
	}{
		{&cfg.PollInterval, time.Second},
		{&cfg.RetryDelay, 5 * time.Second},
		{&cfg.Timeout, 10 * time.Second},
	} {
		if *duration.value < 0 {
			return Config{}, errors.New("outbox: durations must not be negative")
		}

		if *duration.value == 0 {
			*duration.value = duration.fallback
		}

		if *duration.value < time.Millisecond {
			return Config{}, errors.New("outbox: durations must be at least 1ms")
		}
	}

	if cfg.Timeout > time.Duration(1<<63-1)/2 {
		return Config{}, errors.New("outbox: timeout must fit a doubled lease duration")
	}

	return cfg, nil
}
