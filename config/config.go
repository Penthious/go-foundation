// Package config loads startup environment configuration into service-owned structs.
package config

import (
	"errors"
	"fmt"
	"maps"

	"github.com/caarlos0/env/v11"
)

// Options selects an optional prefix and environment source.
type Options struct {
	// Prefix is prepended verbatim to env tags, including nested prefixes.
	Prefix string
	// Environment replaces the process environment when non-nil. An empty map
	// selects no variables, useful for deterministic tests. Treat inputs as
	// immutable while Load runs; the parser receives a copy of this map.
	Environment map[string]string
}

// Load populates a new T, which must be a struct, using env, envDefault, and
// envPrefix tags. It accepts zero or one Options value. Fields without env tags
// are ignored, except nested structs. Missing or empty variables use envDefault.
// Supported conversions include scalars, durations, slices, maps, pointers, and
// encoding.TextUnmarshaler. Use required and notEmpty env options for presence
// and nonempty checks. If *T implements Validate() error, it is called once after
// parsing succeeds. Nested validators are not called automatically.
//
// A failure returns the zero T, never a partially populated config. Parser errors
// omit values and private parser causes. Service-owned validation errors retain
// their causes, so validators must avoid including secrets in their messages.
// Load takes a startup snapshot; it neither watches nor reloads configuration.
func Load[T any](options ...Options) (T, error) {
	var zero T
	if len(options) > 1 {
		return zero, errors.New("config: Load accepts at most one Options value")
	}

	var opts Options
	if len(options) == 1 {
		opts = options[0]
	}

	cfg, err := env.ParseAsWithOptions[T](env.Options{
		Prefix: opts.Prefix, Environment: maps.Clone(opts.Environment),
	})
	if err != nil {
		return zero, parserError(err)
	}

	if validator, ok := any(&cfg).(interface{ Validate() error }); ok {
		if err := validator.Validate(); err != nil {
			return zero, fmt.Errorf("config: validation failed: %w", err)
		}
	}

	return cfg, nil
}

// Parser causes may embed secret values, including inside strconv errors.
// Replace them rather than wrapping them into the returned error chain.
func parserError(err error) error {
	switch err := err.(type) {
	case env.AggregateError:
		errs := make([]error, 0, len(err.Errors))
		for _, item := range err.Errors {
			errs = append(errs, parserError(item))
		}

		return errors.Join(errs...)
	case env.ParseError:
		return fmt.Errorf("config: field %q has an invalid value", err.Name)
	case env.NoParserError:
		return fmt.Errorf("config: field %q has an unsupported type", err.Name)
	case env.VarIsNotSetError:
		return fmt.Errorf("config: required environment variable %q is not set", err.Key)
	case env.EmptyVarError:
		return fmt.Errorf("config: environment variable %q must not be empty", err.Key)
	case env.LoadFileContentError:
		return fmt.Errorf("config: cannot read file for environment variable %q", err.Key)
	case env.NotStructPtrError:
		return errors.New("config: T must be a struct")
	case env.NoSupportedTagOptionError:
		return errors.New("config: unsupported env tag option")
	default:
		return errors.New("config: invalid environment configuration")
	}
}
