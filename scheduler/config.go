package scheduler

import (
	"cmp"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/Penthious/go-foundation/logger"

	"github.com/robfig/cron/v3"
)

// Mode selects whether a job runs independently or coordinates through PostgreSQL.
type Mode string

// Supported job execution modes. An empty mode selects Local.
const (
	Local       Mode = "local"
	Coordinated Mode = "coordinated"
)

// Config configures one scheduler. Dependencies are borrowed and must outlive it.
type Config struct {
	// Namespace identifies a service's schedules, shared by coordinating replicas.
	Namespace string
	Logger    *logger.Logger
	// DB is required only when at least one job is Coordinated.
	DB *sql.DB
	// MaxConcurrentJobs defaults to 10, capped at the registered job count.
	MaxConcurrentJobs int
	// PollInterval defaults to 1s for coordinated jobs; StorageTimeout defaults
	// to 5s for each storage operation, including registration and lease release.
	PollInterval   time.Duration
	StorageTimeout time.Duration
}

type job struct {
	Job
	calendar cron.Schedule
	location *time.Location
	spec     string
}

func normalizeConfig(cfg Config, jobs []Job) (Config, []job, error) {
	if !validName(cfg.Namespace) || cfg.Logger == nil || len(jobs) == 0 || cfg.MaxConcurrentJobs < 0 {
		return Config{}, nil, errors.New("scheduler: namespace, logger and at least one job are required; concurrency must not be negative")
	}

	if cfg.MaxConcurrentJobs == 0 {
		cfg.MaxConcurrentJobs = 10
	}

	cfg.MaxConcurrentJobs = min(cfg.MaxConcurrentJobs, len(jobs))
	for _, duration := range []struct {
		value    *time.Duration
		fallback time.Duration
	}{
		{&cfg.PollInterval, time.Second},
		{&cfg.StorageTimeout, 5 * time.Second},
	} {
		if *duration.value == 0 {
			*duration.value = duration.fallback
		}

		if *duration.value < time.Millisecond {
			return Config{}, nil, errors.New("scheduler: polling and storage durations must be at least 1ms")
		}
	}

	parsed := make([]job, 0, len(jobs))
	names := make(map[string]bool)
	for _, input := range jobs {
		if !validName(input.Name) || input.Handler == nil || names[input.Name] {
			return Config{}, nil, errors.New("scheduler: jobs require unique identifier names and handlers")
		}

		names[input.Name] = true
		if input.Mode == "" {
			input.Mode = Local
		}

		if input.Mode != Local && input.Mode != Coordinated || input.Mode == Coordinated && cfg.DB == nil {
			return Config{}, nil, errors.New("scheduler: unsupported mode or missing coordination database")
		}

		if input.Timeout == 0 {
			input.Timeout = 5 * time.Minute
		}

		if input.Timeout < time.Millisecond || input.Timeout > time.Duration(1<<63-1)/2 {
			return Config{}, nil, errors.New("scheduler: job timeout must be at least 1ms and fit a doubled lease")
		}

		entry, err := parseJob(input)
		if err != nil {
			return Config{}, nil, err
		}

		parsed = append(parsed, entry)
	}

	// Replicas must acquire registration locks in the same order, including Fx groups.
	slices.SortFunc(parsed, func(a, b job) int { return cmp.Compare(a.Name, b.Name) })

	return cfg, parsed, nil
}

func parseJob(input Job) (job, error) {
	input.Cron = strings.Join(strings.Fields(input.Cron), " ")
	if (input.Cron == "") == (input.Every == 0) || input.Every < 0 || input.Every > 0 && input.Every < time.Millisecond {
		return job{}, errors.New("scheduler: specify exactly one cron expression or interval of at least 1ms")
	}

	if input.Timezone == "" {
		input.Timezone = "UTC"
	}

	location, err := time.LoadLocation(input.Timezone)
	if err != nil || input.Timezone == "Local" {
		return job{}, errors.New("scheduler: timezone must be UTC or an IANA timezone, not Local")
	}

	entry := job{Job: input, location: location}
	if input.Every > 0 {
		entry.spec = "every:" + input.Every.String()
		return entry, nil
	}

	if strings.HasPrefix(input.Cron, "TZ=") || strings.HasPrefix(input.Cron, "CRON_TZ=") || strings.HasPrefix(input.Cron, "@every") {
		return job{}, errors.New("scheduler: use Timezone and Every fields instead of inline timezone/interval directives")
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	calendar, err := parser.Parse(input.Cron)
	if err != nil {
		return job{}, fmt.Errorf("scheduler: invalid cron expression for job %q", input.Name)
	}

	entry.calendar, entry.spec = calendar, "cron:"+input.Cron
	if entry.next(time.Now()).IsZero() {
		return job{}, fmt.Errorf("scheduler: job %q has no upcoming occurrence", input.Name)
	}

	return entry, nil
}

func (j job) next(now time.Time) time.Time {
	if j.Every > 0 {
		return now.UTC().Truncate(j.Every).Add(j.Every)
	}

	return j.calendar.Next(now.In(j.location)).UTC()
}

func validName(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}

	for _, c := range value {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-') {
			return false
		}
	}

	return true
}
