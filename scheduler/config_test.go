package scheduler

import (
	"context"
	"database/sql"
	"io"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/logger"
)

func TestNew_Configuration(t *testing.T) {
	valid := Job{Name: "cleanup", Every: time.Minute, Handler: func(context.Context, Run) error { return nil }}
	for _, tt := range []struct {
		name   string
		change func(*Config, *Job)
	}{
		{"namespace", func(c *Config, _ *Job) { c.Namespace = "bad:name" }},
		{"logger", func(c *Config, _ *Job) { c.Logger = nil }},
		{"concurrency", func(c *Config, _ *Job) { c.MaxConcurrentJobs = -1 }},
		{"poll", func(c *Config, _ *Job) { c.PollInterval = -time.Second }},
		{"storage timeout", func(c *Config, _ *Job) { c.StorageTimeout = time.Nanosecond }},
		{"name", func(_ *Config, j *Job) { j.Name = " " }},
		{"handler", func(_ *Config, j *Job) { j.Handler = nil }},
		{"mode", func(_ *Config, j *Job) { j.Mode = "unknown" }},
		{"database", func(_ *Config, j *Job) { j.Mode = Coordinated }},
		{"no schedule", func(_ *Config, j *Job) { j.Every = 0 }},
		{"two schedules", func(_ *Config, j *Job) { j.Cron = "* * * * *" }},
		{"tiny interval", func(_ *Config, j *Job) { j.Every = time.Nanosecond }},
		{"negative interval", func(_ *Config, j *Job) { j.Every = -time.Second }},
		{"timeout", func(_ *Config, j *Job) { j.Timeout = -time.Second }},
		{"lease overflow", func(_ *Config, j *Job) { j.Timeout = time.Duration(1<<63 - 1) }},
		{"unknown timezone", func(_ *Config, j *Job) { j.Timezone = "invalid/timezone" }},
		{"OS timezone", func(_ *Config, j *Job) { j.Timezone = "Local" }},
		{"cron seconds", func(_ *Config, j *Job) { j.Every = 0; j.Cron = "* * * * * *" }},
		{"cron syntax", func(_ *Config, j *Job) { j.Every = 0; j.Cron = "not cron" }},
		{"impossible date", func(_ *Config, j *Job) { j.Every = 0; j.Cron = "0 0 30 2 *" }},
		{"inline timezone", func(_ *Config, j *Job) { j.Every = 0; j.Cron = "CRON_TZ=UTC * * * * *" }},
		{"inline interval", func(_ *Config, j *Job) { j.Every = 0; j.Cron = "@every 1m" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg, entry := testConfig(t), valid
			tt.change(&cfg, &entry)

			if worker, err := New(cfg, entry); worker != nil || err == nil {
				t.Fatal("invalid scheduler configuration accepted")
			}
		})
	}

	if _, err := New(testConfig(t)); err == nil {
		t.Fatal("no jobs accepted")
	}

	if _, err := New(testConfig(t), valid, valid); err == nil {
		t.Fatal("duplicate job names accepted")
	}

	cfg := testConfig(t)
	cfg.PollInterval, cfg.StorageTimeout = 0, 0
	cfg.DB = &sql.DB{}
	valid.Mode = Coordinated
	worker, err := New(cfg, valid)
	if err != nil {
		t.Fatal(err)
	}

	if worker.cfg.StorageTimeout != 5*time.Second || worker.cfg.PollInterval != time.Second || worker.cfg.MaxConcurrentJobs != 1 || worker.jobs[0].Timeout != 5*time.Minute || worker.jobs[0].Timezone != "UTC" {
		t.Fatal("unexpected scheduler defaults")
	}
}

func TestJob_CalendarAndIntervalTimes(t *testing.T) {
	for _, tt := range []struct {
		name, cron, zone, now, want string
		every                       time.Duration
	}{
		{name: "UTC default", cron: "0 9 * * *", now: "2026-01-01T08:00:00Z", want: "2026-01-01T09:00:00Z"},
		{name: "winter", cron: "0 9 * * *", zone: "America/Denver", now: "2026-01-01T08:00:00Z", want: "2026-01-01T16:00:00Z"},
		{name: "summer", cron: "0 9 * * *", zone: "America/Denver", now: "2026-07-01T08:00:00Z", want: "2026-07-01T15:00:00Z"},
		{name: "descriptor", cron: "@hourly", now: "2026-01-01T08:01:00Z", want: "2026-01-01T09:00:00Z"},
		{name: "aligned interval", every: 5 * time.Minute, now: "2026-01-01T08:01:23Z", want: "2026-01-01T08:05:00Z"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			entry, err := parseJob(Job{Name: "job", Cron: tt.cron, Every: tt.every, Timezone: tt.zone})
			if err != nil {
				t.Fatal(err)
			}

			now, err := time.Parse(time.RFC3339, tt.now)
			if err != nil {
				t.Fatal(err)
			}

			if got := entry.next(now).Format(time.RFC3339); got != tt.want {
				t.Fatalf("next occurrence = %s, want %s", got, tt.want)
			}
		})
	}
}

func testConfig(t *testing.T) Config {
	t.Helper()

	log, err := logger.New(logger.Config{Service: "scheduler-test", Output: io.Discard})
	if err != nil {
		t.Fatal(err)
	}

	return Config{Namespace: "service", Logger: log, PollInterval: 5 * time.Millisecond, StorageTimeout: time.Second}
}
