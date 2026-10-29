package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestRun_BorrowedConnections(t *testing.T) {
	root := t.TempDir()
	commandDir := filepath.Join(root, "testsupport", "integration")
	if err := os.MkdirAll(commandDir, 0755); err != nil {
		t.Fatal(err)
	}

	for name, contents := range map[string]string{
		"go.mod":         "module github.com/Penthious/go-foundation\n\ngo 1.27.1\n",
		"sample.go":      "package sample\nconst Answer = 42\n",
		"sample_test.go": "package sample\nimport \"testing\"\nfunc TestAnswer(t *testing.T) { if Answer != 42 { t.Fatal(Answer) } }\n",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
	}

	t.Chdir(commandDir)
	t.Setenv(postgresEnv, "postgres://borrowed")
	t.Setenv(redisEnv, "borrowed:6379")
	t.Setenv("DOCKER_HOST", "unix:///nonexistent-test-docker.sock")

	t.Run("success without Docker", func(t *testing.T) {
		if err := run(t.Context(), nil); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("test command failure", func(t *testing.T) {
		err := run(t.Context(), []string{"./missing-package"})
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() == 0 {
			t.Fatalf("test command error = %v", err)
		}
	})

	t.Run("canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		if err := run(ctx, nil); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled run error = %v", err)
		}
	})
}

func TestPrepare_ConnectionOverrides(t *testing.T) {
	for _, tt := range []struct {
		name         string
		env          []string
		wantStarts   []string
		wantPassword string
	}{
		{name: "local", wantStarts: []string{postgresEnv, redisEnv}, wantPassword: testPassword},
		{name: "CI", env: []string{postgresEnv + "=postgres://ci", redisEnv + "=redis-ci:6379", redisPasswordEnv + "=ci-secret"}},
		{name: "borrow postgres", env: []string{postgresEnv + "=postgres://ci"}, wantStarts: []string{redisEnv}, wantPassword: testPassword},
		{name: "borrow redis", env: []string{redisEnv + "=redis-ci:6379"}, wantStarts: []string{postgresEnv}},
		{name: "local redis password", env: []string{redisPasswordEnv + "=custom"}, wantStarts: []string{postgresEnv, redisEnv}, wantPassword: "custom"},
		{name: "empty overrides", env: []string{postgresEnv + "=", redisEnv + "="}, wantStarts: []string{postgresEnv, redisEnv}, wantPassword: testPassword},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var starts, stops []string
			original := slices.Clone(tt.env)
			start := func(_ context.Context, key, password string) (string, func() error, error) {
				starts = append(starts, key)
				if key == redisEnv && password != tt.wantPassword {
					t.Fatalf("redis password = %q, want %q", password, tt.wantPassword)
				}

				return "local-" + key, func() error {
					stops = append(stops, key)
					return nil
				}, nil
			}

			env, cleanup, err := prepare(t.Context(), tt.env, start)
			if err != nil {
				t.Fatal(err)
			}

			if err := cleanup(); err != nil {
				t.Fatal(err)
			}

			if !slices.Equal(starts, tt.wantStarts) || !slices.Equal(tt.env, original) {
				t.Fatalf("starts = %v, want %v; input environment = %v", starts, tt.wantStarts, tt.env)
			}

			wantStops := slices.Clone(tt.wantStarts)
			slices.Reverse(wantStops)
			if !slices.Equal(stops, wantStops) {
				t.Fatalf("cleanup order = %v, want %v", stops, wantStops)
			}

			values := make(map[string]string)
			for _, entry := range env {
				key, value, _ := strings.Cut(entry, "=")
				values[key] = value
			}

			for _, key := range starts {
				if values[key] != "local-"+key {
					t.Errorf("missing generated connection for %s", key)
				}
			}

			for _, entry := range original {
				key, value, _ := strings.Cut(entry, "=")
				if value != "" && values[key] != value {
					t.Errorf("override changed for %s", key)
				}
			}

			if slices.Contains(starts, redisEnv) && values[redisPasswordEnv] != tt.wantPassword {
				t.Error("generated Redis password was not passed to tests")
			}
		})
	}
}

func TestPrepare_PartialStartupCleanup(t *testing.T) {
	startupErr := errors.New("redis startup failed")
	cleanupErr := errors.New("postgres cleanup failed")
	var stopped []string
	start := func(_ context.Context, key, _ string) (string, func() error, error) {
		stop := func() error {
			stopped = append(stopped, key)
			if key == postgresEnv {
				return cleanupErr
			}

			return nil
		}
		if key == redisEnv {
			return "", stop, startupErr
		}

		return "local-postgres", stop, nil
	}

	_, cleanup, err := prepare(t.Context(), nil, start)
	if !errors.Is(err, startupErr) {
		t.Fatalf("startup error = %v", err)
	}

	if err := cleanup(); !errors.Is(err, cleanupErr) {
		t.Fatalf("cleanup error = %v", err)
	}

	if !slices.Equal(stopped, []string{redisEnv, postgresEnv}) {
		t.Fatalf("partial resources not cleaned in reverse order: %v", stopped)
	}
}
