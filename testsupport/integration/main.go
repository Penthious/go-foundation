// Command integration provisions missing test databases and runs the SDK tests.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	postgresEnv      = "GO_FOUNDATION_POSTGRES_TEST_DSN"
	redisEnv         = "GO_FOUNDATION_REDIS_TEST_ADDRESS"
	redisPasswordEnv = "GO_FOUNDATION_REDIS_TEST_PASSWORD"
	testPassword     = "foundation-test"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) (err error) {
	root, err := filepath.Abs("../..")
	if err != nil {
		return err
	}

	module, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil || !strings.HasPrefix(string(module), "module github.com/Penthious/go-foundation\n") {
		return errors.New("run from the repository root with: go -C testsupport/integration run . [go test arguments]")
	}

	startup, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	env, cleanup, err := prepare(startup, os.Environ(), startContainer)
	if err != nil {
		return errors.Join(err, cleanup())
	}

	defer func() { err = errors.Join(err, cleanup()) }()

	cancel()

	if len(args) == 0 {
		args = []string{"./..."}
	}

	cmd := exec.CommandContext(ctx, "go", append([]string{"test", "-race", "-count=1", "-timeout=10m"}, args...)...)
	cmd.Dir = root
	cmd.Env = env
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	cmd.WaitDelay = 10 * time.Second

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("integration tests: %w", err)
	}

	return nil
}

func prepare(ctx context.Context, env []string, start func(context.Context, string, string) (string, func() error, error)) ([]string, func() error, error) {
	values := make(map[string]string)
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = value
		}
	}

	var stops []func() error
	cleanup := func() error {
		var err error
		for _, stop := range slices.Backward(stops) {
			err = errors.Join(err, stop())
		}

		return err
	}

	result := slices.Clone(env)
	for _, key := range []string{postgresEnv, redisEnv} {
		if values[key] != "" {
			continue
		}

		password := testPassword
		if key == redisEnv && values[redisPasswordEnv] != "" {
			password = values[redisPasswordEnv]
		}

		value, stop, err := start(ctx, key, password)
		if stop != nil {
			stops = append(stops, stop)
		}

		if err != nil {
			return nil, cleanup, fmt.Errorf("provision %s: %w", key, err)
		}

		result = append(result, key+"="+value)
		if key == redisEnv {
			result = append(result, redisPasswordEnv+"="+password)
		}
	}

	return result, cleanup, nil
}

func startContainer(ctx context.Context, key, password string) (value string, stop func() error, err error) {
	// Docker discovery can panic when no daemon is configured.
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("Testcontainers startup: %v", recovered)
		}
	}()

	if key == postgresEnv {
		container, err := postgres.Run(ctx, "postgres:18-alpine",
			postgres.WithDatabase("foundation"), postgres.WithUsername("foundation"),
			postgres.WithPassword(password), postgres.BasicWaitStrategies(),
		)
		if container != nil {
			stop = cleanupContainer(container.Container)
		}

		if err != nil {
			return "", stop, err
		}

		value, err = container.ConnectionString(ctx, "sslmode=disable")
		return value, stop, err
	}

	container, err := redis.Run(ctx, "redis:7.4.2-alpine",
		testcontainers.WithCmd("redis-server", "--requirepass", password),
		testcontainers.WithAdditionalWaitStrategy(wait.ForExec([]string{"redis-cli", "--no-auth-warning", "-a", password, "ping"})),
	)
	if container != nil {
		stop = cleanupContainer(container.Container)
	}

	if err != nil {
		return "", stop, err
	}

	value, err = container.PortEndpoint(ctx, "6379/tcp", "")
	return value, stop, err
}

func cleanupContainer(container testcontainers.Container) func() error {
	return func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		return container.Terminate(ctx, testcontainers.StopTimeout(10*time.Second))
	}
}
