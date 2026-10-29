// Package logger provides structured logging without exposing its slog backend.
// Create a Logger at startup and pass it to the components that need it.
package logger

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"strings"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/trace"
)

// Level is the minimum severity written to either output.
type Level string

// Supported logging levels, in increasing severity.
const (
	LevelDebug Level = "debug"
	LevelInfo  Level = "info"
	LevelWarn  Level = "warn"
	LevelError Level = "error"
)

// Config configures a logger and its outputs.
type Config struct {
	// Service is required and identifies the emitting service.
	Service string
	// Environment identifies the deployment environment.
	Environment string
	// Version identifies the service release.
	Version string
	// Level defaults to LevelInfo.
	Level Level
	// Output receives newline-delimited JSON and defaults to os.Stdout.
	// Writes through a logger and its children are serialized.
	Output io.Writer
	// LoggerProvider receives logs through the OpenTelemetry bridge. If nil,
	// the bridge uses the global provider. The caller owns flushing and shutdown.
	LoggerProvider otellog.LoggerProvider
	// OnError handles output errors and defaults to otel.Handle. It must be
	// concurrency-safe and must not log through this logger. Asynchronous
	// exporter failures are handled by OpenTelemetry, not this callback.
	OnError func(error)
}

// Logger writes events to JSON and OpenTelemetry. It is safe for concurrent use.
// Its zero value is not usable; construct one with New.
type Logger struct {
	backend *slog.Logger
	level   slog.Level
	fields  map[string]any
}

// New creates a logger. It rejects an empty Service or an unsupported Level.
// It does not configure an exporter or change the global logger or OTel provider.
func New(cfg Config) (*Logger, error) {
	if strings.TrimSpace(cfg.Service) == "" {
		return nil, errors.New("logger: service is required")
	}

	level, err := parseLevel(cfg.Level)
	if err != nil {
		return nil, err
	}

	output := cfg.Output
	if output == nil {
		output = os.Stdout
	}

	report := cfg.OnError
	if report == nil {
		report = otel.Handle
	}

	jsonHandler := slog.NewJSONHandler(output, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			if len(groups) == 0 && attr.Key == slog.TimeKey {
				return slog.Time(slog.TimeKey, attr.Value.Time().UTC())
			}

			return attr
		},
	})
	otelHandler := otelslog.NewHandler("go-foundation/logger",
		otelslog.WithLoggerProvider(cfg.LoggerProvider),
	)
	handler := slog.NewMultiHandler(jsonHandler, otelHandler).WithAttrs([]slog.Attr{
		slog.String("service", cfg.Service),
		slog.String("environment", cfg.Environment),
		slog.String("version", cfg.Version),
	})

	return &Logger{
		backend: slog.New(&errorHandler{Handler: handler, report: report}),
		level:   level,
	}, nil
}

// With returns a child logger with additional fields, leaving the parent intact.
// Later fields override earlier fields with the same key. Empty keys are ignored.
func (l *Logger) With(fields ...Field) *Logger {
	return &Logger{
		backend: l.backend,
		level:   l.level,
		fields:  mergeFields(l.fields, fields),
	}
}

// Enabled reports whether level would be logged. Unsupported levels return false.
func (l *Logger) Enabled(ctx context.Context, level Level) bool {
	severity, err := parseLevel(level)
	if err != nil {
		return false
	}

	if ctx == nil {
		ctx = context.Background()
	}

	return severity >= l.level && l.backend.Enabled(ctx, severity)
}

// Debug writes a diagnostic event.
func (l *Logger) Debug(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, slog.LevelDebug, msg, fields)
}

// Info writes an informational event.
func (l *Logger) Info(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, slog.LevelInfo, msg, fields)
}

// Warn writes an event describing a recoverable problem.
func (l *Logger) Warn(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, slog.LevelWarn, msg, fields)
}

// Error writes an event describing a failed operation. It does not exit or panic.
func (l *Logger) Error(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, slog.LevelError, msg, fields)
}

func (l *Logger) log(ctx context.Context, level slog.Level, msg string, fields []Field) {
	if level < l.level {
		return
	}

	if ctx == nil {
		ctx = context.Background()
	}

	if !l.backend.Enabled(ctx, level) {
		return
	}

	var attrs []slog.Attr
	span := trace.SpanContextFromContext(ctx)
	if span.IsValid() {
		attrs = append(attrs,
			slog.String("trace_id", span.TraceID().String()),
			slog.String("span_id", span.SpanID().String()),
		)
	}

	values := mergeFields(l.fields, fields)
	if len(values) != 0 {
		fieldAttrs := make([]slog.Attr, 0, len(values))
		for key, value := range values {
			fieldAttrs = append(fieldAttrs, slog.Any(key, value))
		}

		attrs = append(attrs, slog.GroupAttrs("fields", fieldAttrs...))
	}

	l.backend.LogAttrs(ctx, level, msg, attrs...)
}

func mergeFields(base map[string]any, fields []Field) map[string]any {
	values := make(map[string]any, len(base)+len(fields))
	maps.Copy(values, base)

	for _, field := range fields {
		if field.key == "" {
			continue
		}

		values[field.key] = field.value
	}

	return values
}

func parseLevel(level Level) (slog.Level, error) {
	switch level {
	case "", LevelInfo:
		return slog.LevelInfo, nil
	case LevelDebug:
		return slog.LevelDebug, nil
	case LevelWarn:
		return slog.LevelWarn, nil
	case LevelError:
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("logger: unsupported level %q", level)
	}
}

// slog discards handler errors, so report them before they reach slog.Logger.
type errorHandler struct {
	slog.Handler
	report func(error)
}

func (h *errorHandler) Handle(ctx context.Context, record slog.Record) error {
	err := h.Handler.Handle(ctx, record)
	if err != nil {
		h.report(err)
	}

	return err
}
