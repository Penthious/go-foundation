package logger_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/logger"
)

func TestFields_Encoding(t *testing.T) {
	var output bytes.Buffer
	log := newLogger(t, logger.Config{Service: "orders", Output: &output})

	log.Info(t.Context(), "typed fields",
		logger.String("text", "quotes: \"hello\"\n世界"),
		logger.Bool("enabled", true),
		logger.Int("attempt", -2),
		logger.Int64("large", 9223372036854775807),
		logger.Duration("duration", 42*time.Millisecond),
		logger.Err(errors.New("database unavailable")),
		logger.String("duplicate", "first"),
		logger.String("duplicate", "last"),
		logger.String("", "ignored"),
		logger.Field{},
	)

	records := readRecords(t, &output)
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}

	want := map[string]any{
		"text": "quotes: \"hello\"\n世界", "enabled": true,
		"attempt": json.Number("-2"), "large": json.Number("9223372036854775807"),
		"duration": "42ms", "error": "database unavailable", "duplicate": "last",
	}
	if !reflect.DeepEqual(records[0].Fields, want) {
		t.Errorf("fields = %#v, want %#v", records[0].Fields, want)
	}
}

func TestErr_Nil(t *testing.T) {
	var output bytes.Buffer
	log := newLogger(t, logger.Config{Service: "orders", Output: &output})

	log.With(logger.Err(nil)).Info(t.Context(), "success", logger.Err(nil))

	records := readRecords(t, &output)
	if len(records) != 1 || records[0].Fields != nil {
		t.Errorf("nil errors should not produce fields: %+v", records)
	}
}
