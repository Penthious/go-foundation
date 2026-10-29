package foundationfx_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/Penthious/go-foundation/logger"

	"go.uber.org/fx"
)

func TestModule_LoggerWithoutTelemetry(t *testing.T) {
	var output bytes.Buffer
	var log, sameLog *logger.Logger
	startApp(t,
		fx.Supply(logger.Config{Service: "orders", Output: &output}),
		fx.Populate(&log, &sameLog),
	)

	log.Info(t.Context(), "service started")

	if log != sameLog {
		t.Error("logger was constructed more than once")
	}

	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}

	if record["service"] != "orders" || record["msg"] != "service started" {
		t.Errorf("unexpected service log: %v", record)
	}
}
