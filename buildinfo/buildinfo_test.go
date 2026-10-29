package buildinfo_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/buildinfo"
)

func TestCurrent_ReturnsCopy(t *testing.T) {
	original := buildinfo.Current()
	changed := original
	changed.Version += "-changed"

	if buildinfo.Current() != original {
		t.Error("changing a returned Info changed the embedded metadata")
	}

	if changed.Version == original.Version {
		t.Fatal("test did not change its copy")
	}
}

func TestCurrent_LinkedServiceBinary(t *testing.T) {
	packageDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	root := filepath.Dir(packageDir)
	dir := t.TempDir()
	module := "module example.test/service\n\ngo 1.27.1\n\nrequire github.com/Penthious/go-foundation v0.0.0\n\nreplace github.com/Penthious/go-foundation => " + strconv.Quote(root) + "\n"
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(module), 0600); err != nil {
		t.Fatal(err)
	}

	mainSource, err := os.ReadFile(filepath.Join(packageDir, "testdata", "service.go"))
	if err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(dir, "main.go"), mainSource, 0600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name  string
		flags string
		want  buildinfo.Info
	}{
		{name: "defaults", want: buildinfo.Info{Version: "dev", Commit: "unknown", BuildTime: "unknown"}},
		{name: "linker metadata", flags: "-s -w -X github.com/Penthious/go-foundation/buildinfo.version=v2.3.4 -X github.com/Penthious/go-foundation/buildinfo.commit=abc123 -X github.com/Penthious/go-foundation/buildinfo.buildTime=2026-10-28T12:00:00Z", want: buildinfo.Info{Version: "v2.3.4", Commit: "abc123", BuildTime: "2026-10-28T12:00:00Z"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()
			binary := filepath.Join(dir, "service")
			if runtime.GOOS == "windows" {
				binary += ".exe"
			}

			cmd := exec.CommandContext(ctx, "go", "build", "-mod=mod", "-trimpath", "-ldflags", tt.flags, "-o", binary, ".")
			cmd.Dir = dir
			cmd.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off")

			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("build consumer: %v\n%s", err, output)
			}

			for _, environment := range []string{"staging", "production"} {
				run := exec.CommandContext(ctx, binary)
				run.Env = append(os.Environ(), "SERVICE_ENVIRONMENT="+environment, "VERSION=runtime-value-must-not-replace-build")
				output, err := run.Output()
				if err != nil {
					t.Fatal(err)
				}

				var got struct {
					Build       buildinfo.Info
					Environment string
				}
				if err := json.Unmarshal(output, &got); err != nil {
					t.Fatal(err)
				}

				if got.Build != tt.want || got.Environment != environment {
					t.Errorf("binary metadata/config = %+v, want %+v / %s", got, tt.want, environment)
				}
			}
		})
	}
}
