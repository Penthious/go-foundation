package config_test

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/config"
)

type settings struct {
	Name     string            `env:"NAME,required,notEmpty"`
	Port     int               `env:"PORT" envDefault:"8080"`
	Enabled  bool              `env:"ENABLED" envDefault:"true"`
	Timeout  time.Duration     `env:"TIMEOUT" envDefault:"5s"`
	Workers  []int             `env:"WORKERS" envDefault:"1,2"`
	Labels   map[string]string `env:"LABELS" envKeyValSeparator:"="`
	Optional *int              `env:"OPTIONAL"`
	Region   region            `env:"REGION" envDefault:"west"`
	HTTP     httpSettings      `envPrefix:"HTTP_"`
	Ignored  string            `env:"-"`
	Untagged string
}

type httpSettings struct {
	Address string `env:"ADDRESS" envDefault:":8080"`
}

type region string

func (r *region) UnmarshalText(data []byte) error {
	if string(data) == "west" || string(data) == "east" {
		*r = region(data)
		return nil
	}

	return fmt.Errorf("invalid private region %s", data)
}

func TestLoad_TypesDefaultsAndPrefixes(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		port int
	}{
		{"defaults", map[string]string{"APP_NAME": "orders"}, 8080},
		{"empty uses default", map[string]string{"APP_NAME": "orders", "APP_PORT": ""}, 8080},
		{"overrides", map[string]string{"APP_NAME": "orders", "APP_PORT": "9090"}, 9090},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := config.Load[settings](config.Options{Prefix: "APP_", Environment: tt.env})
			if err != nil {
				t.Fatal(err)
			}

			if got.Name != "orders" || got.Port != tt.port || !got.Enabled || got.Timeout != 5*time.Second || !reflect.DeepEqual(got.Workers, []int{1, 2}) || got.HTTP.Address != ":8080" || got.Region != "west" || got.Optional != nil {
				t.Errorf("unexpected config: %+v", got)
			}
		})
	}

	environment := map[string]string{
		"APP_NAME": "orders", "APP_ENABLED": "false", "APP_TIMEOUT": "250ms",
		"APP_WORKERS": "3,4", "APP_LABELS": "zone=local,tier=backend",
		"APP_OPTIONAL": "7", "APP_REGION": "east", "APP_HTTP_ADDRESS": "127.0.0.1:9000",
		"APP_IGNORED": "ignored", "APP_UNTAGGED": "ignored",
	}
	got, err := config.Load[settings](config.Options{Prefix: "APP_", Environment: environment})
	if err != nil {
		t.Fatal(err)
	}

	if got.Enabled || got.Timeout != 250*time.Millisecond || !reflect.DeepEqual(got.Workers, []int{3, 4}) || got.Labels["zone"] != "local" || got.Labels["tier"] != "backend" || got.Optional == nil || *got.Optional != 7 || got.Region != "east" || got.HTTP.Address != "127.0.0.1:9000" || got.Ignored != "" || got.Untagged != "" {
		t.Errorf("overrides were not loaded: %+v", got)
	}

	if environment["APP_REGION"] != "east" {
		t.Error("input environment was modified")
	}
}

func TestLoad_ParsingErrorsHideValuesAndDiscardPartialConfig(t *testing.T) {
	for _, key := range []string{"PORT", "ENABLED", "TIMEOUT", "WORKERS", "LABELS", "OPTIONAL", "REGION"} {
		t.Run(key, func(t *testing.T) {
			const secret = "private-value-must-not-appear"
			cfg, err := config.Load[settings](config.Options{Prefix: "APP_", Environment: map[string]string{
				"APP_NAME": "already-loaded", "APP_" + key: secret,
			}})
			if err == nil {
				t.Fatal("accepted invalid value")
			}

			if !reflect.DeepEqual(cfg, settings{}) {
				t.Error("returned partially populated config")
			}

			if strings.Contains(fmt.Sprintf("%+v", err), secret) {
				t.Errorf("parse error exposed value: %v", err)
			}

			var numeric *strconv.NumError
			if errors.As(err, &numeric) {
				t.Error("raw parser cause exposed through error chain")
			}
		})
	}
}

func TestLoad_RequiredAndEmptyValues(t *testing.T) {
	for _, environment := range []map[string]string{{}, {"APP_NAME": ""}} {
		_, err := config.Load[settings](config.Options{Prefix: "APP_", Environment: environment})
		if err == nil || !strings.Contains(err.Error(), "APP_NAME") {
			t.Errorf("required field error = %v", err)
		}
	}

	type requiredOnly struct {
		Name string `env:"NAME,required"`
	}
	cfg, err := config.Load[requiredOnly](config.Options{Environment: map[string]string{"NAME": ""}})
	if err != nil || cfg.Name != "" {
		t.Errorf("required without notEmpty should allow an explicitly empty value: %+v %v", cfg, err)
	}
}

var errInvalidPort = errors.New("port must be positive")

type validated struct {
	Port      int `env:"PORT" envDefault:"8080"`
	Validated int `env:"-"`
}

func (c *validated) Validate() error {
	c.Validated++
	if c.Port <= 0 {
		return errInvalidPort
	}

	return nil
}

type valueValidated struct {
	Enabled bool `env:"ENABLED"`
}

func (c valueValidated) Validate() error {
	if !c.Enabled {
		return errors.New("enabled is required")
	}

	return nil
}

func TestLoad_ServiceValidation(t *testing.T) {
	cfg, err := config.Load[validated](config.Options{Environment: map[string]string{}})
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Validated != 1 || cfg.Port != 8080 {
		t.Errorf("validation did not run once after defaults: %+v", cfg)
	}

	failed, err := config.Load[validated](config.Options{Environment: map[string]string{"PORT": "0"}})
	if !errors.Is(err, errInvalidPort) || failed != (validated{}) {
		t.Errorf("validation error lost its cause or returned partial config: %+v %v", failed, err)
	}

	if _, err := config.Load[validated](config.Options{Environment: map[string]string{"PORT": "invalid"}}); err == nil || errors.Is(err, errInvalidPort) {
		t.Errorf("validation ran before successful parsing: %v", err)
	}

	if _, err := config.Load[valueValidated](config.Options{Environment: map[string]string{}}); err == nil {
		t.Error("value receiver validator was not called")
	}
}

func TestLoad_EnvironmentSnapshotAndIsolation(t *testing.T) {
	type service struct {
		Name string `env:"GOF_CONFIG_TEST_NAME" envDefault:"local"`
	}
	t.Setenv("GOF_CONFIG_TEST_NAME", "first")
	first, err := config.Load[service]()
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv("GOF_CONFIG_TEST_NAME", "second")
	second, err := config.Load[service]()
	if err != nil {
		t.Fatal(err)
	}

	isolated, err := config.Load[service](config.Options{Environment: map[string]string{}})
	if err != nil {
		t.Fatal(err)
	}

	if first.Name != "first" || second.Name != "second" || isolated.Name != "local" {
		t.Errorf("unexpected snapshots: %+v %+v %+v", first, second, isolated)
	}
}

func TestLoad_InvalidDefinitions(t *testing.T) {
	type badType struct {
		Value chan string `env:"VALUE"`
	}
	type badTag struct {
		Value string `env:"VALUE,unknownOption"`
	}
	if _, err := config.Load[int](); err == nil {
		t.Error("accepted non-struct T")
	}

	if _, err := config.Load[*settings](); err == nil {
		t.Error("accepted pointer T")
	}

	if _, err := config.Load[settings](config.Options{}, config.Options{}); err == nil {
		t.Error("accepted multiple options")
	}

	if _, err := config.Load[badType](config.Options{Environment: map[string]string{"VALUE": "private-value"}}); err == nil || strings.Contains(err.Error(), "private-value") {
		t.Errorf("unsupported field error = %v", err)
	}

	if _, err := config.Load[badTag](config.Options{Environment: map[string]string{}}); err == nil {
		t.Error("accepted invalid tag option")
	}
}

func TestLoad_ReportsAllFieldErrors(t *testing.T) {
	_, err := config.Load[settings](config.Options{Environment: map[string]string{"PORT": "private-value"}})
	if err == nil || !strings.Contains(err.Error(), "NAME") || !strings.Contains(err.Error(), "Port") || strings.Contains(err.Error(), "private-value") {
		t.Errorf("aggregate error = %v", err)
	}
}

func TestLoad_ErrorDoesNotExposeDefaultsInTypeTags(t *testing.T) {
	type unsafeType struct {
		Values map[string]struct {
			Password string `envDefault:"private-default"`
		} `env:"VALUES"`
	}
	_, err := config.Load[unsafeType](config.Options{Environment: map[string]string{"VALUES": "key:value"}})
	if err == nil || strings.Contains(err.Error(), "private-default") {
		t.Errorf("field type exposed a default value: %v", err)
	}
}

func TestLoad_ConcurrentReads(t *testing.T) {
	opts := config.Options{Environment: map[string]string{"NAME": "orders"}}
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			cfg, err := config.Load[settings](opts)
			if err != nil || cfg.Name != "orders" {
				t.Errorf("concurrent load: %+v %v", cfg, err)
			}
		})
	}

	workers.Wait()
}
