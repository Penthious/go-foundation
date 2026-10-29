package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/Penthious/go-foundation/buildinfo"
	"github.com/Penthious/go-foundation/config"
)

type settings struct {
	Environment string `env:"ENVIRONMENT" envDefault:"local"`
}

func main() {
	cfg, err := config.Load[settings](config.Options{Prefix: "SERVICE_"})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	result := struct {
		Build       buildinfo.Info
		Environment string
	}{buildinfo.Current(), cfg.Environment}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
