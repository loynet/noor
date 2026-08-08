package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"noor/internal/app"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if len(os.Args) == 2 && os.Args[1] == "check-health" {
		if err := app.CheckHealth(os.Getenv("HEALTHCHECK_ADDR")); err != nil {
			fmt.Fprintln(os.Stderr, "noor:", err)
			os.Exit(1)
		}
		return
	}
	path := os.Getenv("CONFIG_FILE")
	if path == "" {
		path = "config/noor.toml"
	}
	cfg, err := app.LoadConfig(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "noor:", err)
		os.Exit(1)
	}
	if len(os.Args) == 2 && os.Args[1] == "check-config" {
		return
	}
	if len(os.Args) > 1 {
		fmt.Fprintln(os.Stderr, "usage: noor [check-config|check-health]")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := app.Run(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "noor:", err)
		os.Exit(1)
	}
}
