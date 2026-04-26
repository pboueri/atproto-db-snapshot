// at-snapshot is the CLI for producing windowed Bluesky/ATProto analytic snapshots.
//
// Subcommands:
//
//	bootstrap  Build the initial social_graph.duckdb baseline from PLC + Constellation.
//	run        Long-running jetstream consumer that writes date-partitioned parquet.
//	snapshot   Roll up the past N days of raw parquet into current_*.duckdb files.
//	monitor    Lightweight HTTP server reporting job status from the output directory.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/pboueri/atproto-db-snapshot/internal/bootstrap"
	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/logging"
	"github.com/pboueri/atproto-db-snapshot/internal/monitor"
	"github.com/pboueri/atproto-db-snapshot/internal/runcmd"
	"github.com/pboueri/atproto-db-snapshot/internal/snapshot"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		// flag.ErrHelp means -h was passed; flag already printed usage.
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return errors.New("no subcommand given")
	}
	sub, rest := args[0], args[1:]

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	switch sub {
	case "bootstrap":
		cfg, err := config.ParseFlags("bootstrap", rest)
		if err != nil {
			return err
		}
		logging.Setup(cfg.LogLevel)
		slog.Info("starting bootstrap", "data_dir", cfg.DataDir)
		return bootstrap.Run(ctx, cfg)
	case "run":
		cfg, err := config.ParseFlags("run", rest)
		if err != nil {
			return err
		}
		logging.Setup(cfg.LogLevel)
		slog.Info("starting run", "data_dir", cfg.DataDir)
		return runcmd.Run(ctx, cfg)
	case "snapshot":
		cfg, err := config.ParseFlags("snapshot", rest)
		if err != nil {
			return err
		}
		logging.Setup(cfg.LogLevel)
		slog.Info("starting snapshot", "data_dir", cfg.DataDir, "lookback_days", cfg.LookbackDays)
		return snapshot.Run(ctx, cfg)
	case "monitor":
		cfg, err := config.ParseFlags("monitor", rest)
		if err != nil {
			return err
		}
		logging.Setup(cfg.LogLevel)
		slog.Info("starting monitor", "data_dir", cfg.DataDir, "addr", cfg.MonitorAddr)
		return monitor.Run(ctx, cfg)
	case "-h", "--help", "help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown subcommand %q", sub)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: at-snapshot <command> [flags]

commands:
  bootstrap   Build initial social_graph.duckdb from historicals
  run         Long-running jetstream consumer
  snapshot    Materialize current_*.duckdb from raw/ for the past N days
  monitor     HTTP server reporting job status

Run 'at-snapshot <command> -h' for command-specific flags.`)
}
