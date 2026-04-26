package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pboueri/atproto-db-snapshot/internal/build"
	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/labels"
	"github.com/pboueri/atproto-db-snapshot/internal/run"
	"github.com/pboueri/atproto-db-snapshot/internal/serve"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	switch cmd {
	case "build":
		runBuild(ctx, args, logger)
	case "run":
		runRun(ctx, args, logger)
	case "serve":
		runServe(ctx, args, logger)
	case "labels":
		runLabels(ctx, args, logger)
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", cmd)
		usage()
		os.Exit(2)
	}
}

func runBuild(ctx context.Context, args []string, logger *slog.Logger) {
	fs := flag.NewFlagSet("build", flag.ExitOnError)
	cfg := config.Default()

	// Knobs that apply to the legacy graph-backfill path.
	fs.StringVar(&cfg.RelayHost, "relay", cfg.RelayHost, "relay host, e.g. https://bsky.network (graph-backfill / force-rebuild only)")
	fs.IntVar(&cfg.Workers, "workers", cfg.Workers, "parallel getRepo workers (graph-backfill / force-rebuild only)")
	fs.Float64Var(&cfg.RateLimitRPS, "rps", cfg.RateLimitRPS, "global rate limit in requests per second (graph-backfill / force-rebuild only)")
	fs.IntVar(&cfg.DIDLimit, "limit", cfg.DIDLimit, "cap backfill at this many DIDs (0 = no cap)")
	fs.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "local working directory")
	fs.DurationVar(&cfg.HTTPTimeout, "http-timeout", cfg.HTTPTimeout, "per-request HTTP timeout")

	// New mode-driven flags (§9 / spec).
	var (
		mode                 string
		bootstrap            bool
		noUpload             bool
		date                 string
		retainN              int
		fileStore            string
		configPath           string
		takedowns            string
		skipLabelerTakedowns bool
	)
	fs.StringVar(&mode, "mode", "incremental", "build mode: graph-backfill | incremental | force-rebuild")
	fs.BoolVar(&bootstrap, "bootstrap", false, "force-emit a graph bootstrap archive (auto when -mode=force-rebuild)")
	fs.BoolVar(&noUpload, "no-upload", false, "skip object-store uploads (build artifacts stay local)")
	fs.StringVar(&date, "date", "", "build target date in YYYY-MM-DD; defaults to today UTC")
	fs.IntVar(&retainN, "retain", 7, "delete ./data/daily/* directories older than N days from build date")
	fs.StringVar(&fileStore, "file-store", "", "directory to use as a local file-backed object store (default: none)")
	fs.StringVar(&configPath, "config", "", "path to config.yaml (currently consumed only for object_store.*)")
	fs.StringVar(&takedowns, "takedowns", "", "path to a takedowns.yaml; rows referenced are nullified/deleted in current_all.duckdb (incremental mode)")
	fs.BoolVar(&skipLabelerTakedowns, "skip-labeler-takedowns", false, "ignore <data-dir>/labels.db produced by the `labels` subcommand (incremental mode)")

	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), `at-snapshotter build — produce current_graph.duckdb / current_all.duckdb

Usage:
  at-snapshotter build [flags]

Modes:
  -mode=incremental    Replay ./data/daily/YYYY-MM-DD/*.parquet into an
                       existing current_all.duckdb (downloaded from the
                       object store if missing locally). Default.
  -mode=graph-backfill CAR-crawl path; produces current_graph.duckdb only.
  -mode=force-rebuild  Same as graph-backfill, plus auto-emits a bootstrap.

Flags:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}

	// Resolve the object store: -no-upload > -file-store > -config (R2).
	store, err := resolveStore(ctx, configPath, fileStore, noUpload, logger)
	if err != nil {
		logger.Error("resolve object store", "err", err)
		os.Exit(1)
	}

	// -takedowns asks for a specific file. If set but missing, fail loudly.
	if takedowns != "" {
		if _, err := os.Stat(takedowns); err != nil {
			logger.Error("takedowns file unreadable", "path", takedowns, "err", err)
			os.Exit(1)
		}
	}

	opts := build.Options{
		Mode:                 build.Mode(mode),
		Bootstrap:            bootstrap,
		NoUpload:             noUpload,
		Date:                 date,
		RetainDays:           retainN,
		TakedownsPath:        takedowns,
		SkipLabelerTakedowns: skipLabelerTakedowns,
		Store:                store,
	}

	start := time.Now()
	if err := build.Run(ctx, cfg, opts, logger); err != nil {
		logger.Error("build failed", "err", err)
		os.Exit(1)
	}
	logger.Info("done", "elapsed", time.Since(start).Round(time.Second))
}

func runRun(ctx context.Context, args []string, logger *slog.Logger) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	cfg := config.Default()

	var (
		endpoint   string
		noUpload   bool
		fileStore  string
		configPath string
	)

	fs.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "local working directory")
	fs.StringVar(&endpoint, "endpoint", "",
		"override Jetstream endpoint (single-host mode); when empty the default failover list is used")
	fs.BoolVar(&noUpload, "no-upload", false,
		"do not upload sealed-day parquet files to the object store")
	fs.StringVar(&fileStore, "file-store", "",
		"directory to use as a local file-backed object store (defaults to off; -no-upload overrides this)")
	fs.StringVar(&configPath, "config", "",
		"path to config.yaml (currently consumed only for object_store.*)")

	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), `at-snapshotter run — Jetstream consumer

Usage:
  at-snapshotter run [flags]

Flags:`)
		fs.PrintDefaults()
		fmt.Fprintln(fs.Output(), `
Behavior:
  - Connects to Jetstream and writes a per-day SQLite staging table at
    <data-dir>/staging.db.
  - At UTC midnight + 10 min grace, seals yesterday's events into
    <data-dir>/daily/YYYY-MM-DD/<collection>.parquet (zstd-6) plus a
    _manifest.json, then uploads via the configured object store and
    purges staging rows for that day.
  - Cursor is persisted atomically at <data-dir>/cursor.json after each
    SQLite checkpoint. Restart-safe.

  Without -file-store and without an injected object store the consumer
  runs in -no-upload mode automatically: parquet files stay on local disk.`)
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}

	if endpoint != "" {
		cfg.Jetstream.Endpoints = []string{endpoint}
	}
	if noUpload {
		cfg.Jetstream.NoUpload = true
	}

	store, err := resolveStore(ctx, configPath, fileStore, noUpload, logger)
	if err != nil {
		logger.Error("resolve object store", "err", err)
		os.Exit(1)
	}

	if err := run.Run(ctx, cfg, store, logger); err != nil {
		logger.Error("run failed", "err", err)
		os.Exit(1)
	}
}

func runServe(ctx context.Context, args []string, logger *slog.Logger) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	opts := serve.Options{
		Listen:         "127.0.0.1:8080",
		DataDir:        "./data",
		RefreshSeconds: 5,
		LogTailLines:   20000,
	}
	var (
		fileStore  string
		configPath string
	)
	fs.StringVar(&opts.Listen, "listen", opts.Listen, "bind address for the HTTP dashboard")
	fs.StringVar(&opts.DataDir, "data-dir", opts.DataDir, "local working directory to read from")
	fs.IntVar(&opts.RefreshSeconds, "refresh", opts.RefreshSeconds, "dashboard auto-refresh interval in seconds")
	fs.IntVar(&opts.LogTailLines, "log-tail", opts.LogTailLines, "trailing log lines scanned for rates + errors")
	fs.StringVar(&fileStore, "file-store", "", "optional file-backed object store root to list daily/ + bootstrap/")
	fs.StringVar(&configPath, "config", "", "path to config.yaml (currently consumed only for object_store.*)")

	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), `at-snapshotter serve — read-only health dashboard

Usage:
  at-snapshotter serve [flags]

Exposes GET / (HTML dashboard), /api/summary, /api/jetstream, /api/build,
/api/disk, /api/retention, /api/errors, /metrics (Prometheus). Loopback
binding by default — front with a reverse proxy for remote access.

Flags:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}
	store, err := resolveStore(ctx, configPath, fileStore, false, logger)
	if err != nil {
		logger.Error("resolve object store", "err", err)
		os.Exit(1)
	}
	opts.Store = store
	if err := serve.Run(ctx, opts, logger); err != nil {
		logger.Error("serve failed", "err", err)
		os.Exit(1)
	}
}

func runLabels(ctx context.Context, args []string, logger *slog.Logger) {
	fs := flag.NewFlagSet("labels", flag.ExitOnError)
	opts := labels.Options{}

	fs.StringVar(&opts.DataDir, "data-dir", "./data",
		"local working directory; labels.db + labels_cursor.json live here")
	fs.StringVar(&opts.LabelerURL, "labeler-url", labels.DefaultLabelerURL,
		"override the labeler subscription endpoint (for testing)")
	fs.StringVar(&opts.LabelerDID, "labeler-did", labels.DefaultLabelerDID,
		"only persist labels with this src DID (empty = keep all)")

	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), `at-snapshotter labels — subscribe to Bluesky moderation labels

Usage:
  at-snapshotter labels [flags]

Connects to wss://mod.bsky.app/xrpc/com.atproto.label.subscribeLabels and
persists every emitted label into <data-dir>/labels.db. Cursor is
checkpointed to <data-dir>/labels_cursor.json so restarts resume from the
last-known seq. Reconnects with exponential backoff (5s → 60s max).

Pairs with the nightly build: when labels.db exists at build time the
incremental mode loads every unnegated !takedown / !hide label and
applies them through the same path as takedowns.yaml (posts nullified,
likes/reposts/follows/blocks deleted, profiles redacted). Disable with:

  at-snapshotter build -skip-labeler-takedowns

Flags:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}
	if err := labels.Run(ctx, opts, logger); err != nil {
		if ctx.Err() != nil {
			// graceful shutdown
			return
		}
		logger.Error("labels failed", "err", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `at-snapshotter — ATProto snapshot builder

Usage:
  at-snapshotter build  [flags]   one-shot full backfill / nightly build
  at-snapshotter run    [flags]   long-running Jetstream consumer
  at-snapshotter labels [flags]   long-running Bluesky labeler subscriber
  at-snapshotter serve  [flags]   read-only HTTP health dashboard

Run "at-snapshotter <subcommand> -h" for subcommand-specific flags.`)
}
