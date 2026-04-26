// Package build implements the `at-snapshotter build` subcommand. See
// specs/001_bootstrap.md §3, §5, §7, §9, §14 — those are canonical.
//
// build has three modes (Mode):
//
//   - graph-backfill : full CAR crawl via com.atproto.sync.{listRepos,getRepo}
//     producing current_graph.duckdb only. The legacy first-run path.
//
//   - incremental    : nightly path. Replays per-day parquet shards from
//     ./data/daily/YYYY-MM-DD/*.parquet into current_all.duckdb (downloaded
//     from the object store if missing locally), updates denormalized
//     counts, and copies the graph subset out into current_graph.duckdb.
//
//   - force-rebuild  : graph-backfill + automatic bootstrap emission.
//
// All upload work is gated on opts.Store + opts.NoUpload. With Store == nil
// or NoUpload == true the build still produces local artifacts.
package build

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
)

// Mode selects which build path Run() executes.
type Mode string

const (
	ModeGraphBackfill Mode = "graph-backfill"
	ModeIncremental   Mode = "incremental"
	ModeForceRebuild  Mode = "force-rebuild"
)

// Options are the build-time knobs. See cmd/at-snapshotter/main.go for the
// CLI bindings.
type Options struct {
	Mode                 Mode
	Bootstrap            bool   // force-emit a bootstrap archive
	NoUpload             bool   // skip all object-store uploads
	Date                 string // "YYYY-MM-DD"; default = today UTC
	RetainDays           int    // delete ./data/daily/* older than this date - retain
	TakedownsPath        string // optional path to takedowns.yaml; "" disables
	SkipLabelerTakedowns bool   // disable reading labels.db for auto-takedowns
	Store                objstore.ObjectStore
}

// defaultOptions fills in zero-value defaults.
func (o *Options) defaultOptions() {
	if o.Mode == "" {
		o.Mode = ModeIncremental
	}
	if o.Date == "" {
		o.Date = time.Now().UTC().Format("2006-01-02")
	}
	if o.RetainDays <= 0 {
		o.RetainDays = 7
	}
}

// Run dispatches a build per opts.Mode.
//
// Legacy callers using `build.Run(ctx, cfg, logger)` should now pass an
// Options{Mode: ModeGraphBackfill} (preserves the old behavior).
func Run(ctx context.Context, cfg config.Config, opts Options, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	opts.defaultOptions()

	switch opts.Mode {
	case ModeGraphBackfill:
		return runGraphMode(ctx, cfg, opts, logger, false)
	case ModeForceRebuild:
		return runGraphMode(ctx, cfg, opts, logger, true)
	case ModeIncremental:
		return runIncrementalMode(ctx, cfg, opts, logger)
	default:
		return fmt.Errorf("build: unknown mode %q", opts.Mode)
	}
}

// runGraphMode handles ModeGraphBackfill and ModeForceRebuild. The latter
// auto-emits a bootstrap archive after the backfill (§9 / §7).
func runGraphMode(ctx context.Context, cfg config.Config, opts Options, logger *slog.Logger, forceRebuild bool) error {
	graphPath, err := runGraphBackfill(ctx, cfg, logger)
	if err != nil {
		return err
	}

	mode := "backfill"
	if forceRebuild {
		mode = "force-rebuild"
	}

	// Bootstrap emission runs when:
	//   - this is a force-rebuild (auto), OR
	//   - the user passed --bootstrap.
	emitBootstrap := opts.Bootstrap || forceRebuild

	publish := publishContext{
		cfg:         cfg,
		opts:        opts,
		logger:      logger,
		graphPath:   graphPath,
		buildMode:   mode,
		buildStart:  time.Now().UTC(), // best-effort; see Run wrapper for true start
		emitBootstrap: emitBootstrap,
	}
	return publish.runGraphPublish(ctx)
}

// runIncrementalMode resolves a base current_all.duckdb (local | object
// store | empty), replays new parquet days, recomputes counts, copies the
// graph subset, and publishes everything atomically.
func runIncrementalMode(ctx context.Context, cfg config.Config, opts Options, logger *slog.Logger) error {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("mkdir data dir: %w", err)
	}

	allPath := filepath.Join(cfg.DataDir, "current_all.duckdb")
	graphPath := filepath.Join(cfg.DataDir, "current_graph.duckdb")

	start := time.Now().UTC()

	// --- Step 1: resolve the base snapshot. ---
	if err := resolveBaseSnapshot(ctx, allPath, opts, logger); err != nil {
		return fmt.Errorf("resolve base snapshot: %w", err)
	}

	// --- Step 2: open base, replay any newer parquet days. ---
	allStore, err := openOrInitAll(allPath)
	if err != nil {
		return fmt.Errorf("open current_all: %w", err)
	}
	defer allStore.Close()

	dailyDir := filepath.Join(cfg.DataDir, "daily")
	replayed, jetstreamCursor, err := replayParquet(ctx, allStore, dailyDir, opts.Date, cfg.Filters, logger)
	if err != nil {
		return fmt.Errorf("replay parquet: %w", err)
	}

	if replayed == 0 {
		// Per §9 wording: "If no prior snapshot and no parquet, return an
		// error." We treat any successful open + zero parquet as
		// acceptable IFF the store had pre-existing rows; otherwise this
		// is a cold start with no inputs and we abort.
		nActors, _ := allStore.RegistryRowCount()
		if nActors == 0 {
			return errors.New("incremental build: no prior snapshot and no parquet days to replay")
		}
		logger.Info("no new parquet days to replay; rewriting current artifacts only")
	}

	// --- Step 2.5: apply takedowns (§19). ---
	// Two sources, merged: (a) operator-curated YAML at -takedowns, and
	// (b) Bluesky's moderation labeler — labels persisted by the `labels`
	// subcommand into <DataDir>/labels.db. Both are idempotent via
	// takedowns_applied. The YAML source is opt-in; the labeler source is
	// on-by-default when labels.db exists and opts.SkipLabelerTakedowns
	// is false.
	yamlTD, err := LoadTakedowns(opts.TakedownsPath)
	if err != nil {
		return fmt.Errorf("load takedowns yaml: %w", err)
	}
	var labelerTD Takedowns
	if !opts.SkipLabelerTakedowns {
		labelsDB := filepath.Join(cfg.DataDir, "labels.db")
		labelerTD, err = LoadLabelerTakedowns(labelsDB)
		if err != nil {
			return fmt.Errorf("load labeler takedowns: %w", err)
		}
	}
	logger.Info("takedown sources",
		"yaml_entries", len(yamlTD.URIs),
		"labeler_entries", len(labelerTD.URIs),
	)
	if len(yamlTD.URIs) > 0 || len(labelerTD.URIs) > 0 {
		merged := mergeTakedowns(yamlTD, labelerTD)
		if err := EnsureTakedownsAppliedTable(ctx, allStore.DB); err != nil {
			return fmt.Errorf("ensure takedowns_applied table: %w", err)
		}
		if err := ApplyTakedowns(ctx, allStore.DB, merged, logger); err != nil {
			return fmt.Errorf("apply takedowns: %w", err)
		}
	}

	// --- Step 3: recompute counts. ---
	if err := recomputeAllCounts(ctx, allStore); err != nil {
		return fmt.Errorf("recompute counts: %w", err)
	}

	// --- Step 4: write _meta + checkpoint. ---
	filterJSON, _ := marshalFilters(cfg.Filters)
	if err := allStore.WriteMeta(time.Now().UTC(), jetstreamCursor, "incremental", filterJSON); err != nil {
		return fmt.Errorf("write _meta: %w", err)
	}
	if err := allStore.Checkpoint(); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	if err := allStore.Close(); err != nil {
		return fmt.Errorf("close current_all: %w", err)
	}

	// --- Step 5: COPY graph subset → current_graph.duckdb ---
	if err := copyGraphSubset(ctx, allPath, graphPath, jetstreamCursor); err != nil {
		return fmt.Errorf("copy graph subset: %w", err)
	}

	// --- Step 6: publish. ---
	publish := publishContext{
		cfg:           cfg,
		opts:          opts,
		logger:        logger,
		graphPath:     graphPath,
		allPath:       allPath,
		buildMode:     "incremental",
		buildStart:    start,
		emitBootstrap: opts.Bootstrap,
	}
	return publish.runFullPublish(ctx)
}
