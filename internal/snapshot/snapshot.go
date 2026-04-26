// Package snapshot implements the `at-snapshot snapshot` subcommand.
//
// The job stitches the immutable bootstrap social graph baseline together with
// the windowed deltas accumulated by the run command into the two final
// DuckDB outputs that consumers actually query:
//
//   - snapshot/current_graph.duckdb — current-state actors / follows / blocks
//     plus per-actor all-time aggregates. Not bounded by the lookback window:
//     a follow created the day after bootstrap and never deleted is in here.
//
//   - snapshot/current_all.duckdb — everything in current_graph plus the
//     posts / likes / reposts / post_media tables and per-post / per-actor
//     engagement aggregates, all bounded by the configured lookback window.
//     Window-scoped fields use the `_in_window` suffix so consumers don't
//     confuse them with all-time totals.
//
//   - snapshot/snapshot_metadata.json — sidecar describing what was rolled
//     up: source bootstrap date, window endpoints, and per-table row counts.
//
// Design choices worth flagging:
//
//   - The job reads only from object storage. There is no expectation of any
//     local file from a prior bootstrap or run. For the local backend
//     objstore.URL returns an absolute filesystem path, which we hand directly
//     to DuckDB's ATTACH and read_parquet so we never re-stage parquet files
//     on disk. An s3 backend would need a download-to-temp helper here.
//
//   - DuckDB does the heavy lifting: a sequence of CREATE TABLE AS SELECT
//     statements pulls from the ATTACHed bootstrap database and read_parquet
//     globs against the raw/ tree. Doing the merge in-process would mean
//     writing the same SQL in Go.
//
//   - For idempotent behavior the staging file is removed at the start of
//     each run. We then build the file locally, close DuckDB cleanly, and
//     upload atomically via objstore.Put — DuckDB needs a real writable path
//     so a streaming write directly to objstore is not on the table.
package snapshot

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2" // register the duckdb driver

	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
)

// Deps lets tests stub out the object store and pin the wall clock that
// drives the lookback window math. Run wires the production defaults.
type Deps struct {
	ObjStore objstore.Store
	// Now returns the upper bound of the snapshot window. The window is
	// (Now - LookbackDays, Now], inclusive of Now.
	Now func() time.Time
}

// Run materializes both snapshot DuckDB files and the metadata sidecar.
func Run(ctx context.Context, cfg config.Config) error {
	obj, err := objstore.FromConfig(cfg)
	if err != nil {
		return err
	}
	deps := Deps{
		ObjStore: obj,
		Now:      func() time.Time { return time.Now().UTC() },
	}
	return RunWith(ctx, cfg, deps)
}

// RunWith is the testable entrypoint: every IO dependency is on Deps.
func RunWith(ctx context.Context, cfg config.Config, deps Deps) error {
	if deps.Now == nil {
		deps.Now = func() time.Time { return time.Now().UTC() }
	}

	now := deps.Now().UTC()
	windowEnd := now
	windowStart := now.AddDate(0, 0, -cfg.LookbackDays)

	bootstrapDate, bootstrapPath, err := resolveBootstrap(ctx, deps.ObjStore)
	if err != nil {
		return err
	}
	rawRoot := deps.ObjStore.URL("raw")
	if rawRoot == "" {
		return fmt.Errorf("snapshot: object store backend does not expose URL paths; only local is supported today")
	}

	stagingDir := filepath.Join(cfg.DataDir, "snapshot-staging")
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		return fmt.Errorf("snapshot: mkdir staging: %w", err)
	}
	graphPath := filepath.Join(stagingDir, "current_graph.duckdb")
	allPath := filepath.Join(stagingDir, "current_all.duckdb")
	for _, p := range []string{graphPath, allPath} {
		// DuckDB will happily reopen an existing file, but to keep the run
		// idempotent we want each invocation to materialize from scratch.
		_ = os.Remove(p)
	}

	graphCounts, err := buildGraphDB(ctx, graphPath, bootstrapPath, rawRoot, cfg.DuckDBMemoryLimit, windowStart, windowEnd)
	if err != nil {
		return fmt.Errorf("snapshot: build graph: %w", err)
	}
	allCounts, err := buildAllDB(ctx, allPath, bootstrapPath, rawRoot, cfg.DuckDBMemoryLimit, windowStart, windowEnd)
	if err != nil {
		return fmt.Errorf("snapshot: build all: %w", err)
	}

	if err := uploadFile(ctx, deps.ObjStore, "snapshot/current_graph.duckdb", graphPath); err != nil {
		return fmt.Errorf("snapshot: upload graph: %w", err)
	}
	if err := uploadFile(ctx, deps.ObjStore, "snapshot/current_all.duckdb", allPath); err != nil {
		return fmt.Errorf("snapshot: upload all: %w", err)
	}

	meta := metadata{
		SnapshotAt:    now,
		BootstrapDate: bootstrapDate,
		LookbackDays:  cfg.LookbackDays,
		WindowStart:   windowStart,
		WindowEnd:     windowEnd,
		RowCounts:     mergeCounts(graphCounts, allCounts),
	}
	if err := uploadJSON(ctx, deps.ObjStore, "snapshot/snapshot_metadata.json", meta); err != nil {
		return fmt.Errorf("snapshot: upload metadata: %w", err)
	}

	slog.Info("snapshot complete",
		"bootstrap_date", bootstrapDate,
		"window_start", windowStart,
		"window_end", windowEnd,
		"counts", meta.RowCounts,
	)
	return nil
}

// resolveBootstrap finds the most recent bootstrap/YYYY-MM-DD/social_graph.duckdb
// in object storage and returns its date and a DuckDB-readable path.
func resolveBootstrap(ctx context.Context, obj objstore.Store) (string, string, error) {
	objs, err := obj.List(ctx, "bootstrap/")
	if err != nil {
		return "", "", fmt.Errorf("snapshot: list bootstrap/: %w", err)
	}
	var dates []string
	for _, o := range objs {
		// Expect bootstrap/YYYY-MM-DD/social_graph.duckdb.
		parts := strings.Split(o.Path, "/")
		if len(parts) != 3 || parts[2] != "social_graph.duckdb" {
			continue
		}
		dates = append(dates, parts[1])
	}
	if len(dates) == 0 {
		return "", "", errors.New("snapshot: no bootstrap social_graph.duckdb found in object storage")
	}
	sort.Strings(dates)
	latest := dates[len(dates)-1]
	path := obj.URL(fmt.Sprintf("bootstrap/%s/social_graph.duckdb", latest))
	if path == "" {
		return "", "", errors.New("snapshot: object store backend does not expose URL paths; only local is supported today")
	}
	return latest, path, nil
}

// metadata is the on-disk shape of snapshot_metadata.json.
type metadata struct {
	SnapshotAt    time.Time        `json:"snapshot_at"`
	BootstrapDate string           `json:"bootstrap_date"`
	LookbackDays  int              `json:"lookback_days"`
	WindowStart   time.Time        `json:"window_start"`
	WindowEnd     time.Time        `json:"window_end"`
	RowCounts     map[string]int64 `json:"row_counts"`
}

func mergeCounts(a, b map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(a)+len(b))
	for _, m := range []map[string]int64{a, b} {
		for k, v := range m {
			out[k] = v
		}
	}
	return out
}

// applyMemoryLimit issues the SET memory_limit pragma when configured.
func applyMemoryLimit(ctx context.Context, db *sql.DB, limit string) error {
	if limit == "" {
		return nil
	}
	// DuckDB's SET takes a literal so we splice rather than parameter-bind.
	_, err := db.ExecContext(ctx, fmt.Sprintf("SET memory_limit = '%s'", strings.ReplaceAll(limit, "'", "''")))
	return err
}

// uploadFile streams a local file to obj at dst.
func uploadFile(ctx context.Context, obj objstore.Store, dst, src string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	return obj.Put(ctx, dst, f, "application/x-duckdb")
}

func uploadJSON(ctx context.Context, obj objstore.Store, dst string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return obj.Put(ctx, dst, strings.NewReader(string(b)), "application/json")
}
