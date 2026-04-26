package run

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
)

// rollover owns UTC-midnight + grace processing: for each (yesterday,
// collection) pair, COPY rows out of the SQLite staging db into a parquet
// shard, write a per-day _manifest.json, upload everything via
// `ObjectStore`, then DELETE the rows from staging.
type rollover struct {
	dataDir     string
	dailyDir    string // dataDir/daily
	stagingPath string

	store   objstore.ObjectStore
	cfg     config.JetstreamConfig
	staging *stagingDB

	logger *slog.Logger
}

// runScheduler triggers a rollover at every UTC-midnight + grace boundary.
// It also runs an immediate sweep on startup to catch up on any days that
// accumulated while the consumer was stopped.
func (r *rollover) runScheduler(ctx context.Context) {
	// Initial sweep for any back-dated days in staging.
	r.sweepOnce(ctx)

	for {
		now := time.Now().UTC()
		wait := secondsUntilUTCMidnightPlusGrace(now, r.cfg.RolloverGrace)
		r.logger.Info("rollover scheduled", "in", wait.Round(time.Second))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		r.sweepOnce(ctx)
	}
}

// sweepOnce processes every staging-db day strictly older than today UTC.
// It's idempotent: a day that's already been sealed and emptied will scan
// to zero rows and skip cleanly.
func (r *rollover) sweepOnce(ctx context.Context) {
	today := time.Now().UTC().Format("2006-01-02")
	days, err := r.staging.daysWithEvents(ctx)
	if err != nil {
		r.logger.Warn("rollover: enumerate days failed", "err", err)
		return
	}
	sort.Strings(days)
	for _, d := range days {
		if d >= today {
			continue
		}
		if err := r.sealDay(ctx, d); err != nil {
			r.logger.Error("rollover: seal day failed",
				"day", d, "err", err)
			// don't bail — try the next day; partial progress is fine
			continue
		}
	}
}

// sealDay materializes parquet files for `day` from the staging db, writes
// _manifest.json, optionally uploads, and DELETEs the rows.
func (r *rollover) sealDay(ctx context.Context, day string) error {
	r.logger.Info("sealing day", "day", day)

	outDir := filepath.Join(r.dailyDir, day)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	// Map staging tables to the per-collection parquet basename.
	// `staging_events_delete` → `deletions.parquet` per §6.
	parquetWrites := []parquetExportSpec{
		{table: "staging_events_post", parquet: "posts.parquet", kind: "post"},
		{table: "staging_events_post", parquet: "post_embeds.parquet", kind: "post_embed"},
		{table: "staging_events_like", parquet: "likes.parquet", kind: "like"},
		{table: "staging_events_repost", parquet: "reposts.parquet", kind: "repost"},
		{table: "staging_events_follow", parquet: "follows.parquet", kind: "follow"},
		{table: "staging_events_block", parquet: "blocks.parquet", kind: "block"},
		{table: "staging_events_profile", parquet: "profile_updates.parquet", kind: "profile"},
		{table: "staging_events_delete", parquet: "deletions.parquet", kind: "delete"},
	}

	// Pre-count rows so the manifest can populate row_counts. Most kinds
	// can use the bare staging-table count; post_embeds reuses the posts
	// table but only emits one row per embed-bearing post, so it needs a
	// JSON-predicated count to avoid double-counting in the manifest.
	rowCounts := make(map[string]int64, len(parquetWrites))
	totalRows := int64(0)
	for _, spec := range parquetWrites {
		var (
			n   int64
			err error
		)
		if spec.kind == "post_embed" {
			n, err = r.staging.postEmbedRowCount(ctx, day)
		} else {
			n, err = r.staging.rowCount(ctx, spec.table, day)
			totalRows += n // post_embeds rides on staging_events_post; don't double-count
		}
		if err != nil {
			return fmt.Errorf("count %s/%s: %w", spec.table, day, err)
		}
		rowCounts[spec.parquet] = n
	}

	if totalRows == 0 {
		// Nothing for this day; still drop the (empty) staging entries
		// and advance.
		r.logger.Info("rollover: empty day, skipping", "day", day)
		return r.purgeDay(ctx, day)
	}

	// Cursor bounds for the manifest.
	curStart, curEnd, err := r.staging.cursorBoundsForDay(ctx, day)
	if err != nil {
		return fmt.Errorf("cursor bounds: %w", err)
	}

	// Emit parquet via DuckDB. Skip empty tables — DuckDB COPY of an
	// empty SELECT works, but we'd rather not emit zero-byte files.
	bytesByFile := make(map[string]int64, len(parquetWrites))
	for _, spec := range parquetWrites {
		if rowCounts[spec.parquet] == 0 {
			continue
		}
		path := filepath.Join(outDir, spec.parquet)
		if err := exportParquet(ctx, r.stagingPath, day, spec, path); err != nil {
			return fmt.Errorf("export %s: %w", spec.parquet, err)
		}
		fi, err := os.Stat(path)
		if err == nil {
			bytesByFile[spec.parquet] = fi.Size()
		}
	}

	// Per-day _manifest.json (§6).
	manifest := dayManifest{
		Date:                 day,
		SchemaVersion:        "v1",
		JetstreamCursorStart: curStart,
		JetstreamCursorEnd:   curEnd,
		BuiltAt:              time.Now().UTC(),
		RowCounts:            rowCounts,
		Bytes:                bytesByFile,
	}
	manifestPath := filepath.Join(outDir, "_manifest.json")
	if err := writeManifest(manifestPath, manifest); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	bytesByFile["_manifest.json"] = manifestSize(manifestPath)

	// Upload (best effort — caller may have passed nil store, or NoUpload).
	if !r.cfg.NoUpload && r.store != nil {
		if err := r.uploadDay(ctx, day, outDir, parquetWrites, manifestPath); err != nil {
			// Don't purge if upload failed — better to keep retrying.
			return fmt.Errorf("upload day: %w", err)
		}
	} else {
		r.logger.Info("upload skipped", "day", day, "no_upload", r.cfg.NoUpload, "store_nil", r.store == nil)
	}

	// Purge staging rows for this day. Even if upload was skipped, the
	// parquet files remain on disk — `build` is the system-of-record for
	// retention.
	if err := r.purgeDay(ctx, day); err != nil {
		return fmt.Errorf("purge day: %w", err)
	}
	r.logger.Info("sealed day", "day", day, "rows", totalRows)
	return nil
}

func (r *rollover) purgeDay(ctx context.Context, day string) error {
	for _, t := range allStagingTables() {
		if _, err := r.staging.deleteDay(ctx, t, day); err != nil {
			return fmt.Errorf("delete %s/%s: %w", t, day, err)
		}
	}
	return nil
}

func (r *rollover) uploadDay(ctx context.Context, day, dir string, specs []parquetExportSpec, manifestPath string) error {
	prefix := "daily/" + day + "/"
	// Upload parquet shards first; manifest last.
	for _, spec := range specs {
		path := filepath.Join(dir, spec.parquet)
		fi, err := os.Stat(path)
		if err != nil {
			continue // empty / not emitted
		}
		key := prefix + spec.parquet
		if err := putAtomicFile(ctx, r.store, key, path, fi.Size()); err != nil {
			return fmt.Errorf("upload %s: %w", key, err)
		}
		r.logger.Info("uploaded", "key", key, "bytes", fi.Size())
	}
	manifestKey := prefix + "_manifest.json"
	mfi, err := os.Stat(manifestPath)
	if err != nil {
		return fmt.Errorf("stat manifest: %w", err)
	}
	if err := putAtomicFile(ctx, r.store, manifestKey, manifestPath, mfi.Size()); err != nil {
		return fmt.Errorf("upload manifest: %w", err)
	}
	r.logger.Info("uploaded manifest", "key", manifestKey)
	return nil
}

func putAtomicFile(ctx context.Context, store objstore.ObjectStore, key, path string, size int64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return store.PutAtomic(ctx, key, f, size)
}

func manifestSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}
