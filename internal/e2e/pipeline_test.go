package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"

	"github.com/pboueri/atproto-db-snapshot/internal/build"
	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
	"github.com/pboueri/atproto-db-snapshot/internal/run"
)

// TestPipelineRunToParquet runs the partial pipeline:
//
//	fake jetstream WS → internal/run.Run → staging.db → SealYesterdayForTest
//	→ daily/YYYY-MM-DD/*.parquet
//
// and asserts the parquet shards have the expected row counts. This is the
// piece of the pipeline we can verify today without tripping the source
// bug documented in TestPipelineEndToEnd.
func TestPipelineRunToParquet(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e pipeline test in -short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	dataDir, plan := runToParquet(ctx, t)

	dayDir := filepath.Join(dataDir, "daily", plan.Day)
	for _, name := range []string{"posts.parquet", "likes.parquet", "follows.parquet", "blocks.parquet", "_manifest.json"} {
		if _, err := os.Stat(filepath.Join(dayDir, name)); err != nil {
			t.Fatalf("expected %s: %v", name, err)
		}
	}

	verifyParquetRows(t, filepath.Join(dayDir, "posts.parquet"), int64(plan.PostCount))
	verifyParquetRows(t, filepath.Join(dayDir, "likes.parquet"), int64(plan.LikeCount))
	verifyParquetRows(t, filepath.Join(dayDir, "follows.parquet"), int64(plan.FollowCount))
	verifyParquetRows(t, filepath.Join(dayDir, "blocks.parquet"), int64(plan.BlockCount))

	// Manifest row_counts should align.
	mb, err := os.ReadFile(filepath.Join(dayDir, "_manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var m struct {
		Date      string           `json:"date"`
		RowCounts map[string]int64 `json:"row_counts"`
	}
	if err := json.Unmarshal(mb, &m); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	if m.Date != plan.Day {
		t.Fatalf("manifest date = %q; want %q", m.Date, plan.Day)
	}
	if m.RowCounts["posts.parquet"] != int64(plan.PostCount) {
		t.Fatalf("posts.parquet count = %d; want %d", m.RowCounts["posts.parquet"], plan.PostCount)
	}
	if m.RowCounts["likes.parquet"] != int64(plan.LikeCount) {
		t.Fatalf("likes.parquet count = %d; want %d", m.RowCounts["likes.parquet"], plan.LikeCount)
	}
}

// TestPipelineEndToEnd is the full pipeline assertion: fake jetstream →
// run → staging → seal → parquet → build (incremental) → DuckDB query.
func TestPipelineEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e pipeline test in -short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dataDir, plan := runToParquet(ctx, t)

	// File-backed object store under the temp dir so latest.json &
	// current_all-v1.duckdb publish during build.
	storeDir := filepath.Join(filepath.Dir(dataDir), "store")
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		t.Fatalf("mkdir store: %v", err)
	}

	cfg := config.Default()
	cfg.DataDir = dataDir

	logger := newDiscardLogger()
	opts := build.Options{
		Mode:     build.ModeIncremental,
		NoUpload: false,
		Date:     plan.Day,
		Store:    objstore.NewFileStore(storeDir, ""),
		// SkipLabelerTakedowns avoids opening labels.db (doesn't exist in this fixture).
		SkipLabelerTakedowns: true,
	}

	if err := build.Run(ctx, cfg, opts, logger); err != nil {
		t.Fatalf("build.Run: %v", err)
	}

	// Verify the published artifacts exist.
	allDB := filepath.Join(dataDir, "current_all.duckdb")
	if _, err := os.Stat(allDB); err != nil {
		t.Fatalf("current_all.duckdb missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(storeDir, "latest.json")); err != nil {
		t.Fatalf("latest.json missing in store: %v", err)
	}

	// Now query — assert the row counts match the fixture.
	db, err := sql.Open("duckdb", allDB+"?access_mode=READ_ONLY")
	if err != nil {
		t.Fatalf("open current_all.duckdb: %v", err)
	}
	defer db.Close()

	var posts, likes, follows, blocks int64
	if err := db.QueryRow("SELECT count(*) FROM posts").Scan(&posts); err != nil {
		t.Fatalf("count posts: %v", err)
	}
	if err := db.QueryRow("SELECT count(*) FROM likes_current").Scan(&likes); err != nil {
		t.Fatalf("count likes_current: %v", err)
	}
	if err := db.QueryRow("SELECT count(*) FROM follows_current").Scan(&follows); err != nil {
		t.Fatalf("count follows_current: %v", err)
	}
	if err := db.QueryRow("SELECT count(*) FROM blocks_current").Scan(&blocks); err != nil {
		t.Fatalf("count blocks_current: %v", err)
	}

	if posts != int64(plan.PostCount) {
		t.Errorf("posts = %d; want %d", posts, plan.PostCount)
	}
	if likes != int64(plan.LikeCount) {
		t.Errorf("likes_current = %d; want %d", likes, plan.LikeCount)
	}
	if follows != int64(plan.FollowCount) {
		t.Errorf("follows_current = %d; want %d", follows, plan.FollowCount)
	}
	if blocks != int64(plan.BlockCount) {
		t.Errorf("blocks_current = %d; want %d", blocks, plan.BlockCount)
	}

	// Spot-check that the denormalized like_count was recomputed:
	// at least one post must have a non-zero like_count, since the
	// fixture wires likes to actual posts.
	var maxLikes int64
	if err := db.QueryRow("SELECT COALESCE(MAX(like_count), 0) FROM posts").Scan(&maxLikes); err != nil {
		t.Fatalf("max(like_count): %v", err)
	}
	if maxLikes == 0 {
		t.Errorf("expected at least one post with like_count > 0; got 0")
	}
}

// runToParquet is a helper that does every step except build.Run. It's
// split out so a future fix to the event_type bug can re-enable the full
// assertion without restructuring the fixture / server plumbing.
func runToParquet(ctx context.Context, t *testing.T) (string, fixturePlan) {
	t.Helper()

	events, plan := buildFixture()

	// Fake jetstream WebSocket server.
	fake := newFakeJetstream(events)
	t.Cleanup(fake.Close)

	// Temp data dir + FileStore.
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir data: %v", err)
	}

	cfg := config.Default()
	cfg.DataDir = dataDir
	cfg.Jetstream.Endpoints = []string{fake.URL()}
	cfg.Jetstream.Compress = false
	cfg.Jetstream.CheckpointEveryRows = 5
	cfg.Jetstream.CheckpointInterval = 200 * time.Millisecond
	cfg.Jetstream.RolloverGrace = 1 * time.Second
	cfg.Jetstream.MinFreeBytes = 0
	cfg.Jetstream.NoUpload = true
	cfg.Jetstream.LagAlarm = 0

	logger := newDiscardLogger()

	runCtx, runCancel := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() {
		runDone <- run.Run(runCtx, cfg, nil, logger)
	}()

	expected := plan.PostCount + plan.LikeCount + plan.FollowCount + plan.BlockCount
	stagingPath := filepath.Join(dataDir, "staging.db")
	if err := wait(ctx, 25*time.Second, func() bool {
		n, err := countStagingRows(stagingPath)
		if err != nil {
			return false
		}
		return n >= int64(expected)
	}); err != nil {
		runCancel()
		<-runDone
		t.Fatalf("events never landed in staging: %v", err)
	}

	runCancel()
	if err := <-runDone; err != nil && err != context.Canceled {
		t.Fatalf("run returned: %v", err)
	}

	if err := run.SealYesterdayForTest(ctx, dataDir, cfg.Jetstream, nil, plan.Day, logger); err != nil {
		t.Fatalf("SealYesterdayForTest: %v", err)
	}
	return dataDir, plan
}

// countStagingRows sums SELECT count(*) across every staging table.
func countStagingRows(path string) (int64, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro&_pragma=busy_timeout(1000)")
	if err != nil {
		return 0, err
	}
	defer db.Close()
	if _, err := db.Exec("SELECT 1"); err != nil {
		return 0, err
	}

	var total int64
	tables := []string{
		"staging_events_post", "staging_events_like",
		"staging_events_follow", "staging_events_block",
		"staging_events_repost", "staging_events_profile",
		"staging_events_delete",
	}
	for _, t := range tables {
		var n int64
		err := db.QueryRow("SELECT count(*) FROM " + t).Scan(&n)
		if err != nil {
			return total, nil
		}
		total += n
	}
	return total, nil
}

// verifyParquetRows asserts a parquet file has exactly `want` rows.
func verifyParquetRows(t *testing.T, path string, want int64) {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	var n int64
	q := fmt.Sprintf("SELECT count(*) FROM read_parquet('%s')", path)
	if err := db.QueryRow(q).Scan(&n); err != nil {
		t.Fatalf("read_parquet %s: %v", path, err)
	}
	if n != want {
		t.Fatalf("%s rows = %d; want %d", path, n, want)
	}
}

// Compile-time references so unused imports stay wired in case the build
// path gets re-enabled by a future fix.
var _ = build.Run
var _ = objstore.NewFileStore
