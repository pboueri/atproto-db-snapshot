package run

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pboueri/atproto-db-snapshot/internal/config"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// seedStagingTwoDays inserts a few hand-crafted events spanning two UTC
// days so tests can exercise the day-by-day sealing logic.
func seedStagingTwoDays(t *testing.T, s *stagingDB) (yesterday, today string) {
	t.Helper()
	yesterday = "2026-04-23"
	today = "2026-04-24"
	ctx := context.Background()

	// Yesterday: two posts + one like + one follow + one delete. Enough to
	// exercise every export_kind.
	events := []stagingEvent{
		{
			DID: "did:plc:a", Collection: "app.bsky.feed.post",
			Rkey: "p1", Operation: "create", TimeUS: 1_714_000_000_000_000,
			CID: "bafypost1", Day: yesterday,
			RecordJSON: []byte(`{"text":"hello","createdAt":"2026-04-23T12:00:00Z","langs":["en"]}`),
		},
		{
			DID: "did:plc:b", Collection: "app.bsky.feed.post",
			Rkey: "p2", Operation: "create", TimeUS: 1_714_000_010_000_000,
			CID: "bafypost2", Day: yesterday,
			RecordJSON: []byte(`{"text":"second","createdAt":"2026-04-23T12:00:01Z"}`),
		},
		{
			DID: "did:plc:c", Collection: "app.bsky.feed.like",
			Rkey: "l1", Operation: "create", TimeUS: 1_714_000_020_000_000,
			CID: "bafylike", Day: yesterday,
			RecordJSON: []byte(`{"subject":{"uri":"at://did:plc:a/app.bsky.feed.post/p1","cid":"bafypost1"},"createdAt":"2026-04-23T12:00:02Z"}`),
		},
		{
			DID: "did:plc:a", Collection: "app.bsky.graph.follow",
			Rkey: "f1", Operation: "create", TimeUS: 1_714_000_030_000_000,
			Day:        yesterday,
			RecordJSON: []byte(`{"subject":"did:plc:b","createdAt":"2026-04-23T12:00:03Z"}`),
		},
		{
			DID: "did:plc:a", Collection: "app.bsky.feed.post",
			Rkey: "p3", Operation: "delete", TimeUS: 1_714_000_040_000_000,
			Day: yesterday,
		},
		// Today: a post that must NOT be swept.
		{
			DID: "did:plc:a", Collection: "app.bsky.feed.post",
			Rkey: "p4", Operation: "create", TimeUS: 1_714_100_000_000_000,
			CID: "bafypost4", Day: today,
			RecordJSON: []byte(`{"text":"today","createdAt":"2026-04-24T12:00:00Z"}`),
		},
	}
	for i := range events {
		ev := events[i]
		ev.Endpoint = "wss://e"
		if _, err := s.insert(ctx, &ev); err != nil {
			t.Fatalf("seed insert %d: %v", i, err)
		}
	}
	return
}

// TestSealDayWritesParquetAndPurges seeds two days, calls sealDay on the
// older one with no object store, and verifies the expected parquet
// files land under daily/YYYY-MM-DD/ and the staging rows for that day
// are gone.
func TestSealDayWritesParquetAndPurges(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")
	dailyDir := filepath.Join(dataDir, "daily")
	if err := os.MkdirAll(dailyDir, 0o755); err != nil {
		t.Fatalf("mkdir daily: %v", err)
	}
	stagingPath := filepath.Join(dataDir, "staging.db")

	cfg := testJetstreamCfg()
	cfg.NoUpload = true

	s, err := openStaging(stagingPath, cfg, testStagingLogger())
	if err != nil {
		t.Fatalf("openStaging: %v", err)
	}
	defer s.Close()

	yesterday, today := seedStagingTwoDays(t, s)

	roll := &rollover{
		dataDir:     dataDir,
		dailyDir:    dailyDir,
		stagingPath: stagingPath,
		store:       nil,
		cfg:         cfg,
		staging:     s,
		logger:      testStagingLogger(),
	}

	ctx := context.Background()
	if err := roll.sealDay(ctx, yesterday); err != nil {
		t.Fatalf("sealDay: %v", err)
	}

	outDir := filepath.Join(dailyDir, yesterday)
	mustExist := []string{
		"posts.parquet", "likes.parquet", "follows.parquet", "deletions.parquet", "_manifest.json",
	}
	for _, name := range mustExist {
		p := filepath.Join(outDir, name)
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatalf("expected file %s: %v", name, err)
		}
		if fi.Size() == 0 {
			t.Fatalf("%s is zero-sized", name)
		}
	}
	// reposts/blocks/profile_updates shouldn't be present because we didn't seed any.
	for _, name := range []string{"reposts.parquet", "blocks.parquet", "profile_updates.parquet"} {
		p := filepath.Join(outDir, name)
		if _, err := os.Stat(p); err == nil {
			t.Fatalf("unexpected file %s present", name)
		}
	}

	// Staging for yesterday must be empty; today's row must remain.
	for _, tbl := range []string{
		"staging_events_post", "staging_events_like", "staging_events_follow", "staging_events_delete",
	} {
		n, err := s.rowCount(ctx, tbl, yesterday)
		if err != nil {
			t.Fatalf("rowCount %s: %v", tbl, err)
		}
		if n != 0 {
			t.Fatalf("%s yesterday rows = %d; want 0", tbl, n)
		}
	}
	n, err := s.rowCount(ctx, "staging_events_post", today)
	if err != nil {
		t.Fatalf("rowCount today: %v", err)
	}
	if n != 1 {
		t.Fatalf("today post rows = %d; want 1", n)
	}

	// Verify manifest.
	mb, err := os.ReadFile(filepath.Join(outDir, "_manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var m dayManifest
	if err := json.Unmarshal(mb, &m); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	if m.Date != yesterday {
		t.Fatalf("manifest date = %q; want %q", m.Date, yesterday)
	}
	if m.RowCounts["posts.parquet"] != 2 {
		t.Fatalf("posts.parquet count = %d; want 2", m.RowCounts["posts.parquet"])
	}
	if m.RowCounts["likes.parquet"] != 1 {
		t.Fatalf("likes.parquet count = %d; want 1", m.RowCounts["likes.parquet"])
	}
	if m.RowCounts["follows.parquet"] != 1 {
		t.Fatalf("follows.parquet count = %d; want 1", m.RowCounts["follows.parquet"])
	}
	if m.RowCounts["deletions.parquet"] != 1 {
		t.Fatalf("deletions.parquet count = %d; want 1", m.RowCounts["deletions.parquet"])
	}
	if m.JetstreamCursorStart == 0 || m.JetstreamCursorEnd < m.JetstreamCursorStart {
		t.Fatalf("bad cursor bounds in manifest: %+v", m)
	}

	// Sanity: read the parquet back with DuckDB to confirm row counts on disk.
	countParquetRows(t, filepath.Join(outDir, "posts.parquet"), 2)
	countParquetRows(t, filepath.Join(outDir, "likes.parquet"), 1)
	countParquetRows(t, filepath.Join(outDir, "follows.parquet"), 1)
	countParquetRows(t, filepath.Join(outDir, "deletions.parquet"), 1)
}

// TestSealEmptyDayIsNoop ensures a day with no events gets purged without
// writing any files.
func TestSealEmptyDayIsNoop(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")
	dailyDir := filepath.Join(dataDir, "daily")
	if err := os.MkdirAll(dailyDir, 0o755); err != nil {
		t.Fatalf("mkdir daily: %v", err)
	}
	stagingPath := filepath.Join(dataDir, "staging.db")

	cfg := testJetstreamCfg()
	cfg.NoUpload = true
	s, err := openStaging(stagingPath, cfg, testStagingLogger())
	if err != nil {
		t.Fatalf("openStaging: %v", err)
	}
	defer s.Close()

	roll := &rollover{
		dataDir: dataDir, dailyDir: dailyDir, stagingPath: stagingPath,
		cfg: cfg, staging: s, logger: testStagingLogger(),
	}
	if err := roll.sealDay(context.Background(), "2026-04-23"); err != nil {
		t.Fatalf("sealDay empty: %v", err)
	}
	entries, _ := os.ReadDir(dailyDir)
	// sealDay on an empty day creates no output files. (It may mkdir the
	// day directory via os.MkdirAll; allow that but require it's empty.)
	for _, e := range entries {
		sub, _ := os.ReadDir(filepath.Join(dailyDir, e.Name()))
		if len(sub) != 0 {
			t.Fatalf("seal-empty produced files: %v", sub)
		}
	}
}

// countParquetRows opens a fresh in-memory DuckDB and counts rows in path.
func countParquetRows(t *testing.T, path string, want int64) {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	q := fmt.Sprintf(`SELECT count(*) FROM read_parquet('%s')`, path)
	var n int64
	if err := db.QueryRow(q).Scan(&n); err != nil {
		t.Fatalf("read parquet %s: %v", path, err)
	}
	if n != want {
		t.Fatalf("%s row count = %d; want %d", path, n, want)
	}
}

// TestSecondsUntilUTCMidnightPlusGrace sanity-checks the pure scheduler helper.
func TestSecondsUntilUTCMidnightPlusGrace(t *testing.T) {
	now := time.Date(2026, 4, 24, 23, 50, 0, 0, time.UTC)
	grace := 10 * time.Minute
	got := secondsUntilUTCMidnightPlusGrace(now, grace)
	// Next midnight = 2026-04-25 00:00 + 10 min = 20 min from now.
	want := 20 * time.Minute
	if got != want {
		t.Fatalf("secondsUntilUTCMidnightPlusGrace = %v; want %v", got, want)
	}

	// Even at exactly midnight now, we go to the NEXT midnight (24h + grace).
	now = time.Date(2026, 4, 24, 0, 0, 0, 0, time.UTC)
	got = secondsUntilUTCMidnightPlusGrace(now, grace)
	want = 24*time.Hour + grace
	if got != want {
		t.Fatalf("at midnight: got %v; want %v", got, want)
	}
}

// Compile-time use of the JetstreamConfig type to keep the import.
var _ = config.JetstreamConfig{}
