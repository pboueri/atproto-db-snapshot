package build

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
	_ "modernc.org/sqlite"

	"github.com/pboueri/atproto-db-snapshot/internal/archive"
	"github.com/pboueri/atproto-db-snapshot/internal/config"
)

// TestIncrementalReplay produces a synthetic daily parquet directory using
// the archive package, runs the incremental build, and checks that
// current_all.duckdb + current_graph.duckdb were produced with the expected
// row counts. Exercises the §14.1 upsert path end-to-end against a real
// DuckDB.
func TestIncrementalReplay(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	dataDir := tmp
	dailyDir := filepath.Join(dataDir, "daily", "2026-04-24")

	// --- write a synthetic day of parquet via the archive writer ---
	w, err := archive.NewWriter(ctx, dailyDir)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	t.Cleanup(func() { w.Close() })

	day := archive.Day("2026-04-24")
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)

	// Two profile updates — one with handle, one without.
	if _, _, err := w.WriteProfileUpdates(ctx, day, []archive.ProfileUpdateEvent{
		{DID: "did:plc:alice", Handle: "alice.bsky.social", DisplayName: "Alice", EventTS: now},
		{DID: "did:plc:bob", Handle: "bob.bsky.social", DisplayName: "Bob", EventTS: now},
	}); err != nil {
		t.Fatalf("WriteProfileUpdates: %v", err)
	}

	// Two posts.
	if _, _, err := w.WritePosts(ctx, day, []archive.PostEvent{
		{
			EventType: "create", URI: "at://did:plc:alice/app.bsky.feed.post/p1",
			DID: "did:plc:alice", Rkey: "p1", CID: "bafyP1",
			EventTS: now, RecordTS: now, Text: "hello", Lang: "en",
		},
		{
			EventType: "create", URI: "at://did:plc:bob/app.bsky.feed.post/p2",
			DID: "did:plc:bob", Rkey: "p2", CID: "bafyP2",
			EventTS: now, RecordTS: now, Text: "hi back",
			ReplyRootURI:   "at://did:plc:alice/app.bsky.feed.post/p1",
			ReplyParentURI: "at://did:plc:alice/app.bsky.feed.post/p1",
		},
	}); err != nil {
		t.Fatalf("WritePosts: %v", err)
	}

	// Bob likes Alice's post.
	if _, _, err := w.WriteLikes(ctx, day, []archive.LikeEvent{
		{
			EventType: "create", URI: "at://did:plc:bob/app.bsky.feed.like/L1",
			LikerDID: "did:plc:bob", SubjectURI: "at://did:plc:alice/app.bsky.feed.post/p1",
			EventTS: now, RecordTS: now,
		},
	}); err != nil {
		t.Fatalf("WriteLikes: %v", err)
	}

	// Alice follows Bob.
	if _, _, err := w.WriteFollows(ctx, day, []archive.FollowEvent{
		{
			EventType: "create", URI: "at://did:plc:alice/app.bsky.graph.follow/F1",
			SrcDID: "did:plc:alice", DstDID: "did:plc:bob",
			EventTS: now, RecordTS: now,
		},
	}); err != nil {
		t.Fatalf("WriteFollows: %v", err)
	}

	// Alice blocks Bob (yes, even though she just followed — exercise the
	// blocks_current path).
	if _, _, err := w.WriteBlocks(ctx, day, []archive.BlockEvent{
		{
			EventType: "create", URI: "at://did:plc:alice/app.bsky.graph.block/B1",
			SrcDID: "did:plc:alice", DstDID: "did:plc:bob",
			EventTS: now, RecordTS: now,
		},
	}); err != nil {
		t.Fatalf("WriteBlocks: %v", err)
	}
	if _, err := w.WriteManifest(ctx, archive.Manifest{
		Date: day, JetstreamCursorEnd: 1714000000000000,
	}); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	// --- run incremental build ---
	cfg := config.Default()
	cfg.DataDir = dataDir
	opts := Options{
		Mode:       ModeIncremental,
		NoUpload:   true,
		Date:       "2026-04-24",
		RetainDays: 30,
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	if err := Run(ctx, cfg, opts, logger); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// --- inspect produced files ---
	allPath := filepath.Join(dataDir, "current_all.duckdb")
	graphPath := filepath.Join(dataDir, "current_graph.duckdb")

	if _, err := os.Stat(allPath); err != nil {
		t.Fatalf("current_all.duckdb missing: %v", err)
	}
	if _, err := os.Stat(graphPath); err != nil {
		t.Fatalf("current_graph.duckdb missing: %v", err)
	}

	db, err := sql.Open("duckdb", allPath+"?access_mode=READ_ONLY")
	if err != nil {
		t.Fatalf("open all: %v", err)
	}
	defer db.Close()

	check := func(table string, want int64) {
		t.Helper()
		var n int64
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if n != want {
			t.Fatalf("count %s: got %d want %d", table, n, want)
		}
	}

	// Two distinct DIDs total in the day, so two registry / actors rows.
	check("actors_registry", 2)
	check("actors", 2)
	check("posts", 2)
	check("likes_current", 1)
	check("follows_current", 1)
	check("blocks_current", 1)

	// Counts populated.
	var alicePostCount, aliceFollower, aliceFollowing int64
	if err := db.QueryRowContext(ctx,
		`SELECT post_count, follower_count, following_count FROM actors WHERE did = 'did:plc:alice'`,
	).Scan(&alicePostCount, &aliceFollower, &aliceFollowing); err != nil {
		t.Fatalf("scan alice: %v", err)
	}
	if alicePostCount != 1 || aliceFollower != 0 || aliceFollowing != 1 {
		t.Fatalf("alice counts: post=%d follower=%d following=%d",
			alicePostCount, aliceFollower, aliceFollowing)
	}

	// Post p1 should have like_count = 1, reply_count = 1.
	var p1Likes, p1Replies int64
	if err := db.QueryRowContext(ctx, `
		SELECT p.like_count, p.reply_count
		  FROM posts p JOIN actors_registry r ON r.actor_id = p.author_id
		 WHERE r.did = 'did:plc:alice' AND p.rkey = 'p1'
	`).Scan(&p1Likes, &p1Replies); err != nil {
		t.Fatalf("scan p1: %v", err)
	}
	if p1Likes != 1 || p1Replies != 1 {
		t.Fatalf("p1 counts: likes=%d replies=%d", p1Likes, p1Replies)
	}

	// latest.json local mirror should exist with sane fields.
	if _, err := os.Stat(filepath.Join(dataDir, "latest.json")); err != nil {
		t.Fatalf("latest.json missing: %v", err)
	}

	// current_graph should hold the graph subset only (actors + follows + blocks + _meta).
	gdb, err := sql.Open("duckdb", graphPath+"?access_mode=READ_ONLY")
	if err != nil {
		t.Fatalf("open graph: %v", err)
	}
	defer gdb.Close()
	var gActors, gFollows, gBlocks int64
	if err := gdb.QueryRowContext(ctx, `SELECT count(*) FROM actors`).Scan(&gActors); err != nil {
		t.Fatalf("graph actors count: %v", err)
	}
	if err := gdb.QueryRowContext(ctx, `SELECT count(*) FROM follows_current`).Scan(&gFollows); err != nil {
		t.Fatalf("graph follows count: %v", err)
	}
	if err := gdb.QueryRowContext(ctx, `SELECT count(*) FROM blocks_current`).Scan(&gBlocks); err != nil {
		t.Fatalf("graph blocks count: %v", err)
	}
	if gActors != 2 || gFollows != 1 || gBlocks != 1 {
		t.Fatalf("graph subset mismatch: actors=%d follows=%d blocks=%d", gActors, gFollows, gBlocks)
	}
}

// TestIncrementalReplay_WithLabelerTakedowns writes a synthetic day,
// seeds labels.db with a !takedown for Alice's post p1, runs the build,
// and asserts the labeler source nullified the post content (same
// semantics as takedowns.yaml). Confirms the end-to-end wiring through
// build.Run.
func TestIncrementalReplay_WithLabelerTakedowns(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	dataDir := tmp
	dailyDir := filepath.Join(dataDir, "daily", "2026-04-24")

	w, err := archive.NewWriter(ctx, dailyDir)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	t.Cleanup(func() { w.Close() })

	day := archive.Day("2026-04-24")
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)

	if _, _, err := w.WriteProfileUpdates(ctx, day, []archive.ProfileUpdateEvent{
		{DID: "did:plc:alice", Handle: "alice.bsky.social", DisplayName: "Alice", EventTS: now},
	}); err != nil {
		t.Fatalf("WriteProfileUpdates: %v", err)
	}
	if _, _, err := w.WritePosts(ctx, day, []archive.PostEvent{
		{
			EventType: "create", URI: "at://did:plc:alice/app.bsky.feed.post/p1",
			DID: "did:plc:alice", Rkey: "p1", CID: "bafyP1",
			EventTS: now, RecordTS: now, Text: "hello", Lang: "en",
		},
	}); err != nil {
		t.Fatalf("WritePosts: %v", err)
	}
	if _, err := w.WriteManifest(ctx, archive.Manifest{
		Date: day, JetstreamCursorEnd: 1714000000000000,
	}); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Seed labels.db at <dataDir>/labels.db with a !takedown targeting
	// Alice's post p1. That's the same URI the build will have just
	// inserted.
	labelsDB := filepath.Join(dataDir, "labels.db")
	seedDB, err := sql.Open("sqlite", "file:"+labelsDB+"?_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatalf("open labels.db: %v", err)
	}
	if _, err := seedDB.Exec(`
CREATE TABLE labels (
  seq    BIGINT NOT NULL,
  src    TEXT   NOT NULL,
  uri    TEXT   NOT NULL,
  cid    TEXT,
  val    TEXT   NOT NULL,
  cts    TEXT   NOT NULL,
  exp    TEXT,
  neg    INTEGER NOT NULL,
  PRIMARY KEY (src, uri, val, cts)
);`); err != nil {
		t.Fatalf("schema: %v", err)
	}
	if _, err := seedDB.Exec(`
INSERT INTO labels (seq, src, uri, val, cts, neg)
VALUES (1, 'did:plc:lbl', 'at://did:plc:alice/app.bsky.feed.post/p1',
        '!takedown', '2026-04-24T15:00:00Z', 0)`); err != nil {
		t.Fatalf("seed label: %v", err)
	}
	if err := seedDB.Close(); err != nil {
		t.Fatalf("close seedDB: %v", err)
	}

	cfg := config.Default()
	cfg.DataDir = dataDir
	opts := Options{
		Mode:       ModeIncremental,
		NoUpload:   true,
		Date:       "2026-04-24",
		RetainDays: 30,
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	if err := Run(ctx, cfg, opts, logger); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Open the produced snapshot and confirm p1's content is nullified.
	allPath := filepath.Join(dataDir, "current_all.duckdb")
	db, err := sql.Open("duckdb", allPath+"?access_mode=READ_ONLY")
	if err != nil {
		t.Fatalf("open all: %v", err)
	}
	defer db.Close()

	var text, cid sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT p.text, p.cid
		  FROM posts p JOIN actors_registry r ON r.actor_id = p.author_id
		 WHERE r.did = 'did:plc:alice' AND p.rkey = 'p1'
	`).Scan(&text, &cid); err != nil {
		t.Fatalf("scan p1: %v", err)
	}
	if text.Valid || cid.Valid {
		t.Fatalf("expected labeler takedown to nullify post; got text=%v cid=%v", text, cid)
	}

	// Audit row should exist in takedowns_applied.
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM takedowns_applied WHERE uri = 'at://did:plc:alice/app.bsky.feed.post/p1'`).Scan(&n); err != nil {
		t.Fatalf("audit query: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 audit row, got %d", n)
	}

	// Re-run the build with -skip-labeler-takedowns: the URI is already
	// applied, so this should be a no-op (idempotence + skip flag).
	// Close the prior read-only handle first — DuckDB rejects a second
	// open with a different mode concurrently.
	db.Close()
	opts.SkipLabelerTakedowns = true
	if err := Run(ctx, cfg, opts, logger); err != nil {
		t.Fatalf("Run (skip=true): %v", err)
	}
	db2, err := sql.Open("duckdb", allPath+"?access_mode=READ_ONLY")
	if err != nil {
		t.Fatalf("reopen all: %v", err)
	}
	defer db2.Close()
	if err := db2.QueryRowContext(ctx, `SELECT count(*) FROM takedowns_applied`).Scan(&n); err != nil {
		t.Fatalf("re-query audit: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected audit to stay at 1, got %d", n)
	}
}

// TestFilterLangs writes a day with three posts (en, ja, no-lang), runs
// the incremental build with Filters.Posts.Langs=["en"] and ExcludeNoLang
// enabled, and asserts only the English post survives in current_all.
func TestFilterLangs(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	dataDir := tmp
	dailyDir := filepath.Join(dataDir, "daily", "2026-04-24")

	w, err := archive.NewWriter(ctx, dailyDir)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	t.Cleanup(func() { w.Close() })

	day := archive.Day("2026-04-24")
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)

	if _, _, err := w.WriteProfileUpdates(ctx, day, []archive.ProfileUpdateEvent{
		{DID: "did:plc:alice", Handle: "alice.bsky.social", DisplayName: "Alice", EventTS: now},
		{DID: "did:plc:bob", Handle: "bob.bsky.social", DisplayName: "Bob", EventTS: now},
		{DID: "did:plc:carol", Handle: "carol.bsky.social", DisplayName: "Carol", EventTS: now},
	}); err != nil {
		t.Fatalf("WriteProfileUpdates: %v", err)
	}

	if _, _, err := w.WritePosts(ctx, day, []archive.PostEvent{
		{
			EventType: "create", URI: "at://did:plc:alice/app.bsky.feed.post/p1",
			DID: "did:plc:alice", Rkey: "p1", CID: "bafyP1",
			EventTS: now, RecordTS: now, Text: "hello", Lang: "en",
		},
		{
			EventType: "create", URI: "at://did:plc:bob/app.bsky.feed.post/p2",
			DID: "did:plc:bob", Rkey: "p2", CID: "bafyP2",
			EventTS: now, RecordTS: now, Text: "konnichiwa", Lang: "ja",
		},
		{
			EventType: "create", URI: "at://did:plc:carol/app.bsky.feed.post/p3",
			DID: "did:plc:carol", Rkey: "p3", CID: "bafyP3",
			EventTS: now, RecordTS: now, Text: "no lang", // Lang empty
		},
	}); err != nil {
		t.Fatalf("WritePosts: %v", err)
	}
	if _, err := w.WriteManifest(ctx, archive.Manifest{
		Date: day, JetstreamCursorEnd: 1714000000000000,
	}); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	cfg := config.Default()
	cfg.DataDir = dataDir
	cfg.Filters = config.FilterConfig{
		Posts: config.PostFilter{
			Langs:         []string{"en"},
			ExcludeNoLang: true,
		},
	}
	opts := Options{
		Mode:       ModeIncremental,
		NoUpload:   true,
		Date:       "2026-04-24",
		RetainDays: 30,
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	if err := Run(ctx, cfg, opts, logger); err != nil {
		t.Fatalf("Run: %v", err)
	}

	allPath := filepath.Join(dataDir, "current_all.duckdb")
	db, err := sql.Open("duckdb", allPath+"?access_mode=READ_ONLY")
	if err != nil {
		t.Fatalf("open all: %v", err)
	}
	defer db.Close()

	var nPosts int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM posts`).Scan(&nPosts); err != nil {
		t.Fatalf("count posts: %v", err)
	}
	if nPosts != 1 {
		t.Fatalf("expected 1 post after lang filter, got %d", nPosts)
	}
	var lang sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT lang FROM posts`).Scan(&lang); err != nil {
		t.Fatalf("scan lang: %v", err)
	}
	if !lang.Valid || lang.String != "en" {
		t.Fatalf("expected only English post to survive, got lang=%v", lang)
	}
}
