package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
	_ "modernc.org/sqlite"

	"github.com/pboueri/atproto-db-snapshot/internal/build"
	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/duckdbstore"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
)

// TestPipelineTakedownsEndToEnd exercises the full takedown application
// path through build.Run for both the YAML-driven and labeler-driven
// sources, then re-runs build.Run to verify idempotence (no double-apply
// against takedowns_applied).
//
// Per spec §19:
//   - app.bsky.feed.post: nullify text/cid/lang/embed_type/etc., keep row
//   - app.bsky.actor.profile: nullify display_name/description/avatar_cid
//   - app.bsky.feed.like: DELETE the row outright
func TestPipelineTakedownsEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e takedowns test in -short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir data: %v", err)
	}
	storeDir := filepath.Join(tmp, "store")

	allDBPath := filepath.Join(dataDir, "current_all.duckdb")
	labelsDBPath := filepath.Join(dataDir, "labels.db")
	yamlPath := filepath.Join(tmp, "takedowns.yaml")

	// rkey on the like that the labeler will take down. We need to know
	// it up-front so we can seed it AND label it.
	const labelerLikeRkey = "lk-a-3"

	seedAllStore(t, allDBPath, labelerLikeRkey)
	seedTakedownLabelsDB(t, labelsDBPath, labelerLikeRkey)
	writeTakedownsYAML(t, yamlPath)

	cfg := config.Default()
	cfg.DataDir = dataDir

	logger := newSilentLogger()
	opts := build.Options{
		Mode:                 build.ModeIncremental,
		Date:                 time.Now().UTC().Format("2006-01-02"),
		TakedownsPath:        yamlPath,
		SkipLabelerTakedowns: false,
		NoUpload:             true,
		Store:                objstore.NewFileStore(storeDir, ""),
	}

	// First build: should apply 4 takedowns (2 yaml + 2 labeler).
	const expectedApplied = 4
	if err := build.Run(ctx, cfg, opts, logger); err != nil {
		t.Fatalf("build.Run (first): %v", err)
	}
	assertRedactions(t, allDBPath, labelerLikeRkey, expectedApplied)

	// Second build: idempotence — counts must not change, redactions stick.
	if err := build.Run(ctx, cfg, opts, logger); err != nil {
		t.Fatalf("build.Run (second): %v", err)
	}
	assertRedactions(t, allDBPath, labelerLikeRkey, expectedApplied)
}

// seedAllStore hand-constructs current_all.duckdb at allPath with the
// schema OpenAll provides, then INSERTs a tiny fixture: 3 actors, 4
// posts, 3 likes on p3 (one of which is the labeler's takedown target),
// 1 follow, 1 actor-profile display_name, and a _meta row dated in the
// past so the incremental build flows through the no-parquet branch.
func seedAllStore(t *testing.T, allPath, labelerLikeRkey string) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(allPath), 0o755); err != nil {
		t.Fatalf("mkdir all parent: %v", err)
	}
	store, err := duckdbstore.OpenAll(allPath, "1GB", 1)
	if err != nil {
		t.Fatalf("OpenAll: %v", err)
	}

	db := store.DB
	const ddl = `
INSERT INTO actors_registry VALUES
  (1, 'did:plc:a', now()),
  (2, 'did:plc:b', now()),
  (3, 'did:plc:c', now());

-- actor 2 has display_name "B" (yaml profile takedown nullifies this).
INSERT INTO actors (
  actor_id, did, handle, display_name, description, avatar_cid,
  follower_count, following_count, post_count,
  likes_given_count, likes_received_count, reposts_received_count, blocks_given_count
) VALUES
  (1, 'did:plc:a', NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, 0, 0),
  (2, 'did:plc:b', NULL, 'B',  NULL, NULL, 0, 0, 0, 0, 0, 0, 0),
  (3, 'did:plc:c', NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, 0, 0);

INSERT INTO posts (
  author_id, rkey, cid, created_at, text, lang,
  reply_root_author_id, reply_root_rkey,
  reply_parent_author_id, reply_parent_rkey,
  quote_author_id, quote_rkey,
  embed_type, like_count, repost_count, reply_count
) VALUES
  (1, 'p1', 'bafyP1', now(), 'this gets nullified by yaml',     'en', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0),
  (2, 'p2', 'bafyP2', now(), 'this gets nullified by labeler',  'en', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0),
  (2, 'p3', 'bafyP3', now(), 'untouched',                       'en', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0),
  (3, 'p4', 'bafyP4', now(), 'untouched',                       'en', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);

-- a follows b
INSERT INTO follows_current VALUES (1, 2, 'f1', now());
`
	if _, err := db.Exec(ddl); err != nil {
		store.Close()
		t.Fatalf("seed ddl: %v", err)
	}

	// Three likes on (author=2, rkey=p3): one from a, one from b, one from c.
	// `a`'s like uses the rkey the labeler will target; the other two
	// MUST remain after the build.
	likes := []struct {
		liker int64
		rkey  string
	}{
		{1, labelerLikeRkey},
		{2, "lk-b-3"},
		{3, "lk-c-3"},
	}
	for _, l := range likes {
		if _, err := db.Exec(
			`INSERT INTO likes_current VALUES (?, ?, 2, 'p3', now())`,
			l.liker, l.rkey,
		); err != nil {
			store.Close()
			t.Fatalf("insert like %d/%s: %v", l.liker, l.rkey, err)
		}
	}

	// _meta with a built_at well in the past so incremental sees a
	// non-zero "last build" but no daily dirs to replay either.
	pastBuilt := time.Now().UTC().Add(-48 * time.Hour)
	if err := store.WriteMeta(pastBuilt, 1234567890, "incremental", "{}"); err != nil {
		store.Close()
		t.Fatalf("WriteMeta: %v", err)
	}
	if err := store.Checkpoint(); err != nil {
		store.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// seedTakedownLabelsDB writes a labels.db at path with two label rows:
//   - a positive `!takedown` on did:plc:b/app.bsky.feed.post/p2  → applied
//   - a NEGATED `!hide`     on did:plc:c/app.bsky.feed.post/p4  → NOT applied
//   - a positive `!takedown` on did:plc:a/app.bsky.feed.like/<rkey>
//
// Schema mirrors what the `labels` subcommand writes (see
// internal/labels/store.go ensureSchema).
func seedTakedownLabelsDB(t *testing.T, path, labelerLikeRkey string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir labels.db parent: %v", err)
	}
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatalf("open labels.db: %v", err)
	}
	defer db.Close()

	const ddl = `
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
);
CREATE INDEX labels_val ON labels(val);
CREATE INDEX labels_seq ON labels(seq);
`
	if _, err := db.Exec(ddl); err != nil {
		t.Fatalf("schema labels: %v", err)
	}

	const src = "did:plc:ar7c4by46qjdydhdevvrndac"
	const ins = `
INSERT INTO labels (seq, src, uri, cid, val, cts, exp, neg)
VALUES (?, ?, ?, NULL, ?, ?, NULL, ?)`

	rows := []struct {
		seq int64
		uri string
		val string
		neg int
	}{
		// Positive takedown on p2 → must be applied.
		{1, "at://did:plc:b/app.bsky.feed.post/p2", "!takedown", 0},
		// Negated !hide on p4 → MUST be ignored (negated).
		{2, "at://did:plc:c/app.bsky.feed.post/p4", "!hide", 1},
		// Positive takedown on a's like → like row gets DELETED.
		{3, fmt.Sprintf("at://did:plc:a/app.bsky.feed.like/%s", labelerLikeRkey), "!takedown", 0},
	}
	for _, r := range rows {
		if _, err := db.Exec(ins,
			r.seq, src, r.uri, r.val, "2026-04-24T00:00:00Z", r.neg,
		); err != nil {
			t.Fatalf("insert label %d: %v", r.seq, err)
		}
	}
}

// writeTakedownsYAML drops a takedowns.yaml at path with one post URI
// (did:plc:a/p1) and one profile URI (did:plc:b/self).
func writeTakedownsYAML(t *testing.T, path string) {
	t.Helper()
	const body = `takedowns:
  - uri: at://did:plc:a/app.bsky.feed.post/p1
    reason: yaml-test
    date: 2026-04-25
  - uri: at://did:plc:b/app.bsky.actor.profile/self
    reason: yaml-profile-test
    date: 2026-04-25
`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write takedowns.yaml: %v", err)
	}
}

// assertRedactions opens current_all.duckdb read-only and checks that:
//   - p1 is nullified (yaml post)
//   - p2 is nullified (labeler post)
//   - p3 / p4 are untouched (negated label must not redact p4)
//   - actor 2 display_name is NULL (yaml profile)
//   - the labeler-targeted like row has been deleted; the other two remain
//   - takedowns_applied has exactly `wantApplied` rows.
func assertRedactions(t *testing.T, allPath, labelerLikeRkey string, wantApplied int) {
	t.Helper()
	db, err := sql.Open("duckdb", allPath+"?access_mode=READ_ONLY")
	if err != nil {
		t.Fatalf("open current_all.duckdb: %v", err)
	}
	defer db.Close()

	// p1 row exists; text IS NULL.
	{
		var n int
		var text sql.NullString
		if err := db.QueryRow(
			`SELECT count(*) FROM posts WHERE author_id = 1 AND rkey = 'p1'`,
		).Scan(&n); err != nil {
			t.Fatalf("count p1: %v", err)
		}
		if n != 1 {
			t.Fatalf("p1 row: got count=%d, want 1", n)
		}
		if err := db.QueryRow(
			`SELECT text FROM posts WHERE author_id = 1 AND rkey = 'p1'`,
		).Scan(&text); err != nil {
			t.Fatalf("scan p1.text: %v", err)
		}
		if text.Valid {
			t.Errorf("p1 text should be NULL after yaml takedown, got %q", text.String)
		}
	}

	// p2 row exists; text IS NULL (labeler).
	{
		var n int
		var text sql.NullString
		if err := db.QueryRow(
			`SELECT count(*) FROM posts WHERE author_id = 2 AND rkey = 'p2'`,
		).Scan(&n); err != nil {
			t.Fatalf("count p2: %v", err)
		}
		if n != 1 {
			t.Fatalf("p2 row: got count=%d, want 1", n)
		}
		if err := db.QueryRow(
			`SELECT text FROM posts WHERE author_id = 2 AND rkey = 'p2'`,
		).Scan(&text); err != nil {
			t.Fatalf("scan p2.text: %v", err)
		}
		if text.Valid {
			t.Errorf("p2 text should be NULL after labeler takedown, got %q", text.String)
		}
	}

	// p3 untouched.
	{
		var text sql.NullString
		if err := db.QueryRow(
			`SELECT text FROM posts WHERE author_id = 2 AND rkey = 'p3'`,
		).Scan(&text); err != nil {
			t.Fatalf("scan p3.text: %v", err)
		}
		if !text.Valid || text.String != "untouched" {
			t.Errorf("p3 text: got %v, want %q", text, "untouched")
		}
	}

	// p4 untouched (negated label MUST NOT redact).
	{
		var text sql.NullString
		if err := db.QueryRow(
			`SELECT text FROM posts WHERE author_id = 3 AND rkey = 'p4'`,
		).Scan(&text); err != nil {
			t.Fatalf("scan p4.text: %v", err)
		}
		if !text.Valid || text.String != "untouched" {
			t.Errorf("p4 text: got %v, want %q (negated label must not redact)", text, "untouched")
		}
	}

	// actor 2 display_name is NULL after yaml profile takedown.
	{
		var dn sql.NullString
		if err := db.QueryRow(
			`SELECT display_name FROM actors WHERE actor_id = 2`,
		).Scan(&dn); err != nil {
			t.Fatalf("scan actor 2 display_name: %v", err)
		}
		if dn.Valid {
			t.Errorf("actor 2 display_name should be NULL after profile takedown, got %q", dn.String)
		}
	}

	// labeler-targeted like deleted; the other two on (2, p3) remain.
	{
		var n int
		if err := db.QueryRow(
			`SELECT count(*) FROM likes_current WHERE liker_id = 1 AND rkey = ?`,
			labelerLikeRkey,
		).Scan(&n); err != nil {
			t.Fatalf("count labeler-like: %v", err)
		}
		if n != 0 {
			t.Errorf("labeler-targeted like row should be deleted, got count=%d", n)
		}

		var total int
		if err := db.QueryRow(
			`SELECT count(*) FROM likes_current WHERE subject_author_id = 2 AND subject_rkey = 'p3'`,
		).Scan(&total); err != nil {
			t.Fatalf("count likes on p3: %v", err)
		}
		if total != 2 {
			t.Errorf("expected 2 surviving likes on p3 after takedown, got %d", total)
		}
	}

	// takedowns_applied count.
	{
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM takedowns_applied`).Scan(&n); err != nil {
			t.Fatalf("count takedowns_applied: %v", err)
		}
		if n != wantApplied {
			t.Errorf("takedowns_applied: got %d, want %d", n, wantApplied)
		}
	}
}

// newSilentLogger returns a slog logger that discards output.
func newSilentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}
