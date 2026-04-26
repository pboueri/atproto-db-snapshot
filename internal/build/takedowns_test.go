package build

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/marcboeker/go-duckdb/v2"
)

const sampleTakedownsYAML = `takedowns:
  - uri: at://did:plc:alice/app.bsky.feed.post/p1
    reason: CSAM report 2026-03-14
    date: 2026-03-14
  - uri: at://did:plc:bob/app.bsky.actor.profile/self
    reason: DMCA
    date: 2026-03-20
  - uri: at://did:plc:carol/app.bsky.feed.like/L1
    reason: harassment
    date: 2026-03-21
  - uri: at://did:plc:carol/app.bsky.graph.follow/F1
    reason: harassment
    date: 2026-03-21
`

// TestLoadTakedowns checks the YAML parser handles the spec-shaped input.
func TestLoadTakedowns(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "takedowns.yaml")
	if err := os.WriteFile(path, []byte(sampleTakedownsYAML), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	td, err := LoadTakedowns(path)
	if err != nil {
		t.Fatalf("LoadTakedowns: %v", err)
	}
	if len(td.URIs) != 4 {
		t.Fatalf("want 4 entries, got %d", len(td.URIs))
	}
	if td.URIs[0].URI != "at://did:plc:alice/app.bsky.feed.post/p1" {
		t.Errorf("entry 0 URI mismatch: %q", td.URIs[0].URI)
	}
	if td.URIs[1].Reason != "DMCA" {
		t.Errorf("entry 1 reason mismatch: %q", td.URIs[1].Reason)
	}

	// Missing file → empty, no error.
	missingTd, err := LoadTakedowns(filepath.Join(tmp, "does-not-exist.yaml"))
	if err != nil {
		t.Fatalf("missing file should not error: %v", err)
	}
	if len(missingTd.URIs) != 0 {
		t.Fatalf("missing file should produce empty Takedowns, got %d entries", len(missingTd.URIs))
	}
}

// seedTakedownsDB creates an in-memory DuckDB with the columns ApplyTakedowns
// touches and seeds enough rows to verify each branch.
func seedTakedownsDB(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })

	const schema = `
CREATE TABLE actors_registry (
  actor_id BIGINT PRIMARY KEY,
  did VARCHAR NOT NULL UNIQUE,
  first_seen TIMESTAMP
);
CREATE TABLE actors (
  actor_id BIGINT PRIMARY KEY,
  did VARCHAR NOT NULL UNIQUE,
  handle VARCHAR,
  display_name VARCHAR,
  description VARCHAR,
  avatar_cid VARCHAR,
  created_at TIMESTAMP,
  indexed_at TIMESTAMP,
  follower_count BIGINT,
  following_count BIGINT,
  post_count BIGINT,
  likes_given_count BIGINT,
  likes_received_count BIGINT,
  reposts_received_count BIGINT,
  blocks_given_count BIGINT
);
CREATE TABLE posts (
  author_id BIGINT NOT NULL,
  rkey VARCHAR NOT NULL,
  cid VARCHAR,
  created_at TIMESTAMP,
  text VARCHAR,
  lang VARCHAR,
  reply_root_author_id BIGINT,
  reply_root_rkey VARCHAR,
  reply_parent_author_id BIGINT,
  reply_parent_rkey VARCHAR,
  quote_author_id BIGINT,
  quote_rkey VARCHAR,
  embed_type VARCHAR,
  like_count BIGINT,
  repost_count BIGINT,
  reply_count BIGINT,
  PRIMARY KEY (author_id, rkey)
);
CREATE TABLE likes_current (
  liker_id BIGINT NOT NULL,
  rkey VARCHAR NOT NULL,
  subject_author_id BIGINT NOT NULL,
  subject_rkey VARCHAR NOT NULL,
  created_at TIMESTAMP,
  PRIMARY KEY (liker_id, rkey)
);
CREATE TABLE reposts_current (
  reposter_id BIGINT NOT NULL,
  rkey VARCHAR NOT NULL,
  subject_author_id BIGINT NOT NULL,
  subject_rkey VARCHAR NOT NULL,
  created_at TIMESTAMP,
  PRIMARY KEY (reposter_id, rkey)
);
CREATE TABLE follows_current (
  src_id BIGINT NOT NULL,
  dst_id BIGINT NOT NULL,
  rkey VARCHAR NOT NULL,
  created_at TIMESTAMP,
  PRIMARY KEY (src_id, rkey)
);
CREATE TABLE blocks_current (
  src_id BIGINT NOT NULL,
  dst_id BIGINT NOT NULL,
  rkey VARCHAR NOT NULL,
  created_at TIMESTAMP,
  PRIMARY KEY (src_id, rkey)
);
INSERT INTO actors_registry VALUES
  (1, 'did:plc:alice', now()),
  (2, 'did:plc:bob',   now()),
  (3, 'did:plc:carol', now());
INSERT INTO actors (actor_id, did, display_name, description, avatar_cid)
  VALUES
  (1, 'did:plc:alice', 'Alice', 'a bio',  'avA'),
  (2, 'did:plc:bob',   'Bob',   'b bio',  'avB'),
  (3, 'did:plc:carol', 'Carol', 'c bio',  'avC');
INSERT INTO posts (author_id, rkey, cid, text, lang, like_count, repost_count, reply_count)
  VALUES
  (1, 'p1', 'bafyP1', 'hello world', 'en', 0, 0, 0),
  (2, 'p2', 'bafyP2', 'hi back',     'en', 0, 0, 0);
INSERT INTO likes_current VALUES (3, 'L1', 1, 'p1', now());
INSERT INTO follows_current VALUES (3, 2, 'F1', now());
`
	if _, err := db.ExecContext(ctx, schema); err != nil {
		t.Fatalf("schema: %v", err)
	}
	if err := EnsureTakedownsAppliedTable(ctx, db); err != nil {
		t.Fatalf("EnsureTakedownsAppliedTable: %v", err)
	}
	return db
}

// TestApplyTakedowns checks each collection branch and that a second Apply
// call does not double-process.
func TestApplyTakedowns(t *testing.T) {
	ctx := context.Background()
	db := seedTakedownsDB(t, ctx)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	td := Takedowns{URIs: []TakedownEntry{
		{URI: "at://did:plc:alice/app.bsky.feed.post/p1", Reason: "CSAM"},
		{URI: "at://did:plc:bob/app.bsky.actor.profile/self", Reason: "DMCA"},
		{URI: "at://did:plc:carol/app.bsky.feed.like/L1", Reason: "harassment"},
		{URI: "at://did:plc:carol/app.bsky.graph.follow/F1", Reason: "harassment"},
	}}

	if err := ApplyTakedowns(ctx, db, td, logger); err != nil {
		t.Fatalf("ApplyTakedowns: %v", err)
	}

	// post: row preserved but content nullified.
	var nPosts int
	if err := db.QueryRow(`SELECT count(*) FROM posts WHERE author_id = 1 AND rkey = 'p1'`).Scan(&nPosts); err != nil {
		t.Fatalf("count post: %v", err)
	}
	if nPosts != 1 {
		t.Fatalf("expected post row preserved, got count=%d", nPosts)
	}
	var text, cid, lang, embed sql.NullString
	if err := db.QueryRow(
		`SELECT text, cid, lang, embed_type FROM posts WHERE author_id = 1 AND rkey = 'p1'`,
	).Scan(&text, &cid, &lang, &embed); err != nil {
		t.Fatalf("scan post: %v", err)
	}
	if text.Valid || cid.Valid || lang.Valid || embed.Valid {
		t.Fatalf("post fields not nullified: text=%v cid=%v lang=%v embed=%v", text, cid, lang, embed)
	}

	// profile: actors row remains, sensitive fields nulled.
	var dn, desc, avatar sql.NullString
	if err := db.QueryRow(
		`SELECT display_name, description, avatar_cid FROM actors WHERE did = 'did:plc:bob'`,
	).Scan(&dn, &desc, &avatar); err != nil {
		t.Fatalf("scan actor: %v", err)
	}
	if dn.Valid || desc.Valid || avatar.Valid {
		t.Fatalf("actor profile fields not nullified: dn=%v desc=%v avatar=%v", dn, desc, avatar)
	}

	// like: row deleted.
	var nLikes int
	if err := db.QueryRow(`SELECT count(*) FROM likes_current`).Scan(&nLikes); err != nil {
		t.Fatalf("count likes: %v", err)
	}
	if nLikes != 0 {
		t.Fatalf("expected like row deleted, got count=%d", nLikes)
	}

	// follow: row deleted.
	var nFollows int
	if err := db.QueryRow(`SELECT count(*) FROM follows_current`).Scan(&nFollows); err != nil {
		t.Fatalf("count follows: %v", err)
	}
	if nFollows != 0 {
		t.Fatalf("expected follow row deleted, got count=%d", nFollows)
	}

	// audit: 4 rows in takedowns_applied.
	var nApplied int
	if err := db.QueryRow(`SELECT count(*) FROM takedowns_applied`).Scan(&nApplied); err != nil {
		t.Fatalf("count takedowns_applied: %v", err)
	}
	if nApplied != 4 {
		t.Fatalf("expected 4 audit rows, got %d", nApplied)
	}

	// Second invocation must be a no-op (idempotence) — re-insert a like
	// row for the previously taken-down URI to confirm it isn't deleted
	// again on a redundant pass.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO likes_current VALUES (3, 'L1', 1, 'p1', now())`,
	); err != nil {
		t.Fatalf("re-insert like: %v", err)
	}
	if err := ApplyTakedowns(ctx, db, td, logger); err != nil {
		t.Fatalf("ApplyTakedowns (2nd call): %v", err)
	}
	var nLikesAfter int
	if err := db.QueryRow(`SELECT count(*) FROM likes_current`).Scan(&nLikesAfter); err != nil {
		t.Fatalf("count likes after second apply: %v", err)
	}
	if nLikesAfter != 1 {
		t.Fatalf("idempotence broken: re-inserted like row was deleted by second Apply (count=%d)", nLikesAfter)
	}
	// Audit table count unchanged.
	if err := db.QueryRow(`SELECT count(*) FROM takedowns_applied`).Scan(&nApplied); err != nil {
		t.Fatalf("count takedowns_applied: %v", err)
	}
	if nApplied != 4 {
		t.Fatalf("expected audit unchanged at 4, got %d", nApplied)
	}
}

// TestParseTakedownURI exercises a couple of malformed inputs.
func TestParseTakedownURI(t *testing.T) {
	if did, coll, rkey, err := parseTakedownURI("at://did:plc:x/app.bsky.feed.post/abc"); err != nil ||
		did != "did:plc:x" || coll != "app.bsky.feed.post" || rkey != "abc" {
		t.Fatalf("parse: did=%q coll=%q rkey=%q err=%v", did, coll, rkey, err)
	}
	if _, _, _, err := parseTakedownURI("did:plc:x/app.bsky.feed.post/abc"); err == nil {
		t.Fatalf("expected error on missing at:// prefix")
	}
	if _, _, _, err := parseTakedownURI("at://did:plc:x/app.bsky.feed.post"); err == nil {
		t.Fatalf("expected error on missing rkey")
	}
}
