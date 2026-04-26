package bootstrap

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// TestEmitFromMinimalGraphDB hand-crafts a tiny current_graph.duckdb,
// runs Emit, and verifies the produced parquet files exist and are
// readable.
func TestEmitFromMinimalGraphDB(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	graphPath := filepath.Join(tmp, "current_graph.duckdb")

	// Build a minimal graph DB. Schema mirrors store.go's schemaSQL but
	// re-declared here so we don't import that package's internals.
	db, err := sql.Open("duckdb", graphPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for _, q := range []string{
		`CREATE TABLE actors (
		  actor_id BIGINT PRIMARY KEY, did VARCHAR NOT NULL UNIQUE, handle VARCHAR,
		  display_name VARCHAR, description VARCHAR, avatar_cid VARCHAR,
		  created_at TIMESTAMP, indexed_at TIMESTAMP,
		  follower_count BIGINT, following_count BIGINT, post_count BIGINT,
		  likes_given_count BIGINT, likes_received_count BIGINT,
		  reposts_received_count BIGINT, blocks_given_count BIGINT
		)`,
		`CREATE TABLE follows_current (
		  src_id BIGINT NOT NULL, dst_id BIGINT NOT NULL,
		  rkey VARCHAR NOT NULL, created_at TIMESTAMP,
		  PRIMARY KEY (src_id, rkey)
		)`,
		`CREATE TABLE blocks_current (
		  src_id BIGINT NOT NULL, dst_id BIGINT NOT NULL,
		  rkey VARCHAR NOT NULL, created_at TIMESTAMP,
		  PRIMARY KEY (src_id, rkey)
		)`,
		`INSERT INTO actors (actor_id, did, handle) VALUES
		  (1, 'did:plc:alice', 'alice.bsky.social'),
		  (2, 'did:plc:bob',   'bob.bsky.social')`,
		`INSERT INTO follows_current (src_id, dst_id, rkey, created_at) VALUES (1, 2, 'F1', NOW())`,
		`INSERT INTO blocks_current  (src_id, dst_id, rkey, created_at) VALUES (2, 1, 'B1', NOW())`,
		`CHECKPOINT`,
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close setup: %v", err)
	}

	// Run Emit.
	dayDir := filepath.Join(tmp, "bootstrap", "2026-04-24")
	mf, err := Emit(ctx, graphPath, dayDir, 1714000000000000)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if mf.Date != "2026-04-24" {
		t.Fatalf("manifest date: %q", mf.Date)
	}
	if mf.RowCounts["profiles"] != 2 {
		t.Fatalf("profiles row count: %v", mf.RowCounts)
	}
	if mf.RowCounts["follows"] != 1 || mf.RowCounts["blocks"] != 1 {
		t.Fatalf("follows/blocks row count: %v", mf.RowCounts)
	}
	if mf.JetstreamCursor != 1714000000000000 {
		t.Fatalf("jetstream cursor not preserved: %d", mf.JetstreamCursor)
	}
	if mf.SnapshotTS.IsZero() || time.Since(mf.SnapshotTS) > time.Hour {
		t.Fatalf("snapshot_ts looks wrong: %s", mf.SnapshotTS)
	}

	// Sanity: read profiles.parquet back and confirm raw DIDs surface.
	rb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("read open: %v", err)
	}
	defer rb.Close()
	var did, handle string
	if err := rb.QueryRowContext(ctx, `SELECT did, handle FROM read_parquet(?) ORDER BY did LIMIT 1`,
		filepath.Join(dayDir, "profiles.parquet"),
	).Scan(&did, &handle); err != nil {
		t.Fatalf("scan profile: %v", err)
	}
	if did != "did:plc:alice" || handle != "alice.bsky.social" {
		t.Fatalf("profile mismatch: did=%q handle=%q", did, handle)
	}
}
