package duckdbstore

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// TestOpenAppliesSchema opens a new Store and verifies the core tables
// exist by inserting and selecting a row.
func TestOpenAppliesSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "graph.duckdb")
	s, err := Open(path, "", 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	for _, tbl := range []string{"actors", "follows_current", "blocks_current", "_meta"} {
		if _, err := s.DB.Exec("SELECT count(*) FROM " + tbl); err != nil {
			t.Fatalf("table %s not present: %v", tbl, err)
		}
	}
	if err := s.WriteMeta("backfill", 0, 0); err != nil {
		t.Fatalf("WriteMeta: %v", err)
	}
}

// TestStreamingWriterAppends verifies actors / follows / blocks round-trip
// through the Appender API and counts match.
func TestStreamingWriterAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "graph.duckdb")
	s, err := Open(path, "", 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close store for writer: %v", err)
	}

	w, err := NewStreamingWriter(path)
	if err != nil {
		t.Fatalf("NewStreamingWriter: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	actors := []ActorRow{
		{ActorID: 1, DID: "did:plc:a", Handle: "a.bsky.social", CreatedAt: now, IndexedAt: now},
		{ActorID: 2, DID: "did:plc:b", Handle: "b.bsky.social", CreatedAt: now, IndexedAt: now},
		{ActorID: 3, DID: "did:plc:c", CreatedAt: now, IndexedAt: now},
	}
	for _, a := range actors {
		if err := w.AppendActor(a); err != nil {
			t.Fatalf("AppendActor: %v", err)
		}
	}
	follows := []FollowRow{
		{SrcID: 1, DstID: 2, Rkey: "f1", CreatedAt: now},
		{SrcID: 1, DstID: 3, Rkey: "f2", CreatedAt: now},
		{SrcID: 2, DstID: 3, Rkey: "f3", CreatedAt: now},
	}
	for _, f := range follows {
		if err := w.AppendFollow(f); err != nil {
			t.Fatalf("AppendFollow: %v", err)
		}
	}
	blocks := []BlockRow{
		{SrcID: 3, DstID: 1, Rkey: "b1", CreatedAt: now},
	}
	for _, b := range blocks {
		if err := w.AppendBlock(b); err != nil {
			t.Fatalf("AppendBlock: %v", err)
		}
	}
	if err := w.FlushAll(); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("writer Close: %v", err)
	}

	// Reopen to read counts.
	s2, err := Open(path, "", 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	counts := map[string]int64{}
	for _, tbl := range []string{"actors", "follows_current", "blocks_current"} {
		var n int64
		if err := s2.DB.QueryRow("SELECT count(*) FROM " + tbl).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", tbl, err)
		}
		counts[tbl] = n
	}
	if counts["actors"] != 3 {
		t.Fatalf("actors count = %d; want 3", counts["actors"])
	}
	if counts["follows_current"] != 3 {
		t.Fatalf("follows_current count = %d; want 3", counts["follows_current"])
	}
	if counts["blocks_current"] != 1 {
		t.Fatalf("blocks_current count = %d; want 1", counts["blocks_current"])
	}
}

// TestRecomputeCounts seeds a tiny actor/follow/block set and verifies the
// denormalized graph columns are correct.
func TestRecomputeCounts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "graph.duckdb")
	s, err := Open(path, "", 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	w, err := NewStreamingWriter(path)
	if err != nil {
		t.Fatalf("NewStreamingWriter: %v", err)
	}
	now := time.Now().UTC()
	for _, a := range []ActorRow{
		{ActorID: 1, DID: "did:plc:a", CreatedAt: now, IndexedAt: now},
		{ActorID: 2, DID: "did:plc:b", CreatedAt: now, IndexedAt: now},
		{ActorID: 3, DID: "did:plc:c", CreatedAt: now, IndexedAt: now},
	} {
		if err := w.AppendActor(a); err != nil {
			t.Fatalf("AppendActor: %v", err)
		}
	}
	// a → b, a → c, b → c. So a.following=2, b.following=1, c.following=0.
	// follower: a=0, b=1, c=2.
	for _, f := range []FollowRow{
		{SrcID: 1, DstID: 2, Rkey: "f1", CreatedAt: now},
		{SrcID: 1, DstID: 3, Rkey: "f2", CreatedAt: now},
		{SrcID: 2, DstID: 3, Rkey: "f3", CreatedAt: now},
	} {
		if err := w.AppendFollow(f); err != nil {
			t.Fatalf("AppendFollow: %v", err)
		}
	}
	// a blocks c; no block on others.
	if err := w.AppendBlock(BlockRow{SrcID: 1, DstID: 3, Rkey: "b1", CreatedAt: now}); err != nil {
		t.Fatalf("AppendBlock: %v", err)
	}
	if err := w.FlushAll(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("writer Close: %v", err)
	}

	// Reopen and run recompute.
	s2, err := Open(path, "", 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if err := s2.RecomputeCounts(); err != nil {
		t.Fatalf("RecomputeCounts: %v", err)
	}

	want := map[int64][3]int64{
		// actor_id → [following, follower, blocks_given]
		1: {2, 0, 1},
		2: {1, 1, 0},
		3: {0, 2, 0},
	}
	for id, w := range want {
		var following, follower, blocksGiven int64
		err := s2.DB.QueryRow(
			`SELECT following_count, follower_count, blocks_given_count FROM actors WHERE actor_id = ?`, id,
		).Scan(&following, &follower, &blocksGiven)
		if err != nil {
			t.Fatalf("row %d: %v", id, err)
		}
		if following != w[0] || follower != w[1] || blocksGiven != w[2] {
			t.Fatalf("actor %d: got (following=%d, follower=%d, blocks=%d); want (%d, %d, %d)",
				id, following, follower, blocksGiven, w[0], w[1], w[2])
		}
	}
	// Non-graph counts should have been zeroed, not NULL.
	var zeroed int64
	err = s2.DB.QueryRow(`SELECT count(*) FROM actors WHERE post_count = 0 AND likes_given_count = 0 AND likes_received_count = 0 AND reposts_received_count = 0`).Scan(&zeroed)
	if err != nil {
		t.Fatalf("count zeroed: %v", err)
	}
	if zeroed != 3 {
		t.Fatalf("zeroed-count actors = %d; want 3", zeroed)
	}

	_ = context.Background()
}
