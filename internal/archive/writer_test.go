package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

func TestWriterRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const day Day = "2026-04-24"

	w, err := NewWriter(ctx, dir)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer w.Close()

	eventTS := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	postRows := []PostEvent{{
		EventType: "create", URI: "at://did:plc:abc/app.bsky.feed.post/3kn",
		DID: "did:plc:abc", Rkey: "3kn", CID: "bafy1",
		EventTS: eventTS, Text: "hello", Lang: "en",
	}}
	likeRows := []LikeEvent{
		{EventType: "create", URI: "at://did:plc:liker/app.bsky.feed.like/r1",
			LikerDID: "did:plc:liker", SubjectURI: "at://x", SubjectCID: "bafyL", EventTS: eventTS},
	}

	pn, pb, err := w.WritePosts(ctx, day, postRows)
	if err != nil {
		t.Fatalf("WritePosts: %v", err)
	}
	if pn != 1 || pb <= 0 {
		t.Fatalf("WritePosts counts: rows=%d bytes=%d", pn, pb)
	}
	ln, lb, err := w.WriteLikes(ctx, day, likeRows)
	if err != nil {
		t.Fatalf("WriteLikes: %v", err)
	}
	if ln != 1 || lb <= 0 {
		t.Fatalf("WriteLikes counts: rows=%d bytes=%d", ln, lb)
	}

	// Empty collections do not produce a file.
	zn, zb, err := w.WriteFollows(ctx, day, nil)
	if err != nil || zn != 0 || zb != 0 {
		t.Fatalf("WriteFollows empty: rows=%d bytes=%d err=%v", zn, zb, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "follows.parquet")); !os.IsNotExist(err) {
		t.Fatalf("empty follows.parquet should not exist, stat err=%v", err)
	}

	// Read parquet back via in-memory DuckDB.
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	var gotText, gotLang, gotDID string
	row := db.QueryRowContext(ctx,
		`SELECT did, text, lang FROM read_parquet('`+filepath.Join(dir, "posts.parquet")+`')`)
	if err := row.Scan(&gotDID, &gotText, &gotLang); err != nil {
		t.Fatalf("scan posts: %v", err)
	}
	if gotDID != "did:plc:abc" || gotText != "hello" || gotLang != "en" {
		t.Fatalf("posts row mismatch: did=%q text=%q lang=%q", gotDID, gotText, gotLang)
	}

	var likerDID, eventType string
	row = db.QueryRowContext(ctx,
		`SELECT liker_did, event_type FROM read_parquet('`+filepath.Join(dir, "likes.parquet")+`')`)
	if err := row.Scan(&likerDID, &eventType); err != nil {
		t.Fatalf("scan likes: %v", err)
	}
	if likerDID != "did:plc:liker" || eventType != "create" {
		t.Fatalf("likes row mismatch: liker=%q event_type=%q", likerDID, eventType)
	}

	// Manifest round-trips.
	manifestIn := Manifest{
		Date: day, SchemaVersion: "v1",
		JetstreamCursorStart: 1, JetstreamCursorEnd: 2,
		BuiltAt:   eventTS,
		RowCounts: map[string]int64{"posts": pn, "likes": ln},
		Bytes:     map[string]int64{"posts.parquet": pb, "likes.parquet": lb},
	}
	mpath, err := w.WriteManifest(ctx, manifestIn)
	if err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	raw, err := os.ReadFile(mpath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var got Manifest
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	if got.Date != day || got.RowCounts["posts"] != 1 || got.Bytes["likes.parquet"] != lb {
		t.Fatalf("manifest mismatch: %+v", got)
	}
}
