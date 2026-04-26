package archive

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// TestWritePostEmbedsRoundTrip exercises the typed-struct write path and
// asserts the parquet shard reads back with the expected types and the
// three-state BOOLEAN for video_has_alt (NULL / false / true).
func TestWritePostEmbedsRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const day Day = "2026-04-24"

	w, err := NewWriter(ctx, dir)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer w.Close()

	ts := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	rows := []PostEmbedEvent{
		{ // images, one with alt out of two
			URI: "at://did:plc:a/app.bsky.feed.post/1", DID: "did:plc:a", Rkey: "1",
			EventTS: ts, Kind: "images",
			ImageCount: 2, ImageWithAltCount: 1,
		},
		{ // external link
			URI: "at://did:plc:a/app.bsky.feed.post/2", DID: "did:plc:a", Rkey: "2",
			EventTS: ts, Kind: "external",
			ExternalURI: "https://example.com/x", ExternalDomain: "example.com", ExternalTitle: "X",
		},
		{ // video, no alt
			URI: "at://did:plc:a/app.bsky.feed.post/3", DID: "did:plc:a", Rkey: "3",
			EventTS: ts, Kind: "video", HasVideo: true, VideoHasAlt: false,
		},
		{ // video, with alt
			URI: "at://did:plc:a/app.bsky.feed.post/4", DID: "did:plc:a", Rkey: "4",
			EventTS: ts, Kind: "video", HasVideo: true, VideoHasAlt: true,
		},
		{ // recordWithMedia:images carries both quote + media counts
			URI: "at://did:plc:a/app.bsky.feed.post/5", DID: "did:plc:a", Rkey: "5",
			EventTS: ts, Kind: "recordWithMedia:images",
			ImageCount: 1, ImageWithAltCount: 1,
		},
	}

	n, b, err := w.WritePostEmbeds(ctx, day, rows)
	if err != nil {
		t.Fatalf("WritePostEmbeds: %v", err)
	}
	if n != int64(len(rows)) || b <= 0 {
		t.Fatalf("WritePostEmbeds counts: rows=%d bytes=%d", n, b)
	}

	// Empty input → no file.
	zn, zb, err := w.WritePostEmbeds(ctx, day, nil)
	if err != nil || zn != 0 || zb != 0 {
		t.Fatalf("WritePostEmbeds empty: rows=%d bytes=%d err=%v", zn, zb, err)
	}
	// (The non-empty call above already wrote post_embeds.parquet, so it'll
	// still exist — the empty-input path is exercised by checking the
	// returned counts, not the filesystem.)

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	// Image row: SMALLINT counts come back as int16.
	var imgCount, imgWithAlt sql.NullInt16
	if err := db.QueryRowContext(ctx,
		`SELECT image_count, image_with_alt FROM read_parquet('`+filepath.Join(dir, "post_embeds.parquet")+`')
		 WHERE rkey = '1'`,
	).Scan(&imgCount, &imgWithAlt); err != nil {
		t.Fatalf("scan images row: %v", err)
	}
	if !imgCount.Valid || imgCount.Int16 != 2 || !imgWithAlt.Valid || imgWithAlt.Int16 != 1 {
		t.Fatalf("images counts: count=%v with_alt=%v", imgCount, imgWithAlt)
	}

	// External row: image counts are NULL; external_domain populated.
	var domain sql.NullString
	if err := db.QueryRowContext(ctx,
		`SELECT external_domain, image_count FROM read_parquet('`+filepath.Join(dir, "post_embeds.parquet")+`')
		 WHERE rkey = '2'`,
	).Scan(&domain, &imgCount); err != nil {
		t.Fatalf("scan external row: %v", err)
	}
	if !domain.Valid || domain.String != "example.com" {
		t.Fatalf("external_domain = %v", domain)
	}
	if imgCount.Valid {
		t.Fatalf("image_count for external should be NULL, got %v", imgCount)
	}

	// Video without alt: BOOLEAN false (not NULL).
	var hasAlt sql.NullBool
	if err := db.QueryRowContext(ctx,
		`SELECT video_has_alt FROM read_parquet('`+filepath.Join(dir, "post_embeds.parquet")+`')
		 WHERE rkey = '3'`,
	).Scan(&hasAlt); err != nil {
		t.Fatalf("scan video row: %v", err)
	}
	if !hasAlt.Valid || hasAlt.Bool {
		t.Fatalf("video_has_alt for rkey=3: %v (want valid=true bool=false)", hasAlt)
	}

	// Video with alt.
	if err := db.QueryRowContext(ctx,
		`SELECT video_has_alt FROM read_parquet('`+filepath.Join(dir, "post_embeds.parquet")+`')
		 WHERE rkey = '4'`,
	).Scan(&hasAlt); err != nil {
		t.Fatalf("scan video row 4: %v", err)
	}
	if !hasAlt.Valid || !hasAlt.Bool {
		t.Fatalf("video_has_alt for rkey=4: %v (want true)", hasAlt)
	}

	// Image row: video_has_alt should be NULL.
	if err := db.QueryRowContext(ctx,
		`SELECT video_has_alt FROM read_parquet('`+filepath.Join(dir, "post_embeds.parquet")+`')
		 WHERE rkey = '1'`,
	).Scan(&hasAlt); err != nil {
		t.Fatalf("scan images row video flag: %v", err)
	}
	if hasAlt.Valid {
		t.Fatalf("video_has_alt should be NULL on image row, got %v", hasAlt)
	}

	// recordWithMedia:images: kind string preserved verbatim.
	var kind string
	if err := db.QueryRowContext(ctx,
		`SELECT kind FROM read_parquet('`+filepath.Join(dir, "post_embeds.parquet")+`')
		 WHERE rkey = '5'`,
	).Scan(&kind); err != nil {
		t.Fatalf("scan rwm row: %v", err)
	}
	if kind != "recordWithMedia:images" {
		t.Fatalf("kind = %q; want recordWithMedia:images", kind)
	}

	// File path should exist.
	if _, err := os.Stat(filepath.Join(dir, "post_embeds.parquet")); err != nil {
		t.Fatalf("expected post_embeds.parquet to exist: %v", err)
	}
}
