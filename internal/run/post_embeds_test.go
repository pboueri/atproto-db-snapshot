package run

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// TestPostEmbedExport drives the Jetstream→parquet path end-to-end:
// stage a handful of post events with each embed shape, run exportParquet
// for kind="post_embed", and verify the projections come out correctly via
// read_parquet.
func TestPostEmbedExport(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	stagingPath := filepath.Join(dir, "staging.db")
	s, err := openStaging(stagingPath, testJetstreamCfg(), testStagingLogger())
	if err != nil {
		t.Fatalf("openStaging: %v", err)
	}
	defer s.Close()

	const day = "2026-04-24"
	type ev struct {
		rkey string
		json string
	}
	events := []ev{
		// 1: images, 2 alt'd of 3 (one alt is empty string)
		{"img1", `{"text":"x","embed":{"$type":"app.bsky.embed.images","images":[{"alt":"a"},{"alt":"b"},{"alt":""}]}}`},
		// 2: video with alt
		{"vid1", `{"text":"x","embed":{"$type":"app.bsky.embed.video","alt":"v"}}`},
		// 3: video without alt
		{"vid2", `{"text":"x","embed":{"$type":"app.bsky.embed.video"}}`},
		// 4: external link
		{"ext1", `{"text":"x","embed":{"$type":"app.bsky.embed.external","external":{"uri":"https://news.example.com/path?x=1","title":"T","description":"D"}}}`},
		// 5: record (quote)
		{"rec1", `{"text":"x","embed":{"$type":"app.bsky.embed.record","record":{"uri":"at://did:plc:o/app.bsky.feed.post/q","cid":"bafyq"}}}`},
		// 6: recordWithMedia + images
		{"rwm1", `{"text":"x","embed":{"$type":"app.bsky.embed.recordWithMedia","record":{"record":{"uri":"at://did:plc:o/app.bsky.feed.post/q2","cid":"bafyq"}},"media":{"$type":"app.bsky.embed.images","images":[{"alt":"a"}]}}}`},
		// 7: recordWithMedia + external
		{"rwm2", `{"text":"x","embed":{"$type":"app.bsky.embed.recordWithMedia","record":{"record":{"uri":"at://did:plc:o/app.bsky.feed.post/q3","cid":"bafyq"}},"media":{"$type":"app.bsky.embed.external","external":{"uri":"https://example.com/y","title":"Y"}}}}`},
		// 8: post with no embed at all — must be filtered out by the WHERE clause
		{"noembed", `{"text":"plain text"}`},
	}

	var ts int64 = 1_714_000_000_000_000
	for i, e := range events {
		if _, err := s.insert(ctx, &stagingEvent{
			DID:        "did:plc:a",
			Collection: "app.bsky.feed.post",
			Rkey:       e.rkey,
			Operation:  "create",
			TimeUS:     ts + int64(i),
			CID:        "bafyx",
			Day:        day,
			RecordJSON: []byte(e.json),
			Endpoint:   "wss://jetstream2.us-east.bsky.network/subscribe",
		}); err != nil {
			t.Fatalf("insert %s: %v", e.rkey, err)
		}
	}

	// Sanity: pre-count via the new helper.
	n, err := s.postEmbedRowCount(ctx, day)
	if err != nil {
		t.Fatalf("postEmbedRowCount: %v", err)
	}
	if n != int64(len(events)-1) { // -1 for the noembed row
		t.Fatalf("postEmbedRowCount = %d; want %d", n, len(events)-1)
	}

	// Drive the export.
	out := filepath.Join(dir, "post_embeds.parquet")
	spec := parquetExportSpec{
		table:   "staging_events_post",
		parquet: "post_embeds.parquet",
		kind:    "post_embed",
	}
	if err := exportParquet(ctx, stagingPath, day, spec, out); err != nil {
		t.Fatalf("exportParquet: %v", err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	// Total rows: should equal the embed-bearing input count.
	var got int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM read_parquet('`+out+`')`,
	).Scan(&got); err != nil {
		t.Fatalf("count: %v", err)
	}
	if got != len(events)-1 {
		t.Fatalf("rows in parquet = %d; want %d", got, len(events)-1)
	}

	// Check a few specific projections.
	type row struct {
		kind        sql.NullString
		extDomain   sql.NullString
		imgCount    sql.NullInt16
		imgWithAlt  sql.NullInt16
		videoHasAlt sql.NullBool
	}
	scan := func(rkey string) row {
		var r row
		if err := db.QueryRowContext(ctx, `
			SELECT kind, external_domain, image_count, image_with_alt, video_has_alt
			FROM read_parquet('`+out+`') WHERE rkey = ?`, rkey,
		).Scan(&r.kind, &r.extDomain, &r.imgCount, &r.imgWithAlt, &r.videoHasAlt); err != nil {
			t.Fatalf("scan %s: %v", rkey, err)
		}
		return r
	}

	if r := scan("img1"); r.kind.String != "images" || r.imgCount.Int16 != 3 || r.imgWithAlt.Int16 != 2 {
		t.Fatalf("img1 row mismatch: %+v", r)
	}
	if r := scan("vid1"); r.kind.String != "video" || !r.videoHasAlt.Valid || !r.videoHasAlt.Bool {
		t.Fatalf("vid1 row mismatch: %+v", r)
	}
	if r := scan("vid2"); r.kind.String != "video" || !r.videoHasAlt.Valid || r.videoHasAlt.Bool {
		t.Fatalf("vid2 row mismatch: %+v", r)
	}
	if r := scan("ext1"); r.kind.String != "external" || r.extDomain.String != "news.example.com" {
		t.Fatalf("ext1 row mismatch: %+v", r)
	}
	if r := scan("rec1"); r.kind.String != "record" {
		t.Fatalf("rec1 kind = %q; want record", r.kind.String)
	}
	if r := scan("rwm1"); r.kind.String != "recordWithMedia:images" || r.imgCount.Int16 != 1 || r.imgWithAlt.Int16 != 1 {
		t.Fatalf("rwm1 row mismatch: %+v", r)
	}
	if r := scan("rwm2"); r.kind.String != "recordWithMedia:external" || r.extDomain.String != "example.com" {
		t.Fatalf("rwm2 row mismatch: %+v", r)
	}
}
