package labels

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

// TestStoreRoundTrip opens a fresh labels.db, inserts a couple of rows,
// and verifies that Upsert is idempotent on the primary key.
func TestStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	path := filepath.Join(tmp, "labels.db")

	s, err := openStore(path)
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	cid := "bafyFoo"
	exp := "2099-01-01T00:00:00Z"
	rows := []Label{
		{
			Seq: 1, Src: "did:plc:lbl", URI: "at://did:plc:a/app.bsky.feed.post/p1",
			CID: &cid, Val: "!takedown", CTS: "2026-04-24T12:00:00Z", Exp: &exp,
			Neg: false,
		},
		{
			Seq: 2, Src: "did:plc:lbl", URI: "at://did:plc:a/app.bsky.feed.post/p2",
			Val: "porn", CTS: "2026-04-24T12:01:00Z",
		},
		// Same primary key as row 1 — should be ignored.
		{
			Seq: 99, Src: "did:plc:lbl", URI: "at://did:plc:a/app.bsky.feed.post/p1",
			Val: "!takedown", CTS: "2026-04-24T12:00:00Z", Neg: true,
		},
	}
	inserts := 0
	for _, r := range rows {
		ok, err := s.Upsert(ctx, r)
		if err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		if ok {
			inserts++
		}
	}
	if inserts != 2 {
		t.Fatalf("expected 2 fresh inserts, got %d", inserts)
	}

	if err := s.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	// Open a second read-only handle to confirm persistence + pragma
	// compatibility for readers.
	ro, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer ro.Close()

	var n int
	if err := ro.QueryRow(`SELECT count(*) FROM labels`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Fatalf("labels count: want 2, got %d", n)
	}

	// Verify nullable fields stored correctly.
	var scannedCID, scannedExp sql.NullString
	if err := ro.QueryRow(`
		SELECT cid, exp FROM labels
		 WHERE src = 'did:plc:lbl' AND uri = 'at://did:plc:a/app.bsky.feed.post/p1'
	`).Scan(&scannedCID, &scannedExp); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !scannedCID.Valid || scannedCID.String != cid {
		t.Fatalf("cid: want %q, got %v", cid, scannedCID)
	}
	if !scannedExp.Valid || scannedExp.String != exp {
		t.Fatalf("exp: want %q, got %v", exp, scannedExp)
	}

	// The second post row should have cid=NULL, exp=NULL.
	var c2, e2 sql.NullString
	if err := ro.QueryRow(`SELECT cid, exp FROM labels WHERE uri = 'at://did:plc:a/app.bsky.feed.post/p2'`).Scan(&c2, &e2); err != nil {
		t.Fatalf("scan p2: %v", err)
	}
	if c2.Valid || e2.Valid {
		t.Fatalf("p2: expected null cid+exp, got cid=%v exp=%v", c2, e2)
	}
}

// TestCursorRoundTrip verifies the atomic cursor tempfile write.
func TestCursorRoundTrip(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "labels_cursor.json")

	// Missing file → zero cursor.
	c, err := loadCursor(path)
	if err != nil {
		t.Fatalf("loadCursor missing: %v", err)
	}
	if c.Seq != 0 || c.SchemaVersion != "v1" {
		t.Fatalf("fresh cursor: %+v", c)
	}

	c.Seq = 42
	c.Endpoint = "wss://test.example"
	if err := saveCursorAtomic(path, c); err != nil {
		t.Fatalf("saveCursorAtomic: %v", err)
	}

	c2, err := loadCursor(path)
	if err != nil {
		t.Fatalf("loadCursor: %v", err)
	}
	if c2.Seq != 42 || c2.Endpoint != "wss://test.example" {
		t.Fatalf("round trip: %+v", c2)
	}

	// Confirm no tempfile leaked.
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("tempfile leaked: err=%v", err)
	}
}
