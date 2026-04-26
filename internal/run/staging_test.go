package run

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
)

func testStagingLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testJetstreamCfg() config.JetstreamConfig {
	cfg := config.DefaultJetstream()
	cfg.CheckpointInterval = 5 * time.Second
	return cfg
}

// TestStagingInsertPersists inserts a simple event and verifies rowCount.
func TestStagingInsertPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "staging.db")
	s, err := openStaging(path, testJetstreamCfg(), testStagingLogger())
	if err != nil {
		t.Fatalf("openStaging: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	day := "2026-04-24"
	ev := &stagingEvent{
		DID:        "did:plc:a",
		Collection: "app.bsky.feed.post",
		Rkey:       "rk1",
		Operation:  "create",
		TimeUS:     1_714_000_000_000_000,
		CID:        "bafyx",
		Day:        day,
		RecordJSON: []byte(`{"text":"hi"}`),
		Endpoint:   "wss://jetstream2.us-east.bsky.network/subscribe",
	}
	ok, err := s.insert(ctx, ev)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if !ok {
		t.Fatalf("first insert should return ok=true")
	}
	n, err := s.rowCount(ctx, "staging_events_post", day)
	if err != nil {
		t.Fatalf("rowCount: %v", err)
	}
	if n != 1 {
		t.Fatalf("rowCount = %d; want 1", n)
	}
}

// TestStagingInsertDedup inserts the same event twice; the second must be a no-op.
func TestStagingInsertDedup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "staging.db")
	s, err := openStaging(path, testJetstreamCfg(), testStagingLogger())
	if err != nil {
		t.Fatalf("openStaging: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	day := "2026-04-24"
	ev := &stagingEvent{
		DID:        "did:plc:a",
		Collection: "app.bsky.feed.like",
		Rkey:       "lk1",
		Operation:  "create",
		TimeUS:     1_714_000_100_000_000,
		Day:        day,
		RecordJSON: []byte(`{"subject":{"uri":"at://did:plc:b/app.bsky.feed.post/xyz","cid":"bafybz"}}`),
		Endpoint:   "wss://e",
	}
	// First insert: new.
	ok1, err := s.insert(ctx, ev)
	if err != nil {
		t.Fatalf("insert #1: %v", err)
	}
	if !ok1 {
		t.Fatalf("first insert should return true")
	}
	// Second insert with identical PK: ignored.
	ok2, err := s.insert(ctx, ev)
	if err != nil {
		t.Fatalf("insert #2: %v", err)
	}
	if ok2 {
		t.Fatalf("duplicate insert should return false")
	}
	n, err := s.rowCount(ctx, "staging_events_like", day)
	if err != nil {
		t.Fatalf("rowCount: %v", err)
	}
	if n != 1 {
		t.Fatalf("rowCount after dup = %d; want 1", n)
	}
}

// TestStagingDeleteRouting verifies deletes go to staging_events_delete.
func TestStagingDeleteRouting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "staging.db")
	s, err := openStaging(path, testJetstreamCfg(), testStagingLogger())
	if err != nil {
		t.Fatalf("openStaging: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	day := "2026-04-24"
	if _, err := s.insert(ctx, &stagingEvent{
		DID:        "did:plc:a",
		Collection: "app.bsky.feed.post",
		Rkey:       "rk1",
		Operation:  "delete",
		TimeUS:     1_714_000_200_000_000,
		Day:        day,
		Endpoint:   "wss://e",
	}); err != nil {
		t.Fatalf("insert delete: %v", err)
	}
	if n, _ := s.rowCount(ctx, "staging_events_delete", day); n != 1 {
		t.Fatalf("delete row count = %d; want 1", n)
	}
	if n, _ := s.rowCount(ctx, "staging_events_post", day); n != 0 {
		t.Fatalf("post row count = %d; want 0", n)
	}
}

// TestCursorAtomicRoundTrip writes a cursor file and reads it back.
func TestCursorAtomicRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cursor.json")

	// Missing file → zero cursor, schema version v1.
	c, err := loadCursor(path)
	if err != nil {
		t.Fatalf("loadCursor missing: %v", err)
	}
	if c.Cursor != 0 || c.SchemaVersion != "v1" {
		t.Fatalf("empty cursor = %+v; want {Cursor:0, SchemaVersion:v1}", c)
	}

	want := cursorState{
		Cursor:   1_714_000_300_000_000,
		Endpoint: "wss://jetstream2.us-east.bsky.network/subscribe",
	}
	if err := saveCursorAtomic(path, want); err != nil {
		t.Fatalf("saveCursorAtomic: %v", err)
	}
	got, err := loadCursor(path)
	if err != nil {
		t.Fatalf("loadCursor after save: %v", err)
	}
	if got.Cursor != want.Cursor {
		t.Fatalf("cursor: got %d, want %d", got.Cursor, want.Cursor)
	}
	if got.Endpoint != want.Endpoint {
		t.Fatalf("endpoint: got %q, want %q", got.Endpoint, want.Endpoint)
	}
	if got.SchemaVersion != "v1" {
		t.Fatalf("schema_version: got %q, want v1", got.SchemaVersion)
	}
	if got.UpdatedAt.IsZero() {
		t.Fatalf("updated_at is zero; saveCursorAtomic should stamp it")
	}
}

// TestStagingCursorBoundsAndDays seeds events across two UTC days and
// verifies cursorBoundsForDay + daysWithEvents.
func TestStagingCursorBoundsAndDays(t *testing.T) {
	path := filepath.Join(t.TempDir(), "staging.db")
	s, err := openStaging(path, testJetstreamCfg(), testStagingLogger())
	if err != nil {
		t.Fatalf("openStaging: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	events := []stagingEvent{
		{DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r1", Operation: "create",
			TimeUS: 1_700_000_000_000_000, Day: "2026-04-23"},
		{DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r2", Operation: "create",
			TimeUS: 1_700_000_500_000_000, Day: "2026-04-23"},
		{DID: "did:plc:b", Collection: "app.bsky.feed.like", Rkey: "l1", Operation: "create",
			TimeUS: 1_700_001_000_000_000, Day: "2026-04-24"},
	}
	for i := range events {
		ev := events[i]
		ev.Endpoint = "wss://e"
		if _, err := s.insert(ctx, &ev); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	lo, hi, err := s.cursorBoundsForDay(ctx, "2026-04-23")
	if err != nil {
		t.Fatalf("bounds 23: %v", err)
	}
	if lo != 1_700_000_000_000_000 || hi != 1_700_000_500_000_000 {
		t.Fatalf("bounds 23 = (%d, %d); want (1700000000000000, 1700000500000000)", lo, hi)
	}
	lo, hi, err = s.cursorBoundsForDay(ctx, "2026-04-24")
	if err != nil {
		t.Fatalf("bounds 24: %v", err)
	}
	if lo != 1_700_001_000_000_000 || hi != 1_700_001_000_000_000 {
		t.Fatalf("bounds 24 = (%d, %d)", lo, hi)
	}

	days, err := s.daysWithEvents(ctx)
	if err != nil {
		t.Fatalf("daysWithEvents: %v", err)
	}
	want := map[string]bool{"2026-04-23": true, "2026-04-24": true}
	if len(days) != 2 {
		t.Fatalf("daysWithEvents = %v; want 2 entries", days)
	}
	for _, d := range days {
		if !want[d] {
			t.Fatalf("unexpected day %q in %v", d, days)
		}
	}
}
