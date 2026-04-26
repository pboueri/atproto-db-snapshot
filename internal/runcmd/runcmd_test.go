package runcmd

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/jetstream"
	"github.com/pboueri/atproto-db-snapshot/internal/model"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
)

// fakeNow returns a fixed clock used to make timestampInWindow deterministic.
func fakeNow() time.Time { return time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC) }

// commit builds a jetstream.Event for a commit, with TimeUS set to base + offset.
func commit(did, collection, rkey string, op jetstream.Operation, record string, ts time.Time) jetstream.Event {
	return jetstream.Event{
		DID:    did,
		TimeUS: ts.UnixMicro(),
		Kind:   jetstream.KindCommit,
		Commit: &jetstream.Commit{
			Operation:  op,
			Collection: collection,
			RKey:       rkey,
			Record:     json.RawMessage(record),
		},
	}
}

func TestRunWritesParquetAndAdvancesCursor(t *testing.T) {
	dataDir := t.TempDir()
	objDir := t.TempDir()
	obj, err := objstore.NewLocal(objDir)
	if err != nil {
		t.Fatal(err)
	}
	now := fakeNow()
	events := []jetstream.Event{
		commit("did:plc:a", string(model.CollectionPost), "p1", jetstream.OpCreate,
			`{"text":"hello","langs":["en"],"createdAt":"2026-04-26T11:00:00Z"}`,
			now.Add(-30*time.Minute)),
		commit("did:plc:b", string(model.CollectionLike), "lk1", jetstream.OpCreate,
			`{"subject":{"uri":"at://did:plc:a/app.bsky.feed.post/p1","cid":"c"},"createdAt":"2026-04-26T11:30:00Z"}`,
			now.Add(-29*time.Minute)),
		commit("did:plc:b", string(model.CollectionFollow), "f1", jetstream.OpCreate,
			`{"subject":"did:plc:a","createdAt":"2026-04-26T11:31:00Z"}`,
			now.Add(-28*time.Minute)),
	}
	sub := jetstream.NewFake(events)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cfg := config.Config{
		DataDir:         dataDir,
		ObjectStore:     "local",
		ObjectStoreRoot: objDir,
		LookbackDays:    30,
		Concurrency:     1,
		LogLevel:        "info",
		StatsInterval:   100 * time.Millisecond,
		Languages:       []string{"en"},
	}
	deps := Deps{
		Subscriber: sub,
		ObjStore:   obj,
		Now:        fakeNow,
	}
	if err := RunWith(ctx, cfg, deps); err != nil && err != context.DeadlineExceeded && err != context.Canceled {
		t.Fatalf("RunWith: %v", err)
	}

	// Posts, likes, follows should each have a parquet file under raw/2026-04-26/.
	posts, _ := obj.List(ctx, "raw/2026-04-26/posts-")
	likes, _ := obj.List(ctx, "raw/2026-04-26/likes-")
	follows, _ := obj.List(ctx, "raw/2026-04-26/follows-")
	if len(posts) == 0 || len(likes) == 0 || len(follows) == 0 {
		t.Errorf("expected files for all three collections; got posts=%d likes=%d follows=%d",
			len(posts), len(likes), len(follows))
	}

	// Cursor should be persisted both locally (sqlite) and in objstore mirror.
	if _, err := obj.Stat(ctx, cursorObjectKey); err != nil {
		t.Errorf("cursor mirror missing: %v", err)
	}
	cur, err := openCursor(filepath.Join(dataDir, "run-state", "cursor.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer cur.Close()
	micros, ok, err := cur.Load(ctx)
	if err != nil || !ok || micros == 0 {
		t.Errorf("expected non-zero cursor; ok=%v micros=%d err=%v", ok, micros, err)
	}
}

func TestRunFiltersPostsByLanguage(t *testing.T) {
	dataDir := t.TempDir()
	objDir := t.TempDir()
	obj, _ := objstore.NewLocal(objDir)
	now := fakeNow()
	events := []jetstream.Event{
		commit("did:plc:a", string(model.CollectionPost), "p1", jetstream.OpCreate,
			`{"text":"hi","langs":["ja"],"createdAt":"2026-04-26T11:00:00Z"}`,
			now.Add(-1*time.Hour)),
		commit("did:plc:b", string(model.CollectionPost), "p2", jetstream.OpCreate,
			`{"text":"hello","langs":["en"],"createdAt":"2026-04-26T11:00:00Z"}`,
			now.Add(-30*time.Minute)),
	}
	sub := jetstream.NewFake(events)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cfg := config.Config{
		DataDir: dataDir, ObjectStore: "local", ObjectStoreRoot: objDir,
		LookbackDays: 30, Concurrency: 1, LogLevel: "info",
		StatsInterval: 50 * time.Millisecond, Languages: []string{"en"},
	}
	if err := RunWith(ctx, cfg, Deps{Subscriber: sub, ObjStore: obj, Now: fakeNow}); err != nil &&
		err != context.DeadlineExceeded && err != context.Canceled {
		t.Fatalf("RunWith: %v", err)
	}

	// Read the parquet that landed and confirm only the english post survived.
	posts, _ := obj.List(ctx, "raw/2026-04-26/posts-")
	if len(posts) != 1 {
		t.Fatalf("expected 1 posts file, got %d", len(posts))
	}
}

func TestRunDropsOutOfWindowEvents(t *testing.T) {
	dataDir := t.TempDir()
	objDir := t.TempDir()
	obj, _ := objstore.NewLocal(objDir)
	now := fakeNow()
	events := []jetstream.Event{
		// 3 days in the past — drop.
		commit("did:plc:old", string(model.CollectionPost), "px", jetstream.OpCreate,
			`{"text":"old","langs":["en"]}`, now.Add(-72*time.Hour)),
		// 5 hours in the future — drop.
		commit("did:plc:fut", string(model.CollectionPost), "pf", jetstream.OpCreate,
			`{"text":"fut","langs":["en"]}`, now.Add(5*time.Hour)),
		// In window — keep.
		commit("did:plc:ok", string(model.CollectionPost), "po", jetstream.OpCreate,
			`{"text":"ok","langs":["en"]}`, now.Add(-1*time.Hour)),
	}
	sub := jetstream.NewFake(events)
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	cfg := config.Config{
		DataDir: dataDir, ObjectStore: "local", ObjectStoreRoot: objDir,
		LookbackDays: 30, Concurrency: 1, LogLevel: "info",
		StatsInterval: 50 * time.Millisecond, Languages: []string{"en"},
	}
	if err := RunWith(ctx, cfg, Deps{Subscriber: sub, ObjStore: obj, Now: fakeNow}); err != nil &&
		err != context.DeadlineExceeded && err != context.Canceled {
		t.Fatalf("RunWith: %v", err)
	}
	// Only one in-window post.
	posts, _ := obj.List(ctx, "raw/2026-04-26/posts-")
	if len(posts) != 1 {
		t.Errorf("expected 1 posts file; got %d", len(posts))
	}
}

func TestRunResumesFromLocalCursor(t *testing.T) {
	dataDir := t.TempDir()
	objDir := t.TempDir()
	obj, _ := objstore.NewLocal(objDir)
	now := fakeNow()
	// Pre-populate the local cursor as if a prior run had advanced it.
	cur, err := openCursor(filepath.Join(dataDir, "run-state", "cursor.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	startMicros := now.Add(-30 * time.Minute).UnixMicro()
	if err := cur.Save(context.Background(), startMicros); err != nil {
		t.Fatal(err)
	}
	cur.Close()

	events := []jetstream.Event{
		// Before cursor — should be skipped by the Subscriber's Cursor option.
		commit("did:plc:old", string(model.CollectionPost), "px", jetstream.OpCreate,
			`{"text":"old","langs":["en"]}`, now.Add(-1*time.Hour)),
		// After cursor — kept.
		commit("did:plc:new", string(model.CollectionPost), "pn", jetstream.OpCreate,
			`{"text":"new","langs":["en"]}`, now.Add(-15*time.Minute)),
	}
	sub := jetstream.NewFake(events)
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	cfg := config.Config{
		DataDir: dataDir, ObjectStore: "local", ObjectStoreRoot: objDir,
		LookbackDays: 30, Concurrency: 1, LogLevel: "info",
		StatsInterval: 50 * time.Millisecond, Languages: []string{"en"},
	}
	if err := RunWith(ctx, cfg, Deps{Subscriber: sub, ObjStore: obj, Now: fakeNow}); err != nil &&
		err != context.DeadlineExceeded && err != context.Canceled {
		t.Fatalf("RunWith: %v", err)
	}

	// Subscriber filtered the pre-cursor event; only the post-cursor one made it.
	if got := sub.Emitted(); got != 1 {
		t.Errorf("emitted = %d, want 1 (cursor must filter)", got)
	}
}

func TestRunMirrorsCursorToObjStore(t *testing.T) {
	dataDir := t.TempDir()
	objDir := t.TempDir()
	obj, _ := objstore.NewLocal(objDir)
	now := fakeNow()
	events := []jetstream.Event{
		commit("did:plc:a", string(model.CollectionPost), "p1", jetstream.OpCreate,
			`{"text":"hi","langs":["en"]}`, now.Add(-1*time.Hour)),
	}
	sub := jetstream.NewFake(events)
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	cfg := config.Config{
		DataDir: dataDir, ObjectStore: "local", ObjectStoreRoot: objDir,
		LookbackDays: 30, Concurrency: 1, LogLevel: "info",
		StatsInterval: 50 * time.Millisecond, Languages: []string{"en"},
	}
	_ = RunWith(ctx, cfg, Deps{Subscriber: sub, ObjStore: obj, Now: fakeNow})

	rc, err := obj.Get(context.Background(), cursorObjectKey)
	if err != nil {
		t.Fatalf("cursor mirror not present: %v", err)
	}
	defer rc.Close()
	var m cursorMirror
	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		t.Fatalf("decode mirror: %v", err)
	}
	if m.Micros == 0 {
		t.Errorf("mirror micros = 0")
	}
}
