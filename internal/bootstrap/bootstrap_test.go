package bootstrap

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/constellation"
	"github.com/pboueri/atproto-db-snapshot/internal/model"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
	"github.com/pboueri/atproto-db-snapshot/internal/plc"
	"github.com/pboueri/atproto-db-snapshot/internal/slingshot"
)

func TestBootstrapEndToEndWithFakes(t *testing.T) {
	dataDir := t.TempDir()
	objDir := t.TempDir()
	obj, err := objstore.NewLocal(objDir)
	if err != nil {
		t.Fatal(err)
	}

	plcFake := plc.NewFake(time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC), []string{"did:plc:a", "did:plc:b"})

	// Profiles via Slingshot.
	sling := slingshot.NewFake()
	sling.Set("did:plc:a", string(model.CollectionProfile), "self", slingshot.Record{
		URI: "at://did:plc:a/app.bsky.actor.profile/self",
		CID: "bafy",
		Value: json.RawMessage(`{"displayName":"Alice","createdAt":"2025-01-01T00:00:00Z"}`),
	})
	sling.Set("did:plc:b", string(model.CollectionProfile), "self", slingshot.Record{
		URI: "at://did:plc:b/app.bsky.actor.profile/self",
		CID: "bafy",
		Value: json.RawMessage(`{"displayName":"Bob"}`),
	})

	// Follows via Constellation, target-indexed: alice and a third party
	// follow bob, so bob's bundle gets two incoming follows. No one follows
	// alice, so alice's incoming bundle is empty.
	con := constellation.NewFake()
	con.Set("did:plc:b", string(model.CollectionFollow), ".subject", []constellation.Link{
		{DID: "did:plc:a", Collection: string(model.CollectionFollow), RKey: "3l6oveex3ii2l"},
		{DID: "did:plc:c", Collection: string(model.CollectionFollow), RKey: "3l6oveex3ii2l"},
	})

	cfg := config.Config{
		DataDir:         dataDir,
		ObjectStore:     "local",
		ObjectStoreRoot: objDir,
		LookbackDays:    30,
		Concurrency:     2,
		LogLevel:        "info",
		StatsInterval:   time.Hour,
		PLCEndpoint:     "fake",
	}
	deps := Deps{
		PLC:           plcFake,
		Slingshot:     sling,
		Constellation: con,
		ObjStore:      obj,
		Now:           func() time.Time { return time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC) },
	}

	if err := RunWith(context.Background(), cfg, deps); err != nil {
		t.Fatalf("RunWith: %v", err)
	}

	if _, err := obj.Stat(context.Background(), "bootstrap/2026-04-26/social_graph.duckdb"); err != nil {
		t.Fatalf("expected uploaded duckdb: %v", err)
	}

	dbPath := filepath.Join(dataDir, "bootstrap-staging", "social_graph.duckdb")
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var actors, follows, progress int64
	if err := db.QueryRow("SELECT count(*) FROM actors").Scan(&actors); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT count(*) FROM follows").Scan(&follows); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT count(*) FROM bootstrap_progress").Scan(&progress); err != nil {
		t.Fatal(err)
	}
	// 2 actors (a, b). 2 follows (a→b and c→b, both captured when b was the
	// target). 2 progress rows (one per DID iterated from PLC).
	if actors != 2 {
		t.Errorf("actors = %d, want 2", actors)
	}
	if follows != 2 {
		t.Errorf("follows = %d, want 2", follows)
	}
	if progress != 2 {
		t.Errorf("progress = %d, want 2", progress)
	}

	// Spot-check direction: there should be a row with src=a, dst=b.
	var srcADstB int64
	if err := db.QueryRow(`SELECT count(*) FROM follows WHERE src_did = 'did:plc:a' AND dst_did = 'did:plc:b'`).Scan(&srcADstB); err != nil {
		t.Fatal(err)
	}
	if srcADstB != 1 {
		t.Errorf("a→b follows = %d, want 1", srcADstB)
	}
}

func TestBootstrapResumeSkipsCompleted(t *testing.T) {
	dataDir := t.TempDir()
	objDir := t.TempDir()
	obj, err := objstore.NewLocal(objDir)
	if err != nil {
		t.Fatal(err)
	}

	sling := slingshot.NewFake()
	sling.Set("did:plc:a", string(model.CollectionProfile), "self", slingshot.Record{
		URI: "u", CID: "c",
		Value: json.RawMessage(`{"displayName":"Alice"}`),
	})
	sling.Set("did:plc:b", string(model.CollectionProfile), "self", slingshot.Record{
		URI: "u", CID: "c",
		Value: json.RawMessage(`{"displayName":"Bob"}`),
	})
	con := constellation.NewFake()

	cfg := config.Config{
		DataDir:         dataDir,
		ObjectStore:     "local",
		ObjectStoreRoot: objDir,
		LookbackDays:    30,
		Concurrency:     2,
		LogLevel:        "info",
		StatsInterval:   time.Hour,
	}

	// First pass: only DID a is in PLC.
	deps1 := Deps{
		PLC:           plc.NewFake(time.Now(), []string{"did:plc:a"}),
		Slingshot:     sling,
		Constellation: con,
		ObjStore:      obj,
		Now:           func() time.Time { return time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC) },
	}
	if err := RunWith(context.Background(), cfg, deps1); err != nil {
		t.Fatalf("first run: %v", err)
	}

	// Second pass: PLC has a + b. Delete the remote so RunWith doesn't
	// refuse-overwrite, then re-run. FailOnce on did:plc:a as a tripwire:
	// if the resume path re-fetched it, the test fails.
	if err := obj.Delete(context.Background(), "bootstrap/2026-04-26/social_graph.duckdb"); err != nil {
		t.Fatal(err)
	}
	sling.FailOnce["did:plc:a"] = true
	deps2 := Deps{
		PLC:           plc.NewFake(time.Now(), []string{"did:plc:a", "did:plc:b"}),
		Slingshot:     sling,
		Constellation: con,
		ObjStore:      obj,
		Now:           func() time.Time { return time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC) },
	}
	if err := RunWith(context.Background(), cfg, deps2); err != nil {
		t.Fatalf("second run: %v", err)
	}
	if !sling.FailOnce["did:plc:a"] {
		t.Errorf("resume re-fetched did:plc:a (FailOnce was consumed)")
	}
}

func TestBootstrapRefusesToOverwrite(t *testing.T) {
	dataDir := t.TempDir()
	objDir := t.TempDir()
	obj, err := objstore.NewLocal(objDir)
	if err != nil {
		t.Fatal(err)
	}

	cfg := config.Config{
		DataDir:         dataDir,
		ObjectStore:     "local",
		ObjectStoreRoot: objDir,
		LookbackDays:    30,
		Concurrency:     1,
		LogLevel:        "info",
		StatsInterval:   time.Hour,
	}
	now := time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)
	deps := Deps{
		PLC:           plc.NewFake(now, []string{"did:plc:a"}),
		Slingshot:     slingshot.NewFake(),
		Constellation: constellation.NewFake(),
		ObjStore:      obj,
		Now:           func() time.Time { return now },
	}

	if err := RunWith(context.Background(), cfg, deps); err != nil {
		t.Fatalf("first run: %v", err)
	}
	err = RunWith(context.Background(), cfg, deps)
	if err == nil {
		t.Errorf("expected overwrite-refusal error")
	}
}
