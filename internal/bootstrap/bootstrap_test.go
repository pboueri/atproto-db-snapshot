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
	"github.com/pboueri/atproto-db-snapshot/internal/repo"
	"github.com/pboueri/atproto-db-snapshot/internal/model"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
	"github.com/pboueri/atproto-db-snapshot/internal/plc"
)

func TestBootstrapEndToEndWithFakes(t *testing.T) {
	dataDir := t.TempDir()
	objDir := t.TempDir()
	obj, err := objstore.NewLocal(objDir)
	if err != nil {
		t.Fatal(err)
	}

	plcFake := plc.NewFake(time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC), []string{"did:plc:a", "did:plc:b"})
	con := repo.NewFake()

	// Profile + 2 follows for did:plc:a; just a profile for did:plc:b.
	con.Set("did:plc:a", model.CollectionProfile, []repo.Record{
		{URI: "at://did:plc:a/app.bsky.actor.profile/self", Value: json.RawMessage(`{"displayName":"Alice","createdAt":"2025-01-01T00:00:00Z"}`)},
	})
	con.Set("did:plc:a", model.CollectionFollow, []repo.Record{
		{URI: "at://did:plc:a/app.bsky.graph.follow/r1", Value: json.RawMessage(`{"subject":"did:plc:b","createdAt":"2025-01-02T00:00:00Z"}`)},
		{URI: "at://did:plc:a/app.bsky.graph.follow/r2", Value: json.RawMessage(`{"subject":"did:plc:c","createdAt":"2025-01-03T00:00:00Z"}`)},
	})
	con.Set("did:plc:a", model.CollectionBlock, nil)
	con.Set("did:plc:b", model.CollectionProfile, []repo.Record{
		{URI: "at://did:plc:b/app.bsky.actor.profile/self", Value: json.RawMessage(`{"displayName":"Bob"}`)},
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
		PLC:      plcFake,
		Repo:     con,
		ObjStore: obj,
		Now:      func() time.Time { return time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC) },
	}

	if err := RunWith(context.Background(), cfg, deps); err != nil {
		t.Fatalf("RunWith: %v", err)
	}

	// The remote duckdb should exist at bootstrap/2026-04-26/social_graph.duckdb.
	if _, err := obj.Stat(context.Background(), "bootstrap/2026-04-26/social_graph.duckdb"); err != nil {
		t.Fatalf("expected uploaded duckdb: %v", err)
	}

	// Verify table contents via the local staging copy.
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
	if actors != 2 {
		t.Errorf("actors = %d, want 2", actors)
	}
	if follows != 2 {
		t.Errorf("follows = %d, want 2", follows)
	}
	if progress != 2 {
		t.Errorf("progress = %d, want 2", progress)
	}
}

func TestBootstrapResumeSkipsCompleted(t *testing.T) {
	dataDir := t.TempDir()
	objDir := t.TempDir()
	obj, err := objstore.NewLocal(objDir)
	if err != nil {
		t.Fatal(err)
	}

	con := repo.NewFake()
	con.Set("did:plc:a", model.CollectionProfile, []repo.Record{
		{URI: "at://did:plc:a/app.bsky.actor.profile/self", Value: json.RawMessage(`{"displayName":"Alice"}`)},
	})
	con.Set("did:plc:b", model.CollectionProfile, []repo.Record{
		{URI: "at://did:plc:b/app.bsky.actor.profile/self", Value: json.RawMessage(`{"displayName":"Bob"}`)},
	})

	cfg := config.Config{
		DataDir:         dataDir,
		ObjectStore:     "local",
		ObjectStoreRoot: objDir,
		LookbackDays:    30,
		Concurrency:     2,
		LogLevel:        "info",
		StatsInterval:   time.Hour,
	}

	// First pass: only DID a is in PLC, succeeds.
	deps1 := Deps{
		PLC:      plc.NewFake(time.Now(), []string{"did:plc:a"}),
		Repo:     con,
		ObjStore: obj,
		Now:      func() time.Time { return time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC) },
	}
	if err := RunWith(context.Background(), cfg, deps1); err != nil {
		t.Fatalf("first run: %v", err)
	}

	// Second pass: PLC now has a + b. Bootstrap should refuse to overwrite
	// the already-uploaded file but still process b locally — to verify the
	// resume path we re-run before the upload step by deleting the remote
	// file. (The real-world resume scenario is interrupting *before* upload.)
	if err := obj.Delete(context.Background(), "bootstrap/2026-04-26/social_graph.duckdb"); err != nil {
		t.Fatal(err)
	}
	// FailOnce on did:plc:a so we'd notice if the resume path re-fetched it.
	con.FailOnce["did:plc:a"] = true
	deps2 := Deps{
		PLC:      plc.NewFake(time.Now(), []string{"did:plc:a", "did:plc:b"}),
		Repo:     con,
		ObjStore: obj,
		Now:      func() time.Time { return time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC) },
	}
	if err := RunWith(context.Background(), cfg, deps2); err != nil {
		t.Fatalf("second run: %v", err)
	}
	// FailOnce stays true until the fake consumes it — i.e. until someone
	// actually calls ListRecords for the DID. If the resume path skipped
	// did:plc:a (the desired behavior), the entry should still be set.
	if !con.FailOnce["did:plc:a"] {
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
		PLC:      plc.NewFake(now, []string{"did:plc:a"}),
		Repo:     repo.NewFake(),
		ObjStore: obj,
		Now:      func() time.Time { return now },
	}

	// First run uploads.
	if err := RunWith(context.Background(), cfg, deps); err != nil {
		t.Fatalf("first run: %v", err)
	}
	// Second run with the staging file already present and the remote in place
	// must refuse so we don't clobber the canonical baseline.
	err = RunWith(context.Background(), cfg, deps)
	if err == nil {
		t.Errorf("expected overwrite-refusal error")
	}
}
