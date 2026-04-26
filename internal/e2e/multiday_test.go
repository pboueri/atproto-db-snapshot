package e2e

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"

	"github.com/pboueri/atproto-db-snapshot/internal/build"
	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
	"github.com/pboueri/atproto-db-snapshot/internal/run"
)

// TestPipelineMultiDayReplay exercises spec §14.1's deletion-then-create
// ordering across two adjacent UTC days. The bug we're guarding against:
// a day-2 deletion of a row created on day 1 must NOT be resurrected by
// the day-1 replay running before the day-2 replay (which would happen
// if either day's deletes were applied AFTER the other day's creates).
//
// Pipeline:
//
//	fake jetstream → run.Run → staging.db → SealYesterdayForTest(day1, day2)
//	→ build.Run(ModeIncremental, Date=day2) → query current_all.duckdb
func TestPipelineMultiDayReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-day e2e pipeline test in -short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	events, plan := buildFixtureMultiDay()

	// Stand up the fake jetstream + temp data dir + run.Run.
	fake := newFakeJetstream(events)
	t.Cleanup(fake.Close)

	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir data: %v", err)
	}

	cfg := config.Default()
	cfg.DataDir = dataDir
	cfg.Jetstream.Endpoints = []string{fake.URL()}
	cfg.Jetstream.Compress = false
	cfg.Jetstream.CheckpointEveryRows = 5
	cfg.Jetstream.CheckpointInterval = 200 * time.Millisecond
	cfg.Jetstream.RolloverGrace = 1 * time.Second
	cfg.Jetstream.MinFreeBytes = 0
	cfg.Jetstream.NoUpload = true
	cfg.Jetstream.LagAlarm = 0

	logger := newDiscardLogger()

	runCtx, runCancel := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() {
		runDone <- run.Run(runCtx, cfg, nil, logger)
	}()

	// Wait until staging holds every event we emitted (creates + deletes).
	expected := int64(len(events))
	stagingPath := filepath.Join(dataDir, "staging.db")
	if err := wait(ctx, 25*time.Second, func() bool {
		n, err := countStagingRows(stagingPath)
		if err != nil {
			return false
		}
		return n >= expected
	}); err != nil {
		runCancel()
		<-runDone
		t.Fatalf("events never landed in staging: %v", err)
	}

	runCancel()
	if err := <-runDone; err != nil && err != context.Canceled {
		t.Fatalf("run returned: %v", err)
	}

	// Seal both days in chronological order. Per spec §9, the build
	// replays days in chronological order — but the seal order matters
	// here only insofar as both days must be on disk before the build.
	for _, day := range []string{plan.Day1, plan.Day2} {
		if err := run.SealYesterdayForTest(ctx, dataDir, cfg.Jetstream, nil, day, logger); err != nil {
			t.Fatalf("seal %s: %v", day, err)
		}
	}

	// Build (incremental, target date = day2) against a file-backed store.
	storeDir := filepath.Join(tmp, "store")
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		t.Fatalf("mkdir store: %v", err)
	}
	opts := build.Options{
		Mode:                 build.ModeIncremental,
		NoUpload:             false,
		Date:                 plan.Day2,
		Store:                objstore.NewFileStore(storeDir, ""),
		SkipLabelerTakedowns: true,
	}
	if err := build.Run(ctx, cfg, opts, logger); err != nil {
		t.Fatalf("build.Run: %v", err)
	}

	// ---- Assert against current_all.duckdb ----
	allDB := filepath.Join(dataDir, "current_all.duckdb")
	db, err := sql.Open("duckdb", allDB+"?access_mode=READ_ONLY")
	if err != nil {
		t.Fatalf("open current_all: %v", err)
	}
	defer db.Close()

	// posts row count: 10 (5 surviving + 5 day-2 creates).
	var posts int64
	if err := db.QueryRow("SELECT count(*) FROM posts").Scan(&posts); err != nil {
		t.Fatalf("count posts: %v", err)
	}
	if posts != int64(plan.ExpectedPosts) {
		t.Errorf("posts = %d; want %d", posts, plan.ExpectedPosts)
	}

	// Deleted rkeys must NOT be present.
	for _, p := range plan.DeletedPosts {
		var n int64
		err := db.QueryRow(`
			SELECT count(*) FROM posts p
			  JOIN actors a ON a.actor_id = p.author_id
			 WHERE a.did = ? AND p.rkey = ?`,
			p.DID, p.Rkey,
		).Scan(&n)
		if err != nil {
			t.Fatalf("query deleted post (%s,%s): %v", p.DID, p.Rkey, err)
		}
		if n != 0 {
			t.Errorf("deleted post resurrected: did=%s rkey=%s count=%d", p.DID, p.Rkey, n)
		}
	}

	// New day-2 rkeys MUST be present.
	for _, p := range plan.NewPostsDay2 {
		var n int64
		err := db.QueryRow(`
			SELECT count(*) FROM posts p
			  JOIN actors a ON a.actor_id = p.author_id
			 WHERE a.did = ? AND p.rkey = ?`,
			p.DID, p.Rkey,
		).Scan(&n)
		if err != nil {
			t.Fatalf("query new post (%s,%s): %v", p.DID, p.Rkey, err)
		}
		if n != 1 {
			t.Errorf("missing day-2 post: did=%s rkey=%s count=%d", p.DID, p.Rkey, n)
		}
	}

	// likes_current row count: 23 (15 surviving day-1 + 8 day-2).
	var likes int64
	if err := db.QueryRow("SELECT count(*) FROM likes_current").Scan(&likes); err != nil {
		t.Fatalf("count likes_current: %v", err)
	}
	if likes != int64(plan.ExpectedLikes) {
		t.Errorf("likes_current = %d; want %d", likes, plan.ExpectedLikes)
	}

	// Spot-check a surviving day-1 post's like_count vs the planner's
	// expected value. Pick one we know has a non-zero expected count.
	var probe postRef
	for _, sp := range plan.SurvivingPosts {
		if plan.ExpectedLikeCount[sp.DID+"/"+sp.Rkey] > 0 {
			probe = sp
			break
		}
	}
	if probe.DID == "" {
		t.Fatalf("planner has no surviving post with expected likes (fixture bug)")
	}
	wantLikes := plan.ExpectedLikeCount[probe.DID+"/"+probe.Rkey]
	var gotLikes int64
	err = db.QueryRow(`
		SELECT p.like_count FROM posts p
		  JOIN actors a ON a.actor_id = p.author_id
		 WHERE a.did = ? AND p.rkey = ?`,
		probe.DID, probe.Rkey,
	).Scan(&gotLikes)
	if err != nil {
		t.Fatalf("query like_count for probe (%s,%s): %v", probe.DID, probe.Rkey, err)
	}
	if gotLikes != int64(wantLikes) {
		t.Errorf("like_count for %s/%s = %d; want %d",
			probe.DID, probe.Rkey, gotLikes, wantLikes)
	}

	// _meta sanity: build_mode='incremental', built_at non-zero.
	var mode string
	var builtAt time.Time
	err = db.QueryRow(`SELECT build_mode, built_at FROM _meta ORDER BY built_at DESC LIMIT 1`).Scan(&mode, &builtAt)
	if err != nil {
		t.Fatalf("read _meta: %v", err)
	}
	if mode != "incremental" {
		t.Errorf("_meta.build_mode = %q; want %q", mode, "incremental")
	}
	if builtAt.IsZero() {
		t.Errorf("_meta.built_at is zero")
	}
}
