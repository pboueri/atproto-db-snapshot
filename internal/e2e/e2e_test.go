// Package e2e exercises the bootstrap → run → snapshot → monitor pipeline as
// a single test, with the mock source clients and the local-filesystem
// object store. It is the spec's "everything wired together" check —
// individual package tests stub aggressively, but this one runs each command
// with realistic inputs against a shared object store and asserts that the
// final snapshot can answer the analytic queries called out in the spec.
package e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"

	"github.com/pboueri/atproto-db-snapshot/internal/bootstrap"
	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/constellation"
	"github.com/pboueri/atproto-db-snapshot/internal/intern"
	"github.com/pboueri/atproto-db-snapshot/internal/jetstream"
	"github.com/pboueri/atproto-db-snapshot/internal/model"
	"github.com/pboueri/atproto-db-snapshot/internal/monitor"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
	"github.com/pboueri/atproto-db-snapshot/internal/plc"
	"github.com/pboueri/atproto-db-snapshot/internal/runcmd"
	"github.com/pboueri/atproto-db-snapshot/internal/slingshot"
	"github.com/pboueri/atproto-db-snapshot/internal/snapshot"
)

// fixedNow is the wall clock the e2e test pins. The bootstrap publishes under
// fixedNow's date; the snapshot window centers on it; the run command's
// "drop more-than-2-days-out events" check uses it.
var fixedNow = time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)

func nowFn() time.Time { return fixedNow }

// TestPipelineEndToEnd is the headline test: it runs all four commands in
// order against a shared object store, then runs the four analytic queries
// from the spec on the produced current_all.duckdb.
func TestPipelineEndToEnd(t *testing.T) {
	dataDir := t.TempDir()
	objDir := t.TempDir()
	obj, err := objstore.NewLocal(objDir)
	if err != nil {
		t.Fatal(err)
	}

	// --- bootstrap ---
	bootstrapPLCFake := plc.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		[]string{"did:plc:alice", "did:plc:bob", "did:plc:carol"})

	// Profiles via Slingshot.
	sling := slingshot.NewFake()
	sling.Set("did:plc:alice", string(model.CollectionProfile), "self", slingshot.Record{
		URI: "at://did:plc:alice/app.bsky.actor.profile/self",
		Value: json.RawMessage(`{"displayName":"Alice","createdAt":"2025-12-01T00:00:00Z"}`),
	})
	sling.Set("did:plc:bob", string(model.CollectionProfile), "self", slingshot.Record{
		URI: "at://did:plc:bob/app.bsky.actor.profile/self",
		Value: json.RawMessage(`{"displayName":"Bob"}`),
	})
	sling.Set("did:plc:carol", string(model.CollectionProfile), "self", slingshot.Record{
		URI: "at://did:plc:carol/app.bsky.actor.profile/self",
		Value: json.RawMessage(`{"displayName":"Carol"}`),
	})

	// Follows via Constellation, target-indexed: edge (alice → bob) is
	// captured when bob is the target; edge (bob → alice) when alice is
	// the target. Block (carol → alice) is on alice's incoming bundle.
	// Real Constellation responses don't include createdAt, so the rkey
	// is the only timestamp signal — we use real TIDs that decode to
	// 2024-10 to keep the schema realistic. The aliceFollowsBobRKey is
	// shared with the firehose deleteF event below to verify tombstone
	// reconciliation.
	const aliceFollowsBobRKey = "3l6oveex3ii2l"
	const bobFollowsAliceRKey = "3l6oveex3ii2m"
	const carolBlocksAliceRKey = "3l6oveex3ii2n"
	con := constellation.NewFake()
	con.Set("did:plc:bob", string(model.CollectionFollow), ".subject", []constellation.Link{
		{DID: "did:plc:alice", Collection: string(model.CollectionFollow), RKey: aliceFollowsBobRKey},
	})
	con.Set("did:plc:alice", string(model.CollectionFollow), ".subject", []constellation.Link{
		{DID: "did:plc:bob", Collection: string(model.CollectionFollow), RKey: bobFollowsAliceRKey},
	})
	con.Set("did:plc:alice", string(model.CollectionBlock), ".subject", []constellation.Link{
		{DID: "did:plc:carol", Collection: string(model.CollectionBlock), RKey: carolBlocksAliceRKey},
	})

	cfg := config.Config{
		DataDir:           dataDir,
		ObjectStore:       "local",
		ObjectStoreRoot:   objDir,
		LookbackDays:      30,
		Concurrency:       2,
		LogLevel:          "info",
		StatsInterval:     50 * time.Millisecond,
		Languages:         []string{"en"},
		DuckDBMemoryLimit: "",
		MonitorAddr:       ":0",
	}
	if err := bootstrap.RunWith(context.Background(), cfg, bootstrap.Deps{
		PLC: bootstrapPLCFake, Slingshot: sling, Constellation: con, ObjStore: obj, Now: nowFn,
	}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	// --- run: stream a synthetic firehose ---
	postP1 := commit("did:plc:alice", string(model.CollectionPost), "p1", jetstream.OpCreate,
		`{"text":"hello world","langs":["en"],"createdAt":"2026-04-26T11:00:00Z"}`,
		fixedNow.Add(-1*time.Hour))
	postP2 := commit("did:plc:bob", string(model.CollectionPost), "p2", jetstream.OpCreate,
		`{"text":"replying","langs":["en"],"reply":{"parent":{"uri":"at://did:plc:alice/app.bsky.feed.post/p1","cid":"c"},"root":{"uri":"at://did:plc:alice/app.bsky.feed.post/p1","cid":"c"}},"createdAt":"2026-04-26T11:05:00Z"}`,
		fixedNow.Add(-55*time.Minute))
	postP3 := commit("did:plc:carol", string(model.CollectionPost), "p3", jetstream.OpCreate,
		`{"text":"konnichiwa","langs":["ja"],"createdAt":"2026-04-26T11:10:00Z"}`,
		fixedNow.Add(-50*time.Minute))
	likeL1 := commit("did:plc:carol", string(model.CollectionLike), "l1", jetstream.OpCreate,
		`{"subject":{"uri":"at://did:plc:alice/app.bsky.feed.post/p1","cid":"c"},"createdAt":"2026-04-26T11:06:00Z"}`,
		fixedNow.Add(-54*time.Minute))
	repostR1 := commit("did:plc:bob", string(model.CollectionRepost), "rp1", jetstream.OpCreate,
		`{"subject":{"uri":"at://did:plc:alice/app.bsky.feed.post/p1","cid":"c"},"createdAt":"2026-04-26T11:07:00Z"}`,
		fixedNow.Add(-53*time.Minute))
	// New follow that wasn't in bootstrap.
	followF1 := commit("did:plc:carol", string(model.CollectionFollow), "f1", jetstream.OpCreate,
		`{"subject":"did:plc:bob","createdAt":"2026-04-25T10:00:00Z"}`,
		fixedNow.Add(-26*time.Hour)) // yesterday
	// Delete one bootstrap follow (alice unfollows bob). The rkey must
	// match the one set via Constellation above so the tombstone hits.
	deleteF := jetstream.Event{
		DID:    "did:plc:alice",
		TimeUS: fixedNow.Add(-2 * time.Hour).UnixMicro(),
		Kind:   jetstream.KindCommit,
		Commit: &jetstream.Commit{Operation: jetstream.OpDelete, Collection: string(model.CollectionFollow), RKey: aliceFollowsBobRKey},
	}
	events := []jetstream.Event{postP1, postP2, postP3, likeL1, repostR1, followF1, deleteF}
	sub := jetstream.NewFake(events)

	runCtx, runCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer runCancel()
	if err := runcmd.RunWith(runCtx, cfg, runcmd.Deps{
		Subscriber: sub, ObjStore: obj, Now: nowFn,
	}); err != nil && err != context.DeadlineExceeded && err != context.Canceled {
		t.Fatalf("runcmd: %v", err)
	}

	// --- snapshot ---
	if err := snapshot.RunWith(context.Background(), cfg, snapshot.Deps{
		ObjStore: obj, Now: nowFn,
	}); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// --- analytic queries on current_all.duckdb ---
	allPath := filepath.Join(objDir, "snapshot", "current_all.duckdb")
	db, err := sql.Open("duckdb", allPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. "How many people followed someone yesterday" — carol's follow of bob
	// happened on 2026-04-25 in the fixture. Expected: 1.
	var followersYesterday int64
	if err := db.QueryRow(`
        SELECT count(DISTINCT src_did_id) FROM follows
        WHERE created_at >= TIMESTAMP '2026-04-25 00:00:00'
          AND created_at <  TIMESTAMP '2026-04-26 00:00:00'
    `).Scan(&followersYesterday); err != nil {
		t.Fatalf("yesterday-follows query: %v", err)
	}
	if followersYesterday != 1 {
		t.Errorf("followers-yesterday = %d, want 1", followersYesterday)
	}

	// 2. "What % of total follows generated yesterday" — 1 of 2 surviving
	// follows (alice→bob was deleted, bob→alice + carol→bob remain). 50%.
	var pct sql.NullFloat64
	if err := db.QueryRow(`
        SELECT 100.0 * sum(CASE WHEN created_at >= TIMESTAMP '2026-04-25 00:00:00'
                                  AND created_at <  TIMESTAMP '2026-04-26 00:00:00'
                                THEN 1 ELSE 0 END) / nullif(count(*), 0)
        FROM follows
    `).Scan(&pct); err != nil {
		t.Fatalf("pct-follows-yesterday: %v", err)
	}
	if !pct.Valid || pct.Float64 < 49 || pct.Float64 > 51 {
		t.Errorf("pct-follows-yesterday = %v, want ~50", pct)
	}

	// 3. "How many people who posted got 1 like" — alice's p1 has 1 like;
	// bob and carol's posts have 0 (and carol's was filtered out by lang).
	var posters int64
	if err := db.QueryRow(`
        SELECT count(DISTINCT p.did_id)
        FROM posts p
        JOIN post_aggs a USING (uri_id)
        WHERE a.likes_in_window >= 1
    `).Scan(&posters); err != nil {
		t.Fatalf("posters-with-1-like: %v", err)
	}
	if posters != 1 {
		t.Errorf("posters-with-1-like = %d, want 1", posters)
	}

	// 4. "How many posts got at least one like" — just p1.
	var likedPosts int64
	if err := db.QueryRow(`
        SELECT count(*) FROM post_aggs WHERE likes_in_window >= 1
    `).Scan(&likedPosts); err != nil {
		t.Fatalf("liked-posts: %v", err)
	}
	if likedPosts != 1 {
		t.Errorf("liked-posts = %d, want 1", likedPosts)
	}

	// Sanity checks on graph state: the deleted follow should be gone, the
	// new follow should be present.
	var aliceFollowsBob int64
	if err := db.QueryRow(`
        SELECT count(*) FROM follows
        WHERE src_did_id = ? AND dst_did_id = ?`,
		intern.DIDID("did:plc:alice"), intern.DIDID("did:plc:bob")).Scan(&aliceFollowsBob); err != nil {
		t.Fatal(err)
	}
	if aliceFollowsBob != 0 {
		t.Errorf("alice→bob follow should be deleted; got %d rows", aliceFollowsBob)
	}
	var carolFollowsBob int64
	if err := db.QueryRow(`
        SELECT count(*) FROM follows
        WHERE src_did_id = ? AND dst_did_id = ?`,
		intern.DIDID("did:plc:carol"), intern.DIDID("did:plc:bob")).Scan(&carolFollowsBob); err != nil {
		t.Fatal(err)
	}
	if carolFollowsBob != 1 {
		t.Errorf("carol→bob follow missing; got %d rows", carolFollowsBob)
	}

	// Carol's japanese post was filtered out by the language filter.
	var carolPosts int64
	if err := db.QueryRow(`
        SELECT count(*) FROM posts WHERE did_id = ?`,
		intern.DIDID("did:plc:carol")).Scan(&carolPosts); err != nil {
		t.Fatal(err)
	}
	if carolPosts != 0 {
		t.Errorf("carol posts = %d, want 0 (filtered by lang)", carolPosts)
	}

	// --- monitor ---
	h := monitor.NewHandler(cfg, monitor.Deps{ObjStore: obj})
	srv := httptest.NewServer(h)
	defer srv.Close()

	// /healthz
	resp, err := srv.Client().Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("healthz status = %d", resp.StatusCode)
	}

	// /status — bootstrap section should report the row counts
	statusResp, err := srv.Client().Get(srv.URL + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer statusResp.Body.Close()
	var status monitor.Status
	if err := json.NewDecoder(statusResp.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	if !status.Bootstrap.Found {
		t.Errorf("monitor bootstrap.found = false; want true")
	}
	if status.Bootstrap.Actors != 3 {
		t.Errorf("monitor actors = %d, want 3", status.Bootstrap.Actors)
	}
	if status.Run.RawFilesTotal == 0 {
		t.Errorf("monitor run.raw_files_total = 0; expected > 0")
	}
	if !status.Snapshot.Found {
		t.Errorf("monitor snapshot.found = false; want true")
	}
}

// TestPipelineInterruptResumeBootstrap verifies that a crash mid-bootstrap
// does not lose progress: a fresh run picks up only the not-yet-completed DIDs.
func TestPipelineInterruptResumeBootstrap(t *testing.T) {
	dataDir := t.TempDir()
	objDir := t.TempDir()
	obj, err := objstore.NewLocal(objDir)
	if err != nil {
		t.Fatal(err)
	}
	sling := slingshot.NewFake()
	for _, d := range []string{"did:plc:a", "did:plc:b", "did:plc:c"} {
		sling.Set(d, string(model.CollectionProfile), "self", slingshot.Record{
			URI: "at://" + d + "/app.bsky.actor.profile/self",
			Value: json.RawMessage(`{"displayName":"x"}`),
		})
	}
	con := constellation.NewFake()
	cfg := config.Config{
		DataDir:         dataDir,
		ObjectStore:     "local",
		ObjectStoreRoot: objDir,
		LookbackDays:    30,
		Concurrency:     1, // serialize so FailOnce timing is predictable
		LogLevel:        "info",
		StatsInterval:   time.Hour,
	}

	// First run: simulate a transient failure on did:plc:b's profile fetch.
	// The fetcher still emits a final chunk for did:plc:b (so we don't loop
	// forever on broken upstreams), so all three land in bootstrap_progress
	// — but only a and c successfully fetched a profile.
	sling.FailOnce["did:plc:b"] = true
	if err := bootstrap.RunWith(context.Background(), cfg, bootstrap.Deps{
		PLC:           plc.NewFake(time.Now(), []string{"did:plc:a", "did:plc:b", "did:plc:c"}),
		Slingshot:     sling,
		Constellation: con,
		ObjStore:      obj,
		Now:           nowFn,
	}); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if sling.FailOnce["did:plc:b"] {
		t.Errorf("FailOnce on did:plc:b never consumed (no fetch attempted)")
	}

	// Second run: PLC has the same three DIDs. All are already in
	// bootstrap_progress, so the producer skips them. FailOnce on
	// did:plc:a as a tripwire: if the resume re-fetched, the test fails.
	sling.FailOnce["did:plc:a"] = true
	if err := obj.Delete(context.Background(), "bootstrap/2026-04-26/social_graph.duckdb"); err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.RunWith(context.Background(), cfg, bootstrap.Deps{
		PLC:           plc.NewFake(time.Now(), []string{"did:plc:a", "did:plc:b", "did:plc:c"}),
		Slingshot:     sling,
		Constellation: con,
		ObjStore:      obj,
		Now:           nowFn,
	}); err != nil {
		t.Fatalf("second run: %v", err)
	}
	if !sling.FailOnce["did:plc:a"] {
		t.Errorf("resume incorrectly re-fetched did:plc:a")
	}

	// Final state: 3 DIDs complete.
	dbPath := filepath.Join(dataDir, "bootstrap-staging", "social_graph.duckdb")
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int64
	if err := db.QueryRow("SELECT count(*) FROM bootstrap_progress").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("bootstrap_progress = %d, want 3", n)
	}
}

// TestPipelineInterruptResumeRun verifies the run command's cursor advances
// across restarts: events emitted before the cursor are skipped on resume.
func TestPipelineInterruptResumeRun(t *testing.T) {
	dataDir := t.TempDir()
	objDir := t.TempDir()
	obj, err := objstore.NewLocal(objDir)
	if err != nil {
		t.Fatal(err)
	}

	// Round 1: one event in window, then context cancels.
	cfg := config.Config{
		DataDir:         dataDir,
		ObjectStore:     "local",
		ObjectStoreRoot: objDir,
		LookbackDays:    30,
		Concurrency:     1,
		LogLevel:        "info",
		StatsInterval:   50 * time.Millisecond,
		Languages:       []string{"en"},
	}
	earlyEvent := commit("did:plc:a", string(model.CollectionPost), "p1", jetstream.OpCreate,
		`{"text":"first","langs":["en"]}`, fixedNow.Add(-2*time.Hour))
	round1 := jetstream.NewFake([]jetstream.Event{earlyEvent})
	ctx1, cancel1 := context.WithTimeout(context.Background(), 600*time.Millisecond)
	if err := runcmd.RunWith(ctx1, cfg, runcmd.Deps{Subscriber: round1, ObjStore: obj, Now: nowFn}); err != nil &&
		err != context.DeadlineExceeded && err != context.Canceled {
		t.Fatalf("round 1: %v", err)
	}
	cancel1()
	if round1.Emitted() != 1 {
		t.Errorf("round 1 emitted = %d, want 1", round1.Emitted())
	}

	// Round 2: subscriber sees both the old event and a new one. The cursor
	// should make the subscriber filter out the old event.
	lateEvent := commit("did:plc:b", string(model.CollectionPost), "p2", jetstream.OpCreate,
		`{"text":"second","langs":["en"]}`, fixedNow.Add(-1*time.Hour))
	round2 := jetstream.NewFake([]jetstream.Event{earlyEvent, lateEvent})
	ctx2, cancel2 := context.WithTimeout(context.Background(), 600*time.Millisecond)
	if err := runcmd.RunWith(ctx2, cfg, runcmd.Deps{Subscriber: round2, ObjStore: obj, Now: nowFn}); err != nil &&
		err != context.DeadlineExceeded && err != context.Canceled {
		t.Fatalf("round 2: %v", err)
	}
	cancel2()
	if round2.Emitted() != 1 {
		t.Errorf("round 2 emitted = %d, want 1 (cursor must filter the old event)", round2.Emitted())
	}
}

// commit builds a jetstream.Event for a commit.
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

// helper for verbose JSON debugging in test failures.
var _ = func() *bytes.Buffer {
	return &bytes.Buffer{}
}

// _ avoids `fmt` going unused if the assertion blocks above are commented out
// during local debugging.
var _ = fmt.Sprintf
