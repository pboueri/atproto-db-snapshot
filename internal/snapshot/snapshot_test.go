package snapshot

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"

	"github.com/pboueri/atproto-db-snapshot/internal/bootstrap"
	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/intern"
	"github.com/pboueri/atproto-db-snapshot/internal/model"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
	"github.com/pboueri/atproto-db-snapshot/internal/rawio"
)

// fixedNow is the wall clock the tests pin everything to. The window endpoints
// fall out of cfg.LookbackDays applied against this value.
var fixedNow = time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)

// fixture builds a complete object-storage layout suitable for a snapshot run:
//
//   - bootstrap/2026-04-01/social_graph.duckdb with two actors (alice, bob)
//     and one follow alice → bob.
//   - raw/2026-04-15/follows-*.parquet adding a follow alice → carol (create).
//   - raw/2026-04-20/follows-*.parquet deleting the alice → bob baseline follow.
//   - raw/2026-04-22/profiles-*.parquet adding a brand-new actor david.
//   - raw/2026-04-22/posts-*.parquet — alice posts twice; one in-window,
//     one out-of-window.
//   - raw/2026-04-22/likes-*.parquet — bob likes both alice posts.
//   - raw/2026-04-22/reposts-*.parquet — carol reposts the in-window post.
//   - raw/2026-04-22/posts-*.parquet — carol posts a reply to the in-window post.
//
// The two posts sit either side of the (now - 30d, now] boundary so the
// window-bounded tables filter exactly one out.
type fixture struct {
	dataDir string
	objDir  string
	obj     *objstore.Local
}

func setupFixture(t *testing.T) fixture {
	t.Helper()
	dataDir := t.TempDir()
	objDir := t.TempDir()
	obj, err := objstore.NewLocal(objDir)
	if err != nil {
		t.Fatal(err)
	}

	// Bootstrap baseline: alice + bob, alice follows bob.
	bootstrapPath := filepath.Join(objDir, "bootstrap", "2026-04-01", "social_graph.duckdb")
	if err := bootstrap.WriteFixture(context.Background(), bootstrapPath, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	// Replace the synthetic baseline with one we control. WriteFixture's
	// schema is what we want; we just want named DIDs, so we open the file
	// and insert by hand.
	db, err := sql.Open("duckdb", bootstrapPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	insertActor(t, db, "did:plc:alice")
	insertActor(t, db, "did:plc:bob")
	insertFollow(t, db, "did:plc:alice", "did:plc:bob", "f-alice-bob", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Raw deltas via the rawio sink so the on-disk parquet matches what
	// rawio writes in production.
	sink, err := rawio.New(filepath.Join(dataDir, "rawio-staging"), obj)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sink.Close(context.Background()) })

	// New follow on day 15.
	d15 := time.Date(2026, 4, 15, 12, 0, 0, 0, time.UTC)
	if err := sink.AppendFollows([]model.Follow{{
		SrcDID: "did:plc:alice", SrcDIDID: intern.DIDID("did:plc:alice"),
		DstDID: "did:plc:carol", DstDIDID: intern.DIDID("did:plc:carol"),
		RKey: "f-alice-carol", CreatedAt: d15, IndexedAt: d15,
		Op: model.OpCreate, Source: model.SourceFirehose,
	}}); err != nil {
		t.Fatal(err)
	}
	// Delete the bootstrap follow on day 20.
	d20 := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	if err := sink.AppendFollows([]model.Follow{{
		SrcDID: "did:plc:alice", SrcDIDID: intern.DIDID("did:plc:alice"),
		RKey: "f-alice-bob", IndexedAt: d20,
		Op: model.OpDelete, Source: model.SourceFirehose,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := sink.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	// New profile on day 22.
	d22 := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	if err := sink.AppendProfiles([]model.Profile{{
		DID: "did:plc:david", DIDID: intern.DIDID("did:plc:david"),
		Handle: "david.bsky.social", DisplayName: "David",
		CreatedAt: d22, IndexedAt: d22,
		Op: model.OpCreate, Source: model.SourceFirehose,
	}}); err != nil {
		t.Fatal(err)
	}

	// Posts: one in-window (april 22, well within 30-day lookback ending
	// april 26) and one out-of-window (march 1).
	postIn := makePost("did:plc:alice", "p-in", d22)
	postOut := makePost("did:plc:alice", "p-out", time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))
	postReply := makePost("did:plc:carol", "p-reply", d22)
	postReply.ReplyParentURI = postIn.URI
	postReply.ReplyParentID = postIn.URIID
	if err := sink.AppendPosts([]model.Post{postIn, postOut, postReply}); err != nil {
		t.Fatal(err)
	}

	// Likes: bob likes both alice posts. Both are in-window appends, but
	// the like targeting the out-of-window post is itself emitted in-window
	// so it survives.
	if err := sink.AppendLikes([]model.Like{
		{ActorDID: "did:plc:bob", ActorDIDID: intern.DIDID("did:plc:bob"),
			SubjectURI: postIn.URI, SubjectID: postIn.URIID, RKey: "lk-1",
			CreatedAt: d22, IndexedAt: d22, Op: model.OpCreate, Source: model.SourceFirehose},
		{ActorDID: "did:plc:bob", ActorDIDID: intern.DIDID("did:plc:bob"),
			SubjectURI: postOut.URI, SubjectID: postOut.URIID, RKey: "lk-2",
			CreatedAt: d22, IndexedAt: d22, Op: model.OpCreate, Source: model.SourceFirehose},
	}); err != nil {
		t.Fatal(err)
	}

	// Repost: carol reposts the in-window post.
	if err := sink.AppendReposts([]model.Repost{{
		ActorDID: "did:plc:carol", ActorDIDID: intern.DIDID("did:plc:carol"),
		SubjectURI: postIn.URI, SubjectID: postIn.URIID, RKey: "rp-1",
		CreatedAt: d22, IndexedAt: d22, Op: model.OpCreate, Source: model.SourceFirehose,
	}}); err != nil {
		t.Fatal(err)
	}

	if err := sink.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	return fixture{dataDir: dataDir, objDir: objDir, obj: obj}
}

func insertActor(t *testing.T, db *sql.DB, did string) {
	t.Helper()
	now := fixedNow
	if _, err := db.Exec(
		`INSERT INTO actors(did_id, did, handle, indexed_at, source) VALUES (?, ?, ?, ?, ?)`,
		intern.DIDID(did), did, did, now, model.SourceBootstrap,
	); err != nil {
		t.Fatal(err)
	}
}

func insertFollow(t *testing.T, db *sql.DB, src, dst, rkey string, created time.Time) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT INTO follows(src_did_id, rkey, dst_did_id, src_did, dst_did, created_at, indexed_at, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		intern.DIDID(src), rkey, intern.DIDID(dst), src, dst, created, created, model.SourceBootstrap,
	); err != nil {
		t.Fatal(err)
	}
}

func makePost(did, rkey string, t time.Time) model.Post {
	uri := "at://" + did + "/app.bsky.feed.post/" + rkey
	return model.Post{
		URI:       uri,
		URIID:     intern.URIID(uri),
		DID:       did,
		DIDID:     intern.DIDID(did),
		RKey:      rkey,
		Text:      "hello",
		CreatedAt: t,
		IndexedAt: t,
		Op:        model.OpCreate,
		Source:    model.SourceFirehose,
	}
}

func TestSnapshotProducesGraphAndAll(t *testing.T) {
	fx := setupFixture(t)
	cfg := config.Config{
		DataDir:           fx.dataDir,
		ObjectStore:       "local",
		ObjectStoreRoot:   fx.objDir,
		LookbackDays:      30,
		Concurrency:       1,
		LogLevel:          "info",
		StatsInterval:     time.Hour,
		DuckDBMemoryLimit: "1GB",
	}
	deps := Deps{
		ObjStore: fx.obj,
		Now:      func() time.Time { return fixedNow },
	}
	if err := RunWith(context.Background(), cfg, deps); err != nil {
		t.Fatalf("RunWith: %v", err)
	}

	for _, p := range []string{
		"snapshot/current_graph.duckdb",
		"snapshot/current_all.duckdb",
		"snapshot/snapshot_metadata.json",
	} {
		if _, err := fx.obj.Stat(context.Background(), p); err != nil {
			t.Fatalf("expected uploaded %s: %v", p, err)
		}
	}

	// Inspect the produced graph file.
	graphPath := filepath.Join(fx.objDir, "snapshot", "current_graph.duckdb")
	gdb, err := sql.Open("duckdb", graphPath)
	if err != nil {
		t.Fatal(err)
	}
	defer gdb.Close()

	// actors: alice, bob, david. carol is referenced but not seen as a profile,
	// so should not appear (we don't auto-create from follow targets).
	var actors int64
	if err := gdb.QueryRow("SELECT count(*) FROM actors").Scan(&actors); err != nil {
		t.Fatal(err)
	}
	if actors != 3 {
		t.Errorf("actors = %d, want 3", actors)
	}

	// David should be present from the raw profile.
	var david int64
	if err := gdb.QueryRow("SELECT count(*) FROM actors WHERE did = 'did:plc:david'").Scan(&david); err != nil {
		t.Fatal(err)
	}
	if david != 1 {
		t.Errorf("david row = %d, want 1", david)
	}

	// follows: bootstrap had alice→bob; raw deleted it and added alice→carol.
	// Net result: 1 row.
	var follows int64
	if err := gdb.QueryRow("SELECT count(*) FROM follows").Scan(&follows); err != nil {
		t.Fatal(err)
	}
	if follows != 1 {
		t.Errorf("follows = %d, want 1 (alice→carol after delete)", follows)
	}

	var alicecarol int64
	if err := gdb.QueryRow("SELECT count(*) FROM follows WHERE src_did = 'did:plc:alice' AND dst_did = 'did:plc:carol'").Scan(&alicecarol); err != nil {
		t.Fatal(err)
	}
	if alicecarol != 1 {
		t.Errorf("alice→carol follow missing: got %d", alicecarol)
	}

	// actor_aggs: alice has following=1 (carol), 0 followers. carol has
	// followers=1, 0 following. bob has followers=0, following=0.
	var aliceFollowing, carolFollowers int64
	if err := gdb.QueryRow("SELECT following FROM actor_aggs WHERE did = 'did:plc:alice'").Scan(&aliceFollowing); err != nil {
		t.Fatal(err)
	}
	if aliceFollowing != 1 {
		t.Errorf("alice.following = %d, want 1", aliceFollowing)
	}
	if err := gdb.QueryRow("SELECT followers FROM actor_aggs WHERE did = 'did:plc:bob'").Scan(&carolFollowers); err == nil {
		// bob exists; followers should be 0 after delete.
		if carolFollowers != 0 {
			t.Errorf("bob.followers = %d, want 0 after follow delete", carolFollowers)
		}
	}

	// Inspect the all file.
	allPath := filepath.Join(fx.objDir, "snapshot", "current_all.duckdb")
	adb, err := sql.Open("duckdb", allPath)
	if err != nil {
		t.Fatal(err)
	}
	defer adb.Close()

	// Posts in-window: 2 (postIn + postReply); postOut excluded by indexed_at.
	var posts int64
	if err := adb.QueryRow("SELECT count(*) FROM posts").Scan(&posts); err != nil {
		t.Fatal(err)
	}
	if posts != 2 {
		t.Errorf("posts = %d, want 2 (postOut filtered)", posts)
	}

	// post_aggs: alice's in-window post should have likes_count=1 reposts_count=1 replies_count=1.
	var lc, rc, rep int64
	if err := adb.QueryRow(`
        SELECT likes_count, reposts_count, replies_count
        FROM post_aggs
        WHERE uri_id = ?`, intern.URIID("at://did:plc:alice/app.bsky.feed.post/p-in")).Scan(&lc, &rc, &rep); err != nil {
		t.Fatal(err)
	}
	if lc != 1 || rc != 1 || rep != 1 {
		t.Errorf("post_aggs(p-in) likes=%d reposts=%d replies=%d; want 1/1/1", lc, rc, rep)
	}

	// actor_aggs: alice should have total_posts_in_window=1, total_likes_received_in_window=1.
	// (postOut has a like but the post itself is out-of-window so the JOIN
	// against in-window posts drops it.)
	var alicePostsIn, aliceLikesIn int64
	if err := adb.QueryRow("SELECT total_posts_in_window, total_likes_received_in_window FROM actor_aggs WHERE did = 'did:plc:alice'").Scan(&alicePostsIn, &aliceLikesIn); err != nil {
		t.Fatal(err)
	}
	if alicePostsIn != 1 {
		t.Errorf("alice.total_posts_in_window = %d, want 1", alicePostsIn)
	}
	if aliceLikesIn != 1 {
		t.Errorf("alice.total_likes_received_in_window = %d, want 1", aliceLikesIn)
	}
}

// TestSnapshotAnalyticQueries exercises the queries called out in the spec
// against the produced current_all.duckdb so a regression in the schema or
// the SQL pipeline shows up here rather than at consumer time.
func TestSnapshotAnalyticQueries(t *testing.T) {
	fx := setupFixture(t)
	cfg := config.Config{
		DataDir:           fx.dataDir,
		ObjectStore:       "local",
		ObjectStoreRoot:   fx.objDir,
		LookbackDays:      30,
		Concurrency:       1,
		LogLevel:          "info",
		StatsInterval:     time.Hour,
		DuckDBMemoryLimit: "",
	}
	deps := Deps{ObjStore: fx.obj, Now: func() time.Time { return fixedNow }}
	if err := RunWith(context.Background(), cfg, deps); err != nil {
		t.Fatalf("RunWith: %v", err)
	}

	allPath := filepath.Join(fx.objDir, "snapshot", "current_all.duckdb")
	db, err := sql.Open("duckdb", allPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. "How many people followed someone yesterday" — count distinct src
	// where created_at falls in the previous calendar day. The fixture
	// has no follows on (apr 25), so the answer is 0; we just want the
	// query to execute without error.
	var followers int64
	if err := db.QueryRow(`
        SELECT count(DISTINCT src_did_id)
        FROM follows
        WHERE created_at >= TIMESTAMP '2026-04-25 00:00:00'
          AND created_at <  TIMESTAMP '2026-04-26 00:00:00'
    `).Scan(&followers); err != nil {
		t.Errorf("yesterday-follows query: %v", err)
	}

	// 2. "% of total follows generated yesterday" — division.
	var pct sql.NullFloat64
	if err := db.QueryRow(`
        SELECT 100.0 * sum(CASE WHEN created_at >= TIMESTAMP '2026-04-25 00:00:00' AND created_at < TIMESTAMP '2026-04-26 00:00:00' THEN 1 ELSE 0 END) / nullif(count(*), 0)
        FROM follows
    `).Scan(&pct); err != nil {
		t.Errorf("pct-follows-yesterday query: %v", err)
	}

	// 3. "How many people who posted got 1 like" — JOIN posts → post_aggs
	// where likes_count >= 1, count distinct authors. Expected 1 (alice
	// posted p-in which received 1 like).
	var posters int64
	if err := db.QueryRow(`
        SELECT count(DISTINCT p.did_id)
        FROM posts p
        JOIN post_aggs a USING (uri_id)
        WHERE a.likes_count >= 1
    `).Scan(&posters); err != nil {
		t.Fatal(err)
	}
	if posters != 1 {
		t.Errorf("posters-with-1-like = %d, want 1", posters)
	}

	// 4. "How many posts got at least one like" — should be 1 (p-in).
	var likedPosts int64
	if err := db.QueryRow(`
        SELECT count(*)
        FROM post_aggs
        WHERE likes_count >= 1
    `).Scan(&likedPosts); err != nil {
		t.Fatal(err)
	}
	if likedPosts != 1 {
		t.Errorf("posts-with-1-like = %d, want 1", likedPosts)
	}
}
