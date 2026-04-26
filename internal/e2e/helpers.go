// Package e2e contains end-to-end tests that exercise the full
// Jetstream → staging → parquet → current_all.duckdb pipeline.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// newDiscardLogger returns a slog.Logger that swallows output so test runs
// stay readable.
func newDiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// jetstreamEventLine is the server-side shape of a single Jetstream commit
// event. It's intentionally hand-written (not imported from internal/run)
// because the test is external and we want to mirror the on-the-wire shape
// exactly.
type jetstreamEventLine struct {
	DID    string          `json:"did"`
	TimeUS int64           `json:"time_us"`
	Kind   string          `json:"kind"`
	Commit jetstreamCommit `json:"commit"`
}

type jetstreamCommit struct {
	Rev        string          `json:"rev"`
	Operation  string          `json:"operation"`
	Collection string          `json:"collection"`
	Rkey       string          `json:"rkey"`
	CID        string          `json:"cid"`
	Record     json.RawMessage `json:"record,omitempty"`
}

// fakeJetstream is a minimal test double for the Jetstream WebSocket
// endpoint. It serves a pre-loaded slice of commit events, one per text
// frame, and closes the connection cleanly when the fixture is exhausted.
type fakeJetstream struct {
	srv    *httptest.Server
	events []jetstreamEventLine

	// sent is the total number of frames sent across all connections.
	mu   sync.Mutex
	sent int
}

// newFakeJetstream builds an httptest server that upgrades /subscribe to
// WebSocket and streams `events` to each connection.
func newFakeJetstream(events []jetstreamEventLine) *fakeJetstream {
	f := &fakeJetstream{events: events}
	mux := http.NewServeMux()
	mux.HandleFunc("/subscribe", f.handleWS)
	f.srv = httptest.NewServer(mux)
	return f
}

func (f *fakeJetstream) Close() { f.srv.Close() }

// URL returns a ws:// URL that internal/run can dial directly.
func (f *fakeJetstream) URL() string {
	// httptest.Server uses http://; websocket dialing accepts http/ws.
	return "ws://" + strings.TrimPrefix(f.srv.URL, "http://") + "/subscribe"
}

func (f *fakeJetstream) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
	})
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "fixture exhausted")

	ctx := r.Context()
	for _, ev := range f.events {
		b, err := json.Marshal(ev)
		if err != nil {
			return
		}
		if err := conn.Write(ctx, websocket.MessageText, b); err != nil {
			return
		}
		f.mu.Lock()
		f.sent++
		f.mu.Unlock()
	}
}

// yesterdayUTCMicros returns (day-string, microseconds-since-epoch) for
// noon UTC yesterday relative to now. Noon avoids timezone-edge weirdness.
func yesterdayUTCMicros() (string, int64) {
	now := time.Now().UTC()
	y := time.Date(now.Year(), now.Month(), now.Day()-1, 12, 0, 0, 0, time.UTC)
	return y.Format("2006-01-02"), y.UnixMicro()
}

// fixturePlan enumerates what the e2e fixture contains so the
// post-pipeline assertions can reference canonical counts.
type fixturePlan struct {
	Day       string
	Actors    []string // DIDs
	PostCount int
	LikeCount int
	// LikesByPost indexes likes per (authorDID, rkey).
	LikesByPost map[string]int
	// PostsByAuthor counts posts per DID.
	PostsByAuthor map[string]int
	FollowCount   int
	BlockCount    int
}

// buildFixture constructs a deterministic set of commit events covering
// three DIDs, ~20 posts, ~40 likes, 5 follows, 1 block — all stamped at
// yesterday UTC so the rollover seals them as "yesterday".
func buildFixture() ([]jetstreamEventLine, fixturePlan) {
	day, baseUS := yesterdayUTCMicros()
	const us = int64(1)
	var (
		events []jetstreamEventLine
		offset int64
	)
	push := func(ev jetstreamEventLine) {
		offset += us
		ev.TimeUS = baseUS + offset
		events = append(events, ev)
	}

	dids := []string{"did:plc:a", "did:plc:b", "did:plc:c"}

	// 20 posts distributed across DIDs: a=8, b=7, c=5.
	postsPerDID := map[string]int{
		dids[0]: 8, dids[1]: 7, dids[2]: 5,
	}
	type postKey struct{ did, rkey string }
	var allPosts []postKey
	postsByAuthor := map[string]int{}

	for _, d := range dids {
		for i := 0; i < postsPerDID[d]; i++ {
			rkey := fmt.Sprintf("p-%s-%d", d[len(d)-1:], i)
			rec, _ := json.Marshal(map[string]any{
				"text":      fmt.Sprintf("hello from %s #%d", d, i),
				"createdAt": time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339),
				"langs":     []string{"en"},
			})
			push(jetstreamEventLine{
				DID:  d,
				Kind: "commit",
				Commit: jetstreamCommit{
					Rev:        "rev1",
					Operation:  "create",
					Collection: "app.bsky.feed.post",
					Rkey:       rkey,
					CID:        "bafy" + rkey,
					Record:     rec,
				},
			})
			allPosts = append(allPosts, postKey{did: d, rkey: rkey})
			postsByAuthor[d]++
		}
	}

	// 40 likes: spread across posts so we can compute per-post counts.
	// Pattern: each liker (one of the 3 DIDs) likes the first N posts of
	// each OTHER author. Choose counts so total is 40.
	// a likes b's first 7 (all) + c's first 5 (all) = 12
	// b likes a's first 8 (all) + c's first 5 (all) = 13
	// c likes a's first 8 (all) + b's first 7 (all) = 15
	// 12 + 13 + 15 = 40.
	likesByPost := map[string]int{}
	addLike := func(liker, subjDID, subjRkey string, idx int) {
		rec, _ := json.Marshal(map[string]any{
			"subject": map[string]any{
				"uri": fmt.Sprintf("at://%s/app.bsky.feed.post/%s", subjDID, subjRkey),
				"cid": "bafy" + subjRkey,
			},
			"createdAt": time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339),
		})
		rkey := fmt.Sprintf("lk-%s-%d", liker[len(liker)-1:], idx)
		push(jetstreamEventLine{
			DID:  liker,
			Kind: "commit",
			Commit: jetstreamCommit{
				Rev:        "rev1",
				Operation:  "create",
				Collection: "app.bsky.feed.like",
				Rkey:       rkey,
				CID:        "bafy" + rkey,
				Record:     rec,
			},
		})
		likesByPost[subjDID+"/"+subjRkey]++
	}
	likerIdx := map[string]int{}
	likeAllPostsByAuthor := func(liker, author string) {
		for _, p := range allPosts {
			if p.did != author {
				continue
			}
			addLike(liker, p.did, p.rkey, likerIdx[liker])
			likerIdx[liker]++
		}
	}
	likeAllPostsByAuthor(dids[0], dids[1])
	likeAllPostsByAuthor(dids[0], dids[2])
	likeAllPostsByAuthor(dids[1], dids[0])
	likeAllPostsByAuthor(dids[1], dids[2])
	likeAllPostsByAuthor(dids[2], dids[0])
	likeAllPostsByAuthor(dids[2], dids[1])

	// 5 follows: a→b, a→c, b→a, b→c, c→a.
	addFollow := func(src, dst string, idx int) {
		rec, _ := json.Marshal(map[string]any{
			"subject":   dst,
			"createdAt": time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339),
		})
		push(jetstreamEventLine{
			DID:  src,
			Kind: "commit",
			Commit: jetstreamCommit{
				Rev:        "rev1",
				Operation:  "create",
				Collection: "app.bsky.graph.follow",
				Rkey:       fmt.Sprintf("f-%s-%d", src[len(src)-1:], idx),
				CID:        "bafyfollow",
				Record:     rec,
			},
		})
	}
	addFollow(dids[0], dids[1], 0)
	addFollow(dids[0], dids[2], 1)
	addFollow(dids[1], dids[0], 0)
	addFollow(dids[1], dids[2], 1)
	addFollow(dids[2], dids[0], 0)

	// 1 block: c blocks a.
	blockRec, _ := json.Marshal(map[string]any{
		"subject":   dids[0],
		"createdAt": time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339),
	})
	push(jetstreamEventLine{
		DID:  dids[2],
		Kind: "commit",
		Commit: jetstreamCommit{
			Rev:        "rev1",
			Operation:  "create",
			Collection: "app.bsky.graph.block",
			Rkey:       "b-c-0",
			CID:        "bafyblock",
			Record:     blockRec,
		},
	})

	return events, fixturePlan{
		Day:           day,
		Actors:        dids,
		PostCount:     20,
		LikeCount:     40,
		LikesByPost:   likesByPost,
		PostsByAuthor: postsByAuthor,
		FollowCount:   5,
		BlockCount:    1,
	}
}

// wait is a test-oriented busy-poller that returns nil once `check`
// returns true, or an error when deadline is reached.
func wait(ctx context.Context, deadline time.Duration, check func() bool) error {
	end := time.Now().Add(deadline)
	for {
		if check() {
			return nil
		}
		if time.Now().After(end) {
			return fmt.Errorf("wait: timed out after %v", deadline)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// ---------------------------------------------------------------------------
// Multi-day fixture (TestPipelineMultiDayReplay)
// ---------------------------------------------------------------------------

// multiDayPlan describes a fixture spanning two adjacent UTC days. It's
// the multi-day counterpart of fixturePlan and exposes everything the
// post-build assertions need to compute expected row counts and
// per-post like_counts deterministically.
type multiDayPlan struct {
	Day1, Day2 string

	// Day-1 creates that survive into the final snapshot.
	SurvivingPosts []postRef
	// Day-1 posts that are deleted on day 2.
	DeletedPosts []postRef
	// New posts created on day 2.
	NewPostsDay2 []postRef

	// Total counts for the final snapshot — e.g. 10 posts
	// (5 survivors + 5 day-2 creates) and 23 likes.
	ExpectedPosts int
	ExpectedLikes int

	// Expected like_count keyed by "did/rkey" (post identity in raw
	// space). Includes only surviving posts.
	ExpectedLikeCount map[string]int
}

// postRef is a raw (DID, rkey) pair, used to identify a post in plan
// space without depending on actor_id.
type postRef struct {
	DID  string
	Rkey string
}

// buildFixtureMultiDay constructs a deterministic two-day Jetstream
// fixture exercising spec §14.1 deletion-then-create ordering:
//
//   - day-1 creates (10 posts, 20 likes, 3 follows)
//   - day-2 deletes targeting some day-1 rows + new day-2 creates
//
// Day-1 is "two days ago UTC" so both days are sealable (strictly < today).
func buildFixtureMultiDay() ([]jetstreamEventLine, multiDayPlan) {
	now := time.Now().UTC()
	day1Time := time.Date(now.Year(), now.Month(), now.Day()-2, 12, 0, 0, 0, time.UTC)
	day2Time := day1Time.Add(24 * time.Hour)
	day1 := day1Time.Format("2006-01-02")
	day2 := day2Time.Format("2006-01-02")

	// Monotonic event timestamps. Each day gets its own base; events
	// within a day step by 1µs. Chronological emit order across days is
	// guaranteed because day2Base > all day1 timestamps.
	day1Base := day1Time.UnixMicro()
	day2Base := day2Time.UnixMicro()

	dids := []string{"did:plc:a", "did:plc:b", "did:plc:c"}

	// ---------- Day 1: 10 posts, 20 likes, 3 follows ----------
	// Posts: 3 by a, 4 by b, 3 by c.
	postsPerDID := map[string]int{dids[0]: 3, dids[1]: 4, dids[2]: 3}

	type post = postRef
	var day1Posts []post
	for _, d := range dids {
		for i := 0; i < postsPerDID[d]; i++ {
			day1Posts = append(day1Posts, post{DID: d, Rkey: fmt.Sprintf("p1-%s-%d", d[len(d)-1:], i)})
		}
	}

	// Pre-define the like graph so we can deterministically compute
	// per-post like_counts. Each entry: (likerDID, postIdx, rkey).
	// 20 likes total: each post receives ~2 likes from various DIDs.
	type like struct {
		liker   string
		postIdx int    // index into day1Posts
		rkey    string // unique like rkey
	}
	likes := []like{
		// Post 0 (a's #0): 2 likes (b, c)
		{dids[1], 0, "lk1-b-0"}, {dids[2], 0, "lk1-c-0"},
		// Post 1 (a's #1): 2 likes (b, c)
		{dids[1], 1, "lk1-b-1"}, {dids[2], 1, "lk1-c-1"},
		// Post 2 (a's #2): 2 likes (b, c)
		{dids[1], 2, "lk1-b-2"}, {dids[2], 2, "lk1-c-2"},
		// Post 3 (b's #0): 2 likes (a, c)
		{dids[0], 3, "lk1-a-0"}, {dids[2], 3, "lk1-c-3"},
		// Post 4 (b's #1): 2 likes (a, c)
		{dids[0], 4, "lk1-a-1"}, {dids[2], 4, "lk1-c-4"},
		// Post 5 (b's #2): 2 likes (a, c)
		{dids[0], 5, "lk1-a-2"}, {dids[2], 5, "lk1-c-5"},
		// Post 6 (b's #3): 2 likes (a, c)
		{dids[0], 6, "lk1-a-3"}, {dids[2], 6, "lk1-c-6"},
		// Post 7 (c's #0): 2 likes (a, b)
		{dids[0], 7, "lk1-a-4"}, {dids[1], 7, "lk1-b-3"},
		// Post 8 (c's #1): 2 likes (a, b)
		{dids[0], 8, "lk1-a-5"}, {dids[1], 8, "lk1-b-4"},
		// Post 9 (c's #2): 2 likes (a, b)
		{dids[0], 9, "lk1-a-6"}, {dids[1], 9, "lk1-b-5"},
	}

	var events []jetstreamEventLine
	push := func(base, off int64, ev jetstreamEventLine) {
		ev.TimeUS = base + off
		events = append(events, ev)
	}

	off := int64(0)
	for _, p := range day1Posts {
		rec, _ := json.Marshal(map[string]any{
			"text":      "day1 " + p.DID + " " + p.Rkey,
			"createdAt": day1Time.Format(time.RFC3339),
			"langs":     []string{"en"},
		})
		off++
		push(day1Base, off, jetstreamEventLine{
			DID: p.DID, Kind: "commit",
			Commit: jetstreamCommit{
				Rev: "rev1", Operation: "create",
				Collection: "app.bsky.feed.post",
				Rkey:       p.Rkey, CID: "bafy" + p.Rkey,
				Record: rec,
			},
		})
	}
	for _, l := range likes {
		subj := day1Posts[l.postIdx]
		rec, _ := json.Marshal(map[string]any{
			"subject": map[string]any{
				"uri": fmt.Sprintf("at://%s/app.bsky.feed.post/%s", subj.DID, subj.Rkey),
				"cid": "bafy" + subj.Rkey,
			},
			"createdAt": day1Time.Format(time.RFC3339),
		})
		off++
		push(day1Base, off, jetstreamEventLine{
			DID: l.liker, Kind: "commit",
			Commit: jetstreamCommit{
				Rev: "rev1", Operation: "create",
				Collection: "app.bsky.feed.like",
				Rkey:       l.rkey, CID: "bafy" + l.rkey,
				Record: rec,
			},
		})
	}
	// 3 follows: a→b, b→c, c→a.
	follows := [][2]string{{dids[0], dids[1]}, {dids[1], dids[2]}, {dids[2], dids[0]}}
	for i, f := range follows {
		rec, _ := json.Marshal(map[string]any{
			"subject":   f[1],
			"createdAt": day1Time.Format(time.RFC3339),
		})
		off++
		push(day1Base, off, jetstreamEventLine{
			DID: f[0], Kind: "commit",
			Commit: jetstreamCommit{
				Rev: "rev1", Operation: "create",
				Collection: "app.bsky.graph.follow",
				Rkey:       fmt.Sprintf("f1-%s-%d", f[0][len(f[0])-1:], i),
				CID:        "bafyfollow",
				Record:     rec,
			},
		})
	}

	// ---------- Day 2: deletes targeting day-1, plus new creates ----------
	// Delete 5 day-1 posts: 2 of a's (#0, #1), 2 of b's (#3, #4), 1 of c's (#7).
	deletedPostIdx := []int{0, 1, 3, 4, 7}
	deletedSet := map[int]bool{}
	for _, i := range deletedPostIdx {
		deletedSet[i] = true
	}
	var deletedPosts, survivingPosts []postRef
	for i, p := range day1Posts {
		if deletedSet[i] {
			deletedPosts = append(deletedPosts, p)
		} else {
			survivingPosts = append(survivingPosts, p)
		}
	}

	off2 := int64(0)
	for _, p := range deletedPosts {
		off2++
		push(day2Base, off2, jetstreamEventLine{
			DID: p.DID, Kind: "commit",
			Commit: jetstreamCommit{
				Rev: "rev2", Operation: "delete",
				Collection: "app.bsky.feed.post",
				Rkey:       p.Rkey,
			},
		})
	}

	// 5 new posts on day 2 (one by each DID + 2 more by a).
	newPosts := []postRef{
		{dids[0], "p2-a-0"}, {dids[0], "p2-a-1"},
		{dids[1], "p2-b-0"},
		{dids[2], "p2-c-0"}, {dids[2], "p2-c-1"},
	}
	for _, p := range newPosts {
		rec, _ := json.Marshal(map[string]any{
			"text":      "day2 " + p.DID + " " + p.Rkey,
			"createdAt": day2Time.Format(time.RFC3339),
			"langs":     []string{"en"},
		})
		off2++
		push(day2Base, off2, jetstreamEventLine{
			DID: p.DID, Kind: "commit",
			Commit: jetstreamCommit{
				Rev: "rev2", Operation: "create",
				Collection: "app.bsky.feed.post",
				Rkey:       p.Rkey, CID: "bafy" + p.Rkey,
				Record: rec,
			},
		})
	}

	// 5 like deletes targeting day-1 likes — pick first 5 from `likes`.
	deletedLikes := likes[:5]
	for _, l := range deletedLikes {
		off2++
		push(day2Base, off2, jetstreamEventLine{
			DID: l.liker, Kind: "commit",
			Commit: jetstreamCommit{
				Rev: "rev2", Operation: "delete",
				Collection: "app.bsky.feed.like",
				Rkey:       l.rkey,
			},
		})
	}

	// 8 NEW likes on day 2: 4 target surviving day-1 posts, 4 target new
	// day-2 posts. We deterministically pick subjects so per-post counts
	// are computable.
	type day2Like struct {
		liker string
		subj  postRef
		rkey  string
	}
	day2Likes := []day2Like{
		// Surviving day-1 posts (post idx 2, 5, 6, 8 — none deleted).
		{dids[1], day1Posts[2], "lk2-b-0"},
		{dids[2], day1Posts[5], "lk2-c-0"},
		{dids[0], day1Posts[6], "lk2-a-0"},
		{dids[1], day1Posts[8], "lk2-b-1"},
		// New day-2 posts.
		{dids[1], newPosts[0], "lk2-b-2"},
		{dids[2], newPosts[1], "lk2-c-1"},
		{dids[0], newPosts[2], "lk2-a-1"},
		{dids[2], newPosts[3], "lk2-c-2"},
	}
	for _, l := range day2Likes {
		rec, _ := json.Marshal(map[string]any{
			"subject": map[string]any{
				"uri": fmt.Sprintf("at://%s/app.bsky.feed.post/%s", l.subj.DID, l.subj.Rkey),
				"cid": "bafy" + l.subj.Rkey,
			},
			"createdAt": day2Time.Format(time.RFC3339),
		})
		off2++
		push(day2Base, off2, jetstreamEventLine{
			DID: l.liker, Kind: "commit",
			Commit: jetstreamCommit{
				Rev: "rev2", Operation: "create",
				Collection: "app.bsky.feed.like",
				Rkey:       l.rkey, CID: "bafy" + l.rkey,
				Record: rec,
			},
		})
	}

	// Build expected like_count per surviving post by walking the same
	// data the asserter will see.
	expected := make(map[string]int)
	// Day-1 likes that survive: every like whose target was NOT deleted
	// AND whose like rkey is not in deletedLikes.
	deletedLikeRkey := map[string]bool{}
	for _, l := range deletedLikes {
		deletedLikeRkey[l.rkey] = true
	}
	for _, l := range likes {
		if deletedSet[l.postIdx] {
			continue // post is gone — likes get blown away too via our SQL? actually no, but they don't matter for the surviving like_count
		}
		if deletedLikeRkey[l.rkey] {
			continue
		}
		subj := day1Posts[l.postIdx]
		expected[subj.DID+"/"+subj.Rkey]++
	}
	// Day-2 likes (all surviving since none are deleted).
	for _, l := range day2Likes {
		expected[l.subj.DID+"/"+l.subj.Rkey]++
	}

	// Final like count assertion: total likes on surviving subjects.
	// We had 20 day-1 likes; 5 were deleted; some target deleted posts,
	// but those rows are still in likes_current (delete only removes the
	// post, not its likes — the build doesn't cascade).
	//   surviving day-1 likes = 20 - 5 = 15
	//   day-2 likes           = 8
	//   total                 = 23
	expectedLikes := 15 + 8

	plan := multiDayPlan{
		Day1:              day1,
		Day2:              day2,
		SurvivingPosts:    survivingPosts,
		DeletedPosts:      deletedPosts,
		NewPostsDay2:      newPosts,
		ExpectedPosts:     len(survivingPosts) + len(newPosts), // 5 + 5 = 10
		ExpectedLikes:     expectedLikes,
		ExpectedLikeCount: expected,
	}
	return events, plan
}

// ---------------------------------------------------------------------------
// Paging fake Jetstream (TestPipelineRestartResume)
// ---------------------------------------------------------------------------

// pagingFake mirrors Jetstream's "send events strictly greater than
// cursor" protocol. The first connection emits `pauseAfter` events from
// the start of `events`, then idles until the consumer disconnects. A
// reconnect with `?cursor=<value>` resumes by emitting events whose
// time_us > cursor.
//
// This lets a test SIGTERM the consumer mid-stream, restart it, and
// observe that the persisted cursor on disk drives a clean resume with
// no event loss and no duplicates (PK-driven dedup proves the latter
// even in the presence of cursor rewind overlap).
type pagingFake struct {
	srv        *httptest.Server
	events     []jetstreamEventLine
	pauseAfter int

	mu          sync.Mutex
	connections int
	sent        int
}

// newFakeJetstreamPaging builds a paging fake. `pauseAfter` applies only
// to the very first connection; subsequent connections always emit until
// `events` is exhausted, then close cleanly.
func newFakeJetstreamPaging(events []jetstreamEventLine, pauseAfter int) *pagingFake {
	f := &pagingFake{events: events, pauseAfter: pauseAfter}
	mux := http.NewServeMux()
	mux.HandleFunc("/subscribe", f.handleWS)
	f.srv = httptest.NewServer(mux)
	return f
}

func (f *pagingFake) Close() { f.srv.Close() }

func (f *pagingFake) URL() string {
	return "ws://" + strings.TrimPrefix(f.srv.URL, "http://") + "/subscribe"
}

func (f *pagingFake) Connections() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connections
}

func (f *pagingFake) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
	})
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "fixture exhausted")

	f.mu.Lock()
	f.connections++
	connIdx := f.connections
	f.mu.Unlock()

	// Parse ?cursor= — Jetstream sends events strictly greater than this.
	var cursor int64
	if cs := r.URL.Query().Get("cursor"); cs != "" {
		_, _ = fmt.Sscanf(cs, "%d", &cursor)
	}

	// Build the slice of events to emit on this connection: always a
	// strict cursor filter, plus a pauseAfter-cap on the very first
	// connection.
	var toSend []jetstreamEventLine
	for _, ev := range f.events {
		if ev.TimeUS > cursor {
			toSend = append(toSend, ev)
		}
	}
	limit := len(toSend)
	if connIdx == 1 && f.pauseAfter > 0 && f.pauseAfter < limit {
		limit = f.pauseAfter
	}

	ctx := r.Context()
	for i := 0; i < limit; i++ {
		b, err := json.Marshal(toSend[i])
		if err != nil {
			return
		}
		if err := conn.Write(ctx, websocket.MessageText, b); err != nil {
			return
		}
		f.mu.Lock()
		f.sent++
		f.mu.Unlock()
	}

	// On the very first connection (and only when we capped early),
	// idle: keep the WS open so the consumer can't observe a clean
	// close and reconnect during the test's "frozen" phase. The
	// test signals end-of-phase by cancelling its consumer ctx, which
	// closes the WS; the server-side ctx then fires.
	if connIdx == 1 && f.pauseAfter > 0 && f.pauseAfter < len(toSend) {
		<-ctx.Done()
		return
	}
	// Otherwise let the function return (clean close).
}
