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
