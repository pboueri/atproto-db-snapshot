package jetstream

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// TestWSSubscriberRoundTrip dials a httptest server that speaks just enough
// of the jetstream WS contract to push a few frames, and asserts the
// subscriber surfaces them as decoded Events.
func TestWSSubscriberRoundTrip(t *testing.T) {
	frames := []Event{
		{DID: "did:plc:a", TimeUS: 1_700_000_000_000_000, Kind: KindCommit,
			Commit: &Commit{Operation: OpCreate, Collection: "app.bsky.feed.post", RKey: "p1", Record: json.RawMessage(`{"text":"hi"}`)}},
		{DID: "did:plc:b", TimeUS: 1_700_000_000_000_001, Kind: KindCommit,
			Commit: &Commit{Operation: OpCreate, Collection: "app.bsky.feed.like", RKey: "l1"}},
	}

	var seenCursor string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenCursor = r.URL.Query().Get("cursor")
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		ctx := r.Context()
		for _, ev := range frames {
			body, _ := json.Marshal(ev)
			if err := conn.Write(ctx, websocket.MessageText, body); err != nil {
				return
			}
		}
		// Hold the connection open briefly so the client has time to read
		// before we close.
		time.Sleep(100 * time.Millisecond)
		conn.Close(websocket.StatusNormalClosure, "")
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/subscribe"
	sub := &WSSubscriber{
		Endpoints:  []string{wsURL},
		Backoff:    50 * time.Millisecond,
		MaxBackoff: 100 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	out, errc := sub.Subscribe(ctx, SubscribeOptions{
		Cursor:      42,
		Collections: []string{"app.bsky.feed.post", "app.bsky.feed.like"},
	})

	var got []Event
	for ev := range out {
		got = append(got, ev)
		if len(got) == len(frames) {
			cancel() // signal the run loop to exit cleanly
			break
		}
	}
	// Drain so the subscriber can write its terminal error.
	for range out {
	}
	<-errc

	if len(got) != len(frames) {
		t.Fatalf("got %d events, want %d", len(got), len(frames))
	}
	if got[0].DID != "did:plc:a" || got[1].DID != "did:plc:b" {
		t.Errorf("event order wrong: %+v", got)
	}
	if seenCursor != "42" {
		t.Errorf("cursor query param = %q, want \"42\"", seenCursor)
	}
}

// TestWSSubscriberFailsOver verifies that when the first endpoint refuses
// connections the subscriber walks to the next one.
func TestWSSubscriberFailsOver(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		body, _ := json.Marshal(Event{DID: "did:plc:ok", TimeUS: 1, Kind: KindCommit, Commit: &Commit{Collection: "app.bsky.feed.post"}})
		conn.Write(r.Context(), websocket.MessageText, body)
		time.Sleep(50 * time.Millisecond)
		conn.Close(websocket.StatusNormalClosure, "")
	}))
	defer good.Close()

	badURL := "ws://127.0.0.1:1/subscribe" // port 1 reliably refuses
	goodURL := "ws" + strings.TrimPrefix(good.URL, "http") + "/subscribe"

	sub := &WSSubscriber{
		Endpoints:  []string{badURL, goodURL},
		Backoff:    20 * time.Millisecond,
		MaxBackoff: 50 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, _ := sub.Subscribe(ctx, SubscribeOptions{})
	select {
	case ev, ok := <-out:
		if !ok {
			t.Fatal("channel closed without any event")
		}
		if ev.DID != "did:plc:ok" {
			t.Errorf("unexpected event: %+v", ev)
		}
	case <-ctx.Done():
		t.Fatal("timed out before failover delivered an event")
	}
}
