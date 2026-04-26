package labels

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/coder/websocket"
)

// frameLabels encodes an EventHeader{op:1, t:"#labels"} followed by a
// LabelSubscribeLabels_Labels payload using indigo's own CBOR marshalers
// — identical to what a real labeler pushes on the wire.
func frameLabels(t *testing.T, evt *comatproto.LabelSubscribeLabels_Labels) []byte {
	t.Helper()
	var buf bytes.Buffer
	hdr := events.EventHeader{Op: events.EvtKindMessage, MsgType: "#labels"}
	if err := hdr.MarshalCBOR(&buf); err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	if err := evt.MarshalCBOR(&buf); err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return buf.Bytes()
}

// fakeLabelerServer stands up an httptest.Server that speaks the labeler
// subscription protocol. On connect it pushes `frames` in order and then
// holds the connection open until the client closes it.
func fakeLabelerServer(t *testing.T, frames [][]byte) *httptest.Server {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
		})
		if err != nil {
			t.Logf("ws accept: %v", err)
			return
		}
		defer c.Close(websocket.StatusNormalClosure, "bye")

		for _, f := range frames {
			if err := c.Write(r.Context(), websocket.MessageBinary, f); err != nil {
				t.Logf("ws write: %v", err)
				return
			}
		}
		// Keep the connection open so the client can close it via ctx
		// cancellation. Read until the peer hangs up.
		_, _, _ = c.Read(r.Context())
	})
	return httptest.NewServer(handler)
}

// TestSubscribeSmoke drives the full Run() loop against a fake labeler,
// asserting that our client decodes the frame, filters by src, and
// persists rows + cursor.
func TestSubscribeSmoke(t *testing.T) {
	// Build one labels event with two entries: one matching the filter
	// (src = mod DID) and one that should be filtered out.
	filterDID := "did:plc:mod"
	otherDID := "did:plc:other"
	cid := "bafyFoo"
	evt := &comatproto.LabelSubscribeLabels_Labels{
		Seq: 4242,
		Labels: []*comatproto.LabelDefs_Label{
			{
				Src: filterDID,
				Uri: "at://did:plc:a/app.bsky.feed.post/p1",
				Cid: &cid,
				Val: "!takedown",
				Cts: "2026-04-24T12:00:00Z",
			},
			{
				Src: otherDID,
				Uri: "at://did:plc:b/app.bsky.feed.post/p2",
				Val: "porn",
				Cts: "2026-04-24T12:00:00Z",
			},
		},
	}
	frame := frameLabels(t, evt)

	srv := fakeLabelerServer(t, [][]byte{frame})
	defer srv.Close()

	// httptest serves http://; rewrite to ws:// for the client.
	u, _ := url.Parse(srv.URL)
	u.Scheme = "ws"

	tmp := t.TempDir()
	opts := Options{
		DataDir:               tmp,
		LabelerURL:            u.String(),
		LabelerDID:            filterDID,
		CheckpointEveryLabels: 1,
		CheckpointInterval:    100 * time.Millisecond,
	}

	// Run() loops forever — we drive it with a timed context. 2 seconds
	// is plenty for a single frame to be consumed + checkpointed.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// Kick off Run in a goroutine so we can stop it after the expected
	// DB state arrives.
	errCh := make(chan error, 1)
	go func() { errCh <- Run(ctx, opts, logger) }()

	// Poll labels.db until we see the expected row.
	dbPath := filepath.Join(tmp, "labels.db")
	deadline := time.Now().Add(3 * time.Second)
	var n int
	for time.Now().Before(deadline) {
		if _, err := os.Stat(dbPath); err == nil {
			db, err := sql.Open("sqlite", "file:"+dbPath+"?mode=ro")
			if err == nil {
				_ = db.QueryRow(`SELECT count(*) FROM labels`).Scan(&n)
				db.Close()
			}
			if n >= 1 {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if n != 1 {
		cancel()
		<-errCh
		t.Fatalf("expected exactly 1 persisted label (after src filter), got %d", n)
	}

	// Give the periodic checkpoint a beat to flush the cursor.
	time.Sleep(200 * time.Millisecond)
	cancel()
	<-errCh

	// The filtered-out row should not be present.
	db, err := sql.Open("sqlite", "file:"+dbPath+"?mode=ro")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	var val, src string
	if err := db.QueryRow(`SELECT val, src FROM labels`).Scan(&val, &src); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if val != "!takedown" || src != filterDID {
		t.Fatalf("persisted wrong row: val=%q src=%q", val, src)
	}

	// Cursor should reflect evt.Seq.
	cur, err := loadCursor(filepath.Join(tmp, "labels_cursor.json"))
	if err != nil {
		t.Fatalf("loadCursor: %v", err)
	}
	if cur.Seq != 4242 {
		t.Fatalf("cursor.seq: want 4242, got %d", cur.Seq)
	}
	if !strings.HasPrefix(cur.Endpoint, "ws://") {
		t.Fatalf("cursor.endpoint: want ws:// URL, got %q", cur.Endpoint)
	}
}

// TestNoFilterKeepsAll confirms that LabelerDID="" (empty) disables the
// src filter — both rows persist.
func TestNoFilterKeepsAll(t *testing.T) {
	evt := &comatproto.LabelSubscribeLabels_Labels{
		Seq: 1,
		Labels: []*comatproto.LabelDefs_Label{
			{Src: "did:plc:x", Uri: "at://did:plc:a/app.bsky.feed.post/p1", Val: "a", Cts: "2026-04-24T12:00:00Z"},
			{Src: "did:plc:y", Uri: "at://did:plc:b/app.bsky.feed.post/p2", Val: "b", Cts: "2026-04-24T12:00:00Z"},
		},
	}
	srv := fakeLabelerServer(t, [][]byte{frameLabels(t, evt)})
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	u.Scheme = "ws"

	tmp := t.TempDir()
	opts := Options{
		DataDir:               tmp,
		LabelerURL:            u.String(),
		LabelerDID:            "", // no filter
		CheckpointEveryLabels: 1,
		CheckpointInterval:    100 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	errCh := make(chan error, 1)
	go func() { errCh <- Run(ctx, opts, logger) }()

	dbPath := filepath.Join(tmp, "labels.db")
	deadline := time.Now().Add(3 * time.Second)
	var n int
	for time.Now().Before(deadline) {
		if _, err := os.Stat(dbPath); err == nil {
			db, err := sql.Open("sqlite", "file:"+dbPath+"?mode=ro")
			if err == nil {
				_ = db.QueryRow(`SELECT count(*) FROM labels`).Scan(&n)
				db.Close()
			}
			if n >= 2 {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	<-errCh

	if n != 2 {
		t.Fatalf("expected 2 rows without src filter, got %d", n)
	}
}
