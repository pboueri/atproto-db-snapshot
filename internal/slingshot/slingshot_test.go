package slingshot

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPClientGetRecord(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/xrpc/com.atproto.repo.getRecord" {
			t.Errorf("path = %s", r.URL.Path)
		}
		q := r.URL.Query()
		if q.Get("repo") != "did:plc:x" || q.Get("collection") != "app.bsky.actor.profile" || q.Get("rkey") != "self" {
			t.Errorf("query = %v", q)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"uri":   "at://did:plc:x/app.bsky.actor.profile/self",
			"cid":   "bafy",
			"value": map[string]any{"displayName": "Test"},
		})
	}))
	defer srv.Close()

	c := NewHTTP(srv.URL, "ops@example.com", 100, 100)
	c.HTTP = srv.Client()

	rec, err := c.GetRecord(context.Background(), "did:plc:x", "app.bsky.actor.profile", "self")
	if err != nil {
		t.Fatal(err)
	}
	if rec.URI == "" || rec.CID == "" {
		t.Errorf("URI/CID empty: %+v", rec)
	}
	if string(rec.Value) == "" {
		t.Errorf("Value empty")
	}
}

func TestHTTPClientNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"RecordNotFound","message":"no record"}`))
	}))
	defer srv.Close()

	c := NewHTTP(srv.URL, "", 100, 100)
	c.HTTP = srv.Client()

	_, err := c.GetRecord(context.Background(), "did:plc:x", "app.bsky.actor.profile", "self")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestHTTPClientUserAgent(t *testing.T) {
	var ua string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ua = r.Header.Get("User-Agent")
		json.NewEncoder(w).Encode(map[string]any{"uri": "u", "cid": "c", "value": map[string]any{}})
	}))
	defer srv.Close()

	c := NewHTTP(srv.URL, "ops@example.com", 100, 100)
	c.HTTP = srv.Client()
	_, _ = c.GetRecord(context.Background(), "d", "c", "r")
	if ua == "" || !contains(ua, "ops@example.com") {
		t.Errorf("User-Agent = %q", ua)
	}
}

func TestHTTPClientRetriesOn429(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"uri": "u", "cid": "c", "value": map[string]any{}})
	}))
	defer srv.Close()

	c := NewHTTP(srv.URL, "", 100, 100)
	c.HTTP = srv.Client()
	c.MinBackoff = 1 * time.Millisecond
	c.MaxBackoff = 1 * time.Millisecond

	if _, err := c.GetRecord(context.Background(), "d", "c", "r"); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Errorf("calls = %d, want 2", calls.Load())
	}
}

func TestFakeNotFound(t *testing.T) {
	f := NewFake()
	_, err := f.GetRecord(context.Background(), "x", "c", "r")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("missing key should be ErrNotFound, got %v", err)
	}
}

func TestFakeFailOnce(t *testing.T) {
	f := NewFake()
	f.FailOnce["x"] = true
	if _, err := f.GetRecord(context.Background(), "x", "c", "r"); err == nil {
		t.Errorf("expected induced failure")
	}
	// Second call now returns ErrNotFound (cleared, but no Set, so missing).
	if _, err := f.GetRecord(context.Background(), "x", "c", "r"); !errors.Is(err, ErrNotFound) {
		t.Errorf("second call err = %v, want ErrNotFound", err)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
