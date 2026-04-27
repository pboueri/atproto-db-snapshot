package constellation

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPClientPaginates(t *testing.T) {
	// Two pages totaling 3 links; verify the cursor handshake and that the
	// caller sees every link exactly once.
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if got := r.URL.Path; got != "/links" {
			t.Errorf("path = %s", got)
		}
		if got := r.URL.Query().Get("target"); got != "did:plc:t" {
			t.Errorf("target = %q", got)
		}
		if got := r.URL.Query().Get("path"); got != ".subject" {
			t.Errorf("path query = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("cursor") {
		case "":
			json.NewEncoder(w).Encode(linksResponse{
				Total: 3,
				LinkingRecords: []Link{
					{DID: "did:plc:a", Collection: "app.bsky.graph.follow", RKey: "r1"},
					{DID: "did:plc:b", Collection: "app.bsky.graph.follow", RKey: "r2"},
				},
				Cursor: "cur1",
			})
		case "cur1":
			json.NewEncoder(w).Encode(linksResponse{
				Total: 3,
				LinkingRecords: []Link{
					{DID: "did:plc:c", Collection: "app.bsky.graph.follow", RKey: "r3"},
				},
				Cursor: "",
			})
		default:
			t.Errorf("unexpected cursor %q", r.URL.Query().Get("cursor"))
		}
	}))
	defer srv.Close()

	c := NewHTTP(srv.URL, "test@example.com", 100, 100)
	c.HTTP = srv.Client()

	var got []Link
	err := c.GetBacklinks(context.Background(), "did:plc:t", "app.bsky.graph.follow", ".subject", func(l Link) bool {
		got = append(got, l)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Errorf("got %d links, want 3", len(got))
	}
	if got[2].RKey != "r3" {
		t.Errorf("third link rkey = %q, want r3", got[2].RKey)
	}
	if calls.Load() != 2 {
		t.Errorf("server saw %d calls, want 2", calls.Load())
	}
}

func TestHTTPClientYieldStops(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(linksResponse{
			LinkingRecords: []Link{
				{DID: "did:plc:a", Collection: "x", RKey: "r1"},
				{DID: "did:plc:b", Collection: "x", RKey: "r2"},
			},
			Cursor: "next",
		})
	}))
	defer srv.Close()

	c := NewHTTP(srv.URL, "", 100, 100)
	c.HTTP = srv.Client()

	var seen int
	err := c.GetBacklinks(context.Background(), "t", "x", ".subject", func(l Link) bool {
		seen++
		return false // stop after first
	})
	if err != nil {
		t.Fatal(err)
	}
	if seen != 1 {
		t.Errorf("yield called %d times, want 1 (stopped early)", seen)
	}
}

func TestHTTPClientUserAgent(t *testing.T) {
	var ua string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ua = r.Header.Get("User-Agent")
		json.NewEncoder(w).Encode(linksResponse{})
	}))
	defer srv.Close()

	c := NewHTTP(srv.URL, "ops@example.com", 100, 100)
	c.HTTP = srv.Client()
	_ = c.GetBacklinks(context.Background(), "t", "c", ".s", func(Link) bool { return true })
	if ua == "" {
		t.Fatal("User-Agent missing")
	}
	if !contains(ua, "at-snapshot/1.0") || !contains(ua, "ops@example.com") {
		t.Errorf("User-Agent = %q; want project + contact", ua)
	}
}

func TestHTTPClientRetriesOn429(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		json.NewEncoder(w).Encode(linksResponse{
			LinkingRecords: []Link{{DID: "did:plc:a", Collection: "x", RKey: "r1"}},
		})
	}))
	defer srv.Close()

	c := NewHTTP(srv.URL, "", 100, 100)
	c.HTTP = srv.Client()
	c.MinBackoff = 1 * time.Millisecond
	c.MaxBackoff = 1 * time.Millisecond

	var got []Link
	err := c.GetBacklinks(context.Background(), "t", "x", ".s", func(l Link) bool {
		got = append(got, l)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Errorf("calls = %d, want 2 (one 429 + one success)", calls.Load())
	}
	if len(got) != 1 {
		t.Errorf("got %d links, want 1", len(got))
	}
}

func TestParseRetryAfter(t *testing.T) {
	if d := parseRetryAfter("12"); d != 12*time.Second {
		t.Errorf("delta-seconds: got %v", d)
	}
	if d := parseRetryAfter(""); d != 0 {
		t.Errorf("empty: got %v", d)
	}
}

func TestFakePagination(t *testing.T) {
	f := NewFake()
	f.Set("did:plc:t", "app.bsky.graph.follow", ".subject", []Link{
		{DID: "did:plc:a", Collection: "app.bsky.graph.follow", RKey: "r1"},
		{DID: "did:plc:b", Collection: "app.bsky.graph.follow", RKey: "r2"},
	})
	var n int
	err := f.GetBacklinks(context.Background(), "did:plc:t", "app.bsky.graph.follow", ".subject", func(Link) bool {
		n++
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("yield called %d times, want 2", n)
	}
}

func TestFakeFailOnce(t *testing.T) {
	f := NewFake()
	f.FailOnce["t"] = true
	err := f.GetBacklinks(context.Background(), "t", "c", ".s", func(Link) bool { return true })
	if err == nil {
		t.Errorf("expected induced failure")
	}
	// Second call: FailOnce was cleared, so this succeeds (with empty data).
	err = f.GetBacklinks(context.Background(), "t", "c", ".s", func(Link) bool { return true })
	if err != nil {
		t.Errorf("second call: %v", err)
	}
}

func contains(s, substr string) bool {
	return len(substr) <= len(s) && stringIndex(s, substr) >= 0
}

func stringIndex(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// keep imports alive in case future tests need them
var _ = strconv.Itoa
