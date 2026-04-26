package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestListReposPagination verifies ListRepos walks paginated cursors until
// the server returns an empty cursor, passing each page to the callback.
func TestListReposPagination(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/xrpc/com.atproto.sync.listRepos") {
			http.NotFound(w, r)
			return
		}
		atomic.AddInt32(&hits, 1)
		cursor := r.URL.Query().Get("cursor")
		var resp listReposResp
		switch cursor {
		case "":
			resp = listReposResp{
				Cursor: "page2",
				Repos: []ListReposItem{
					{DID: "did:plc:a", Active: true},
					{DID: "did:plc:b", Active: true},
				},
			}
		case "page2":
			resp = listReposResp{
				Cursor: "page3",
				Repos:  []ListReposItem{{DID: "did:plc:c", Active: true}},
			}
		case "page3":
			resp = listReposResp{Cursor: "", Repos: []ListReposItem{{DID: "did:plc:d", Active: true}}}
		default:
			http.Error(w, "unexpected cursor", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	c := New(srv.URL, 1000, 5*time.Second)
	var all []ListReposItem
	err := c.ListRepos(context.Background(), 50, func(batch []ListReposItem) error {
		all = append(all, batch...)
		return nil
	})
	if err != nil {
		t.Fatalf("ListRepos: %v", err)
	}
	if got, want := len(all), 4; got != want {
		t.Fatalf("total repos = %d; want %d", got, want)
	}
	if atomic.LoadInt32(&hits) != 3 {
		t.Fatalf("server hit count = %d; want 3", hits)
	}
	wantDIDs := []string{"did:plc:a", "did:plc:b", "did:plc:c", "did:plc:d"}
	for i, d := range wantDIDs {
		if all[i].DID != d {
			t.Fatalf("all[%d].DID = %q; want %q", i, all[i].DID, d)
		}
	}
}

// TestListReposCallbackErrorHalts checks that a cb returning an error stops pagination.
func TestListReposCallbackErrorHalts(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(listReposResp{
			Cursor: "never-reached",
			Repos:  []ListReposItem{{DID: "did:plc:x"}},
		})
	}))
	defer srv.Close()

	c := New(srv.URL, 1000, 5*time.Second)
	sentinel := errors.New("stop")
	err := c.ListRepos(context.Background(), 10, func(_ []ListReposItem) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("ListRepos err = %v; want sentinel", err)
	}
}

// TestGetRepoNotFound verifies 404 → ErrNotFound.
func TestGetRepoNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "gone", http.StatusNotFound)
	}))
	defer srv.Close()

	c := New(srv.URL, 1000, 2*time.Second)
	_, err := c.GetRepo(context.Background(), "did:plc:absent")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetRepo err = %v; want ErrNotFound", err)
	}
}

// TestGetRepoReturns410AsNotFound covers the takendown / deactivated case.
func TestGetRepoReturns410AsNotFound(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusGone} {
		t.Run(fmt.Sprintf("status-%d", status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "nope", status)
			}))
			defer srv.Close()
			c := New(srv.URL, 1000, 2*time.Second)
			_, err := c.GetRepo(context.Background(), "did:plc:absent")
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("GetRepo(%d) err = %v; want ErrNotFound", status, err)
			}
		})
	}
}

// TestDoWithRetryOn429 verifies that 429 Retry-After is respected and the
// request is eventually retried successfully.
func TestDoWithRetryOn429(t *testing.T) {
	var hits int32
	start := time.Now()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&hits, 1)
		if n == 1 {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
		// Keep the body minimal to satisfy GetRepo.
		_, _ = w.Write([]byte("BYTES"))
	}))
	defer srv.Close()

	c := New(srv.URL, 1000, 5*time.Second)
	body, err := c.GetRepo(context.Background(), "did:plc:retry")
	if err != nil {
		t.Fatalf("GetRepo: %v", err)
	}
	if string(body) != "BYTES" {
		t.Fatalf("body = %q; want BYTES", body)
	}
	if elapsed := time.Since(start); elapsed < 800*time.Millisecond {
		t.Fatalf("Retry-After not honored: elapsed %v < 1s", elapsed)
	}
	if hits != 2 {
		t.Fatalf("hit count = %d; want 2", hits)
	}
}

// TestRateLimiterWaits verifies that setting a very low rps value serializes
// calls. Two GetRepo calls at 2 rps should take ~0.5s.
func TestRateLimiterWaits(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	c := New(srv.URL, 2, 5*time.Second) // 2 rps, burst = 3
	// Drain the burst so the rate limit actually stalls subsequent requests.
	for i := 0; i < 3; i++ {
		if _, err := c.GetRepo(context.Background(), "did:plc:x"); err != nil {
			t.Fatalf("warm-up GetRepo: %v", err)
		}
	}
	// Next two requests must wait at ~500ms each (rate.Limit(2) = 1 token / 500ms).
	start := time.Now()
	for i := 0; i < 2; i++ {
		if _, err := c.GetRepo(context.Background(), "did:plc:x"); err != nil {
			t.Fatalf("GetRepo: %v", err)
		}
	}
	elapsed := time.Since(start)
	if elapsed < 400*time.Millisecond {
		t.Fatalf("rate limiter did not wait; elapsed %v", elapsed)
	}
}
