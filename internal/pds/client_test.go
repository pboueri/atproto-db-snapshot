package pds

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pboueri/atproto-db-snapshot/internal/parse"
)

func TestListRecordsPagination(t *testing.T) {
	page1 := listRecordsResp{
		Cursor: "cur2",
		Records: []listRecordsEntry{
			{URI: "at://did:plc:a/app.bsky.graph.follow/r1", Value: json.RawMessage(`{"subject":"did:plc:b","createdAt":"2026-01-01T00:00:00Z"}`)},
			{URI: "at://did:plc:a/app.bsky.graph.follow/r2", Value: json.RawMessage(`{"subject":"did:plc:c","createdAt":"2026-01-01T00:00:00Z"}`)},
		},
	}
	page2 := listRecordsResp{
		Cursor: "",
		Records: []listRecordsEntry{
			{URI: "at://did:plc:a/app.bsky.graph.follow/r3", Value: json.RawMessage(`{"subject":"did:plc:d","createdAt":"2026-01-01T00:00:00Z"}`)},
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/xrpc/com.atproto.repo.listRecords" {
			http.NotFound(w, r)
			return
		}
		var resp listRecordsResp
		if r.URL.Query().Get("cursor") == "" {
			resp = page1
		} else {
			resp = page2
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	c := New(srv.URL, Options{RPS: 1000, HTTPTimeout: 5 * time.Second})
	var got []string
	err := c.ListRecords(context.Background(), "did:plc:a", "app.bsky.graph.follow", func(value json.RawMessage, rkey string) error {
		got = append(got, rkey)
		return nil
	})
	if err != nil {
		t.Fatalf("ListRecords: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d records; want 3", len(got))
	}
	want := []string{"r1", "r2", "r3"}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("got[%d] = %q; want %q", i, got[i], w)
		}
	}
}

func TestListRecords429Retry(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&hits, 1)
		if n == 1 {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		_ = json.NewEncoder(w).Encode(listRecordsResp{
			Cursor: "",
			Records: []listRecordsEntry{
				{URI: "at://did:plc:a/app.bsky.graph.follow/r1", Value: json.RawMessage(`{"subject":"did:plc:b"}`)},
			},
		})
	}))
	defer srv.Close()

	start := time.Now()
	c := New(srv.URL, Options{RPS: 1000, HTTPTimeout: 5 * time.Second})
	var got int
	err := c.ListRecords(context.Background(), "did:plc:a", "app.bsky.graph.follow", func(json.RawMessage, string) error {
		got++
		return nil
	})
	if err != nil {
		t.Fatalf("ListRecords: %v", err)
	}
	if got != 1 {
		t.Fatalf("got %d; want 1", got)
	}
	if elapsed := time.Since(start); elapsed < 800*time.Millisecond {
		t.Fatalf("Retry-After not honored: elapsed %v < 1s", elapsed)
	}
	if h := atomic.LoadInt32(&hits); h != 2 {
		t.Fatalf("hits = %d; want 2", h)
	}
}

func TestListRecordsRepoNotFoundSkips(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"RepoNotFound","message":"Could not find repo: did:plc:abc"}`))
	}))
	defer srv.Close()

	c := New(srv.URL, Options{RPS: 1000, HTTPTimeout: 5 * time.Second})
	err := c.ListRecords(context.Background(), "did:plc:abc", "app.bsky.graph.follow", func(json.RawMessage, string) error { return nil })
	if !errors.Is(err, ErrSkip) {
		t.Fatalf("err = %v; want ErrSkip", err)
	}
}

func TestListRecords404Skips(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "gone", http.StatusNotFound)
	}))
	defer srv.Close()

	c := New(srv.URL, Options{RPS: 1000, HTTPTimeout: 5 * time.Second})
	err := c.ListRecords(context.Background(), "did:plc:abc", "app.bsky.graph.follow", func(json.RawMessage, string) error { return nil })
	if !errors.Is(err, ErrSkip) {
		t.Fatalf("err = %v; want ErrSkip", err)
	}
}

func TestCircuitBreakerTrips(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	// 1 max retry per call so each call burns 2 attempts.
	c := New(srv.URL, Options{
		RPS:              1000,
		HTTPTimeout:      2 * time.Second,
		MaxRetries:       1,
		BreakerThreshold: 5,
		BreakerCooldown:  time.Hour, // long enough that the breaker stays open
	})

	// 5 consecutive failures → breaker should open. We need 5 calls because
	// recordFailure increments on every 5xx response (each call records >=1).
	for i := 0; i < 5; i++ {
		_ = c.ListRecords(context.Background(), "did:plc:x", "app.bsky.graph.follow", func(json.RawMessage, string) error { return nil })
	}
	if !c.CircuitOpen() {
		t.Fatalf("expected breaker to be open after %d failures", 5)
	}
	hitsBefore := atomic.LoadInt32(&hits)
	err := c.ListRecords(context.Background(), "did:plc:x", "app.bsky.graph.follow", func(json.RawMessage, string) error { return nil })
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("err = %v; want ErrCircuitOpen", err)
	}
	if hitsAfter := atomic.LoadInt32(&hits); hitsAfter != hitsBefore {
		t.Fatalf("hit server while breaker open: before=%d after=%d", hitsBefore, hitsAfter)
	}
}

func TestDecodeFollow(t *testing.T) {
	val := json.RawMessage(`{"subject":"did:plc:b","createdAt":"2026-04-24T12:00:00Z"}`)
	got, err := Decode(val, "app.bsky.graph.follow")
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	rec, ok := got.(parse.FollowRec)
	if !ok {
		t.Fatalf("Decode returned %T; want parse.FollowRec", got)
	}
	if rec.TargetDID != "did:plc:b" {
		t.Fatalf("TargetDID = %q; want did:plc:b", rec.TargetDID)
	}
	if rec.CreatedAt.Year() != 2026 {
		t.Fatalf("CreatedAt year = %d; want 2026", rec.CreatedAt.Year())
	}
}

func TestDecodeBlock(t *testing.T) {
	val := json.RawMessage(`{"subject":"did:plc:c","createdAt":"2026-04-24T12:00:00Z"}`)
	got, err := Decode(val, "app.bsky.graph.block")
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	rec, ok := got.(parse.BlockRec)
	if !ok {
		t.Fatalf("Decode returned %T; want parse.BlockRec", got)
	}
	if rec.TargetDID != "did:plc:c" {
		t.Fatalf("TargetDID = %q; want did:plc:c", rec.TargetDID)
	}
}

func TestDecodeProfile(t *testing.T) {
	val := json.RawMessage(`{"displayName":"Alice","description":"hi","createdAt":"2025-01-01T00:00:00Z","avatar":{"ref":{"$link":"bafyabc"}}}`)
	got, err := Decode(val, "app.bsky.actor.profile")
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	rec, ok := got.(*parse.ProfileRec)
	if !ok {
		t.Fatalf("Decode returned %T; want *parse.ProfileRec", got)
	}
	if rec.DisplayName == nil || *rec.DisplayName != "Alice" {
		t.Fatalf("DisplayName = %v; want Alice", rec.DisplayName)
	}
	if rec.Description == nil || *rec.Description != "hi" {
		t.Fatalf("Description = %v; want hi", rec.Description)
	}
	if rec.AvatarCID == nil || *rec.AvatarCID != "bafyabc" {
		t.Fatalf("AvatarCID = %v; want bafyabc", rec.AvatarCID)
	}
	if rec.CreatedAt == nil || rec.CreatedAt.Year() != 2025 {
		t.Fatalf("CreatedAt = %v; want 2025", rec.CreatedAt)
	}
}

func TestDecodeUnknownReturnsNil(t *testing.T) {
	got, err := Decode(json.RawMessage(`{}`), "app.bsky.feed.post")
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != nil {
		t.Fatalf("Decode unknown = %v; want nil", got)
	}
}
