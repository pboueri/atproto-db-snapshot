package constellation

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCountsDecode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("target") != "did:plc:abc" {
			http.Error(w, "missing target", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"app.bsky.graph.follow": {"subject": 42},
			"app.bsky.graph.block":  {"subject": 3},
			"app.bsky.feed.like":    {"subject": 100, "subject.uri": 1},
			"app.bsky.feed.repost":  {"subject": 7}
		}`))
	}))
	defer srv.Close()

	c := New(srv.URL, 1000, 5*time.Second)
	got, err := c.Counts(context.Background(), "did:plc:abc")
	if err != nil {
		t.Fatalf("Counts: %v", err)
	}
	want := Counts{FollowerCount: 42, BlocksReceived: 3, LikesReceived: 100, RepostsReceived: 7}
	if got != want {
		t.Fatalf("got %+v; want %+v", got, want)
	}
}

func TestCounts4xxReturnsZero(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no such did", http.StatusNotFound)
	}))
	defer srv.Close()

	c := New(srv.URL, 1000, 5*time.Second)
	got, err := c.Counts(context.Background(), "did:plc:abc")
	if err != nil {
		t.Fatalf("Counts: %v", err)
	}
	if (got != Counts{}) {
		t.Fatalf("got %+v; want zero", got)
	}
}

func TestCounts5xxErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := New(srv.URL, 1000, 5*time.Second)
	_, err := c.Counts(context.Background(), "did:plc:abc")
	if err == nil {
		t.Fatalf("expected error on 5xx; got nil")
	}
}
