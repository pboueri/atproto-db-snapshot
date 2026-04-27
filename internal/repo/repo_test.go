package repo

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/pboueri/atproto-db-snapshot/internal/model"
)

func TestHTTPListRecordsPaginates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Path; got != "/xrpc/com.atproto.repo.listRecords" {
			http.Error(w, "bad path", 404)
			return
		}
		q, _ := url.ParseQuery(r.URL.RawQuery)
		if q.Get("repo") != "did:plc:abc" {
			http.Error(w, "bad repo", 400)
			return
		}
		page := q.Get("cursor")
		w.Header().Set("Content-Type", "application/json")
		switch page {
		case "":
			json.NewEncoder(w).Encode(listRecordsResp{
				Cursor: "p2",
				Records: []Record{
					{URI: "at://did:plc:abc/app.bsky.graph.follow/" + strconv.Itoa(1), CID: "c1", Value: json.RawMessage(`{}`)},
					{URI: "at://did:plc:abc/app.bsky.graph.follow/" + strconv.Itoa(2), CID: "c2", Value: json.RawMessage(`{}`)},
				},
			})
		case "p2":
			json.NewEncoder(w).Encode(listRecordsResp{
				Records: []Record{
					{URI: "at://did:plc:abc/app.bsky.graph.follow/3", CID: "c3", Value: json.RawMessage(`{}`)},
				},
			})
		}
	}))
	defer srv.Close()

	c := &HTTPClient{HTTP: srv.Client(), PageSize: 2}
	got, err := c.ListRecords(context.Background(), srv.URL, "did:plc:abc", model.CollectionFollow)
	if err != nil {
		t.Fatalf("ListRecords: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("len = %d, want 3", len(got))
	}
	for _, r := range got {
		if r.DID != "did:plc:abc" {
			t.Errorf("DID not populated: %+v", r)
		}
		if r.Collection != model.CollectionFollow {
			t.Errorf("Collection not populated: %+v", r)
		}
		if r.RKey == "" {
			t.Errorf("RKey not populated: %+v", r)
		}
	}
}

func TestHTTPListRecords404IsEmpty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "deactivated", http.StatusNotFound)
	}))
	defer srv.Close()
	c := &HTTPClient{HTTP: srv.Client()}
	got, err := c.ListRecords(context.Background(), srv.URL, "did:plc:gone", model.CollectionFollow)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if len(got) != 0 {
		t.Errorf("len = %d, want 0", len(got))
	}
}

func TestHTTPListRecordsEmptyPDS(t *testing.T) {
	c := &HTTPClient{}
	got, err := c.ListRecords(context.Background(), "", "did:plc:x", model.CollectionFollow)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if got != nil {
		t.Errorf("len = %d, want nil (empty pds short-circuits)", len(got))
	}
}

func TestHTTPRetriesOn429HonoringRetryAfter(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls < 3 {
			w.Header().Set("Retry-After", "0") // immediate retry, just exercise the code path
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		json.NewEncoder(w).Encode(listRecordsResp{
			Records: []Record{{URI: "at://x/app.bsky.graph.follow/r1", Value: json.RawMessage(`{}`)}},
		})
	}))
	defer srv.Close()

	c := &HTTPClient{HTTP: srv.Client(), MaxRetries: 5, MinBackoff: time.Millisecond}
	got, err := c.ListRecords(context.Background(), srv.URL, "did:plc:x", model.CollectionFollow)
	if err != nil {
		t.Fatalf("ListRecords: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("len = %d, want 1", len(got))
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3 (two 429s then 200)", calls)
	}
}

func TestHTTPRetriesGiveUpEventually(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "still rate limited", http.StatusTooManyRequests)
	}))
	defer srv.Close()
	c := &HTTPClient{HTTP: srv.Client(), MaxRetries: 2, MinBackoff: time.Millisecond}
	_, err := c.ListRecords(context.Background(), srv.URL, "did:plc:x", model.CollectionFollow)
	if err == nil {
		t.Errorf("expected error after exhausting retries")
	}
}

func TestFakeFailOnce(t *testing.T) {
	f := NewFake()
	f.FailOnce["did:plc:x"] = true
	f.Set("did:plc:x", model.CollectionProfile, []Record{{URI: "at://did:plc:x/app.bsky.actor.profile/self"}})

	if _, err := f.ListRecords(context.Background(), "irrelevant", "did:plc:x", model.CollectionProfile); err == nil {
		t.Fatalf("expected first call to fail")
	}
	got, err := f.ListRecords(context.Background(), "irrelevant", "did:plc:x", model.CollectionProfile)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("len = %d, want 1", len(got))
	}
}
