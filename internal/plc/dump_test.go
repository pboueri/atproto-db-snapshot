package plc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestStreamPagination(t *testing.T) {
	pages := [][]map[string]any{
		// page 1: did:a (creates pds endpoint), did:b (creates pds endpoint)
		{
			{
				"did": "did:plc:a", "cid": "c1", "nullified": false,
				"createdAt": "2026-01-01T00:00:00Z",
				"operation": map[string]any{
					"type": "plc_operation",
					"services": map[string]any{
						"atproto_pds": map[string]any{"type": "AtprotoPersonalDataServer", "endpoint": "https://pds-1.example"},
					},
				},
			},
			{
				"did": "did:plc:b", "cid": "c2", "nullified": false,
				"createdAt": "2026-01-02T00:00:00Z",
				"operation": map[string]any{
					"type": "plc_operation",
					"services": map[string]any{
						"atproto_pds": map[string]any{"type": "AtprotoPersonalDataServer", "endpoint": "https://pds-2.example"},
					},
				},
			},
		},
		// page 2: did:a moves to a new pds; did:c tombstoned; nullified op skipped
		{
			{
				"did": "did:plc:a", "cid": "c3", "nullified": false,
				"createdAt": "2026-01-03T00:00:00Z",
				"operation": map[string]any{
					"type": "plc_operation",
					"services": map[string]any{
						"atproto_pds": map[string]any{"type": "AtprotoPersonalDataServer", "endpoint": "https://pds-1b.example"},
					},
				},
			},
			{
				"did": "did:plc:c", "cid": "c4", "nullified": false,
				"createdAt": "2026-01-04T00:00:00Z",
				"operation": map[string]any{"type": "plc_tombstone"},
			},
			{
				"did": "did:plc:a", "cid": "c5", "nullified": true,
				"createdAt": "2026-01-05T00:00:00Z",
				"operation": map[string]any{
					"type": "plc_operation",
					"services": map[string]any{
						"atproto_pds": map[string]any{"type": "AtprotoPersonalDataServer", "endpoint": "https://nullified.example"},
					},
				},
			},
		},
	}

	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/export") {
			http.NotFound(w, r)
			return
		}
		idx := atomic.AddInt32(&hits, 1) - 1
		w.Header().Set("Content-Type", "application/jsonl")
		if int(idx) >= len(pages) {
			// empty page → done
			return
		}
		for _, op := range pages[idx] {
			b, _ := json.Marshal(op)
			fmt.Fprintln(w, string(b))
		}
	}))
	defer srv.Close()

	c := New(srv.URL, 1000, 5*time.Second)
	c.PageSize = 10

	var ops []Op
	cur, err := c.Stream(context.Background(), "", func(o Op) error {
		ops = append(ops, o)
		return nil
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	// 2 ops on page 1, 2 ops (one tombstone) on page 2 (nullified is filtered out).
	if got := len(ops); got != 4 {
		t.Fatalf("ops len = %d; want 4", got)
	}
	// Replay last-write-wins per DID.
	endpoints := map[string]string{}
	tomb := map[string]bool{}
	for _, op := range ops {
		if op.Tombstone {
			tomb[op.DID] = true
			delete(endpoints, op.DID)
			continue
		}
		endpoints[op.DID] = op.Endpoint
	}
	if endpoints["did:plc:a"] != "https://pds-1b.example" {
		t.Fatalf("did:plc:a endpoint = %q; want pds-1b", endpoints["did:plc:a"])
	}
	if endpoints["did:plc:b"] != "https://pds-2.example" {
		t.Fatalf("did:plc:b endpoint = %q; want pds-2", endpoints["did:plc:b"])
	}
	if !tomb["did:plc:c"] {
		t.Fatalf("did:plc:c not marked tombstoned")
	}
	if cur == "" {
		t.Fatalf("expected non-empty cursor after stream; got empty")
	}

	// hits should be 3: page 1, page 2, empty page that terminates.
	if h := atomic.LoadInt32(&hits); h != 3 {
		t.Fatalf("server hits = %d; want 3", h)
	}
}

func TestStreamRespectsResumeCursor(t *testing.T) {
	var seenAfter string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenAfter = r.URL.Query().Get("after")
		// always reply with empty page → terminates immediately
	}))
	defer srv.Close()

	c := New(srv.URL, 1000, 5*time.Second)
	if _, err := c.Stream(context.Background(), "2026-04-01T00:00:00Z", func(Op) error { return nil }); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if seenAfter != "2026-04-01T00:00:00Z" {
		t.Fatalf("server saw after=%q; want resume cursor", seenAfter)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "plc_cursor.json")
	if err := SaveCursorAtomic(path, CursorState{Cursor: "abc-123"}); err != nil {
		t.Fatalf("SaveCursorAtomic: %v", err)
	}
	got, err := LoadCursor(path)
	if err != nil {
		t.Fatalf("LoadCursor: %v", err)
	}
	if got.Cursor != "abc-123" {
		t.Fatalf("Cursor = %q; want abc-123", got.Cursor)
	}
	if got.SchemaVersion != "v1" {
		t.Fatalf("SchemaVersion = %q; want v1", got.SchemaVersion)
	}
	if got.UpdatedAt.IsZero() {
		t.Fatalf("UpdatedAt is zero; expected populated")
	}
}

func TestLoadCursorMissingReturnsZero(t *testing.T) {
	got, err := LoadCursor(filepath.Join(t.TempDir(), "missing.json"))
	if err != nil {
		t.Fatalf("LoadCursor: %v", err)
	}
	if got.Cursor != "" {
		t.Fatalf("Cursor = %q; want empty", got.Cursor)
	}
	if got.SchemaVersion != "v1" {
		t.Fatalf("SchemaVersion = %q; want v1", got.SchemaVersion)
	}
}
