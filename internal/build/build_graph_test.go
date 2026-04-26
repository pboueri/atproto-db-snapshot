package build

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/duckdbstore"
)

// TestRunGraphBackfillIntegration wires a fake plc.directory plus two fake
// PDS hosts via httptest, runs runGraphBackfill end-to-end, and asserts that
// the produced current_graph.duckdb has the expected actors / follows /
// blocks rows and that pds_endpoints was populated. A second invocation
// then verifies resume behavior — already-processed DIDs must not be
// re-fetched.
func TestRunGraphBackfillIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// ---- fake PDS host A: serves did:plc:alice ----
	var pdsAHits int32
	pdsA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&pdsAHits, 1)
		if r.URL.Path != "/xrpc/com.atproto.repo.listRecords" {
			http.NotFound(w, r)
			return
		}
		repo := r.URL.Query().Get("repo")
		coll := r.URL.Query().Get("collection")
		if repo != "did:plc:alice" {
			// Only alice lives here; everything else is a routing mistake.
			writeXRPCError(w, http.StatusBadRequest, "RepoNotFound", "wrong host")
			return
		}
		switch coll {
		case "app.bsky.actor.profile":
			writeListRecords(w, "", []recordEntry{
				{
					URI: "at://did:plc:alice/app.bsky.actor.profile/self",
					Value: rawJSON(map[string]any{
						"displayName": "Alice",
						"description": "alice profile",
						"createdAt":   "2026-01-01T00:00:00Z",
					}),
				},
			})
		case "app.bsky.graph.follow":
			writeListRecords(w, "", []recordEntry{
				{
					URI: "at://did:plc:alice/app.bsky.graph.follow/f1",
					Value: rawJSON(map[string]any{
						"subject":   "did:plc:bob",
						"createdAt": "2026-01-02T00:00:00Z",
					}),
				},
				{
					URI: "at://did:plc:alice/app.bsky.graph.follow/f2",
					Value: rawJSON(map[string]any{
						"subject":   "did:plc:carol",
						"createdAt": "2026-01-03T00:00:00Z",
					}),
				},
			})
		case "app.bsky.graph.block":
			writeListRecords(w, "", []recordEntry{
				{
					URI: "at://did:plc:alice/app.bsky.graph.block/b1",
					Value: rawJSON(map[string]any{
						"subject":   "did:plc:carol",
						"createdAt": "2026-01-04T00:00:00Z",
					}),
				},
			})
		default:
			writeListRecords(w, "", nil)
		}
	}))
	defer pdsA.Close()

	// ---- fake PDS host B: serves did:plc:bob ----
	var pdsBHits int32
	pdsB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&pdsBHits, 1)
		if r.URL.Path != "/xrpc/com.atproto.repo.listRecords" {
			http.NotFound(w, r)
			return
		}
		repo := r.URL.Query().Get("repo")
		coll := r.URL.Query().Get("collection")
		if repo != "did:plc:bob" {
			writeXRPCError(w, http.StatusBadRequest, "RepoNotFound", "wrong host")
			return
		}
		switch coll {
		case "app.bsky.actor.profile":
			writeListRecords(w, "", []recordEntry{
				{
					URI: "at://did:plc:bob/app.bsky.actor.profile/self",
					Value: rawJSON(map[string]any{
						"displayName": "Bob",
						"createdAt":   "2026-01-01T00:00:00Z",
					}),
				},
			})
		case "app.bsky.graph.follow":
			writeListRecords(w, "", []recordEntry{
				{
					URI: "at://did:plc:bob/app.bsky.graph.follow/fb1",
					Value: rawJSON(map[string]any{
						"subject":   "did:plc:alice",
						"createdAt": "2026-01-05T00:00:00Z",
					}),
				},
			})
		case "app.bsky.graph.block":
			// Bob has no blocks — empty page.
			writeListRecords(w, "", nil)
		default:
			writeListRecords(w, "", nil)
		}
	}))
	defer pdsB.Close()

	// ---- fake plc.directory: 2 active DIDs across pdsA/pdsB + 1 tombstone ----
	plcOps := []map[string]any{
		{
			"did":       "did:plc:alice",
			"cid":       "c1",
			"nullified": false,
			"createdAt": "2026-01-01T00:00:00Z",
			"operation": map[string]any{
				"type": "plc_operation",
				"services": map[string]any{
					"atproto_pds": map[string]any{
						"type":     "AtprotoPersonalDataServer",
						"endpoint": pdsA.URL,
					},
				},
			},
		},
		{
			"did":       "did:plc:bob",
			"cid":       "c2",
			"nullified": false,
			"createdAt": "2026-01-02T00:00:00Z",
			"operation": map[string]any{
				"type": "plc_operation",
				"services": map[string]any{
					"atproto_pds": map[string]any{
						"type":     "AtprotoPersonalDataServer",
						"endpoint": pdsB.URL,
					},
				},
			},
		},
		{
			// Tombstoned DID — must NOT end up in pds_endpoints.
			"did":       "did:plc:ghost",
			"cid":       "c3",
			"nullified": false,
			"createdAt": "2026-01-03T00:00:00Z",
			"operation": map[string]any{"type": "plc_tombstone"},
		},
	}
	var plcHits int32
	plcSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/export") {
			http.NotFound(w, r)
			return
		}
		idx := atomic.AddInt32(&plcHits, 1)
		w.Header().Set("Content-Type", "application/jsonl")
		if idx > 1 {
			// page 2 → empty → end of stream
			return
		}
		for _, op := range plcOps {
			b, _ := json.Marshal(op)
			fmt.Fprintln(w, string(b))
		}
	}))
	defer plcSrv.Close()

	tmp := t.TempDir()
	cfg := config.Default()
	cfg.DataDir = tmp
	cfg.HTTPTimeout = 5 * time.Second
	cfg.DIDLimit = 0 // no cap
	cfg.PLC.Endpoint = plcSrv.URL
	cfg.PLC.RPS = 1000
	cfg.PLC.PageSize = 10
	cfg.PLC.RefreshDays = 0 // never skip enumeration
	cfg.PDS.PerHostWorkers = 2
	cfg.PDS.PerHostRPS = 1000
	cfg.PDS.HTTPTimeout = 5 * time.Second
	cfg.PDS.MaxRetries = 1
	cfg.PDS.BreakerThreshold = 5
	cfg.PDS.BreakerCooldown = time.Second
	cfg.Constellation.Enabled = false

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	out, err := runGraphBackfill(ctx, cfg, logger)
	if err != nil {
		t.Fatalf("runGraphBackfill: %v", err)
	}
	if out != filepath.Join(tmp, "current_graph.duckdb") {
		t.Errorf("output path = %q; want %q", out, filepath.Join(tmp, "current_graph.duckdb"))
	}

	// ---- assert DB contents ----
	store, err := duckdbstore.Open(out, "1GB", 2)
	if err != nil {
		t.Fatalf("reopen duckdb: %v", err)
	}
	defer store.Close()

	// pds_endpoints: alice → pdsA, bob → pdsB. Ghost is tombstoned and
	// must be absent.
	endpoints, err := store.LoadPDSEndpoints()
	if err != nil {
		t.Fatalf("LoadPDSEndpoints: %v", err)
	}
	if got := endpoints["did:plc:alice"]; got != pdsA.URL {
		t.Errorf("endpoints[alice] = %q; want %q", got, pdsA.URL)
	}
	if got := endpoints["did:plc:bob"]; got != pdsB.URL {
		t.Errorf("endpoints[bob] = %q; want %q", got, pdsB.URL)
	}
	if _, ok := endpoints["did:plc:ghost"]; ok {
		t.Errorf("endpoints[ghost] present; tombstone should have removed it")
	}

	// actors: 3 total — alice (processed), bob (processed), carol
	// (target-only via materialize).
	if n := scalarInt(t, store, `SELECT count(*) FROM actors`); n != 3 {
		t.Errorf("actors count = %d; want 3", n)
	}
	if n := scalarInt(t, store, `SELECT count(*) FROM actors WHERE repo_processed = TRUE`); n != 2 {
		t.Errorf("processed actors = %d; want 2", n)
	}

	// follows_current: alice→bob, alice→carol, bob→alice = 3.
	if n := scalarInt(t, store, `SELECT count(*) FROM follows_current`); n != 3 {
		t.Errorf("follows_current = %d; want 3", n)
	}
	// blocks_current: alice→carol = 1.
	if n := scalarInt(t, store, `SELECT count(*) FROM blocks_current`); n != 1 {
		t.Errorf("blocks_current = %d; want 1", n)
	}

	// processed dids returned by the resume helper.
	processed, err := store.LoadProcessedDIDs()
	if err != nil {
		t.Fatalf("LoadProcessedDIDs: %v", err)
	}
	if len(processed) != 2 {
		t.Errorf("processed dids = %d; want 2", len(processed))
	}
	if _, ok := processed["did:plc:alice"]; !ok {
		t.Errorf("processed missing alice")
	}
	if _, ok := processed["did:plc:bob"]; !ok {
		t.Errorf("processed missing bob")
	}

	// Reset PDS hit counters to verify resume.
	atomic.StoreInt32(&pdsAHits, 0)
	atomic.StoreInt32(&pdsBHits, 0)
	atomic.StoreInt32(&plcHits, 0)

	// We must close the store before re-running — runGraphBackfill opens
	// its own connection.
	store.Close()

	// ---- second run: resume should skip already-processed DIDs ----
	// Force PLC enumeration to skip via RefreshDays so we can also verify
	// the cursor/cache short-circuit.
	cfg.PLC.RefreshDays = 30
	if _, err := runGraphBackfill(ctx, cfg, logger); err != nil {
		t.Fatalf("runGraphBackfill (resume): %v", err)
	}
	// PLC should NOT have been hit at all during resume.
	if h := atomic.LoadInt32(&plcHits); h != 0 {
		t.Errorf("plc hits on resume = %d; want 0 (RefreshDays should short-circuit)", h)
	}
	// Neither PDS should have been hit — every DID is already processed.
	if h := atomic.LoadInt32(&pdsAHits); h != 0 {
		t.Errorf("pdsA hits on resume = %d; want 0", h)
	}
	if h := atomic.LoadInt32(&pdsBHits); h != 0 {
		t.Errorf("pdsB hits on resume = %d; want 0", h)
	}

	// Reopen and verify counts are unchanged.
	store2, err := duckdbstore.Open(out, "1GB", 2)
	if err != nil {
		t.Fatalf("reopen duckdb after resume: %v", err)
	}
	defer store2.Close()
	if n := scalarInt(t, store2, `SELECT count(*) FROM follows_current`); n != 3 {
		t.Errorf("post-resume follows_current = %d; want 3", n)
	}
	if n := scalarInt(t, store2, `SELECT count(*) FROM blocks_current`); n != 1 {
		t.Errorf("post-resume blocks_current = %d; want 1", n)
	}
	if n := scalarInt(t, store2, `SELECT count(*) FROM actors`); n != 3 {
		t.Errorf("post-resume actors = %d; want 3", n)
	}
}

// ---------- httptest helpers ----------

type recordEntry struct {
	URI   string          `json:"uri"`
	CID   string          `json:"cid"`
	Value json.RawMessage `json:"value"`
}

func writeListRecords(w http.ResponseWriter, cursor string, recs []recordEntry) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Cursor  string        `json:"cursor"`
		Records []recordEntry `json:"records"`
	}{Cursor: cursor, Records: recs})
}

func writeXRPCError(w http.ResponseWriter, code int, name, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error":   name,
		"message": msg,
	})
}

func rawJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return json.RawMessage(b)
}

func scalarInt(t *testing.T, s *duckdbstore.Store, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := s.DB.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("scalar %q: %v", query, err)
	}
	return n
}
