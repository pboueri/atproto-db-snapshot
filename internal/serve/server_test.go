package serve

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestDashboard_RendersWithoutPanics spins up the server on :0, asks for
// every documented endpoint with a fully populated DataDir, and asserts
// that each handler returns a 200 with reasonable content.
//
// We seed:
//   - cursor.json    — for /api/jetstream + the firehose card
//   - latest.json    — for /api/build + the build card
//   - logs/run.log   — for /api/errors + events/sec
//   - daily/<date>/  — for /api/retention
//
// We deliberately omit current_graph.duckdb and staging.db so the server
// has to render `—` for those without crashing.
func TestDashboard_RendersWithoutPanics(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "cursor.json"), `{
  "cursor": 1714095582123456,
  "endpoint": "wss://jetstream2.us-east.bsky.network/subscribe",
  "updated_at": "2026-04-26T00:14:37.208Z",
  "schema_version": "v1"
}`)
	mustWrite(t, filepath.Join(dir, "latest.json"), `{
  "schema_version": "v1",
  "built_at": "2026-04-26T04:18:00Z",
  "build_mode": "incremental",
  "build_duration_seconds": 1082,
  "row_counts": {
    "actors": 35102443,
    "posts": 1412998552
  },
  "jetstream_cursor": 1714095582123456
}`)
	logsDir := filepath.Join(dir, "logs")
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		t.Fatalf("mkdir logs: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	mustWrite(t, filepath.Join(logsDir, "run.log"),
		`{"time":"`+now+`","level":"INFO","msg":"event","collection":"app.bsky.feed.post"}`+"\n"+
			`{"time":"`+now+`","level":"INFO","msg":"event","collection":"app.bsky.feed.like"}`+"\n"+
			`{"time":"`+now+`","level":"WARN","msg":"cursor rewind 42"}`+"\n"+
			`{"time":"`+now+`","level":"ERROR","msg":"getRepo 429 (did:plc:abc)"}`+"\n",
	)
	mustWrite(t, filepath.Join(logsDir, "build.log"),
		`{"time":"`+now+`","level":"INFO","msg":"build start","mode":"incremental"}`+"\n",
	)
	dayDir := filepath.Join(dir, "daily", "2026-04-25")
	if err := os.MkdirAll(dayDir, 0o755); err != nil {
		t.Fatalf("mkdir daily: %v", err)
	}
	mustWrite(t, filepath.Join(dayDir, "posts.parquet"), strings.Repeat("x", 4096))

	srv := newServer(Options{DataDir: dir, RefreshSeconds: 5, LogTailLines: 1000}, nil)
	ts := httptest.NewServer(srv.routes())
	t.Cleanup(ts.Close)

	t.Run("dashboard root", func(t *testing.T) {
		body := mustGET(t, ts.URL+"/")
		if !strings.Contains(body, "at-snapshotter health") {
			t.Fatalf("dashboard missing title: %.200q", body)
		}
		if !strings.Contains(body, "Firehose") {
			t.Fatalf("dashboard missing Firehose card")
		}
	})

	t.Run("api/summary", func(t *testing.T) {
		body := mustGET(t, ts.URL+"/api/summary")
		var sum summary
		if err := json.Unmarshal([]byte(body), &sum); err != nil {
			t.Fatalf("unmarshal summary: %v\nbody=%s", err, body)
		}
		if sum.Jetstream == nil {
			t.Fatalf("expected jetstream card, got nil")
		}
		if sum.Jetstream.Cursor != 1714095582123456 {
			t.Fatalf("cursor: got %d", sum.Jetstream.Cursor)
		}
		if sum.LastBuild == nil || sum.LastBuild.Mode != "incremental" {
			t.Fatalf("build card: %+v", sum.LastBuild)
		}
		if sum.Retention == nil || sum.Retention.Days != 1 {
			t.Fatalf("retention card: %+v", sum.Retention)
		}
		if sum.Disk == nil || sum.Disk.TotalBytes == 0 {
			t.Fatalf("disk card: %+v", sum.Disk)
		}
		if sum.Errors == nil || sum.Errors.ErrorCount < 1 || sum.Errors.WarnCount < 1 {
			t.Fatalf("errors card: %+v", sum.Errors)
		}
	})

	t.Run("api/jetstream", func(t *testing.T) {
		body := mustGET(t, ts.URL+"/api/jetstream")
		if !strings.Contains(body, `"cursor":`) {
			t.Fatalf("api/jetstream: %s", body)
		}
	})
	t.Run("api/build", func(t *testing.T) {
		body := mustGET(t, ts.URL+"/api/build")
		if !strings.Contains(body, `"mode": "incremental"`) {
			t.Fatalf("api/build: %s", body)
		}
	})
	t.Run("api/disk", func(t *testing.T) {
		body := mustGET(t, ts.URL+"/api/disk")
		if !strings.Contains(body, `"free_bytes":`) {
			t.Fatalf("api/disk: %s", body)
		}
	})
	t.Run("api/retention", func(t *testing.T) {
		body := mustGET(t, ts.URL+"/api/retention")
		if !strings.Contains(body, `"2026-04-25"`) {
			t.Fatalf("api/retention: %s", body)
		}
	})
	t.Run("api/errors", func(t *testing.T) {
		body := mustGET(t, ts.URL+"/api/errors?since=1h")
		if !strings.Contains(body, `"error_count":`) {
			t.Fatalf("api/errors: %s", body)
		}
	})
	t.Run("metrics", func(t *testing.T) {
		body := mustGET(t, ts.URL+"/metrics")
		want := []string{
			"at_snapshotter_jetstream_cursor_us",
			"at_snapshotter_disk_free_bytes",
			"at_snapshotter_retention_local_days",
			"at_snapshotter_errors_total",
		}
		for _, m := range want {
			if !strings.Contains(body, m) {
				t.Fatalf("metrics missing %s:\n%s", m, body)
			}
		}
	})
}

// TestRun_ShutdownOnContextCancel verifies that Run returns cleanly when
// its context is canceled.
func TestRun_ShutdownOnContextCancel(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{Listen: "127.0.0.1:0", DataDir: dir}, nil)
	}()
	// Give Run a moment to bind.
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned err: %v", err)
		}
	case <-time.After(8 * time.Second):
		t.Fatalf("Run did not exit after ctx cancel")
	}
}

// ---- helpers ----

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir for %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func mustGET(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("GET %s: status %d body=%s", url, resp.StatusCode, body)
	}
	return string(body)
}
