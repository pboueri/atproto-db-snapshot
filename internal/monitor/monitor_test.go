package monitor_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/pboueri/atproto-db-snapshot/internal/bootstrap"
	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/monitor"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
)

// newTestServer builds a hermetic monitor handler against a fresh memory
// objstore and tmp data dir. Tests then layer in fixtures as needed.
func newTestServer(t *testing.T) (*httptest.Server, config.Config, objstore.Store) {
	t.Helper()
	dataDir := t.TempDir()
	obj := objstore.NewMemory()
	cfg := config.Config{
		DataDir:      dataDir,
		MonitorAddr:  "127.0.0.1:0",
		LogLevel:     "info",
		LookbackDays: 30,
	}
	srv := httptest.NewServer(monitor.NewHandler(cfg, monitor.Deps{ObjStore: obj}))
	t.Cleanup(srv.Close)
	return srv, cfg, obj
}

func decodeJSON[T any](t *testing.T, body io.ReadCloser) T {
	t.Helper()
	defer body.Close()
	var out T
	if err := json.NewDecoder(body).Decode(&out); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	return out
}

func TestHealthz(t *testing.T) {
	srv, _, _ := newTestServer(t)

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("get /healthz: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	body := decodeJSON[map[string]bool](t, resp.Body)
	if !body["ok"] {
		t.Errorf("body = %v, want {ok:true}", body)
	}
}

// TestStatusEmpty verifies that a freshly-installed monitor (no DB, no
// objstore data) emits all three sections without crashing and reports
// found=false where appropriate.
func TestStatusEmpty(t *testing.T) {
	srv, _, _ := newTestServer(t)

	resp, err := http.Get(srv.URL + "/status")
	if err != nil {
		t.Fatalf("get /status: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	st := decodeJSON[monitor.Status](t, resp.Body)

	if st.Bootstrap.Found {
		t.Errorf("bootstrap.found = true, want false on empty data dir")
	}
	if st.Bootstrap.Error != "" {
		t.Errorf("bootstrap.error = %q, want empty", st.Bootstrap.Error)
	}
	if st.Snapshot.Found {
		t.Errorf("snapshot.found = true, want false on empty objstore")
	}
	if st.Run.Error != "" {
		t.Errorf("run.error = %q, want empty", st.Run.Error)
	}
	if st.Run.RawFilesTotal != 0 {
		t.Errorf("run.raw_files_total = %d, want 0", st.Run.RawFilesTotal)
	}
	// LookbackDays falls back to the config value when no metadata sidecar
	// is present.
	if st.Snapshot.LookbackDays != 30 {
		t.Errorf("snapshot.lookback_days = %d, want 30", st.Snapshot.LookbackDays)
	}
}

// TestStatusWithBootstrapFixture writes a real social_graph.duckdb and
// verifies the bootstrap section reflects the row counts and the remote
// upload presence.
func TestStatusWithBootstrapFixture(t *testing.T) {
	srv, cfg, obj := newTestServer(t)

	dbPath := filepath.Join(cfg.DataDir, "bootstrap-staging", "social_graph.duckdb")
	if err := bootstrap.WriteFixture(context.Background(), dbPath, 5, 7, 2); err != nil {
		t.Fatalf("WriteFixture: %v", err)
	}
	// Stage a remote bootstrap upload via the memory objstore.
	if err := obj.Put(context.Background(), "bootstrap/2026-04-26/social_graph.duckdb",
		strings.NewReader("fake-duckdb-bytes"), "application/x-duckdb"); err != nil {
		t.Fatalf("obj.Put: %v", err)
	}

	resp, err := http.Get(srv.URL + "/status/bootstrap")
	if err != nil {
		t.Fatalf("get /status/bootstrap: %v", err)
	}
	defer resp.Body.Close()
	st := decodeJSON[monitor.BootstrapStatus](t, resp.Body)

	if !st.Found {
		t.Fatalf("bootstrap.found = false, want true")
	}
	if st.Actors != 5 {
		t.Errorf("actors = %d, want 5", st.Actors)
	}
	if st.Follows != 7 {
		t.Errorf("follows = %d, want 7", st.Follows)
	}
	if st.Blocks != 2 {
		t.Errorf("blocks = %d, want 2", st.Blocks)
	}
	if st.CompletedDIDs != 5 {
		t.Errorf("completed_dids = %d, want 5", st.CompletedDIDs)
	}
	if st.StartedAt == nil {
		t.Errorf("started_at = nil, want populated")
	}
	if st.CompletedAt == nil {
		t.Errorf("completed_at = nil, want populated (fixture writes both)")
	}
	if st.RemoteBaseline == nil || *st.RemoteBaseline != "bootstrap/2026-04-26/social_graph.duckdb" {
		t.Errorf("remote_baseline = %v, want bootstrap/2026-04-26/social_graph.duckdb",
			derefOrNil(st.RemoteBaseline))
	}
	if st.Error != "" {
		t.Errorf("error = %q, want empty", st.Error)
	}
}

// TestStatusRunSection verifies the run section reads a sqlite cursor and
// rolls up raw/ files by collection.
func TestStatusRunSection(t *testing.T) {
	srv, cfg, obj := newTestServer(t)

	cursorPath := filepath.Join(cfg.DataDir, "run-state", "cursor.sqlite")
	if err := writeCursorFixture(cursorPath, 1714150000000000); err != nil {
		t.Fatalf("writeCursorFixture: %v", err)
	}

	for _, p := range []string{
		"raw/2026-04-25/posts-1-00000001.parquet",
		"raw/2026-04-25/posts-2-00000002.parquet",
		"raw/2026-04-26/posts-3-00000003.parquet",
		"raw/2026-04-26/likes-1-00000001.parquet",
	} {
		if err := obj.Put(context.Background(), p, strings.NewReader("x"), ""); err != nil {
			t.Fatalf("obj.Put %s: %v", p, err)
		}
	}

	resp, err := http.Get(srv.URL + "/status/run")
	if err != nil {
		t.Fatalf("get /status/run: %v", err)
	}
	defer resp.Body.Close()
	st := decodeJSON[monitor.RunStatus](t, resp.Body)

	if st.CursorMicros == nil || *st.CursorMicros != 1714150000000000 {
		t.Errorf("cursor_micros = %v, want 1714150000000000", derefIntOrNil(st.CursorMicros))
	}
	if st.CursorAt == nil {
		t.Errorf("cursor_at = nil, want populated")
	}
	if st.RawFilesTotal != 4 {
		t.Errorf("raw_files_total = %d, want 4", st.RawFilesTotal)
	}
	if st.RawFilesByCollection["posts"] != 3 {
		t.Errorf("posts = %d, want 3", st.RawFilesByCollection["posts"])
	}
	if st.RawFilesByCollection["likes"] != 1 {
		t.Errorf("likes = %d, want 1", st.RawFilesByCollection["likes"])
	}
	if len(st.RawDates) != 2 || st.RawDates[0] != "2026-04-25" || st.RawDates[1] != "2026-04-26" {
		t.Errorf("raw_dates = %v, want [2026-04-25 2026-04-26]", st.RawDates)
	}
}

// TestStatusSnapshotSection writes a metadata sidecar and stages one of the
// duckdb outputs so the size probe and metadata parse paths both fire.
func TestStatusSnapshotSection(t *testing.T) {
	srv, _, obj := newTestServer(t)

	snapAt := time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC)
	meta := map[string]any{
		"snapshot_at":   snapAt,
		"lookback_days": 14,
		"row_counts":    map[string]int64{"actors": 100, "posts": 200},
	}
	body, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := obj.Put(context.Background(), "snapshot/snapshot_metadata.json", strings.NewReader(string(body)), "application/json"); err != nil {
		t.Fatal(err)
	}
	if err := obj.Put(context.Background(), "snapshot/current_all.duckdb", strings.NewReader("12345"), ""); err != nil {
		t.Fatal(err)
	}
	if err := obj.Put(context.Background(), "snapshot/current_graph.duckdb", strings.NewReader("123"), ""); err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(srv.URL + "/status/snapshot")
	if err != nil {
		t.Fatalf("get /status/snapshot: %v", err)
	}
	defer resp.Body.Close()
	st := decodeJSON[monitor.SnapshotStatus](t, resp.Body)

	if !st.Found {
		t.Errorf("snapshot.found = false, want true")
	}
	if st.LookbackDays != 14 {
		t.Errorf("lookback_days = %d, want 14 (from metadata)", st.LookbackDays)
	}
	if st.SnapshotAt == nil || !st.SnapshotAt.Equal(snapAt) {
		t.Errorf("snapshot_at = %v, want %v", derefTimeOrNil(st.SnapshotAt), snapAt)
	}
	if st.RowCounts["actors"] != 100 || st.RowCounts["posts"] != 200 {
		t.Errorf("row_counts = %v, want actors=100 posts=200", st.RowCounts)
	}
	if st.CurrentAllSizeBytes != 5 {
		t.Errorf("current_all_size_bytes = %d, want 5", st.CurrentAllSizeBytes)
	}
	if st.CurrentGraphSizeBytes != 3 {
		t.Errorf("current_graph_size_bytes = %d, want 3", st.CurrentGraphSizeBytes)
	}
}

// TestRunWithGracefulShutdown verifies the server returns nil when the
// context is cancelled rather than after a long ListenAndServe error.
func TestRunWithGracefulShutdown(t *testing.T) {
	cfg := config.Config{
		DataDir:      t.TempDir(),
		MonitorAddr:  "127.0.0.1:0", // port 0 = let the OS pick
		LogLevel:     "info",
		LookbackDays: 30,
	}
	deps := monitor.Deps{ObjStore: objstore.NewMemory()}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- monitor.RunWith(ctx, cfg, deps) }()

	// Give the goroutine a moment to enter ListenAndServe so the cancel
	// path actually exercises Shutdown rather than racing to the ListenAndServe.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("RunWith returned %v, want nil after graceful shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunWith did not return within 5s of context cancel")
	}
}

// writeCursorFixture writes a sqlite file at path with the cursor schema
// runcmd uses, populated with a single jetstream row.
func writeCursorFixture(path string, micros int64) error {
	if err := mkdirAll(filepath.Dir(path)); err != nil {
		return err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE cursor (
        name TEXT PRIMARY KEY,
        micros BIGINT NOT NULL,
        updated_at TEXT NOT NULL
    )`); err != nil {
		return err
	}
	_, err = db.Exec(`INSERT INTO cursor(name, micros, updated_at) VALUES (?, ?, ?)`,
		"jetstream", micros, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func mkdirAll(dir string) error { return os.MkdirAll(dir, 0o755) }

func derefOrNil(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

func derefIntOrNil(i *int64) any {
	if i == nil {
		return nil
	}
	return *i
}

func derefTimeOrNil(t *time.Time) any {
	if t == nil {
		return nil
	}
	return *t
}
