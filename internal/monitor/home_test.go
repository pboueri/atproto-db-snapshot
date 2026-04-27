package monitor

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

	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
)

func readBody(t *testing.T, r io.Reader) string {
	t.Helper()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func writeFakeSidecar(t *testing.T, dir string, completedDIDs, actors int64) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, "bootstrap-staging"), 0o755); err != nil {
		t.Fatal(err)
	}
	body := map[string]any{
		"started_at":     time.Now().UTC().Add(-10 * time.Minute),
		"updated_at":     time.Now().UTC(),
		"completed_dids": completedDIDs,
		"actors":         actors,
		"follows":        actors * 50, // arbitrary fixture ratio
		"blocks":         actors / 10,
		"fetched":        completedDIDs,
		"written":        completedDIDs,
		"fetch_errors":   0,
	}
	b, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bootstrap-staging", "progress.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestHomeRendersHTML(t *testing.T) {
	dataDir := t.TempDir()
	objDir := t.TempDir()
	obj, err := objstore.NewLocal(objDir)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{
		DataDir:         dataDir,
		ObjectStore:     "local",
		ObjectStoreRoot: objDir,
		LookbackDays:    30,
		Languages:       []string{"en"},
	}

	srv := httptest.NewServer(NewHandler(cfg, Deps{ObjStore: obj}))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", got)
	}
	body := readBody(t, resp.Body)
	for _, want := range []string{
		"at-snapshot",
		"<h2>bootstrap",
		"<h2>run",
		"<h2>snapshot",
		"<h2>object store",
		"refreshes every",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("homepage missing %q", want)
		}
	}
}

func TestHomeReadsBootstrapSidecar(t *testing.T) {
	dataDir := t.TempDir()
	objDir := t.TempDir()
	obj, err := objstore.NewLocal(objDir)
	if err != nil {
		t.Fatal(err)
	}

	// Drop a sidecar JSON pointing at the staging dir without a duckdb.
	// The sidecar reader should pick this up so the monitor doesn't try to
	// open the (nonexistent) duckdb and surface the "no bootstrap data" UI.
	writeFakeSidecar(t, dataDir, 7, 100_000)
	_ = context.Background

	cfg := config.Config{
		DataDir: dataDir, ObjectStore: "local", ObjectStoreRoot: objDir,
		LookbackDays: 30,
	}
	srv := httptest.NewServer(NewHandler(cfg, Deps{ObjStore: obj}))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/status/bootstrap")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body := readBody(t, resp.Body)
	if !strings.Contains(body, `"completed_dids": 7`) {
		t.Errorf("expected completed_dids 7 from sidecar, got: %s", body)
	}
	if !strings.Contains(body, `"actors": 100000`) {
		t.Errorf("expected actors 100000 from sidecar, got: %s", body)
	}
}

func TestHomeUnknownPath404(t *testing.T) {
	dataDir := t.TempDir()
	objDir := t.TempDir()
	obj, _ := objstore.NewLocal(objDir)
	cfg := config.Config{
		DataDir: dataDir, ObjectStore: "local", ObjectStoreRoot: objDir,
		LookbackDays: 30,
	}
	srv := httptest.NewServer(NewHandler(cfg, Deps{ObjStore: obj}))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/no-such-route")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}
