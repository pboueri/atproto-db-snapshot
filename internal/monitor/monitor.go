// Package monitor implements the `at-snapshot monitor` subcommand: a
// lightweight read-only HTTP server that surfaces the health of the
// bootstrap, run, and snapshot pipelines.
//
// Design choices worth flagging:
//
//   - The server only reads. It never writes, never holds DuckDB or SQLite
//     locks, and never blocks the long-running pipelines. Every database
//     handle is opened with the backend's read-only mode (DuckDB
//     access_mode=read_only, SQLite mode=ro) so a concurrent bootstrap or
//     run process keeps its writer lock.
//
//   - Each request opens its own database connection on demand and closes
//     it before responding. We never cache handles. Two consequences:
//     status reads pay an open/query/close cost, and concurrent requests do
//     not contend on a shared mutex. The status endpoints are not on a hot
//     path — operators poll once a minute or so — so the overhead is
//     irrelevant compared to the lock-safety win.
//
//   - Sections degrade independently. If the bootstrap DuckDB is missing
//     but raw/ has files, the run section is still reported. If a query
//     fails, the section emits found=true with an embedded error string
//     rather than failing the whole response.
//
//   - The HTTP handlers are stateless: all per-request reads go through
//     reader functions that take the config + objstore + a context.
package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
)

// Deps lets tests inject a pre-built object store. Run wires production
// defaults via objstore.FromConfig.
type Deps struct {
	ObjStore objstore.Store
}

// Run starts the monitor HTTP server with the production object store backend.
func Run(ctx context.Context, cfg config.Config) error {
	obj, err := objstore.FromConfig(cfg)
	if err != nil {
		return err
	}
	return RunWith(ctx, cfg, Deps{ObjStore: obj})
}

// RunWith starts the monitor HTTP server with explicit dependencies.
//
// The server listens on cfg.MonitorAddr and shuts down gracefully when ctx
// is cancelled (5s timeout for in-flight requests).
func RunWith(ctx context.Context, cfg config.Config, deps Deps) error {
	if deps.ObjStore == nil {
		return errors.New("monitor: ObjStore is required")
	}
	srv := &http.Server{
		Addr:              cfg.MonitorAddr,
		Handler:           NewHandler(cfg, deps),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		slog.Info("monitor listening", "addr", cfg.MonitorAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Warn("monitor shutdown", "err", err)
		}
		// Drain the listen goroutine so we don't return before it exits.
		<-errCh
		return nil
	case err := <-errCh:
		return err
	}
}

// NewHandler wires the read-only routes onto an http.ServeMux and returns it.
//
// Exposed so tests (and a hypothetical embedding host) can mount the routes
// without spinning a real listener.
func NewHandler(cfg config.Config, deps Deps) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", handleHealth)
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, readStatus(r.Context(), cfg, deps))
	})
	mux.HandleFunc("/status/bootstrap", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, readBootstrap(r.Context(), cfg, deps))
	})
	mux.HandleFunc("/status/run", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, readRun(r.Context(), cfg, deps))
	})
	mux.HandleFunc("/status/snapshot", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, readSnapshot(r.Context(), cfg, deps))
	})
	return mux
}

// Status is the consolidated payload returned by /status.
type Status struct {
	Bootstrap BootstrapStatus `json:"bootstrap"`
	Run       RunStatus       `json:"run"`
	Snapshot  SnapshotStatus  `json:"snapshot"`
}

// BootstrapStatus reflects the local bootstrap-staging duckdb plus the
// remote bootstrap/{date}/social_graph.duckdb upload state.
type BootstrapStatus struct {
	Found          bool       `json:"found"`
	StartedAt      *time.Time `json:"started_at"`
	CompletedAt    *time.Time `json:"completed_at"`
	CompletedDIDs  int64      `json:"completed_dids"`
	Actors         int64      `json:"actors"`
	Follows        int64      `json:"follows"`
	Blocks         int64      `json:"blocks"`
	RemoteBaseline *string    `json:"remote_baseline"`
	Error          string     `json:"error,omitempty"`
}

// RunStatus reflects the jetstream cursor checkpoint plus the raw/ tree.
type RunStatus struct {
	CursorMicros         *int64         `json:"cursor_micros"`
	CursorAt             *time.Time     `json:"cursor_at"`
	RawDates             []string       `json:"raw_dates"`
	RawFilesTotal        int            `json:"raw_files_total"`
	RawFilesByCollection map[string]int `json:"raw_files_by_collection"`
	Error                string         `json:"error,omitempty"`
}

// SnapshotStatus reflects the snapshot/ tree in object storage.
type SnapshotStatus struct {
	Found                 bool             `json:"found"`
	SnapshotAt            *time.Time       `json:"snapshot_at"`
	LookbackDays          int              `json:"lookback_days"`
	CurrentAllSizeBytes   int64            `json:"current_all_size_bytes"`
	CurrentGraphSizeBytes int64            `json:"current_graph_size_bytes"`
	RowCounts             map[string]int64 `json:"row_counts"`
	Error                 string           `json:"error,omitempty"`
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func readStatus(ctx context.Context, cfg config.Config, deps Deps) Status {
	return Status{
		Bootstrap: readBootstrap(ctx, cfg, deps),
		Run:       readRun(ctx, cfg, deps),
		Snapshot:  readSnapshot(ctx, cfg, deps),
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(body); err != nil {
		// We've already written the status header; surface a server log so
		// the failure is debuggable but don't try to rewrite the response.
		slog.Warn("monitor encode response", "err", err)
	}
}

// errMessage extracts a stable string from an error for inclusion in a JSON
// response. Wrapped sentinel errors (objstore.ErrNotExist) are normalized so
// callers can compare strings if they want, but the field is purely
// diagnostic.
func errMessage(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("%s", err)
}
