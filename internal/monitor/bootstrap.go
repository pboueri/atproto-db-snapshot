package monitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2" // register duckdb driver

	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
)

// readBootstrap inspects the local bootstrap-staging state and the remote
// bootstrap/{date}/social_graph.duckdb upload (if present).
//
// Two paths to the local data:
//
//  1. progress.json sidecar — preferred while bootstrap is running. Bootstrap
//     writes this atomically on every flush since DuckDB only allows one
//     process to hold the writer lock and the monitor can't open the .duckdb
//     until bootstrap exits.
//
//  2. social_graph.duckdb — fallback for after a finished run, since the
//     sidecar isn't strictly required to be present.
func readBootstrap(ctx context.Context, cfg config.Config, deps Deps) BootstrapStatus {
	st := BootstrapStatus{}
	stagingDir := filepath.Join(cfg.DataDir, "bootstrap-staging")
	sidecarPath := filepath.Join(stagingDir, "progress.json")
	dbPath := filepath.Join(stagingDir, "social_graph.duckdb")

	if filled, err := fillBootstrapFromSidecar(sidecarPath, &st); err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.Warn("monitor: read bootstrap sidecar", "path", sidecarPath, "err", err)
	} else if filled {
		st.Found = true
	}

	if !st.Found {
		// No sidecar — try the duckdb directly. This succeeds once
		// bootstrap has exited and released its writer lock.
		if _, err := os.Stat(dbPath); err == nil {
			st.Found = true
			if err := fillBootstrapFromDB(ctx, dbPath, &st); err != nil {
				slog.Warn("monitor: read bootstrap db", "path", dbPath, "err", err)
				st.Error = errMessage(err)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			st.Error = errMessage(err)
		}
	}

	if remote, err := latestBootstrapRemote(ctx, deps.ObjStore); err == nil && remote != "" {
		st.RemoteBaseline = &remote
	} else if err != nil && !errors.Is(err, objstore.ErrNotExist) {
		slog.Warn("monitor: list bootstrap remote", "err", err)
		// Non-fatal: keep RemoteBaseline=nil and append the error if no
		// other one is set yet so the caller still sees the section.
		if st.Error == "" {
			st.Error = errMessage(err)
		}
	}

	return st
}

// sidecarShape mirrors internal/bootstrap.Progress; we duplicate the struct
// here rather than importing internal/bootstrap because bootstrap imports
// objstore and we'd rather keep the monitor's import graph minimal.
type sidecarShape struct {
	StartedAt     time.Time  `json:"started_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
	CompletedAt   *time.Time `json:"completed_at,omitempty"`
	CompletedDIDs int64      `json:"completed_dids"`
	Fetched       int64      `json:"fetched"`
	Written       int64      `json:"written"`
	FetchErrors   int64      `json:"fetch_errors"`
	Actors        int64      `json:"actors"`
	Follows       int64      `json:"follows"`
	Blocks        int64      `json:"blocks"`
}

// fillBootstrapFromSidecar reads the JSON sidecar bootstrap writes during a
// run. Returns (true, nil) if the file exists and decodes; (false, err) for
// not-exist (caller checks) or decode errors.
func fillBootstrapFromSidecar(path string, st *BootstrapStatus) (bool, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}
	var s sidecarShape
	if err := json.Unmarshal(body, &s); err != nil {
		return false, fmt.Errorf("decode sidecar: %w", err)
	}
	if !s.StartedAt.IsZero() {
		t := s.StartedAt.UTC()
		st.StartedAt = &t
	}
	if s.CompletedAt != nil && !s.CompletedAt.IsZero() {
		t := s.CompletedAt.UTC()
		st.CompletedAt = &t
	}
	st.CompletedDIDs = s.CompletedDIDs
	st.Actors = s.Actors
	st.Follows = s.Follows
	st.Blocks = s.Blocks
	return true, nil
}

// fillBootstrapFromDB queries the canonical bootstrap tables. Each query
// is wrapped so a missing table (e.g. a brand-new staging file before the
// schema is applied) doesn't take down the section.
func fillBootstrapFromDB(ctx context.Context, dbPath string, st *BootstrapStatus) error {
	db, err := openDuckDBReadOnly(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	// bootstrap_meta: started_at / completed_at. The table is supposed to
	// have at most one row but we LIMIT 1 defensively.
	var startedAt, completedAt sql.NullTime
	row := db.QueryRowContext(ctx, "SELECT started_at, completed_at FROM bootstrap_meta LIMIT 1")
	switch err := row.Scan(&startedAt, &completedAt); {
	case err == nil:
		if startedAt.Valid {
			t := startedAt.Time.UTC()
			st.StartedAt = &t
		}
		if completedAt.Valid {
			t := completedAt.Time.UTC()
			st.CompletedAt = &t
		}
	case errors.Is(err, sql.ErrNoRows):
		// No rows yet — leave timestamps nil.
	default:
		return fmt.Errorf("read bootstrap_meta: %w", err)
	}

	// Counts: actors / follows / blocks / progress.
	row = db.QueryRowContext(ctx, `SELECT
        (SELECT count(*) FROM actors),
        (SELECT count(*) FROM follows),
        (SELECT count(*) FROM blocks),
        (SELECT count(*) FROM bootstrap_progress)`)
	if err := row.Scan(&st.Actors, &st.Follows, &st.Blocks, &st.CompletedDIDs); err != nil {
		return fmt.Errorf("read counts: %w", err)
	}
	return nil
}

// openDuckDBReadOnly opens path with access_mode=read_only so we never lock
// out a concurrent writer process.
func openDuckDBReadOnly(path string) (*sql.DB, error) {
	dsn := path + "?access_mode=read_only"
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, err
	}
	// One connection is enough — the monitor opens a fresh handle per
	// request and discards it. SetMaxOpenConns also limits the
	// connection-pool reuse that would otherwise re-issue the read-only
	// open without our DSN.
	db.SetMaxOpenConns(1)
	if err := db.PingContext(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// latestBootstrapRemote returns the bootstrap/{date}/social_graph.duckdb
// path with the lexicographically-greatest date prefix, or "" when none.
//
// We list bootstrap/ rather than Stat'ing today's date because operators
// may run snapshots against an older successful baseline; reporting only
// "today" would hide that we have a usable baseline at all.
func latestBootstrapRemote(ctx context.Context, obj objstore.Store) (string, error) {
	objs, err := obj.List(ctx, "bootstrap/")
	if err != nil {
		return "", err
	}
	var dates []string
	for _, o := range objs {
		// Expect bootstrap/YYYY-MM-DD/social_graph.duckdb.
		parts := strings.Split(o.Path, "/")
		if len(parts) != 3 || parts[2] != "social_graph.duckdb" {
			continue
		}
		// Ignore obviously-unparseable date directories so a stray entry
		// doesn't shadow the real baseline.
		if _, perr := time.Parse("2006-01-02", parts[1]); perr != nil {
			continue
		}
		dates = append(dates, parts[1])
	}
	if len(dates) == 0 {
		return "", nil
	}
	sort.Strings(dates)
	latest := dates[len(dates)-1]
	return fmt.Sprintf("bootstrap/%s/social_graph.duckdb", latest), nil
}
