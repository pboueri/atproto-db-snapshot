package bootstrap

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// Progress is the on-disk shape of bootstrap-staging/progress.json.
//
// We mirror enough of the duckdb's bootstrap_meta + bootstrap_progress state
// here so the monitor command can render bootstrap status without opening the
// duckdb file — DuckDB only allows one process to hold a writer on the file
// at a time, and bootstrap is that writer for hours-to-days while the run is
// underway. The sidecar is updated atomically on every commit + on the
// stats tick, so monitor reads see a consistent snapshot.
type Progress struct {
	StartedAt     time.Time `json:"started_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	CompletedAt   *time.Time `json:"completed_at,omitempty"`
	CompletedDIDs int64     `json:"completed_dids"`
	Fetched       int64     `json:"fetched"`
	Written       int64     `json:"written"`
	FetchErrors   int64     `json:"fetch_errors"`
	Actors        int64     `json:"actors"`
	Follows       int64     `json:"follows"`
	Blocks        int64     `json:"blocks"`
	PLCEndpoint   string    `json:"plc_endpoint"`
	Source        string    `json:"source"`
}

// progressWriter coalesces sidecar updates so a fast inner loop doesn't
// thrash the disk. Updates land on every commit but only flush every Tick
// (default 1s) plus on the final close.
type progressWriter struct {
	path string
	st   *store
	mu   sync.Mutex
	cur  Progress
	// counters live as pointers so the bootstrap orchestrator can advance
	// them without going through the writer's mutex.
	fetched, written, fetchErrs *atomic.Int64
}

func newProgressWriter(path string, st *store, fetched, written, fetchErrs *atomic.Int64, plcEndpoint string) (*progressWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("bootstrap: mkdir progress dir: %w", err)
	}
	return &progressWriter{
		path:      path,
		st:        st,
		cur:       Progress{StartedAt: time.Now().UTC(), PLCEndpoint: plcEndpoint, Source: "bootstrap"},
		fetched:   fetched,
		written:   written,
		fetchErrs: fetchErrs,
	}, nil
}

// Tick refreshes the progress file with current counters. Cheap; safe to
// call from a ticker.
func (p *progressWriter) Tick(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.flushLocked(ctx, false)
}

// Close marks the run finished and writes the final state.
func (p *progressWriter) Close(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.flushLocked(ctx, true)
}

func (p *progressWriter) flushLocked(ctx context.Context, final bool) error {
	now := time.Now().UTC()
	p.cur.UpdatedAt = now
	p.cur.Fetched = p.fetched.Load()
	p.cur.Written = p.written.Load()
	p.cur.FetchErrors = p.fetchErrs.Load()
	if c, err := p.st.Counts(ctx); err == nil {
		p.cur.Actors = c.Actors
		p.cur.Follows = c.Follows
		p.cur.Blocks = c.Blocks
		p.cur.CompletedDIDs = c.CompletedDIDs
	}
	if final {
		t := now
		p.cur.CompletedAt = &t
	}
	body, err := json.MarshalIndent(p.cur, "", "  ")
	if err != nil {
		return err
	}
	tmp := p.path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, p.path)
}
