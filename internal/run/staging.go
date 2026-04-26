package run

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pboueri/atproto-db-snapshot/internal/config"

	// modernc.org/sqlite is pure Go; cgo-free coexistence with go-duckdb.
	_ "modernc.org/sqlite"
)

// stagingTables maps Jetstream collections to staging table names per §8.
// Deletes are routed to staging_events_delete regardless of collection.
var stagingTables = map[string]string{
	"app.bsky.feed.post":     "staging_events_post",
	"app.bsky.feed.like":     "staging_events_like",
	"app.bsky.feed.repost":   "staging_events_repost",
	"app.bsky.graph.follow":  "staging_events_follow",
	"app.bsky.graph.block":   "staging_events_block",
	"app.bsky.actor.profile": "staging_events_profile",
}

// allStagingTables enumerates every table the run package writes to.
func allStagingTables() []string {
	names := []string{"staging_events_delete"}
	for _, t := range stagingTables {
		names = append(names, t)
	}
	return names
}

// stagingDB wraps the local SQLite database used to durably stage
// Jetstream events between UTC-day flushes (§8 step 3).
type stagingDB struct {
	db   *sql.DB
	path string
	cfg  config.JetstreamConfig

	logger *slog.Logger

	// rowsSinceCheckpoint is incremented by Insert and reset by checkpoint.
	rowsSinceCheckpoint int64
	// cursorMu protects lastCursorState — a snapshot of the largest time_us
	// we've successfully written, plus the endpoint that produced it.
	cursorMu         sync.Mutex
	lastCursorState  cursorState
	hasUnsavedCursor bool

	// preparedInserts keeps one prepared INSERT statement per staging table.
	preparedMu sync.Mutex
	prepared   map[string]*sql.Stmt
}

// openStaging opens (or creates) ./data/staging.db, applies pragmas for
// WAL + reasonable durability, and ensures schema.
func openStaging(path string, cfg config.JetstreamConfig, logger *slog.Logger) (*stagingDB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// Single writer is plenty; SQLite serializes anyway.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := db.PingContext(context.Background()); err != nil {
		db.Close()
		return nil, err
	}

	s := &stagingDB{
		db:       db,
		path:     path,
		cfg:      cfg,
		logger:   logger,
		prepared: make(map[string]*sql.Stmt),
	}
	if err := s.ensureSchema(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// ensureSchema creates one append-only events table per collection plus a
// shared deletions table. PK is the dedup key per §8 step 6.
func (s *stagingDB) ensureSchema() error {
	stmts := []string{}
	for _, tbl := range stagingTables {
		stmts = append(stmts, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  did         TEXT NOT NULL,
  collection  TEXT NOT NULL,
  rkey        TEXT NOT NULL,
  operation   TEXT NOT NULL,
  time_us     INTEGER NOT NULL,
  cid         TEXT,
  day         TEXT NOT NULL,
  record_json TEXT,
  PRIMARY KEY (did, collection, rkey, operation, time_us)
) WITHOUT ROWID;`, tbl))
		stmts = append(stmts, fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_day_idx ON %s(day);`, tbl, tbl))
	}
	stmts = append(stmts, `
CREATE TABLE IF NOT EXISTS staging_events_delete (
  did         TEXT NOT NULL,
  collection  TEXT NOT NULL,
  rkey        TEXT NOT NULL,
  operation   TEXT NOT NULL,
  time_us     INTEGER NOT NULL,
  day         TEXT NOT NULL,
  PRIMARY KEY (did, collection, rkey, operation, time_us)
) WITHOUT ROWID;`)
	stmts = append(stmts, `CREATE INDEX IF NOT EXISTS staging_events_delete_day_idx ON staging_events_delete(day);`)

	for _, q := range stmts {
		if _, err := s.db.Exec(q); err != nil {
			return fmt.Errorf("staging schema: %w", err)
		}
	}
	return nil
}

// stagingEvent is the minimal event shape the consumer hands to the staging
// layer. All time math is in UTC.
type stagingEvent struct {
	DID        string
	Collection string
	Rkey       string
	Operation  string // "create" | "update" | "delete"
	TimeUS     int64
	CID        string
	Day        string // YYYY-MM-DD UTC, derived from TimeUS
	RecordJSON []byte // raw JSON of commit.record (nil for deletes)
	Endpoint   string // jetstream endpoint that produced this event
}

// insert writes the event to the right staging table. Returns true if it
// was newly inserted, false if it was a duplicate (PK conflict).
func (s *stagingDB) insert(ctx context.Context, ev *stagingEvent) (bool, error) {
	var (
		stmt *sql.Stmt
		err  error
	)
	if ev.Operation == "delete" {
		stmt, err = s.preparedFor("staging_events_delete", `
INSERT OR IGNORE INTO staging_events_delete (did, collection, rkey, operation, time_us, day)
VALUES (?, ?, ?, ?, ?, ?)`)
		if err != nil {
			return false, err
		}
		res, err := stmt.ExecContext(ctx, ev.DID, ev.Collection, ev.Rkey, ev.Operation, ev.TimeUS, ev.Day)
		if err != nil {
			return false, err
		}
		n, _ := res.RowsAffected()
		s.recordWrite(ev, n > 0)
		return n > 0, nil
	}
	tbl, ok := stagingTables[ev.Collection]
	if !ok {
		return false, nil // unknown collection — silently skip
	}
	stmt, err = s.preparedFor(tbl, fmt.Sprintf(`
INSERT OR IGNORE INTO %s (did, collection, rkey, operation, time_us, cid, day, record_json)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, tbl))
	if err != nil {
		return false, err
	}
	res, err := stmt.ExecContext(ctx, ev.DID, ev.Collection, ev.Rkey, ev.Operation, ev.TimeUS, ev.CID, ev.Day, string(ev.RecordJSON))
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	s.recordWrite(ev, n > 0)
	return n > 0, nil
}

func (s *stagingDB) preparedFor(name, query string) (*sql.Stmt, error) {
	s.preparedMu.Lock()
	defer s.preparedMu.Unlock()
	if st, ok := s.prepared[name]; ok {
		return st, nil
	}
	st, err := s.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	s.prepared[name] = st
	return st, nil
}

// recordWrite advances the cursor watermark and the row counter even when a
// duplicate was ignored — both are inputs to the lag/checkpoint heuristics.
func (s *stagingDB) recordWrite(ev *stagingEvent, inserted bool) {
	s.cursorMu.Lock()
	if ev.TimeUS > s.lastCursorState.Cursor {
		s.lastCursorState.Cursor = ev.TimeUS
		s.lastCursorState.Endpoint = ev.Endpoint
		s.hasUnsavedCursor = true
	}
	s.cursorMu.Unlock()
	if inserted {
		atomic.AddInt64(&s.rowsSinceCheckpoint, 1)
	}
}

// lastCursor returns a snapshot of the most-recent cursor watermark.
func (s *stagingDB) lastCursor() cursorState {
	s.cursorMu.Lock()
	defer s.cursorMu.Unlock()
	c := s.lastCursorState
	if c.UpdatedAt.IsZero() {
		c.UpdatedAt = time.Now().UTC()
	}
	return c
}

// rewindCursor moves the cursor backward by `seconds` to cover endpoint
// switch / reconnect overlap. Caller is responsible for using the result.
func (s *stagingDB) snapshotCursor() cursorState {
	return s.lastCursor()
}

// checkpointLoop runs in its own goroutine and triggers a SQLite WAL
// checkpoint either every CheckpointInterval or after CheckpointEveryRows
// inserts, whichever comes first. Cursor is fsync'd after each checkpoint.
func (s *stagingDB) checkpointLoop(ctx context.Context, cursorPath string) {
	tick := time.NewTicker(s.cfg.CheckpointInterval)
	defer tick.Stop()

	threshold := int64(s.cfg.CheckpointEveryRows)
	if threshold <= 0 {
		threshold = 10_000
	}

	rowsTick := time.NewTicker(500 * time.Millisecond)
	defer rowsTick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			s.runCheckpointAndPersist(ctx, cursorPath, "interval")
		case <-rowsTick.C:
			if atomic.LoadInt64(&s.rowsSinceCheckpoint) >= threshold {
				s.runCheckpointAndPersist(ctx, cursorPath, "rows")
			}
		}
	}
}

func (s *stagingDB) runCheckpointAndPersist(ctx context.Context, cursorPath, why string) {
	if err := s.checkpoint(ctx); err != nil {
		s.logger.Warn("checkpoint failed", "why", why, "err", err)
		return
	}
	cur := s.lastCursor()
	if cur.Cursor == 0 {
		return
	}
	s.cursorMu.Lock()
	dirty := s.hasUnsavedCursor
	s.cursorMu.Unlock()
	if !dirty {
		return
	}
	if err := saveCursorAtomic(cursorPath, cur); err != nil {
		s.logger.Warn("cursor save failed", "err", err)
		return
	}
	s.cursorMu.Lock()
	s.hasUnsavedCursor = false
	s.cursorMu.Unlock()

	// staging.db bloat warning per §8 graceful degradation.
	if fi, err := os.Stat(s.path); err == nil && s.cfg.MaxStagingBytes > 0 && fi.Size() > s.cfg.MaxStagingBytes {
		s.logger.Warn("staging.db over warning threshold",
			"size_bytes", fi.Size(),
			"threshold_bytes", s.cfg.MaxStagingBytes,
		)
	}
}

// checkpoint runs PRAGMA wal_checkpoint(TRUNCATE) and zeroes the row counter.
func (s *stagingDB) checkpoint(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE);"); err != nil {
		return err
	}
	atomic.StoreInt64(&s.rowsSinceCheckpoint, 0)
	return nil
}

// deleteDay drops all rows for the given UTC day in the given table.
func (s *stagingDB) deleteDay(ctx context.Context, table, day string) (int64, error) {
	res, err := s.db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE day = ?`, table), day)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// rowCount returns SELECT count(*) FROM <table> WHERE day = ? as an int64.
func (s *stagingDB) rowCount(ctx context.Context, table, day string) (int64, error) {
	row := s.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM %s WHERE day = ?`, table), day)
	var n int64
	if err := row.Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// postEmbedRowCount returns the count of staging_events_post rows for the
// day that carry a non-null `embed` — i.e. the row count that
// post_embeds.parquet will materialize. Used by rollover to populate the
// manifest accurately. Relies on SQLite's built-in json1 extension.
func (s *stagingDB) postEmbedRowCount(ctx context.Context, day string) (int64, error) {
	const q = `SELECT count(*) FROM staging_events_post
		WHERE day = ?
		  AND operation IN ('create','update')
		  AND json_extract(record_json, '$.embed') IS NOT NULL`
	row := s.db.QueryRowContext(ctx, q, day)
	var n int64
	if err := row.Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// daysWithEvents returns every distinct `day` present across all staging
// tables. Used by rollover to find sealable days.
func (s *stagingDB) daysWithEvents(ctx context.Context) ([]string, error) {
	seen := make(map[string]struct{})
	for _, t := range allStagingTables() {
		rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`SELECT DISTINCT day FROM %s`, t))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var d string
			if err := rows.Scan(&d); err != nil {
				rows.Close()
				return nil, err
			}
			seen[d] = struct{}{}
		}
		rows.Close()
	}
	out := make([]string, 0, len(seen))
	for d := range seen {
		out = append(out, d)
	}
	return out, nil
}

// cursorBoundsForDay returns (min(time_us), max(time_us)) across all
// staging tables for the given UTC day, used to populate the per-day
// _manifest.json (§6).
func (s *stagingDB) cursorBoundsForDay(ctx context.Context, day string) (int64, int64, error) {
	var (
		gotMin = int64(0)
		gotMax = int64(0)
		any    = false
	)
	for _, t := range allStagingTables() {
		var lo, hi sql.NullInt64
		row := s.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT MIN(time_us), MAX(time_us) FROM %s WHERE day = ?`, t), day)
		if err := row.Scan(&lo, &hi); err != nil {
			return 0, 0, err
		}
		if lo.Valid {
			if !any || lo.Int64 < gotMin {
				gotMin = lo.Int64
			}
			any = true
		}
		if hi.Valid {
			if !any || hi.Int64 > gotMax {
				gotMax = hi.Int64
			}
			any = true
		}
	}
	if !any {
		return 0, 0, nil
	}
	return gotMin, gotMax, nil
}

// Close releases prepared statements and closes the database.
func (s *stagingDB) Close() error {
	s.preparedMu.Lock()
	for _, st := range s.prepared {
		_ = st.Close()
	}
	s.prepared = nil
	s.preparedMu.Unlock()
	return s.db.Close()
}

// ---------------------- cursor.json ----------------------

// cursorState mirrors the on-disk cursor file (§8.1). schema_version is
// pinned to "v1" — bumped only on breaking format changes.
type cursorState struct {
	Cursor        int64     `json:"cursor"`
	Endpoint      string    `json:"endpoint"`
	UpdatedAt     time.Time `json:"updated_at"`
	SchemaVersion string    `json:"schema_version"`
}

// loadCursor reads cursor.json. Missing file ⇒ zero cursor (cold start).
func loadCursor(path string) (cursorState, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return cursorState{SchemaVersion: "v1"}, nil
		}
		return cursorState{}, err
	}
	var c cursorState
	if err := json.Unmarshal(b, &c); err != nil {
		return cursorState{}, fmt.Errorf("cursor.json: %w", err)
	}
	if c.SchemaVersion == "" {
		c.SchemaVersion = "v1"
	}
	return c, nil
}

// saveCursorAtomic writes cursor.json via tempfile + os.Rename so a crash
// mid-write can never corrupt the cursor.
func saveCursorAtomic(path string, c cursorState) error {
	c.SchemaVersion = "v1"
	c.UpdatedAt = time.Now().UTC()
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	// fsync the tempfile before rename so the rename can't beat the data
	// to disk on a power-cut.
	f, err := os.OpenFile(tmp, os.O_RDWR, 0o644)
	if err == nil {
		_ = f.Sync()
		_ = f.Close()
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
