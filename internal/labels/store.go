// Package labels implements the `at-snapshotter labels` subcommand — a
// long-running WebSocket subscriber for com.atproto.label.subscribeLabels
// that persists labels into a local SQLite database for consumption by
// the nightly build.
//
// Unlike jetstream (JSON-over-WebSocket), the labeler subscription uses
// AT-Proto's event-stream binary framing: each WebSocket binary frame is
// two concatenated DAG-CBOR values — a header `{op, t}` then a payload.
// We use indigo's CBOR-generated types to decode both.
package labels

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	// modernc.org/sqlite is pure Go; cgo-free.
	_ "modernc.org/sqlite"
)

// Label is the persistence-layer view of one emitted label row. Nullable
// fields use pointers so callers can distinguish "absent" from "empty".
type Label struct {
	Seq int64
	Src string
	URI string
	CID *string
	Val string
	CTS string
	Exp *string
	Neg bool
}

// store wraps the SQLite database used to durably persist labels.
type store struct {
	db   *sql.DB
	path string

	mu    sync.Mutex
	stmt  *sql.Stmt
	count int64 // labels inserted or ignored since last checkpoint
}

// openStore opens (or creates) labels.db, applies WAL + schema, and returns
// a *store with a prepared INSERT OR IGNORE statement.
func openStore(path string) (*store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	s := &store{db: db, path: path}
	if err := s.ensureSchema(); err != nil {
		db.Close()
		return nil, err
	}
	stmt, err := db.Prepare(`
INSERT OR IGNORE INTO labels (seq, src, uri, cid, val, cts, exp, neg)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		db.Close()
		return nil, err
	}
	s.stmt = stmt
	return s, nil
}

func (s *store) ensureSchema() error {
	const ddl = `
CREATE TABLE IF NOT EXISTS labels (
  seq    BIGINT NOT NULL,
  src    TEXT   NOT NULL,
  uri    TEXT   NOT NULL,
  cid    TEXT,
  val    TEXT   NOT NULL,
  cts    TEXT   NOT NULL,
  exp    TEXT,
  neg    INTEGER NOT NULL,
  PRIMARY KEY (src, uri, val, cts)
);
CREATE INDEX IF NOT EXISTS labels_val ON labels(val);
CREATE INDEX IF NOT EXISTS labels_seq ON labels(seq);
`
	_, err := s.db.Exec(ddl)
	return err
}

// Upsert writes one label with INSERT OR IGNORE semantics. Returns true if
// a new row was inserted, false if it was a duplicate.
func (s *store) Upsert(ctx context.Context, l Label) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	negInt := 0
	if l.Neg {
		negInt = 1
	}
	var cid, exp any
	if l.CID != nil {
		cid = *l.CID
	}
	if l.Exp != nil {
		exp = *l.Exp
	}
	res, err := s.stmt.ExecContext(ctx, l.Seq, l.Src, l.URI, cid, l.Val, l.CTS, exp, negInt)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	s.count++
	return n > 0, nil
}

// Count returns (and resets) the number of Upsert calls since the last
// Count invocation. Used by the subscriber to trigger checkpoints every
// N labels.
func (s *store) Count() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.count
	s.count = 0
	return c
}

// Checkpoint runs PRAGMA wal_checkpoint(TRUNCATE) to fold the WAL back
// into the main db file.
func (s *store) Checkpoint(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE);")
	return err
}

// Close releases the prepared statement and closes the database.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stmt != nil {
		_ = s.stmt.Close()
		s.stmt = nil
	}
	return s.db.Close()
}

// ----------------- cursor.json -------------------

// cursorState mirrors labels_cursor.json. Parallels internal/run cursor.json
// but keyed on `seq` (int64) instead of time_us.
type cursorState struct {
	Seq           int64     `json:"seq"`
	Endpoint      string    `json:"endpoint"`
	UpdatedAt     time.Time `json:"updated_at"`
	SchemaVersion string    `json:"schema_version"`
}

// loadCursor reads labels_cursor.json. Missing file ⇒ zero seq (cold start).
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
		return cursorState{}, fmt.Errorf("labels_cursor.json: %w", err)
	}
	if c.SchemaVersion == "" {
		c.SchemaVersion = "v1"
	}
	return c, nil
}

// saveCursorAtomic writes labels_cursor.json via tempfile + os.Rename.
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
