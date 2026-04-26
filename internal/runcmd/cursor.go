package runcmd

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite" // pure-Go sqlite driver, registers as "sqlite"
)

// cursorName is the row key used in the cursor table. We use a single named
// cursor for the jetstream stream rather than allowing multiple, because the
// spec requires "only one jetstream connection at a time".
const cursorName = "jetstream"

// cursor wraps the local sqlite checkpoint store.
//
// The cursor table is intentionally tiny — one row keyed by name — so the
// monitor command can read it concurrently without locking issues.
type cursor struct {
	db *sql.DB
}

// openCursor opens (creating if absent) the local cursor sqlite at path.
func openCursor(path string) (*cursor, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)")
	if err != nil {
		return nil, fmt.Errorf("runcmd: open cursor sqlite: %w", err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS cursor (
        name TEXT PRIMARY KEY,
        micros BIGINT NOT NULL,
        updated_at TEXT NOT NULL
    )`); err != nil {
		db.Close()
		return nil, err
	}
	return &cursor{db: db}, nil
}

func (c *cursor) Close() error { return c.db.Close() }

// Load returns the persisted cursor; ok=false means no checkpoint yet.
func (c *cursor) Load(ctx context.Context) (micros int64, ok bool, err error) {
	row := c.db.QueryRowContext(ctx, "SELECT micros FROM cursor WHERE name = ?", cursorName)
	if err := row.Scan(&micros); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return micros, true, nil
}

// Save persists the cursor at the given microsecond timestamp.
func (c *cursor) Save(ctx context.Context, micros int64) error {
	_, err := c.db.ExecContext(ctx,
		`INSERT INTO cursor(name, micros, updated_at) VALUES (?, ?, ?)
         ON CONFLICT(name) DO UPDATE SET micros = excluded.micros, updated_at = excluded.updated_at`,
		cursorName, micros, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}
