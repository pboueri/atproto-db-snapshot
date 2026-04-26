package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2" // register the duckdb driver

	"github.com/pboueri/atproto-db-snapshot/internal/model"
)

// store wraps the local social_graph.duckdb file with the schema and the
// per-DID completion tracking the bootstrap orchestrator needs.
//
// All exported methods are safe for concurrent use.
type store struct {
	mu sync.Mutex
	db *sql.DB
}

// schemaSQL is the canonical schema. INSERT OR IGNORE on the natural keys
// keeps re-runs idempotent: a follow-up bootstrap that revisits a DID will
// drop duplicates rather than crashing on PK conflicts.
const schemaSQL = `
CREATE TABLE IF NOT EXISTS actors (
    did_id BIGINT PRIMARY KEY,
    did TEXT NOT NULL,
    handle TEXT,
    display_name TEXT,
    description TEXT,
    avatar_cid TEXT,
    banner_cid TEXT,
    created_at TIMESTAMP,
    indexed_at TIMESTAMP,
    source TEXT
);
CREATE TABLE IF NOT EXISTS follows (
    src_did_id BIGINT NOT NULL,
    rkey TEXT NOT NULL,
    dst_did_id BIGINT NOT NULL,
    src_did TEXT NOT NULL,
    dst_did TEXT NOT NULL,
    created_at TIMESTAMP,
    indexed_at TIMESTAMP,
    source TEXT,
    PRIMARY KEY (src_did_id, rkey)
);
CREATE TABLE IF NOT EXISTS blocks (
    src_did_id BIGINT NOT NULL,
    rkey TEXT NOT NULL,
    dst_did_id BIGINT NOT NULL,
    src_did TEXT NOT NULL,
    dst_did TEXT NOT NULL,
    created_at TIMESTAMP,
    indexed_at TIMESTAMP,
    source TEXT,
    PRIMARY KEY (src_did_id, rkey)
);
CREATE TABLE IF NOT EXISTS bootstrap_progress (
    did TEXT PRIMARY KEY,
    completed_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS bootstrap_meta (
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    plc_endpoint TEXT,
    constellation_endpoint TEXT
);
`

// openStore opens (creating if absent) the local bootstrap duckdb at path.
func openStore(path string) (*store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: open duckdb: %w", err)
	}
	// DuckDB's database/sql driver is single-writer; let the pool reflect that.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schemaSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("bootstrap: apply schema: %w", err)
	}
	return &store{db: db}, nil
}

func (s *store) Close() error { return s.db.Close() }

// CompletedDIDs returns the set of DIDs already finished by a prior run.
func (s *store) CompletedDIDs(ctx context.Context) (map[string]struct{}, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT did FROM bootstrap_progress")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]struct{})
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		out[d] = struct{}{}
	}
	return out, rows.Err()
}

// MarkStarted records when the run began (idempotent on rerun).
func (s *store) MarkStarted(ctx context.Context, plc, constellation string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int
	if err := s.db.QueryRowContext(ctx, "SELECT count(*) FROM bootstrap_meta").Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	_, err := s.db.ExecContext(ctx,
		"INSERT INTO bootstrap_meta(started_at, plc_endpoint, constellation_endpoint) VALUES (?, ?, ?)",
		time.Now().UTC(), plc, constellation)
	return err
}

// MarkFinished sets completed_at on the meta row (latest write wins).
func (s *store) MarkFinished(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, "UPDATE bootstrap_meta SET completed_at = ?", time.Now().UTC())
	return err
}

// didBundle is what a worker hands to the writer goroutine: everything we
// need to atomically commit one DID's worth of work.
type didBundle struct {
	did     string
	profile *model.Profile
	follows []model.Follow
	blocks  []model.Block
}

// commitBundle writes a bundle in a transaction and marks the DID complete.
func (s *store) commitBundle(ctx context.Context, b didBundle) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()

	if b.profile != nil {
		p := b.profile
		if _, err := tx.ExecContext(ctx,
			`INSERT OR REPLACE INTO actors(did_id, did, handle, display_name, description, avatar_cid, banner_cid, created_at, indexed_at, source)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			p.DIDID, p.DID, p.Handle, p.DisplayName, p.Description, p.AvatarCID, p.BannerCID, p.CreatedAt, p.IndexedAt, p.Source,
		); err != nil {
			return fmt.Errorf("insert actor %s: %w", b.did, err)
		}
	} else {
		// We always want at least the DID row in actors so downstream joins
		// don't lose the DID even if the user has no public profile.
		if _, err := tx.ExecContext(ctx,
			`INSERT OR IGNORE INTO actors(did_id, did, indexed_at, source) VALUES (?, ?, ?, ?)`,
			internDID(b.did), b.did, time.Now().UTC(), model.SourceBootstrap,
		); err != nil {
			return fmt.Errorf("insert minimal actor %s: %w", b.did, err)
		}
	}

	for _, f := range b.follows {
		if _, err := tx.ExecContext(ctx,
			`INSERT OR REPLACE INTO follows(src_did_id, rkey, dst_did_id, src_did, dst_did, created_at, indexed_at, source)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			f.SrcDIDID, f.RKey, f.DstDIDID, f.SrcDID, f.DstDID, f.CreatedAt, f.IndexedAt, f.Source,
		); err != nil {
			return fmt.Errorf("insert follow %s/%s: %w", b.did, f.RKey, err)
		}
	}
	for _, bl := range b.blocks {
		if _, err := tx.ExecContext(ctx,
			`INSERT OR REPLACE INTO blocks(src_did_id, rkey, dst_did_id, src_did, dst_did, created_at, indexed_at, source)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			bl.SrcDIDID, bl.RKey, bl.DstDIDID, bl.SrcDID, bl.DstDID, bl.CreatedAt, bl.IndexedAt, bl.Source,
		); err != nil {
			return fmt.Errorf("insert block %s/%s: %w", b.did, bl.RKey, err)
		}
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT OR REPLACE INTO bootstrap_progress(did, completed_at) VALUES (?, ?)`,
		b.did, time.Now().UTC()); err != nil {
		return fmt.Errorf("mark progress %s: %w", b.did, err)
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	tx = nil
	return nil
}

// Counts returns the row counts for the major tables; used by the monitor.
func (s *store) Counts(ctx context.Context) (Counts, error) {
	var c Counts
	row := s.db.QueryRowContext(ctx, `SELECT
        (SELECT count(*) FROM actors),
        (SELECT count(*) FROM follows),
        (SELECT count(*) FROM blocks),
        (SELECT count(*) FROM bootstrap_progress)`)
	if err := row.Scan(&c.Actors, &c.Follows, &c.Blocks, &c.CompletedDIDs); err != nil {
		return Counts{}, err
	}
	return c, nil
}

// Counts is a small struct returned to the monitor.
type Counts struct {
	Actors        int64
	Follows       int64
	Blocks        int64
	CompletedDIDs int64
}

// errStoreClosed is returned when bundles arrive after Close.
var errStoreClosed = errors.New("bootstrap: store closed")

func internDID(did string) int64 {
	// Avoid pulling intern as a dependency in this file's interface; route
	// through the helper to keep the package surface small.
	return didInterner(did)
}
