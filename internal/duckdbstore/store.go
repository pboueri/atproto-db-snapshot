package duckdbstore

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"

	"github.com/pboueri/atproto-db-snapshot/internal/registry"
)

// Schemas for current_graph.duckdb per spec §5: actors + edge graph
// (follows, blocks) only. Posts / likes / reposts live in
// current_all.duckdb, which is assembled separately from the parquet
// archive by the incremental `build` path — NOT emitted by CAR backfill.
//
// actors_registry is the resume-safe DID → actor_id map: it's written
// incrementally during backfill so a crashed run can resume with stable
// ids. actors.repo_processed marks DIDs whose repos have been crawled
// to completion — resume re-pulls only the unmarked ones.
const schemaSQL = `
CREATE TABLE IF NOT EXISTS actors (
  actor_id   BIGINT PRIMARY KEY,
  did        VARCHAR NOT NULL UNIQUE,
  handle     VARCHAR,
  display_name VARCHAR,
  description  VARCHAR,
  avatar_cid   VARCHAR,
  created_at   TIMESTAMP,
  indexed_at   TIMESTAMP,
  follower_count       BIGINT,
  following_count      BIGINT,
  post_count           BIGINT,
  likes_given_count    BIGINT,
  likes_received_count BIGINT,
  reposts_received_count BIGINT,
  blocks_given_count   BIGINT,
  repo_processed       BOOLEAN
);

CREATE TABLE IF NOT EXISTS actors_registry (
  actor_id   BIGINT PRIMARY KEY,
  did        VARCHAR NOT NULL UNIQUE,
  first_seen TIMESTAMP
);

CREATE TABLE IF NOT EXISTS follows_current (
  src_id     BIGINT NOT NULL,
  dst_id     BIGINT NOT NULL,
  rkey       VARCHAR NOT NULL,
  created_at TIMESTAMP,
  PRIMARY KEY (src_id, rkey)
);

CREATE TABLE IF NOT EXISTS blocks_current (
  src_id     BIGINT NOT NULL,
  dst_id     BIGINT NOT NULL,
  rkey       VARCHAR NOT NULL,
  created_at TIMESTAMP,
  PRIMARY KEY (src_id, rkey)
);

CREATE TABLE IF NOT EXISTS _meta (
  schema_version   VARCHAR,
  built_at         TIMESTAMP,
  build_mode       VARCHAR,
  did_limit        BIGINT,
  source_did_count BIGINT
);
`

// Store wraps a DuckDB connection.
type Store struct {
	DB   *sql.DB
	path string
}

// Open creates (or reopens) a DuckDB file at path and applies the schema.
// Caller must Close() when done.
func Open(path string, memoryLimit string, threads int) (*Store, error) {
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if memoryLimit != "" {
		if _, err := db.Exec("SET memory_limit = '" + memoryLimit + "'"); err != nil {
			db.Close()
			return nil, fmt.Errorf("set memory_limit: %w", err)
		}
	}
	if threads > 0 {
		if _, err := db.Exec(fmt.Sprintf("SET threads = %d", threads)); err != nil {
			db.Close()
			return nil, fmt.Errorf("set threads: %w", err)
		}
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	// Defensive migration for graph DBs created before resume support
	// landed. CREATE TABLE IF NOT EXISTS is a no-op on existing tables,
	// so the new column needs an explicit ALTER.
	if _, err := db.Exec(`ALTER TABLE actors ADD COLUMN IF NOT EXISTS repo_processed BOOLEAN`); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate actors.repo_processed: %w", err)
	}
	return &Store{DB: db, path: path}, nil
}

// LoadRegistry returns every (actor_id, did, first_seen) row from
// actors_registry. Used on resume to rehydrate the in-memory Registry so
// freshly-minted actor_ids continue past the persisted max.
func (s *Store) LoadRegistry() ([]registry.Entry, error) {
	rows, err := s.DB.Query(`SELECT actor_id, did, first_seen FROM actors_registry ORDER BY actor_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []registry.Entry
	for rows.Next() {
		var e registry.Entry
		var fs sql.NullTime
		if err := rows.Scan(&e.ActorID, &e.DID, &fs); err != nil {
			return nil, err
		}
		if fs.Valid {
			e.FirstSeen = fs.Time
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// LoadProcessedDIDs returns the DIDs whose repos have been crawled in a
// previous run (rows in actors with repo_processed = TRUE). On resume
// these are skipped from the listRepos enumeration.
func (s *Store) LoadProcessedDIDs() (map[string]struct{}, error) {
	rows, err := s.DB.Query(`SELECT did FROM actors WHERE repo_processed = TRUE`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]struct{})
	for rows.Next() {
		var did string
		if err := rows.Scan(&did); err != nil {
			return nil, err
		}
		out[did] = struct{}{}
	}
	return out, rows.Err()
}

// MaterializeTargetOnlyActors fills the actors table with rows for every
// actor_id in actors_registry that isn't already an actor row. Called
// once at end-of-run after all processed-author rows have been written.
// Idempotent: re-running after a crash overwrites prior target-only
// rows by deleting them first.
func (s *Store) MaterializeTargetOnlyActors(now time.Time) error {
	if _, err := s.DB.Exec(
		`DELETE FROM actors WHERE repo_processed IS NOT TRUE`,
	); err != nil {
		return fmt.Errorf("clear target-only: %w", err)
	}
	_, err := s.DB.Exec(`
		INSERT INTO actors (actor_id, did, indexed_at, repo_processed)
		SELECT r.actor_id, r.did, ?, FALSE
		  FROM actors_registry r
		 WHERE r.actor_id NOT IN (SELECT actor_id FROM actors)
	`, now)
	if err != nil {
		return fmt.Errorf("materialize target-only: %w", err)
	}
	return nil
}

// IsComplete reports whether a successful build_graph.Run has previously
// finalized this DB. The presence of any _meta row is the signal — the
// streaming writer never inserts there, only the post-finalize step.
func (s *Store) IsComplete() (bool, error) {
	var n int64
	row := s.DB.QueryRow(`SELECT count(*) FROM _meta`)
	if err := row.Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *Store) Close() error { return s.DB.Close() }

// Checkpoint forces DuckDB to flush the WAL.
func (s *Store) Checkpoint() error {
	_, err := s.DB.Exec("CHECKPOINT")
	return err
}

// WriteMeta records a _meta row.
func (s *Store) WriteMeta(mode string, didLimit, sourceDIDs int64) error {
	_, err := s.DB.Exec(
		`INSERT INTO _meta (schema_version, built_at, build_mode, did_limit, source_did_count) VALUES (?, ?, ?, ?, ?)`,
		"v1", time.Now().UTC(), mode, didLimit, sourceDIDs,
	)
	return err
}

// RecomputeCounts fills denormalized counts on actors. Graph-only columns
// (follower/following/blocks_given). post_count and like_* counts stay
// 0 here — those are populated by the incremental build path that
// replays parquet events into current_all.duckdb.
func (s *Store) RecomputeCounts() error {
	stmts := []string{
		`UPDATE actors SET following_count = COALESCE(c.n, 0)
		   FROM (SELECT src_id, COUNT(*) AS n FROM follows_current GROUP BY 1) c
		  WHERE actors.actor_id = c.src_id`,
		`UPDATE actors SET following_count = 0 WHERE following_count IS NULL`,

		`UPDATE actors SET follower_count = COALESCE(c.n, 0)
		   FROM (SELECT dst_id, COUNT(*) AS n FROM follows_current GROUP BY 1) c
		  WHERE actors.actor_id = c.dst_id`,
		`UPDATE actors SET follower_count = 0 WHERE follower_count IS NULL`,

		`UPDATE actors SET blocks_given_count = COALESCE(c.n, 0)
		   FROM (SELECT src_id, COUNT(*) AS n FROM blocks_current GROUP BY 1) c
		  WHERE actors.actor_id = c.src_id`,
		`UPDATE actors SET blocks_given_count = 0 WHERE blocks_given_count IS NULL`,

		// Zero out the columns that aren't populated by graph-only backfill
		// so they read consistently (0 rather than NULL).
		`UPDATE actors SET post_count = 0 WHERE post_count IS NULL`,
		`UPDATE actors SET likes_given_count = 0 WHERE likes_given_count IS NULL`,
		`UPDATE actors SET likes_received_count = 0 WHERE likes_received_count IS NULL`,
		`UPDATE actors SET reposts_received_count = 0 WHERE reposts_received_count IS NULL`,
	}
	for _, q := range stmts {
		if _, err := s.DB.Exec(q); err != nil {
			return fmt.Errorf("recompute: %s: %w", q, err)
		}
	}
	return nil
}
