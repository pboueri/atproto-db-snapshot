package duckdbstore

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// Schemas for current_graph.duckdb per spec §5: actors + edge graph
// (follows, blocks) only. Posts / likes / reposts live in
// current_all.duckdb, which is assembled separately from the parquet
// archive by the incremental `build` path — NOT emitted by CAR backfill.
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
  blocks_given_count   BIGINT
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
	return &Store{DB: db, path: path}, nil
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
