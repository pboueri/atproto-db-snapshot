package duckdbstore

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// schemaAllSQL is the schema for current_all.duckdb per spec §5: actors +
// graph + posts + likes_current + reposts_current + _meta + an inline
// actors_registry sidecar so the file is self-decoding (§3).
//
// Mirrors the style of schemaSQL in store.go. The two schemas share the
// names of `actors`, `follows_current`, `blocks_current`, `_meta` so that
// COPY ... FROM DATABASE can pull the graph subset out of current_all into
// current_graph as a verbatim copy.
const schemaAllSQL = `
CREATE TABLE IF NOT EXISTS actors_registry (
  actor_id   BIGINT PRIMARY KEY,
  did        VARCHAR NOT NULL UNIQUE,
  first_seen TIMESTAMP
);

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

CREATE TABLE IF NOT EXISTS posts (
  author_id              BIGINT NOT NULL,
  rkey                   VARCHAR NOT NULL,
  cid                    VARCHAR,
  created_at             TIMESTAMP,
  text                   VARCHAR,
  lang                   VARCHAR,
  reply_root_author_id   BIGINT,
  reply_root_rkey        VARCHAR,
  reply_parent_author_id BIGINT,
  reply_parent_rkey      VARCHAR,
  quote_author_id        BIGINT,
  quote_rkey             VARCHAR,
  embed_type             VARCHAR,
  like_count             BIGINT,
  repost_count           BIGINT,
  reply_count            BIGINT,
  PRIMARY KEY (author_id, rkey)
);

CREATE TABLE IF NOT EXISTS likes_current (
  liker_id          BIGINT NOT NULL,
  rkey              VARCHAR NOT NULL,
  subject_author_id BIGINT NOT NULL,
  subject_rkey      VARCHAR NOT NULL,
  created_at        TIMESTAMP,
  PRIMARY KEY (liker_id, rkey)
);

CREATE TABLE IF NOT EXISTS reposts_current (
  reposter_id       BIGINT NOT NULL,
  rkey              VARCHAR NOT NULL,
  subject_author_id BIGINT NOT NULL,
  subject_rkey      VARCHAR NOT NULL,
  created_at        TIMESTAMP,
  PRIMARY KEY (reposter_id, rkey)
);

CREATE TABLE IF NOT EXISTS _meta (
  schema_version   VARCHAR,
  built_at         TIMESTAMP,
  jetstream_cursor BIGINT,
  build_mode       VARCHAR,
  filter_config    VARCHAR
);
`

// AllStore wraps the DuckDB connection backing current_all.duckdb. Unlike
// the graph-only Store in store.go, this carries the full Option-A schema
// including posts and individual likes/reposts.
type AllStore struct {
	DB   *sql.DB
	path string
}

// OpenAll creates / re-opens current_all.duckdb at path with the §5 schema.
// Uses MaxOpenConns=1 — DuckDB serializes writes on a single connection.
func OpenAll(path string, memoryLimit string, threads int) (*AllStore, error) {
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
	if _, err := db.Exec(schemaAllSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply all schema: %w", err)
	}
	return &AllStore{DB: db, path: path}, nil
}

// Path returns the on-disk path of the database.
func (s *AllStore) Path() string { return s.path }

// Close releases the underlying *sql.DB.
func (s *AllStore) Close() error { return s.DB.Close() }

// Checkpoint flushes the WAL.
func (s *AllStore) Checkpoint() error {
	_, err := s.DB.Exec("CHECKPOINT")
	return err
}

// WriteMeta records a _meta row for current_all.duckdb. filterJSON is the
// `filter_config` blob (canonical JSON of FilterConfig).
func (s *AllStore) WriteMeta(builtAt time.Time, jetstreamCursor int64, mode, filterJSON string) error {
	_, err := s.DB.Exec(
		`INSERT INTO _meta (schema_version, built_at, jetstream_cursor, build_mode, filter_config) VALUES (?, ?, ?, ?, ?)`,
		"v1", builtAt.UTC(), jetstreamCursor, mode, filterJSON,
	)
	return err
}

// LastBuiltAt returns the most recent _meta.built_at, or zero time if the
// table is empty (fresh database).
func (s *AllStore) LastBuiltAt() (time.Time, error) {
	row := s.DB.QueryRow(`SELECT max(built_at) FROM _meta`)
	var t sql.NullTime
	if err := row.Scan(&t); err != nil {
		return time.Time{}, err
	}
	if !t.Valid {
		return time.Time{}, nil
	}
	return t.Time.UTC(), nil
}

// CountRow returns COUNT(*) on a table; 0 if the table is empty / missing.
func (s *AllStore) CountRow(table string) (int64, error) {
	row := s.DB.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %s`, table))
	var n int64
	if err := row.Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// RegistryRowCount counts rows in actors_registry.
func (s *AllStore) RegistryRowCount() (int64, error) {
	return s.CountRow("actors_registry")
}
