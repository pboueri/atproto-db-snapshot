-- Load entity parquets emitted by stage as DuckDB TABLEs so the
-- resulting snapshot.duckdb is self-contained — consumers can query
-- it without keeping the source parquets around.
--
-- Link tables (follows / blocks / likes / reposts) drop `rkey`: a
-- 13-char TID string per row that adds ~13 bytes × billions of rows
-- and isn't load-bearing for any aggregate (the (src,dst) edge plus
-- created_at is sufficient). It's recoverable from the source parquet
-- if ever needed. created_at is preserved for time-window queries.
--
-- TIME WINDOW: when {WINDOW_LO}/{WINDOW_HI} substitute to real
-- timestamps, `likes` / `reposts` / `posts_from_*` are filtered to
-- created_at within those bounds. `actors` / `blocks` / `follows`
-- represent state ("alice currently follows bob") rather than
-- events, so they always load in full — restricting them by
-- created_at would measure churn-in-window, not graph state. When
-- the window is disabled the placeholders are substituted with
-- 1970-01-01 / 2999-12-31 sentinels so the WHERE clauses become
-- no-ops.
--
-- posts_from_records / posts_from_targets are materialized here so
-- 03_build_posts.sql can dedup against real tables (CHECKPOINT
-- between stages flushes them to disk before the union); 03 then
-- DROPs them once `posts` is built since they're staging artifacts.
--
-- An explicit CHECKPOINT after each load forces dirty pages to disk
-- before the next stage so a memory-pressure event in a later stage
-- can't leave earlier tables in a half-flushed state.

CREATE TABLE actors AS
  SELECT did_id, did, active
  FROM read_parquet('{RAW}/actors.parquet');
CHECKPOINT;

CREATE TABLE follows AS
  SELECT src_did_id, dst_did_id, created_at
  FROM read_parquet('{RAW}/follows.parquet');
CHECKPOINT;

CREATE TABLE blocks AS
  SELECT src_did_id, dst_did_id, created_at
  FROM read_parquet('{RAW}/blocks.parquet');
CHECKPOINT;

CREATE TABLE likes AS
  SELECT actor_did_id, subject_uri_id, created_at
  FROM read_parquet('{RAW}/likes.parquet')
  WHERE created_at BETWEEN TIMESTAMP '{WINDOW_LO}' AND TIMESTAMP '{WINDOW_HI}';
CHECKPOINT;

CREATE TABLE reposts AS
  SELECT actor_did_id, subject_uri_id, created_at
  FROM read_parquet('{RAW}/reposts.parquet')
  WHERE created_at BETWEEN TIMESTAMP '{WINDOW_LO}' AND TIMESTAMP '{WINDOW_HI}';
CHECKPOINT;

CREATE TABLE posts_from_records AS
  SELECT * FROM read_parquet('{RAW}/posts_from_records.parquet')
  WHERE created_at BETWEEN TIMESTAMP '{WINDOW_LO}' AND TIMESTAMP '{WINDOW_HI}';
CHECKPOINT;

CREATE TABLE posts_from_targets AS
  SELECT * FROM read_parquet('{RAW}/posts_from_targets.parquet')
  WHERE created_at BETWEEN TIMESTAMP '{WINDOW_LO}' AND TIMESTAMP '{WINDOW_HI}';
CHECKPOINT;
