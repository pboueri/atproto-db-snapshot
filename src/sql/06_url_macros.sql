-- Lookup-by-id URL macros. These wrap a correlated subquery against
-- `posts` + `actors`, so they have to be created AFTER 02_load_raw
-- has materialized those tables — DuckDB resolves macro body
-- references at CREATE time. The pure-string variants in
-- 01_macros.sql remain the fast path when posts/actors are already
-- in your FROM clause; these are the convenience layer for ad-hoc
-- expansion of a uri_id or did_id sitting alone in another table
-- (e.g. `SELECT post_url_by_id(subject_uri_id) FROM likes WHERE ...`).
--
-- LIMIT 1 is defensive — uri_id / did_id are unique per the
-- validate_snapshot battery — but DuckDB requires a single-row
-- result for a scalar subquery and the explicit LIMIT documents
-- the intent.

CREATE OR REPLACE MACRO post_url_by_id(p_uri_id) AS (
  SELECT 'https://bsky.app/profile/' || a.did || '/post/' || p.rkey
  FROM posts p JOIN actors a ON a.did_id = p.author_did_id
  WHERE p.uri_id = p_uri_id
  LIMIT 1
);

CREATE OR REPLACE MACRO post_at_uri_by_id(p_uri_id) AS (
  SELECT 'at://' || a.did || '/app.bsky.feed.post/' || p.rkey
  FROM posts p JOIN actors a ON a.did_id = p.author_did_id
  WHERE p.uri_id = p_uri_id
  LIMIT 1
);

CREATE OR REPLACE MACRO actor_url_by_id(d_id) AS (
  SELECT 'https://bsky.app/profile/' || did
  FROM actors
  WHERE did_id = d_id
  LIMIT 1
);
