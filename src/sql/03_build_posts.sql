-- posts: union of records-derived rows with target-only rows, deduped
-- on uri_id. Records win on conflict because they carry richer fields
-- (reply/quote refs); target-only rows have NULLs for those.
--
-- Both sides already have uri_id pre-computed in stage (xxhash3-64 of
-- the URI string), so dedup is a u64 group-by — no string ops, no
-- 10B-row hash on URIs.
--
-- The `source` column ('record' | 'target_only') is materialized so
-- consumers can distinguish posts whose body data is in scope (record
-- = future text-content spec) from posts we only know about via
-- inbound links (target_only).

CREATE TABLE posts AS
WITH unioned AS (
  SELECT
    uri_id, author_did_id, rkey, created_at,
    reply_root_uri_id, reply_parent_uri_id, quote_uri_id,
    'record' AS source
  FROM posts_from_records
  UNION ALL
  SELECT
    uri_id, author_did_id, rkey, created_at,
    reply_root_uri_id, reply_parent_uri_id, quote_uri_id,
    'target_only' AS source
  FROM posts_from_targets
),
ranked AS (
  -- record < target_only lexicographically, so MIN(source) keeps the
  -- record row when both exist. Aggregating via FIRST_VALUE keyed on
  -- (source = 'record' DESC) gives the same result with one pass —
  -- DuckDB compiles ARG_MAX into a fast aggregate.
  SELECT
    uri_id,
    ARG_MAX(author_did_id, source = 'record')        AS author_did_id,
    ARG_MAX(rkey, source = 'record')                 AS rkey,
    ARG_MAX(created_at, source = 'record')           AS created_at,
    ARG_MAX(reply_root_uri_id, source = 'record')    AS reply_root_uri_id,
    ARG_MAX(reply_parent_uri_id, source = 'record')  AS reply_parent_uri_id,
    ARG_MAX(quote_uri_id, source = 'record')         AS quote_uri_id,
    MIN(source)                                      AS source
  FROM unioned
  GROUP BY 1
)
SELECT * FROM ranked;
