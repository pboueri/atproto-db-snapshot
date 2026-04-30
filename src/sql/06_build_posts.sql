-- posts: union of records-derived rows with target-only rows that aren't
-- already represented in records. Records win on conflict (richer fields).
-- Target-only rows are recovered by filtering `targets` to AT-URIs of
-- post collection, deduping by URI, and parsing author_did / rkey /
-- created_at from the URI itself.
--
-- Each unique uri gets a sequential uri_id. posts is the source of truth
-- for uri ↔ uri_id; every other table references posts via uri_id and
-- never stores a post URI string.
--
-- Self-references (reply_root / reply_parent / quote) are resolved
-- against `ranked` itself. The naive shape — three independent LEFT
-- JOINs on `ranked` — builds three full-post hash tables. Instead we
-- stack the three reference columns into `ref_pairs`, do a single LEFT
-- JOIN against `ranked` to translate uri → uri_id, and pivot the result
-- back into three columns. One hash table on `ranked` instead of three.

CREATE TABLE posts AS
WITH target_post_uris AS (
  SELECT DISTINCT t.target AS uri
  FROM targets t
  WHERE t.target LIKE 'at://%/app.bsky.feed.post/%'
),
target_only AS (
  SELECT
    u.uri,
    split_part(substring(u.uri, 6), '/', 1) AS author_did,
    split_part(substring(u.uri, 6), '/', 3) AS rkey
  FROM target_post_uris u
  WHERE NOT EXISTS (SELECT 1 FROM posts_from_records r WHERE r.uri = u.uri)
    {TID_WINDOW}
),
unioned AS (
  SELECT
    uri, author_did_id, rkey, created_at,
    reply_root_uri, reply_parent_uri, quote_uri,
    'record' AS source
  FROM posts_from_records
  UNION ALL
  SELECT
    o.uri,
    a.did_id              AS author_did_id,
    o.rkey,
    tid_to_ts(o.rkey)     AS created_at,
    NULL                  AS reply_root_uri,
    NULL                  AS reply_parent_uri,
    NULL                  AS quote_uri,
    'target_only'         AS source
  FROM target_only o
  LEFT JOIN actors a ON a.did = o.author_did
),
ranked AS (
  SELECT
    *,
    CAST(ROW_NUMBER() OVER (ORDER BY uri) AS BIGINT) AS uri_id
  FROM unioned
),
ref_pairs AS (
  SELECT uri_id, 'root'   AS k, reply_root_uri   AS u FROM ranked WHERE reply_root_uri   IS NOT NULL
  UNION ALL
  SELECT uri_id, 'parent' AS k, reply_parent_uri AS u FROM ranked WHERE reply_parent_uri IS NOT NULL
  UNION ALL
  SELECT uri_id, 'quote'  AS k, quote_uri        AS u FROM ranked WHERE quote_uri        IS NOT NULL
),
resolved AS (
  SELECT rp.uri_id, rp.k, l.uri_id AS ref_id
  FROM ref_pairs rp
  LEFT JOIN ranked l ON l.uri = rp.u
),
pivoted AS (
  SELECT
    uri_id,
    MAX(CASE WHEN k = 'root'   THEN ref_id END) AS reply_root_uri_id,
    MAX(CASE WHEN k = 'parent' THEN ref_id END) AS reply_parent_uri_id,
    MAX(CASE WHEN k = 'quote'  THEN ref_id END) AS quote_uri_id
  FROM resolved
  GROUP BY 1
)
SELECT
  r.uri_id,
  r.uri,
  r.author_did_id,
  r.rkey,
  r.created_at,
  p.reply_root_uri_id,
  p.reply_parent_uri_id,
  p.quote_uri_id,
  r.source
FROM ranked r
LEFT JOIN pivoted p USING (uri_id);
