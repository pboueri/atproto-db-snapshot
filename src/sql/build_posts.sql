-- posts: union of records-derived rows with target-only rows that aren't
-- already represented in records. Records win on conflict (richer fields).
-- Target-only rows are recovered by filtering `targets` to AT-URIs of
-- post collection, deduping by URI, and parsing author_did / rkey /
-- created_at from the URI itself.

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
)
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
LEFT JOIN actors a ON a.did = o.author_did;
