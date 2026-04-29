-- posts: union of records-derived rows with target-only rows that aren't
-- already represented in records. Records win on conflict (richer fields).
-- Target-only rows are recovered by filtering `targets` to AT-URIs of
-- post collection, deduping by URI, and parsing author_did / rkey /
-- created_at from the URI itself.
--
-- Each unique uri gets a sequential uri_id. posts is the source of truth
-- for uri ↔ uri_id; every other table references posts via uri_id and
-- never stores a post URI string. Reply / quote self-references are
-- resolved via a self-join on uri (NULL when the referenced post is not
-- present, which should be rare since constellation indexes back-links).

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
)
SELECT
  r.uri_id,
  r.uri,
  r.author_did_id,
  r.rkey,
  r.created_at,
  rr.uri_id AS reply_root_uri_id,
  rp.uri_id AS reply_parent_uri_id,
  q.uri_id  AS quote_uri_id,
  r.source
FROM ranked r
LEFT JOIN ranked rr ON rr.uri = r.reply_root_uri
LEFT JOIN ranked rp ON rp.uri = r.reply_parent_uri
LEFT JOIN ranked q  ON q.uri  = r.quote_uri;
