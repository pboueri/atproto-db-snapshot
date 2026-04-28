-- posts: union of records-derived rows with target-only rows that aren't
-- already represented in records. Records win on conflict (richer fields).

CREATE TABLE posts AS
SELECT
  uri, author_did_id, rkey, created_at,
  reply_root_uri, reply_parent_uri, quote_uri,
  'record' AS source
FROM posts_from_records
UNION ALL
SELECT
  t.uri,
  a.did_id              AS author_did_id,
  t.rkey,
  t.created_at,
  NULL                  AS reply_root_uri,
  NULL                  AS reply_parent_uri,
  NULL                  AS quote_uri,
  'target_only'         AS source
FROM posts_from_targets t
LEFT JOIN actors a ON a.did = t.author_did
WHERE NOT EXISTS (SELECT 1 FROM posts_from_records r WHERE r.uri = t.uri);
