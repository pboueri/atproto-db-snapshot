-- reposts: same shape as likes. SELECT body — chunked on r.did_id.

WITH lt_repost AS (
  SELECT did_id, rkey, target_id
  FROM link_record_targets
  WHERE collection = 'app.bsky.feed.repost'
    AND rpath      = '.subject.uri'
    {CHUNK_PRED_LT}
)
SELECT
  r.did_id           AS actor_did_id,
  p.uri_id           AS subject_uri_id,
  r.rkey,
  r.created_at
FROM link_records r
JOIN lt_repost lt
  ON  lt.did_id = r.did_id
  AND lt.rkey   = r.rkey
JOIN targets t
  ON t.target_id = lt.target_id
LEFT JOIN posts p
  ON p.uri = t.target
WHERE r.collection = 'app.bsky.feed.repost'
  {REC_WINDOW}
  {CHUNK_PRED}
