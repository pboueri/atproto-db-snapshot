-- likes: feed.like records joined to their .subject.uri target, then
-- resolved to the post via posts.uri. subject_uri_id is NULL when the
-- subject isn't a post in posts (orphan — typically a list / feed URI).
--
-- SELECT body — hydrate.rs wraps in CREATE TABLE / chunked INSERT
-- partitioned on r.did_id (the actor doing the like). {CHUNK_PRED}
-- and {CHUNK_PRED_LT} expand to the per-pass predicate or empty.

WITH lt_like AS (
  SELECT did_id, rkey, target_id
  FROM link_record_targets
  WHERE collection = 'app.bsky.feed.like'
    AND rpath      = '.subject.uri'
    {CHUNK_PRED_LT}
)
SELECT
  r.did_id           AS actor_did_id,
  p.uri_id           AS subject_uri_id,
  r.rkey,
  r.created_at
FROM link_records r
JOIN lt_like lt
  ON  lt.did_id = r.did_id
  AND lt.rkey   = r.rkey
JOIN targets t
  ON t.target_id = lt.target_id
LEFT JOIN posts p
  ON p.uri = t.target
WHERE r.collection = 'app.bsky.feed.like'
  {REC_WINDOW}
  {CHUNK_PRED}
