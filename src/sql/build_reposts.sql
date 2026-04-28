-- reposts: same shape as likes.

CREATE TABLE reposts AS
SELECT
  r.did_id           AS actor_did_id,
  t.target           AS subject_uri,
  r.rkey,
  r.created_at
FROM link_records r
JOIN link_record_targets lt
  ON  lt.did_id     = r.did_id
  AND lt.collection = r.collection
  AND lt.rkey       = r.rkey
  AND lt.rpath      = '.subject.uri'
JOIN targets t
  ON t.target_id = lt.target_id
WHERE r.collection = 'app.bsky.feed.repost';
