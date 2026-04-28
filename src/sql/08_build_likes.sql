-- likes: feed.like records joined to their .subject.uri target, then
-- resolved to the post via posts.uri. subject_uri_id is NULL when the
-- subject isn't a post in posts (orphan — typically a list / feed URI).

CREATE TABLE likes AS
SELECT
  r.did_id           AS actor_did_id,
  p.uri_id           AS subject_uri_id,
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
LEFT JOIN posts p
  ON p.uri = t.target
WHERE r.collection = 'app.bsky.feed.like';
