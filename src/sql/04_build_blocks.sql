-- blocks: same shape as follows.

CREATE TABLE blocks AS
SELECT
  r.did_id           AS src_did_id,
  a.did_id           AS dst_did_id,
  r.rkey,
  r.created_at
FROM link_records r
JOIN link_record_targets lt
  ON  lt.did_id     = r.did_id
  AND lt.collection = r.collection
  AND lt.rkey       = r.rkey
  AND lt.rpath      = '.subject'
JOIN targets t
  ON t.target_id = lt.target_id
JOIN actors a
  ON a.did = t.target
WHERE r.collection = 'app.bsky.graph.block';
