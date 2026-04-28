-- follows: graph.follow records joined to their .subject target, then
-- the target string (a DID) resolved to a did_id via the actors table.
-- Records whose target is not in actors are dropped (orphan follows).

CREATE TABLE follows AS
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
WHERE r.collection = 'app.bsky.graph.follow';
