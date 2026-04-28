-- posts_from_records: feed.post records, with author DID resolved from
-- actors and reply/quote URIs pulled in via LEFT JOINs against the
-- record's targets at the well-known rpaths.

CREATE TABLE posts_from_records AS
WITH target_at AS (
  SELECT
    lt.did_id,
    lt.collection,
    lt.rkey,
    lt.rpath,
    t.target
  FROM link_record_targets lt
  JOIN targets t ON t.target_id = lt.target_id
)
SELECT
  'at://' || a.did || '/' || r.collection || '/' || r.rkey AS uri,
  r.did_id            AS author_did_id,
  r.rkey,
  r.created_at,
  rr.target           AS reply_root_uri,
  rp.target           AS reply_parent_uri,
  COALESCE(qe.target, qrwm.target) AS quote_uri
FROM link_records r
JOIN actors a
  ON a.did_id = r.did_id
LEFT JOIN target_at rr
  ON  rr.did_id     = r.did_id
  AND rr.collection = r.collection
  AND rr.rkey       = r.rkey
  AND rr.rpath      = '.reply.root.uri'
LEFT JOIN target_at rp
  ON  rp.did_id     = r.did_id
  AND rp.collection = r.collection
  AND rp.rkey       = r.rkey
  AND rp.rpath      = '.reply.parent.uri'
LEFT JOIN target_at qe
  ON  qe.did_id     = r.did_id
  AND qe.collection = r.collection
  AND qe.rkey       = r.rkey
  AND qe.rpath      = '.embed.record.uri'
LEFT JOIN target_at qrwm
  ON  qrwm.did_id     = r.did_id
  AND qrwm.collection = r.collection
  AND qrwm.rkey       = r.rkey
  AND qrwm.rpath      = '.embed.recordWithMedia.record.record.uri'
WHERE r.collection = 'app.bsky.feed.post';
