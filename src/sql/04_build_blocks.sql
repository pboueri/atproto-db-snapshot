-- blocks: same shape as follows. SELECT-body convention identical to
-- 03_build_follows.sql — hydrate.rs wraps it in CREATE TABLE / chunked
-- INSERT. {CHUNK_PRED} expands to `AND r.did_id % N = k` or empty.

WITH lt_block AS (
  SELECT did_id, rkey, target_id
  FROM link_record_targets
  WHERE collection = 'app.bsky.graph.block'
    AND rpath      = '.subject'
    {CHUNK_PRED_LT}
)
SELECT
  r.did_id           AS src_did_id,
  a.did_id           AS dst_did_id,
  r.rkey,
  r.created_at
FROM link_records r
JOIN lt_block lt
  ON  lt.did_id = r.did_id
  AND lt.rkey   = r.rkey
JOIN targets t
  ON t.target_id = lt.target_id
JOIN actors a
  ON a.did = t.target
WHERE r.collection = 'app.bsky.graph.block'
  {CHUNK_PRED}
