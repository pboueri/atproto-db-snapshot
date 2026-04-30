-- follows: graph.follow records joined to their .subject target, then
-- the target string (a DID) resolved to a did_id via the actors table.
-- Records whose target is not in actors are dropped (orphan follows).
--
-- This file is a *SELECT body*, not a full statement. hydrate.rs wraps
-- it in either `CREATE TABLE follows AS <body>` (single-shot) or a
-- chunked CREATE-then-INSERT loop partitioned on r.did_id. {CHUNK_PRED}
-- expands to the per-bucket predicate (`AND r.did_id % N = k`) or empty.
--
-- lt_follow pre-filters link_record_targets to (collection='follow',
-- rpath='.subject') so the join hash builds on a small slice of the
-- 1B+-row parquet rather than the whole thing — DuckDB pushes the
-- predicate to the parquet scan, which is cheap and shrinks the hash.

WITH lt_follow AS (
  SELECT did_id, rkey, target_id
  FROM link_record_targets
  WHERE collection = 'app.bsky.graph.follow'
    AND rpath      = '.subject'
    {CHUNK_PRED_LT}
)
SELECT
  r.did_id           AS src_did_id,
  a.did_id           AS dst_did_id,
  r.rkey,
  r.created_at
FROM link_records r
JOIN lt_follow lt
  ON  lt.did_id = r.did_id
  AND lt.rkey   = r.rkey
JOIN targets t
  ON t.target_id = lt.target_id
JOIN actors a
  ON a.did = t.target
WHERE r.collection = 'app.bsky.graph.follow'
  {CHUNK_PRED}
