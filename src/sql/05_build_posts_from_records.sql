-- posts_from_records: feed.post records, with author DID resolved from
-- actors and reply/quote URIs pulled in via the record's targets at the
-- four well-known rpaths.
--
-- This is a SELECT body wrapped by hydrate.rs in CREATE TABLE / chunked
-- INSERT loops partitioned on did_id. {CHUNK_N} / {CHUNK_K} expand to
-- the bucket count and current pass index; when not chunked they
-- expand to 1 / 0 and `did_id % 1 = 0` is folded away.
--
-- Plan shape matters here. The naive form
--   FROM lt JOIN targets JOIN windowed_posts ...
-- lets DuckDB pick `targets` as a hash side — and `targets` is the
-- master URI dictionary (billions of unique URIs, many GB hash) — so
-- the runner OOMs even on a 1% dry-run, because the chunk filter only
-- shrinks lt and windowed_posts, not the global targets table.
--
-- Instead: filter lt first via EXISTS against windowed_posts, then
-- join targets. post_lt is now the windowed × chunked × 4-rpath
-- subset (~few million rows per chunk), and DuckDB has no choice but
-- to hash post_lt (small) and probe targets (huge but read-only).
--
-- The four rpaths are scalar (one target per record at most), so
-- per-record reply/quote columns recover via a single GROUP BY on
-- (did_id, rkey) using conditional aggregation — replacing what used
-- to be five LEFT JOINs against the same CTE.

-- MATERIALIZED on windowed_posts and post_lt is load-bearing: without
-- it DuckDB inlines the CTE bodies into one plan tree and is free to
-- pick `targets` (the global URI dictionary, ~10B rows) as the hash
-- build side of the targets join, which OOMs the runner even on a 1%
-- dry-run. MATERIALIZED forces post_lt to be computed first (small —
-- the windowed × chunked × 4-rpath subset), and the targets join
-- then has only one viable build side, the small one.
WITH windowed_posts AS MATERIALIZED (
  SELECT r.did_id, r.rkey, r.created_at
  FROM link_records r
  WHERE r.collection = 'app.bsky.feed.post'
    AND r.did_id % {CHUNK_N} = {CHUNK_K}
    {REC_WINDOW}
),
post_lt AS MATERIALIZED (
  SELECT lt.did_id, lt.rkey, lt.rpath, lt.target_id
  FROM link_record_targets lt
  WHERE lt.collection = 'app.bsky.feed.post'
    AND lt.did_id % {CHUNK_N} = {CHUNK_K}
    AND lt.rpath IN (
      '.reply.root.uri',
      '.reply.parent.uri',
      '.embed.record.uri',
      '.embed.recordWithMedia.record.record.uri'
    )
    AND EXISTS (
      SELECT 1 FROM windowed_posts wp
      WHERE wp.did_id = lt.did_id
        AND wp.rkey   = lt.rkey
    )
),
post_target_at AS (
  SELECT
    plt.did_id,
    plt.rkey,
    MAX(CASE WHEN plt.rpath = '.reply.root.uri'                          THEN t.target END) AS reply_root_uri,
    MAX(CASE WHEN plt.rpath = '.reply.parent.uri'                        THEN t.target END) AS reply_parent_uri,
    MAX(CASE WHEN plt.rpath = '.embed.record.uri'                        THEN t.target END) AS quote_embed_uri,
    MAX(CASE WHEN plt.rpath = '.embed.recordWithMedia.record.record.uri' THEN t.target END) AS quote_rwm_uri
  FROM post_lt plt
  JOIN targets t ON t.target_id = plt.target_id
  GROUP BY 1, 2
)
SELECT
  'at://' || a.did || '/app.bsky.feed.post/' || wp.rkey AS uri,
  wp.did_id            AS author_did_id,
  wp.rkey,
  wp.created_at,
  ta.reply_root_uri,
  ta.reply_parent_uri,
  COALESCE(ta.quote_embed_uri, ta.quote_rwm_uri) AS quote_uri
FROM windowed_posts wp
JOIN actors a
  ON a.did_id = wp.did_id
LEFT JOIN post_target_at ta
  ON  ta.did_id = wp.did_id
  AND ta.rkey   = wp.rkey
