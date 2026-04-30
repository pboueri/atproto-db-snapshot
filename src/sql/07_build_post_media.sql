-- post_media: one row per media-rpath target on a feed.post record.
-- `ord` is dense within each post (0..N-1) following the order the
-- targets appeared in the original RecordLinkTargets vec. Joins to
-- posts via (author_did_id, rkey).
--
-- SELECT body — chunked on did_id (the post author). The chunk
-- predicate is applied to media_targets and to the posts join, so each
-- pass hashes only 1/N of posts.

WITH media_targets AS (
  SELECT
    lt.did_id,
    lt.collection,
    lt.rkey,
    lt.ord                       AS source_ord,
    CASE
      WHEN lt.rpath LIKE '.embed.images%'                       THEN 'image'
      WHEN lt.rpath = '.embed.video.video'                       THEN 'video'
      WHEN lt.rpath = '.embed.external.uri'                      THEN 'external'
      WHEN lt.rpath = '.embed.external.thumb'                    THEN 'external_thumb'
      WHEN lt.rpath LIKE '.embed.recordWithMedia.media.images%'  THEN 'image'
    END                          AS kind,
    t.target                     AS ref
  FROM link_record_targets lt
  JOIN targets t ON t.target_id = lt.target_id
  WHERE lt.collection = 'app.bsky.feed.post'
    AND lt.did_id % {CHUNK_N} = {CHUNK_K}
    AND (
         lt.rpath LIKE '.embed.images%'
      OR lt.rpath = '.embed.video.video'
      OR lt.rpath = '.embed.external.uri'
      OR lt.rpath = '.embed.external.thumb'
      OR lt.rpath LIKE '.embed.recordWithMedia.media.images%'
    )
)
SELECT
  p.uri_id,
  CAST(ROW_NUMBER() OVER (
    PARTITION BY m.did_id, m.rkey
    ORDER BY     m.source_ord
  ) - 1 AS INTEGER) AS ord,
  m.kind,
  m.ref
FROM media_targets m
JOIN posts p
  ON  p.author_did_id = m.did_id
  AND p.rkey          = m.rkey
WHERE p.author_did_id % {CHUNK_N} = {CHUNK_K}
