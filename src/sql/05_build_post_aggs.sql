-- post_aggs: per-post engagement counts, keyed on uri_id.
--
-- SELECT body — chunked on posts.uri_id. Each inner subquery filters
-- its respective uri_id key by the same modulo so per-chunk hash aggs
-- only see 1/N of likes / reposts / posts.

SELECT
  p.uri_id,
  COALESCE(l.c,  0) AS likes,
  COALESCE(r.c,  0) AS reposts,
  COALESCE(rp.c, 0) AS replies,
  COALESCE(q.c,  0) AS quotes
FROM posts p
LEFT JOIN (SELECT subject_uri_id AS uri_id, COUNT(*) c
           FROM likes
           WHERE subject_uri_id IS NOT NULL
             AND subject_uri_id % {CHUNK_N} = {CHUNK_K}
           GROUP BY 1) l USING(uri_id)
LEFT JOIN (SELECT subject_uri_id AS uri_id, COUNT(*) c
           FROM reposts
           WHERE subject_uri_id IS NOT NULL
             AND subject_uri_id % {CHUNK_N} = {CHUNK_K}
           GROUP BY 1) r USING(uri_id)
LEFT JOIN (SELECT reply_parent_uri_id AS uri_id, COUNT(*) c
           FROM posts
           WHERE reply_parent_uri_id IS NOT NULL
             AND reply_parent_uri_id % {CHUNK_N} = {CHUNK_K}
           GROUP BY 1) rp USING(uri_id)
LEFT JOIN (SELECT quote_uri_id AS uri_id, COUNT(*) c
           FROM posts
           WHERE quote_uri_id IS NOT NULL
             AND quote_uri_id % {CHUNK_N} = {CHUNK_K}
           GROUP BY 1) q USING(uri_id)
WHERE p.uri_id % {CHUNK_N} = {CHUNK_K}
