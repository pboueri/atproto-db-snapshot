-- actor_aggs: per-actor counts. Pulled directly from the resolved
-- relationship tables (follows / blocks / posts / likes / reposts).
--
-- SELECT body — chunked on actors.did_id. Each inner subquery filters
-- its respective key by the same modulo so every per-chunk hash agg
-- only sees 1/N of its source table.

WITH posts_by_author AS (
  SELECT
    author_did_id AS did_id,
    COUNT(*)                                                 AS posts,
    COUNT(*) FILTER (WHERE reply_parent_uri_id IS NOT NULL)  AS replies_out,
    COUNT(*) FILTER (WHERE quote_uri_id        IS NOT NULL)  AS quotes_out
  FROM posts
  WHERE author_did_id % {CHUNK_N} = {CHUNK_K}
  GROUP BY 1
)
SELECT
  a.did_id,
  COALESCE(f_out.c, 0)         AS follows,
  COALESCE(f_in.c,  0)         AS followers,
  COALESCE(b_out.c, 0)         AS blocks_out,
  COALESCE(b_in.c,  0)         AS blocks_in,
  COALESCE(pba.posts, 0)       AS posts,
  COALESCE(l_out.c, 0)         AS likes_out,
  COALESCE(l_in.c,  0)         AS likes_in,
  COALESCE(r_out.c, 0)         AS reposts_out,
  COALESCE(r_in.c,  0)         AS reposts_in,
  COALESCE(pba.replies_out, 0) AS replies_out,
  COALESCE(pba.quotes_out,  0) AS quotes_out,
  COALESCE(qd.c,    0)         AS quoted_count
FROM actors a
LEFT JOIN (SELECT src_did_id AS did_id, COUNT(*) c
           FROM follows WHERE src_did_id % {CHUNK_N} = {CHUNK_K} GROUP BY 1) f_out USING(did_id)
LEFT JOIN (SELECT dst_did_id AS did_id, COUNT(*) c
           FROM follows WHERE dst_did_id % {CHUNK_N} = {CHUNK_K} GROUP BY 1) f_in  USING(did_id)
LEFT JOIN (SELECT src_did_id AS did_id, COUNT(*) c
           FROM blocks  WHERE src_did_id % {CHUNK_N} = {CHUNK_K} GROUP BY 1) b_out USING(did_id)
LEFT JOIN (SELECT dst_did_id AS did_id, COUNT(*) c
           FROM blocks  WHERE dst_did_id % {CHUNK_N} = {CHUNK_K} GROUP BY 1) b_in  USING(did_id)
LEFT JOIN posts_by_author pba USING(did_id)
LEFT JOIN (SELECT actor_did_id AS did_id, COUNT(*) c
           FROM likes WHERE actor_did_id % {CHUNK_N} = {CHUNK_K} GROUP BY 1) l_out USING(did_id)
LEFT JOIN (SELECT p.author_did_id AS did_id, COUNT(*) c
           FROM likes l JOIN posts p ON p.uri_id = l.subject_uri_id
           WHERE p.author_did_id % {CHUNK_N} = {CHUNK_K} GROUP BY 1) l_in USING(did_id)
LEFT JOIN (SELECT actor_did_id AS did_id, COUNT(*) c
           FROM reposts WHERE actor_did_id % {CHUNK_N} = {CHUNK_K} GROUP BY 1) r_out USING(did_id)
LEFT JOIN (SELECT p.author_did_id AS did_id, COUNT(*) c
           FROM reposts r JOIN posts p ON p.uri_id = r.subject_uri_id
           WHERE p.author_did_id % {CHUNK_N} = {CHUNK_K} GROUP BY 1) r_in USING(did_id)
LEFT JOIN (SELECT p.author_did_id AS did_id, COUNT(*) c
           FROM posts q JOIN posts p ON p.uri_id = q.quote_uri_id
           WHERE p.author_did_id % {CHUNK_N} = {CHUNK_K} GROUP BY 1) qd USING(did_id)
WHERE a.did_id % {CHUNK_N} = {CHUNK_K}
