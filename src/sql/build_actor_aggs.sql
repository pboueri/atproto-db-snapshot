-- actor_aggs: per-actor counts. Pulled directly from the resolved
-- relationship tables (follows / blocks / posts / likes / reposts).

CREATE TABLE actor_aggs AS
SELECT
  a.did_id,
  COALESCE(f_out.c, 0)  AS follows,
  COALESCE(f_in.c,  0)  AS followers,
  COALESCE(b_out.c, 0)  AS blocks_out,
  COALESCE(b_in.c,  0)  AS blocks_in,
  COALESCE(p.c,     0)  AS posts,
  COALESCE(l_out.c, 0)  AS likes_out,
  COALESCE(l_in.c,  0)  AS likes_in,
  COALESCE(r_out.c, 0)  AS reposts_out,
  COALESCE(r_in.c,  0)  AS reposts_in,
  COALESCE(rep.c,   0)  AS replies_out,
  COALESCE(qo.c,    0)  AS quotes_out,
  COALESCE(qd.c,    0)  AS quoted_count
FROM actors a
LEFT JOIN (SELECT src_did_id AS did_id, COUNT(*) c FROM follows GROUP BY 1) f_out USING(did_id)
LEFT JOIN (SELECT dst_did_id AS did_id, COUNT(*) c FROM follows GROUP BY 1) f_in  USING(did_id)
LEFT JOIN (SELECT src_did_id AS did_id, COUNT(*) c FROM blocks  GROUP BY 1) b_out USING(did_id)
LEFT JOIN (SELECT dst_did_id AS did_id, COUNT(*) c FROM blocks  GROUP BY 1) b_in  USING(did_id)
LEFT JOIN (SELECT author_did_id AS did_id, COUNT(*) c FROM posts GROUP BY 1) p USING(did_id)
LEFT JOIN (SELECT actor_did_id AS did_id, COUNT(*) c FROM likes GROUP BY 1) l_out USING(did_id)
LEFT JOIN (SELECT p.author_did_id AS did_id, COUNT(*) c
           FROM likes l JOIN posts p ON p.uri = l.subject_uri GROUP BY 1) l_in USING(did_id)
LEFT JOIN (SELECT actor_did_id AS did_id, COUNT(*) c FROM reposts GROUP BY 1) r_out USING(did_id)
LEFT JOIN (SELECT p.author_did_id AS did_id, COUNT(*) c
           FROM reposts r JOIN posts p ON p.uri = r.subject_uri GROUP BY 1) r_in USING(did_id)
LEFT JOIN (SELECT author_did_id AS did_id, COUNT(*) c FROM posts WHERE reply_parent_uri IS NOT NULL GROUP BY 1) rep USING(did_id)
LEFT JOIN (SELECT author_did_id AS did_id, COUNT(*) c FROM posts WHERE quote_uri IS NOT NULL GROUP BY 1) qo USING(did_id)
LEFT JOIN (SELECT p.author_did_id AS did_id, COUNT(*) c
           FROM posts q JOIN posts p ON p.uri = q.quote_uri GROUP BY 1) qd USING(did_id);
