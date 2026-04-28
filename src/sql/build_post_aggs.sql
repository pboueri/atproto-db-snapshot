-- post_aggs: per-post engagement counts.

CREATE TABLE post_aggs AS
SELECT
  p.uri,
  COALESCE(l.c,  0) AS likes,
  COALESCE(r.c,  0) AS reposts,
  COALESCE(rp.c, 0) AS replies,
  COALESCE(q.c,  0) AS quotes
FROM posts p
LEFT JOIN (SELECT subject_uri AS uri, COUNT(*) c FROM likes   GROUP BY 1) l USING(uri)
LEFT JOIN (SELECT subject_uri AS uri, COUNT(*) c FROM reposts GROUP BY 1) r USING(uri)
LEFT JOIN (SELECT reply_parent_uri AS uri, COUNT(*) c
           FROM posts WHERE reply_parent_uri IS NOT NULL GROUP BY 1) rp USING(uri)
LEFT JOIN (SELECT quote_uri AS uri, COUNT(*) c
           FROM posts WHERE quote_uri IS NOT NULL GROUP BY 1) q USING(uri);
