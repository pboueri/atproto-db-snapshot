-- Sample analytics queries against current_all.duckdb.
--
-- Run locally:
--   duckdb data/current_all.duckdb < queries.sql
--
-- Or, against a published snapshot:
--   duckdb -c "INSTALL httpfs; LOAD httpfs;
--              ATTACH 'https://pub-<hash>.r2.dev/current_all-v1.duckdb' AS s (READ_ONLY);
--              SELECT count(*) FROM s.posts;"
--
-- Caveat: in a sample (e.g. 10k-DID bootstrap), `posts.like_count` reflects
-- only likes given by the crawled DIDs — not the global like count.

-- 1. Proportion of posts that received exactly 1 like.
SELECT
  COUNT(*)                                            AS total_posts,
  SUM(CASE WHEN like_count = 1 THEN 1 ELSE 0 END)     AS posts_with_exactly_1_like,
  SUM(CASE WHEN like_count >= 1 THEN 1 ELSE 0 END)    AS posts_with_at_least_1_like,
  ROUND(100.0 * SUM(CASE WHEN like_count = 1 THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_exactly_1,
  ROUND(100.0 * SUM(CASE WHEN like_count >= 1 THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_at_least_1
FROM posts;

-- 2. Of authors who posted, what fraction has at least one post with exactly 1 like?
WITH per_author AS (
  SELECT author_id,
         SUM(CASE WHEN like_count = 1 THEN 1 ELSE 0 END) AS n1
  FROM posts GROUP BY author_id
)
SELECT
  COUNT(*) AS authors_who_posted,
  SUM(CASE WHEN n1 >= 1 THEN 1 ELSE 0 END) AS authors_with_a_1_like_post,
  ROUND(100.0 * SUM(CASE WHEN n1 >= 1 THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct
FROM per_author;

-- 3. Of authors who posted, what fraction has total likes across all posts = 1?
WITH per_author AS (
  SELECT author_id, SUM(like_count) AS total_likes
  FROM posts GROUP BY author_id
)
SELECT
  COUNT(*) AS authors_who_posted,
  SUM(CASE WHEN total_likes = 0 THEN 1 ELSE 0 END) AS authors_0_total_likes,
  SUM(CASE WHEN total_likes = 1 THEN 1 ELSE 0 END) AS authors_exactly_1_total_like,
  ROUND(100.0 * SUM(CASE WHEN total_likes = 1 THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct
FROM per_author;

-- 4. Top 10 most-followed actors.
SELECT a.handle, a.did, a.follower_count
FROM actors a
ORDER BY a.follower_count DESC
LIMIT 10;

-- 5. Top 10 posts by like_count.
SELECT a.handle, p.text, p.like_count, p.created_at
FROM posts p
JOIN actors a ON a.actor_id = p.author_id
ORDER BY p.like_count DESC
LIMIT 10;

-- 6. Sample-coverage sanity: what fraction of likes point at posts in our table?
SELECT
  (SELECT COUNT(*) FROM likes_current) AS likes_total,
  (SELECT COUNT(*) FROM likes_current l
     WHERE EXISTS (SELECT 1 FROM posts p
                   WHERE p.author_id = l.subject_author_id
                     AND p.rkey      = l.subject_rkey)) AS likes_on_our_posts;
