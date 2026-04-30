-- Orphan rate: fraction of {TABLE} subject_uri_id values that don't
-- resolve to a row in `posts`. With stage v2, subject_uri_id is the
-- xxhash3-64 of the subject URI string regardless of whether the post
-- is in our snapshot — orphans are detected by the missing posts.uri_id
-- after a LEFT JOIN. {TABLE} is replaced by hydrate.rs (likes/reposts).

SELECT CAST(SUM(CASE WHEN p.uri_id IS NULL THEN 1 ELSE 0 END) AS DOUBLE)
       / GREATEST(COUNT(*), 1)
FROM {TABLE} t
LEFT JOIN posts p ON p.uri_id = t.subject_uri_id;
