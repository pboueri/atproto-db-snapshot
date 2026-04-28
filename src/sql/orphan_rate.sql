-- Orphan rate: fraction of {TABLE} subject_uri values that don't resolve
-- against the posts table. {TABLE} is replaced by hydrate.rs.

SELECT CAST(SUM(CASE WHEN p.uri IS NULL THEN 1 ELSE 0 END) AS DOUBLE) /
       GREATEST(COUNT(*), 1)
FROM {TABLE} l
LEFT JOIN posts p ON p.uri = l.subject_uri;
