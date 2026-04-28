-- Orphan rate: fraction of {TABLE} subject_uri_id values that didn't
-- resolve to a row in posts (NULL after the LEFT JOIN in build_{TABLE}).
-- {TABLE} is replaced by hydrate.rs.

SELECT CAST(SUM(CASE WHEN subject_uri_id IS NULL THEN 1 ELSE 0 END) AS DOUBLE) /
       GREATEST(COUNT(*), 1)
FROM {TABLE};
