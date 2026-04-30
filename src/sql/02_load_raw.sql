-- Load entity parquets emitted by stage as DuckDB views. Using VIEW
-- (rather than CREATE TABLE AS) keeps the entity parquets out of
-- snapshot.duckdb so the .duckdb file only contains the deduped
-- `posts` table and the two aggregate tables. Downstream queries push
-- projections + predicates into the parquet readers.

CREATE VIEW actors             AS SELECT * FROM read_parquet('{RAW}/actors.parquet');
CREATE VIEW follows            AS SELECT * FROM read_parquet('{RAW}/follows.parquet');
CREATE VIEW blocks             AS SELECT * FROM read_parquet('{RAW}/blocks.parquet');
CREATE VIEW likes              AS SELECT * FROM read_parquet('{RAW}/likes.parquet');
CREATE VIEW reposts            AS SELECT * FROM read_parquet('{RAW}/reposts.parquet');
CREATE VIEW posts_from_records AS SELECT * FROM read_parquet('{RAW}/posts_from_records.parquet');
CREATE VIEW posts_from_targets AS SELECT * FROM read_parquet('{RAW}/posts_from_targets.parquet');
