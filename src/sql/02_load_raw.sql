-- Stage staging "tables" as views over the raw parquet files. {RAW} is
-- replaced by hydrate.rs with the absolute raw_dir path. Using VIEW
-- instead of CREATE TABLE keeps the ~400 GB of raw parquet from being
-- duplicated into snapshot.duckdb before any build runs; downstream
-- build_*.sql files reference these names unchanged and DuckDB pushes
-- projections/predicates into the parquet scans on each query.

CREATE VIEW actors AS
SELECT * FROM read_parquet('{RAW}/actors.parquet');

CREATE VIEW link_records AS
SELECT * FROM read_parquet('{RAW}/link_records.parquet');

CREATE VIEW link_record_targets AS
SELECT * FROM read_parquet('{RAW}/link_record_targets.parquet');

CREATE VIEW targets AS
SELECT * FROM read_parquet('{RAW}/targets.parquet');
