-- Stage staging tables. {RAW} is replaced by hydrate.rs with the absolute
-- raw_dir path. Each table mirrors the parquet emitted by stage.rs.

CREATE TABLE actors AS
SELECT * FROM read_parquet('{RAW}/actors.parquet');

CREATE TABLE link_records AS
SELECT * FROM read_parquet('{RAW}/link_records.parquet');

CREATE TABLE link_record_targets AS
SELECT * FROM read_parquet('{RAW}/link_record_targets.parquet');

CREATE TABLE targets AS
SELECT * FROM read_parquet('{RAW}/targets.parquet');
