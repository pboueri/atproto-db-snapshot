-- Enrich `actors` with creation + deactivation dates from the PLC directory
-- export. The `plc` build phase fetches genesis (create) and tombstone ops and
-- writes them, checkpointed, to raw/<date>/plc/part-NNNNN.parquet with schema
-- (did, kind, ts). Here we pivot to one row per DID, enrich existing
-- (microcosm) actors in place, then INSERT the PLC-only DIDs that microcosm
-- never indexed (they exist on PLC but authored/followed nothing we saw).
--
-- did:web actors are absent from PLC and keep NULL created_at = unknown-age.
-- This stage runs after 04_build_actor_aggs, so PLC-only actors have no
-- actor_aggs row; downstream consumers LEFT-JOIN aggs and treat them as zero.
--
-- hydrate.rs skips this whole stage when raw/<date>/plc/ has no parquet, so a
-- snapshot built without the `plc` phase simply lacks the created_at column
-- rather than failing.

-- pds / handle: take the value from the *latest* op that carries one
-- (arg_max over ts) so migrations/handle-changes are reflected, not genesis.
CREATE TABLE plc_acct AS
  SELECT did,
         MIN(ts) FILTER (WHERE kind = 'create')          AS created_at,
         MAX(ts) FILTER (WHERE kind = 'tombstone')        AS tombstoned_at,
         arg_max(pds, ts)    FILTER (WHERE pds IS NOT NULL)    AS pds,
         arg_max(handle, ts) FILTER (WHERE handle IS NOT NULL) AS handle
  FROM read_parquet('{RAW}/plc/*.parquet')
  GROUP BY did;
CHECKPOINT;

-- 1) enrich existing (microcosm) actors. Existing rows default in_microcosm=TRUE.
ALTER TABLE actors ADD COLUMN created_at    TIMESTAMP;
ALTER TABLE actors ADD COLUMN tombstoned_at TIMESTAMP;
ALTER TABLE actors ADD COLUMN in_microcosm  BOOLEAN DEFAULT TRUE;
ALTER TABLE actors ADD COLUMN pds           VARCHAR;
ALTER TABLE actors ADD COLUMN handle        VARCHAR;

UPDATE actors
  SET created_at    = p.created_at,
      tombstoned_at = p.tombstoned_at,
      pds           = p.pds,
      handle        = p.handle
  FROM plc_acct p
  WHERE actors.did = p.did;

-- 2) insert PLC-only DIDs (present in PLC, absent from microcosm) with fresh
--    did_ids continuing past the current max. active = not-yet-tombstoned.
INSERT INTO actors (did_id, did, active, created_at, tombstoned_at, in_microcosm, pds, handle)
  SELECT CAST((SELECT max(did_id) FROM actors) AS UBIGINT)
           + CAST(ROW_NUMBER() OVER (ORDER BY p.did) AS UBIGINT) AS did_id,
         p.did,
         (p.tombstoned_at IS NULL)                               AS active,
         p.created_at,
         p.tombstoned_at,
         FALSE                                                   AS in_microcosm,
         p.pds,
         p.handle
  FROM plc_acct p
  LEFT JOIN actors a ON a.did = p.did
  WHERE a.did IS NULL;

DROP TABLE plc_acct;
CHECKPOINT;
