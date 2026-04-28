use anyhow::{Context, Result};
use rocksdb::{IteratorMode, ReadOptions, DB};
use std::path::PathBuf;

use crate::config::Config;
use crate::rocks::open::open_readonly;
use crate::rocks::schema::{
    self as rs, Collection, Did, DidId, DidIdValue, RKey, RPath, RecordLinkKey,
    RecordLinkTargets, Target, TargetId, TargetKey,
};
use crate::tid;
use crate::writers::{ActorWriter, LinkRecordTargetsWriter, LinkRecordWriter, TargetWriter};

const COL_PROFILE: &str = "app.bsky.actor.profile";

pub struct StageOutcome {
    pub raw_dir: PathBuf,
    pub counts: Vec<(String, u64)>,
}

pub async fn run(cfg: &Config, snapshot_date: &str) -> Result<StageOutcome> {
    let raw_dir = cfg.raw_dir(snapshot_date);
    crate::writers::common::ensure_dir(&raw_dir)?;

    let rocks_dir = cfg.rocks_dir();
    tracing::info!(
        rocks_dir = %rocks_dir.display(),
        raw_dir = %raw_dir.display(),
        "opening rocks read-only"
    );
    let cache_bytes = cfg.rocks_block_cache_bytes()?;
    let db = open_readonly(&rocks_dir, cache_bytes)?;

    let actors_count = pass_a_actors(&db, &raw_dir, cfg.batch_size)?;
    let link = pass_b_link_targets(&db, &raw_dir, cfg.batch_size)?;
    let targets_count = pass_c_targets(&db, &raw_dir, cfg.batch_size)?;

    let counts = vec![
        ("actors".to_string(), actors_count),
        ("link_records".to_string(), link.records),
        ("link_record_targets".to_string(), link.targets),
        ("targets".to_string(), targets_count),
    ];
    Ok(StageOutcome { raw_dir, counts })
}

fn pass_a_actors(db: &DB, raw_dir: &std::path::Path, batch_size: usize) -> Result<u64> {
    let cf = db
        .cf_handle(rs::CF_DID_IDS)
        .context("missing did_ids cf")?;
    let mut writer = ActorWriter::create(raw_dir.join("actors.parquet"), batch_size)?;

    let mut read_opts = ReadOptions::default();
    read_opts.set_verify_checksums(false);
    let iter = db.iterator_cf_opt(cf, read_opts, IteratorMode::Start);

    let mut scanned = 0u64;
    let mut emitted = 0u64;
    let mut bad_did_keys = 0u64;
    let mut bad_did_values = 0u64;
    for item in iter {
        let (k, v) = item.context("iterate did_ids")?;
        scanned += 1;
        if k.len() == 8 {
            // reverse-mapping entry: skip
            continue;
        }
        let did: Did = match rs::decode(&k) {
            Ok(d) => d,
            Err(e) => {
                if bad_did_keys == 0 {
                    tracing::warn!(error=?e, key=?k, "first undecodable did key (further occurrences counted only)");
                }
                bad_did_keys += 1;
                continue;
            }
        };
        let val: DidIdValue = match rs::decode(&v) {
            Ok(d) => d,
            Err(e) => {
                if bad_did_values == 0 {
                    tracing::warn!(error=?e, key=?k, "first undecodable did value (further occurrences counted only)");
                }
                bad_did_values += 1;
                continue;
            }
        };
        writer.push(val.0 .0, &did.0, val.1)?;
        emitted += 1;
        if emitted % 1_000_000 == 0 {
            tracing::info!(scanned, emitted, bad_did_keys, bad_did_values, "pass A progress");
        }
    }
    let (path, total) = writer.finish()?;
    tracing::info!(
        scanned,
        emitted = total,
        bad_did_keys,
        bad_did_values,
        path = %path.display(),
        "pass A complete"
    );
    Ok(total)
}

#[derive(Default)]
struct PassBCounts {
    records: u64,
    targets: u64,
}

/// Pass B: pure sequential scan of `link_targets`. Emits one row per
/// RecordLinkKey to `link_records.parquet` (with TID-decoded created_at)
/// and one row per (record, rpath, target_id) to
/// `link_record_targets.parquet`. No point lookups — hydrate joins
/// against `targets.parquet` and `actors.parquet` to resolve identifiers.
fn pass_b_link_targets(
    db: &DB,
    raw_dir: &std::path::Path,
    batch_size: usize,
) -> Result<PassBCounts> {
    let cf = db
        .cf_handle(rs::CF_LINK_TARGETS)
        .context("missing link_targets cf")?;

    let mut records = LinkRecordWriter::create(raw_dir.join("link_records.parquet"), batch_size)?;
    let mut targets = LinkRecordTargetsWriter::create(
        raw_dir.join("link_record_targets.parquet"),
        batch_size,
    )?;

    let mut read_opts = ReadOptions::default();
    read_opts.set_verify_checksums(false);
    let iter = db.iterator_cf_opt(cf, read_opts, IteratorMode::Start);

    let mut counts = PassBCounts::default();
    let mut bad_link_keys = 0u64;
    let mut bad_link_values = 0u64;
    for item in iter {
        let (k, v) = item.context("iterate link_targets")?;
        let key: RecordLinkKey = match rs::decode(&k) {
            Ok(k) => k,
            Err(e) => {
                if bad_link_keys == 0 {
                    tracing::warn!(error=?e, key=?k, "first undecodable link_targets key (further occurrences counted only)");
                }
                bad_link_keys += 1;
                continue;
            }
        };
        let value: RecordLinkTargets = match rs::decode(&v) {
            Ok(v) => v,
            Err(e) => {
                if bad_link_values == 0 {
                    tracing::warn!(error=?e, key=?k, "first undecodable link_targets value (further occurrences counted only)");
                }
                bad_link_values += 1;
                continue;
            }
        };
        let RecordLinkKey(DidId(did_id), Collection(collection), RKey(rkey)) = key;

        if collection == COL_PROFILE {
            continue;
        }

        let created_at = tid::decode_tid_micros(&rkey);
        records.push(did_id, &collection, &rkey, created_at)?;
        counts.records += 1;

        for (ord, target) in value.0.iter().enumerate() {
            let RPath(rpath) = &target.0;
            let TargetId(target_id) = target.1;
            targets.push(did_id, &collection, &rkey, ord as i32, rpath, target_id)?;
            counts.targets += 1;
        }

        if counts.records % 1_000_000 == 0 {
            tracing::info!(
                records = counts.records,
                targets = counts.targets,
                bad_link_keys,
                bad_link_values,
                "pass B progress"
            );
        }
    }

    let (records_path, records_total) = records.finish()?;
    let (targets_path, targets_total) = targets.finish()?;
    tracing::info!(
        records = records_total,
        targets = targets_total,
        bad_link_keys,
        bad_link_values,
        records_path = %records_path.display(),
        targets_path = %targets_path.display(),
        "pass B complete"
    );
    counts.records = records_total;
    counts.targets = targets_total;
    Ok(counts)
}

/// Pass C: scan `target_ids` *reverse* map (8-byte target_id → TargetKey).
/// We use the reverse side because constellation may assign multiple
/// target_ids to the same (target, collection, rpath) triple; the forward
/// map only retains the last writer, while the reverse map keeps every
/// id. Emits one row per target_id to `targets.parquet`. Hydrate filters
/// this to AT-URIs of posts to recover target-only posts.
fn pass_c_targets(
    db: &DB,
    raw_dir: &std::path::Path,
    batch_size: usize,
) -> Result<u64> {
    let cf = db
        .cf_handle(rs::CF_TARGET_IDS)
        .context("missing target_ids cf")?;
    let mut targets = TargetWriter::create(raw_dir.join("targets.parquet"), batch_size)?;

    let mut read_opts = ReadOptions::default();
    read_opts.set_verify_checksums(false);
    let iter = db.iterator_cf_opt(cf, read_opts, IteratorMode::Start);

    let mut emitted = 0u64;
    let mut bad_target_values = 0u64;
    for item in iter {
        let (k, v) = item.context("iterate target_ids")?;
        if k.len() != 8 {
            // forward-mapping entry: skip; we only want reverse rows
            continue;
        }
        let mut id_bytes = [0u8; 8];
        id_bytes.copy_from_slice(&k);
        let target_id = u64::from_be_bytes(id_bytes);
        let TargetKey(Target(target), Collection(coll), RPath(path)) =
            match rs::decode::<TargetKey>(&v) {
                Ok(t) => t,
                Err(e) => {
                    if bad_target_values == 0 {
                        tracing::warn!(error=?e, target_id, "first undecodable target_ids reverse value (further occurrences counted only)");
                    }
                    bad_target_values += 1;
                    continue;
                }
            };
        targets.push(target_id, &target, &coll, &path)?;
        emitted += 1;

        if emitted % 1_000_000 == 0 {
            tracing::info!(emitted, bad_target_values, "pass C progress");
        }
    }
    let (path, total) = targets.finish()?;
    tracing::info!(
        emitted = total,
        bad_target_values,
        path = %path.display(),
        "pass C complete"
    );
    Ok(total)
}
