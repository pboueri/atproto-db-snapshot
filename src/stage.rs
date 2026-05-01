//! Stage: rocks → entity parquets.
//!
//! Runs three sequential rocks scans (Pass A then B + C in parallel),
//! followed by a DuckDB sort-merge step (Phase 5) that produces the
//! final entity parquet files. Stage emits *only* per-entity files
//! to `raw/<date>/`; intermediate files live under
//! `raw/<date>/scratch/` and are deleted before stage returns.
//!
//! Entity outputs:
//!
//!   raw/<date>/
//!     actors.parquet
//!     follows.parquet
//!     blocks.parquet
//!     likes.parquet
//!     reposts.parquet
//!     posts_from_records.parquet
//!     posts_from_targets.parquet
//!
//! Hydrate then unions posts_from_records ∪ posts_from_targets and
//! computes aggregates. No 10B-row `targets`/`link_record_targets`
//! join in DuckDB anywhere.

use anyhow::{Context, Result};
use rocksdb::{IteratorMode, ReadOptions, DB};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::config::Config;
use crate::hash::id_for_uri;
use crate::rocks::open::open_readonly;
use crate::rocks::schema::{
    self as rs, parse_at_uri, Collection, Did, DidId, DidIdValue, RKey, RPath, RecordLinkKey,
    RecordLinkTargets, Target, TargetKey,
};
use crate::tid;
use crate::writers::{
    ActorWriter, LtKeyedWriter, LtPostRecordsWriter, LtPostRefsWriter, PostsWriter, U64PairWriter,
};

const COL_PROFILE: &str = "app.bsky.actor.profile";
const COL_POST: &str = "app.bsky.feed.post";
const COL_LIKE: &str = "app.bsky.feed.like";
const COL_REPOST: &str = "app.bsky.feed.repost";
const COL_FOLLOW: &str = "app.bsky.graph.follow";
const COL_BLOCK: &str = "app.bsky.graph.block";

const RPATH_REPLY_ROOT: &str = ".reply.root.uri";
const RPATH_REPLY_PARENT: &str = ".reply.parent.uri";
const RPATH_EMBED_RECORD: &str = ".embed.record.uri";
const RPATH_EMBED_RWM: &str = ".embed.recordWithMedia.record.record.uri";
const RPATH_SUBJECT: &str = ".subject";
const RPATH_SUBJECT_URI: &str = ".subject.uri";

pub struct StageOutcome {
    pub raw_dir: PathBuf,
    pub counts: Vec<(String, u64)>,
}

/// Sequential-scan readahead. Default rocksdb iterator reads ~32 KiB
/// at a time; pumping that to 8 MiB converts each pass into bulk
/// sequential I/O. Big win on network FS, measurable even on local
/// NVMe (page-cache prefetch alignment).
const SCAN_READAHEAD_BYTES: usize = 8 * 1024 * 1024;

fn scan_read_opts() -> ReadOptions {
    let mut opts = ReadOptions::default();
    opts.set_verify_checksums(false);
    opts.set_readahead_size(SCAN_READAHEAD_BYTES);
    opts
}

fn estimate_keys(db: &DB, cf_name: &str) -> u64 {
    let Some(cf) = db.cf_handle(cf_name) else {
        return 0;
    };
    db.property_int_value_cf(&cf, "rocksdb.estimate-num-keys")
        .ok()
        .flatten()
        .unwrap_or(0)
}

fn pct_str(scanned: u64, total: u64) -> String {
    if total == 0 {
        "?".to_string()
    } else {
        format!("{:.1}", 100.0 * scanned as f64 / total as f64)
    }
}

pub async fn run(cfg: &Config, snapshot_date: &str) -> Result<StageOutcome> {
    let raw_dir = cfg.raw_dir(snapshot_date);
    let scratch_dir = raw_dir.join("scratch");
    crate::writers::common::ensure_dir(&raw_dir)?;
    crate::writers::common::ensure_dir(&scratch_dir)?;

    let rocks_dir = cfg.rocks_dir();
    tracing::info!(
        rocks_dir = %rocks_dir.display(),
        raw_dir = %raw_dir.display(),
        scratch_dir = %scratch_dir.display(),
        "opening rocks read-only"
    );
    let cache_bytes = cfg.rocks_block_cache_bytes()?;
    let db = open_readonly(&rocks_dir, cache_bytes)?;

    let batch_size = cfg.batch_size;

    // ---- Pass A (sequential): emit actors + build the in-mem maps
    let pass_a_t0 = Instant::now();
    let actor_map = pass_a_actors(&db, &raw_dir, batch_size)?;
    let n_actors = actor_map.n_actors;
    tracing::info!(
        actors = n_actors,
        elapsed_secs = pass_a_t0.elapsed().as_secs_f64(),
        "pass A done"
    );

    // ---- Passes B and C in parallel (both use rocks read-only + ActorMap)
    let bc_t0 = Instant::now();
    let (b_counts, c_counts) = std::thread::scope(|s| -> Result<(PassBCounts, PassCCounts)> {
        let am: &ActorMap = &actor_map;
        let db_ref: &DB = &db;
        let scratch: &Path = &scratch_dir;
        let raw: &Path = &raw_dir;
        let b_handle =
            s.spawn(move || pass_b_link_targets(db_ref, scratch, am, batch_size));
        let c_handle = s.spawn(move || pass_c_targets(db_ref, scratch, raw, am, batch_size));
        let b = b_handle
            .join()
            .map_err(|_| anyhow::anyhow!("pass B panicked"))??;
        let c = c_handle
            .join()
            .map_err(|_| anyhow::anyhow!("pass C panicked"))??;
        Ok((b, c))
    })?;
    tracing::info!(
        elapsed_secs = bc_t0.elapsed().as_secs_f64(),
        "passes B+C done"
    );

    // Free the maps before Phase 5; DuckDB will manage its own join
    // hash over actors.parquet. Holding ~15-20 GB of HashMap during
    // the DuckDB sort would compete needlessly.
    drop(actor_map);
    drop(db);

    // ---- Phase 5: DuckDB sort-merge → final entity parquets
    let phase5_t0 = Instant::now();
    let entity_counts = phase5_merge(&scratch_dir, &raw_dir, cfg)?;
    tracing::info!(
        elapsed_secs = phase5_t0.elapsed().as_secs_f64(),
        "phase 5 done"
    );

    // Cleanup scratch unconditionally — leaving it would mislead a
    // resume into thinking stage hadn't completed. If removal fails
    // we log; it's just disk noise.
    if let Err(e) = std::fs::remove_dir_all(&scratch_dir) {
        tracing::warn!(error = %e, dir = %scratch_dir.display(), "remove scratch (continuing)");
    }

    let mut counts = vec![("actors".to_string(), n_actors)];
    counts.extend(b_counts.summary());
    counts.extend(c_counts.summary());
    counts.extend(entity_counts);
    Ok(StageOutcome { raw_dir, counts })
}

// =====================================================================
// ActorMap
// =====================================================================

/// Two views of the actors table built during Pass A:
/// - `by_id`: `did_id → did string`. Dense Vec keyed on did_id;
///   ~7-8 GB for 100M actors. Pass B uses this to compute the post
///   record's own uri_id (`id_for_uri('at://'||did||'/app.bsky.feed.post/'||rkey)`)
///   without paying a rocks point lookup per record.
/// - `by_did`: `did string → did_id`. Pass C uses this to resolve
///   follow/block target DID strings to dst_did_id, again without
///   point lookups. ~10-12 GB for 100M actors.
///
/// Both maps live only between Pass A and the end of Passes B+C, then
/// are dropped before Phase 5 starts.
struct ActorMap {
    by_id: Vec<Option<String>>,
    by_did: HashMap<String, u64>,
    n_actors: u64,
}

impl ActorMap {
    fn lookup_did(&self, did_id: u64) -> Option<&str> {
        let i = did_id as usize;
        self.by_id.get(i).and_then(|s| s.as_deref())
    }
}

fn pass_a_actors(db: &DB, raw_dir: &Path, batch_size: usize) -> Result<ActorMap> {
    let cf = db
        .cf_handle(rs::CF_DID_IDS)
        .context("missing did_ids cf")?;
    let mut writer = ActorWriter::create(raw_dir.join("actors.parquet"), batch_size)?;

    let estimate_total = estimate_keys(db, rs::CF_DID_IDS);
    tracing::info!(estimate_total, "pass A starting");
    let iter = db.iterator_cf_opt(cf, scan_read_opts(), IteratorMode::Start);

    let mut by_id: Vec<Option<String>> = Vec::with_capacity(8 * 1024 * 1024);
    let mut by_did: HashMap<String, u64> = HashMap::with_capacity(estimate_total as usize / 2);

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
                    tracing::warn!(error=?e, key=?k, "first undecodable did key");
                }
                bad_did_keys += 1;
                continue;
            }
        };
        let val: DidIdValue = match rs::decode(&v) {
            Ok(d) => d,
            Err(e) => {
                if bad_did_values == 0 {
                    tracing::warn!(error=?e, key=?k, "first undecodable did value");
                }
                bad_did_values += 1;
                continue;
            }
        };
        let id = val.0 .0;
        let did_str = did.0;
        writer.push(id, &did_str, val.1)?;
        // by_id: dense keyed on did_id. Resize on demand. Constellation
        // ids are typically dense from 0/1; the worst case is a sparse
        // tail which costs ~24 bytes/None — acceptable.
        let i = id as usize;
        if i >= by_id.len() {
            // Grow with headroom so the common dense case is amortized.
            let new_len = (i + 1).max(by_id.len() * 2);
            by_id.resize(new_len, None);
        }
        by_id[i] = Some(did_str.clone());
        by_did.insert(did_str, id);
        emitted += 1;
        if emitted % 1_000_000 == 0 {
            tracing::info!(
                scanned,
                emitted,
                bad_did_keys,
                bad_did_values,
                pct = pct_str(scanned, estimate_total),
                "pass A progress"
            );
        }
    }
    let (path, total) = writer.finish()?;
    tracing::info!(
        scanned,
        emitted = total,
        bad_did_keys,
        bad_did_values,
        path = %path.display(),
        by_id_len = by_id.len(),
        by_did_len = by_did.len(),
        "pass A complete"
    );

    Ok(ActorMap {
        by_id,
        by_did,
        n_actors: total,
    })
}

// =====================================================================
// Pass B — link_targets → scratch lt_*.parquet
// =====================================================================

#[derive(Default)]
struct PassBCounts {
    /// Records emitted to `lt_post_records.parquet` (one per post record).
    post_records: u64,
    /// Ref rows emitted to `lt_post_refs.parquet` (per reply/quote rpath).
    post_refs: u64,
    likes: u64,
    reposts: u64,
    follows: u64,
    blocks: u64,
    /// Records skipped because their author did_id wasn't in actors
    /// (shouldn't normally happen — all record-author DIDs are in
    /// did_ids — but log to surface fixture/mock issues fast).
    missing_author: u64,
}

impl PassBCounts {
    fn summary(&self) -> Vec<(String, u64)> {
        // Stage-internal counts, surfaced via metadata for debugging.
        // The user-facing entity row counts come from Phase 5 outputs.
        vec![
            ("scratch_lt_post_records".to_string(), self.post_records),
            ("scratch_lt_post_refs".to_string(), self.post_refs),
            ("scratch_lt_like".to_string(), self.likes),
            ("scratch_lt_repost".to_string(), self.reposts),
            ("scratch_lt_follow".to_string(), self.follows),
            ("scratch_lt_block".to_string(), self.blocks),
        ]
    }
}

fn pass_b_link_targets(
    db: &DB,
    scratch_dir: &Path,
    actor_map: &ActorMap,
    batch_size: usize,
) -> Result<PassBCounts> {
    let cf = db
        .cf_handle(rs::CF_LINK_TARGETS)
        .context("missing link_targets cf")?;

    let mut post_records =
        LtPostRecordsWriter::create(scratch_dir.join("lt_post_records.parquet"), batch_size)?;
    let mut post_refs =
        LtPostRefsWriter::create(scratch_dir.join("lt_post_refs.parquet"), batch_size)?;
    let mut likes = LtKeyedWriter::create(scratch_dir.join("lt_like.parquet"), batch_size)?;
    let mut reposts = LtKeyedWriter::create(scratch_dir.join("lt_repost.parquet"), batch_size)?;
    let mut follows = LtKeyedWriter::create(scratch_dir.join("lt_follow.parquet"), batch_size)?;
    let mut blocks = LtKeyedWriter::create(scratch_dir.join("lt_block.parquet"), batch_size)?;

    let estimate_total = estimate_keys(db, rs::CF_LINK_TARGETS);
    tracing::info!(estimate_total, "pass B starting");
    let iter = db.iterator_cf_opt(cf, scan_read_opts(), IteratorMode::Start);

    let mut counts = PassBCounts::default();
    let mut scanned = 0u64;
    let mut bad_link_keys = 0u64;
    let mut bad_link_values = 0u64;

    // Pre-allocated scratch URI buffer. Posts compute
    // `at://<did>/app.bsky.feed.post/<rkey>` per record; reuse the
    // same String to avoid an alloc per post. A typical bsky DID is
    // ~32 chars; rkey is 13. ~80 char ceiling; size for a comfortable
    // headroom.
    let mut uri_buf = String::with_capacity(128);

    for item in iter {
        let (k, v) = item.context("iterate link_targets")?;
        scanned += 1;
        let key: RecordLinkKey = match rs::decode(&k) {
            Ok(k) => k,
            Err(e) => {
                if bad_link_keys == 0 {
                    tracing::warn!(error=?e, key=?k, "first undecodable link_targets key");
                }
                bad_link_keys += 1;
                continue;
            }
        };
        let value: RecordLinkTargets = match rs::decode(&v) {
            Ok(v) => v,
            Err(e) => {
                if bad_link_values == 0 {
                    tracing::warn!(error=?e, key=?k, "first undecodable link_targets value");
                }
                bad_link_values += 1;
                continue;
            }
        };
        let RecordLinkKey(DidId(did_id), Collection(collection), RKey(rkey)) = key;

        if collection.as_str() == COL_PROFILE {
            continue;
        }

        let created_at = tid::decode_tid_micros(&rkey);

        match collection.as_str() {
            COL_POST => {
                let Some(author_did) = actor_map.lookup_did(did_id) else {
                    counts.missing_author += 1;
                    if counts.missing_author == 1 {
                        tracing::warn!(did_id, "first post record with author missing from actors");
                    }
                    continue;
                };
                uri_buf.clear();
                uri_buf.push_str("at://");
                uri_buf.push_str(author_did);
                uri_buf.push('/');
                uri_buf.push_str(COL_POST);
                uri_buf.push('/');
                uri_buf.push_str(&rkey);
                let post_uri_id = id_for_uri(&uri_buf);

                post_records.push(post_uri_id, did_id, &rkey, created_at)?;
                counts.post_records += 1;

                for target in value.0.iter() {
                    let RPath(rpath) = &target.0;
                    let crate::rocks::schema::TargetId(target_id) = target.1;
                    if is_post_ref_rpath(rpath) {
                        post_refs.push(post_uri_id, rpath, target_id)?;
                        counts.post_refs += 1;
                    }
                    // media rpaths and any other rpaths are intentionally dropped.
                }
            }
            COL_LIKE => {
                if let Some(target_id) = first_subject_uri_target(&value) {
                    likes.push(target_id, did_id, &rkey, created_at)?;
                    counts.likes += 1;
                }
            }
            COL_REPOST => {
                if let Some(target_id) = first_subject_uri_target(&value) {
                    reposts.push(target_id, did_id, &rkey, created_at)?;
                    counts.reposts += 1;
                }
            }
            COL_FOLLOW => {
                if let Some(target_id) = first_subject_target(&value) {
                    follows.push(target_id, did_id, &rkey, created_at)?;
                    counts.follows += 1;
                }
            }
            COL_BLOCK => {
                if let Some(target_id) = first_subject_target(&value) {
                    blocks.push(target_id, did_id, &rkey, created_at)?;
                    counts.blocks += 1;
                }
            }
            _ => {
                // unknown collection — not in our snapshot scope
            }
        }

        if scanned % 1_000_000 == 0 {
            tracing::info!(
                scanned,
                post_records = counts.post_records,
                post_refs = counts.post_refs,
                likes = counts.likes,
                reposts = counts.reposts,
                follows = counts.follows,
                blocks = counts.blocks,
                missing_author = counts.missing_author,
                bad_link_keys,
                bad_link_values,
                pct = pct_str(scanned, estimate_total),
                "pass B progress"
            );
        }
    }

    let (post_records_path, post_records_total) = post_records.finish()?;
    let (post_refs_path, post_refs_total) = post_refs.finish()?;
    let (likes_path, likes_total) = likes.finish()?;
    let (reposts_path, reposts_total) = reposts.finish()?;
    let (follows_path, follows_total) = follows.finish()?;
    let (blocks_path, blocks_total) = blocks.finish()?;

    tracing::info!(
        post_records = post_records_total,
        post_refs = post_refs_total,
        likes = likes_total,
        reposts = reposts_total,
        follows = follows_total,
        blocks = blocks_total,
        missing_author = counts.missing_author,
        bad_link_keys,
        bad_link_values,
        post_records_path = %post_records_path.display(),
        post_refs_path = %post_refs_path.display(),
        likes_path = %likes_path.display(),
        reposts_path = %reposts_path.display(),
        follows_path = %follows_path.display(),
        blocks_path = %blocks_path.display(),
        "pass B complete"
    );

    counts.post_records = post_records_total;
    counts.post_refs = post_refs_total;
    counts.likes = likes_total;
    counts.reposts = reposts_total;
    counts.follows = follows_total;
    counts.blocks = blocks_total;

    Ok(counts)
}

#[inline]
fn is_post_ref_rpath(p: &str) -> bool {
    p == RPATH_REPLY_ROOT
        || p == RPATH_REPLY_PARENT
        || p == RPATH_EMBED_RECORD
        || p == RPATH_EMBED_RWM
}

fn first_subject_uri_target(value: &RecordLinkTargets) -> Option<u64> {
    for t in value.0.iter() {
        let RPath(rp) = &t.0;
        if rp == RPATH_SUBJECT_URI {
            return Some(t.1 .0);
        }
    }
    None
}

fn first_subject_target(value: &RecordLinkTargets) -> Option<u64> {
    for t in value.0.iter() {
        let RPath(rp) = &t.0;
        if rp == RPATH_SUBJECT {
            return Some(t.1 .0);
        }
    }
    None
}

// =====================================================================
// Pass C — target_ids reverse → scratch t_*.parquet + raw posts_from_targets
// =====================================================================

#[derive(Default)]
struct PassCCounts {
    t_post_refs: u64,
    t_did_refs: u64,
    posts_from_targets_raw: u64,
    /// Targets whose collection/rpath isn't in our short list — by far
    /// the majority of total target_ids in the scan.
    unused: u64,
    /// Follow/block targets whose target string wasn't found in the
    /// actors map (the linked-to DID isn't in `did_ids`, e.g. deleted
    /// account). Dropped silently.
    missing_did: u64,
    /// posts_from_targets candidates whose author DID isn't in actors.
    missing_post_author: u64,
}

impl PassCCounts {
    fn summary(&self) -> Vec<(String, u64)> {
        vec![
            ("scratch_t_post_refs".to_string(), self.t_post_refs),
            ("scratch_t_did_refs".to_string(), self.t_did_refs),
            (
                "scratch_posts_from_targets_raw".to_string(),
                self.posts_from_targets_raw,
            ),
        ]
    }
}

fn pass_c_targets(
    db: &DB,
    scratch_dir: &Path,
    raw_dir: &Path,
    actor_map: &ActorMap,
    batch_size: usize,
) -> Result<PassCCounts> {
    let cf = db
        .cf_handle(rs::CF_TARGET_IDS)
        .context("missing target_ids cf")?;

    let mut t_post_refs =
        U64PairWriter::create(scratch_dir.join("t_post_refs.parquet"), batch_size, "target_id", "uri_id")?;
    let mut t_did_refs = U64PairWriter::create(
        scratch_dir.join("t_did_refs.parquet"),
        batch_size,
        "target_id",
        "dst_did_id",
    )?;
    // posts_from_targets candidates — multiple target_ids may resolve
    // to the same post URI (different (collection, rpath) tuples for
    // the same string). Dedup is deferred to a Phase-5 SELECT DISTINCT
    // because in-memory dedup would require ~5B uri_id entries.
    let mut posts_raw = PostsWriter::create_scratch(
        scratch_dir.join("posts_from_targets_raw.parquet"),
        batch_size,
    )?;

    let estimate_total = estimate_keys(db, rs::CF_TARGET_IDS);
    tracing::info!(estimate_total, "pass C starting");
    let iter = db.iterator_cf_opt(cf, scan_read_opts(), IteratorMode::Start);

    let mut counts = PassCCounts::default();
    let mut scanned = 0u64;
    let mut bad_target_values = 0u64;

    for item in iter {
        let (k, v) = item.context("iterate target_ids")?;
        scanned += 1;
        if k.len() != 8 {
            // forward-mapping entry: skip
            continue;
        }
        let mut id_bytes = [0u8; 8];
        id_bytes.copy_from_slice(&k);
        let target_id = u64::from_be_bytes(id_bytes);
        let TargetKey(Target(target), Collection(coll), RPath(rpath)) =
            match rs::decode::<TargetKey>(&v) {
                Ok(t) => t,
                Err(e) => {
                    if bad_target_values == 0 {
                        tracing::warn!(error=?e, target_id, "first undecodable target_ids reverse value");
                    }
                    bad_target_values += 1;
                    continue;
                }
            };

        // We compute uri_id at most once per row, regardless of how
        // many of the branches below want it. Profile showed
        // id_for_uri being called twice when the target was both a
        // ref-rpath AND itself a feed.post URI.
        let mut uri_id_cached: Option<u64> = None;

        // 1. Project into the appropriate scratch file by (collection, rpath).
        let mut classified = false;
        match (coll.as_str(), rpath.as_str()) {
            (COL_POST, RPATH_REPLY_ROOT)
            | (COL_POST, RPATH_REPLY_PARENT)
            | (COL_POST, RPATH_EMBED_RECORD)
            | (COL_POST, RPATH_EMBED_RWM)
            | (COL_LIKE, RPATH_SUBJECT_URI)
            | (COL_REPOST, RPATH_SUBJECT_URI) => {
                let uri_id = *uri_id_cached.get_or_insert_with(|| id_for_uri(&target));
                t_post_refs.push(target_id, uri_id)?;
                counts.t_post_refs += 1;
                classified = true;
            }
            (COL_FOLLOW, RPATH_SUBJECT) | (COL_BLOCK, RPATH_SUBJECT) => {
                if let Some(&dst_did_id) = actor_map.by_did.get(target.as_str()) {
                    t_did_refs.push(target_id, dst_did_id)?;
                    counts.t_did_refs += 1;
                } else {
                    counts.missing_did += 1;
                }
                classified = true;
            }
            _ => {}
        }
        if !classified {
            counts.unused += 1;
        }

        // 2. Independently: if the target *string* itself is an AT-URI
        //    of a feed.post, emit a candidate posts_from_targets row.
        //    The same URI string can appear as the target of multiple
        //    target_ids (one per (collection, rpath) triple) — Phase 5
        //    dedups on uri_id.
        if let Some(uri) = parse_at_uri(&target) {
            if uri.collection == COL_POST {
                if let Some(&author_did_id) = actor_map.by_did.get(&uri.did) {
                    let uri_id = *uri_id_cached.get_or_insert_with(|| id_for_uri(&target));
                    let created_at = tid::decode_tid_micros(&uri.rkey);
                    posts_raw.push(uri_id, author_did_id, &uri.rkey, created_at, None, None, None)?;
                    counts.posts_from_targets_raw += 1;
                } else {
                    counts.missing_post_author += 1;
                }
            }
        }

        if scanned % 1_000_000 == 0 {
            tracing::info!(
                scanned,
                t_post_refs = counts.t_post_refs,
                t_did_refs = counts.t_did_refs,
                posts_raw = counts.posts_from_targets_raw,
                unused = counts.unused,
                missing_did = counts.missing_did,
                missing_post_author = counts.missing_post_author,
                bad_target_values,
                pct = pct_str(scanned, estimate_total),
                "pass C progress"
            );
        }
    }

    let (post_refs_path, post_refs_total) = t_post_refs.finish()?;
    let (did_refs_path, did_refs_total) = t_did_refs.finish()?;
    let (posts_raw_path, posts_raw_total) = posts_raw.finish()?;
    let _ = raw_dir; // posts_from_targets final lives at raw_dir; written in Phase 5.

    tracing::info!(
        scanned,
        t_post_refs = post_refs_total,
        t_did_refs = did_refs_total,
        posts_raw = posts_raw_total,
        unused = counts.unused,
        missing_did = counts.missing_did,
        missing_post_author = counts.missing_post_author,
        bad_target_values,
        post_refs_path = %post_refs_path.display(),
        did_refs_path = %did_refs_path.display(),
        posts_raw_path = %posts_raw_path.display(),
        "pass C complete"
    );

    counts.t_post_refs = post_refs_total;
    counts.t_did_refs = did_refs_total;
    counts.posts_from_targets_raw = posts_raw_total;
    Ok(counts)
}

// =====================================================================
// Phase 5 — DuckDB sort-merge from scratch into entity parquets
// =====================================================================

fn phase5_merge(scratch_dir: &Path, raw_dir: &Path, cfg: &Config) -> Result<Vec<(String, u64)>> {
    use duckdb::Connection;

    // Open transient DuckDB inside scratch_dir so its temp spill stays
    // on the same filesystem as the inputs.
    let duckdb_path = scratch_dir.join("phase5.duckdb");
    if duckdb_path.exists() {
        let _ = std::fs::remove_file(&duckdb_path);
    }
    let tmp = scratch_dir.join("duckdb_tmp");
    std::fs::create_dir_all(&tmp).with_context(|| format!("create tmp {}", tmp.display()))?;

    let conn = Connection::open(&duckdb_path).context("phase5 duckdb open")?;
    let memory_limit = cfg.resolved_memory_limit()?;
    tracing::info!(
        memory_limit = %memory_limit,
        tmp = %tmp.display(),
        "phase 5 duckdb opened"
    );
    pragma(&conn, &format!("SET memory_limit='{memory_limit}'"))?;
    pragma(&conn, "SET preserve_insertion_order=false")?;
    pragma(&conn, &format!("SET temp_directory='{}'", tmp.display()))?;

    let scratch = scratch_dir.to_string_lossy().to_string();
    let scratch = scratch.trim_end_matches('/').to_string();
    let raw = raw_dir.to_string_lossy().to_string();
    let raw = raw.trim_end_matches('/').to_string();

    // All COPY statements use uncompressed timestamps + ZSTD
    // compression with a fixed row-group target so downstream reads
    // are predictable.
    let copy_opts = "(FORMAT 'parquet', COMPRESSION 'zstd', ROW_GROUP_SIZE 1000000)";

    let mut entity_counts: Vec<(String, u64)> = Vec::new();

    // ---- follows
    let sql = format!(
        "COPY (
           WITH lt AS (
             SELECT target_id, src_did_id, rkey, created_at
             FROM read_parquet('{scratch}/lt_follow.parquet')
             ORDER BY target_id
           )
           SELECT lt.src_did_id, td.dst_did_id, lt.rkey, lt.created_at
           FROM lt
           JOIN read_parquet('{scratch}/t_did_refs.parquet') td
             ON td.target_id = lt.target_id
         ) TO '{raw}/follows.parquet' {copy_opts};"
    );
    run_copy(&conn, "follows", &sql)?;
    entity_counts.push(("follows".to_string(), count_rows(&conn, &format!("{raw}/follows.parquet"))?));
    let _ = std::fs::remove_file(format!("{scratch}/lt_follow.parquet"));

    // ---- blocks
    let sql = format!(
        "COPY (
           WITH lt AS (
             SELECT target_id, src_did_id, rkey, created_at
             FROM read_parquet('{scratch}/lt_block.parquet')
             ORDER BY target_id
           )
           SELECT lt.src_did_id, td.dst_did_id, lt.rkey, lt.created_at
           FROM lt
           JOIN read_parquet('{scratch}/t_did_refs.parquet') td
             ON td.target_id = lt.target_id
         ) TO '{raw}/blocks.parquet' {copy_opts};"
    );
    run_copy(&conn, "blocks", &sql)?;
    entity_counts.push(("blocks".to_string(), count_rows(&conn, &format!("{raw}/blocks.parquet"))?));
    let _ = std::fs::remove_file(format!("{scratch}/lt_block.parquet"));

    // t_did_refs only joined by follows + blocks above; safe to drop.
    let _ = std::fs::remove_file(format!("{scratch}/t_did_refs.parquet"));

    // ---- likes (orphans allowed: subject_uri_id NULL when target
    //              wasn't a feed.post URI in our universe)
    let sql = format!(
        "COPY (
           WITH lt AS (
             SELECT target_id, src_did_id, rkey, created_at
             FROM read_parquet('{scratch}/lt_like.parquet')
             ORDER BY target_id
           )
           SELECT lt.src_did_id AS actor_did_id,
                  tp.uri_id      AS subject_uri_id,
                  lt.rkey,
                  lt.created_at
           FROM lt
           LEFT JOIN read_parquet('{scratch}/t_post_refs.parquet') tp
             ON tp.target_id = lt.target_id
         ) TO '{raw}/likes.parquet' {copy_opts};"
    );
    run_copy(&conn, "likes", &sql)?;
    entity_counts.push(("likes".to_string(), count_rows(&conn, &format!("{raw}/likes.parquet"))?));
    let _ = std::fs::remove_file(format!("{scratch}/lt_like.parquet"));

    // ---- reposts (same shape)
    let sql = format!(
        "COPY (
           WITH lt AS (
             SELECT target_id, src_did_id, rkey, created_at
             FROM read_parquet('{scratch}/lt_repost.parquet')
             ORDER BY target_id
           )
           SELECT lt.src_did_id AS actor_did_id,
                  tp.uri_id      AS subject_uri_id,
                  lt.rkey,
                  lt.created_at
           FROM lt
           LEFT JOIN read_parquet('{scratch}/t_post_refs.parquet') tp
             ON tp.target_id = lt.target_id
         ) TO '{raw}/reposts.parquet' {copy_opts};"
    );
    run_copy(&conn, "reposts", &sql)?;
    entity_counts.push(("reposts".to_string(), count_rows(&conn, &format!("{raw}/reposts.parquet"))?));
    let _ = std::fs::remove_file(format!("{scratch}/lt_repost.parquet"));

    // ---- posts_from_records: pivot per-rpath refs into 3 columns,
    //      LEFT JOIN against the per-record table to keep posts that
    //      had no targets at all (plain text-only posts).
    let sql = format!(
        "COPY (
           WITH lt AS (
             SELECT post_uri_id, rpath, target_id
             FROM read_parquet('{scratch}/lt_post_refs.parquet')
             ORDER BY target_id
           ),
           refs_resolved AS (
             SELECT lt.post_uri_id, lt.rpath, tp.uri_id AS ref_uri_id
             FROM lt
             JOIN read_parquet('{scratch}/t_post_refs.parquet') tp
               ON tp.target_id = lt.target_id
           ),
           pivoted AS (
             SELECT post_uri_id,
                    MAX(CASE WHEN rpath = '.reply.root.uri'                          THEN ref_uri_id END) AS reply_root_uri_id,
                    MAX(CASE WHEN rpath = '.reply.parent.uri'                        THEN ref_uri_id END) AS reply_parent_uri_id,
                    MAX(CASE WHEN rpath = '.embed.record.uri'                        THEN ref_uri_id END) AS quote_embed_uri_id,
                    MAX(CASE WHEN rpath = '.embed.recordWithMedia.record.record.uri' THEN ref_uri_id END) AS quote_rwm_uri_id
             FROM refs_resolved
             GROUP BY 1
           )
           SELECT
             pr.post_uri_id   AS uri_id,
             pr.did_id        AS author_did_id,
             pr.rkey,
             pr.created_at,
             p.reply_root_uri_id,
             p.reply_parent_uri_id,
             COALESCE(p.quote_embed_uri_id, p.quote_rwm_uri_id) AS quote_uri_id
           FROM read_parquet('{scratch}/lt_post_records.parquet') pr
           LEFT JOIN pivoted p USING (post_uri_id)
         ) TO '{raw}/posts_from_records.parquet' {copy_opts};"
    );
    run_copy(&conn, "posts_from_records", &sql)?;
    entity_counts.push((
        "posts_from_records".to_string(),
        count_rows(&conn, &format!("{raw}/posts_from_records.parquet"))?,
    ));
    let _ = std::fs::remove_file(format!("{scratch}/lt_post_refs.parquet"));
    let _ = std::fs::remove_file(format!("{scratch}/lt_post_records.parquet"));
    let _ = std::fs::remove_file(format!("{scratch}/t_post_refs.parquet"));

    // ---- posts_from_targets: dedup on uri_id (multiple target_ids
    //      may resolve to the same URI). DISTINCT ON keeps the first
    //      row per uri_id; since they all carry the same author/rkey/
    //      created_at by construction, any choice is equivalent.
    let sql = format!(
        "COPY (
           SELECT DISTINCT ON (uri_id)
             uri_id, author_did_id, rkey, created_at,
             reply_root_uri_id, reply_parent_uri_id, quote_uri_id
           FROM read_parquet('{scratch}/posts_from_targets_raw.parquet')
         ) TO '{raw}/posts_from_targets.parquet' {copy_opts};"
    );
    run_copy(&conn, "posts_from_targets", &sql)?;
    entity_counts.push((
        "posts_from_targets".to_string(),
        count_rows(&conn, &format!("{raw}/posts_from_targets.parquet"))?,
    ));
    let _ = std::fs::remove_file(format!("{scratch}/posts_from_targets_raw.parquet"));

    drop(conn);
    let _ = std::fs::remove_file(&duckdb_path);
    Ok(entity_counts)
}

fn pragma(conn: &duckdb::Connection, sql: &str) -> Result<()> {
    conn.execute_batch(sql)
        .with_context(|| format!("phase5 pragma: {sql}"))
}

fn run_copy(conn: &duckdb::Connection, label: &str, sql: &str) -> Result<()> {
    let t0 = Instant::now();
    tracing::info!(label, "phase 5 copy start");
    conn.execute_batch(sql)
        .with_context(|| format!("phase5 copy: {label}"))?;
    tracing::info!(
        label,
        elapsed_secs = t0.elapsed().as_secs_f64(),
        "phase 5 copy done"
    );
    Ok(())
}

fn count_rows(conn: &duckdb::Connection, parquet: &str) -> Result<u64> {
    let n: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM read_parquet('{parquet}')"),
        [],
        |r| r.get(0),
    )?;
    Ok(n as u64)
}
