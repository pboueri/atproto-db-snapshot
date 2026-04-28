use anyhow::{Context, Result};
use rocksdb::{IteratorMode, ReadOptions, DB};
use std::path::PathBuf;

use crate::config::Config;
use crate::rocks::open::open_readonly;
use crate::rocks::schema::{
    self as rs, AtUri, Collection, Did, DidIdValue, RKey, RecordLinkKey, RecordLinkTargets,
    Target, TargetKey,
};
use crate::tid;
use crate::writers::{
    ActorWriter, BlockWriter, FollowWriter, LikeWriter, PostFromRecordWriter,
    PostFromTargetWriter, PostMediaWriter, RepostWriter,
};

const COL_POST: &str = "app.bsky.feed.post";
const COL_LIKE: &str = "app.bsky.feed.like";
const COL_REPOST: &str = "app.bsky.feed.repost";
const COL_FOLLOW: &str = "app.bsky.graph.follow";
const COL_BLOCK: &str = "app.bsky.graph.block";
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
    let db = open_readonly(&rocks_dir)?;

    let actors_count = pass_a_actors(&db, &raw_dir, cfg.batch_size)?;
    let pass_b = pass_b_link_targets(&db, &raw_dir, cfg.batch_size)?;
    let posts_targets_count = pass_c_target_ids(&db, &raw_dir, cfg.batch_size)?;

    let counts = vec![
        ("actors".to_string(), actors_count),
        ("follows".to_string(), pass_b.follows),
        ("blocks".to_string(), pass_b.blocks),
        ("likes".to_string(), pass_b.likes),
        ("reposts".to_string(), pass_b.reposts),
        (
            "posts_from_records".to_string(),
            pass_b.posts_from_records,
        ),
        ("post_media".to_string(), pass_b.post_media),
        (
            "posts_from_targets".to_string(),
            posts_targets_count,
        ),
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
    follows: u64,
    blocks: u64,
    likes: u64,
    reposts: u64,
    posts_from_records: u64,
    post_media: u64,
}

fn pass_b_link_targets(
    db: &DB,
    raw_dir: &std::path::Path,
    batch_size: usize,
) -> Result<PassBCounts> {
    let cf_link_targets = db
        .cf_handle(rs::CF_LINK_TARGETS)
        .context("missing link_targets cf")?;
    let cf_target_ids = db
        .cf_handle(rs::CF_TARGET_IDS)
        .context("missing target_ids cf")?;
    let cf_did_ids = db.cf_handle(rs::CF_DID_IDS).context("missing did_ids cf")?;

    let mut follows = FollowWriter::create(
        raw_dir.join("follows.parquet"),
        batch_size,
        "src_did_id",
        "dst_did_id",
    )?;
    let mut blocks = BlockWriter::create(
        raw_dir.join("blocks.parquet"),
        batch_size,
        "src_did_id",
        "dst_did_id",
    )?;
    let mut likes = LikeWriter::create(raw_dir.join("likes.parquet"), batch_size)?;
    let mut reposts = RepostWriter::create(raw_dir.join("reposts.parquet"), batch_size)?;
    let mut posts =
        PostFromRecordWriter::create(raw_dir.join("posts_from_records.parquet"), batch_size)?;
    let mut media = PostMediaWriter::create(raw_dir.join("post_media.parquet"), batch_size)?;

    let mut counts = PassBCounts::default();

    let mut read_opts = ReadOptions::default();
    read_opts.set_verify_checksums(false);
    let iter = db.iterator_cf_opt(cf_link_targets, read_opts, IteratorMode::Start);

    let mut author_did_cache = lru::LruCache::<u64, String>::new(
        std::num::NonZeroUsize::new(50_000).unwrap(),
    );
    let mut target_uri_cache = lru::LruCache::<u64, (String, String, String)>::new(
        std::num::NonZeroUsize::new(200_000).unwrap(),
    );
    let mut did_to_id_cache = lru::LruCache::<String, Option<u64>>::new(
        std::num::NonZeroUsize::new(50_000).unwrap(),
    );

    let mut scanned = 0u64;
    let mut bad_link_keys = 0u64;
    let mut bad_link_values = 0u64;
    for item in iter {
        let (k, v) = item.context("iterate link_targets")?;
        scanned += 1;
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
        let RecordLinkKey(did_id, Collection(collection), RKey(rkey)) = key;
        let created_at = tid::decode_tid_micros(&rkey);

        if collection == COL_PROFILE {
            continue;
        }

        match collection.as_str() {
            COL_POST => {
                handle_post(
                    db,
                    cf_target_ids,
                    cf_did_ids,
                    did_id.0,
                    &rkey,
                    created_at,
                    &value,
                    &mut author_did_cache,
                    &mut target_uri_cache,
                    &mut posts,
                    &mut media,
                    &mut counts,
                )?;
            }
            COL_LIKE | COL_REPOST => {
                if let Some((_target, _coll, _path)) = lookup_first_subject_uri(
                    db,
                    cf_target_ids,
                    &value,
                    ".subject.uri",
                    &mut target_uri_cache,
                )? {
                    let writer = if collection == COL_LIKE {
                        &mut likes
                    } else {
                        &mut reposts
                    };
                    writer.push(did_id.0, &_target, &rkey, created_at)?;
                    if collection == COL_LIKE {
                        counts.likes += 1;
                    } else {
                        counts.reposts += 1;
                    }
                }
            }
            COL_FOLLOW | COL_BLOCK => {
                if let Some((target, _, _)) = lookup_first_subject_uri(
                    db,
                    cf_target_ids,
                    &value,
                    ".subject",
                    &mut target_uri_cache,
                )? {
                    if let Some(dst_did_id) =
                        resolve_did_to_id(db, cf_did_ids, &target, &mut did_to_id_cache)?
                    {
                        let writer = if collection == COL_FOLLOW {
                            &mut follows
                        } else {
                            &mut blocks
                        };
                        writer.push(did_id.0, dst_did_id, &rkey, created_at)?;
                        if collection == COL_FOLLOW {
                            counts.follows += 1;
                        } else {
                            counts.blocks += 1;
                        }
                    }
                }
            }
            _ => {}
        }
        if scanned % 1_000_000 == 0 {
            tracing::info!(
                scanned,
                follows = counts.follows,
                likes = counts.likes,
                posts = counts.posts_from_records,
                bad_link_keys,
                bad_link_values,
                "pass B progress"
            );
        }
    }

    let (_, _) = follows.finish()?;
    let (_, _) = blocks.finish()?;
    let (_, _) = likes.finish()?;
    let (_, _) = reposts.finish()?;
    let (_, _) = posts.finish()?;
    let (_, _) = media.finish()?;
    tracing::info!(
        scanned,
        follows = counts.follows,
        blocks = counts.blocks,
        likes = counts.likes,
        reposts = counts.reposts,
        posts_from_records = counts.posts_from_records,
        post_media = counts.post_media,
        bad_link_keys,
        bad_link_values,
        "pass B complete"
    );
    Ok(counts)
}

fn pass_c_target_ids(
    db: &DB,
    raw_dir: &std::path::Path,
    batch_size: usize,
) -> Result<u64> {
    let cf = db
        .cf_handle(rs::CF_TARGET_IDS)
        .context("missing target_ids cf")?;
    let mut writer =
        PostFromTargetWriter::create(raw_dir.join("posts_from_targets.parquet"), batch_size)?;

    let mut read_opts = ReadOptions::default();
    read_opts.set_verify_checksums(false);
    let iter = db.iterator_cf_opt(cf, read_opts, IteratorMode::Start);

    let mut scanned = 0u64;
    let mut emitted = 0u64;
    let mut bad_target_keys = 0u64;
    let mut non_post_targets = 0u64;
    for item in iter {
        let (k, _v) = item.context("iterate target_ids")?;
        scanned += 1;
        if k.len() == 8 {
            continue;
        }
        let TargetKey(Target(target), _, _) = match rs::decode::<TargetKey>(&k) {
            Ok(t) => t,
            Err(e) => {
                if bad_target_keys == 0 {
                    tracing::warn!(error=?e, key=?k, "first undecodable target_ids key (further occurrences counted only)");
                }
                bad_target_keys += 1;
                continue;
            }
        };
        let Some(uri) = parse_post_uri(&target) else {
            non_post_targets += 1;
            continue;
        };
        let created = tid::decode_tid_micros(&uri.rkey);
        writer.push(&target, &uri.did, &uri.rkey, created)?;
        emitted += 1;
        if emitted % 1_000_000 == 0 {
            tracing::info!(
                scanned,
                emitted,
                bad_target_keys,
                non_post_targets,
                "pass C progress"
            );
        }
    }
    let (path, total) = writer.finish()?;
    tracing::info!(
        scanned,
        emitted = total,
        bad_target_keys,
        non_post_targets,
        path = %path.display(),
        "pass C complete"
    );
    Ok(total)
}

fn parse_post_uri(s: &str) -> Option<AtUri> {
    let uri = rs::parse_at_uri(s)?;
    if uri.collection == COL_POST {
        Some(uri)
    } else {
        None
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_post(
    db: &DB,
    cf_target_ids: &rocksdb::ColumnFamily,
    cf_did_ids: &rocksdb::ColumnFamily,
    author_did_id: u64,
    rkey: &str,
    created_at: Option<i64>,
    targets: &RecordLinkTargets,
    author_did_cache: &mut lru::LruCache<u64, String>,
    target_uri_cache: &mut lru::LruCache<u64, (String, String, String)>,
    posts: &mut PostFromRecordWriter,
    media: &mut PostMediaWriter,
    counts: &mut PassBCounts,
) -> Result<()> {
    let author_did = match resolve_did_id_to_string(db, cf_did_ids, author_did_id, author_did_cache)?
    {
        Some(s) => s,
        None => return Ok(()),
    };
    let uri = format!("at://{}/{}/{}", author_did, COL_POST, rkey);

    let mut reply_root: Option<String> = None;
    let mut reply_parent: Option<String> = None;
    let mut quote: Option<String> = None;
    let mut media_ord: i32 = 0;

    for t in &targets.0 {
        let path = &t.0 .0;
        let target_id = t.1;
        let resolved = resolve_target(db, cf_target_ids, target_id.0, target_uri_cache)?;
        let Some((target_str, _, _)) = resolved else {
            continue;
        };
        match path.as_str() {
            ".reply.root.uri" => reply_root = Some(target_str.clone()),
            ".reply.parent.uri" => reply_parent = Some(target_str.clone()),
            ".embed.record.uri" => quote = Some(target_str.clone()),
            p if p.starts_with(".embed.images") => {
                media.push(&uri, media_ord, "image", &target_str)?;
                media_ord += 1;
                counts.post_media += 1;
            }
            ".embed.video.video" => {
                media.push(&uri, media_ord, "video", &target_str)?;
                media_ord += 1;
                counts.post_media += 1;
            }
            ".embed.external.uri" => {
                media.push(&uri, media_ord, "external", &target_str)?;
                media_ord += 1;
                counts.post_media += 1;
            }
            ".embed.external.thumb" => {
                media.push(&uri, media_ord, "external_thumb", &target_str)?;
                media_ord += 1;
                counts.post_media += 1;
            }
            p if p.starts_with(".embed.recordWithMedia.media.images") => {
                media.push(&uri, media_ord, "image", &target_str)?;
                media_ord += 1;
                counts.post_media += 1;
            }
            ".embed.recordWithMedia.record.record.uri" => quote = Some(target_str.clone()),
            _ => {}
        }
    }

    posts.push(
        &uri,
        author_did_id,
        rkey,
        created_at,
        reply_root.as_deref(),
        reply_parent.as_deref(),
        quote.as_deref(),
    )?;
    counts.posts_from_records += 1;
    Ok(())
}

fn resolve_target(
    db: &DB,
    cf: &rocksdb::ColumnFamily,
    target_id: u64,
    cache: &mut lru::LruCache<u64, (String, String, String)>,
) -> Result<Option<(String, String, String)>> {
    if let Some(v) = cache.get(&target_id) {
        return Ok(Some(v.clone()));
    }
    let raw = db
        .get_cf(cf, target_id.to_be_bytes())
        .context("get_cf target_ids reverse")?;
    let Some(raw) = raw else { return Ok(None) };
    let TargetKey(Target(t), Collection(c), crate::rocks::schema::RPath(p)) =
        match rs::decode::<TargetKey>(&raw) {
            Ok(k) => k,
            Err(_) => return Ok(None),
        };
    let tup = (t, c, p);
    cache.put(target_id, tup.clone());
    Ok(Some(tup))
}

fn lookup_first_subject_uri(
    db: &DB,
    cf: &rocksdb::ColumnFamily,
    targets: &RecordLinkTargets,
    expected_path: &str,
    cache: &mut lru::LruCache<u64, (String, String, String)>,
) -> Result<Option<(String, String, String)>> {
    for t in &targets.0 {
        if t.0 .0 == expected_path {
            return resolve_target(db, cf, t.1 .0, cache);
        }
    }
    Ok(None)
}

fn resolve_did_id_to_string(
    db: &DB,
    cf: &rocksdb::ColumnFamily,
    did_id: u64,
    cache: &mut lru::LruCache<u64, String>,
) -> Result<Option<String>> {
    if let Some(s) = cache.get(&did_id) {
        return Ok(Some(s.clone()));
    }
    let raw = db
        .get_cf(cf, did_id.to_be_bytes())
        .context("get_cf did_ids reverse")?;
    let Some(raw) = raw else { return Ok(None) };
    let did: Did = match rs::decode(&raw) {
        Ok(d) => d,
        Err(_) => return Ok(None),
    };
    cache.put(did_id, did.0.clone());
    Ok(Some(did.0))
}

fn resolve_did_to_id(
    db: &DB,
    cf: &rocksdb::ColumnFamily,
    did_str: &str,
    cache: &mut lru::LruCache<String, Option<u64>>,
) -> Result<Option<u64>> {
    if let Some(v) = cache.get(did_str) {
        return Ok(*v);
    }
    let key = bincode::Options::serialize(rs::bincode_opts(), &Did(did_str.to_string()))
        .context("encode did key")?;
    let raw = db.get_cf(cf, key).context("get_cf did_ids forward")?;
    let resolved = match raw {
        Some(raw) => match rs::decode::<DidIdValue>(&raw) {
            Ok(v) => Some(v.0 .0),
            Err(_) => None,
        },
        None => None,
    };
    cache.put(did_str.to_string(), resolved);
    Ok(resolved)
}
