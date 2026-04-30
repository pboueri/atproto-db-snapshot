//! Synthetic rocks generator for stage profiling.
//!
//! Builds a constellation-shaped rocksdb at parametric scale. Output
//! mirrors the four CFs (`did_ids`, `target_ids`, `target_links`,
//! `link_targets`) with realistic key/value distributions and ratios
//! roughly matching production (post:like:follow ≈ 1:5:2). Use to
//! profile stage end-to-end without paying the ~80 GB constellation
//! download.
//!
//! Usage:
//!   cargo run --release --bin synth_gen -- \
//!     --out ./var/synth-1m/rocks \
//!     --actors 1_000_000 \
//!     --scale 1.0
//!
//! `--scale` multiplies all per-actor activity (posts, likes,
//! follows, blocks, reposts) so 0.5 halves activity, 2.0 doubles it.
//! At default scale + 1M actors the rocks DB lands ~5-8 GB; at 10M
//! actors ~50-80 GB. Pick the smallest size that surfaces the
//! bottleneck you care about.

use anyhow::{Context, Result};
use bincode::Options;
use clap::Parser;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rocksdb::{ColumnFamilyDescriptor, Options as RocksOpts, DB};
use std::path::PathBuf;
use std::time::Instant;

use at_snapshot::rocks::schema::{
    bincode_opts, Collection, Did, DidId, DidIdValue, RKey, RPath, RecordLinkKey,
    RecordLinkTarget, RecordLinkTargets, Target, TargetId, TargetKey, CF_DID_IDS, CF_LINK_TARGETS,
    CF_TARGET_IDS, CF_TARGET_LINKS,
};

#[derive(Parser, Debug)]
#[command(name = "synth_gen", about = "Synthetic constellation rocks generator")]
struct Args {
    /// Output rocks directory.
    #[arg(long)]
    out: PathBuf,
    /// Number of actors. Activity counts scale linearly with this.
    #[arg(long, default_value_t = 100_000)]
    actors: u64,
    /// Activity multiplier. 1.0 ≈ realistic per-actor counts.
    #[arg(long, default_value_t = 1.0)]
    scale: f64,
    /// Random seed for deterministic output.
    #[arg(long, default_value_t = 0xC0FFEE)]
    seed: u64,
    /// Fraction of target subjects (likes/reposts) that point at a
    /// "deleted" post (not present in the post universe). Models the
    /// real-world orphan rate and exercises the LEFT JOIN path.
    #[arg(long, default_value_t = 0.05)]
    orphan_rate: f64,
    /// Fraction of actors marked inactive.
    #[arg(long, default_value_t = 0.10)]
    inactive_rate: f64,
}

const COL_POST: &str = "app.bsky.feed.post";
const COL_LIKE: &str = "app.bsky.feed.like";
const COL_REPOST: &str = "app.bsky.feed.repost";
const COL_FOLLOW: &str = "app.bsky.graph.follow";
const COL_BLOCK: &str = "app.bsky.graph.block";

// Per-active-actor activity at scale=1.0. Roughly bsky-shaped.
const ACTIVE_FRAC: f64 = 0.5;
const POSTS_PER_ACTIVE: f64 = 30.0;
const LIKES_PER_ACTIVE: f64 = 150.0;
const FOLLOWS_PER_ACTIVE: f64 = 80.0;
const BLOCKS_PER_ACTIVE: f64 = 0.5;
const REPOSTS_PER_ACTIVE: f64 = 20.0;
// Per-post probability of having a reply parent / reply root / quote.
// (Reply root and parent always co-occur; quote is independent.)
const REPLY_PROB: f64 = 0.20;
const QUOTE_PROB: f64 = 0.05;

const TID_ALPHABET: &[u8; 32] = b"234567abcdefghijklmnopqrstuvwxyz";

fn make_did(i: u64) -> String {
    // Real DIDs are `did:plc:<24 lowercase b32 chars>`. We don't need
    // resolvability — just unique strings of similar length.
    let mut out = String::with_capacity(40);
    out.push_str("did:plc:");
    let mut x = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    for _ in 0..24 {
        out.push(char::from(TID_ALPHABET[(x & 0x1f) as usize]));
        x = x.rotate_left(5).wrapping_add(0x1234_5678);
    }
    out
}

fn make_tid(rng: &mut SmallRng, base_micros: i64) -> String {
    let micros = base_micros + rng.gen_range(0..100_000_000) as i64;
    let value = ((micros as u64) << 10) | (rng.gen::<u64>() & 0x3ff);
    let mut out = [0u8; 13];
    for i in (0..13).rev() {
        out[i] = TID_ALPHABET[((value >> (5 * (12 - i))) & 0x1f) as usize];
    }
    String::from_utf8(out.to_vec()).unwrap()
}

struct Builder {
    db: DB,
    next_target_id: u64,
    n_did_ids: u64,
    n_target_ids: u64,
    n_link_targets: u64,
}

impl Builder {
    fn open(path: &std::path::Path) -> Result<Self> {
        if path.exists() {
            std::fs::remove_dir_all(path)
                .with_context(|| format!("rm -rf {}", path.display()))?;
        }
        std::fs::create_dir_all(path)
            .with_context(|| format!("mkdir -p {}", path.display()))?;
        let mut opts = RocksOpts::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        // Fast bulk-load tweaks. The real constellation DB is built by
        // a different process; we just need this dir to be readable
        // by stage's read-only open with the same CF list.
        opts.set_max_background_jobs(4);
        opts.set_disable_auto_compactions(true);
        opts.set_unordered_write(true);
        let cfs = vec![
            ColumnFamilyDescriptor::new(CF_DID_IDS, RocksOpts::default()),
            ColumnFamilyDescriptor::new(CF_TARGET_IDS, RocksOpts::default()),
            ColumnFamilyDescriptor::new(CF_TARGET_LINKS, RocksOpts::default()),
            ColumnFamilyDescriptor::new(CF_LINK_TARGETS, RocksOpts::default()),
        ];
        let db = DB::open_cf_descriptors(&opts, path, cfs)
            .with_context(|| format!("open rocks at {}", path.display()))?;
        // Mark mirror complete so stage's checks pass.
        std::fs::write(path.join(".cursor"), b"{}").context("write .cursor")?;
        Ok(Self {
            db,
            next_target_id: 1,
            n_did_ids: 0,
            n_target_ids: 0,
            n_link_targets: 0,
        })
    }

    fn put_actor(&mut self, did: &str, did_id: u64, active: bool) -> Result<()> {
        let cf = self.db.cf_handle(CF_DID_IDS).unwrap();
        let opts = bincode_opts();
        let did_obj = Did(did.to_string());
        let val = DidIdValue(DidId(did_id), active);
        let k = opts.serialize(&did_obj)?;
        let v = opts.serialize(&val)?;
        self.db.put_cf(&cf, k, v)?;
        let kr = did_id.to_be_bytes();
        let vr = opts.serialize(&did_obj)?;
        self.db.put_cf(&cf, kr, vr)?;
        self.n_did_ids += 1;
        Ok(())
    }

    fn put_target(&mut self, target: &str, collection: &str, rpath: &str) -> Result<u64> {
        let target_id = self.next_target_id;
        self.next_target_id += 1;
        let cf = self.db.cf_handle(CF_TARGET_IDS).unwrap();
        let opts = bincode_opts();
        let tk = TargetKey(
            Target(target.to_string()),
            Collection(collection.to_string()),
            RPath(rpath.to_string()),
        );
        let k = opts.serialize(&tk)?;
        let v = opts.serialize(&TargetId(target_id))?;
        self.db.put_cf(&cf, k, v)?;
        let kr = target_id.to_be_bytes();
        let vr = opts.serialize(&tk)?;
        self.db.put_cf(&cf, kr, vr)?;
        self.n_target_ids += 1;
        Ok(target_id)
    }

    fn put_record_links(
        &mut self,
        did_id: u64,
        collection: &str,
        rkey: &str,
        targets: &[(String, u64)],
    ) -> Result<()> {
        let cf = self.db.cf_handle(CF_LINK_TARGETS).unwrap();
        let opts = bincode_opts();
        let key = RecordLinkKey(
            DidId(did_id),
            Collection(collection.to_string()),
            RKey(rkey.to_string()),
        );
        let value = RecordLinkTargets(
            targets
                .iter()
                .map(|(p, t)| RecordLinkTarget(RPath(p.clone()), TargetId(*t)))
                .collect(),
        );
        let k = opts.serialize(&key)?;
        let v = opts.serialize(&value)?;
        self.db.put_cf(&cf, k, v)?;
        self.n_link_targets += 1;
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        let cfs = [CF_DID_IDS, CF_TARGET_IDS, CF_TARGET_LINKS, CF_LINK_TARGETS];
        for cf_name in cfs {
            let cf = self.db.cf_handle(cf_name).unwrap();
            self.db.flush_cf(&cf).context(cf_name)?;
        }
        // Run a final compaction so the read side hits SSTs in tiered
        // form rather than memtables, matching real-world layout.
        for cf_name in cfs {
            let cf = self.db.cf_handle(cf_name).unwrap();
            self.db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let total_t0 = Instant::now();
    tracing::info!(?args, "synth_gen starting");

    let mut rng = SmallRng::seed_from_u64(args.seed);
    let mut b = Builder::open(&args.out)?;

    // ---- Phase 1: actors
    let n_actors = args.actors;
    let n_active = (n_actors as f64 * ACTIVE_FRAC) as u64;
    let dids: Vec<String> = (0..n_actors).map(make_did).collect();
    {
        let t0 = Instant::now();
        for (i, did) in dids.iter().enumerate() {
            let inactive = rng.gen::<f64>() < args.inactive_rate;
            b.put_actor(did, (i + 1) as u64, !inactive)?;
        }
        b.flush()?;
        tracing::info!(actors = n_actors, secs = t0.elapsed().as_secs_f64(), "actors written");
    }

    // ---- Phase 2: posts (one record + 0..2 ref rpaths each)
    let posts_per_active = (POSTS_PER_ACTIVE * args.scale) as u64;
    let total_posts = n_active * posts_per_active;
    let base_micros: i64 = 1_770_000_000_000_000; // ~mid-2026
    let mut post_uris: Vec<(u64, String, String)> = Vec::with_capacity(total_posts as usize);
    {
        let t0 = Instant::now();
        let mut emitted = 0u64;
        for i in 0..n_active {
            let author_idx = (i as usize) * (n_actors as usize) / (n_active as usize);
            let did_id = (author_idx + 1) as u64;
            let author_did = &dids[author_idx];
            for _ in 0..posts_per_active {
                let rkey = make_tid(&mut rng, base_micros);
                let uri = format!("at://{}/{}/{}", author_did, COL_POST, rkey);
                let mut targets: Vec<(String, u64)> = Vec::new();
                if !post_uris.is_empty() {
                    if rng.gen::<f64>() < REPLY_PROB {
                        let r = &post_uris[rng.gen_range(0..post_uris.len())];
                        let tid_root = b.put_target(&r.1, COL_POST, ".reply.root.uri")?;
                        let tid_par = b.put_target(&r.1, COL_POST, ".reply.parent.uri")?;
                        targets.push((".reply.root.uri".to_string(), tid_root));
                        targets.push((".reply.parent.uri".to_string(), tid_par));
                    }
                    if rng.gen::<f64>() < QUOTE_PROB {
                        let r = &post_uris[rng.gen_range(0..post_uris.len())];
                        let tid_q = b.put_target(&r.1, COL_POST, ".embed.record.uri")?;
                        targets.push((".embed.record.uri".to_string(), tid_q));
                    }
                }
                b.put_record_links(did_id, COL_POST, &rkey, &targets)?;
                post_uris.push((did_id, uri, rkey));
                emitted += 1;
                if emitted % 500_000 == 0 {
                    tracing::info!(emitted, total = total_posts, "posts progress");
                }
            }
        }
        b.flush()?;
        tracing::info!(posts = emitted, secs = t0.elapsed().as_secs_f64(), "posts written");
    }

    // ---- Phase 3: likes/reposts (subject = post URI; some orphans)
    let likes_per_active = (LIKES_PER_ACTIVE * args.scale) as u64;
    let reposts_per_active = (REPOSTS_PER_ACTIVE * args.scale) as u64;
    {
        let t0 = Instant::now();
        let mut emitted = 0u64;
        for i in 0..n_active {
            let actor_idx = (i as usize) * (n_actors as usize) / (n_active as usize);
            let did_id = (actor_idx + 1) as u64;
            for _ in 0..likes_per_active {
                let rkey = make_tid(&mut rng, base_micros);
                let subject_uri = if rng.gen::<f64>() < args.orphan_rate || post_uris.is_empty() {
                    // Synthetic orphan: looks like an at-URI but points
                    // at a fictional collection. Stage will still emit
                    // a row in t_post_refs (we only filter by collection
                    // there) but hydrate's LEFT JOIN to posts will miss.
                    format!("at://{}/{}/{}", make_did(rng.gen()), COL_POST, make_tid(&mut rng, base_micros))
                } else {
                    post_uris[rng.gen_range(0..post_uris.len())].1.clone()
                };
                let tid = b.put_target(&subject_uri, COL_LIKE, ".subject.uri")?;
                b.put_record_links(did_id, COL_LIKE, &rkey, &[(".subject.uri".to_string(), tid)])?;
                emitted += 1;
            }
            for _ in 0..reposts_per_active {
                let rkey = make_tid(&mut rng, base_micros);
                let subject_uri = if rng.gen::<f64>() < args.orphan_rate || post_uris.is_empty() {
                    format!("at://{}/{}/{}", make_did(rng.gen()), COL_POST, make_tid(&mut rng, base_micros))
                } else {
                    post_uris[rng.gen_range(0..post_uris.len())].1.clone()
                };
                let tid = b.put_target(&subject_uri, COL_REPOST, ".subject.uri")?;
                b.put_record_links(did_id, COL_REPOST, &rkey, &[(".subject.uri".to_string(), tid)])?;
                emitted += 1;
            }
            if emitted >= 500_000 {
                tracing::info!(emitted, "engagement progress");
                emitted = 0;
            }
        }
        b.flush()?;
        tracing::info!(secs = t0.elapsed().as_secs_f64(), "engagement written");
    }

    // ---- Phase 4: follows + blocks (subject = DID)
    let follows_per_active = (FOLLOWS_PER_ACTIVE * args.scale) as u64;
    let blocks_per_active = (BLOCKS_PER_ACTIVE * args.scale).ceil() as u64;
    {
        let t0 = Instant::now();
        for i in 0..n_active {
            let actor_idx = (i as usize) * (n_actors as usize) / (n_active as usize);
            let did_id = (actor_idx + 1) as u64;
            for _ in 0..follows_per_active {
                let rkey = make_tid(&mut rng, base_micros);
                let target_idx = rng.gen_range(0..n_actors as usize);
                let target_did = &dids[target_idx];
                let tid = b.put_target(target_did, COL_FOLLOW, ".subject")?;
                b.put_record_links(did_id, COL_FOLLOW, &rkey, &[(".subject".to_string(), tid)])?;
            }
            for _ in 0..blocks_per_active {
                let rkey = make_tid(&mut rng, base_micros);
                let target_idx = rng.gen_range(0..n_actors as usize);
                let target_did = &dids[target_idx];
                let tid = b.put_target(target_did, COL_BLOCK, ".subject")?;
                b.put_record_links(did_id, COL_BLOCK, &rkey, &[(".subject".to_string(), tid)])?;
            }
        }
        b.flush()?;
        tracing::info!(secs = t0.elapsed().as_secs_f64(), "graph written");
    }

    tracing::info!(
        actors = b.n_did_ids,
        target_ids = b.n_target_ids,
        link_targets = b.n_link_targets,
        secs = total_t0.elapsed().as_secs_f64(),
        path = %args.out.display(),
        "synth_gen complete"
    );

    Ok(())
}
