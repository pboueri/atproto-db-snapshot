use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

use crate::config::Config;
use crate::metadata::{self, UploadStats};

#[derive(Parser, Debug)]
#[command(name = "at-snapshot", about = "ATProto analytic snapshot pipeline")]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Run the full pipeline: mirror, then stage, then hydrate.
    Build(CommonArgs),
    /// Mirror the constellation rocksdb backup to local disk.
    Mirror(CommonArgs),
    /// Convert the local rocks mirror into per-entity parquet files.
    Stage(CommonArgs),
    /// Fetch account creation/deactivation dates from the PLC directory
    /// export into raw/<date>/plc/ (checkpointed + resumable). Consumed
    /// by hydrate's 07_enrich_actors_created stage.
    Plc(CommonArgs),
    /// Build snapshot.duckdb from the staged parquet files.
    Hydrate(CommonArgs),
    /// Upload raw + snapshot artifacts to the configured object store.
    Upload(CommonArgs),
    /// Cheap read-only metadata snapshot of the rocks mirror — per-CF
    /// estimate-num-keys / SST sizes, no scanning. Use to size pass B
    /// before kicking off stage.
    Inspect(CommonArgs),
    /// Replace a single table or view in an existing snapshot.duckdb
    /// with a fresh TABLE materialized from a local parquet file.
    /// Use to fix VIEW-only entity tables (actors, follows, blocks,
    /// likes, reposts) in a downloaded snapshot whose source parquets
    /// no longer live at the build host's paths.
    RepairTable(RepairTableArgs),
    /// Drop the `posts` table (and dependent aggs) and rebuild from
    /// the staging posts_from_records.parquet + posts_from_targets.parquet.
    /// Use this when posts has block-level corruption.
    RebuildPosts(RebuildPostsArgs),
    /// (Re)install the URL/URI macros (`post_url`, `actor_url`, etc.)
    /// into an existing snapshot.duckdb. Idempotent — safe to run
    /// against a snapshot that already has them.
    InstallMacros(InstallMacrosArgs),
}

#[derive(Parser, Debug, Clone)]
pub struct InstallMacrosArgs {
    /// Path to the existing snapshot.duckdb.
    #[arg(long)]
    pub db: PathBuf,
}

#[derive(Parser, Debug, Clone)]
pub struct RepairTableArgs {
    /// Path to the existing snapshot.duckdb to modify in place.
    #[arg(long)]
    pub db: PathBuf,
    /// Name of the table/view to replace (e.g. `actors`).
    #[arg(long)]
    pub table: String,
    /// Path to the parquet file to materialize the table from.
    #[arg(long)]
    pub parquet: PathBuf,
}

#[derive(Parser, Debug, Clone)]
pub struct RebuildPostsArgs {
    /// Path to the existing snapshot.duckdb to modify in place.
    #[arg(long)]
    pub db: PathBuf,
    /// Path to posts_from_records.parquet.
    #[arg(long)]
    pub records: PathBuf,
    /// Path to posts_from_targets.parquet.
    #[arg(long)]
    pub targets: PathBuf,
    /// DuckDB memory_limit. Defaults to 16GiB; bump if your machine
    /// has more RAM and the dedup is slow.
    #[arg(long, default_value = "16GiB")]
    pub memory_limit: String,
}

/// Flags shared by every subcommand. Each subcommand uses the subset
/// that applies to it; unused flags are silently ignored so a single
/// `at-snapshot --config foo.toml ...` invocation works for any phase.
#[derive(Parser, Debug, Clone)]
pub struct CommonArgs {
    #[arg(long)]
    pub config: Option<PathBuf>,
    #[arg(long)]
    pub work_dir: Option<PathBuf>,
    #[arg(long)]
    pub snapshot_date: Option<String>,
    #[arg(long)]
    pub memory_limit: Option<String>,
    #[arg(long)]
    pub batch_size: Option<usize>,
    #[arg(long)]
    pub source_url: Option<String>,
    #[arg(long)]
    pub mirror_concurrency: Option<usize>,
    /// Skip crc32c verification in mirror. Lets a warm rerun trust
    /// local SST sizes instead of reading every file end-to-end.
    #[arg(long)]
    pub mirror_no_verify: bool,
    #[arg(long)]
    pub backup_id: Option<u64>,
    /// RocksDB block cache size, e.g. "4GiB". Bigger = fewer disk reads in pass B.
    #[arg(long)]
    pub rocks_block_cache: Option<String>,
    /// Number of worker threads for pass B (link_targets scan).
    #[arg(long)]
    pub stage_threads: Option<usize>,
    /// Delete the rocks mirror once the scans finish, before Phase 5,
    /// to give DuckDB's sort spill room. DESTRUCTIVE — only pass this
    /// when the mirror is a disposable per-run copy, never against a
    /// canonical one you'd have to re-download.
    #[arg(long)]
    pub stage_drop_rocks: bool,
    /// Hydrate time-window: keep events whose created_at falls in
    /// [snapshot_date - days_back, snapshot_date - days_lag]. Both
    /// flags must be set together. Applies to likes / reposts /
    /// posts_from_*. actors / blocks / follows are always loaded
    /// in full (state, not events).
    #[arg(long)]
    pub window_days_back: Option<u32>,
    #[arg(long)]
    pub window_days_lag: Option<u32>,
    /// PLC directory base URL (the `/export` endpoint is appended).
    #[arg(long)]
    pub plc_export_url: Option<String>,
    /// Skip the PLC fetch; hydrate still adds a (NULL) created_at column.
    #[arg(long)]
    pub skip_plc: bool,
    /// PLC export page size (`count`, max 1000).
    #[arg(long)]
    pub plc_page_size: Option<usize>,
}

pub async fn run() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Build(args) => run_build(args).await,
        Cmd::Mirror(args) => run_mirror(args).await,
        Cmd::Stage(args) => run_stage(args).await,
        Cmd::Plc(args) => run_plc(args).await,
        Cmd::Hydrate(args) => run_hydrate(args).await,
        Cmd::Upload(args) => run_upload(args).await,
        Cmd::Inspect(args) => run_inspect(args).await,
        Cmd::RepairTable(args) => {
            crate::repair::repair_table(&args.db, &args.table, &args.parquet)
        }
        Cmd::RebuildPosts(args) => crate::repair::rebuild_posts(
            &args.db,
            &args.records,
            &args.targets,
            &args.memory_limit,
        ),
        Cmd::InstallMacros(args) => crate::repair::install_macros(&args.db),
    }
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

struct Prepared {
    cfg: Config,
    snapshot_date: String,
}

fn prepare(args: &CommonArgs) -> Result<Prepared> {
    let mut cfg = match &args.config {
        Some(p) => Config::from_toml_file(p)?,
        None => Config::defaults(),
    };
    apply_overrides(&mut cfg, args);

    let snapshot_date = cfg
        .snapshot_date
        .clone()
        .unwrap_or_else(|| chrono::Utc::now().format("%Y-%m-%d").to_string());

    std::fs::create_dir_all(&cfg.work_dir)
        .with_context(|| format!("create work_dir {}", cfg.work_dir.display()))?;

    Ok(Prepared { cfg, snapshot_date })
}

fn apply_overrides(cfg: &mut Config, args: &CommonArgs) {
    if let Some(d) = &args.work_dir {
        cfg.work_dir = d.clone();
    }
    if let Some(s) = &args.snapshot_date {
        cfg.snapshot_date = Some(s.clone());
    }
    if let Some(m) = &args.memory_limit {
        cfg.memory_limit = m.clone();
    }
    if let Some(b) = args.batch_size {
        cfg.batch_size = b;
    }
    if let Some(s) = &args.source_url {
        cfg.source_url = s.clone();
    }
    if let Some(c) = args.mirror_concurrency {
        cfg.mirror_concurrency = c;
    }
    if args.mirror_no_verify {
        cfg.mirror_verify = false;
    }
    if let Some(b) = args.backup_id {
        cfg.backup_id = Some(b);
    }
    if let Some(c) = &args.rocks_block_cache {
        cfg.rocks_block_cache = c.clone();
    }
    if let Some(t) = args.stage_threads {
        cfg.stage_threads = t;
    }
    if args.stage_drop_rocks {
        cfg.stage_drop_rocks = true;
    }
    if let Some(b) = args.window_days_back {
        cfg.hydrate_window_days_back = Some(b);
    }
    if let Some(l) = args.window_days_lag {
        cfg.hydrate_window_days_lag = Some(l);
    }
    if let Some(u) = &args.plc_export_url {
        cfg.plc_export_url = u.clone();
    }
    if args.skip_plc {
        cfg.skip_plc = true;
    }
    if let Some(n) = args.plc_page_size {
        cfg.plc_page_size = n;
    }
}

async fn run_build(args: CommonArgs) -> Result<()> {
    let p = prepare(&args)?;
    tracing::info!(
        snapshot_date = p.snapshot_date,
        source_url = %p.cfg.source_url,
        work_dir = %p.cfg.work_dir.display(),
        "build start"
    );

    // The PLC fetch only talks to plc.directory — it's independent of the
    // rocks mirror and stage, so run it concurrently with the mirror→stage
    // chain (which is sequential: stage needs the mirrored rocksdb). Hydrate
    // waits on both: it needs the staged parquets AND the plc shards.
    let mirror_then_stage = async {
        do_mirror(&p).await?;
        do_stage(&p).await?;
        Ok::<(), anyhow::Error>(())
    };
    tokio::try_join!(do_plc(&p), mirror_then_stage)?;
    do_hydrate(&p).await?;

    Ok(())
}

async fn run_mirror(args: CommonArgs) -> Result<()> {
    let p = prepare(&args)?;
    do_mirror(&p).await?;
    Ok(())
}

async fn run_stage(args: CommonArgs) -> Result<()> {
    let p = prepare(&args)?;
    do_stage(&p).await?;
    Ok(())
}

async fn run_plc(args: CommonArgs) -> Result<()> {
    let p = prepare(&args)?;
    do_plc(&p).await?;
    Ok(())
}

async fn run_hydrate(args: CommonArgs) -> Result<()> {
    let p = prepare(&args)?;
    do_hydrate(&p).await?;
    Ok(())
}

async fn run_upload(args: CommonArgs) -> Result<()> {
    let p = prepare(&args)?;
    do_upload(&p).await?;
    Ok(())
}

async fn run_inspect(args: CommonArgs) -> Result<()> {
    let p = prepare(&args)?;
    let rocks_dir = p.cfg.rocks_dir();
    let cache_bytes = p.cfg.rocks_block_cache_bytes()?;
    let outcome = crate::inspect::run(&rocks_dir, cache_bytes)?;
    let total_keys: u64 = outcome.per_cf.iter().map(|s| s.estimate_num_keys).sum();
    let total_sst: u64 = outcome.per_cf.iter().map(|s| s.live_sst_files_size).sum();
    tracing::info!(
        total_estimate_num_keys = total_keys,
        total_live_sst_size_gb = format!("{:.2}", total_sst as f64 / 1e9),
        "inspect complete"
    );
    Ok(())
}

async fn do_mirror(p: &Prepared) -> Result<()> {
    let m = crate::mirror::run(&p.cfg).await?;
    let path = metadata::update(&p.cfg, &p.snapshot_date, |meta| {
        meta.mirror_bytes = m.bytes_on_disk;
    })?;
    tracing::info!(path = %path.display(), bytes = m.bytes_on_disk, "mirror complete; metadata updated");
    Ok(())
}

async fn do_stage(p: &Prepared) -> Result<()> {
    let s = crate::stage::run(&p.cfg, &p.snapshot_date).await?;
    let path = metadata::update(&p.cfg, &p.snapshot_date, |meta| {
        meta.stage_counts = s.counts.clone();
    })?;
    tracing::info!(path = %path.display(), counts = ?s.counts, "stage complete; metadata updated");
    Ok(())
}

async fn do_plc(p: &Prepared) -> Result<()> {
    let o = crate::plc::run(&p.cfg, &p.snapshot_date).await?;
    tracing::info!(
        rows = o.rows,
        shards = o.shards,
        completed = o.completed,
        "plc phase complete"
    );
    Ok(())
}

async fn do_hydrate(p: &Prepared) -> Result<()> {
    let h = crate::hydrate::run(&p.cfg, &p.snapshot_date).await?;
    let path = metadata::update(&p.cfg, &p.snapshot_date, |meta| {
        meta.hydrate_counts = h.row_counts.clone();
        meta.orphan_like_rate = h.orphan_like_rate;
        meta.orphan_repost_rate = h.orphan_repost_rate;
    })?;
    tracing::info!(
        duckdb = %h.duckdb_path.display(),
        metadata = %path.display(),
        counts = ?h.row_counts,
        orphan_like = h.orphan_like_rate,
        orphan_repost = h.orphan_repost_rate,
        "hydrate complete; query with: duckdb {}",
        h.duckdb_path.display()
    );
    Ok(())
}

async fn do_upload(p: &Prepared) -> Result<()> {
    let outcome = crate::upload::run(&p.cfg, &p.snapshot_date).await?;
    let stats = UploadStats {
        kind: outcome.kind.clone(),
        bucket: outcome.bucket.clone(),
        prefix: outcome.prefix.clone(),
        files: outcome.files,
        bytes: outcome.bytes,
        completed_at: chrono::Utc::now().to_rfc3339(),
    };
    let path = metadata::update(&p.cfg, &p.snapshot_date, |meta| {
        meta.upload = Some(stats);
    })?;
    tracing::info!(
        bucket = outcome.bucket,
        prefix = outcome.prefix,
        files = outcome.files,
        bytes = outcome.bytes,
        metadata = %path.display(),
        "upload complete; metadata updated"
    );
    Ok(())
}
