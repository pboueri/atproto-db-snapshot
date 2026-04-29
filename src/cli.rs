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
    /// Build snapshot.duckdb from the staged parquet files.
    Hydrate(CommonArgs),
    /// Upload raw + snapshot artifacts to the configured object store.
    Upload(CommonArgs),
    /// Cheap read-only metadata snapshot of the rocks mirror — per-CF
    /// estimate-num-keys / SST sizes, no scanning. Use to size pass B
    /// before kicking off stage.
    Inspect(CommonArgs),
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
    #[arg(long)]
    pub backup_id: Option<u64>,
    /// RocksDB block cache size, e.g. "4GiB". Bigger = fewer disk reads in pass B.
    #[arg(long)]
    pub rocks_block_cache: Option<String>,
    /// Number of worker threads for pass B (link_targets scan).
    #[arg(long)]
    pub stage_threads: Option<usize>,
}

pub async fn run() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Build(args) => run_build(args).await,
        Cmd::Mirror(args) => run_mirror(args).await,
        Cmd::Stage(args) => run_stage(args).await,
        Cmd::Hydrate(args) => run_hydrate(args).await,
        Cmd::Upload(args) => run_upload(args).await,
        Cmd::Inspect(args) => run_inspect(args).await,
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
    if let Some(b) = args.backup_id {
        cfg.backup_id = Some(b);
    }
    if let Some(c) = &args.rocks_block_cache {
        cfg.rocks_block_cache = c.clone();
    }
    if let Some(t) = args.stage_threads {
        cfg.stage_threads = t;
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

    do_mirror(&p).await?;
    do_stage(&p).await?;
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
