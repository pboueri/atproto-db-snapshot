use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

use crate::config::Config;
use crate::disk::DiskGuard;
use crate::metadata::SnapshotMetadata;

#[derive(Parser, Debug)]
#[command(name = "at-snapshot", about = "ATProto analytic snapshot pipeline")]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Run the full pipeline: mirror, stage, hydrate (publish skipped).
    Build(BuildArgs),
}

#[derive(Parser, Debug)]
pub struct BuildArgs {
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
    pub disk_cap_bytes: Option<u64>,
    #[arg(long)]
    pub mirror_concurrency: Option<usize>,
    #[arg(long)]
    pub backup_id: Option<u64>,
    #[arg(long)]
    pub skip_mirror: bool,
    #[arg(long)]
    pub skip_stage: bool,
    #[arg(long)]
    pub skip_hydrate: bool,
}

pub async fn run() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Build(args) => run_build(args).await,
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

async fn run_build(args: BuildArgs) -> Result<()> {
    let mut cfg = match &args.config {
        Some(p) => Config::from_toml_file(p)?,
        None => Config::defaults(),
    };
    apply_overrides(&mut cfg, &args);

    let snapshot_date = cfg
        .snapshot_date
        .clone()
        .unwrap_or_else(|| chrono::Utc::now().format("%Y-%m-%d").to_string());

    std::fs::create_dir_all(&cfg.work_dir)
        .with_context(|| format!("create work_dir {}", cfg.work_dir.display()))?;
    let _disk = DiskGuard::start(cfg.work_dir.clone(), cfg.disk_cap_bytes);

    tracing::info!(
        snapshot_date,
        source_url = %cfg.source_url,
        work_dir = %cfg.work_dir.display(),
        disk_cap_bytes = cfg.disk_cap_bytes,
        "build start"
    );

    let mut mirror_bytes = 0u64;
    let mut stage_counts: Vec<(String, u64)> = Vec::new();
    let mut hydrate = None;

    if !args.skip_mirror {
        let m = crate::mirror::run(&cfg).await?;
        mirror_bytes = m.bytes_on_disk;
    } else {
        tracing::info!("skipping mirror stage");
    }

    if !args.skip_stage {
        let s = crate::stage::run(&cfg, &snapshot_date).await?;
        stage_counts = s.counts.clone();
        tracing::info!(?stage_counts, "stage complete");
    } else {
        tracing::info!("skipping stage");
    }

    if !args.skip_hydrate {
        let h = crate::hydrate::run(&cfg, &snapshot_date).await?;
        tracing::info!(counts = ?h.row_counts, orphan_like = h.orphan_like_rate, orphan_repost = h.orphan_repost_rate, "hydrate complete");
        hydrate = Some(h);
    } else {
        tracing::info!("skipping hydrate");
    }

    let meta = SnapshotMetadata {
        snapshot_date: snapshot_date.clone(),
        source_url: cfg.source_url.clone(),
        backup_id: cfg.backup_id,
        mirror_bytes,
        stage_counts,
        hydrate_counts: hydrate
            .as_ref()
            .map(|h| h.row_counts.clone())
            .unwrap_or_default(),
        orphan_like_rate: hydrate.as_ref().map(|h| h.orphan_like_rate).unwrap_or(0.0),
        orphan_repost_rate: hydrate.as_ref().map(|h| h.orphan_repost_rate).unwrap_or(0.0),
        generated_at: chrono::Utc::now().to_rfc3339(),
        schema_version: 1,
    };
    let meta_path = cfg
        .snapshot_dir(&snapshot_date)
        .join("snapshot_metadata.json");
    crate::metadata::write(&meta_path, &meta)?;
    tracing::info!(path = %meta_path.display(), "wrote snapshot metadata");
    if let Some(h) = hydrate.as_ref() {
        tracing::info!(
            duckdb = %h.duckdb_path.display(),
            "snapshot ready; query with: duckdb {}",
            h.duckdb_path.display()
        );
    }
    Ok(())
}

fn apply_overrides(cfg: &mut Config, args: &BuildArgs) {
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
    if let Some(c) = args.disk_cap_bytes {
        cfg.disk_cap_bytes = c;
    }
    if let Some(c) = args.mirror_concurrency {
        cfg.mirror_concurrency = c;
    }
    if let Some(b) = args.backup_id {
        cfg.backup_id = Some(b);
    }
}
