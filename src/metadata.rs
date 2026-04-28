use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::config::Config;

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SnapshotMetadata {
    pub snapshot_date: String,
    pub source_url: String,
    #[serde(default)]
    pub backup_id: Option<u64>,
    #[serde(default)]
    pub mirror_bytes: u64,
    #[serde(default)]
    pub stage_counts: Vec<(String, u64)>,
    #[serde(default)]
    pub hydrate_counts: Vec<(String, u64)>,
    #[serde(default)]
    pub orphan_like_rate: f64,
    #[serde(default)]
    pub orphan_repost_rate: f64,
    #[serde(default)]
    pub upload: Option<UploadStats>,
    #[serde(default)]
    pub generated_at: String,
    #[serde(default)]
    pub schema_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadStats {
    pub kind: String,
    pub bucket: String,
    pub prefix: String,
    pub files: u64,
    pub bytes: u64,
    pub completed_at: String,
}

pub fn path_for(cfg: &Config, snapshot_date: &str) -> PathBuf {
    cfg.snapshot_dir(snapshot_date)
        .join("snapshot_metadata.json")
}

pub fn read_or_seed(path: &Path, cfg: &Config, snapshot_date: &str) -> Result<SnapshotMetadata> {
    if path.exists() {
        let body = std::fs::read(path)
            .with_context(|| format!("read {}", path.display()))?;
        let mut m: SnapshotMetadata = serde_json::from_slice(&body)
            .with_context(|| format!("parse {}", path.display()))?;
        if m.schema_version == 0 {
            m.schema_version = SCHEMA_VERSION;
        }
        Ok(m)
    } else {
        Ok(SnapshotMetadata {
            snapshot_date: snapshot_date.to_string(),
            source_url: cfg.source_url.clone(),
            backup_id: cfg.backup_id,
            schema_version: SCHEMA_VERSION,
            ..Default::default()
        })
    }
}

pub fn write(path: &Path, meta: &SnapshotMetadata) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create {}", parent.display()))?;
    }
    let body = serde_json::to_vec_pretty(meta).context("serialize snapshot metadata")?;
    std::fs::write(path, body).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

/// Read-or-seed, mutate, and write back. Each phase calls this so the
/// metadata file stays coherent whether you ran `build` or invoked the
/// phases individually.
pub fn update<F>(cfg: &Config, snapshot_date: &str, mutate: F) -> Result<PathBuf>
where
    F: FnOnce(&mut SnapshotMetadata),
{
    let path = path_for(cfg, snapshot_date);
    let mut meta = read_or_seed(&path, cfg, snapshot_date)?;
    meta.snapshot_date = snapshot_date.to_string();
    meta.source_url = cfg.source_url.clone();
    meta.backup_id = cfg.backup_id.or(meta.backup_id);
    meta.schema_version = SCHEMA_VERSION;
    mutate(&mut meta);
    meta.generated_at = chrono::Utc::now().to_rfc3339();
    write(&path, &meta)?;
    Ok(path)
}
