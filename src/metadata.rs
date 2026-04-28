use anyhow::{Context, Result};
use serde::Serialize;
use std::path::Path;

#[derive(Debug, Serialize)]
pub struct SnapshotMetadata {
    pub snapshot_date: String,
    pub source_url: String,
    pub backup_id: Option<u64>,
    pub mirror_bytes: u64,
    pub stage_counts: Vec<(String, u64)>,
    pub hydrate_counts: Vec<(String, u64)>,
    pub orphan_like_rate: f64,
    pub orphan_repost_rate: f64,
    pub generated_at: String,
    pub schema_version: u32,
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
