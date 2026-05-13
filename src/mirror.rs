//! Mirror constellation's rocksdb backup to a local directory.
//!
//! Thin wrapper around `eat_rocks::restore()`. As of eat-rocks v0.2,
//! the upstream restore is itself incremental — files already present
//! on disk that match the new backup's manifest (size + crc32c when
//! `verify`, else size alone) are skipped, and orphaned local files
//! are swept after CURRENT is committed. This module exists for
//! `.cursor` bookkeeping (per-run delta stats, last_full / last_incr
//! timestamps) and to resolve the target backup_id up front so the
//! cursor can record it.
//!
//! Cursor format (`.cursor`, JSON v2): backup_id, file_count,
//! bytes_on_disk, completed_at, last_full_at, last_incremental_at
//! plus per-run counters for added / skipped. Lets a daily refresh
//! log say "of 11,439 files, 87 changed; downloaded 12 MiB, skipped
//! 698 GiB" in one line.

use anyhow::{anyhow, Context, Result};
use eat_rocks::{RestoreOptions, TargetMode};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;

use crate::config::Config;

pub struct MirrorOutcome {
    pub rocks_dir: PathBuf,
    pub backup_id: Option<u64>,
    pub bytes_on_disk: u64,
    pub files_skipped: u64,
    pub files_downloaded: u64,
    pub bytes_downloaded: u64,
}

/// Persisted next to the rocks tree. Lets a later mirror invocation
/// (a) short-circuit when already at the target backup, and
/// (b) report meaningful delta stats. `last_full_at` is set only
/// on a truly cold-start mirror — every subsequent refresh updates
/// `last_incremental_at` but preserves `last_full_at`.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Cursor {
    #[serde(default)]
    pub schema_version: u32,
    pub source_url: String,
    #[serde(default)]
    pub backup_id: Option<u64>,
    #[serde(default)]
    pub bytes_on_disk: u64,
    #[serde(default)]
    pub file_count: u64,
    #[serde(default)]
    pub completed_at: String,
    #[serde(default)]
    pub last_full_at: Option<String>,
    #[serde(default)]
    pub last_incremental_at: Option<String>,
    #[serde(default)]
    pub last_added_files: u64,
    #[serde(default)]
    pub last_added_bytes: u64,
    #[serde(default)]
    pub last_skipped_files: u64,
    #[serde(default)]
    pub last_skipped_bytes: u64,
}

pub async fn run(cfg: &Config) -> Result<MirrorOutcome> {
    let rocks_dir = cfg.rocks_dir();
    std::fs::create_dir_all(&rocks_dir)
        .with_context(|| format!("create rocks dir {}", rocks_dir.display()))?;

    let store: Arc<dyn ObjectStore> = eat_rocks::public_bucket(&cfg.source_url)
        .with_context(|| format!("open object store for {}", &cfg.source_url))?;

    // Resolve "latest" upstream once so the cursor can record the id
    // we actually targeted. eat-rocks::restore resolves this
    // internally but doesn't surface the chosen id in RestoreOutcome.
    let target_id = match cfg.backup_id {
        Some(id) => id,
        None => {
            let ids = eat_rocks::list_backup_ids(&*store, "")
                .await
                .context("list_backup_ids")?;
            *ids.last().ok_or_else(|| anyhow!("upstream has no backups"))?
        }
    };

    let cursor_path = rocks_dir.join(".cursor");
    let existing_cursor: Option<Cursor> = std::fs::read(&cursor_path)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok());

    tracing::info!(
        source = %cfg.source_url,
        target = %rocks_dir.display(),
        target_backup_id = target_id,
        prior_backup_id = ?existing_cursor.as_ref().and_then(|c| c.backup_id),
        concurrency = cfg.mirror_concurrency,
        verify = cfg.mirror_verify,
        "incremental restore start"
    );

    let opts = RestoreOptions {
        backup_id: Some(target_id),
        concurrency: cfg.mirror_concurrency,
        verify: cfg.mirror_verify,
        wal_dir: None,
        always_download: false,
        // CreateOrReplace: we may be running against a cold dir
        // (cursor-less first-time mirror) or a warm one (daily
        // refresh). Either path is fine.
        target_mode: TargetMode::CreateOrReplace,
    };

    let outcome = eat_rocks::restore(store, "", &rocks_dir, opts)
        .await
        .context("eat_rocks::restore")?;

    let files_downloaded = (outcome.total_files - outcome.skipped_files) as u64;
    let bytes_downloaded = outcome.downloaded_bytes as u64;
    let files_skipped = outcome.skipped_files as u64;
    let bytes_skipped = outcome.skipped_bytes as u64;
    let bytes_on_disk = dir_size_bytes(&rocks_dir);
    let now = chrono::Utc::now().to_rfc3339();

    let new_cursor = Cursor {
        schema_version: 2,
        source_url: cfg.source_url.clone(),
        backup_id: Some(target_id),
        bytes_on_disk,
        file_count: outcome.total_files as u64,
        completed_at: now.clone(),
        last_full_at: match existing_cursor.as_ref().and_then(|c| c.last_full_at.clone()) {
            Some(prev) => Some(prev),
            None => Some(now.clone()),
        },
        last_incremental_at: Some(now),
        last_added_files: files_downloaded,
        last_added_bytes: bytes_downloaded,
        last_skipped_files: files_skipped,
        last_skipped_bytes: bytes_skipped,
    };
    std::fs::write(&cursor_path, serde_json::to_vec_pretty(&new_cursor)?)
        .with_context(|| format!("write cursor file {}", cursor_path.display()))?;

    tracing::info!(
        target_backup_id = target_id,
        replaced_existing = outcome.replaced_existing_db,
        downloaded_files = files_downloaded,
        downloaded_bytes = bytes_downloaded,
        skipped_files = files_skipped,
        skipped_bytes = bytes_skipped,
        orphans_deleted = outcome.orphans_deleted.len(),
        bytes_on_disk,
        elapsed_secs = format!("{:.1}", outcome.elapsed.as_secs_f64()),
        "incremental restore done"
    );

    Ok(MirrorOutcome {
        rocks_dir,
        backup_id: Some(target_id),
        bytes_on_disk,
        files_skipped,
        files_downloaded,
        bytes_downloaded,
    })
}

fn dir_size_bytes(path: &std::path::Path) -> u64 {
    let mut total: u64 = 0;
    if let Ok(rd) = std::fs::read_dir(path) {
        for entry in rd.flatten() {
            let p = entry.path();
            match entry.file_type() {
                Ok(ft) if ft.is_file() => {
                    if let Ok(meta) = entry.metadata() {
                        total += meta.len();
                    }
                }
                Ok(ft) if ft.is_dir() => {
                    total += dir_size_bytes(&p);
                }
                _ => {}
            }
        }
    }
    total
}
