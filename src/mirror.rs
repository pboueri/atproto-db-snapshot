use anyhow::{Context, Result};
use std::path::PathBuf;
use std::sync::Arc;

use crate::config::Config;

pub struct MirrorOutcome {
    pub rocks_dir: PathBuf,
    pub backup_id: Option<u64>,
    pub bytes_on_disk: u64,
}

pub async fn run(cfg: &Config) -> Result<MirrorOutcome> {
    let rocks_dir = cfg.rocks_dir();
    std::fs::create_dir_all(&rocks_dir)
        .with_context(|| format!("create rocks dir {}", rocks_dir.display()))?;

    if existing_db_looks_complete(&rocks_dir) {
        let bytes = crate::disk::dir_size_bytes(&rocks_dir);
        tracing::info!(
            rocks_dir = %rocks_dir.display(),
            bytes,
            "rocks mirror already populated; skipping download"
        );
        return Ok(MirrorOutcome {
            rocks_dir,
            backup_id: cfg.backup_id,
            bytes_on_disk: bytes,
        });
    }

    if let Some(free) = crate::disk::free_bytes_for(&rocks_dir) {
        tracing::info!(
            free_bytes = free,
            cap_bytes = cfg.disk_cap_bytes,
            "starting mirror"
        );
        if free < cfg.disk_cap_bytes / 2 {
            tracing::warn!(
                free_bytes = free,
                "free disk is less than half the configured cap; mirror may abort"
            );
        }
    }

    let store: Arc<dyn object_store::ObjectStore> =
        eat_rocks::public_bucket(&cfg.source_url)
            .with_context(|| format!("open object store for {}", &cfg.source_url))?;

    let opts = eat_rocks::RestoreOptions {
        backup_id: cfg.backup_id,
        concurrency: cfg.mirror_concurrency,
        verify: true,
        wal_dir: None,
    };

    tracing::info!(
        source = %cfg.source_url,
        target = %rocks_dir.display(),
        backup_id = ?cfg.backup_id,
        concurrency = cfg.mirror_concurrency,
        "calling eat_rocks::restore"
    );
    eat_rocks::restore(store, "", rocks_dir.clone(), opts)
        .await
        .context("eat_rocks::restore failed")?;

    let bytes = crate::disk::dir_size_bytes(&rocks_dir);
    let cursor_path = rocks_dir.join(".cursor");
    let body = serde_json::json!({
        "source_url": cfg.source_url,
        "backup_id": cfg.backup_id,
        "bytes_on_disk": bytes,
        "completed_at": chrono::Utc::now().to_rfc3339(),
    });
    std::fs::write(&cursor_path, serde_json::to_vec_pretty(&body)?)
        .with_context(|| format!("write cursor file {}", cursor_path.display()))?;

    tracing::info!(bytes, "mirror complete");
    Ok(MirrorOutcome {
        rocks_dir,
        backup_id: cfg.backup_id,
        bytes_on_disk: bytes,
    })
}

fn existing_db_looks_complete(path: &std::path::Path) -> bool {
    let current = path.join("CURRENT");
    let cursor = path.join(".cursor");
    current.exists() && cursor.exists()
}
