//! Object-store upload. Generic over `object_store::ObjectStore`, with a
//! concrete R2 backend. Secrets come from env (`R2_ACCESS_KEY_ID`,
//! `R2_SECRET_ACCESS_KEY`); everything else from the config file.

use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{MultipartUpload, ObjectStore, ObjectStoreExt, PutPayload};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncReadExt;

use crate::config::{Config, UploadConfig};

const MULTIPART_THRESHOLD: u64 = 16 * 1024 * 1024;
const PART_SIZE: usize = 8 * 1024 * 1024;

pub struct UploadOutcome {
    pub kind: String,
    pub bucket: String,
    pub prefix: String,
    pub files: u64,
    pub bytes: u64,
}

pub async fn run(cfg: &Config, snapshot_date: &str) -> Result<UploadOutcome> {
    let upload_cfg = cfg
        .upload
        .as_ref()
        .context("no [upload] block in config; nothing to upload")?;
    let store = build_store(upload_cfg)?;
    let prefix = upload_cfg.prefix.trim_matches('/').to_string();

    let raw_dir = cfg.raw_dir(snapshot_date);
    let snap_dir = cfg.snapshot_dir(snapshot_date);

    let include: Vec<String> = upload_cfg
        .include
        .iter()
        .map(|s| s.to_lowercase())
        .collect();
    let mut sources: Vec<(PathBuf, String)> = Vec::new();
    if include.iter().any(|s| s == "raw") {
        sources.push((raw_dir, format!("raw/{}", snapshot_date)));
    }
    if include.iter().any(|s| s == "snapshot") {
        sources.push((snap_dir, format!("snapshot/{}", snapshot_date)));
    }

    let mut total_files = 0u64;
    let mut total_bytes = 0u64;
    for (local, sub) in sources {
        if !local.exists() {
            tracing::warn!(path = %local.display(), "missing upload source; skipping");
            continue;
        }
        let remote = if prefix.is_empty() {
            sub
        } else {
            format!("{}/{}", prefix, sub)
        };
        let (f, b) = upload_dir(store.as_ref(), &local, &remote, upload_cfg.concurrency).await?;
        tracing::info!(local = %local.display(), remote, files = f, bytes = b, "uploaded directory");
        total_files += f;
        total_bytes += b;
    }

    Ok(UploadOutcome {
        kind: upload_cfg.kind.clone(),
        bucket: upload_cfg.bucket.clone(),
        prefix,
        files: total_files,
        bytes: total_bytes,
    })
}

fn build_store(cfg: &UploadConfig) -> Result<Arc<dyn ObjectStore>> {
    match cfg.kind.to_lowercase().as_str() {
        "r2" => r2::build(cfg),
        other => anyhow::bail!(
            "unsupported upload.kind {other:?}; supported: \"r2\""
        ),
    }
}

mod r2 {
    use super::*;

    pub fn build(cfg: &UploadConfig) -> Result<Arc<dyn ObjectStore>> {
        let access_key = std::env::var("R2_ACCESS_KEY_ID")
            .context("R2_ACCESS_KEY_ID env var required for R2 upload")?;
        let secret_key = std::env::var("R2_SECRET_ACCESS_KEY")
            .context("R2_SECRET_ACCESS_KEY env var required for R2 upload")?;
        let endpoint = match (cfg.endpoint.clone(), cfg.account_id.clone()) {
            (Some(ep), _) => ep,
            (None, Some(id)) => format!("https://{id}.r2.cloudflarestorage.com"),
            (None, None) => anyhow::bail!(
                "upload.endpoint or upload.account_id required for R2"
            ),
        };
        let region = cfg.region.clone().unwrap_or_else(|| "auto".to_string());
        let store = AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_region(region)
            .with_bucket_name(&cfg.bucket)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .build()
            .context("build R2 (S3-compatible) object store")?;
        Ok(Arc::new(store))
    }
}

async fn upload_dir(
    store: &dyn ObjectStore,
    local: &Path,
    remote_prefix: &str,
    concurrency: usize,
) -> Result<(u64, u64)> {
    let files = collect_files(local)?;
    let total = files.len();
    let remote_prefix = remote_prefix.trim_end_matches('/').to_string();

    tracing::info!(
        local = %local.display(),
        remote_prefix,
        files = total,
        concurrency,
        "starting directory upload"
    );

    let results = stream::iter(files.into_iter().enumerate())
        .map(|(idx, abs_path)| {
            let remote_prefix = remote_prefix.clone();
            let local = local.to_path_buf();
            async move {
                let rel = abs_path
                    .strip_prefix(&local)
                    .with_context(|| {
                        format!(
                            "strip_prefix {} from {}",
                            local.display(),
                            abs_path.display()
                        )
                    })?
                    .to_string_lossy()
                    .replace('\\', "/");
                let key = format!("{remote_prefix}/{rel}");
                let n = upload_file(store, &abs_path, ObjectPath::from(key)).await?;
                if (idx + 1) % 50 == 0 {
                    tracing::info!(uploaded = idx + 1, total, "directory upload progress");
                }
                anyhow::Ok(n)
            }
        })
        .buffer_unordered(concurrency.max(1))
        .collect::<Vec<_>>()
        .await;

    let mut bytes = 0u64;
    let mut files = 0u64;
    for r in results {
        let n = r?;
        bytes += n;
        files += 1;
    }
    Ok((files, bytes))
}

fn collect_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        let rd = std::fs::read_dir(&d).with_context(|| format!("read dir {}", d.display()))?;
        for entry in rd {
            let entry = entry.with_context(|| format!("read entry under {}", d.display()))?;
            let p = entry.path();
            let ft = entry.file_type()?;
            if ft.is_dir() {
                stack.push(p);
            } else if ft.is_file() {
                out.push(p);
            }
        }
    }
    out.sort();
    Ok(out)
}

async fn upload_file(store: &dyn ObjectStore, path: &Path, key: ObjectPath) -> Result<u64> {
    let meta = tokio::fs::metadata(path)
        .await
        .with_context(|| format!("stat {}", path.display()))?;
    let size = meta.len();

    if size <= MULTIPART_THRESHOLD {
        let body = tokio::fs::read(path)
            .await
            .with_context(|| format!("read {}", path.display()))?;
        store
            .put(&key, PutPayload::from(body))
            .await
            .with_context(|| format!("put {} -> {}", path.display(), key))?;
    } else {
        let mut upload = store
            .put_multipart(&key)
            .await
            .with_context(|| format!("init multipart {} -> {}", path.display(), key))?;
        let mut f = tokio::fs::File::open(path)
            .await
            .with_context(|| format!("open {}", path.display()))?;
        let mut buf = vec![0u8; PART_SIZE];
        loop {
            let mut filled = 0;
            while filled < buf.len() {
                let n = f.read(&mut buf[filled..]).await?;
                if n == 0 {
                    break;
                }
                filled += n;
            }
            if filled == 0 {
                break;
            }
            let payload = PutPayload::from(buf[..filled].to_vec());
            upload
                .put_part(payload)
                .await
                .with_context(|| format!("put_part for {}", key))?;
        }
        upload
            .complete()
            .await
            .with_context(|| format!("complete multipart {}", key))?;
    }
    Ok(size)
}
