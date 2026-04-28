use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "default_source_url")]
    pub source_url: String,
    #[serde(default = "default_work_dir")]
    pub work_dir: PathBuf,
    #[serde(default)]
    pub snapshot_date: Option<String>,
    #[serde(default = "default_memory_limit")]
    pub memory_limit: String,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_mirror_concurrency")]
    pub mirror_concurrency: usize,
    #[serde(default)]
    pub backup_id: Option<u64>,
    #[serde(default)]
    pub upload: Option<UploadConfig>,
    #[serde(default = "default_rocks_block_cache")]
    pub rocks_block_cache: String,
    #[serde(default = "default_stage_threads")]
    pub stage_threads: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadConfig {
    /// Object store kind. Currently supported: "r2".
    pub kind: String,
    /// Destination bucket name.
    pub bucket: String,
    /// Optional path prefix inside the bucket (no leading/trailing slash).
    #[serde(default)]
    pub prefix: String,
    /// Explicit endpoint URL. Takes precedence over `account_id`.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// R2 account id; the endpoint is derived as
    /// `https://<account_id>.r2.cloudflarestorage.com` when `endpoint` is unset.
    #[serde(default)]
    pub account_id: Option<String>,
    /// Region label. Defaults to "auto" for R2.
    #[serde(default)]
    pub region: Option<String>,
    /// Number of files uploaded in parallel.
    #[serde(default = "default_upload_concurrency")]
    pub concurrency: usize,
    /// What to upload. Defaults to ["raw", "snapshot"].
    #[serde(default = "default_upload_include")]
    pub include: Vec<String>,
}

fn default_source_url() -> String {
    "https://constellation.t3.storage.dev".to_string()
}

fn default_work_dir() -> PathBuf {
    PathBuf::from("./var")
}

fn default_memory_limit() -> String {
    "4GiB".to_string()
}

fn default_batch_size() -> usize {
    100_000
}

fn default_mirror_concurrency() -> usize {
    32
}

fn default_upload_concurrency() -> usize {
    8
}

fn default_upload_include() -> Vec<String> {
    vec!["raw".to_string(), "snapshot".to_string()]
}

fn default_rocks_block_cache() -> String {
    "1GiB".to_string()
}

fn default_stage_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

pub fn parse_size(s: &str) -> Result<usize> {
    let s = s.trim();
    let split_at = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());
    let (num, suffix) = s.split_at(split_at);
    let n: f64 = num
        .trim()
        .parse()
        .with_context(|| format!("parse size number from {s:?}"))?;
    let mult: f64 = match suffix.trim().to_ascii_uppercase().as_str() {
        "" | "B" => 1.0,
        "K" | "KB" | "KIB" => 1024.0,
        "M" | "MB" | "MIB" => 1024.0 * 1024.0,
        "G" | "GB" | "GIB" => 1024.0 * 1024.0 * 1024.0,
        "T" | "TB" | "TIB" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        other => return Err(anyhow!("unknown size suffix in {s:?}: {other:?}")),
    };
    Ok((n * mult) as usize)
}

impl Config {
    pub fn from_toml_file(path: &std::path::Path) -> Result<Self> {
        let body = std::fs::read_to_string(path)
            .with_context(|| format!("read config file {}", path.display()))?;
        toml::from_str(&body).with_context(|| format!("parse config file {}", path.display()))
    }

    pub fn defaults() -> Self {
        Config {
            source_url: default_source_url(),
            work_dir: default_work_dir(),
            snapshot_date: None,
            memory_limit: default_memory_limit(),
            batch_size: default_batch_size(),
            mirror_concurrency: default_mirror_concurrency(),
            backup_id: None,
            upload: None,
            rocks_block_cache: default_rocks_block_cache(),
            stage_threads: default_stage_threads(),
        }
    }

    pub fn rocks_block_cache_bytes(&self) -> Result<usize> {
        parse_size(&self.rocks_block_cache)
    }

    pub fn rocks_dir(&self) -> PathBuf {
        self.work_dir.join("rocks")
    }

    pub fn raw_dir(&self, date: &str) -> PathBuf {
        self.work_dir.join("raw").join(date)
    }

    pub fn snapshot_dir(&self, date: &str) -> PathBuf {
        self.work_dir.join("snapshot").join(date)
    }
}
