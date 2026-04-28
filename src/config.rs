use anyhow::{Context, Result};
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
    #[serde(default)]
    pub object_store: Option<String>,
    #[serde(default = "default_disk_cap_bytes")]
    pub disk_cap_bytes: u64,
    #[serde(default = "default_mirror_concurrency")]
    pub mirror_concurrency: usize,
    #[serde(default)]
    pub backup_id: Option<u64>,
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

fn default_disk_cap_bytes() -> u64 {
    150 * 1024 * 1024 * 1024
}

fn default_mirror_concurrency() -> usize {
    32
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
            object_store: None,
            disk_cap_bytes: default_disk_cap_bytes(),
            mirror_concurrency: default_mirror_concurrency(),
            backup_id: None,
        }
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
