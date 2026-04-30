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
    /// When set, restrict high-volume collections (likes, reposts, posts
    /// and post_media derived from them) to records whose created_at is
    /// within `hydrate_window_days` of the snapshot date. Follows and
    /// blocks are always materialized in full so the social graph stays
    /// complete. None = no windowing.
    #[serde(default)]
    pub hydrate_window_days: Option<u32>,
    /// When set to N>1, chunked stages are run as N modulo-bucketed
    /// passes and the rows are concatenated. Hash tables shrink ~N×
    /// per pass at the cost of re-scanning the input parquet N times.
    /// None or Some(1) disables chunking.
    #[serde(default)]
    pub hydrate_chunk_buckets: Option<u32>,
    /// Dry-run sanity check: when set, chunked stages execute only the
    /// first chunk (k=0 of `hydrate_chunk_buckets`) instead of all N.
    /// The resulting snapshot covers ~1/N of the data — useful to
    /// validate SQL changes end-to-end on Modal in ~1/N the time
    /// before committing to a full run. Non-chunked stages still run
    /// in full but operate on the partial inputs. None = full run.
    #[serde(default)]
    pub hydrate_chunk_dry_run: Option<bool>,
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

/// Read the cgroup memory ceiling, returning bytes if available.
/// Tries cgroup v2 (`/sys/fs/cgroup/memory.max`) first, then v1
/// (`/sys/fs/cgroup/memory/memory.limit_in_bytes`). Returns None on
/// non-Linux, when the file isn't readable, when the value is "max"
/// (v2's no-limit sentinel), or when the value is implausibly large
/// (v1 reports a huge number to mean "no limit").
fn read_cgroup_memory_max() -> Option<u64> {
    const V1_NO_LIMIT_SENTINEL: u64 = u64::MAX / 4096 * 4096;
    let candidates = [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ];
    for path in candidates {
        let Ok(s) = std::fs::read_to_string(path) else {
            continue;
        };
        let s = s.trim();
        if s == "max" {
            return None;
        }
        if let Ok(n) = s.parse::<u64>() {
            if n >= V1_NO_LIMIT_SENTINEL {
                return None;
            }
            return Some(n);
        }
    }
    None
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
            hydrate_window_days: None,
            hydrate_chunk_buckets: None,
            hydrate_chunk_dry_run: None,
        }
    }

    pub fn rocks_block_cache_bytes(&self) -> Result<usize> {
        parse_size(&self.rocks_block_cache)
    }

    /// Resolve `memory_limit` to a concrete size string DuckDB will
    /// accept. The literal "auto" (case-insensitive) is replaced with
    /// `min(80% of cgroup-or-sysinfo total, AUTO_MEMORY_LIMIT_CAP)`.
    ///
    /// On Modal, sysinfo's `total_memory()` reports host RAM, not the
    /// container's cgroup limit, so we also probe `/sys/fs/cgroup/...`
    /// directly and take the smaller of the two. The hard cap keeps a
    /// runaway DuckDB process from monopolizing the worker; pick a
    /// value with headroom for buffer-pool buildup across stages
    /// (chunked builds keep prior-chunk pages cached, and a tight cap
    /// can SIGSEGV a later chunk that needs a fresh hash table).
    /// Override by setting `memory_limit = "<size>"` explicitly.
    pub fn resolved_memory_limit(&self) -> Result<String> {
        if !self.memory_limit.eq_ignore_ascii_case("auto") {
            return Ok(self.memory_limit.clone());
        }
        let mut sys = sysinfo::System::new();
        sys.refresh_memory();
        let sysinfo_bytes = sys.total_memory();
        if sysinfo_bytes == 0 {
            return Err(anyhow!(
                "memory_limit=auto but sysinfo reported 0 total memory"
            ));
        }
        let cgroup_bytes = read_cgroup_memory_max();
        let total_bytes = match cgroup_bytes {
            Some(c) if c < sysinfo_bytes => c,
            _ => sysinfo_bytes,
        };
        tracing::info!(
            sysinfo_total_mib = sysinfo_bytes / (1024 * 1024),
            cgroup_max_mib = cgroup_bytes.map(|c| c / (1024 * 1024)),
            chosen_total_mib = total_bytes / (1024 * 1024),
            "auto memory probe"
        );
        const AUTO_MEMORY_LIMIT_CAP: u64 = 128 * 1024 * 1024 * 1024;
        let target = ((total_bytes as f64 * 0.8) as u64).min(AUTO_MEMORY_LIMIT_CAP);
        let mib = (target / (1024 * 1024)).max(1);
        Ok(format!("{mib}MiB"))
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
