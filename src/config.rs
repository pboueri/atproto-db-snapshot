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
    /// When set to N>1, chunked stages are run as N modulo-bucketed
    /// passes and the rows are concatenated. Hash tables shrink ~N×
    /// per pass at the cost of re-scanning the input parquet N times.
    /// None or Some(1) disables chunking. Stage v2 only chunks the
    /// aggregate stages (`actor_aggs` / `post_aggs`); entity tables
    /// are emitted directly by stage and don't need chunking.
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
    /// When set, `likes`, `reposts`, and `posts_from_*` are filtered
    /// at hydrate time to created_at in
    /// `[snapshot_date - days_back, snapshot_date - days_lag]`. Also
    /// drops orphan likes/reposts whose subject_uri_id doesn't match
    /// any post in the windowed `posts` table. `actors`, `blocks`,
    /// and `follows` are always loaded in full because they represent
    /// state, not events. Both bounds must be set together; if either
    /// is None the window is disabled and the full lifetime of every
    /// table is loaded (current behavior).
    #[serde(default)]
    pub hydrate_window_days_back: Option<u32>,
    #[serde(default)]
    pub hydrate_window_days_lag: Option<u32>,
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
            hydrate_chunk_buckets: None,
            hydrate_chunk_dry_run: None,
            hydrate_window_days_back: None,
            hydrate_window_days_lag: None,
        }
    }

    /// Resolve the [lo, hi] timestamp window for hydrate filtering,
    /// or None when no window is configured. Window endpoints are
    /// `snapshot_date - days_back` and `snapshot_date - days_lag`.
    /// Returned strings are SQL-friendly `YYYY-MM-DD HH:MM:SS`.
    pub fn hydrate_window(&self, snapshot_date: &str) -> Result<Option<(String, String)>> {
        let (Some(back), Some(lag)) = (
            self.hydrate_window_days_back,
            self.hydrate_window_days_lag,
        ) else {
            return Ok(None);
        };
        if lag >= back {
            return Err(anyhow!(
                "hydrate_window_days_lag ({lag}) must be less than \
                 hydrate_window_days_back ({back})"
            ));
        }
        let snap = chrono::NaiveDate::parse_from_str(snapshot_date, "%Y-%m-%d")
            .with_context(|| format!("parse snapshot_date {snapshot_date:?}"))?;
        let lo = snap - chrono::Duration::days(back as i64);
        let hi = snap - chrono::Duration::days(lag as i64);
        let fmt = "%Y-%m-%d %H:%M:%S";
        Ok(Some((
            lo.and_hms_opt(0, 0, 0)
                .expect("midnight is always valid")
                .format(fmt)
                .to_string(),
            hi.and_hms_opt(23, 59, 59)
                .expect("end-of-day is always valid")
                .format(fmt)
                .to_string(),
        )))
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
        // If we can't read the cgroup ceiling on Linux, sysinfo
        // reports host RAM (e.g. ~720 GiB on a Modal worker), which
        // is wildly above the container's actual memory.max. DuckDB
        // would then accept dirty pages past the cgroup limit and
        // either OOM-kill mid-write (leaving partially-flushed
        // blocks with stored_checksum=0 — the symptom seen on the
        // 2026-04-28 build) or thrash. Refuse to guess: cap at 32
        // GiB until the operator passes an explicit memory_limit.
        let target = if cfg!(target_os = "linux") && cgroup_bytes.is_none() {
            const LINUX_NO_CGROUP_FALLBACK: u64 = 32 * 1024 * 1024 * 1024;
            tracing::warn!(
                "cgroup memory limit unreadable on Linux; falling back to 32 GiB. \
                 Pass --memory-limit explicitly to override."
            );
            LINUX_NO_CGROUP_FALLBACK
        } else {
            ((total_bytes as f64 * 0.8) as u64).min(AUTO_MEMORY_LIMIT_CAP)
        };
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
