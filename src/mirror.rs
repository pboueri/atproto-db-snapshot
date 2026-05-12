//! Mirror constellation's rocksdb backup to a local directory,
//! **incrementally** — files whose local size matches the target
//! backup's meta are skipped; only missing or size-changed files
//! are re-downloaded. RocksDB's BackupEngine names SSTs by content
//! (`shared_checksum/<file_no>_<crc>_<size>.sst`), so SSTs that
//! survived from one backup to the next reuse the exact same
//! upstream path — refreshes between adjacent backups typically
//! touch only a handful of new SSTs plus the MANIFEST / CURRENT.
//!
//! eat-rocks's public `restore()` does not de-duplicate against
//! existing local files; this module wraps its lower-level
//! `fetch_meta` / `list_backup_ids` primitives and runs its own
//! concurrent downloader so the skip logic can run. The path
//! unmangling (`shared_checksum/...` → DB filename) is replicated
//! verbatim from eat-rocks's `src/restore.rs:db_filename` since
//! that helper is `pub(crate)` upstream — when eat-rocks publishes
//! the helper (or accepts a skip_present option), this file
//! collapses back to a thin wrapper.
//!
//! Cursor format (`.cursor`, JSON v2): backup_id, file_count,
//! bytes_on_disk, completed_at, last_full_at, last_incremental_at
//! plus per-run counters for added / skipped. Lets a daily
//! refresh log say "of 11,439 files, 87 changed; downloaded
//! 12 MiB, skipped 698 GiB" in one line.

use anyhow::{anyhow, Context, Result};
use eat_rocks::BackupMeta;
use futures::StreamExt;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::AsyncWriteExt;

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

    // Resolve "latest" upstream once so the rest of the run has a
    // stable target. eat-rocks's list_backup_ids returns sorted; we
    // take the max.
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
        "incremental restore start"
    );

    let meta = eat_rocks::fetch_meta(&*store, "", target_id)
        .await
        .context("fetch_meta")?;
    let excluded = meta.files.iter().filter(|f| f.excluded).count();
    if excluded > 0 {
        return Err(anyhow!(
            "target backup {target_id} has {excluded} excluded files; \
             eat-rocks rejects these — pick a different backup"
        ));
    }
    let total_files = meta.files.len();

    let plan = plan_incremental(&rocks_dir, &meta)?;
    tracing::info!(
        target_backup_id = target_id,
        total_files,
        already_present = plan.skip.len(),
        to_download = plan.download.len(),
        skipped_bytes = plan.skipped_bytes,
        "incremental plan"
    );

    let bytes_downloaded =
        download_missing(store, plan.download.clone(), &rocks_dir, cfg.mirror_concurrency)
            .await?;

    // Atomic CURRENT rename. The downloader writes CURRENT to
    // `CURRENT.tmp` so a partial run can't leave behind a CURRENT
    // pointing at uncommitted state; we only rename once every
    // other download succeeded.
    let current_tmp = rocks_dir.join("CURRENT.tmp");
    if current_tmp.exists() {
        let final_current = rocks_dir.join("CURRENT");
        std::fs::rename(&current_tmp, &final_current)
            .with_context(|| format!("rename CURRENT.tmp -> CURRENT in {}", rocks_dir.display()))?;
    }

    // Orphan sweep: delete any local file in rocks_dir that the
    // new meta doesn't reference. Upstream compactions roll old
    // SSTs into new ones with different content-addressed names;
    // without this, every incremental run leaves the prior
    // backup's SSTs behind as ~hundreds of GB of dead weight that
    // RocksDB will never read (CURRENT now points elsewhere) but
    // that still bloats the volume and the rocks-→-stage copy.
    // .cursor is ours and stays. CURRENT was just renamed in.
    let cleanup_stats = sweep_orphans(&rocks_dir, &meta)?;
    tracing::info!(
        deleted_files = cleanup_stats.deleted_files,
        deleted_bytes = cleanup_stats.deleted_bytes,
        kept_files = cleanup_stats.kept_files,
        "orphan sweep done"
    );

    let bytes_on_disk = dir_size_bytes(&rocks_dir);
    let now = chrono::Utc::now().to_rfc3339();

    let new_cursor = Cursor {
        schema_version: 2,
        source_url: cfg.source_url.clone(),
        backup_id: Some(target_id),
        bytes_on_disk,
        file_count: total_files as u64,
        completed_at: now.clone(),
        last_full_at: match existing_cursor.as_ref().and_then(|c| c.last_full_at.clone()) {
            Some(prev) => Some(prev),
            None => Some(now.clone()),
        },
        last_incremental_at: Some(now),
        last_added_files: plan.download.len() as u64,
        last_added_bytes: bytes_downloaded,
        last_skipped_files: plan.skip.len() as u64,
        last_skipped_bytes: plan.skipped_bytes,
    };
    std::fs::write(&cursor_path, serde_json::to_vec_pretty(&new_cursor)?)
        .with_context(|| format!("write cursor file {}", cursor_path.display()))?;

    tracing::info!(
        target_backup_id = target_id,
        downloaded_files = plan.download.len(),
        downloaded_bytes = bytes_downloaded,
        skipped_files = plan.skip.len(),
        skipped_bytes = plan.skipped_bytes,
        bytes_on_disk,
        "incremental restore done"
    );

    Ok(MirrorOutcome {
        rocks_dir,
        backup_id: Some(target_id),
        bytes_on_disk,
        files_skipped: plan.skip.len() as u64,
        files_downloaded: plan.download.len() as u64,
        bytes_downloaded,
    })
}

/// What `plan_incremental` returns: the list of files to actually
/// fetch from upstream, plus accounting for what we kept.
struct Plan {
    download: Vec<DownloadJob>,
    skip: Vec<()>, // count only; we don't need to track skip details
    skipped_bytes: u64,
}

#[derive(Clone)]
struct DownloadJob {
    backup_path: String,
    dest: PathBuf,
    expected_size: Option<u64>,
    expected_crc32c: Option<u32>,
}

/// Walk the meta and decide, for each entry, whether the local
/// filesystem already has a matching file. Match criterion is
/// `local.size == expected_size`, where `expected_size` is
/// either taken from the meta's `size` field or — when missing,
/// which constellation's backup meta currently does — parsed from
/// the embedded size in the `shared_checksum/<no>_<crc>_<size>.sst`
/// filename. crc32c is not re-validated against on-disk bytes
/// because that'd cost a full ~650 GB read every run.
///
/// Adjacent-backup SSTs that share a shared_checksum name also
/// share a size *and* crc — RocksDB's content-addressed scheme
/// means filename collisions are content collisions. Size match
/// is a sufficient skip predicate.
fn plan_incremental(rocks_dir: &Path, meta: &BackupMeta) -> Result<Plan> {
    let mut plan = Plan {
        download: Vec::new(),
        skip: Vec::new(),
        skipped_bytes: 0,
    };
    for f in &meta.files {
        let dest = local_dest_for(rocks_dir, &f.path)?;
        let expected_size = f.size.or_else(|| size_from_shared_checksum_path(&f.path));
        if let Some(expected) = expected_size {
            if let Ok(meta_local) = std::fs::metadata(&dest) {
                if meta_local.is_file() && meta_local.len() == expected {
                    plan.skip.push(());
                    plan.skipped_bytes += expected;
                    continue;
                }
            }
        }
        plan.download.push(DownloadJob {
            backup_path: f.path.clone(),
            dest,
            expected_size,
            expected_crc32c: f.crc32c,
        });
    }
    Ok(plan)
}

/// Recover the SST size from a `shared_checksum/<no>_<crc>_<size>.sst`
/// path when the meta line itself doesn't carry `size <bytes>`.
/// Returns None for any other shape (CURRENT, MANIFEST, *.log,
/// shared/<name>) — those are small and we don't bother.
fn size_from_shared_checksum_path(backup_path: &str) -> Option<u64> {
    let sp = StorePath::from(backup_path);
    let mut parts = sp.parts();
    if parts.next().map(|p| p.as_ref().to_string()).as_deref() != Some("shared_checksum") {
        return None;
    }
    let mangled = parts.next()?.as_ref().to_string();
    let stem = Path::new(&mangled).file_stem()?.to_str()?;
    // stem looks like "000007_2894567812_590"
    let mut fields = stem.split('_');
    let _file_no = fields.next()?;
    let _crc = fields.next()?;
    let size_str = fields.next()?;
    if fields.next().is_some() {
        // unexpected extra underscore — bail rather than guess
        return None;
    }
    size_str.parse::<u64>().ok()
}

async fn download_missing(
    store: Arc<dyn ObjectStore>,
    jobs: Vec<DownloadJob>,
    rocks_dir: &Path,
    concurrency: usize,
) -> Result<u64> {
    if jobs.is_empty() {
        return Ok(0);
    }
    // Pre-create parent directories so worker tasks don't race on mkdir.
    let mut parents: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
    for j in &jobs {
        if let Some(parent) = j.dest.parent() {
            parents.insert(parent.to_path_buf());
        }
    }
    for p in parents {
        std::fs::create_dir_all(&p)
            .with_context(|| format!("create parent dir {}", p.display()))?;
    }

    let started = Instant::now();
    let total = jobs.len();
    let total_bytes = jobs
        .iter()
        .map(|j| j.expected_size.unwrap_or(0))
        .sum::<u64>();
    tracing::info!(
        total,
        total_bytes,
        concurrency,
        "downloading deltas"
    );

    let tasks = jobs.into_iter().map(|j| {
        let store = Arc::clone(&store);
        async move {
            let path = j.backup_path.clone();
            let outcome = download_one_with_retries(&*store, j, 4).await;
            (path, outcome)
        }
    });

    let mut stream = futures::stream::iter(tasks).buffer_unordered(concurrency);
    let mut completed = 0usize;
    let mut bytes_downloaded = 0u64;
    let mut failures: Vec<(String, anyhow::Error)> = Vec::new();
    while let Some((path, r)) = stream.next().await {
        completed += 1;
        match r {
            Ok(bytes) => bytes_downloaded += bytes,
            Err(e) => {
                tracing::warn!(path = %path, err = %e, "download failed; will continue with other files");
                failures.push((path, e));
            }
        }
        if completed.is_multiple_of(50) || completed == total {
            let elapsed = started.elapsed().as_secs_f64();
            let rate = if elapsed > 0.0 {
                bytes_downloaded as f64 / elapsed / 1e6
            } else {
                0.0
            };
            tracing::info!(
                completed,
                total,
                failures = failures.len(),
                bytes_downloaded,
                elapsed_secs = format!("{:.1}", elapsed),
                mb_per_sec = format!("{:.1}", rate),
                "delta progress"
            );
        }
    }
    let _ = rocks_dir;
    if !failures.is_empty() {
        for (path, e) in &failures {
            tracing::error!(path = %path, err = %e, "failed download (after retries)");
        }
        return Err(anyhow!(
            "{} file(s) failed to download after retries; re-run to pick up — \
             surviving files won't be re-fetched thanks to size-skip",
            failures.len()
        ));
    }
    Ok(bytes_downloaded)
}

/// Wrap `download_one` with a bounded retry loop. Each retry issues
/// a fresh `store.get(&key)` so a 416 from a stale Range-resume
/// inside object_store's internal retry doesn't poison subsequent
/// attempts. Exponential backoff (1s, 2s, 4s, 8s, capped) between
/// tries; total wall time on a fully-broken file ≈ 15s.
async fn download_one_with_retries(
    store: &dyn ObjectStore,
    job: DownloadJob,
    max_attempts: u32,
) -> Result<u64> {
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 0..max_attempts {
        if attempt > 0 {
            let backoff = 1u64 << attempt.min(4); // 2, 4, 8, 16 seconds
            tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
            tracing::warn!(
                path = %job.backup_path,
                attempt = attempt + 1,
                max = max_attempts,
                "retrying download after backoff"
            );
        }
        match download_one(store, &job).await {
            Ok(n) => return Ok(n),
            Err(e) => {
                last_err = Some(e);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow!("download retries exhausted with no error captured")))
}

async fn download_one(store: &dyn ObjectStore, job: &DownloadJob) -> Result<u64> {
    let key = StorePath::from(job.backup_path.as_str());
    let result = store
        .get(&key)
        .await
        .with_context(|| format!("GET {}", job.backup_path))?;
    let mut stream = result.into_stream();

    let f = tokio::fs::File::create(&job.dest)
        .await
        .with_context(|| format!("create {}", job.dest.display()))?;
    let mut out = tokio::io::BufWriter::new(f);

    let mut total = 0u64;
    let mut crc = 0u32;
    let verify_crc = job.expected_crc32c.is_some();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk
            .with_context(|| format!("stream chunk for {}", job.backup_path))?;
        total += chunk.len() as u64;
        if verify_crc {
            crc = crc32c::crc32c_append(crc, &chunk);
        }
        out.write_all(&chunk)
            .await
            .with_context(|| format!("write {}", job.dest.display()))?;
    }
    out.shutdown()
        .await
        .with_context(|| format!("flush {}", job.dest.display()))?;

    if let Some(expected) = job.expected_size {
        if total != expected {
            return Err(anyhow!(
                "size mismatch for {}: got {total} expected {expected}",
                job.backup_path
            ));
        }
    }
    if let Some(expected) = job.expected_crc32c {
        if crc != expected {
            return Err(anyhow!(
                "crc32c mismatch for {}: got {crc} expected {expected}",
                job.backup_path
            ));
        }
    }
    Ok(total)
}

/// Map a backup-relative path to where it should land in `rocks_dir`.
/// Mirrors the dispatch in eat-rocks's private `download_file`:
///
/// - `CURRENT` → `rocks_dir/CURRENT.tmp` (atomic-renamed at end)
/// - `*.log`   → `rocks_dir/<name>` (we don't split WAL into a
///   separate dir; matches default RestoreOptions { wal_dir: None })
/// - other     → `rocks_dir/<unmangled-name>` via `db_filename`
fn local_dest_for(rocks_dir: &Path, backup_path: &str) -> Result<PathBuf> {
    let name = db_filename(backup_path)?;
    if name.as_os_str() == "CURRENT" {
        return Ok(rocks_dir.join("CURRENT.tmp"));
    }
    Ok(rocks_dir.join(&name))
}

/// Convert a backup-relative path to the DB filename it restores
/// as. Vendored from eat-rocks (`src/restore.rs:db_filename`) which
/// keeps the helper `pub(crate)`; the logic is stable across the
/// repo's history. Drop this when upstream publishes the helper.
fn db_filename(backup_path: &str) -> Result<PathBuf> {
    let sp = StorePath::from(backup_path);
    let parts: Vec<_> = sp.parts().collect();
    match parts.first().map(|p| p.as_ref()) {
        Some("shared_checksum") => {
            let filename = parts
                .last()
                .ok_or_else(|| anyhow!("shared_checksum without filename: {backup_path}"))?;
            unmangle_shared_checksum(filename.as_ref())
        }
        Some("private") => parts
            .get(2)
            .map(|p| PathBuf::from(p.as_ref()))
            .ok_or_else(|| anyhow!("private path too short: {backup_path}")),
        Some("shared") => parts
            .last()
            .map(|p| PathBuf::from(p.as_ref()))
            .ok_or_else(|| anyhow!("shared without filename: {backup_path}")),
        _ => Err(anyhow!("unrecognized backup path prefix: {backup_path}")),
    }
}

/// `000007_2894567812_590.sst` -> `000007.sst`. Vendored.
fn unmangle_shared_checksum(mangled: &str) -> Result<PathBuf> {
    let p = Path::new(mangled);
    let ext = p
        .extension()
        .and_then(|e| e.to_str())
        .ok_or_else(|| anyhow!("shared_checksum no extension: {mangled}"))?;
    let stem = p
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow!("shared_checksum no stem: {mangled}"))?;
    let underscore = stem
        .find('_')
        .ok_or_else(|| anyhow!("shared_checksum no underscore: {mangled}"))?;
    Ok(PathBuf::from(format!("{}.{ext}", &stem[..underscore])))
}

struct SweepStats {
    deleted_files: u64,
    deleted_bytes: u64,
    kept_files: u64,
}

/// Delete files in `rocks_dir` that aren't referenced by the new
/// meta. Anything the meta lists is expected at its
/// `local_dest_for` path; CURRENT is always preserved (just
/// renamed from CURRENT.tmp); .cursor is our own bookkeeping.
/// Subdirectories are not walked — the upstream backup layout is
/// flat at the destination after `db_filename` unmangling.
fn sweep_orphans(rocks_dir: &Path, meta: &BackupMeta) -> Result<SweepStats> {
    let mut keep: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
    for f in &meta.files {
        let dest = local_dest_for(rocks_dir, &f.path)?;
        // CURRENT lives in `CURRENT.tmp` during download and was
        // renamed to `CURRENT` just before this sweep — keep the
        // post-rename name.
        if dest.file_name().map(|n| n == "CURRENT.tmp").unwrap_or(false) {
            keep.insert(rocks_dir.join("CURRENT"));
        } else {
            keep.insert(dest);
        }
    }
    keep.insert(rocks_dir.join(".cursor"));

    let mut stats = SweepStats {
        deleted_files: 0,
        deleted_bytes: 0,
        kept_files: 0,
    };
    for entry in std::fs::read_dir(rocks_dir)
        .with_context(|| format!("read {}", rocks_dir.display()))?
    {
        let entry = entry?;
        let ft = entry.file_type()?;
        if !ft.is_file() {
            continue;
        }
        let p = entry.path();
        if keep.contains(&p) {
            stats.kept_files += 1;
            continue;
        }
        let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
        std::fs::remove_file(&p)
            .with_context(|| format!("remove orphan {}", p.display()))?;
        stats.deleted_files += 1;
        stats.deleted_bytes += size;
    }
    Ok(stats)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn db_filename_shared_checksum() {
        assert_eq!(
            db_filename("shared_checksum/000007_2894567812_590.sst").unwrap(),
            PathBuf::from("000007.sst")
        );
    }

    #[test]
    fn db_filename_private() {
        assert_eq!(
            db_filename("private/1/MANIFEST-000008").unwrap(),
            PathBuf::from("MANIFEST-000008")
        );
    }

    #[test]
    fn db_filename_shared() {
        assert_eq!(
            db_filename("shared/000007.sst").unwrap(),
            PathBuf::from("000007.sst")
        );
    }

    #[test]
    fn size_from_shared_checksum_extracts_trailing_size() {
        assert_eq!(
            size_from_shared_checksum_path("shared_checksum/000007_2894567812_590.sst"),
            Some(590),
        );
    }

    #[test]
    fn size_from_shared_checksum_rejects_other_shapes() {
        assert_eq!(size_from_shared_checksum_path("CURRENT"), None);
        assert_eq!(size_from_shared_checksum_path("private/1/MANIFEST-000008"), None);
        assert_eq!(size_from_shared_checksum_path("shared/000007.sst"), None);
    }
}
