//! PLC directory export ingestion phase.
//!
//! Streams the full PLC operation log (`{plc_export_url}/export`, JSONL,
//! ordered by `createdAt` ascending) and captures two op kinds per DID:
//!
//!   - genesis   (`operation.prev` is null)            -> kind="create"
//!   - tombstone (`operation.type` == "plc_tombstone")  -> kind="tombstone"
//!
//! and writes them as sharded parquet to `raw/<date>/plc/part-NNNNN.parquet`.
//! The hydrate stage `07_enrich_actors_created.sql` reads these shards to add
//! `created_at` / `tombstoned_at` to `actors` (and to insert PLC-only DIDs).
//!
//! Checkpointing mirrors `mirror.rs`: a `plc/.cursor` JSON records the last
//! `after` cursor, shard count, and row totals. A killed run resumes from the
//! last flushed shard (at most one shard's worth of pages is re-fetched). The
//! load-side `GROUP BY did` in SQL absorbs rare ms-tie boundary dupes.

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::Duration;

use crate::config::Config;
use crate::writers::plc::PlcOpWriter;

/// Emit a shard every this many captured rows. Bounds the work a resume
/// has to re-fetch and keeps per-shard memory modest.
const FLUSH_EVERY: u64 = 1_000_000;
const CURSOR_SCHEMA_VERSION: u32 = 1;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct PlcCursor {
    #[serde(default)]
    pub schema_version: u32,
    /// Raw `createdAt` string of the last op processed; passed back as
    /// the `after` query param. None on a cold start.
    #[serde(default)]
    pub after: Option<String>,
    #[serde(default)]
    pub shards: u32,
    #[serde(default)]
    pub rows: u64,
    #[serde(default)]
    pub pages: u64,
    #[serde(default)]
    pub completed: bool,
    #[serde(default)]
    pub updated_at: String,
}

pub struct PlcOutcome {
    pub rows: u64,
    pub shards: u32,
    pub completed: bool,
}

#[derive(Deserialize)]
struct ExportLine {
    did: String,
    #[serde(rename = "createdAt")]
    created_at: String,
    operation: OpInner,
}

#[derive(Deserialize)]
struct OpInner {
    /// Null for the genesis op, a CID string otherwise.
    #[serde(default)]
    prev: Option<String>,
    #[serde(rename = "type", default)]
    op_type: Option<String>,
    /// Legacy `create` op: PDS endpoint as a bare string.
    #[serde(default)]
    service: Option<String>,
    /// Legacy `create` op: handle.
    #[serde(default)]
    handle: Option<String>,
    /// Modern `plc_operation`: nested service map.
    #[serde(default)]
    services: Option<Services>,
    /// Modern `plc_operation`: `["at://<handle>"]`.
    #[serde(rename = "alsoKnownAs", default)]
    also_known_as: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct Services {
    #[serde(default)]
    atproto_pds: Option<ServiceEndpoint>,
}

#[derive(Deserialize)]
struct ServiceEndpoint {
    #[serde(default)]
    endpoint: Option<String>,
}

/// `create` (genesis), `tombstone` (deactivation), or `update`
/// (migration/rotation — carries the then-current PDS).
fn kind_of(op: &OpInner) -> &'static str {
    if op.op_type.as_deref() == Some("plc_tombstone") {
        "tombstone"
    } else if op.prev.is_none() {
        "create"
    } else {
        "update"
    }
}

/// PDS endpoint at this op: legacy `service`, else modern
/// `services.atproto_pds.endpoint`. None for tombstones.
fn pds_of(op: &OpInner) -> Option<String> {
    if let Some(s) = &op.service {
        return Some(s.clone());
    }
    op.services
        .as_ref()
        .and_then(|s| s.atproto_pds.as_ref())
        .and_then(|p| p.endpoint.clone())
}

/// Handle at this op: legacy `handle`, else first `alsoKnownAs` with the
/// `at://` prefix stripped.
fn handle_of(op: &OpInner) -> Option<String> {
    if let Some(h) = &op.handle {
        return Some(h.clone());
    }
    op.also_known_as
        .as_ref()
        .and_then(|v| v.first())
        .map(|h| h.strip_prefix("at://").unwrap_or(h).to_string())
}

fn load_cursor(path: &Path) -> Option<PlcCursor> {
    std::fs::read(path)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
}

fn save_cursor(path: &Path, cur: &PlcCursor) -> Result<()> {
    let tmp = path.with_extension("cursor.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(cur)?)
        .with_context(|| format!("write plc cursor {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename plc cursor -> {}", path.display()))?;
    Ok(())
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339()
}

fn ts_ms(created_at: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(created_at)
        .ok()
        .map(|d| d.timestamp_millis())
}

fn shard_path(plc_dir: &Path, idx: u32) -> std::path::PathBuf {
    plc_dir.join(format!("part-{idx:05}.parquet"))
}

pub async fn run(cfg: &Config, snapshot_date: &str) -> Result<PlcOutcome> {
    let plc_dir = cfg.raw_dir(snapshot_date).join("plc");
    std::fs::create_dir_all(&plc_dir)
        .with_context(|| format!("create plc dir {}", plc_dir.display()))?;
    let cursor_path = plc_dir.join(".cursor");
    let mut cur = load_cursor(&cursor_path).unwrap_or_default();

    // Skip path: still emit one schema-correct empty shard so the
    // hydrate glob resolves and `actors.created_at` exists (all NULL).
    if cfg.skip_plc {
        if cur.shards == 0 {
            let (path, _) = PlcOpWriter::create(shard_path(&plc_dir, 0), cfg.batch_size)?.finish()?;
            tracing::info!(shard = %path.display(), "skip_plc: wrote empty plc shard");
            cur = PlcCursor {
                schema_version: CURSOR_SCHEMA_VERSION,
                shards: 1,
                completed: true,
                updated_at: now_rfc3339(),
                ..Default::default()
            };
            save_cursor(&cursor_path, &cur)?;
        }
        tracing::info!("skip_plc set; not fetching PLC export");
        return Ok(PlcOutcome { rows: cur.rows, shards: cur.shards, completed: true });
    }

    if cur.completed {
        tracing::info!(
            shards = cur.shards,
            rows = cur.rows,
            "plc export already complete; skipping"
        );
        return Ok(PlcOutcome { rows: cur.rows, shards: cur.shards, completed: true });
    }

    let export_url = format!("{}/export", cfg.plc_export_url.trim_end_matches('/'));
    let page_size = cfg.plc_page_size;
    tracing::info!(
        url = %export_url,
        page_size,
        resume_after = ?cur.after,
        shards = cur.shards,
        rows = cur.rows,
        "plc export fetch start"
    );

    let client = reqwest::Client::builder()
        .user_agent("atproto-db-snapshot/plc")
        .timeout(Duration::from_secs(90))
        .build()
        .context("build reqwest client")?;

    let mut writer: Option<PlcOpWriter> = None;
    let mut rows_since_flush: u64 = 0;
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(60);

    loop {
        // Build the request with the current cursor.
        let mut req = client.get(&export_url).query(&[("count", page_size.to_string())]);
        if let Some(after) = &cur.after {
            req = req.query(&[("after", after)]);
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, backoff_s = backoff.as_secs(), "plc request error; backing off");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
        };
        let status = resp.status();
        if status.as_u16() == 429 || status.is_server_error() {
            tracing::warn!(status = status.as_u16(), backoff_s = backoff.as_secs(), "plc throttled/5xx; backing off");
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(max_backoff);
            continue;
        }
        if !status.is_success() {
            return Err(anyhow!("plc export unexpected status {}", status));
        }
        backoff = Duration::from_secs(1);

        let body = resp.text().await.context("read plc response body")?;
        let lines: Vec<&str> = body.lines().filter(|l| !l.trim().is_empty()).collect();
        if lines.is_empty() {
            // Reached the tail.
            if let Some(w) = writer.take() {
                let (_, _) = w.finish()?;
                cur.shards += 1;
            }
            cur.completed = true;
            cur.updated_at = now_rfc3339();
            save_cursor(&cursor_path, &cur)?;
            break;
        }

        for line in &lines {
            let parsed: ExportLine = match serde_json::from_str(line) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(error = %e, "skipping unparseable plc line");
                    continue;
                }
            };
            let kind = kind_of(&parsed.operation);
            let pds = pds_of(&parsed.operation);
            let handle = handle_of(&parsed.operation);
            if writer.is_none() {
                writer = Some(PlcOpWriter::create(
                    shard_path(&plc_dir, cur.shards),
                    cfg.batch_size,
                )?);
            }
            writer.as_mut().unwrap().push(
                &parsed.did,
                kind,
                ts_ms(&parsed.created_at),
                pds.as_deref(),
                handle.as_deref(),
            )?;
            cur.rows += 1;
            rows_since_flush += 1;
        }

        // Advance the cursor to the last op seen this page.
        if let Some(last) = lines.last() {
            if let Ok(p) = serde_json::from_str::<ExportLine>(last) {
                cur.after = Some(p.created_at);
            }
        }
        cur.pages += 1;

        let short_page = lines.len() < page_size;
        if short_page {
            if let Some(w) = writer.take() {
                let (_, _) = w.finish()?;
                cur.shards += 1;
            }
            cur.completed = true;
            cur.updated_at = now_rfc3339();
            save_cursor(&cursor_path, &cur)?;
            break;
        }

        if rows_since_flush >= FLUSH_EVERY {
            if let Some(w) = writer.take() {
                let (_, _) = w.finish()?;
                cur.shards += 1;
            }
            rows_since_flush = 0;
            cur.updated_at = now_rfc3339();
            save_cursor(&cursor_path, &cur)?;
            tracing::info!(
                pages = cur.pages,
                rows = cur.rows,
                shards = cur.shards,
                after = ?cur.after,
                "plc export progress"
            );
        }
    }

    tracing::info!(
        pages = cur.pages,
        rows = cur.rows,
        shards = cur.shards,
        "plc export complete"
    );
    Ok(PlcOutcome { rows: cur.rows, shards: cur.shards, completed: cur.completed })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Classify a JSON export line the way the fetch loop does.
    fn classify(line: &str) -> &'static str {
        let p: ExportLine = serde_json::from_str(line).unwrap();
        kind_of(&p.operation)
    }

    fn parse(line: &str) -> OpInner {
        serde_json::from_str::<ExportLine>(line).unwrap().operation
    }

    #[test]
    fn genesis_op_is_create() {
        // prev: null => genesis, regardless of type ("create" or "plc_operation").
        let legacy = r#"{"did":"did:plc:aaa","createdAt":"2022-11-17T00:35:16.391Z","operation":{"prev":null,"type":"create"}}"#;
        let modern = r#"{"did":"did:plc:bbb","createdAt":"2023-01-02T04:07:13.767Z","operation":{"prev":null,"type":"plc_operation"}}"#;
        assert_eq!(classify(legacy), "create");
        assert_eq!(classify(modern), "create");
    }

    #[test]
    fn tombstone_op_is_tombstone() {
        let tomb = r#"{"did":"did:plc:ccc","createdAt":"2024-05-01T00:00:00.000Z","operation":{"prev":"bafyrei...","type":"plc_tombstone"}}"#;
        assert_eq!(classify(tomb), "tombstone");
    }

    #[test]
    fn migration_op_is_update() {
        // A non-genesis plc_operation (handle/key rotation or PDS migration)
        // carries a prev and a non-tombstone type => kind "update".
        let mig = r#"{"did":"did:plc:ddd","createdAt":"2024-06-01T00:00:00.000Z","operation":{"prev":"bafyrei...","type":"plc_operation"}}"#;
        assert_eq!(classify(mig), "update");
    }

    #[test]
    fn pds_and_handle_parse_both_formats() {
        // legacy create: bare `service` + `handle`.
        let legacy = parse(
            r#"{"did":"did:plc:a","createdAt":"2022-11-17T00:00:00.000Z","operation":{"prev":null,"type":"create","handle":"paul.bsky.social","service":"https://bsky.social"}}"#,
        );
        assert_eq!(pds_of(&legacy).as_deref(), Some("https://bsky.social"));
        assert_eq!(handle_of(&legacy).as_deref(), Some("paul.bsky.social"));

        // modern: nested services + alsoKnownAs (at:// stripped).
        let modern = parse(
            r#"{"did":"did:plc:b","createdAt":"2025-06-01T00:00:00.000Z","operation":{"prev":"bafy","type":"plc_operation","alsoKnownAs":["at://gametheory.blog"],"services":{"atproto_pds":{"type":"AtprotoPersonalDataServer","endpoint":"https://psathyrella.us-west.host.bsky.network"}}}}"#,
        );
        assert_eq!(
            pds_of(&modern).as_deref(),
            Some("https://psathyrella.us-west.host.bsky.network")
        );
        assert_eq!(handle_of(&modern).as_deref(), Some("gametheory.blog"));

        // tombstone: no PDS.
        let tomb = parse(
            r#"{"did":"did:plc:c","createdAt":"2024-05-01T00:00:00.000Z","operation":{"prev":"bafy","type":"plc_tombstone"}}"#,
        );
        assert_eq!(pds_of(&tomb), None);
    }

    #[test]
    fn ts_ms_parses_rfc3339_z() {
        // 2022-11-17T00:35:16.391Z == 1668645316391 ms since epoch.
        assert_eq!(ts_ms("2022-11-17T00:35:16.391Z"), Some(1_668_645_316_391));
        assert_eq!(ts_ms("not-a-date"), None);
    }

    #[test]
    fn cursor_round_trips() {
        let cur = PlcCursor {
            schema_version: CURSOR_SCHEMA_VERSION,
            after: Some("2023-09-11T21:02:46.369Z".to_string()),
            shards: 3,
            rows: 1_234_567,
            pages: 2048,
            completed: false,
            updated_at: "2026-06-04T02:33:26Z".to_string(),
        };
        let bytes = serde_json::to_vec_pretty(&cur).unwrap();
        let back: PlcCursor = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back.after, cur.after);
        assert_eq!(back.shards, 3);
        assert_eq!(back.rows, 1_234_567);
        assert!(!back.completed);
    }
}
