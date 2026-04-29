use anyhow::{anyhow, Context, Result};
use chrono::{NaiveDate, Utc};
use duckdb::{params, Connection};
use std::path::PathBuf;

use crate::config::Config;

pub struct HydrateOutcome {
    pub duckdb_path: PathBuf,
    pub row_counts: Vec<(String, u64)>,
    pub orphan_like_rate: f64,
    pub orphan_repost_rate: f64,
}

include!(concat!(env!("OUT_DIR"), "/sql_stages.rs"));

const SQL_ORPHAN_RATE: &str = include_str!("sql/orphan_rate.sql");

pub async fn run(cfg: &Config, snapshot_date: &str) -> Result<HydrateOutcome> {
    let raw_dir = cfg.raw_dir(snapshot_date);
    let snapshot_dir = cfg.snapshot_dir(snapshot_date);
    std::fs::create_dir_all(&snapshot_dir)
        .with_context(|| format!("create snapshot dir {}", snapshot_dir.display()))?;
    let duckdb_path = snapshot_dir.join("snapshot.duckdb");
    if duckdb_path.exists() {
        std::fs::remove_file(&duckdb_path)
            .with_context(|| format!("remove stale {}", duckdb_path.display()))?;
    }

    let raw = raw_dir
        .canonicalize()
        .with_context(|| format!("canonicalize {}", raw_dir.display()))?;
    let tmp = raw.join("duckdb_tmp");
    std::fs::create_dir_all(&tmp).with_context(|| format!("create tmp {}", tmp.display()))?;
    let raw_str = raw.to_string_lossy().to_string();
    let raw_str = raw_str.trim_end_matches('/').to_string();

    tracing::info!(duckdb = %duckdb_path.display(), "opening duckdb");
    let conn = Connection::open(&duckdb_path).context("duckdb open")?;
    let memory_limit = cfg.resolved_memory_limit()?;
    tracing::info!(
        configured = %cfg.memory_limit,
        resolved = %memory_limit,
        "duckdb memory_limit"
    );
    pragma(&conn, &format!("SET memory_limit='{memory_limit}'"))?;
    pragma(&conn, "SET preserve_insertion_order=false")?;
    pragma(
        &conn,
        &format!("SET temp_directory='{}/duckdb_tmp'", raw_str),
    )?;

    let window = resolve_window(cfg, snapshot_date)?;
    if let Some(days) = cfg.hydrate_window_days {
        tracing::info!(window_days = days, "applying hydrate window filter");
    }

    for (label, template) in SQL_STAGES {
        let sql = template
            .replace("{RAW}", &raw_str)
            .replace("{REC_WINDOW}", &window.rec_clause)
            .replace("{TID_WINDOW}", &window.tid_clause);
        tracing::info!(label, "running hydrate sql");
        conn.execute_batch(&sql)
            .with_context(|| format!("hydrate sql: {label}"))?;
    }

    write_snapshot_metadata(&conn, cfg, snapshot_date, &memory_limit, &window)
        .context("write snapshot_metadata")?;

    let counts = collect_counts(&conn)?;
    let (orphan_like, orphan_repost) = collect_orphan_rates(&conn)?;
    drop(conn);

    Ok(HydrateOutcome {
        duckdb_path,
        row_counts: counts,
        orphan_like_rate: orphan_like,
        orphan_repost_rate: orphan_repost,
    })
}

fn pragma(conn: &Connection, sql: &str) -> Result<()> {
    conn.execute_batch(sql)
        .with_context(|| format!("pragma: {sql}"))
}

struct WindowSpec {
    cutoff: Option<NaiveDate>,
    rec_clause: String,
    tid_clause: String,
}

/// Resolve the configured window into the cutoff date plus the SQL
/// fragments substituted into build_*.sql. Returns an empty spec when
/// `hydrate_window_days` is None so the queries behave exactly as
/// before. When a window is set, the cutoff is
/// (snapshot_date - window_days) at 00:00 UTC.
///
/// `{REC_WINDOW}` is meant to follow a `WHERE r.collection = '...'`
/// clause where `r` is link_records — it expands to
/// `AND r.created_at >= TIMESTAMP 'YYYY-MM-DD 00:00:00'`.
///
/// `{TID_WINDOW}` is for the target_only branch in 06_build_posts.sql,
/// where the rkey isn't materialized as a column yet — it expands to
/// `AND tid_to_ts(<rkey-extract>) >= TIMESTAMP '...'` so DuckDB can
/// filter directly off the URI string.
fn resolve_window(cfg: &Config, snapshot_date: &str) -> Result<WindowSpec> {
    let Some(days) = cfg.hydrate_window_days else {
        return Ok(WindowSpec {
            cutoff: None,
            rec_clause: String::new(),
            tid_clause: String::new(),
        });
    };
    let date = NaiveDate::parse_from_str(snapshot_date, "%Y-%m-%d")
        .with_context(|| format!("parse snapshot_date {snapshot_date:?}"))?;
    let cutoff = date
        .checked_sub_days(chrono::Days::new(days as u64))
        .ok_or_else(|| anyhow!("window {days} days underflows from {snapshot_date}"))?;
    let cutoff_lit = format!("TIMESTAMP '{} 00:00:00'", cutoff.format("%Y-%m-%d"));
    let rec_clause = format!("AND r.created_at >= {cutoff_lit}");
    // 06_build_posts target_only.uri is `at://<did>/<collection>/<rkey>`;
    // strip the `at://` prefix (substring(uri, 6)) and split on `/` to
    // recover the rkey.
    let tid_clause = format!(
        "AND tid_to_ts(split_part(substring(u.uri, 6), '/', 3)) >= {cutoff_lit}"
    );
    Ok(WindowSpec {
        cutoff: Some(cutoff),
        rec_clause,
        tid_clause,
    })
}

/// Single-row table that records what this snapshot was built from and
/// with — date, source, version, window settings, etc. Designed for
/// downstream queries like `SELECT window_cutoff FROM snapshot_metadata`
/// so consumers can decide whether the snapshot covers the time range
/// they care about without inspecting config files.
fn write_snapshot_metadata(
    conn: &Connection,
    cfg: &Config,
    snapshot_date: &str,
    duckdb_memory_limit: &str,
    window: &WindowSpec,
) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE snapshot_metadata (
            snapshot_date       DATE,
            source_url          TEXT,
            backup_id           BIGINT,
            built_at            TIMESTAMP,
            hydrate_window_days INTEGER,
            window_cutoff       DATE,
            at_snapshot_version TEXT,
            duckdb_memory_limit TEXT
        )",
    )
    .context("create snapshot_metadata")?;

    let built_at = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let window_cutoff = window.cutoff.map(|d| d.format("%Y-%m-%d").to_string());
    let backup_id = cfg.backup_id.map(|b| b as i64);
    let window_days = cfg.hydrate_window_days.map(|d| d as i32);

    conn.execute(
        "INSERT INTO snapshot_metadata
            (snapshot_date, source_url, backup_id, built_at,
             hydrate_window_days, window_cutoff,
             at_snapshot_version, duckdb_memory_limit)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            snapshot_date,
            cfg.source_url,
            backup_id,
            built_at,
            window_days,
            window_cutoff,
            env!("CARGO_PKG_VERSION"),
            duckdb_memory_limit,
        ],
    )
    .context("insert snapshot_metadata row")?;
    Ok(())
}

fn collect_counts(conn: &Connection) -> Result<Vec<(String, u64)>> {
    let tables = [
        "actors",
        "follows",
        "blocks",
        "likes",
        "reposts",
        "posts",
        "post_media",
        "actor_aggs",
        "post_aggs",
    ];
    let mut out = Vec::new();
    for t in tables {
        let n: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {t}"), [], |row| row.get(0))
            .with_context(|| format!("count {t}"))?;
        out.push((t.to_string(), n as u64));
    }
    Ok(out)
}

fn collect_orphan_rates(conn: &Connection) -> Result<(f64, f64)> {
    let likes_sql = SQL_ORPHAN_RATE.replace("{TABLE}", "likes");
    let reposts_sql = SQL_ORPHAN_RATE.replace("{TABLE}", "reposts");
    let l: f64 = conn
        .query_row(&likes_sql, [], |row| row.get(0))
        .context("orphan likes rate")?;
    let r: f64 = conn
        .query_row(&reposts_sql, [], |row| row.get(0))
        .context("orphan reposts rate")?;
    Ok((l, r))
}
