use anyhow::{anyhow, Context, Result};
use chrono::{NaiveDate, Utc};
use duckdb::{params, Connection};
use std::path::PathBuf;
use std::time::Instant;

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
    // Tell DuckDB an explicit spill budget. Default is 90% of *remaining*
    // disk on the temp_directory drive, which on Modal lands well past
    // what /tmp can actually give once raw parquet (~410 GB) and the
    // growing snapshot.duckdb (~150 GB) are accounted for. 400 GiB
    // leaves headroom for both.
    pragma(&conn, "SET max_temp_directory_size='400GiB'")?;

    let window = resolve_window(cfg, snapshot_date)?;
    if let Some(days) = cfg.hydrate_window_days {
        tracing::info!(
            window_days = days,
            cutoff = window
                .cutoff
                .map(|d| d.format("%Y-%m-%d").to_string())
                .as_deref(),
            "applying hydrate window filter"
        );
    } else {
        tracing::info!("no hydrate window filter (full materialization)");
    }
    let chunk_buckets = cfg.hydrate_chunk_buckets.unwrap_or(1).max(1);
    let dry_run = cfg.hydrate_chunk_dry_run.unwrap_or(false);
    if chunk_buckets > 1 {
        tracing::info!(
            chunk_buckets,
            dry_run,
            "chunking enabled for chunked stages"
        );
    } else {
        tracing::info!("chunking disabled (single-shot)");
    }

    let stages_t0 = Instant::now();
    for (label, template) in SQL_STAGES {
        let body = template
            .replace("{RAW}", &raw_str)
            .replace("{REC_WINDOW}", &window.rec_clause)
            .replace("{TID_WINDOW}", &window.tid_clause);
        let stage_t0 = Instant::now();
        tracing::info!(label, "stage start");
        match *label {
            "build_follows" => {
                run_chunked_table(&conn, label, "follows", &body, chunk_buckets, dry_run)?
            }
            "build_blocks" => {
                run_chunked_table(&conn, label, "blocks", &body, chunk_buckets, dry_run)?
            }
            "build_posts_from_records" => run_chunked_table(
                &conn,
                label,
                "posts_from_records",
                &body,
                chunk_buckets,
                dry_run,
            )?,
            "build_post_media" => {
                run_chunked_table(&conn, label, "post_media", &body, chunk_buckets, dry_run)?
            }
            "build_likes" => {
                run_chunked_table(&conn, label, "likes", &body, chunk_buckets, dry_run)?
            }
            "build_reposts" => {
                run_chunked_table(&conn, label, "reposts", &body, chunk_buckets, dry_run)?
            }
            "build_actor_aggs" => {
                run_chunked_table(&conn, label, "actor_aggs", &body, chunk_buckets, dry_run)?
            }
            "build_post_aggs" => {
                run_chunked_table(&conn, label, "post_aggs", &body, chunk_buckets, dry_run)?
            }
            _ => {
                // Non-chunked path: strip {CHUNK_*} placeholders so SQL
                // files that mention them still parse. K=0, N=1 →
                // `col % 1 = 0` is always true and DuckDB folds it away.
                let stripped = body
                    .replace("{CHUNK_PRED}", "")
                    .replace("{CHUNK_PRED_LT}", "")
                    .replace("{CHUNK_N}", "1")
                    .replace("{CHUNK_K}", "0");
                conn.execute_batch(&stripped)
                    .with_context(|| format!("hydrate sql: {label}"))?
            }
        }
        tracing::info!(
            label,
            elapsed_secs = stage_t0.elapsed().as_secs_f64(),
            "stage done"
        );
    }
    tracing::info!(
        elapsed_secs = stages_t0.elapsed().as_secs_f64(),
        "all sql stages complete"
    );

    write_snapshot_metadata(&conn, cfg, snapshot_date, &memory_limit, &window)
        .context("write snapshot_metadata")?;

    let counts = collect_counts(&conn)?;
    let (orphan_like, orphan_repost) = collect_orphan_rates(&conn)?;
    for (table, n) in &counts {
        tracing::info!(table, rows = *n, "row count");
    }
    tracing::info!(
        orphan_like_rate = orphan_like,
        orphan_repost_rate = orphan_repost,
        "orphan rates"
    );
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

/// Materialize a SELECT-body into `table` either in one shot
/// (`n_chunks <= 1`) or as N independent passes partitioned on
/// `r.did_id % N`. The first pass uses `CREATE TABLE … AS` to lock in
/// the schema; subsequent passes `INSERT INTO …`. Each pass executes
/// independently, so DuckDB can free the chunk's hash tables between
/// iterations and the spill ceiling is the largest single chunk rather
/// than the whole join.
fn run_chunked_table(
    conn: &Connection,
    label: &str,
    table: &str,
    body: &str,
    n_chunks: u32,
    dry_run: bool,
) -> Result<()> {
    let n = n_chunks.max(1);
    // Dry-run executes only the first chunk so the resulting snapshot
    // is a 1/N sample of the full output — used to validate SQL shape
    // end-to-end on Modal before committing to a full run. The CREATE
    // TABLE still happens (k=0 path), so downstream stages see a
    // populated, just-smaller table.
    let last = if dry_run { 1.min(n) } else { n };
    if dry_run && n > 1 {
        tracing::warn!(
            label,
            chunks_total = n,
            chunks_to_run = last,
            "dry_run: only first chunk will execute"
        );
    }
    let mut prev_total: i64 = 0;
    for k in 0..last {
        let (pred, pred_lt) = if n == 1 {
            (String::new(), String::new())
        } else {
            (
                format!("AND r.did_id % {n} = {k}"),
                format!("AND did_id % {n} = {k}"),
            )
        };
        // {CHUNK_K} / {CHUNK_N} let SQL files write modulo predicates
        // on whatever column they want to partition on (uri_id,
        // author_did_id, etc.). When n=1 the expression evaluates to
        // `<col> % 1 = 0` (always true) and DuckDB folds it away.
        let select = body
            .replace("{CHUNK_PRED}", &pred)
            .replace("{CHUNK_PRED_LT}", &pred_lt)
            .replace("{CHUNK_N}", &n.to_string())
            .replace("{CHUNK_K}", &k.to_string());
        let stmt = if k == 0 {
            format!("CREATE TABLE {table} AS {select}")
        } else {
            format!("INSERT INTO {table} {select}")
        };
        if n > 1 {
            tracing::info!(label, chunk = k + 1, chunks = n, "chunk start");
        }
        let chunk_t0 = Instant::now();
        conn.execute_batch(&stmt)
            .with_context(|| format!("hydrate sql: {label} chunk {}/{n}", k + 1))?;
        if n > 1 {
            let total: i64 = conn
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
                .with_context(|| format!("count {table} after chunk {}/{n}", k + 1))?;
            tracing::info!(
                label,
                chunk = k + 1,
                chunks = n,
                rows_added = total - prev_total,
                rows_total = total,
                elapsed_secs = chunk_t0.elapsed().as_secs_f64(),
                "chunk done"
            );
            prev_total = total;
        }
    }
    Ok(())
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
    tracing::info!(
        snapshot_date,
        source_url = %cfg.source_url,
        backup_id = ?cfg.backup_id,
        built_at = %built_at,
        window_days = ?cfg.hydrate_window_days,
        window_cutoff = window_cutoff.as_deref(),
        at_snapshot_version = env!("CARGO_PKG_VERSION"),
        duckdb_memory_limit,
        "snapshot_metadata written"
    );
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
