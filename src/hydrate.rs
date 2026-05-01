use anyhow::{Context, Result};
use chrono::Utc;
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
    pragma(&conn, "SET max_temp_directory_size='400GiB'")?;

    let chunk_buckets = cfg.hydrate_chunk_buckets.unwrap_or(1).max(1);
    let dry_run = cfg.hydrate_chunk_dry_run.unwrap_or(false);
    if chunk_buckets > 1 {
        tracing::info!(
            chunk_buckets,
            dry_run,
            "chunking enabled for actor_aggs / post_aggs"
        );
    } else {
        tracing::info!("chunking disabled (single-shot)");
    }

    let stages_t0 = Instant::now();
    for (label, template) in SQL_STAGES {
        let body = template.replace("{RAW}", &raw_str);
        let stage_t0 = Instant::now();
        tracing::info!(label, "stage start");
        match *label {
            "build_actor_aggs" => {
                run_chunked_table(&conn, label, "actor_aggs", &body, chunk_buckets, dry_run)?
            }
            "build_post_aggs" => {
                run_chunked_table(&conn, label, "post_aggs", &body, chunk_buckets, dry_run)?
            }
            _ => {
                // Single-shot stages (load_raw, build_posts, macros) —
                // strip chunk placeholders so files that still mention
                // them parse. Posts dedup is small enough to run in one
                // pass; if it grows past the memory limit we'll add
                // uri_id-modulo chunking the same way actor_aggs does.
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

    write_snapshot_metadata(&conn, cfg, snapshot_date, &memory_limit)
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
/// (`n_chunks <= 1`) or as N independent passes partitioned on the
/// modulo predicate the SQL file specifies. The first pass uses
/// `CREATE TABLE … AS` to lock in the schema; subsequent passes
/// `INSERT INTO …`. Each pass executes independently, so DuckDB can
/// free the chunk's hash tables between iterations and the spill
/// ceiling is the largest single chunk rather than the whole join.
fn run_chunked_table(
    conn: &Connection,
    label: &str,
    table: &str,
    body: &str,
    n_chunks: u32,
    dry_run: bool,
) -> Result<()> {
    let n = n_chunks.max(1);
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
        let select = body
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

fn write_snapshot_metadata(
    conn: &Connection,
    cfg: &Config,
    snapshot_date: &str,
    duckdb_memory_limit: &str,
) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE snapshot_metadata (
            snapshot_date       DATE,
            source_url          TEXT,
            backup_id           BIGINT,
            built_at            TIMESTAMP,
            at_snapshot_version TEXT,
            duckdb_memory_limit TEXT
        )",
    )
    .context("create snapshot_metadata")?;

    let built_at = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let backup_id = cfg.backup_id.map(|b| b as i64);

    conn.execute(
        "INSERT INTO snapshot_metadata
            (snapshot_date, source_url, backup_id, built_at,
             at_snapshot_version, duckdb_memory_limit)
         VALUES (?, ?, ?, ?, ?, ?)",
        params![
            snapshot_date,
            cfg.source_url,
            backup_id,
            built_at,
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
