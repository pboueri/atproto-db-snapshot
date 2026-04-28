use anyhow::{Context, Result};
use duckdb::Connection;
use std::path::PathBuf;

use crate::config::Config;

pub struct HydrateOutcome {
    pub duckdb_path: PathBuf,
    pub row_counts: Vec<(String, u64)>,
    pub orphan_like_rate: f64,
    pub orphan_repost_rate: f64,
}

const SQL_MACROS: &str = include_str!("sql/macros.sql");
const SQL_LOAD_RAW: &str = include_str!("sql/load_raw.sql");
const SQL_BUILD_FOLLOWS: &str = include_str!("sql/build_follows.sql");
const SQL_BUILD_BLOCKS: &str = include_str!("sql/build_blocks.sql");
const SQL_BUILD_LIKES: &str = include_str!("sql/build_likes.sql");
const SQL_BUILD_REPOSTS: &str = include_str!("sql/build_reposts.sql");
const SQL_BUILD_POSTS_FROM_RECORDS: &str = include_str!("sql/build_posts_from_records.sql");
const SQL_BUILD_POST_MEDIA: &str = include_str!("sql/build_post_media.sql");
const SQL_BUILD_POSTS: &str = include_str!("sql/build_posts.sql");
const SQL_BUILD_ACTOR_AGGS: &str = include_str!("sql/build_actor_aggs.sql");
const SQL_BUILD_POST_AGGS: &str = include_str!("sql/build_post_aggs.sql");
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
    pragma(&conn, &format!("SET memory_limit='{}'", cfg.memory_limit))?;
    pragma(&conn, "SET preserve_insertion_order=false")?;
    pragma(
        &conn,
        &format!("SET temp_directory='{}/duckdb_tmp'", raw_str),
    )?;

    let stages: &[(&str, &str)] = &[
        ("macros", SQL_MACROS),
        ("load_raw", &SQL_LOAD_RAW.replace("{RAW}", &raw_str)),
        ("build_follows", SQL_BUILD_FOLLOWS),
        ("build_blocks", SQL_BUILD_BLOCKS),
        ("build_likes", SQL_BUILD_LIKES),
        ("build_reposts", SQL_BUILD_REPOSTS),
        ("build_posts_from_records", SQL_BUILD_POSTS_FROM_RECORDS),
        ("build_post_media", SQL_BUILD_POST_MEDIA),
        ("build_posts", SQL_BUILD_POSTS),
        ("build_actor_aggs", SQL_BUILD_ACTOR_AGGS),
        ("build_post_aggs", SQL_BUILD_POST_AGGS),
    ];
    for (label, sql) in stages {
        tracing::info!(label, "running hydrate sql");
        conn.execute_batch(sql)
            .with_context(|| format!("hydrate sql: {label}"))?;
    }

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
