//! Local-side repair operations for an existing snapshot.duckdb.
//!
//! Use case: a built snapshot.duckdb downloaded from Modal has
//! VIEW-backed entity tables that point at the build host's parquet
//! paths (`/tmp/var/raw/<date>/*.parquet`), or the `posts` table has
//! block-level corruption from a hydrate that ran with a runaway
//! memory_limit. Re-running the full pipeline costs hours; these
//! commands surgically swap out the affected table(s) using parquet
//! files the operator has staged locally (typically via
//! `modal volume get at-snapshot-output var/raw/<date>/...`).

use anyhow::{Context, Result};
use duckdb::Connection;
use std::path::Path;
use std::time::Instant;

/// Replace a single table or view named `table` in `db_path` with a
/// freshly-materialized TABLE built from `parquet_path`. Used to fix
/// VIEW-only entity tables (actors, follows, blocks, likes, reposts)
/// in a downloaded snapshot.duckdb where the build-host parquet
/// paths the views reference no longer exist locally.
pub fn repair_table(db_path: &Path, table: &str, parquet_path: &Path) -> Result<()> {
    if !is_safe_ident(table) {
        anyhow::bail!("refusing unsafe table name {table:?}");
    }
    let parquet = parquet_path
        .canonicalize()
        .with_context(|| format!("canonicalize {}", parquet_path.display()))?;
    let parquet_lit = sql_string_literal(&parquet.to_string_lossy());

    let conn = Connection::open(db_path)
        .with_context(|| format!("open {}", db_path.display()))?;

    let t0 = Instant::now();
    tracing::info!(table, parquet = %parquet.display(), "repair_table start");

    // Drop both forms; whichever exists wins. Order matters because a
    // name can only be one or the other, and DROP IF EXISTS won't
    // error if the other branch is empty.
    conn.execute_batch(&format!("DROP VIEW IF EXISTS {table}"))
        .with_context(|| format!("drop view {table}"))?;
    conn.execute_batch(&format!("DROP TABLE IF EXISTS {table}"))
        .with_context(|| format!("drop table {table}"))?;

    conn.execute_batch(&format!(
        "CREATE TABLE {table} AS SELECT * FROM read_parquet({parquet_lit})"
    ))
    .with_context(|| format!("create table {table}"))?;

    let n: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
        .with_context(|| format!("count {table}"))?;

    conn.execute_batch("FORCE CHECKPOINT")
        .context("force checkpoint")?;
    drop(conn);

    tracing::info!(
        table,
        rows = n,
        elapsed_secs = t0.elapsed().as_secs_f64(),
        "repair_table done"
    );
    Ok(())
}

/// Drop and rebuild the `posts` table from the two staging parquets
/// using the same union/dedup logic as 03_build_posts.sql. This is
/// the recovery path for block-level corruption in posts: the source
/// data lives in the parquets, so we can reconstruct the table even
/// when the in-DB blocks are unreadable.
pub fn rebuild_posts(
    db_path: &Path,
    records_parquet: &Path,
    targets_parquet: &Path,
    memory_limit: &str,
) -> Result<()> {
    let records = records_parquet
        .canonicalize()
        .with_context(|| format!("canonicalize {}", records_parquet.display()))?;
    let targets = targets_parquet
        .canonicalize()
        .with_context(|| format!("canonicalize {}", targets_parquet.display()))?;

    let conn = Connection::open(db_path)
        .with_context(|| format!("open {}", db_path.display()))?;
    conn.execute_batch(&format!("SET memory_limit='{memory_limit}'"))
        .with_context(|| format!("set memory_limit={memory_limit}"))?;
    conn.execute_batch("SET preserve_insertion_order=false")
        .context("preserve_insertion_order")?;
    if let Some(parent) = db_path.parent() {
        let tmp = parent.join("duckdb_tmp");
        std::fs::create_dir_all(&tmp)
            .with_context(|| format!("create {}", tmp.display()))?;
        conn.execute_batch(&format!(
            "SET temp_directory='{}'",
            tmp.to_string_lossy()
        ))
        .context("temp_directory")?;
    }

    let records_lit = sql_string_literal(&records.to_string_lossy());
    let targets_lit = sql_string_literal(&targets.to_string_lossy());

    let t0 = Instant::now();
    tracing::info!(
        records = %records.display(),
        targets = %targets.display(),
        "rebuild_posts start"
    );

    // Drop the corrupt table first so DuckDB can free its blocks
    // before the new write expands the file. Drop dependents
    // explicitly because actor_aggs / post_aggs reference posts.
    conn.execute_batch(
        "DROP TABLE IF EXISTS post_aggs;
         DROP TABLE IF EXISTS actor_aggs;
         DROP TABLE IF EXISTS posts;",
    )
    .context("drop existing posts and dependents")?;
    conn.execute_batch("FORCE CHECKPOINT")
        .context("force checkpoint after drops")?;

    let sql = format!(
        "CREATE TABLE posts AS
         WITH unioned AS (
           SELECT
             uri_id, author_did_id, rkey, created_at,
             reply_root_uri_id, reply_parent_uri_id, quote_uri_id,
             'record' AS source
           FROM read_parquet({records_lit})
           UNION ALL
           SELECT
             uri_id, author_did_id, rkey, created_at,
             reply_root_uri_id, reply_parent_uri_id, quote_uri_id,
             'target_only' AS source
           FROM read_parquet({targets_lit})
         ),
         ranked AS (
           SELECT
             uri_id,
             ARG_MAX(author_did_id, source = 'record')        AS author_did_id,
             ARG_MAX(rkey, source = 'record')                 AS rkey,
             ARG_MAX(created_at, source = 'record')           AS created_at,
             ARG_MAX(reply_root_uri_id, source = 'record')    AS reply_root_uri_id,
             ARG_MAX(reply_parent_uri_id, source = 'record')  AS reply_parent_uri_id,
             ARG_MAX(quote_uri_id, source = 'record')         AS quote_uri_id,
             MIN(source)                                      AS source
           FROM unioned
           GROUP BY 1
         )
         SELECT * FROM ranked"
    );
    conn.execute_batch(&sql).context("rebuild posts")?;

    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM posts", [], |r| r.get(0))
        .context("count posts")?;
    conn.execute_batch("FORCE CHECKPOINT")
        .context("force checkpoint after rebuild")?;
    drop(conn);

    tracing::info!(
        rows = n,
        elapsed_secs = t0.elapsed().as_secs_f64(),
        "rebuild_posts done; rerun aggs separately if needed"
    );
    Ok(())
}

fn is_safe_ident(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn sql_string_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

const MACROS_SQL_PURE: &str = include_str!("sql/01_macros.sql");
const MACROS_SQL_URL_LOOKUP: &str = include_str!("sql/06_url_macros.sql");

/// (Re)create every MACRO that ships with at-snapshot into an
/// existing snapshot.duckdb. Applies both the pure-string macros
/// (`tid_to_ts`, `post_at_uri`, `post_url`, `actor_url` — no table
/// dependencies) and the lookup-by-id wrappers (`post_url_by_id`
/// etc., which require posts + actors to already exist). All
/// statements use `CREATE OR REPLACE`, so it's safe to run repeatedly.
pub fn install_macros(db_path: &Path) -> Result<()> {
    let conn = Connection::open(db_path)
        .with_context(|| format!("open {}", db_path.display()))?;
    let t0 = Instant::now();
    tracing::info!(db = %db_path.display(), "install_macros start");
    conn.execute_batch(MACROS_SQL_PURE)
        .context("apply 01_macros.sql")?;
    conn.execute_batch(MACROS_SQL_URL_LOOKUP)
        .context("apply 06_url_macros.sql")?;
    conn.execute_batch("FORCE CHECKPOINT")
        .context("force checkpoint")?;
    drop(conn);
    tracing::info!(
        elapsed_secs = t0.elapsed().as_secs_f64(),
        "install_macros done"
    );
    Ok(())
}
