use anyhow::{Context, Result};
use duckdb::Connection;
use std::path::{Path, PathBuf};

use crate::config::Config;

pub struct HydrateOutcome {
    pub duckdb_path: PathBuf,
    pub row_counts: Vec<(String, u64)>,
    pub orphan_like_rate: f64,
    pub orphan_repost_rate: f64,
}

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
    let raw = raw.to_string_lossy().to_string();

    tracing::info!(duckdb = %duckdb_path.display(), "opening duckdb");
    let conn = Connection::open(&duckdb_path).context("duckdb open")?;
    pragma(&conn, &format!("SET memory_limit='{}'", cfg.memory_limit))?;
    pragma(&conn, "SET preserve_insertion_order=false")?;
    pragma(
        &conn,
        &format!("SET temp_directory='{}/duckdb_tmp'", raw.trim_end_matches('/')),
    )?;

    load_raw(&conn, &raw)?;
    build_posts(&conn)?;
    build_actor_aggs(&conn)?;
    build_post_aggs(&conn)?;

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

fn load_raw(conn: &Connection, raw: &str) -> Result<()> {
    let tables = [
        ("actors", "actors.parquet"),
        ("follows", "follows.parquet"),
        ("blocks", "blocks.parquet"),
        ("likes", "likes.parquet"),
        ("reposts", "reposts.parquet"),
        ("post_media", "post_media.parquet"),
        ("posts_from_records", "posts_from_records.parquet"),
        ("posts_from_targets", "posts_from_targets.parquet"),
    ];
    for (table, file) in tables {
        let path = format!("{raw}/{file}");
        if !Path::new(&path).exists() {
            tracing::warn!(path, "missing parquet input; skipping");
            continue;
        }
        let sql = format!(
            "CREATE TABLE {table} AS SELECT * FROM read_parquet('{path}');"
        );
        tracing::info!(table, "loading parquet");
        conn.execute_batch(&sql)
            .with_context(|| format!("load {table}"))?;
    }
    Ok(())
}

fn build_posts(conn: &Connection) -> Result<()> {
    tracing::info!("building posts (UNION + dedup)");
    conn.execute_batch(
        r#"
CREATE TABLE posts AS
SELECT
  uri, author_did_id, rkey, created_at,
  reply_root_uri, reply_parent_uri, quote_uri,
  'record' AS source
FROM posts_from_records
UNION ALL
SELECT
  t.uri,
  a.did_id AS author_did_id,
  t.rkey, t.created_at,
  NULL AS reply_root_uri, NULL AS reply_parent_uri, NULL AS quote_uri,
  'target_only' AS source
FROM posts_from_targets t
LEFT JOIN actors a ON a.did = t.author_did
WHERE NOT EXISTS (SELECT 1 FROM posts_from_records r WHERE r.uri = t.uri);
"#,
    )
    .context("build posts")?;
    Ok(())
}

fn build_actor_aggs(conn: &Connection) -> Result<()> {
    tracing::info!("building actor_aggs");
    conn.execute_batch(
        r#"
CREATE TABLE actor_aggs AS
SELECT
  a.did_id,
  COALESCE(f_out.c, 0) AS follows,
  COALESCE(f_in.c, 0) AS followers,
  COALESCE(b_out.c, 0) AS blocks_out,
  COALESCE(b_in.c, 0) AS blocks_in,
  COALESCE(p.c, 0) AS posts,
  COALESCE(l_out.c, 0) AS likes_out,
  COALESCE(l_in.c, 0) AS likes_in,
  COALESCE(r_out.c, 0) AS reposts_out,
  COALESCE(r_in.c, 0) AS reposts_in,
  COALESCE(rep.c, 0) AS replies_out,
  COALESCE(qo.c, 0) AS quotes_out,
  COALESCE(qd.c, 0) AS quoted_count
FROM actors a
LEFT JOIN (SELECT src_did_id AS did_id, COUNT(*) c FROM follows GROUP BY 1) f_out USING(did_id)
LEFT JOIN (SELECT dst_did_id AS did_id, COUNT(*) c FROM follows GROUP BY 1) f_in USING(did_id)
LEFT JOIN (SELECT src_did_id AS did_id, COUNT(*) c FROM blocks GROUP BY 1) b_out USING(did_id)
LEFT JOIN (SELECT dst_did_id AS did_id, COUNT(*) c FROM blocks GROUP BY 1) b_in USING(did_id)
LEFT JOIN (SELECT author_did_id AS did_id, COUNT(*) c FROM posts GROUP BY 1) p USING(did_id)
LEFT JOIN (SELECT actor_did_id AS did_id, COUNT(*) c FROM likes GROUP BY 1) l_out USING(did_id)
LEFT JOIN (SELECT p.author_did_id AS did_id, COUNT(*) c
           FROM likes l JOIN posts p ON p.uri = l.subject_uri GROUP BY 1) l_in USING(did_id)
LEFT JOIN (SELECT actor_did_id AS did_id, COUNT(*) c FROM reposts GROUP BY 1) r_out USING(did_id)
LEFT JOIN (SELECT p.author_did_id AS did_id, COUNT(*) c
           FROM reposts r JOIN posts p ON p.uri = r.subject_uri GROUP BY 1) r_in USING(did_id)
LEFT JOIN (SELECT author_did_id AS did_id, COUNT(*) c FROM posts WHERE reply_parent_uri IS NOT NULL GROUP BY 1) rep USING(did_id)
LEFT JOIN (SELECT author_did_id AS did_id, COUNT(*) c FROM posts WHERE quote_uri IS NOT NULL GROUP BY 1) qo USING(did_id)
LEFT JOIN (SELECT p.author_did_id AS did_id, COUNT(*) c
           FROM posts q JOIN posts p ON p.uri = q.quote_uri GROUP BY 1) qd USING(did_id);
"#,
    )
    .context("build actor_aggs")?;
    Ok(())
}

fn build_post_aggs(conn: &Connection) -> Result<()> {
    tracing::info!("building post_aggs");
    conn.execute_batch(
        r#"
CREATE TABLE post_aggs AS
SELECT
  p.uri,
  COALESCE(l.c, 0)  AS likes,
  COALESCE(r.c, 0)  AS reposts,
  COALESCE(rp.c, 0) AS replies,
  COALESCE(q.c, 0)  AS quotes
FROM posts p
LEFT JOIN (SELECT subject_uri AS uri, COUNT(*) c FROM likes GROUP BY 1) l USING(uri)
LEFT JOIN (SELECT subject_uri AS uri, COUNT(*) c FROM reposts GROUP BY 1) r USING(uri)
LEFT JOIN (SELECT reply_parent_uri AS uri, COUNT(*) c FROM posts WHERE reply_parent_uri IS NOT NULL GROUP BY 1) rp USING(uri)
LEFT JOIN (SELECT quote_uri AS uri, COUNT(*) c FROM posts WHERE quote_uri IS NOT NULL GROUP BY 1) q USING(uri);
"#,
    )
    .context("build post_aggs")?;
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
    let q = |table: &str| {
        format!(
            "SELECT CAST(SUM(CASE WHEN p.uri IS NULL THEN 1 ELSE 0 END) AS DOUBLE) /
                    GREATEST(COUNT(*), 1)
             FROM {t} l LEFT JOIN posts p ON p.uri = l.subject_uri",
            t = table,
        )
    };
    let l: f64 = conn
        .query_row(&q("likes"), [], |row| row.get(0))
        .context("orphan likes rate")?;
    let r: f64 = conn
        .query_row(&q("reposts"), [], |row| row.get(0))
        .context("orphan reposts rate")?;
    Ok((l, r))
}
