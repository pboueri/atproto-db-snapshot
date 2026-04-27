use anyhow::{anyhow, bail, Context, Result};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

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

    let sql = render_sql(&raw_dir, &cfg.memory_limit)?;
    tracing::info!(duckdb = %duckdb_path.display(), "running hydrate via duckdb cli");
    run_duckdb_sql(&duckdb_path, &sql)?;

    let counts = run_counts(&duckdb_path)?;
    let (orphan_like, orphan_repost) = run_orphan_rates(&duckdb_path)?;

    Ok(HydrateOutcome {
        duckdb_path,
        row_counts: counts,
        orphan_like_rate: orphan_like,
        orphan_repost_rate: orphan_repost,
    })
}

fn render_sql(raw_dir: &Path, memory_limit: &str) -> Result<String> {
    let raw = raw_dir
        .canonicalize()
        .with_context(|| format!("canonicalize {}", raw_dir.display()))?;
    let raw = raw.to_string_lossy().to_string();

    let mut sql = String::new();
    sql.push_str(&format!("SET memory_limit='{}';\n", memory_limit));
    sql.push_str("SET preserve_insertion_order=false;\n");
    sql.push_str(&format!(
        "SET temp_directory='{}/duckdb_tmp';\n",
        raw.trim_end_matches('/')
    ));
    sql.push_str("PRAGMA enable_progress_bar;\n");

    let load = |table: &str, file: &str| {
        format!(
            "CREATE TABLE {table} AS SELECT * FROM read_parquet('{raw}/{file}');\n",
            table = table,
            file = file,
            raw = raw,
        )
    };

    sql.push_str(&load("actors", "actors.parquet"));
    sql.push_str(&load("follows", "follows.parquet"));
    sql.push_str(&load("blocks", "blocks.parquet"));
    sql.push_str(&load("likes", "likes.parquet"));
    sql.push_str(&load("reposts", "reposts.parquet"));
    sql.push_str(&load("post_media", "post_media.parquet"));
    sql.push_str(&load("posts_from_records", "posts_from_records.parquet"));
    sql.push_str(&load("posts_from_targets", "posts_from_targets.parquet"));

    sql.push_str(
        r#"
CREATE TABLE posts AS
WITH unioned AS (
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
), ranked AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY uri ORDER BY (source = 'record') DESC
  ) AS rn FROM unioned
)
SELECT uri, author_did_id, rkey, created_at,
       reply_root_uri, reply_parent_uri, quote_uri, source
FROM ranked WHERE rn = 1;

CREATE TABLE actor_aggs AS
SELECT
  a.did_id,
  COALESCE(f_out.c, 0)   AS follows,
  COALESCE(f_in.c, 0)    AS followers,
  COALESCE(b_out.c, 0)   AS blocks_out,
  COALESCE(b_in.c, 0)    AS blocks_in,
  COALESCE(p.c, 0)       AS posts,
  COALESCE(l_out.c, 0)   AS likes_out,
  COALESCE(l_in.c, 0)    AS likes_in,
  COALESCE(r_out.c, 0)   AS reposts_out,
  COALESCE(r_in.c, 0)    AS reposts_in,
  COALESCE(rep.c, 0)     AS replies_out,
  COALESCE(qo.c, 0)      AS quotes_out,
  COALESCE(qd.c, 0)      AS quoted_count
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
    );
    Ok(sql)
}

fn run_duckdb_sql(db_path: &Path, sql: &str) -> Result<()> {
    let mut child = Command::new("duckdb")
        .arg(db_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .context("spawn duckdb cli")?;
    {
        use std::io::Write;
        let stdin = child.stdin.as_mut().ok_or_else(|| anyhow!("no stdin"))?;
        stdin.write_all(sql.as_bytes())?;
    }
    let status = child.wait()?;
    if !status.success() {
        bail!("duckdb cli exited with {}", status);
    }
    Ok(())
}

fn run_duckdb_query(db_path: &Path, sql: &str) -> Result<String> {
    let out = Command::new("duckdb")
        .arg(db_path)
        .arg("-csv")
        .arg("-noheader")
        .arg("-c")
        .arg(sql)
        .output()
        .context("invoke duckdb -c")?;
    if !out.status.success() {
        bail!(
            "duckdb -c failed: {}\n{}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

fn run_counts(db_path: &Path) -> Result<Vec<(String, u64)>> {
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
        let s = run_duckdb_query(db_path, &format!("SELECT COUNT(*) FROM {t}"))?;
        let n: u64 = s.trim().parse().unwrap_or(0);
        out.push((t.to_string(), n));
    }
    Ok(out)
}

fn run_orphan_rates(db_path: &Path) -> Result<(f64, f64)> {
    let q = |table: &str| {
        format!(
            "SELECT CAST(SUM(CASE WHEN p.uri IS NULL THEN 1 ELSE 0 END) AS DOUBLE) /
                    GREATEST(COUNT(*), 1)
             FROM {t} l LEFT JOIN posts p ON p.uri = l.subject_uri",
            t = table,
        )
    };
    let l = run_duckdb_query(db_path, &q("likes"))?;
    let r = run_duckdb_query(db_path, &q("reposts"))?;
    Ok((
        l.trim().parse().unwrap_or(0.0),
        r.trim().parse().unwrap_or(0.0),
    ))
}
