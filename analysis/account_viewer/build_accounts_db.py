"""Export a compact local `accounts.duckdb` for the account viewer.

The published snapshot duckdb (~57 GB) lives on R2. We don't want to download
it. Instead we ATTACH it read-only over httpfs and let DuckDB range-read only
the two tables we need:

  actors      (did_id, did, active)        -- the DID string per actor
  actor_aggs  (did_id, follows, ... )      -- the per-actor counts

We join them, derive an activity `category`, and materialize the result to a
small local duckdb the viewer queries instantly. Per-account profile/bio/join
data is NOT in the snapshot at all -- the viewer hydrates that live from the
bsky public API.

Run:
    .venv/bin/python analysis/account_viewer/build_accounts_db.py
    .venv/bin/python analysis/account_viewer/build_accounts_db.py --date 2026-05-11

The categorization is deliberately coarse and tunable -- a starting point for
eyeballing, not the final labeling scheme. Raw counts are preserved so you can
write arbitrary SQL against the columns in the viewer.
"""

from __future__ import annotations

import argparse
import os
import time

import duckdb

R2_BASE = "https://pub-5ef34deaa1e54c25a97cea1bcfbd6456.r2.dev/atproto-snapshot"
HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT = os.path.join(HERE, "accounts.duckdb")

# Activity category, evaluated in priority order (first match wins).
#   content      = posts + replies_out + reposts_out + quotes_out  (authored/shared)
#   The target cohort for this investigation is `ghost_lt10`.
CATEGORY_SQL = """
CASE
  WHEN content = 0 AND ag.likes_out = 0 AND ag.follows = 0
       THEN 'inert'              -- joined, did literally nothing, not even a follow
  WHEN content = 0 AND ag.likes_out = 0 AND ag.follows BETWEEN 1 AND 9
       THEN 'ghost_lt10'         -- joined, followed <10, no posts/likes  <-- the 40%
  WHEN content = 0 AND ag.likes_out = 0 AND ag.follows >= 10
       THEN 'ghost_followed'     -- followed people but never posted or liked
  WHEN content = 0 AND ag.likes_out > 0
       THEN 'lurker'             -- likes/consumes but never authored anything
  WHEN content BETWEEN 1 AND 4
       THEN 'dabbler'            -- a handful of posts then went quiet (mostly)
  WHEN content BETWEEN 5 AND 99
       THEN 'poster'
  ELSE 'active'                  -- >=100 authored items
END
"""

CONTENT_EXPR = "(ag.posts + ag.replies_out + ag.reposts_out + ag.quotes_out)"


def _retry(con, sql: str, label: str, attempts: int = 8) -> None:
    """Run a remote-scan statement, retrying on R2 throttling (HTTP 429)."""
    for i in range(1, attempts + 1):
        try:
            print(f"  copying {label} (attempt {i}/{attempts})...", flush=True)
            con.execute(sql)
            return
        except duckdb.HTTPException as e:
            if "429" not in str(e) or i == attempts:
                raise
            wait = min(60, 5 * i)
            print(f"    throttled: {e}; sleeping {wait}s", flush=True)
            time.sleep(wait)


def build(out_path: str, date: str) -> None:
    url = f"{R2_BASE}/snapshot/{date}/snapshot.duckdb"
    if os.path.exists(out_path):
        os.remove(out_path)

    con = duckdb.connect(out_path)
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET http_timeout = 120000;")  # ms, be patient with R2
    # The public r2.dev endpoint rate-limits (HTTP 429) under the burst of
    # range requests a parallel scan makes. Read single-threaded and back off
    # hard so DuckDB paces itself instead of hammering the bucket.
    con.execute("SET threads = 1;")
    con.execute("SET http_retries = 20;")
    con.execute("SET http_retry_wait_ms = 2000;")
    con.execute("SET http_retry_backoff = 2;")
    con.execute("SET http_keep_alive = true;")
    con.execute("SET enable_progress_bar = true;")
    print(f"attaching remote snapshot {date} (read-only over httpfs)...", flush=True)
    con.execute(f"ATTACH '{url}' AS rem (READ_ONLY);")

    n = con.execute("SELECT count(*) FROM rem.actor_aggs").fetchone()[0]
    print(f"remote actors: {n:,}. exporting -> {out_path}", flush=True)

    t0 = time.time()
    # Copy each remote table in its own scan so a throttle only costs us that
    # one table, then do the join + categorization locally (fast).
    _retry(con, "CREATE OR REPLACE TABLE _actors AS "
                "SELECT did_id, did, active FROM rem.actors", "actors")
    _retry(con, "CREATE OR REPLACE TABLE _aggs AS "
                "SELECT * FROM rem.actor_aggs", "actor_aggs")
    con.execute("DETACH rem;")

    con.execute(
        f"""
        CREATE TABLE accounts AS
        SELECT
            a.did_id,
            a.did,
            a.active,
            ag.follows,
            ag.followers,
            ag.blocks_out,
            ag.blocks_in,
            ag.posts,
            ag.likes_out,
            ag.likes_in,
            ag.reposts_out,
            ag.reposts_in,
            ag.replies_out,
            ag.quotes_out,
            ag.quoted_count,
            {CONTENT_EXPR} AS content,
            ({CONTENT_EXPR} + ag.likes_out) AS any_activity,
            ({CATEGORY_SQL.replace("content", CONTENT_EXPR)}) AS category
        FROM _actors a
        JOIN _aggs ag USING (did_id);
        """
    )
    con.execute("DROP TABLE _actors; DROP TABLE _aggs;")
    con.execute("CREATE INDEX idx_did ON accounts(did);")
    con.execute("CREATE INDEX idx_cat ON accounts(category);")
    con.execute("CREATE INDEX idx_did_id ON accounts(did_id);")

    con.execute(
        "CREATE TABLE meta AS SELECT ? AS snapshot_date, ? AS built_at",
        [date, time.strftime("%Y-%m-%d %H:%M:%S")],
    )

    dt = time.time() - t0
    print(f"join + write done in {dt:,.0f}s", flush=True)

    print("\ncategory breakdown:", flush=True)
    rows = con.execute(
        """
        SELECT category, count(*) n,
               round(100.0 * count(*) / sum(count(*)) OVER (), 1) pct
        FROM accounts GROUP BY category ORDER BY n DESC
        """
    ).fetchall()
    for cat, cnt, pct in rows:
        print(f"  {cat:16} {cnt:>12,}  {pct:>5}%", flush=True)
    con.close()
    sz = os.path.getsize(out_path) / 1e9
    print(f"\nwrote {out_path} ({sz:.2f} GB)", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="2026-05-11", help="snapshot date on R2")
    ap.add_argument("--out", default=DEFAULT_OUT)
    args = ap.parse_args()
    build(args.out, args.date)


if __name__ == "__main__":
    main()
