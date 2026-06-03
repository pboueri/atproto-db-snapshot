"""Add an estimated account-creation date to accounts.duckdb.

Account creation time is NOT in the snapshot (the `actors` table is only
did/did_id/active). The closest global proxy we can compute is the earliest
*plausible* activity timestamp per account. The full-history `follows` table is
the best single signal: people follow a handful of accounts during onboarding,
so an actor's earliest follow lands very close to signup. (likes/posts/reposts
are time-windowed in the snapshot, so their min is biased late and -- likes
being ~10B rows -- far too expensive to scan; we use follows only.)

This reads the remote 1.33B-row follows table over httpfs (single-threaded with
429 backoff, like build_accounts_db.py) and writes a `created_est` TIMESTAMP
column onto the local accounts table.

TID timestamps decoded from rkeys have bogus outliers (1970, far-future). We
clamp created_at to a plausible window in the aggregation so a single garbage
edge can't drag an actor's min to 1970.

Run:
    .venv/bin/python analysis/account_viewer/add_created_est.py
    .venv/bin/python analysis/account_viewer/add_created_est.py --date 2026-05-11

Accounts with no follow edge (fully-inert + like-only lurkers, ~16%) get NULL.
"""

from __future__ import annotations

import argparse
import os
import time

import duckdb

R2_BASE = "https://pub-5ef34deaa1e54c25a97cea1bcfbd6456.r2.dev/atproto-snapshot"
HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB = os.path.join(HERE, "accounts.duckdb")

# Plausible signup window: did:plc accounts predate the public launch slightly;
# nothing can be created after the snapshot. Anything outside is a bogus TID.
LO = "2022-11-01"


def _retry(con, sql: str, label: str, attempts: int = 8):
    for i in range(1, attempts + 1):
        try:
            print(f"  {label} (attempt {i}/{attempts})...", flush=True)
            return con.execute(sql)
        except duckdb.HTTPException as e:
            if "429" not in str(e) or i == attempts:
                raise
            wait = min(60, 5 * i)
            print(f"    throttled: {e}; sleeping {wait}s", flush=True)
            time.sleep(wait)


def _side_ready(side: str) -> bool:
    """True iff a side file already holds a completed `first_act` aggregation."""
    if not os.path.exists(side):
        return False
    try:
        c = duckdb.connect(side, read_only=True)
        n = c.execute("SELECT count(*) FROM first_act").fetchone()[0]
        c.close()
        return n > 0
    except Exception:
        return False


def _scan_into_side(url: str, side: str, date: str, hi: str) -> None:
    """Phase 1: the heavy remote scan -> standalone side file (no lock on the
    accounts db, so the viewer stays usable for the hours this takes)."""
    if os.path.exists(side):
        os.remove(side)
    scan = duckdb.connect(side)
    scan.execute("INSTALL httpfs; LOAD httpfs;")
    scan.execute("SET threads = 1;")           # single-threaded -> avoids r2.dev 429s
    scan.execute("SET http_retries = 20;")
    scan.execute("SET http_retry_wait_ms = 2000;")
    scan.execute("SET http_retry_backoff = 2;")
    scan.execute("SET http_keep_alive = true;")
    scan.execute("SET enable_progress_bar = true;")
    print(f"attaching remote snapshot {date} ...", flush=True)
    scan.execute(f"ATTACH '{url}' AS rem (READ_ONLY);")
    t0 = time.time()
    _retry(
        scan,
        f"""
        CREATE OR REPLACE TABLE first_act AS
        SELECT src_did_id AS did_id, MIN(created_at) AS created_est
        FROM rem.follows
        WHERE created_at BETWEEN TIMESTAMP '{LO}' AND (TIMESTAMP '{hi}' + INTERVAL 1 DAY)
        GROUP BY src_did_id
        """,
        "aggregating earliest follow per actor (follows ~1.33B rows)",
    )
    scan.execute("DETACH rem;")
    scan.close()
    print(f"aggregation done in {time.time() - t0:,.0f}s", flush=True)


def run(db_path: str, date: str) -> None:
    url = f"{R2_BASE}/snapshot/{date}/snapshot.duckdb"
    hi = date  # snapshot date is the upper bound (no later signups exist)
    side = db_path + ".created_est_tmp.duckdb"  # standalone scratch db

    # --- phase 1: heavy remote scan into a SIDE file ----------------------
    # accounts.duckdb stays untouched/unlocked here, so the viewer keeps
    # working for the ~hours this takes. We only touch it in phase 2.
    # If a completed side file already exists (e.g. phase 2 failed to get the
    # lock last time), reuse it instead of re-running the multi-hour scan.
    if _side_ready(side):
        print(f"reusing existing aggregation in {side}; skipping remote scan",
              flush=True)
    else:
        _scan_into_side(url, side, date, hi)

    # --- phase 2: fast merge into accounts.duckdb (brief write lock) -------
    # Needs exclusive access; a running viewer server holds a read-only lock.
    # Retry for a while so the user can stop the server when they see this.
    con = None
    for i in range(120):  # ~20 min of patience
        try:
            con = duckdb.connect(db_path)
            break
        except duckdb.IOException as e:
            if "lock" not in str(e).lower():
                raise
            if i == 0:
                print("\naccounts.duckdb is locked (viewer server running?). "
                      "Stop the server so I can merge created_est in; retrying…",
                      flush=True)
            time.sleep(10)
    if con is None:
        raise SystemExit(
            f"could not lock {db_path} to merge. Stop the viewer server and run:\n"
            f"  .venv/bin/python analysis/account_viewer/add_created_est.py "
            f"(side file {side} is kept)"
        )
    con.execute(f"ATTACH '{side}' AS side (READ_ONLY)")
    cols = [r[1] for r in con.execute("PRAGMA table_info('accounts')").fetchall()]
    if "created_est" not in cols:
        con.execute("ALTER TABLE accounts ADD COLUMN created_est TIMESTAMP")
    con.execute(
        "UPDATE accounts SET created_est = f.created_est "
        "FROM side.first_act f WHERE accounts.did_id = f.did_id"
    )
    con.execute("DETACH side")
    con.execute("CREATE INDEX IF NOT EXISTS idx_created ON accounts(created_est)")
    os.remove(side)

    cov = con.execute(
        "SELECT count(*) FILTER (WHERE created_est IS NOT NULL), count(*), "
        "min(created_est), max(created_est) FROM accounts"
    ).fetchone()
    print(
        f"\ncreated_est set for {cov[0]:,} / {cov[1]:,} "
        f"({100*cov[0]/cov[1]:.1f}%); range {cov[2]} .. {cov[3]}",
        flush=True,
    )
    # Monthly histogram so we can eyeball signup waves immediately.
    print("\nsignups by month (estimated):", flush=True)
    for m, n in con.execute(
        "SELECT strftime(created_est, '%Y-%m') ym, count(*) n FROM accounts "
        "WHERE created_est IS NOT NULL GROUP BY ym ORDER BY ym"
    ).fetchall():
        print(f"  {m}  {n:>10,}  {'#' * min(60, n // 80000)}", flush=True)
    con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="2026-05-11")
    ap.add_argument("--db", default=DEFAULT_DB)
    args = ap.parse_args()
    run(args.db, args.date)


if __name__ == "__main__":
    main()
