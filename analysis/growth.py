"""Duolingo-style user-growth state-machine analysis.

Each actor on Bluesky is classified into one of five states at every
hour of the analysis window. Transitions between states are tallied and
plotted to answer the question: "is Bluesky growing or shrinking, and
if it's shrinking, is it because new users churn fast or because the
old guard is leaving?"

States
------
new       First-ever record observed less than `at_risk_hours` ago
          (default 48h) and the actor has not done anything since.
active    Any activity within the last `at_risk_hours`.
super     `active` AND ≥ `super_threshold` actions in the trailing
          `super_window_hours` (default: 50 actions / 7 days).
at_risk   No activity for ≥ `at_risk_hours` but < `churn_hours`.
churned   No activity for ≥ `churn_hours` (default 14 days).

Initial conditions
------------------
Actors whose first-ever event predates `existing_baseline_date`
(default 2025-01-01) are treated as already-active at that date — we
don't try to reconstruct what they were doing before then. Actors whose
first event is on/after the baseline are tracked from that event.

Data source
-----------
Reads the full-history staging parquets at `raw_dir` (typically
`/vol-out/var/raw/<snapshot_date>/` on Modal). The hydrated
`snapshot.duckdb` only carries the last 90 days of likes/reposts/posts,
so the parquets are required for a year-scale analysis.

Memory / runtime
----------------
Pre-aggregates events to (did_id, hour) inside DuckDB, then streams the
result sorted by did_id and processes each actor independently with a
small per-actor state machine. Memory is O(one actor's events + a 2D
population delta array of shape (n_states, n_hours) ~ 200KB). Runtime
is dominated by the per-actor Python loop (~1µs/event after Arrow
unmarshaling).

Public entrypoint: `run(con, snapshot_date, raw_dir=None, ...)`.
"""

from __future__ import annotations

import concurrent.futures as _futures
import multiprocessing as _mp
import os
import shutil
import threading
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone

import numpy as np

try:
    from numba import njit
    _HAVE_NUMBA = True
except ImportError:  # numba unavailable (e.g. py3.14 dev box) — fall back to vec
    _HAVE_NUMBA = False

    def njit(*args, **kwargs):  # no-op decorator so the module still imports
        def _wrap(fn):
            return fn
        if args and callable(args[0]) and not kwargs:
            return args[0]
        return _wrap

from .common import (
    BRAND, SHARED_CSS, built_at_utc, fig_html, fmt_int,
    install_template, plotlyjs_inline, timed_query,
)

# State indices. Order matters for stacked-area plotting (we want
# "good" states on top, "bad" on the bottom so the eye reads top-down
# as the healthy population).
STATE_NEW, STATE_ACTIVE, STATE_SUPER, STATE_AT_RISK, STATE_CHURNED = range(5)
N_STATES = 5
STATE_NAMES = ["new", "active", "super", "at_risk", "churned"]

# Plot colors per state. Keep super/active in brand, at_risk amber,
# churned red — consistent with the attrition analysis.
STATE_COLORS = {
    "super":    "#7c3aed",
    "active":   BRAND,
    "new":      "#16a34a",
    "at_risk":  "#f59e0b",
    "churned":  "#ef4444",
}

# Sources we union as "actor activity events". Each tuple is
#   (parquet_filename_without_dir, actor_column, table_name_if_raw_dir_none)
ACTIVITY_SOURCES = [
    ("likes.parquet",              "actor_did_id",   "likes"),
    ("reposts.parquet",            "actor_did_id",   "reposts"),
    ("follows.parquet",            "src_did_id",     "follows"),
    ("posts_from_records.parquet", "author_did_id",  "posts"),
]

# Plausibility window for created_at. Anything before 2022 is decoded
# garbage from a non-TID rkey; anything after the snapshot is a forged
# future timestamp.
_PLAUSIBLE_LO = "2022-01-01"


def run(
    con,
    snapshot_date: str,
    raw_dir: str | None = None,
    *,
    at_risk_hours: int = 48,
    churn_days: int = 14,
    super_threshold: int = 50,
    super_window_hours: int = 7 * 24,
    existing_baseline_date: str = "2025-01-01",
    lookback_days: int | None = None,
    n_workers: int | None = None,
    chunk_size: int = 4_000,
    log: bool = True,
) -> tuple[bytes, dict]:
    """Build the growth-model HTML + JSON sidecar.

    Args:
      con: DuckDB connection. If `raw_dir` is set we read full-history
        parquets directly; otherwise we read the in-DB activity tables
        (used by tests against synthetic snapshots).
      snapshot_date: ISO date for `created_at` upper bound and report label.
      raw_dir: directory containing the staging parquets, or None to
        query the connected DB's tables.
      at_risk_hours: idle threshold to leave "active" (also the length
        of the "new" window).
      churn_days: idle threshold to leave "at_risk".
      super_threshold: actions in `super_window_hours` to be "super".
      super_window_hours: trailing window for super eligibility.
      existing_baseline_date: actors whose first event predates this
        are bucketed as "existing" and seeded `active` on that date.
      lookback_days: if set, ignore events older than this many days
        before `snapshot_date`. None ⇒ full history.
      n_workers: number of worker processes for the state-machine
        loop. None ⇒ os.cpu_count() (or 1 if undetectable). Set to 1
        to disable parallelism (used by tests).
      chunk_size: actors per ProcessPoolExecutor task. Larger amortizes
        pickle overhead; smaller smooths load. 4000 ≈ 100ms of work.
      log: print progress lines.
    """
    churn_hours = churn_days * 24

    snap_ts = f"{snapshot_date} 23:59:59"
    snap_h = _to_hour_index(datetime.fromisoformat(f"{snapshot_date}T23:59:59"))
    baseline_h = _to_hour_index(
        datetime.fromisoformat(f"{existing_baseline_date}T00:00:00")
    )
    if lookback_days is not None:
        lookback_lo_ts = (
            datetime.fromisoformat(f"{snapshot_date}T00:00:00")
            - timedelta(days=lookback_days)
        ).strftime("%Y-%m-%d %H:%M:%S")
    else:
        lookback_lo_ts = f"{_PLAUSIBLE_LO} 00:00:00"

    if log:
        print(
            f"=== growth: snap={snapshot_date} baseline={existing_baseline_date} "
            f"at_risk={at_risk_hours}h churn={churn_days}d "
            f"super={super_threshold}/{super_window_hours}h "
            f"lookback={'all' if lookback_days is None else f'{lookback_days}d'} ===",
            flush=True,
        )

    _materialize_per_hour(
        con, raw_dir=raw_dir,
        snap_ts=snap_ts,
        plausible_lo_ts=f"{_PLAUSIBLE_LO} 00:00:00",
        lookback_lo_ts=lookback_lo_ts,
        log=log,
    )

    # Default path selection:
    #   numba (-2) if the JIT is importable — one compiled pass over all
    #     actors, handles super natively at C speed (no vec/super split).
    #   else vec (-1) — fully vectorized 4-state base + per-actor super
    #     fallback; ~5-6x over the serial loop on the bench.
    # Set n_workers=1 for the serial reference, >1 for ProcessPoolExecutor.
    if n_workers is None:
        n_workers = -2 if _HAVE_NUMBA else -1

    if log:
        path_label = (
            "numba" if n_workers == -2
            else "vec" if n_workers == -1
            else "serial" if n_workers == 1
            else f"{n_workers}-worker"
        )
        print(
            f"=== streaming per-actor state machine "
            f"(path={path_label}, chunk_size={chunk_size}) ===",
            flush=True,
        )

    n_hours = snap_h + 1  # 0..snap_h inclusive
    (pop_delta, transitions, cohort_outcomes, cohort_size,
     churn_buckets, n_actors, n_events) = _run_state_machine(
        con,
        at_risk_h=at_risk_hours,
        churn_h=churn_hours,
        super_h=super_window_hours,
        super_thr=super_threshold,
        baseline_h=baseline_h,
        end_h=snap_h,
        n_workers=n_workers,
        chunk_size=chunk_size,
        log=log,
    )

    # Cumulative-sum the deltas to get population per state per hour.
    populations = np.cumsum(pop_delta, axis=1)[:, :n_hours]

    # Daily aggregation for plotting (hourly is too dense for a year-long chart).
    populations_daily, transitions_daily, day_dates = _aggregate_daily(
        populations, transitions, snap_h, n_hours,
    )

    # Weekly aggregation drives the hero chart + regime classifier.
    weekly = _aggregate_weekly(
        populations, transitions, churn_buckets, snap_h,
    )

    # Markov steady-state from the last `markov_window_days` of activity.
    markov_window_days = 90
    markov = _compute_markov_steady_state(
        populations, transitions, snap_h,
        window_hours=markov_window_days * 24,
    )

    install_template()
    html, sidecar, hero_png = _render(
        snapshot_date=snapshot_date,
        at_risk_hours=at_risk_hours,
        churn_days=churn_days,
        super_threshold=super_threshold,
        super_window_hours=super_window_hours,
        existing_baseline_date=existing_baseline_date,
        populations_daily=populations_daily,
        transitions_daily=transitions_daily,
        day_dates=day_dates,
        weekly=weekly,
        markov=markov,
        markov_window_days=markov_window_days,
        cohort_outcomes=cohort_outcomes,
        cohort_size=cohort_size,
        n_actors=n_actors,
        n_events=n_events,
    )
    return html, sidecar, hero_png


# ---------------------------------------------------------------------------
# Resource monitor: visibility into RSS + /tmp during long DuckDB queries
# ---------------------------------------------------------------------------


def _resource_snapshot(tmp_dir: str = "/tmp") -> str:
    """One-line summary of process RSS + temp-dir disk usage.

    Used by the in-DuckDB-query monitor thread so long-running
    aggregations don't look hung — when a CREATE TABLE silently
    GROUP-BYs a year of events the only signal we get is RSS climbing
    + tmp filling up. Reads /proc/self/status on Linux (the only place
    we run this in anger); falls back to zero on macOS/anywhere it
    isn't available.
    """
    try:
        with open("/proc/self/status") as f:
            rss_gib = 0.0
            for line in f:
                if line.startswith("VmRSS:"):
                    rss_gib = int(line.split()[1]) / (1024 * 1024)
                    break
    except (FileNotFoundError, PermissionError, OSError):
        rss_gib = 0.0
    try:
        tot, used, free = shutil.disk_usage(tmp_dir)
        disk = f"{tmp_dir} used={used / 1e9:.0f}GB free={free / 1e9:.0f}GB"
    except OSError:
        disk = f"{tmp_dir} <unavailable>"
    return f"rss={rss_gib:.1f}GiB {disk}"


def _start_resource_monitor(
    label: str, *, interval_sec: float = 30.0, tmp_dir: str = "/tmp",
) -> threading.Event:
    """Spawn a daemon thread that prints `_resource_snapshot()` every N sec.

    Returns a stop event; caller sets it to terminate the loop.
    """
    stop = threading.Event()

    def _loop():
        t0 = time.time()
        # Immediate snapshot so the start state is visible alongside
        # the operation's begin log line.
        print(
            f"  [res {label} t+0s] {_resource_snapshot(tmp_dir)}",
            flush=True,
        )
        while not stop.wait(interval_sec):
            dt = int(time.time() - t0)
            print(
                f"  [res {label} t+{dt}s] {_resource_snapshot(tmp_dir)}",
                flush=True,
            )

    th = threading.Thread(target=_loop, daemon=True, name=f"resmon-{label}")
    th.start()
    return stop


# ---------------------------------------------------------------------------
# DuckDB pre-aggregation
# ---------------------------------------------------------------------------


def _materialize_per_hour(
    con,
    *,
    raw_dir: str | None,
    snap_ts: str,
    plausible_lo_ts: str,
    lookback_lo_ts: str,
    log: bool,
) -> None:
    """Build the `per_hour_sorted` and per-actor metadata tables.

    Schema:
      per_hour_sorted(did_id BIGINT, hour_idx BIGINT, n_actions INT,
                      cohort_ym INT)
        — one row per actor per hour they were active, ORDER BY did_id,
          hour_idx so the streaming consumer can group cheaply.

    `cohort_ym` is YYYY*100 + MM of the actor's first observed event,
    encoded as INT so it fits in an Arrow column. NULL hour_idx rows
    are filtered out upstream (created_at NULL).
    """
    if raw_dir is not None:
        # Read parquets directly. Inline read_parquet() is fine —
        # DuckDB pushes the predicate down.
        parts = []
        for fname, actor_col, _tbl in ACTIVITY_SOURCES:
            parts.append(
                f"SELECT {actor_col} AS did_id, created_at "
                f"FROM read_parquet('{raw_dir}/{fname}') "
                f"WHERE created_at IS NOT NULL "
                f"  AND created_at BETWEEN TIMESTAMP '{plausible_lo_ts}' "
                f"                     AND TIMESTAMP '{snap_ts}'"
            )
        events_sql = "\n  UNION ALL\n  ".join(parts)
    else:
        # In-DB table mode (tests). For posts we want `author_did_id`.
        parts = []
        for _fname, actor_col, tbl in ACTIVITY_SOURCES:
            parts.append(
                f"SELECT {actor_col} AS did_id, created_at "
                f"FROM {tbl} "
                f"WHERE created_at IS NOT NULL "
                f"  AND created_at BETWEEN TIMESTAMP '{plausible_lo_ts}' "
                f"                     AND TIMESTAMP '{snap_ts}'"
            )
        events_sql = "\n  UNION ALL\n  ".join(parts)

    # Two-step build, no intermediate row-level materialization.
    #
    # Earlier this function created a 14.5-billion-row `all_events`
    # temp table holding raw (did_id, created_at, hour_idx) tuples and
    # then re-grouped it twice (once for `actor_cohort`, once for
    # `per_hour_sorted`). At Modal scale that materialization filled
    # >350 GiB of temp_directory and the subsequent GROUP BY +
    # ORDER BY tipped the worker over the cgroup limit ("Worker
    # disappeared").
    #
    # Now we go straight from the parquet/table reads into a
    # `(did_id, hour_idx)` aggregate (`per_hour`), retaining
    # `MIN(created_at)` per group. The second step adds `cohort_ym`
    # via a window function — `MIN(hour_min_created) OVER (PARTITION
    # BY did_id)` is the same value as the old `actor_cohort` MIN-by
    # group — and sorts to produce the final `per_hour_sorted`. The
    # `per_hour` aggregate is ~10–20x smaller than `all_events`, which
    # makes the sort/spill fit comfortably.
    if log:
        try:
            cpu = os.cpu_count() or 0
            mem_gib = (os.sysconf("SC_PHYS_PAGES")
                       * os.sysconf("SC_PAGE_SIZE")) / (1024 ** 3)
        except (AttributeError, ValueError, OSError):
            cpu, mem_gib = 0, 0.0
        print(
            f"  [env] cpu_count={cpu} host_mem={mem_gib:.0f}GiB  "
            f"start: {_resource_snapshot('/tmp')}",
            flush=True,
        )

    t0 = time.time()
    stop = _start_resource_monitor("per_hour") if log else None
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TEMPORARY TABLE per_hour AS
            SELECT did_id,
                   DATEDIFF('hour',
                            TIMESTAMP '1970-01-01 00:00:00',
                            DATE_TRUNC('hour', created_at))::BIGINT AS hour_idx,
                   COUNT(*)::INT AS n_actions,
                   MIN(created_at) AS hour_min_created
            FROM (
              {events_sql}
            )
            WHERE created_at >= TIMESTAMP '{lookback_lo_ts}'
            GROUP BY did_id, hour_idx
            """
        )
    finally:
        if stop is not None:
            stop.set()
    if log:
        n = con.execute("SELECT COUNT(*) FROM per_hour").fetchone()[0]
        print(
            f"  ({time.time() - t0:.1f}s) per_hour rows = {n:,}  "
            f"end: {_resource_snapshot('/tmp')}",
            flush=True,
        )

    t0 = time.time()
    stop = _start_resource_monitor("per_hour_sorted") if log else None
    try:
        con.execute(
            """
            CREATE OR REPLACE TEMPORARY TABLE per_hour_sorted AS
            SELECT did_id,
                   hour_idx,
                   n_actions,
                   (EXTRACT(year FROM MIN(hour_min_created)
                                      OVER (PARTITION BY did_id)) * 100
                    + EXTRACT(month FROM MIN(hour_min_created)
                                       OVER (PARTITION BY did_id)))::INT AS cohort_ym
            FROM per_hour
            ORDER BY did_id, hour_idx
            """
        )
    finally:
        if stop is not None:
            stop.set()
    if log:
        n = con.execute("SELECT COUNT(*) FROM per_hour_sorted").fetchone()[0]
        print(
            f"  ({time.time() - t0:.1f}s) per_hour_sorted rows = {n:,}  "
            f"end: {_resource_snapshot('/tmp')}",
            flush=True,
        )
        # Drop the now-redundant intermediate to free /tmp before the
        # state-machine load — per_hour_sorted has everything we need.
        con.execute("DROP TABLE per_hour")
        print(
            f"  dropped per_hour; {_resource_snapshot('/tmp')}",
            flush=True,
        )


# ---------------------------------------------------------------------------
# Streaming + per-actor state machine
# ---------------------------------------------------------------------------


def _iter_actor_groups(con, *, log: bool, batch_rows: int = 1_000_000):
    """Yield (did_id, hours_np, counts_np, cohort_ym) for each actor.

    Reads `per_hour_sorted` via Arrow record batches and slices on
    did_id boundaries. A single actor's rows may straddle two batches
    — we hold the trailing partial group as `leftover` until the next
    batch resolves it.
    """
    # `fetch_record_batch` returns a pyarrow.RecordBatchReader that
    # streams batches lazily — large queries never materialize in RAM.
    reader = con.execute(
        "SELECT did_id, hour_idx, n_actions, cohort_ym FROM per_hour_sorted"
    ).fetch_record_batch(batch_rows)

    leftover = None  # (did_id, hours_list, counts_list, cohort_ym)
    for batch in reader:
        did = batch.column("did_id").to_numpy(zero_copy_only=False)
        hr = batch.column("hour_idx").to_numpy(zero_copy_only=False)
        nn = batch.column("n_actions").to_numpy(zero_copy_only=False)
        co = batch.column("cohort_ym").to_numpy(zero_copy_only=False)
        if len(did) == 0:
            continue

        # Find did_id boundaries in this batch.
        change = np.where(np.diff(did) != 0)[0] + 1
        bnds = np.concatenate(([0], change, [len(did)]))

        for i in range(len(bnds) - 1):
            s, e = int(bnds[i]), int(bnds[i + 1])
            this_did = int(did[s])
            this_hr = hr[s:e]
            this_nn = nn[s:e]
            this_co = int(co[s])

            if leftover is not None and leftover[0] == this_did:
                # Continuation of same actor across the batch boundary.
                this_hr = np.concatenate([leftover[1], this_hr])
                this_nn = np.concatenate([leftover[2], this_nn])
                leftover = None

            is_last_in_batch = (i == len(bnds) - 2)
            if is_last_in_batch:
                # Buffer it — the next batch may keep adding to this actor.
                leftover = (this_did, this_hr, this_nn, this_co)
            else:
                if leftover is not None:
                    # Different did_id ended a stretched-out leftover.
                    yield (*leftover,)
                    leftover = None
                yield this_did, this_hr, this_nn, this_co

    if leftover is not None:
        yield (*leftover,)


# ---------------------------------------------------------------------------
# Parallel dispatch: chunked ProcessPoolExecutor over actor groups
# ---------------------------------------------------------------------------


def _iter_actor_chunks(con, *, chunk_size: int, baseline_h: int, log: bool):
    """Yield lists of (hours_i64, counts_i64, cohort_ym, is_existing) tuples.

    Wraps `_iter_actor_groups` and accumulates `chunk_size` actors per
    yield. The numpy arrays are pre-cast to int64 here so workers don't
    redo the cast inside `process_actor`.
    """
    chunk: list = []
    for did_id, hours, counts, cohort_ym in _iter_actor_groups(con, log=log):
        if hours.dtype != np.int64:
            hours = hours.astype(np.int64)
        if counts.dtype != np.int64:
            counts = counts.astype(np.int64)
        is_existing = bool(hours[0] < baseline_h)
        chunk.append((hours, counts, int(cohort_ym), is_existing))
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _process_chunk(chunk: list, params: dict) -> tuple:
    """Worker entrypoint: run `process_actor` over `chunk`, return partials.

    Returns (pop_delta, transitions, cohort_outcomes, cohort_size,
    churn_buckets). All counters are returned as plain dicts to keep
    pickle small (no defaultdict factories to serialize).
    """
    pop_delta = np.zeros((N_STATES, params["n_hours_plus_1"]), dtype=np.int64)
    transitions: Counter = Counter()
    cohort_outcomes: defaultdict = defaultdict(Counter)
    cohort_size: Counter = Counter()
    churn_buckets: Counter = Counter()

    at_risk_h = params["at_risk_h"]
    churn_h = params["churn_h"]
    super_h = params["super_h"]
    super_thr = params["super_thr"]
    baseline_h = params["baseline_h"]
    end_h = params["end_h"]

    for hours, counts, cohort_ym, is_existing in chunk:
        process_actor(
            hours, counts,
            at_risk_h=at_risk_h,
            churn_h=churn_h,
            super_h=super_h,
            super_thr=super_thr,
            baseline_h=baseline_h,
            end_h=end_h,
            is_existing=is_existing,
            pop_delta=pop_delta,
            transitions=transitions,
            cohort_outcomes=cohort_outcomes,
            cohort_size=cohort_size,
            cohort_ym=cohort_ym,
            churn_buckets=churn_buckets,
        )

    # Convert nested defaultdict→dict so the result pickles without
    # dragging the factory and is faster to serialize.
    outcomes_plain = {k: dict(v) for k, v in cohort_outcomes.items()}
    return (pop_delta, dict(transitions), outcomes_plain,
            dict(cohort_size), dict(churn_buckets))


def _run_state_machine(
    con, *,
    at_risk_h: int,
    churn_h: int,
    super_h: int,
    super_thr: int,
    baseline_h: int,
    end_h: int,
    n_workers: int = 1,
    chunk_size: int = 4_000,
    log: bool = True,
) -> tuple:
    """Drive the per-actor state machine; return accumulated counters.

    Returns (pop_delta, transitions, cohort_outcomes, cohort_size,
    churn_buckets, n_actors, n_events).

    `n_workers=1` runs in-process (no fork, no pickling — best for
    tests and small inputs). `n_workers>1` farms chunks of actors out
    to a ProcessPoolExecutor with bounded backpressure (queue depth
    capped at 2x worker count) so memory stays flat.
    """
    n_hours_plus_1 = end_h + 2
    pop_delta = np.zeros((N_STATES, n_hours_plus_1), dtype=np.int64)
    transitions: Counter = Counter()
    cohort_outcomes: defaultdict = defaultdict(Counter)
    cohort_size: Counter = Counter()
    churn_buckets: Counter = Counter()

    n_actors = 0
    n_events = 0
    t0 = time.time()

    if n_workers == -2:
        # Sentinel: Numba-compiled single-pass kernel over all actors.
        return _run_state_machine_numba(
            con,
            at_risk_h=at_risk_h, churn_h=churn_h,
            super_h=super_h, super_thr=super_thr,
            baseline_h=baseline_h, end_h=end_h,
            log=log,
        )

    if n_workers == -1:
        # Sentinel: take the fully vectorized numpy path. Falls back to
        # per-actor process_actor for super-eligible actors only.
        return _run_state_machine_vec(
            con,
            at_risk_h=at_risk_h, churn_h=churn_h,
            super_h=super_h, super_thr=super_thr,
            baseline_h=baseline_h, end_h=end_h,
            log=log,
        )

    if n_workers <= 1:
        for chunk in _iter_actor_chunks(
            con, chunk_size=chunk_size, baseline_h=baseline_h, log=log,
        ):
            for hours, counts, cohort_ym, is_existing in chunk:
                process_actor(
                    hours, counts,
                    at_risk_h=at_risk_h, churn_h=churn_h,
                    super_h=super_h, super_thr=super_thr,
                    baseline_h=baseline_h, end_h=end_h,
                    is_existing=is_existing,
                    pop_delta=pop_delta,
                    transitions=transitions,
                    cohort_outcomes=cohort_outcomes,
                    cohort_size=cohort_size,
                    cohort_ym=cohort_ym,
                    churn_buckets=churn_buckets,
                )
                n_actors += 1
                n_events += len(hours)
                if log and n_actors % 250_000 == 0:
                    dt = time.time() - t0
                    print(
                        f"  {n_actors:>10,} actors  {n_events:>13,} events  "
                        f"({n_actors / max(dt, 0.001):,.0f} actor/s)",
                        flush=True,
                    )
        if log:
            dt = time.time() - t0
            print(
                f"  done: {n_actors:,} actors, {n_events:,} events in {dt:.1f}s",
                flush=True,
            )
        return (pop_delta, transitions, cohort_outcomes, cohort_size,
                churn_buckets, n_actors, n_events)

    # ---- parallel path ------------------------------------------------------
    params = {
        "at_risk_h": at_risk_h, "churn_h": churn_h,
        "super_h": super_h, "super_thr": super_thr,
        "baseline_h": baseline_h, "end_h": end_h,
        "n_hours_plus_1": n_hours_plus_1,
    }

    # Prefer fork on POSIX — avoids re-importing the world per worker.
    # macOS forbids fork after using certain libs (Accelerate, ObjC) but
    # this module's imports (numpy, duckdb) are fork-safe pre-pool.
    ctx_name = "fork" if "fork" in _mp.get_all_start_methods() else "spawn"
    ctx = _mp.get_context(ctx_name)

    if log:
        print(
            f"  parallel: {n_workers} workers, "
            f"chunk_size={chunk_size}, start_method={ctx_name}",
            flush=True,
        )

    def _merge(result):
        nonlocal n_actors, n_events
        p_pop, p_trans, p_outcomes, p_size, p_churn = result
        pop_delta.__iadd__(p_pop)
        transitions.update(p_trans)
        for k, v in p_outcomes.items():
            cohort_outcomes[k].update(v)
        cohort_size.update(p_size)
        churn_buckets.update(p_churn)
        chunk_actors = sum(p_size.values())
        n_actors += chunk_actors

    chunks_iter = _iter_actor_chunks(
        con, chunk_size=chunk_size, baseline_h=baseline_h, log=log,
    )
    max_inflight = max(2, n_workers * 2)
    pending: set = set()

    with _futures.ProcessPoolExecutor(
        max_workers=n_workers, mp_context=ctx,
    ) as ex:
        for chunk in chunks_iter:
            chunk_event_count = sum(len(c[0]) for c in chunk)
            n_events += chunk_event_count
            if len(pending) >= max_inflight:
                done, pending = _futures.wait(
                    pending, return_when=_futures.FIRST_COMPLETED,
                )
                for f in done:
                    _merge(f.result())
                    if log and n_actors > 0 and n_actors % 250_000 < chunk_size:
                        dt = time.time() - t0
                        print(
                            f"  {n_actors:>10,} actors  {n_events:>13,} events  "
                            f"({n_actors / max(dt, 0.001):,.0f} actor/s)",
                            flush=True,
                        )
            pending.add(ex.submit(_process_chunk, chunk, params))
        for f in _futures.as_completed(pending):
            _merge(f.result())

    if log:
        dt = time.time() - t0
        print(
            f"  done (parallel): {n_actors:,} actors, {n_events:,} events "
            f"in {dt:.1f}s ({n_actors / max(dt, 1e-9):,.0f} actor/s)",
            flush=True,
        )
    return (pop_delta, transitions, cohort_outcomes, cohort_size,
            churn_buckets, n_actors, n_events)


# ---------------------------------------------------------------------------
# Fully vectorized state-machine driver
# ---------------------------------------------------------------------------


def _accumulate_transitions(transitions: Counter, hours_arr, from_state, to_state):
    """Bulk-aggregate (hour, from, to) emissions into a Counter.

    Uses np.unique to collapse same-hour repeats, then a single
    `Counter.update` for the unique keys — avoids the Python loop of
    `transitions[(h, fr, to)] += 1` per event.
    """
    if len(hours_arr) == 0:
        return
    u, c = np.unique(hours_arr, return_counts=True)
    transitions.update(
        {(int(h_v), from_state, to_state): int(cnt) for h_v, cnt in zip(u, c)}
    )


def _accumulate_churn_buckets(
    churn_buckets: Counter, fire_hours, first_seen,
    *, age_90d_h: int, age_180d_h: int, week_div: int,
):
    """Bulk-aggregate (week_idx, age_bucket) entries from a churn-fire batch."""
    if len(fire_hours) == 0:
        return
    age_h = fire_hours - first_seen
    bucket = np.where(
        age_h < age_90d_h, 0,
        np.where(age_h < age_180d_h, 1, 2),
    ).astype(np.int64)
    week_idx = (fire_hours // week_div).astype(np.int64)
    packed = week_idx * 3 + bucket
    u, c = np.unique(packed, return_counts=True)
    for p, cnt in zip(u, c):
        w = int(p) // 3
        b = int(p) % 3
        churn_buckets[(w, b)] += int(cnt)


def _run_state_machine_vec(
    con, *,
    at_risk_h: int,
    churn_h: int,
    super_h: int,
    super_thr: int,
    baseline_h: int,
    end_h: int,
    log: bool = True,
) -> tuple:
    """Fully vectorized variant — global numpy ops instead of a per-event loop.

    Strategy:
      * Load the whole `per_hour_sorted` into 4 int64 arrays.
      * Compute per-actor segment metadata once.
      * Compute per-row trailing super-window sums via global `cumsum` +
        `searchsorted` with an actor-offset trick that keeps the hour
        array monotonic across actor boundaries.
      * Identify "super candidates" (actors whose per-row trailing sum
        ever crosses super_thr) and hand them to the existing per-actor
        `process_actor` loop — SUPER state has data-flow dependencies
        we don't try to vectorize, and these actors have many events
        so the per-actor function-call overhead is well amortized.
      * For all *other* actors, emit 4-state (NEW/ACTIVE/AT_RISK/
        CHURNED) transitions and pop_delta deltas vectorized via
        `np.add.at` + `np.unique`.
    """
    if log:
        print("  vec: loading per_hour_sorted into numpy", flush=True)
    t_load = time.time()
    tbl = con.execute(
        "SELECT did_id, hour_idx, n_actions, cohort_ym FROM per_hour_sorted"
    ).fetch_arrow_table()
    did = tbl.column("did_id").to_numpy().astype(np.int64, copy=False)
    hour = tbl.column("hour_idx").to_numpy().astype(np.int64, copy=False)
    count = tbl.column("n_actions").to_numpy().astype(np.int64, copy=False)
    cohort_ym_all = tbl.column("cohort_ym").to_numpy().astype(np.int64, copy=False)
    n_rows = len(did)

    n_hours_plus_1 = end_h + 2
    pop_delta = np.zeros((N_STATES, n_hours_plus_1), dtype=np.int64)
    transitions: Counter = Counter()
    cohort_outcomes: defaultdict = defaultdict(Counter)
    cohort_size: Counter = Counter()
    churn_buckets: Counter = Counter()

    if n_rows == 0:
        return (pop_delta, transitions, cohort_outcomes, cohort_size,
                churn_buckets, 0, 0)

    # ---- per-actor segments ------------------------------------------------
    actor_starts = np.concatenate(
        ([0], np.where(np.diff(did) != 0)[0] + 1)
    ).astype(np.int64)
    actor_ends = np.concatenate((actor_starts[1:], [n_rows])).astype(np.int64)
    n_actors = len(actor_starts)
    actor_lens = actor_ends - actor_starts
    actor_idx_per_row = np.repeat(np.arange(n_actors, dtype=np.int64), actor_lens)
    offset_in_actor = np.arange(n_rows, dtype=np.int64) - actor_starts[actor_idx_per_row]

    actor_first_hour = hour[actor_starts]
    actor_cohort = cohort_ym_all[actor_starts]
    is_existing = actor_first_hour < baseline_h
    is_existing_per_row = is_existing[actor_idx_per_row]

    if log:
        print(
            f"  vec: loaded {n_rows:,} rows / {n_actors:,} actors "
            f"in {(time.time() - t_load) * 1000:.0f} ms",
            flush=True,
        )

    # ---- per-row trailing super-window sum --------------------------------
    # Make `hour` globally monotonic across actor boundaries by offsetting
    # each actor's hours by `a * STRIDE`. `np.searchsorted` then gives
    # the correct per-actor window-start index in one call.
    STRIDE = int(end_h + super_h + 2)
    hour_offset = hour + actor_idx_per_row * STRIDE
    target_offset = hour_offset - (super_h - 1)  # smallest j with hour[j] >= h-super_h+1
    window_start = np.searchsorted(hour_offset, target_offset, side="left").astype(np.int64)

    cum = np.empty(n_rows + 1, dtype=np.int64)
    cum[0] = 0
    np.cumsum(count, out=cum[1:])
    # trailing[k] = cum[k+1] - cum[window_start[k]]
    trailing = cum[1:] - cum[window_start]

    # ---- find super-candidate actors --------------------------------------
    # An actor is a super candidate if any of their per-row trailing sums
    # ever crossed super_thr. They'll be routed to the per-actor loop.
    crossed = (trailing >= super_thr).astype(np.int8)
    # max per actor over the segment
    seg_max_crossed = np.maximum.reduceat(crossed, actor_starts)
    is_super_actor = seg_max_crossed > 0  # bool, per actor

    n_super = int(is_super_actor.sum())
    n_vec = n_actors - n_super
    if log:
        print(
            f"  vec: routing {n_vec:,} actors through vec path, "
            f"{n_super:,} super-candidate actors through per-actor loop "
            f"({100 * n_super / max(n_actors, 1):.2f}% super)",
            flush=True,
        )

    week_div = 7 * 24
    age_90d_h = 90 * 24
    age_180d_h = 180 * 24

    # ---- super-candidate actors: per-actor process_actor ------------------
    super_actor_idx = np.where(is_super_actor)[0]
    for ai in super_actor_idx:
        s = int(actor_starts[ai])
        e = int(actor_ends[ai])
        process_actor(
            hour[s:e], count[s:e],
            at_risk_h=at_risk_h, churn_h=churn_h,
            super_h=super_h, super_thr=super_thr,
            baseline_h=baseline_h, end_h=end_h,
            is_existing=bool(is_existing[ai]),
            pop_delta=pop_delta,
            transitions=transitions,
            cohort_outcomes=cohort_outcomes,
            cohort_size=cohort_size,
            cohort_ym=int(actor_cohort[ai]),
            churn_buckets=churn_buckets,
        )

    # ---- vec path: 4-state base for non-super actors -----------------------
    vec_actor_mask = ~is_super_actor

    # Per-actor effective start and initial state for vec actors
    effective_start = np.where(is_existing, baseline_h, actor_first_hour)
    initial_state = np.where(is_existing, STATE_ACTIVE, STATE_NEW)

    # Per-existing-actor: first row offset with hour >= baseline_h (idx0_offset).
    # For new actors: idx0_offset = 1 (skip the NEW seed event 0).
    has_post_baseline = hour >= baseline_h
    BIG = n_rows + 1
    candidate_offset = np.where(has_post_baseline, offset_in_actor, BIG).astype(np.int64)
    idx0_offset_existing = np.minimum.reduceat(candidate_offset, actor_starts)
    idx0_offset = np.where(is_existing, idx0_offset_existing, 1).astype(np.int64)

    # Some existing actors have NO post-baseline events → idx0_offset == BIG.
    # Some new actors have a single event (actor_lens == 1) → idx0_offset == 1
    # but there are no rows at offset >= 1.
    has_any_processed_row = idx0_offset < actor_lens
    keep_actor = (effective_start <= end_h) & vec_actor_mask

    # ---- per-actor initial pop_delta + cohort_size ------------------------
    kept = keep_actor
    np.add.at(pop_delta, (initial_state[kept], effective_start[kept]), 1)
    u_coh, c_coh = np.unique(actor_cohort[kept], return_counts=True)
    for ym, c in zip(u_coh, c_coh):
        cohort_size[int(ym)] += int(c)

    # ---- NEW → ACTIVE graduation per new vec actor -----------------------
    # Grad happens at first_hour + at_risk_h, but only if NEW state is
    # still in effect by then. With the simplified base (no super), the
    # grad always fires (assuming grad_h <= end_h). The cascade beyond
    # the grad event is handled per-row below for actors with events
    # at/after grad_h, and in the tail for actors whose only events
    # were the NEW seed.
    new_vec_mask = (~is_existing) & keep_actor
    grad_h_arr = (actor_first_hour[new_vec_mask] + at_risk_h).astype(np.int64)
    grad_valid = grad_h_arr <= end_h
    grad_h_v = grad_h_arr[grad_valid]
    np.add.at(pop_delta[STATE_NEW], grad_h_v, -1)
    np.add.at(pop_delta[STATE_ACTIVE], grad_h_v, +1)
    _accumulate_transitions(transitions, grad_h_v, STATE_NEW, STATE_ACTIVE)

    # ---- per-row transitions for rows at offset >= idx0_offset -----------
    # process_row tells us which rows to consider for at_risk / churn /
    # resurrect emission. The "previous active hour" for each processed
    # row is the previous-row hour within the actor, OR the seed hour
    # (baseline_h or first_hour) for the first processed row.
    process_row = (
        keep_actor[actor_idx_per_row]
        & (offset_in_actor >= idx0_offset[actor_idx_per_row])
    )
    is_first_proc = process_row & (
        offset_in_actor == idx0_offset[actor_idx_per_row]
    )

    # prev_hour: previous row's hour for non-first rows; seed hour for first.
    prev_hour = np.empty(n_rows, dtype=np.int64)
    prev_hour[0] = 0
    prev_hour[1:] = hour[:-1]
    seed_per_row = np.where(
        is_existing_per_row, baseline_h, actor_first_hour[actor_idx_per_row],
    )
    prev_hour = np.where(is_first_proc, seed_per_row, prev_hour)

    gap = hour - prev_hour

    # Fire masks and hours
    at_risk_fire_mask = process_row & (gap >= at_risk_h)
    at_risk_fire_h = prev_hour + at_risk_h
    at_risk_valid = at_risk_fire_mask & (at_risk_fire_h <= end_h)

    churn_fire_mask = process_row & (gap >= churn_h)
    churn_fire_h = prev_hour + churn_h
    churn_valid = churn_fire_mask & (churn_fire_h <= end_h)

    # Resurrect: at event hour, transition AT_RISK→ACTIVE or CHURNED→ACTIVE
    resurrect_mask = at_risk_fire_mask & (hour <= end_h)
    res_from_atrisk = resurrect_mask & ~churn_fire_mask
    res_from_churned = resurrect_mask & churn_fire_mask

    # ---- batch-apply pop_delta + transitions ------------------------------
    at_risk_h_v = at_risk_fire_h[at_risk_valid]
    np.add.at(pop_delta[STATE_ACTIVE], at_risk_h_v, -1)
    np.add.at(pop_delta[STATE_AT_RISK], at_risk_h_v, +1)
    _accumulate_transitions(transitions, at_risk_h_v, STATE_ACTIVE, STATE_AT_RISK)

    churn_h_v = churn_fire_h[churn_valid]
    np.add.at(pop_delta[STATE_AT_RISK], churn_h_v, -1)
    np.add.at(pop_delta[STATE_CHURNED], churn_h_v, +1)
    _accumulate_transitions(transitions, churn_h_v, STATE_AT_RISK, STATE_CHURNED)
    _accumulate_churn_buckets(
        churn_buckets, churn_h_v,
        actor_first_hour[actor_idx_per_row][churn_valid],
        age_90d_h=age_90d_h, age_180d_h=age_180d_h, week_div=week_div,
    )

    res_at_h = hour[res_from_atrisk]
    np.add.at(pop_delta[STATE_AT_RISK], res_at_h, -1)
    np.add.at(pop_delta[STATE_ACTIVE], res_at_h, +1)
    _accumulate_transitions(transitions, res_at_h, STATE_AT_RISK, STATE_ACTIVE)

    res_ch_h = hour[res_from_churned]
    np.add.at(pop_delta[STATE_CHURNED], res_ch_h, -1)
    np.add.at(pop_delta[STATE_ACTIVE], res_ch_h, +1)
    _accumulate_transitions(transitions, res_ch_h, STATE_CHURNED, STATE_ACTIVE)

    # ---- per-actor tail projection to end_h --------------------------------
    # Determine each vec actor's last_active (last processed event hour, or
    # baseline_h for existing actors with no post-baseline events), then
    # project at_risk_fire and churn_fire forward.
    candidate_last = np.where(process_row, offset_in_actor, -1).astype(np.int64)
    last_proc_offset = np.maximum.reduceat(candidate_last, actor_starts)
    has_processed = last_proc_offset >= 0
    last_proc_row = (actor_starts + np.where(has_processed, last_proc_offset, 0)).astype(np.int64)
    last_active_per_actor = np.where(
        has_processed, hour[last_proc_row], baseline_h,
    )
    # For new actors with no processed rows (single seed event), last_active
    # is their seed hour (first_hour).
    last_active_per_actor = np.where(
        (~is_existing) & (~has_processed),
        actor_first_hour,
        last_active_per_actor,
    )

    # Tail at_risk fire: only for kept vec actors
    tail_arf = last_active_per_actor + at_risk_h
    tail_cf = last_active_per_actor + churn_h
    tail_arf_valid = keep_actor & (tail_arf <= end_h)
    tail_cf_valid = keep_actor & (tail_cf <= end_h)

    tail_arf_h = tail_arf[tail_arf_valid]
    np.add.at(pop_delta[STATE_ACTIVE], tail_arf_h, -1)
    np.add.at(pop_delta[STATE_AT_RISK], tail_arf_h, +1)
    _accumulate_transitions(transitions, tail_arf_h, STATE_ACTIVE, STATE_AT_RISK)

    tail_cf_h = tail_cf[tail_cf_valid]
    np.add.at(pop_delta[STATE_AT_RISK], tail_cf_h, -1)
    np.add.at(pop_delta[STATE_CHURNED], tail_cf_h, +1)
    _accumulate_transitions(transitions, tail_cf_h, STATE_AT_RISK, STATE_CHURNED)
    _accumulate_churn_buckets(
        churn_buckets, tail_cf_h,
        actor_first_hour[tail_cf_valid],
        age_90d_h=age_90d_h, age_180d_h=age_180d_h, week_div=week_div,
    )

    # ---- per-actor cohort outcome at end_h --------------------------------
    # Final state per kept vec actor:
    #   - If tail_cf fired (<= end_h): CHURNED
    #   - Else if tail_arf fired: AT_RISK
    #   - Else: ACTIVE
    # (NEW grads always fire — we emitted them above — so no actor lands
    # NEW unless grad_h > end_h, in which case keep_actor is False.)
    final_state = np.full(n_actors, STATE_ACTIVE, dtype=np.int64)
    final_state = np.where(tail_arf_valid, STATE_AT_RISK, final_state)
    final_state = np.where(tail_cf_valid, STATE_CHURNED, final_state)

    # Aggregate cohort_outcomes for kept vec actors
    kept_idx = np.where(keep_actor)[0]
    if len(kept_idx) > 0:
        # group by (cohort_ym, final_state)
        coh_keys = actor_cohort[kept_idx]
        st_keys = final_state[kept_idx]
        # Pack (coh, st) for unique-aggregation
        coh_packed = coh_keys * N_STATES + st_keys
        u, c = np.unique(coh_packed, return_counts=True)
        for packed_v, cnt in zip(u, c):
            ym = int(packed_v) // N_STATES
            st = int(packed_v) % N_STATES
            cohort_outcomes[ym][st] += int(cnt)

    n_events = int(n_rows)
    if log:
        dt = time.time() - t_load
        print(
            f"  vec: total {n_actors:,} actors in {dt:.2f}s "
            f"({n_actors / max(dt, 1e-9):,.0f} actor/s)",
            flush=True,
        )
    return (pop_delta, transitions, cohort_outcomes, cohort_size,
            churn_buckets, n_actors, n_events)


# ---------------------------------------------------------------------------
# Numba-compiled state-machine kernel
# ---------------------------------------------------------------------------


@njit(cache=True, nogil=True)
def _sm_kernel(
    seg_starts, seg_lens, is_existing,
    hours_all, counts_all,
    at_risk_h, churn_h, super_h, super_thr, baseline_h, end_h,
    pop_delta, trans_arr, churn_arr, final_state,
    win_h, win_n,
):
    """Compiled per-actor state machine over every actor in one call.

    Direct port of `process_actor` (which remains the readable reference
    and the equality oracle in the test suite). Writes results into
    preallocated arrays instead of Python Counters so the whole body is
    nopython-compilable:

      pop_delta[state, hour]            += / -=  on entry/exit
      trans_arr[hour, from, to]         += 1     per transition
      churn_arr[week_idx, age_bucket]   += 1     per AT_RISK→CHURNED
      final_state[actor]                = state at end_h (-1 if skipped)

    `win_h` / `win_n` are scratch ring buffers sized to the longest
    actor segment, reused across actors (no per-actor allocation).
    """
    n_actors = seg_starts.shape[0]
    week_div = 7 * 24
    age_90d = 90 * 24
    age_180d = 180 * 24

    for ai in range(n_actors):
        s = seg_starts[ai]
        ln = seg_lens[ai]
        first_h = hours_all[s]

        if is_existing[ai]:
            state = STATE_ACTIVE
            start_h = baseline_h
            idx0 = np.searchsorted(hours_all[s:s + ln], baseline_h) + 0
            last_active_h = -1
        else:
            state = STATE_NEW
            start_h = first_h
            idx0 = 0
            last_active_h = first_h

        if start_h > end_h:
            final_state[ai] = -1
            continue

        pop_delta[state, start_h] += 1

        win_head = 0
        win_tail = 0
        win_sum = 0

        if not is_existing[ai]:
            win_h[win_tail] = first_h
            win_n[win_tail] = counts_all[s]
            win_tail += 1
            win_sum = counts_all[s]
            idx0 = 1

        for k in range(idx0, ln):
            h = hours_all[s + k]
            n = counts_all[s + k]

            if last_active_h >= 0:
                at_risk_fire = last_active_h + at_risk_h
                churn_fire = last_active_h + churn_h
            elif is_existing[ai]:
                at_risk_fire = baseline_h + at_risk_h
                churn_fire = baseline_h + churn_h
            else:
                at_risk_fire = -1
                churn_fire = -1

            if state == STATE_NEW:
                grad_h = start_h + at_risk_h
                if h >= grad_h:
                    if grad_h <= end_h:
                        pop_delta[state, grad_h] -= 1
                        pop_delta[STATE_ACTIVE, grad_h] += 1
                        trans_arr[grad_h, state, STATE_ACTIVE] += 1
                        state = STATE_ACTIVE
                    if last_active_h >= 0:
                        arf = last_active_h + at_risk_h
                        if (state == STATE_ACTIVE or state == STATE_SUPER) and h >= arf:
                            if arf <= end_h:
                                pop_delta[state, arf] -= 1
                                pop_delta[STATE_AT_RISK, arf] += 1
                                trans_arr[arf, state, STATE_AT_RISK] += 1
                                state = STATE_AT_RISK
                        cf = last_active_h + churn_h
                        if state == STATE_AT_RISK and h >= cf:
                            if cf <= end_h:
                                pop_delta[state, cf] -= 1
                                pop_delta[STATE_CHURNED, cf] += 1
                                trans_arr[cf, state, STATE_CHURNED] += 1
                                age_h = cf - first_h
                                bkt = 0 if age_h < age_90d else (1 if age_h < age_180d else 2)
                                churn_arr[cf // week_div, bkt] += 1
                                state = STATE_CHURNED
            else:
                if at_risk_fire >= 0 and h >= at_risk_fire \
                        and (state == STATE_ACTIVE or state == STATE_SUPER):
                    if at_risk_fire <= end_h:
                        pop_delta[state, at_risk_fire] -= 1
                        pop_delta[STATE_AT_RISK, at_risk_fire] += 1
                        trans_arr[at_risk_fire, state, STATE_AT_RISK] += 1
                        state = STATE_AT_RISK
                if churn_fire >= 0 and h >= churn_fire and state == STATE_AT_RISK:
                    if churn_fire <= end_h:
                        pop_delta[state, churn_fire] -= 1
                        pop_delta[STATE_CHURNED, churn_fire] += 1
                        trans_arr[churn_fire, state, STATE_CHURNED] += 1
                        age_h = churn_fire - first_h
                        bkt = 0 if age_h < age_90d else (1 if age_h < age_180d else 2)
                        churn_arr[churn_fire // week_div, bkt] += 1
                        state = STATE_CHURNED

            while win_head < win_tail and win_h[win_head] + super_h <= h:
                old_h = win_h[win_head]
                old_n = win_n[win_head]
                win_head += 1
                win_sum -= old_n
                if state == STATE_SUPER and win_sum < super_thr:
                    expire_h = old_h + super_h
                    if expire_h <= end_h:
                        pop_delta[state, expire_h] -= 1
                        pop_delta[STATE_ACTIVE, expire_h] += 1
                        trans_arr[expire_h, state, STATE_ACTIVE] += 1
                        state = STATE_ACTIVE

            if state == STATE_CHURNED or state == STATE_AT_RISK:
                pop_delta[state, h] -= 1
                pop_delta[STATE_ACTIVE, h] += 1
                trans_arr[h, state, STATE_ACTIVE] += 1
                state = STATE_ACTIVE

            win_h[win_tail] = h
            win_n[win_tail] = n
            win_tail += 1
            win_sum += n
            if state == STATE_ACTIVE and win_sum >= super_thr:
                pop_delta[state, h] -= 1
                pop_delta[STATE_SUPER, h] += 1
                trans_arr[h, state, STATE_SUPER] += 1
                state = STATE_SUPER
            last_active_h = h

        if last_active_h < 0:
            last_active_h = baseline_h

        if state == STATE_NEW:
            grad_h = start_h + at_risk_h
            if end_h >= grad_h:
                pop_delta[state, grad_h] -= 1
                pop_delta[STATE_ACTIVE, grad_h] += 1
                trans_arr[grad_h, state, STATE_ACTIVE] += 1
                state = STATE_ACTIVE

        if state == STATE_ACTIVE or state == STATE_SUPER:
            at_risk_fire = last_active_h + at_risk_h
            decay_horizon = at_risk_fire if at_risk_fire < end_h else end_h
            while win_head < win_tail and win_h[win_head] + super_h <= decay_horizon:
                old_h = win_h[win_head]
                old_n = win_n[win_head]
                win_head += 1
                win_sum -= old_n
                if state == STATE_SUPER and win_sum < super_thr:
                    expire_h = old_h + super_h
                    if expire_h <= end_h:
                        pop_delta[state, expire_h] -= 1
                        pop_delta[STATE_ACTIVE, expire_h] += 1
                        trans_arr[expire_h, state, STATE_ACTIVE] += 1
                        state = STATE_ACTIVE
            if at_risk_fire <= end_h:
                pop_delta[state, at_risk_fire] -= 1
                pop_delta[STATE_AT_RISK, at_risk_fire] += 1
                trans_arr[at_risk_fire, state, STATE_AT_RISK] += 1
                state = STATE_AT_RISK

        if state == STATE_AT_RISK:
            churn_fire = last_active_h + churn_h
            if churn_fire <= end_h:
                pop_delta[state, churn_fire] -= 1
                pop_delta[STATE_CHURNED, churn_fire] += 1
                trans_arr[churn_fire, state, STATE_CHURNED] += 1
                age_h = churn_fire - first_h
                bkt = 0 if age_h < age_90d else (1 if age_h < age_180d else 2)
                churn_arr[churn_fire // week_div, bkt] += 1
                state = STATE_CHURNED

        final_state[ai] = state


def _run_state_machine_numba(
    con, *,
    at_risk_h: int,
    churn_h: int,
    super_h: int,
    super_thr: int,
    baseline_h: int,
    end_h: int,
    log: bool = True,
) -> tuple:
    """Numba-backed driver — one compiled pass over every actor.

    Loads `per_hour_sorted` into numpy (same as the vec path), computes
    per-actor segment offsets, then runs `_sm_kernel` once over all
    actors. Dense `trans_arr` / `churn_arr` outputs are folded into the
    Counter / defaultdict structures the renderer expects. Output is
    bitwise-identical to the per-actor reference (asserted in tests).
    """
    if log:
        print("  numba: loading per_hour_sorted into numpy", flush=True)
    t_load = time.time()
    tbl = con.execute(
        "SELECT did_id, hour_idx, n_actions, cohort_ym FROM per_hour_sorted"
    ).fetch_arrow_table()
    did = tbl.column("did_id").to_numpy().astype(np.int64, copy=False)
    hour = tbl.column("hour_idx").to_numpy().astype(np.int64, copy=False)
    count = tbl.column("n_actions").to_numpy().astype(np.int64, copy=False)
    cohort_ym_all = tbl.column("cohort_ym").to_numpy().astype(np.int64, copy=False)
    n_rows = len(did)

    n_hours_plus_1 = end_h + 2
    pop_delta = np.zeros((N_STATES, n_hours_plus_1), dtype=np.int64)
    transitions: Counter = Counter()
    cohort_outcomes: defaultdict = defaultdict(Counter)
    cohort_size: Counter = Counter()
    churn_buckets: Counter = Counter()

    if n_rows == 0:
        return (pop_delta, transitions, cohort_outcomes, cohort_size,
                churn_buckets, 0, 0)

    seg_starts = np.concatenate(
        ([0], np.where(np.diff(did) != 0)[0] + 1)
    ).astype(np.int64)
    seg_ends = np.concatenate((seg_starts[1:], [n_rows])).astype(np.int64)
    seg_lens = (seg_ends - seg_starts).astype(np.int64)
    n_actors = len(seg_starts)
    actor_first_hour = hour[seg_starts]
    actor_cohort = cohort_ym_all[seg_starts]
    is_existing = (actor_first_hour < baseline_h)

    n_weeks_plus_1 = end_h // (7 * 24) + 2
    trans_arr = np.zeros((n_hours_plus_1, N_STATES, N_STATES), dtype=np.int64)
    churn_arr = np.zeros((n_weeks_plus_1, 3), dtype=np.int64)
    final_state = np.empty(n_actors, dtype=np.int64)
    max_len = int(seg_lens.max()) + 1
    win_h = np.empty(max_len, dtype=np.int64)
    win_n = np.empty(max_len, dtype=np.int64)

    if log:
        print(
            f"  numba: loaded {n_rows:,} rows / {n_actors:,} actors "
            f"in {(time.time() - t_load) * 1000:.0f} ms; "
            f"trans_arr={trans_arr.nbytes / 1e6:.0f}MB max_seg={max_len}",
            flush=True,
        )

    t_run = time.time()
    _sm_kernel(
        seg_starts, seg_lens, is_existing,
        hour, count,
        at_risk_h, churn_h, super_h, super_thr, baseline_h, end_h,
        pop_delta, trans_arr, churn_arr, final_state,
        win_h, win_n,
    )
    if log:
        dt = time.time() - t_run
        print(
            f"  numba: kernel ran {n_actors:,} actors in {dt:.2f}s "
            f"({n_actors / max(dt, 1e-9):,.0f} actor/s, includes JIT warmup)",
            flush=True,
        )

    # ---- fold dense outputs into the Counter structures -------------------
    t_fold = time.time()
    nz = np.nonzero(trans_arr)
    nz_vals = trans_arr[nz]
    for h_v, fr, to, v in zip(nz[0], nz[1], nz[2], nz_vals):
        transitions[(int(h_v), int(fr), int(to))] = int(v)

    cz = np.nonzero(churn_arr)
    cz_vals = churn_arr[cz]
    for w, b, v in zip(cz[0], cz[1], cz_vals):
        churn_buckets[(int(w), int(b))] = int(v)

    # cohort_size: count non-skipped actors per cohort
    kept = final_state >= 0
    u_sz, c_sz = np.unique(actor_cohort[kept], return_counts=True)
    for ym, c in zip(u_sz, c_sz):
        cohort_size[int(ym)] = int(c)

    # cohort_outcomes: (cohort, final_state) for kept actors
    coh_packed = actor_cohort[kept] * N_STATES + final_state[kept]
    u_co, c_co = np.unique(coh_packed, return_counts=True)
    for packed_v, cnt in zip(u_co, c_co):
        ym = int(packed_v) // N_STATES
        st = int(packed_v) % N_STATES
        cohort_outcomes[ym][st] += int(cnt)

    if log:
        print(
            f"  numba: folded outputs in {time.time() - t_fold:.2f}s "
            f"({len(transitions):,} transition keys)",
            flush=True,
        )

    return (pop_delta, transitions, cohort_outcomes, cohort_size,
            churn_buckets, n_actors, int(n_rows))


def process_actor(
    hours: np.ndarray,
    counts: np.ndarray,
    *,
    at_risk_h: int,
    churn_h: int,
    super_h: int,
    super_thr: int,
    baseline_h: int,
    end_h: int,
    is_existing: bool,
    pop_delta: np.ndarray,
    transitions: Counter,
    cohort_outcomes: defaultdict,
    cohort_size: Counter,
    cohort_ym: int,
    churn_buckets: Counter | None = None,
) -> None:
    """Run the state machine for one actor's full event timeline.

    Vectorized hot path: hours/counts are cast to int64 arrays once and
    the super-window trailing sum is fed by a preallocated numpy ring
    buffer (no list.pop(0)). The transition emitter is inlined to avoid
    closure lookups. Otherwise the algorithm is identical to the
    line-by-line state machine and the test suite asserts it.

    Mutates `pop_delta`, `transitions`, `cohort_outcomes`, `cohort_size`,
    and `churn_buckets` (if provided).

    `pop_delta[s][h] += 1` when actor enters state s at hour h, `-= 1`
    on exit. cumsum-by-hour gives population over time. Transitions
    after `end_h` are skipped (we can't observe them in the data).

    `churn_buckets[(week_idx, age_bucket)] += 1` is incremented at each
    AT_RISK→CHURNED transition; age_bucket is 0/1/2 for tenure
    <90d / 90–180d / >180d at the moment of churn. Used by the regime
    classifier to distinguish "leaky onboarding" (young cohorts
    churning) from "churning active" (old guard leaving).
    """
    # Cast hours/counts to int64 arrays *once*. The hot loop indexes
    # these arrays as Python ints (via the materialized numpy → int
    # path) instead of calling int() on a numpy scalar every iteration.
    if hours.dtype != np.int64:
        hours = hours.astype(np.int64)
    if counts.dtype != np.int64:
        counts = counts.astype(np.int64)

    # True first-seen hour, used for tenure-at-churn calculation. For
    # existing actors this can be far in the past (pre-baseline).
    true_first_seen_h = int(hours[0])

    # ---- starting state -------------------------------------------------
    if is_existing:
        # Existing user — seeded ACTIVE at baseline_h regardless of when
        # their first observed post-baseline event lands. We do NOT
        # carry any pre-baseline trailing7 contribution: their super
        # status at baseline is unknowable without finer-grained
        # pre-baseline data, so they start as plain "active" and only
        # post-baseline events can promote them.
        state = STATE_ACTIVE
        start_h = baseline_h
        # Skip events before baseline for state purposes.
        idx0 = int(np.searchsorted(hours, baseline_h, side="left"))
        last_active_h = -1
    else:
        state = STATE_NEW
        start_h = true_first_seen_h
        idx0 = 0
        last_active_h = true_first_seen_h

    if start_h > end_h:
        # Actor only appears after the analysis window — skip.
        return
    pop_delta[state, start_h] += 1
    cohort_size[cohort_ym] += 1

    n_events = len(hours)

    # ---- preallocated ring buffer for the super-window ------------------
    # Holds (h, n) for events whose timestamp is within super_h of "now".
    # Sized at n_events_remaining; in practice usage stays <10. Using
    # numpy arrays with head/tail indices avoids the O(n) list.pop(0)
    # in the original implementation.
    remaining = n_events - idx0 + (0 if is_existing else 1)
    if remaining < 1:
        remaining = 1
    win_h = np.empty(remaining, dtype=np.int64)
    win_n = np.empty(remaining, dtype=np.int64)
    win_head = 0
    win_tail = 0
    win_sum = 0

    # Hoist module-level constants into locals — Python looks up locals
    # faster than module globals on every reference.
    S_NEW = STATE_NEW
    S_ACTIVE = STATE_ACTIVE
    S_SUPER = STATE_SUPER
    S_AT_RISK = STATE_AT_RISK
    S_CHURNED = STATE_CHURNED
    week_div = 7 * 24
    age_90d = 90 * 24
    age_180d = 180 * 24
    track_churn = churn_buckets is not None

    # ---- "new" actor: first event seeds the trailing window -----------
    if not is_existing:
        h0 = int(hours[0])
        n0 = int(counts[0])
        win_h[win_tail] = h0
        win_n[win_tail] = n0
        win_tail += 1
        win_sum = n0
        idx0 = 1

    # Walk subsequent events. Between events we fire scheduled
    # transitions (at_risk / churn / super-decay) lazily.
    for k in range(idx0, n_events):
        h = int(hours[k])
        n = int(counts[k])

        # Compute scheduled deadlines from the last known last_active.
        if last_active_h >= 0:
            at_risk_fire = last_active_h + at_risk_h
            churn_fire = last_active_h + churn_h
        elif is_existing:
            at_risk_fire = baseline_h + at_risk_h
            churn_fire = baseline_h + churn_h
        else:
            at_risk_fire = -1
            churn_fire = -1

        # NEW graduation. For new users the graduation hour is
        # first_seen + at_risk_h; before then state is NEW. If this
        # event arrives after graduation and we haven't transitioned
        # yet, transition NEW -> ACTIVE at the graduation moment.
        if state == S_NEW:
            grad_h = start_h + at_risk_h
            if h >= grad_h:
                # NEW → ACTIVE at grad_h
                if grad_h <= end_h:
                    pop_delta[state, grad_h] -= 1
                    pop_delta[S_ACTIVE, grad_h] += 1
                    transitions[(grad_h, state, S_ACTIVE)] += 1
                    state = S_ACTIVE
                # Cascade at_risk / churn fires that happened between
                # grad_h and h, based on last_active_h.
                if last_active_h >= 0:
                    arf = last_active_h + at_risk_h
                    if (state == S_ACTIVE or state == S_SUPER) and h >= arf:
                        if arf <= end_h:
                            pop_delta[state, arf] -= 1
                            pop_delta[S_AT_RISK, arf] += 1
                            transitions[(arf, state, S_AT_RISK)] += 1
                            state = S_AT_RISK
                    cf = last_active_h + churn_h
                    if state == S_AT_RISK and h >= cf:
                        if cf <= end_h:
                            pop_delta[state, cf] -= 1
                            pop_delta[S_CHURNED, cf] += 1
                            transitions[(cf, state, S_CHURNED)] += 1
                            if track_churn:
                                age_h = cf - true_first_seen_h
                                if age_h < age_90d:
                                    bucket = 0
                                elif age_h < age_180d:
                                    bucket = 1
                                else:
                                    bucket = 2
                                churn_buckets[(cf // week_div, bucket)] += 1
                            state = S_CHURNED
        else:
            # Fire active->at_risk and at_risk->churned if due.
            if at_risk_fire >= 0 and h >= at_risk_fire \
                    and (state == S_ACTIVE or state == S_SUPER):
                if at_risk_fire <= end_h:
                    pop_delta[state, at_risk_fire] -= 1
                    pop_delta[S_AT_RISK, at_risk_fire] += 1
                    transitions[(at_risk_fire, state, S_AT_RISK)] += 1
                    state = S_AT_RISK
            if churn_fire >= 0 and h >= churn_fire and state == S_AT_RISK:
                if churn_fire <= end_h:
                    pop_delta[state, churn_fire] -= 1
                    pop_delta[S_CHURNED, churn_fire] += 1
                    transitions[(churn_fire, state, S_CHURNED)] += 1
                    if track_churn:
                        age_h = churn_fire - true_first_seen_h
                        if age_h < age_90d:
                            bucket = 0
                        elif age_h < age_180d:
                            bucket = 1
                        else:
                            bucket = 2
                        churn_buckets[(churn_fire // week_div, bucket)] += 1
                    state = S_CHURNED

        # Process super-decay events between the last_active and h.
        # Walk the ring buffer head forward, demoting if the remaining
        # sum drops below threshold while in SUPER.
        while win_head < win_tail and win_h[win_head] + super_h <= h:
            old_h = int(win_h[win_head])
            old_n = int(win_n[win_head])
            win_head += 1
            win_sum -= old_n
            if state == S_SUPER and win_sum < super_thr:
                expire_h = old_h + super_h
                if expire_h <= end_h:
                    pop_delta[state, expire_h] -= 1
                    pop_delta[S_ACTIVE, expire_h] += 1
                    transitions[(expire_h, state, S_ACTIVE)] += 1
                    state = S_ACTIVE

        # Apply the event itself: resurrect from CHURNED/AT_RISK.
        if state == S_CHURNED or state == S_AT_RISK:
            pop_delta[state, h] -= 1
            pop_delta[S_ACTIVE, h] += 1
            transitions[(h, state, S_ACTIVE)] += 1
            state = S_ACTIVE
        # If NEW, the action keeps them NEW until grad_h (already handled).

        win_h[win_tail] = h
        win_n[win_tail] = n
        win_tail += 1
        win_sum += n
        if state == S_ACTIVE and win_sum >= super_thr:
            pop_delta[state, h] -= 1
            pop_delta[S_SUPER, h] += 1
            transitions[(h, state, S_SUPER)] += 1
            state = S_SUPER
        last_active_h = h

    # ---- tail: project deadlines forward to end_h -----------------------
    if last_active_h < 0:
        # Existing actor with no post-baseline activity; their effective
        # "last_active" is baseline_h.
        last_active_h = baseline_h

    # If still NEW at end of stream: their only-ever event is `start_h`.
    # If end_h > start_h + at_risk_h they graduate to ACTIVE, then cascade.
    if state == S_NEW:
        grad_h = start_h + at_risk_h
        if end_h >= grad_h:
            pop_delta[state, grad_h] -= 1
            pop_delta[S_ACTIVE, grad_h] += 1
            transitions[(grad_h, state, S_ACTIVE)] += 1
            state = S_ACTIVE

    # ACTIVE/SUPER tail: super decay + at_risk firing.
    if state == S_ACTIVE or state == S_SUPER:
        at_risk_fire = last_active_h + at_risk_h
        decay_horizon = at_risk_fire if at_risk_fire < end_h else end_h
        while win_head < win_tail and win_h[win_head] + super_h <= decay_horizon:
            old_h = int(win_h[win_head])
            old_n = int(win_n[win_head])
            win_head += 1
            win_sum -= old_n
            if state == S_SUPER and win_sum < super_thr:
                expire_h = old_h + super_h
                if expire_h <= end_h:
                    pop_delta[state, expire_h] -= 1
                    pop_delta[S_ACTIVE, expire_h] += 1
                    transitions[(expire_h, state, S_ACTIVE)] += 1
                    state = S_ACTIVE
        if at_risk_fire <= end_h:
            pop_delta[state, at_risk_fire] -= 1
            pop_delta[S_AT_RISK, at_risk_fire] += 1
            transitions[(at_risk_fire, state, S_AT_RISK)] += 1
            state = S_AT_RISK

    if state == S_AT_RISK:
        churn_fire = last_active_h + churn_h
        if churn_fire <= end_h:
            pop_delta[state, churn_fire] -= 1
            pop_delta[S_CHURNED, churn_fire] += 1
            transitions[(churn_fire, state, S_CHURNED)] += 1
            if track_churn:
                age_h = churn_fire - true_first_seen_h
                if age_h < age_90d:
                    bucket = 0
                elif age_h < age_180d:
                    bucket = 1
                else:
                    bucket = 2
                churn_buckets[(churn_fire // week_div, bucket)] += 1
            state = S_CHURNED

    # Don't emit a final exit at end_h — the actor is in their last
    # observed state at the censoring horizon. Cumsum of pop_delta thus
    # gives the population in each state at each hour up to end_h.

    # ---- cohort outcome -------------------------------------------------
    cohort_outcomes[cohort_ym][state] += 1


# ---------------------------------------------------------------------------
# Aggregation: hourly → daily
# ---------------------------------------------------------------------------


def _aggregate_daily(populations, transitions, snap_h, n_hours):
    """Down-sample hourly populations and transitions to daily."""
    # Pick a sensible window start: first hour with any actor in any state.
    nonzero = np.where(populations.sum(axis=0) > 0)[0]
    if len(nonzero) == 0:
        return populations[:, :0], {}, []
    start_h = int(nonzero[0])
    # Snap start_h down to a midnight boundary.
    start_h -= start_h % 24
    end_h = snap_h - (snap_h % 24)
    if end_h <= start_h:
        end_h = start_h + 24

    n_days = (end_h - start_h) // 24
    # Take the population at the last hour of each day (24*d + 23). This
    # is "where everyone is at midnight of the next day".
    sample_hours = np.arange(n_days) * 24 + start_h + 23
    sample_hours = np.clip(sample_hours, 0, populations.shape[1] - 1)
    populations_daily = populations[:, sample_hours]

    # Sum transitions per (day_idx, from, to).
    transitions_daily = defaultdict(int)
    for (h, fr, to), cnt in transitions.items():
        if h < start_h or h >= end_h:
            continue
        d = (h - start_h) // 24
        transitions_daily[(d, fr, to)] += cnt

    day_dates = [
        (datetime.fromtimestamp(0, tz=timezone.utc) + timedelta(hours=int(h)))
        .date().isoformat()
        for h in (np.arange(n_days) * 24 + start_h)
    ]
    return populations_daily, transitions_daily, day_dates


def _to_hour_index(dt: datetime) -> int:
    """Hours since 1970-01-01 00:00:00 UTC."""
    return int(dt.replace(tzinfo=timezone.utc).timestamp() // 3600)


# ---------------------------------------------------------------------------
# Aggregation: hourly → weekly, with regime classification
# ---------------------------------------------------------------------------


# Regime labels used by the hero chart and the sidecar.
REGIME_GROWTH = "growth"
REGIME_NO_NEW = "no_new"
REGIME_LEAKY_ONBOARDING = "leaky_onboarding"
REGIME_CHURNING_ACTIVE = "churning_active"

REGIME_COLORS = {
    REGIME_GROWTH:           "rgba(22, 163, 74, 0.18)",
    REGIME_NO_NEW:           "rgba(245, 158, 11, 0.18)",
    REGIME_LEAKY_ONBOARDING: "rgba(124, 58, 237, 0.18)",
    REGIME_CHURNING_ACTIVE:  "rgba(239, 68, 68, 0.20)",
}
REGIME_LABELS = {
    REGIME_GROWTH:           "Growth",
    REGIME_NO_NEW:           "No new users",
    REGIME_LEAKY_ONBOARDING: "Leaky onboarding",
    REGIME_CHURNING_ACTIVE:  "Churning active users",
}


def _aggregate_weekly(populations, transitions, churn_buckets, snap_h):
    """Roll hourly population + transition data up to per-week series.

    Returns a dict with parallel numpy arrays indexed by week, plus the
    list of week-start ISO dates. The regime classifier uses these to
    label each week.
    """
    nonzero = np.where(populations.sum(axis=0) > 0)[0]
    if len(nonzero) == 0:
        return _empty_weekly()

    start_h = int(nonzero[0])
    # Anchor on a Monday 00:00. We work in hour-since-epoch space; the
    # Unix epoch 1970-01-01 was a Thursday, so subtract 96h (4 days) to
    # find the prior Monday boundary, then take week-aligned indices.
    start_h -= (start_h % (7 * 24))
    end_h = snap_h - (snap_h % (7 * 24))
    if end_h <= start_h:
        end_h = start_h + 7 * 24

    n_weeks = (end_h - start_h) // (7 * 24)
    if n_weeks == 0:
        return _empty_weekly()

    # Population at the end of each week (last hour of the week).
    sample_hours = np.arange(n_weeks) * 168 + start_h + 167
    sample_hours = np.clip(sample_hours, 0, populations.shape[1] - 1)
    pop_weekly = populations[:, sample_hours]  # shape (N_STATES, n_weeks)

    # Per-week transition tallies, keyed by (from, to).
    weekly_new = np.zeros(n_weeks, dtype=np.int64)
    weekly_resurrect = np.zeros(n_weeks, dtype=np.int64)
    weekly_recover = np.zeros(n_weeks, dtype=np.int64)
    weekly_to_churned = np.zeros(n_weeks, dtype=np.int64)
    weekly_to_at_risk = np.zeros(n_weeks, dtype=np.int64)
    weekly_super_up = np.zeros(n_weeks, dtype=np.int64)
    weekly_super_down = np.zeros(n_weeks, dtype=np.int64)

    healthy = {STATE_ACTIVE, STATE_SUPER}
    for (h, fr, to), cnt in transitions.items():
        if h < start_h or h >= end_h:
            continue
        w = (h - start_h) // 168
        if fr == STATE_NEW and to in healthy:
            weekly_new[w] += cnt
        elif fr == STATE_CHURNED and to in healthy:
            weekly_resurrect[w] += cnt
        elif fr == STATE_AT_RISK and to in healthy:
            weekly_recover[w] += cnt
        elif fr in healthy and to == STATE_AT_RISK:
            weekly_to_at_risk[w] += cnt
        elif fr == STATE_AT_RISK and to == STATE_CHURNED:
            weekly_to_churned[w] += cnt
        elif fr == STATE_ACTIVE and to == STATE_SUPER:
            weekly_super_up[w] += cnt
        elif fr == STATE_SUPER and to == STATE_ACTIVE:
            weekly_super_down[w] += cnt

    # Young-cohort share of weekly churn, used to flag leaky onboarding.
    # churn_buckets is keyed by (raw_week_idx, age_bucket). Translate raw
    # week index to our 0-indexed week.
    weekly_young_churn = np.zeros(n_weeks, dtype=np.int64)
    weekly_old_churn = np.zeros(n_weeks, dtype=np.int64)
    start_week = start_h // 168
    for (raw_w, bucket), cnt in churn_buckets.items():
        w = raw_w - start_week
        if w < 0 or w >= n_weeks:
            continue
        if bucket == 0:        # < 90 days tenure
            weekly_young_churn[w] += cnt
        elif bucket == 2:      # > 180 days tenure
            weekly_old_churn[w] += cnt
        # bucket == 1 is the 90-180d middle, contributes to neither bias.

    # Regime classification — needs aggregate baselines.
    regimes = _classify_regimes(
        weekly_new=weekly_new,
        weekly_resurrect=weekly_resurrect,
        weekly_to_churned=weekly_to_churned,
        weekly_young_churn=weekly_young_churn,
    )

    week_dates = [
        (datetime.fromtimestamp(0, tz=timezone.utc) + timedelta(hours=int(h)))
        .date().isoformat()
        for h in (np.arange(n_weeks) * 168 + start_h)
    ]
    return {
        "week_dates": week_dates,
        "pop": pop_weekly,
        "new": weekly_new,
        "resurrect": weekly_resurrect,
        "recover": weekly_recover,
        "to_churned": weekly_to_churned,
        "to_at_risk": weekly_to_at_risk,
        "super_up": weekly_super_up,
        "super_down": weekly_super_down,
        "young_churn": weekly_young_churn,
        "old_churn": weekly_old_churn,
        "regimes": regimes,
    }


def _empty_weekly():
    return {
        "week_dates": [],
        "pop": np.zeros((N_STATES, 0), dtype=np.int64),
        "new": np.zeros(0, dtype=np.int64),
        "resurrect": np.zeros(0, dtype=np.int64),
        "recover": np.zeros(0, dtype=np.int64),
        "to_churned": np.zeros(0, dtype=np.int64),
        "to_at_risk": np.zeros(0, dtype=np.int64),
        "super_up": np.zeros(0, dtype=np.int64),
        "super_down": np.zeros(0, dtype=np.int64),
        "young_churn": np.zeros(0, dtype=np.int64),
        "old_churn": np.zeros(0, dtype=np.int64),
        "regimes": [],
    }


def _classify_regimes(
    *, weekly_new, weekly_resurrect, weekly_to_churned, weekly_young_churn,
):
    """Per-week regime label. Returns list[str] of length n_weeks.

    Decision tree (in order):
      1. net inflow ≥ 0  →  growth
      2. weekly_new < 50% of median historical inflow  →  no_new
      3. young-cohort share of churn ≥ 50%  →  leaky_onboarding
      4. otherwise                          →  churning_active

    The thresholds are deliberately blunt — the regime label is meant
    to communicate the dominant story, not capture fine variation.
    """
    n = len(weekly_new)
    if n == 0:
        return []
    # Use median over weeks with non-zero new-inflow to avoid the
    # earliest weeks dragging the baseline to zero.
    nonzero_new = weekly_new[weekly_new > 0]
    median_new = float(np.median(nonzero_new)) if len(nonzero_new) else 0.0

    regimes = []
    for w in range(n):
        net = int(weekly_new[w]) + int(weekly_resurrect[w]) - int(weekly_to_churned[w])
        if net >= 0:
            regimes.append(REGIME_GROWTH)
            continue
        if median_new > 0 and weekly_new[w] < 0.5 * median_new:
            regimes.append(REGIME_NO_NEW)
            continue
        total_churn = int(weekly_to_churned[w])
        young_share = (
            int(weekly_young_churn[w]) / total_churn if total_churn > 0 else 0.0
        )
        if young_share >= 0.5:
            regimes.append(REGIME_LEAKY_ONBOARDING)
        else:
            regimes.append(REGIME_CHURNING_ACTIVE)
    return regimes


# ---------------------------------------------------------------------------
# Markov steady-state
# ---------------------------------------------------------------------------


def _compute_markov_steady_state(populations, transitions, snap_h, *, window_hours):
    """Build a per-hour transition matrix from the most-recent
    `window_hours` of activity and solve for its stationary distribution.

    The matrix P is built as:
      P[i][j] = (transitions[(h, i, j)] in window) / (actor-hours in i in window)
      P[i][i] = 1 - sum_{j!=i} P[i][j]   (self-loop probability)

    Then we solve π P = π, sum π = 1, via lstsq on the augmented system.

    Returns a dict with the matrix, the stationary vector, mean
    dwell-time per state, and the dominant non-self transition per row
    (used in the report's commentary).
    """
    lo_h = max(0, snap_h - window_hours)
    hi_h = snap_h

    # Per-state actor-hours in window.
    state_hours = populations[:, lo_h:hi_h + 1].sum(axis=1).astype(np.float64)

    # Per-(i, j) transition counts in window.
    flow = np.zeros((N_STATES, N_STATES), dtype=np.float64)
    for (h, fr, to), cnt in transitions.items():
        if lo_h <= h <= hi_h:
            flow[fr, to] += cnt

    # Build P row by row. If a state had ~no exposure in the window
    # (state_hours[i] == 0) leave its row as identity — it's effectively
    # absorbing for the purpose of this estimate.
    P = np.eye(N_STATES)
    for i in range(N_STATES):
        if state_hours[i] <= 0:
            continue
        outflow_sum = 0.0
        for j in range(N_STATES):
            if i == j:
                continue
            p_ij = flow[i, j] / state_hours[i]
            P[i, j] = p_ij
            outflow_sum += p_ij
        # Cap outflow at 0.999 to keep P[i][i] non-negative even if a
        # state has lots of churn relative to dwell time in the window.
        if outflow_sum > 0.999:
            scale = 0.999 / outflow_sum
            for j in range(N_STATES):
                if i != j:
                    P[i, j] *= scale
            outflow_sum *= scale
        P[i, i] = 1.0 - outflow_sum

    # Solve π P = π with sum(π) = 1.
    # Equivalently: π (P - I) = 0, plus π · 1 = 1.
    # Stack into a (N+1, N) linear system and use lstsq.
    A = np.vstack([(P - np.eye(N_STATES)).T, np.ones(N_STATES)])
    b = np.zeros(N_STATES + 1)
    b[-1] = 1.0
    pi, *_ = np.linalg.lstsq(A, b, rcond=None)
    # Numerical clean-up: clip tiny negatives, renormalize.
    pi = np.clip(pi, 0.0, None)
    s = pi.sum()
    if s > 0:
        pi = pi / s

    # Mean dwell time per state ≈ 1 / (1 - P[i][i]) in per-hour units.
    dwell_hours = np.zeros(N_STATES)
    for i in range(N_STATES):
        leave_p = 1.0 - P[i, i]
        if leave_p > 1e-9:
            dwell_hours[i] = 1.0 / leave_p
        else:
            dwell_hours[i] = float("inf")

    return {
        "P": P,
        "pi": pi,
        "dwell_hours": dwell_hours,
        "state_hours_in_window": state_hours,
    }


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _render(
    *,
    snapshot_date: str,
    at_risk_hours: int,
    churn_days: int,
    super_threshold: int,
    super_window_hours: int,
    existing_baseline_date: str,
    populations_daily: np.ndarray,
    transitions_daily: dict,
    day_dates: list[str],
    weekly: dict,
    markov: dict,
    markov_window_days: int,
    cohort_outcomes: defaultdict,
    cohort_size: Counter,
    n_actors: int,
    n_events: int,
) -> tuple[bytes, dict, bytes]:
    import plotly.graph_objects as go

    # --- chart 1: stacked area of state populations over time ---------
    # Order from top to bottom: super, active, new, at_risk, churned.
    # Plotly stacks bottom→top so we feed them in reverse.
    stack_order = [STATE_CHURNED, STATE_AT_RISK, STATE_NEW, STATE_ACTIVE, STATE_SUPER]
    fig_pop = go.Figure()
    for s in stack_order:
        name = STATE_NAMES[s]
        fig_pop.add_trace(go.Scatter(
            x=day_dates,
            y=populations_daily[s].tolist(),
            name=name,
            mode="lines",
            stackgroup="one",
            line=dict(width=0.5, color=STATE_COLORS[name]),
            hovertemplate=f"{name}<br>%{{x}}<br>%{{y:,}} actors<extra></extra>",
        ))
    fig_pop.update_layout(
        template="bsky",
        title=dict(
            text="<b>State populations over time</b>  ·  "
                 f"actors classified by recency of their last action "
                 f"(at-risk &gt;{at_risk_hours}h, churned &gt;{churn_days}d)",
            x=0.02, xanchor="left",
        ),
        xaxis=dict(title="Date"),
        yaxis=dict(title="Number of actors"),
        height=440,
        legend=dict(orientation="h", y=-0.18),
    )

    # --- chart 2: daily net change in Active+Super ---------------------
    # Decomposition: +new activations, +resurrections (churned->active),
    # +recoveries (at_risk->active), -active->at_risk, -at_risk->churned.
    n_days = len(day_dates)
    inflow_new = np.zeros(n_days, dtype=np.int64)
    inflow_resurrect = np.zeros(n_days, dtype=np.int64)
    inflow_recover = np.zeros(n_days, dtype=np.int64)
    outflow_to_at_risk = np.zeros(n_days, dtype=np.int64)
    outflow_to_churned = np.zeros(n_days, dtype=np.int64)

    healthy = {STATE_ACTIVE, STATE_SUPER}
    for (d, fr, to), c in transitions_daily.items():
        if 0 <= d < n_days:
            if fr == STATE_NEW and to in healthy:
                inflow_new[d] += c
            elif fr == STATE_CHURNED and to in healthy:
                inflow_resurrect[d] += c
            elif fr == STATE_AT_RISK and to in healthy:
                inflow_recover[d] += c
            elif fr in healthy and to == STATE_AT_RISK:
                outflow_to_at_risk[d] += c
            elif fr == STATE_AT_RISK and to == STATE_CHURNED:
                outflow_to_churned[d] += c

    fig_flow = go.Figure()
    fig_flow.add_trace(go.Bar(
        x=day_dates, y=inflow_new.tolist(),
        name="New activations",
        marker=dict(color="#16a34a"),
        hovertemplate="%{x}<br>+%{y:,} new activations<extra></extra>",
    ))
    fig_flow.add_trace(go.Bar(
        x=day_dates, y=inflow_resurrect.tolist(),
        name="Resurrected (churned→active)",
        marker=dict(color=BRAND),
        hovertemplate="%{x}<br>+%{y:,} resurrections<extra></extra>",
    ))
    fig_flow.add_trace(go.Bar(
        x=day_dates, y=inflow_recover.tolist(),
        name="Recovered (at_risk→active)",
        marker=dict(color="#7c3aed"),
        hovertemplate="%{x}<br>+%{y:,} recoveries<extra></extra>",
    ))
    fig_flow.add_trace(go.Bar(
        x=day_dates, y=(-outflow_to_churned).tolist(),
        name="To churned",
        marker=dict(color="#ef4444"),
        hovertemplate="%{x}<br>−%{customdata:,} churned<extra></extra>",
        customdata=outflow_to_churned.tolist(),
    ))
    fig_flow.update_layout(
        template="bsky",
        barmode="relative",
        title=dict(
            text="<b>Daily flow into and out of the active pool</b>  ·  "
                 "new activations vs. resurrections vs. churn outflow",
            x=0.02, xanchor="left",
        ),
        xaxis=dict(title="Date"),
        yaxis=dict(title="Net change in active pool"),
        height=420,
        legend=dict(orientation="h", y=-0.18),
    )

    # --- chart 3: cohort retention -------------------------------------
    # For each year-month cohort, what fraction of the cohort is still
    # active/super at end of analysis vs at-risk/churned.
    cohort_keys = sorted([k for k in cohort_outcomes if k > 0])
    coh_labels = [_fmt_ym(k) for k in cohort_keys]
    coh_size = [cohort_size[k] for k in cohort_keys]
    coh_active_pct = []
    coh_churned_pct = []
    for k in cohort_keys:
        outc = cohort_outcomes[k]
        sz = cohort_size[k] or 1
        active = outc[STATE_ACTIVE] + outc[STATE_SUPER] + outc[STATE_NEW]
        churned = outc[STATE_CHURNED]
        coh_active_pct.append(100.0 * active / sz)
        coh_churned_pct.append(100.0 * churned / sz)

    fig_cohort = go.Figure()
    fig_cohort.add_trace(go.Bar(
        x=coh_labels, y=coh_active_pct,
        name="Active at snapshot",
        marker=dict(color=BRAND),
        customdata=coh_size,
        hovertemplate="cohort %{x}<br>%{y:.1f}%% active<br>%{customdata:,} signups<extra></extra>",
    ))
    fig_cohort.add_trace(go.Bar(
        x=coh_labels, y=coh_churned_pct,
        name="Churned",
        marker=dict(color="#ef4444"),
        hovertemplate="cohort %{x}<br>%{y:.1f}%% churned<extra></extra>",
    ))
    fig_cohort.update_layout(
        template="bsky",
        barmode="stack",
        title=dict(
            text="<b>Cohort outcomes</b>  ·  share of each first-seen month-cohort "
                 f"that is active vs. churned at {snapshot_date}",
            x=0.02, xanchor="left",
        ),
        xaxis=dict(title="First-seen month cohort"),
        yaxis=dict(title="% of cohort", range=[0, 100]),
        height=420,
        legend=dict(orientation="h", y=-0.18),
    )

    # --- chart 4: end-state composition --------------------------------
    final_pop = populations_daily[:, -1] if populations_daily.shape[1] > 0 \
                else np.zeros(N_STATES, dtype=np.int64)
    total_final = int(final_pop.sum()) or 1
    final_pct = [100.0 * int(final_pop[s]) / total_final for s in range(N_STATES)]
    fig_final = go.Figure(go.Bar(
        x=STATE_NAMES,
        y=final_pct,
        marker=dict(color=[STATE_COLORS[s] for s in STATE_NAMES]),
        text=[f"{p:.1f}%" for p in final_pct],
        textposition="outside",
        customdata=final_pop.tolist(),
        hovertemplate="%{x}<br>%{y:.1f}%%<br>%{customdata:,} actors<extra></extra>",
    ))
    fig_final.update_layout(
        template="bsky",
        title=dict(
            text=f"<b>End-of-window composition</b>  ·  state of every observed "
                 f"actor at {snapshot_date}",
            x=0.02, xanchor="left",
        ),
        xaxis=dict(title="State"),
        yaxis=dict(title="% of all observed actors"),
        height=380,
    )

    # --- chart 5: HERO — active-pool line + regime shading -------------
    # Single-image story for posting. Spans the last ~18 months of
    # weeks; background-shaded by per-week regime classification.
    fig_hero = _build_hero_figure(
        weekly=weekly,
        snapshot_date=snapshot_date,
        at_risk_hours=at_risk_hours,
        churn_days=churn_days,
        markov=markov,
        markov_window_days=markov_window_days,
        hero_lookback_weeks=78,  # ~18 months
    )

    # PNG export for direct posting (requires kaleido in the runtime).
    # If kaleido isn't available locally (e.g. minimal CI), we skip
    # silently — the interactive HTML still embeds the chart.
    try:
        import plotly.io as pio
        hero_png = pio.to_image(
            fig_hero, format="png", width=1600, height=900, scale=2,
        )
    except Exception as e:
        hero_png = b""
        print(f"  (hero PNG export skipped: {e})", flush=True)

    plot_html = {
        "hero": fig_html(fig_hero, "fig_hero"),
        "pop": fig_html(fig_pop, "fig_pop"),
        "flow": fig_html(fig_flow, "fig_flow"),
        "cohort": fig_html(fig_cohort, "fig_cohort"),
        "final": fig_html(fig_final, "fig_final"),
    }
    plotlyjs = plotlyjs_inline()
    built_at = built_at_utc()

    # --- headline numbers ---------------------------------------------
    final_active = int(final_pop[STATE_ACTIVE] + final_pop[STATE_SUPER])
    final_churned = int(final_pop[STATE_CHURNED])
    final_at_risk = int(final_pop[STATE_AT_RISK])
    final_new = int(final_pop[STATE_NEW])

    # Where did the most recent month's daily flow land?
    recent_window = min(28, n_days)
    if recent_window > 0:
        recent_new = int(inflow_new[-recent_window:].sum())
        recent_resurrect = int(inflow_resurrect[-recent_window:].sum())
        recent_to_churned = int(outflow_to_churned[-recent_window:].sum())
        net_recent = recent_new + recent_resurrect - recent_to_churned
    else:
        recent_new = recent_resurrect = recent_to_churned = net_recent = 0

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Is Bluesky growing or shrinking?</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>{SHARED_CSS}</style>
<script>{plotlyjs}</script>
</head>
<body>
<div class="wrap">

<div class="eyebrow">An analysis · snapshot {snapshot_date} · {at_risk_hours}h at-risk · {churn_days}d churn · super = ≥{super_threshold} actions / {super_window_hours}h</div>
<h1>Is Bluesky <span class="accent">growing or shrinking</span>?</h1>
<p class="lede">
  We followed every observed actor on Bluesky through five
  states — <em>new</em>, <em>active</em>, <em>super</em>, <em>at-risk</em>,
  and <em>churned</em> — across the full event history available in
  the snapshot ({fmt_int(n_actors)} actors, {fmt_int(n_events)} actions).
  At {snapshot_date} the active pool was
  <strong>{fmt_int(final_active)}</strong>;
  {fmt_int(final_at_risk)} had gone quiet for &gt;{at_risk_hours}h
  but &lt;{churn_days}d, and
  <strong>{fmt_int(final_churned)}</strong> had crossed the
  {churn_days}-day churn line. Existing accounts as of
  {existing_baseline_date} are seeded "active" — see Methodology.
</p>

<div class="stats">
  <div class="stat">
    <div class="v brand">{fmt_int(final_active)}</div>
    <div class="l">active + super at snapshot</div>
    <div class="sub">{100.0 * final_active / total_final:.1f}% of observed actors</div>
  </div>
  <div class="stat">
    <div class="v bad">{fmt_int(final_churned)}</div>
    <div class="l">churned at snapshot</div>
    <div class="sub">no activity in {churn_days}+ days</div>
  </div>
  <div class="stat">
    <div class="v">{fmt_int(recent_new)}</div>
    <div class="l">new activations, last 28 days</div>
    <div class="sub">first-ever events graduating to active</div>
  </div>
  <div class="stat">
    <div class="v {'bad' if net_recent < 0 else 'brand'}">{net_recent:+,}</div>
    <div class="l">net active-pool change, last 28d</div>
    <div class="sub">new + resurrected − churned</div>
  </div>
</div>

<section>
  <div class="kicker">Hero · {snapshot_date}</div>
  <h2>The active pool, regime-shaded.</h2>
  <p>
    The line is the active+super population at the end of each week
    over the last ~18 months. The colored background classifies each
    week into one of four regimes:
    <span style="background:{REGIME_COLORS[REGIME_GROWTH]};padding:1px 6px;border-radius:3px">growth</span>,
    <span style="background:{REGIME_COLORS[REGIME_NO_NEW]};padding:1px 6px;border-radius:3px">no new users</span>,
    <span style="background:{REGIME_COLORS[REGIME_LEAKY_ONBOARDING]};padding:1px 6px;border-radius:3px">leaky onboarding</span>, or
    <span style="background:{REGIME_COLORS[REGIME_CHURNING_ACTIVE]};padding:1px 6px;border-radius:3px">churning active users</span>.
    Classification is based on weekly inflow / outflow plus the tenure
    profile of churned actors — see Methodology.
  </p>
  <div class="figure">{plot_html["hero"]}</div>
</section>

<section>
  <div class="kicker">Finding 01</div>
  <h2>The shape of the population over time.</h2>
  <p>
    Stacked counts of every observed actor by state at each day. If the
    blue (active) band is widening, the platform is growing in
    engaged-headcount terms. If the red band dominates and grows, the
    long-term story is contraction, even with steady signups.
  </p>
  <div class="figure">{plot_html["pop"]}</div>
</section>

<section>
  <div class="kicker">Finding 02</div>
  <h2>What's the daily growth dynamic — new users, resurrections, or churn?</h2>
  <p>
    This decomposes net change in the active pool every day. <strong>New
    activations</strong> (green) are first-time graduates. <strong>Resurrections</strong>
    are previously-churned accounts coming back. <strong>Recoveries</strong> are
    at-risk users who returned before the {churn_days}d line. The red bar is
    daily churn flow out of the at-risk pool. If green is shrinking and
    red is growing, top-of-funnel is collapsing; if green is steady and
    red is growing, retention is the problem.
  </p>
  <div class="figure">{plot_html["flow"]}</div>
</section>

<section>
  <div class="kicker">Finding 03</div>
  <h2>Are recent cohorts retaining worse than older ones?</h2>
  <p>
    Each bar is a month-of-first-seen cohort. Blue is the share still
    active at {snapshot_date}; red is the share churned. A trend where
    recent cohorts churn faster than older ones is the signature of a
    leaky-bucket onboarding problem; the inverse — long-tenured users
    bleeding out — points at platform-level engagement decay.
  </p>
  <div class="figure">{plot_html["cohort"]}</div>
</section>

<section>
  <div class="kicker">Finding 04</div>
  <h2>Where did everyone end up?</h2>
  <p>
    Composition of every observed actor at the snapshot date. The
    churned bar is the cumulative cost of all the daily outflows above.
  </p>
  <div class="figure">{plot_html["final"]}</div>
</section>

<section>
  <div class="kicker">Finding 05 · Markov steady-state</div>
  <h2>If today's transition rates persisted forever, where would Bluesky converge?</h2>
  <p>
    Treating user-state evolution as a Markov chain with transition
    probabilities estimated from the last {markov_window_days} days
    gives a stationary distribution π. If the current per-hour
    transition rates held indefinitely and the actor universe were
    closed, the population would converge to these state shares. A
    steady-state with a substantial <em>churned</em> fraction means the
    current dynamics are structurally lossy — the platform is bleeding
    even without any change in user behavior. A high
    <em>active</em>+<em>super</em> share means the current rates can
    sustain engagement long-term.
  </p>
  {_render_markov_table(markov)}
</section>

<footer>
  <p>
    <strong>Methodology.</strong> Per-actor events are unioned across
    <code>likes</code>, <code>reposts</code>, <code>follows</code>, and
    <code>posts_from_records</code> from the at-snapshot staging
    parquets (full history, not the 90-day windowed
    <code>snapshot.duckdb</code>). Events are pre-aggregated to
    <code>(did_id, hour)</code> in DuckDB; a per-actor state machine
    walks each timeline in hour order and emits state-interval deltas.
    Population at hour <em>h</em> is the cumulative sum of deltas
    through <em>h</em>. Actors with any pre-{existing_baseline_date}
    event are seeded <code>active</code> at the baseline; their
    pre-baseline trailing-7d counter is intentionally empty (we can't
    reconstruct what their super-status was without their pre-baseline
    activity at finer grain). Hourly resolution; daily downsampling for
    the charts. Transitions firing after {snapshot_date} are not emitted
    (they're censored by the analysis horizon). Built {built_at}.
  </p>
</footer>

</div>
</body>
</html>
"""

    sidecar = {
        "snapshot_date": snapshot_date,
        "at_risk_hours": at_risk_hours,
        "churn_days": churn_days,
        "super_threshold": super_threshold,
        "super_window_hours": super_window_hours,
        "existing_baseline_date": existing_baseline_date,
        "n_actors": n_actors,
        "n_events": n_events,
        "built_at_utc": built_at,
        "final_population": {
            STATE_NAMES[s]: int(final_pop[s]) for s in range(N_STATES)
        },
        "recent_28d": {
            "new_activations": recent_new,
            "resurrections": recent_resurrect,
            "to_churned": recent_to_churned,
            "net_active_pool_change": net_recent,
        },
        "cohort_outcomes": [
            {
                "cohort": _fmt_ym(k),
                "size": cohort_size[k],
                "active_pct": coh_active_pct[i],
                "churned_pct": coh_churned_pct[i],
            }
            for i, k in enumerate(cohort_keys)
        ],
        "markov": {
            "window_days": markov_window_days,
            "P": markov["P"].tolist(),
            "steady_state": {
                STATE_NAMES[s]: float(markov["pi"][s]) for s in range(N_STATES)
            },
            "dwell_hours": {
                STATE_NAMES[s]: (None if not np.isfinite(markov["dwell_hours"][s])
                                 else float(markov["dwell_hours"][s]))
                for s in range(N_STATES)
            },
        },
        "regimes": [
            {"week": d, "regime": r}
            for d, r in zip(weekly["week_dates"], weekly["regimes"])
        ],
        "current_regime": weekly["regimes"][-1] if weekly["regimes"] else None,
    }
    return html.encode("utf-8"), sidecar, hero_png


def _fmt_ym(ym: int) -> str:
    y, m = divmod(ym, 100)
    return f"{y:04d}-{m:02d}"


# ---------------------------------------------------------------------------
# Hero chart construction
# ---------------------------------------------------------------------------


def _build_hero_figure(
    *,
    weekly: dict,
    snapshot_date: str,
    at_risk_hours: int,
    churn_days: int,
    markov: dict,
    markov_window_days: int,
    hero_lookback_weeks: int,
):
    """Build the postable hero line chart with per-week regime shading.

    The figure spans the last `hero_lookback_weeks` weeks (default 78
    ≈ 18 months) ending at the snapshot. The primary y-axis carries the
    weekly active+super population as a line; the secondary y-axis
    carries weekly new-activations and churn-outflow bars for context.
    Background is colored by per-week regime so the dominant story is
    readable at a glance.
    """
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    week_dates = weekly["week_dates"]
    regimes = weekly["regimes"]
    pop = weekly["pop"]
    n_weeks = pop.shape[1]

    if n_weeks == 0:
        # Empty placeholder figure so the wider pipeline still runs.
        return go.Figure().update_layout(
            template="bsky",
            title="Insufficient data for hero chart",
            height=900,
        )

    # Slice to the trailing window.
    lo = max(0, n_weeks - hero_lookback_weeks)
    week_dates = week_dates[lo:]
    regimes = regimes[lo:]
    pop = pop[:, lo:]
    active = (pop[STATE_ACTIVE] + pop[STATE_SUPER]).astype(np.int64)
    new_in = weekly["new"][lo:]
    churn_out = weekly["to_churned"][lo:]
    resurrect = weekly["resurrect"][lo:]

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # ---- background shading: one vrect per contiguous regime run -----
    # Contiguous runs of the same regime get a single rectangle —
    # cleaner visual than per-week stripes.
    runs = _contiguous_runs(regimes)
    for s_i, e_i, r in runs:
        # Each week_date is the Monday of that week; the rect spans
        # from this Monday to next Monday for the last week in the run.
        x0 = week_dates[s_i]
        # The end of the run: take the start of the week after the last
        # one in the run, capped at the last available week's "next"
        # date so we don't extend past snapshot.
        if e_i + 1 < len(week_dates):
            x1 = week_dates[e_i + 1]
        else:
            # Compute the next Monday from the final week_date.
            last_dt = datetime.fromisoformat(week_dates[e_i])
            x1 = (last_dt + timedelta(days=7)).date().isoformat()
        fig.add_vrect(
            x0=x0, x1=x1,
            fillcolor=REGIME_COLORS[r],
            line_width=0, layer="below",
        )

    # ---- regime label annotations on each run, top of plot ----------
    # Only label runs ≥ 3 weeks wide; shorter ones would crowd.
    y_top = float(active.max()) * 1.05 if len(active) else 1.0
    for s_i, e_i, r in runs:
        if e_i - s_i + 1 < 3:
            continue
        mid = (s_i + e_i) // 2
        fig.add_annotation(
            x=week_dates[mid],
            y=y_top,
            text=REGIME_LABELS[r],
            showarrow=False,
            yref="y",
            xref="x",
            font=dict(size=11, color="#475569"),
            opacity=0.85,
        )

    # ---- main line: active pool over time ----------------------------
    fig.add_trace(
        go.Scatter(
            x=week_dates, y=active.tolist(),
            mode="lines",
            name="Active + super",
            line=dict(color=BRAND, width=3.5),
            hovertemplate="%{x}<br><b>%{y:,}</b> active+super<extra></extra>",
        ),
        secondary_y=False,
    )

    # ---- secondary axis: new activations and churn outflow as bars --
    fig.add_trace(
        go.Bar(
            x=week_dates, y=new_in.tolist(),
            name="New activations / week",
            marker=dict(color="rgba(22, 163, 74, 0.55)"),
            hovertemplate="%{x}<br>+%{y:,} new activations<extra></extra>",
        ),
        secondary_y=True,
    )
    fig.add_trace(
        go.Bar(
            x=week_dates, y=(-churn_out).tolist(),
            name="Churn outflow / week",
            marker=dict(color="rgba(239, 68, 68, 0.55)"),
            customdata=churn_out.tolist(),
            hovertemplate="%{x}<br>−%{customdata:,} churned<extra></extra>",
        ),
        secondary_y=True,
    )

    # ---- steady-state callout (right-hand annotation) ----------------
    pi = markov["pi"]
    ss_active = float(pi[STATE_ACTIVE] + pi[STATE_SUPER])
    ss_churned = float(pi[STATE_CHURNED])
    callout = (
        f"<b>Steady-state forecast</b><br>"
        f"(if last {markov_window_days}d rates persist)<br>"
        f"active: {ss_active*100:.1f}%<br>"
        f"churned: {ss_churned*100:.1f}%"
    )
    if len(week_dates) > 0:
        fig.add_annotation(
            x=week_dates[-1], y=active[-1] if len(active) else 0,
            text=callout,
            showarrow=True,
            arrowhead=2, arrowsize=1, arrowwidth=1.5, arrowcolor="#475569",
            ax=-80, ay=-100,
            bgcolor="white", bordercolor="#cbd5e1", borderwidth=1,
            font=dict(size=12, color="#1d2433"),
            align="left",
        )

    current_regime = regimes[-1] if regimes else "n/a"
    fig.update_layout(
        template="bsky",
        title=dict(
            text=f"<b>Is Bluesky growing or shrinking?</b>  ·  "
                 f"active pool, weekly, last 18 months · "
                 f"current regime: <b>{REGIME_LABELS.get(current_regime, current_regime)}</b>",
            x=0.02, xanchor="left",
            font=dict(size=18),
        ),
        height=900,
        width=1600,
        barmode="relative",
        bargap=0.05,
        legend=dict(
            orientation="h", y=-0.10, x=0.5, xanchor="center",
            font=dict(size=13),
        ),
        margin=dict(l=80, r=80, t=90, b=80),
        annotations=list(fig.layout.annotations) + [
            dict(
                xref="paper", yref="paper",
                x=0.0, y=1.06, xanchor="left", yanchor="bottom",
                text=(f"<span style='color:#5b6472;font-size:12px'>"
                      f"thresholds: at-risk &gt;{at_risk_hours}h · "
                      f"churned &gt;{churn_days}d  ·  "
                      f"snapshot {snapshot_date}</span>"),
                showarrow=False,
            ),
        ],
    )
    fig.update_xaxes(title="Week (Monday)")
    fig.update_yaxes(title_text="Active + super population", secondary_y=False)
    fig.update_yaxes(
        title_text="New activations  /  churned (per week)",
        secondary_y=True, showgrid=False,
    )
    return fig


def _contiguous_runs(labels: list[str]) -> list[tuple[int, int, str]]:
    """Return [(start_idx, end_idx_inclusive, label), ...] for runs of
    identical labels in `labels`. Used for vrect shading."""
    out = []
    if not labels:
        return out
    s = 0
    for i in range(1, len(labels)):
        if labels[i] != labels[s]:
            out.append((s, i - 1, labels[s]))
            s = i
    out.append((s, len(labels) - 1, labels[s]))
    return out


def _render_markov_table(markov: dict) -> str:
    """HTML table summarizing the steady-state distribution + per-state
    dwell time. Used in the Finding 05 section.
    """
    pi = markov["pi"]
    dwell = markov["dwell_hours"]
    rows = []
    for s in range(N_STATES):
        name = STATE_NAMES[s]
        pct = pi[s] * 100
        d = dwell[s]
        if not np.isfinite(d):
            dwell_str = "∞ (absorbing)"
        elif d >= 24:
            dwell_str = f"{d / 24:.1f} days"
        else:
            dwell_str = f"{d:.1f} hours"
        rows.append(
            f"<tr>"
            f"<td><span style='display:inline-block;width:10px;height:10px;"
            f"background:{STATE_COLORS[name]};border-radius:2px;"
            f"margin-right:6px'></span>{name}</td>"
            f"<td style='text-align:right'>{pct:.2f}%</td>"
            f"<td style='text-align:right'>{dwell_str}</td>"
            f"</tr>"
        )
    return (
        "<div class='figure'>"
        "<table style='width:100%;border-collapse:collapse;font-size:14px'>"
        "<thead><tr style='border-bottom:1px solid var(--rule);"
        "text-align:left;color:var(--muted)'>"
        "<th style='padding:8px 4px'>State</th>"
        "<th style='padding:8px 4px;text-align:right'>Steady-state share</th>"
        "<th style='padding:8px 4px;text-align:right'>Mean dwell time</th>"
        "</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
        "</div>"
    )
