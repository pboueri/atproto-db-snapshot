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

import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone

import numpy as np

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

    if log:
        print("=== streaming per-actor state machine ===", flush=True)

    n_hours = snap_h + 1  # 0..snap_h inclusive
    pop_delta = np.zeros((N_STATES, n_hours + 1), dtype=np.int64)  # +1 slack
    transitions = Counter()  # (hour_idx, from_state, to_state) -> int

    # Cohort decomposition: each actor is tagged with their cohort
    # (year-month of first_seen) and we tally cohort-level outcomes at
    # snap_h. Used for the new-user retention chart.
    cohort_outcomes = defaultdict(Counter)  # (cohort_ym) -> Counter[state]
    cohort_size = Counter()

    # Per-week churn tenure buckets. Lets us tell "leaky onboarding"
    # (young cohorts churning) from "churning active" (old guard
    # bleeding). Buckets are < 90d, 90-180d, > 180d since first_seen.
    churn_buckets = Counter()  # (week_idx, age_bucket) -> count

    t0 = time.time()
    n_actors = 0
    n_events = 0
    for did_id, hours, counts, cohort_ym in _iter_actor_groups(con, log=log):
        is_existing = hours[0] < baseline_h
        process_actor(
            hours, counts,
            at_risk_h=at_risk_hours,
            churn_h=churn_hours,
            super_h=super_window_hours,
            super_thr=super_threshold,
            baseline_h=baseline_h,
            end_h=snap_h,
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

    t0 = time.time()
    # First materialize all_events so the cohort + per-hour passes don't
    # re-scan the parquets twice.
    con.execute(
        f"""
        CREATE OR REPLACE TEMPORARY TABLE all_events AS
        SELECT did_id, created_at,
               DATEDIFF('hour',
                        TIMESTAMP '1970-01-01 00:00:00',
                        DATE_TRUNC('hour', created_at))::BIGINT AS hour_idx
        FROM (
          {events_sql}
        )
        WHERE created_at >= TIMESTAMP '{lookback_lo_ts}'
        """
    )
    if log:
        n = con.execute("SELECT COUNT(*) FROM all_events").fetchone()[0]
        print(f"  ({time.time() - t0:.1f}s) all_events rows = {n:,}", flush=True)

    t0 = time.time()
    con.execute(
        """
        CREATE OR REPLACE TEMPORARY TABLE actor_cohort AS
        SELECT did_id,
               (EXTRACT(year FROM MIN(created_at)) * 100
                + EXTRACT(month FROM MIN(created_at)))::INT AS cohort_ym
        FROM all_events
        GROUP BY did_id
        """
    )
    if log:
        print(f"  ({time.time() - t0:.1f}s) actor_cohort built", flush=True)

    t0 = time.time()
    con.execute(
        """
        CREATE OR REPLACE TEMPORARY TABLE per_hour_sorted AS
        SELECT e.did_id,
               e.hour_idx,
               COUNT(*)::INT AS n_actions,
               c.cohort_ym
        FROM all_events e
        JOIN actor_cohort c USING (did_id)
        GROUP BY e.did_id, e.hour_idx, c.cohort_ym
        ORDER BY e.did_id, e.hour_idx
        """
    )
    if log:
        n = con.execute("SELECT COUNT(*) FROM per_hour_sorted").fetchone()[0]
        print(
            f"  ({time.time() - t0:.1f}s) per_hour_sorted rows = {n:,}",
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
        start_h = int(hours[0])
        idx0 = 0
        last_active_h = int(hours[0])  # the first event itself is the activity

    if start_h > end_h:
        # Actor only appears after the analysis window — skip.
        return
    pop_delta[state, start_h] += 1
    cohort_size[cohort_ym] += 1

    # ---- helpers --------------------------------------------------------
    # Trailing window for super eligibility. A small Python list used as
    # a ring buffer; for ~99% of actors length stays < 10.
    window_h: list[int] = []
    window_n: list[int] = []
    window_sum = 0

    def go(h: int, new_state: int) -> int:
        """Apply transition at clamped hour `h`. Returns new state."""
        nonlocal state
        if new_state == state:
            return state
        clamp_h = h if h <= end_h else end_h
        # Don't log a transition that fires past the censoring horizon.
        if h <= end_h:
            pop_delta[state, clamp_h] -= 1
            pop_delta[new_state, clamp_h] += 1
            transitions[(clamp_h, state, new_state)] += 1
            # Record tenure-at-churn for regime classification.
            if (churn_buckets is not None
                    and new_state == STATE_CHURNED
                    and state == STATE_AT_RISK):
                age_h = clamp_h - true_first_seen_h
                if age_h < 90 * 24:
                    bucket = 0
                elif age_h < 180 * 24:
                    bucket = 1
                else:
                    bucket = 2
                week_idx = clamp_h // (7 * 24)
                churn_buckets[(week_idx, bucket)] += 1
            state = new_state
        return state

    # ---- main loop ------------------------------------------------------
    # For new actors, the first event is special — we entered NEW above
    # and the trailing window picks it up here.
    if not is_existing:
        window_h.append(int(hours[0]))
        window_n.append(int(counts[0]))
        window_sum += int(counts[0])
        # A first-hour super promotion is possible but vanishingly rare;
        # only apply if state is something other than NEW. For NEW we
        # let the graduate step handle promotion.
        idx0 = 1  # already consumed event 0

    # Walk subsequent events. Between events we fire scheduled
    # transitions (at_risk / churn / super-decay) lazily.
    for k in range(idx0, len(hours)):
        h = int(hours[k])
        n = int(counts[k])

        # Compute scheduled deadlines from the last known last_active.
        if last_active_h >= 0:
            at_risk_fire = last_active_h + at_risk_h
            churn_fire = last_active_h + churn_h
        else:
            at_risk_fire = baseline_h + at_risk_h if is_existing else None
            churn_fire = baseline_h + churn_h if is_existing else None

        # NEW graduation. For new users the graduation hour is
        # first_seen + at_risk_h; before then state is NEW. If this
        # event arrives after graduation and we haven't transitioned
        # yet, transition NEW -> ACTIVE at the graduation moment.
        if state == STATE_NEW:
            grad_h = start_h + at_risk_h
            if h >= grad_h:
                go(grad_h, STATE_ACTIVE)
                # Now cascade the at_risk / churn fires that happened
                # between grad_h and h, based on last_active_h.
                if last_active_h >= 0:
                    arf = last_active_h + at_risk_h
                    if state in (STATE_ACTIVE, STATE_SUPER) and h >= arf:
                        go(arf, STATE_AT_RISK)
                    cf = last_active_h + churn_h
                    if state == STATE_AT_RISK and h >= cf:
                        go(cf, STATE_CHURNED)
        else:
            # Fire active->at_risk and at_risk->churned if due.
            if at_risk_fire is not None and h >= at_risk_fire \
                    and state in (STATE_ACTIVE, STATE_SUPER):
                go(at_risk_fire, STATE_AT_RISK)
            if churn_fire is not None and h >= churn_fire and state == STATE_AT_RISK:
                go(churn_fire, STATE_CHURNED)

        # Process super-decay events between the last_active and h.
        # We pop expiring window entries one at a time, demoting if the
        # remaining sum falls below threshold.
        while window_h and window_h[0] + super_h <= h:
            old_h = window_h.pop(0)
            old_n = window_n.pop(0)
            window_sum -= old_n
            expire_h = old_h + super_h
            if state == STATE_SUPER and window_sum < super_thr:
                go(expire_h, STATE_ACTIVE)

        # Apply the event itself.
        if state == STATE_CHURNED or state == STATE_AT_RISK:
            go(h, STATE_ACTIVE)
        # If NEW, the action keeps them NEW until grad_h (already handled).

        window_h.append(h)
        window_n.append(n)
        window_sum += n
        if state == STATE_ACTIVE and window_sum >= super_thr:
            go(h, STATE_SUPER)
        last_active_h = h

    # ---- tail: project deadlines forward to end_h -----------------------
    if last_active_h < 0:
        # Existing actor with no post-baseline activity; their effective
        # "last_active" is baseline_h.
        last_active_h = baseline_h

    # If still NEW at end of stream: their only-ever event is `start_h`.
    # If end_h > start_h + at_risk_h they graduate to ACTIVE, then cascade.
    if state == STATE_NEW:
        grad_h = start_h + at_risk_h
        if end_h >= grad_h:
            go(grad_h, STATE_ACTIVE)

    # ACTIVE/SUPER tail: super decay + at_risk firing.
    if state in (STATE_ACTIVE, STATE_SUPER):
        at_risk_fire = last_active_h + at_risk_h
        # Decay super before at_risk_fire if applicable.
        while window_h and window_h[0] + super_h <= min(end_h, at_risk_fire):
            old_h = window_h.pop(0)
            old_n = window_n.pop(0)
            window_sum -= old_n
            expire_h = old_h + super_h
            if state == STATE_SUPER and window_sum < super_thr:
                go(expire_h, STATE_ACTIVE)
        if at_risk_fire <= end_h:
            go(at_risk_fire, STATE_AT_RISK)

    if state == STATE_AT_RISK:
        churn_fire = last_active_h + churn_h
        if churn_fire <= end_h:
            go(churn_fire, STATE_CHURNED)

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
