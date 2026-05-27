"""Local benchmark for the growth state-machine analysis.

Builds a scaled synthetic snapshot (cached on disk between runs) and
times the two heavy phases of `analysis.growth.run`:

  1. DuckDB pre-aggregation (`_materialize_per_hour`)
  2. Per-actor state-machine streaming (`process_actor` loop)

Run::

    python tests/analysis/bench_growth.py \
        --actors 100000 --events-per-actor 50 --history-days 365

Prints wall time, actors/sec, events/sec for each phase plus a sidecar
sanity-check. Used to drive vectorization / parallelization work — the
streaming loop is the optimization target.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
import time
from pathlib import Path

# Make the project root importable when run from anywhere.
_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "tests" / "analysis"))

import duckdb  # noqa: E402

from analysis import growth  # noqa: E402
from synth import make_synthetic_snapshot  # noqa: E402


def _cache_path(n_actors: int, n_events: int, history_days: int, seed: int) -> Path:
    """Stable cache filename keyed by scale parameters."""
    key = f"{n_actors}-{n_events}-{history_days}-{seed}"
    digest = hashlib.sha1(key.encode()).hexdigest()[:10]
    cache_dir = Path(os.environ.get("GROWTH_BENCH_CACHE",
                                    "/tmp/growth_bench_cache"))
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"snap_{digest}.duckdb"


def build_or_load_snapshot(
    *, n_actors: int, events_per_actor: int, history_days: int, seed: int,
) -> tuple[str, str]:
    """Return (path, snapshot_date)."""
    n_likes = max(1, int(events_per_actor * n_actors * 0.55))
    n_reposts = max(1, int(events_per_actor * n_actors * 0.10))
    n_follows = max(1, int(events_per_actor * n_actors * 0.25))
    n_posts = max(1, int(events_per_actor * n_actors * 0.10))

    path = _cache_path(n_actors, events_per_actor, history_days, seed)
    snapshot_date = "2026-04-28"
    if path.exists():
        print(f"[bench] using cached snapshot {path}", flush=True)
        return str(path), snapshot_date

    print(
        f"[bench] building synthetic snapshot ({n_actors:,} actors, "
        f"{n_likes + n_reposts + n_follows + n_posts:,} events, "
        f"{history_days}d) at {path}",
        flush=True,
    )
    t0 = time.time()
    make_synthetic_snapshot(
        path,
        seed=seed,
        snapshot_date=snapshot_date,
        n_actors=n_actors,
        n_posts=n_posts,
        n_likes=n_likes,
        n_follows=n_follows,
        n_reposts=n_reposts,
        n_blocks=1,  # not exercised
        history_days=history_days,
    )
    print(f"[bench] built in {time.time() - t0:.1f}s", flush=True)
    return str(path), snapshot_date


def _time_materialize(con, snapshot_date: str) -> float:
    snap_ts = f"{snapshot_date} 23:59:59"
    t0 = time.time()
    growth._materialize_per_hour(
        con,
        raw_dir=None,
        snap_ts=snap_ts,
        plausible_lo_ts="2022-01-01 00:00:00",
        lookback_lo_ts="2022-01-01 00:00:00",
        log=False,
    )
    return time.time() - t0


def _time_state_machine(
    con, snapshot_date: str, *, n_workers: int = 1, chunk_size: int = 4_000,
) -> tuple[float, int, int]:
    """Run the full state machine via `_run_state_machine`.

    Returns (sec, n_actors, n_events). `n_workers=1` exercises the
    in-process hot path; `n_workers>1` exercises the ProcessPoolExecutor
    fan-out.
    """
    from datetime import datetime

    snap_h = growth._to_hour_index(
        datetime.fromisoformat(f"{snapshot_date}T23:59:59")
    )
    baseline_h = growth._to_hour_index(
        datetime.fromisoformat("2025-01-01T00:00:00")
    )

    t0 = time.time()
    (_pop, _trans, _outcomes, _size, _churn, n_actors, n_events) = \
        growth._run_state_machine(
            con,
            at_risk_h=48, churn_h=14 * 24,
            super_h=168, super_thr=50,
            baseline_h=baseline_h, end_h=snap_h,
            n_workers=n_workers, chunk_size=chunk_size,
            log=False,
        )
    return time.time() - t0, n_actors, n_events


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--actors", type=int, default=50_000)
    ap.add_argument("--events-per-actor", type=int, default=40)
    ap.add_argument("--history-days", type=int, default=365)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--skip-rebuild", action="store_true",
                    help="Use the existing cached snapshot even if scale args differ")
    ap.add_argument("--workers", type=int, default=1,
                    help="Worker processes for the state machine (1 = serial).")
    ap.add_argument("--chunk-size", type=int, default=4_000,
                    help="Actors per ProcessPoolExecutor task in parallel mode.")
    args = ap.parse_args()

    path, snapshot_date = build_or_load_snapshot(
        n_actors=args.actors,
        events_per_actor=args.events_per_actor,
        history_days=args.history_days,
        seed=args.seed,
    )

    con = duckdb.connect(path, read_only=True)
    con.execute("PRAGMA threads=8")

    mat_sec = _time_materialize(con, snapshot_date)
    n_per_hour = con.execute("SELECT COUNT(*) FROM per_hour_sorted").fetchone()[0]
    print(f"[bench] materialize_per_hour: {mat_sec*1000:8.1f} ms "
          f"({n_per_hour:,} (actor,hour) rows)", flush=True)

    sm_sec, n_actors, n_events = _time_state_machine(
        con, snapshot_date,
        n_workers=args.workers, chunk_size=args.chunk_size,
    )
    print(f"[bench] state_machine ({args.workers}w): {sm_sec*1000:8.1f} ms "
          f"({n_actors:,} actors, {n_events:,} events, "
          f"{n_actors / max(sm_sec, 1e-9):,.0f} actor/s, "
          f"{n_events / max(sm_sec, 1e-9):,.0f} event/s)", flush=True)
    print(f"[bench] total:                {(mat_sec + sm_sec)*1000:8.1f} ms",
          flush=True)


if __name__ == "__main__":
    main()
