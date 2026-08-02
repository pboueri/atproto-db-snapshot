"""Modal app dispatcher for snapshot analyses.

Each analysis lives in its own submodule:

  analysis/likes.py      analyze_likes
  analysis/ratio.py      analyze_ratio
  analysis/attrition.py  analyze_attrition
  analysis/followers.py  analyze_followers
  analysis/following.py  analyze_following
  analysis/blocks.py     analyze_blocks  (needs scipy)
  analysis/lifelines.py  analyze_lifelines

This file is just the Modal glue: it defines the app, the per-image
remote functions, and a local entrypoint that dispatches based on the
`--analysis` flag.

  modal run analysis/modal_app.py --analysis likes
  modal run analysis/modal_app.py --analysis ratio --snapshot-date 2026-05-11
  modal run analysis/modal_app.py --analysis attrition --inactivity-days 30
  modal run analysis/modal_app.py --analysis blocks --background
  modal run analysis/modal_app.py --analysis lifelines --snapshot-date 2026-07-31

Each remote function reads `/vol-out/var/snapshot/<date>/snapshot.duckdb`
from the shared output volume, calls the corresponding `run()`, writes
the HTML + JSON sidecar to `/vol-out/var/analysis/<date>/`, and returns
the HTML bytes so the local entrypoint can drop a copy on the host.
"""

from __future__ import annotations

import os

import modal

from analysis.common import OUT_VOL_DIR, persist_artifact

volume_out = modal.Volume.from_name("at-snapshot-output", create_if_missing=False)

# Slim image: duckdb + plotly. Used by likes / ratio / attrition / growth.
# Growth needs numpy for the streaming state machine; pulling it into
# the base image avoids a second image variant.
# NOTE: Modal requires `add_local_*` to be the LAST step in any image
# chain. We build `_base_pkgs` (no local source) and tack the local
# source on as the terminal step in each derived image.
_base_pkgs = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install("duckdb==1.5.2", "plotly==5.22.0", "numpy==1.26.4",
                 "pyarrow==16.1.0", "kaleido==0.2.1", "numba==0.61.0")
)
analysis_image = _base_pkgs.add_local_python_source("analysis")

# Spectral image adds scipy for the blocks SVD.
spectral_image = (
    _base_pkgs
    .pip_install("scipy==1.13.0")
    .add_local_python_source("analysis")
)

# Graph image adds python-igraph for the booster / farm analysis.
graph_image = (
    _base_pkgs
    .pip_install("python-igraph==0.11.8")
    .add_local_python_source("analysis")
)

app = modal.App("at-snapshot-analysis")


def _open_snapshot(snapshot_date: str, *, memory_limit: str):
    """Open the snapshot.duckdb on the output volume in read-only mode.

    Pins threads / memory_limit / temp_directory so each analysis runs
    with predictable resource bounds inside its Modal container.
    """
    import duckdb

    db_path = f"{OUT_VOL_DIR}/snapshot/{snapshot_date}/snapshot.duckdb"
    if not os.path.exists(db_path):
        raise SystemExit(f"snapshot not found at {db_path}")
    print(f"=== open {db_path} (read-only) ===", flush=True)
    con = duckdb.connect(db_path, read_only=True)
    con.execute("PRAGMA threads=8")
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute("SET temp_directory='/tmp/duckdb_tmp'")
    os.makedirs("/tmp/duckdb_tmp", exist_ok=True)
    return con


def _persist(
    snapshot_date: str,
    basename: str,
    html: bytes,
    sidecar: dict,
    hero_png: bytes | None = None,
) -> bytes:
    out_dir = f"{OUT_VOL_DIR}/analysis/{snapshot_date}"
    path = persist_artifact(out_dir, basename, html, sidecar)
    if hero_png is not None:
        hero_path = f"{out_dir}/{basename}_hero.png"
        with open(hero_path, "wb") as f:
            f.write(hero_png)
        print(f"=== wrote {hero_path} ({len(hero_png):,} bytes) ===", flush=True)
    volume_out.commit()
    print(f"=== wrote {path} ({len(html):,} bytes) ===", flush=True)
    return html


@app.function(
    image=analysis_image,
    volumes={"/vol-out": volume_out},
    timeout=60 * 60,
    cpu=8.0,
    memory=32 * 1024,
    ephemeral_disk=512 * 1024,
)
def analyze_likes(snapshot_date: str = "2026-04-28") -> bytes:
    from analysis.likes import run
    con = _open_snapshot(snapshot_date, memory_limit="28GiB")
    html, sidecar = run(con, snapshot_date)
    return _persist(snapshot_date, "likes_concentration", html, sidecar)


@app.function(
    image=analysis_image,
    volumes={"/vol-out": volume_out},
    timeout=60 * 60,
    cpu=8.0,
    memory=32 * 1024,
    ephemeral_disk=512 * 1024,
)
def analyze_ratio(
    snapshot_date: str = "2026-04-28",
    window_days: int = 90,
) -> bytes:
    from analysis.ratio import run
    con = _open_snapshot(snapshot_date, memory_limit="28GiB")
    html, sidecar = run(con, snapshot_date, window_days=window_days)
    return _persist(snapshot_date, "ratio", html, sidecar)


@app.function(
    image=analysis_image,
    volumes={"/vol-out": volume_out},
    timeout=60 * 60 * 2,
    cpu=8.0,
    memory=64 * 1024,
    ephemeral_disk=512 * 1024,
)
def analyze_attrition(
    snapshot_date: str = "2026-04-28",
    inactivity_days: int = 30,
) -> bytes:
    from analysis.attrition import run
    con = _open_snapshot(snapshot_date, memory_limit="56GiB")
    html, sidecar = run(con, snapshot_date, inactivity_days=inactivity_days)
    return _persist(snapshot_date, "attrition", html, sidecar)


@app.function(
    image=analysis_image,
    volumes={"/vol-out": volume_out},
    timeout=60 * 60 * 4,
    cpu=8.0,
    # 128 GiB: the numba state-machine load holds ~50 GB of event arrays
    # resident, and the optional per-user state-log adds the interval
    # arrays (~5-8 GB) plus a transient pyarrow copy while writing the
    # parquet. 64 GiB left no headroom for the state-log variant.
    memory=128 * 1024,
    # Growth streams a year of likes/reposts/follows/posts into a
    # `per_hour` (did_id, hour_idx) aggregate (~2.1B rows on the
    # 2026-05-11 snapshot) and sorts it. The aggregation + sort spill
    # to /tmp; 2 TiB matches hydrate and leaves comfortable headroom.
    ephemeral_disk=2 * 1024 * 1024,
    # The pre-aggregation alone runs ~30 min; a spot preemption wipes it
    # and restarts from scratch (no DuckDB temp-table checkpoint). Pin
    # to non-preemptible so a single worker can see the job through.
    nonpreemptible=True,
)
def analyze_growth(
    snapshot_date: str = "2026-04-28",
    at_risk_hours: int = 48,
    churn_days: int = 14,
    super_threshold: int = 50,
    super_window_hours: int = 168,
    existing_baseline_date: str = "2025-01-01",
    lookback_days: int = 0,  # 0 ⇒ full history sentinel
    emit_state_log: int = 0,  # 1 ⇒ write per-user state-interval parquet
) -> bytes:
    import os

    from analysis.growth import run
    lookback = None if lookback_days <= 0 else lookback_days
    raw_dir = f"{OUT_VOL_DIR}/raw/{snapshot_date}"
    con = _open_snapshot(snapshot_date, memory_limit="56GiB")
    # Cap DuckDB temp spill below the ephemeral_disk ceiling so it fails
    # loudly instead of taking the container down on a full /tmp.
    con.execute("SET max_temp_directory_size='1800GiB'")

    state_log_path = None
    if emit_state_log:
        out_dir = f"{OUT_VOL_DIR}/analysis/{snapshot_date}"
        os.makedirs(out_dir, exist_ok=True)
        state_log_path = f"{out_dir}/growth_state_log.parquet"

    html, sidecar, hero_png = run(
        con, snapshot_date,
        raw_dir=raw_dir,
        at_risk_hours=at_risk_hours,
        churn_days=churn_days,
        super_threshold=super_threshold,
        super_window_hours=super_window_hours,
        existing_baseline_date=existing_baseline_date,
        lookback_days=lookback,
        state_log_path=state_log_path,
    )
    if state_log_path is not None:
        # The state log is written before this commit; persist it too.
        volume_out.commit()
        print(f"=== wrote {state_log_path} ===", flush=True)
    return _persist(snapshot_date, "growth", html, sidecar, hero_png=hero_png)


@app.function(
    image=analysis_image,
    volumes={"/vol-out": volume_out},
    timeout=60 * 60,
    cpu=8.0,
    memory=32 * 1024,
    ephemeral_disk=512 * 1024,
)
def analyze_followers(snapshot_date: str = "2026-04-28") -> bytes:
    from analysis.followers import run
    con = _open_snapshot(snapshot_date, memory_limit="28GiB")
    html, sidecar = run(con, snapshot_date)
    return _persist(snapshot_date, "followers_distribution", html, sidecar)


@app.function(
    image=analysis_image,
    volumes={"/vol-out": volume_out},
    timeout=60 * 60,
    cpu=8.0,
    memory=32 * 1024,
    ephemeral_disk=512 * 1024,
)
def analyze_following(snapshot_date: str = "2026-04-28") -> bytes:
    from analysis.following import run
    con = _open_snapshot(snapshot_date, memory_limit="28GiB")
    html, sidecar = run(con, snapshot_date)
    return _persist(snapshot_date, "following_distribution", html, sidecar)


@app.function(
    image=spectral_image,
    volumes={"/vol-out": volume_out},
    timeout=60 * 60 * 2,
    cpu=8.0,
    memory=128 * 1024,
    ephemeral_disk=512 * 1024,
)
def analyze_blocks(
    snapshot_date: str = "2026-04-28",
    n_items: int = 20000,
    k_components: int = 10,
) -> bytes:
    from analysis.blocks import run
    con = _open_snapshot(snapshot_date, memory_limit="80GiB")
    html, sidecar = run(
        con, snapshot_date, n_items=n_items, k_components=k_components,
    )
    return _persist(snapshot_date, "blocks_cleavage", html, sidecar)


@app.function(
    image=graph_image,
    volumes={"/vol-out": volume_out},
    timeout=60 * 60 * 3,
    cpu=16.0,
    memory=96 * 1024,
    ephemeral_disk=512 * 1024,
)
def analyze_graph_boosters(
    snapshot_date: str = "2026-05-11",
    created_after: str = "2025-01-01",
    booster_max_outdeg: int = 3,
    min_target_support: int = 5,
    top_targets: int = 100,
    build_full_graph: bool = False,
) -> bytes:
    # Default path (build_full_graph=False) classifies boosters + farms with
    # DuckDB + an igraph subgraph and fits comfortably in this container. The
    # full 1.33B-edge igraph build (build_full_graph=True) needs far more RAM —
    # run it on a dedicated high-memory function, not this one.
    from analysis.graph_boosters import run
    con = _open_snapshot(snapshot_date, memory_limit="80GiB")
    html, sidecar = run(
        con, snapshot_date,
        created_after=created_after,
        booster_max_outdeg=booster_max_outdeg,
        min_target_support=min_target_support,
        top_targets=top_targets,
        build_full_graph=build_full_graph,
    )
    return _persist(snapshot_date, "graph_boosters", html, sidecar)


@app.function(
    image=graph_image,
    volumes={"/vol-out": volume_out},
    timeout=60 * 60 * 4,
    cpu=16.0,
    memory=200 * 1024,
    ephemeral_disk=512 * 1024,
)
def analyze_graph_boosters_full(
    snapshot_date: str = "2026-05-11",
    created_after: str = "2025-01-01",
    booster_max_outdeg: int = 3,
    min_target_support: int = 5,
    top_targets: int = 100,
) -> bytes:
    # The full path: everything the cheap path does, PLUS materializing the
    # entire ~1.33B-edge directed follow graph in igraph (C-level
    # Read_Edgelist) for global structure (weak components / giant component).
    # Needs a high-memory container; DuckDB is pinned low so igraph has room.
    from analysis.graph_boosters import run
    con = _open_snapshot(snapshot_date, memory_limit="48GiB")
    html, sidecar = run(
        con, snapshot_date,
        created_after=created_after,
        booster_max_outdeg=booster_max_outdeg,
        min_target_support=min_target_support,
        top_targets=top_targets,
        build_full_graph=True,
    )
    return _persist(snapshot_date, "graph_boosters", html, sidecar)


@app.function(
    image=analysis_image,
    volumes={"/vol-out": volume_out},
    timeout=60 * 60 * 3,
    cpu=16.0,
    memory=160 * 1024,
    ephemeral_disk=1024 * 1024,
)
def analyze_lifelines(
    snapshot_date: str = "2026-07-31",
    cohort_days: int = 30,
    horizon_hours: int = 168,
    min_engagement: int = 50,
    max_posts: int = 150_000,
    n_shapes: int = 6,
    authenticity: bool = True,
    link_flagged_examples: bool = False,
) -> bytes:
    # The heaviest step is resolving the in-network flag: the distinct
    # (engager, author) pairs from the event table are joined against all
    # ~1.4B `follows` rows, which DuckDB runs as one sequential pass probing
    # a hash set. That set is what the memory here is for — DuckDB is pinned
    # well below the container so the pair table and the event table can
    # both stay resident.
    from analysis.lifelines import run
    con = _open_snapshot(snapshot_date, memory_limit="120GiB")
    html, sidecar = run(
        con, snapshot_date,
        cohort_days=cohort_days,
        horizon_hours=horizon_hours,
        min_engagement=min_engagement,
        max_posts=max_posts,
        n_shapes=n_shapes,
        authenticity=authenticity,
        link_flagged_examples=link_flagged_examples,
    )
    return _persist(snapshot_date, "lifelines", html, sidecar)


# Maps the public --analysis flag to (remote fn, output basename, extra
# kwargs-derived-from-cli). Each entry lists which CLI params it consumes
# so the dispatcher can pass through only the relevant ones.
_DISPATCH = {
    "likes": {
        "fn": analyze_likes,
        "out_name": lambda d: f"likes_concentration_{d}.html",
        "vol_path": lambda d: f"/vol-out/var/analysis/{d}/likes_concentration.html",
        "kwargs": lambda d, **rest: {"snapshot_date": d},
    },
    "ratio": {
        "fn": analyze_ratio,
        "out_name": lambda d: f"ratio_{d}.html",
        "vol_path": lambda d: f"/vol-out/var/analysis/{d}/ratio.html",
        "kwargs": lambda d, *, window_days, **rest: {
            "snapshot_date": d, "window_days": window_days,
        },
    },
    "attrition": {
        "fn": analyze_attrition,
        "out_name": lambda d: f"attrition_{d}.html",
        "vol_path": lambda d: f"/vol-out/var/analysis/{d}/attrition.html",
        "kwargs": lambda d, *, inactivity_days, **rest: {
            "snapshot_date": d, "inactivity_days": inactivity_days,
        },
    },
    "blocks": {
        "fn": analyze_blocks,
        "out_name": lambda d: f"blocks_cleavage_{d}.html",
        "vol_path": lambda d: f"/vol-out/var/analysis/{d}/blocks_cleavage.html",
        "kwargs": lambda d, **rest: {"snapshot_date": d},
    },
    "followers": {
        "fn": analyze_followers,
        "out_name": lambda d: f"followers_distribution_{d}.html",
        "vol_path": lambda d: f"/vol-out/var/analysis/{d}/followers_distribution.html",
        "kwargs": lambda d, **rest: {"snapshot_date": d},
    },
    "following": {
        "fn": analyze_following,
        "out_name": lambda d: f"following_distribution_{d}.html",
        "vol_path": lambda d: f"/vol-out/var/analysis/{d}/following_distribution.html",
        "kwargs": lambda d, **rest: {"snapshot_date": d},
    },
    "lifelines": {
        "fn": analyze_lifelines,
        "out_name": lambda d: f"lifelines_{d}.html",
        "vol_path": lambda d: f"/vol-out/var/analysis/{d}/lifelines.html",
        "kwargs": lambda d, *, cohort_days, horizon_hours, min_engagement,
                          max_posts, n_shapes, authenticity,
                          link_flagged_examples, **rest: {
            "snapshot_date": d,
            "cohort_days": cohort_days,
            "horizon_hours": horizon_hours,
            "min_engagement": min_engagement,
            "max_posts": max_posts,
            "n_shapes": n_shapes,
            "authenticity": authenticity,
            "link_flagged_examples": link_flagged_examples,
        },
    },
    "graph": {
        "fn": analyze_graph_boosters,
        "out_name": lambda d: f"graph_boosters_{d}.html",
        "vol_path": lambda d: f"/vol-out/var/analysis/{d}/graph_boosters.html",
        "kwargs": lambda d, *, created_after, booster_max_outdeg,
                          min_target_support, top_targets, build_full_graph, **rest: {
            "snapshot_date": d,
            "created_after": created_after,
            "booster_max_outdeg": booster_max_outdeg,
            "min_target_support": min_target_support,
            "top_targets": top_targets,
            "build_full_graph": build_full_graph,
        },
    },
    "graph-full": {
        "fn": analyze_graph_boosters_full,
        "out_name": lambda d: f"graph_boosters_{d}.html",
        "vol_path": lambda d: f"/vol-out/var/analysis/{d}/graph_boosters.html",
        "kwargs": lambda d, *, created_after, booster_max_outdeg,
                          min_target_support, top_targets, **rest: {
            "snapshot_date": d,
            "created_after": created_after,
            "booster_max_outdeg": booster_max_outdeg,
            "min_target_support": min_target_support,
            "top_targets": top_targets,
        },
    },
    "growth": {
        "fn": analyze_growth,
        "out_name": lambda d: f"growth_{d}.html",
        "vol_path": lambda d: f"/vol-out/var/analysis/{d}/growth.html",
        "kwargs": lambda d, *, at_risk_hours, churn_days, super_threshold,
                          super_window_hours, existing_baseline_date,
                          lookback_days, emit_state_log, **rest: {
            "snapshot_date": d,
            "at_risk_hours": at_risk_hours,
            "churn_days": churn_days,
            "super_threshold": super_threshold,
            "super_window_hours": super_window_hours,
            "existing_baseline_date": existing_baseline_date,
            "lookback_days": lookback_days,
            "emit_state_log": emit_state_log,
        },
    },
}


@app.local_entrypoint()
def main(
    analysis: str = "likes",
    snapshot_date: str = "2026-04-28",
    window_days: int = 90,
    inactivity_days: int = 30,
    at_risk_hours: int = 48,
    churn_days: int = 14,
    super_threshold: int = 50,
    super_window_hours: int = 168,
    existing_baseline_date: str = "2025-01-01",
    lookback_days: int = 0,
    emit_state_log: int = 0,
    created_after: str = "2025-01-01",
    booster_max_outdeg: int = 3,
    min_target_support: int = 5,
    top_targets: int = 100,
    build_full_graph: bool = False,
    cohort_days: int = 30,
    horizon_hours: int = 168,
    min_engagement: int = 50,
    max_posts: int = 150_000,
    n_shapes: int = 6,
    authenticity: bool = True,
    link_flagged_examples: bool = False,
    background: bool = False,
) -> None:
    """Dispatch to one of the snapshot analyses.

    Args:
      analysis: which analysis to run — likes, ratio, attrition,
        followers, following, blocks, growth, lifelines.
      snapshot_date: which snapshot in /vol-out/var/snapshot/<date>/ to read.
      window_days: time-window length for windowed analyses (ratio).
      inactivity_days: inactivity threshold for attrition.
      cohort_days: lifelines only — length of the post-creation window.
      horizon_hours: lifelines only — observation horizon applied identically
        to every post. The cohort ends one full horizon before the data does,
        so raising this trades cohort recency for the ability to see slow
        sleepers (720 = 30 days of observation).
      min_engagement: lifelines only — floor on total engagement per post.
      authenticity: lifelines only — compute the inauthentic-amplification
        axis (a second pass over the extraction's temp tables).
      link_flagged_examples: lifelines only — link out to the posts scoring
        highest on that axis. Off by default: the report ships aggregate
        rates and redacted rows because the inference is probabilistic and
        the subject is a named account.
      emit_state_log: growth only — 1 writes the per-user state-interval
        parquet to /vol-out/var/analysis/<date>/growth_state_log.parquet.
      background: spawn the remote call instead of waiting on it.
    """
    spec = _DISPATCH.get(analysis)
    if spec is None:
        raise SystemExit(
            f"unknown analysis {analysis!r}; expected one of: "
            + ", ".join(_DISPATCH)
        )

    fn = spec["fn"]
    kwargs = spec["kwargs"](
        snapshot_date,
        window_days=window_days,
        inactivity_days=inactivity_days,
        at_risk_hours=at_risk_hours,
        churn_days=churn_days,
        super_threshold=super_threshold,
        super_window_hours=super_window_hours,
        existing_baseline_date=existing_baseline_date,
        lookback_days=lookback_days,
        emit_state_log=emit_state_log,
        created_after=created_after,
        booster_max_outdeg=booster_max_outdeg,
        min_target_support=min_target_support,
        top_targets=top_targets,
        build_full_graph=build_full_graph,
        cohort_days=cohort_days,
        horizon_hours=horizon_hours,
        min_engagement=min_engagement,
        max_posts=max_posts,
        n_shapes=n_shapes,
        authenticity=authenticity,
        link_flagged_examples=link_flagged_examples,
    )
    out_name = spec["out_name"](snapshot_date)
    vol_path = spec["vol_path"](snapshot_date)

    if background:
        call = fn.spawn(**kwargs)
        print(
            f"[spawn] FunctionCall {call.object_id} — follow with "
            f"`modal app logs at-snapshot-analysis` or check "
            f"https://modal.com/apps"
        )
        print(f"[analyze] file will be at {vol_path}")
        return

    result = fn.remote(**kwargs)
    if isinstance(result, (bytes, bytearray)):
        with open(out_name, "wb") as f:
            f.write(result)
        print(f"[analyze] wrote local copy to ./{out_name}")
    else:
        print(f"[analyze] file persisted to volume at {vol_path}")
