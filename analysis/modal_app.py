"""Modal app dispatcher for snapshot analyses.

Each analysis lives in its own submodule:

  analysis/likes.py      analyze_likes
  analysis/ratio.py      analyze_ratio
  analysis/attrition.py  analyze_attrition
  analysis/blocks.py     analyze_blocks  (needs scipy)

This file is just the Modal glue: it defines the app, the per-image
remote functions, and a local entrypoint that dispatches based on the
`--analysis` flag.

  modal run analysis/modal_app.py --analysis likes
  modal run analysis/modal_app.py --analysis ratio --snapshot-date 2026-05-11
  modal run analysis/modal_app.py --analysis attrition --inactivity-days 30
  modal run analysis/modal_app.py --analysis blocks --background

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
                 "pyarrow==16.1.0", "kaleido==0.2.1")
)
analysis_image = _base_pkgs.add_local_python_source("analysis")

# Spectral image adds scipy for the blocks SVD.
spectral_image = (
    _base_pkgs
    .pip_install("scipy==1.13.0")
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
    memory=64 * 1024,
    ephemeral_disk=512 * 1024,
)
def analyze_growth(
    snapshot_date: str = "2026-04-28",
    at_risk_hours: int = 48,
    churn_days: int = 14,
    super_threshold: int = 50,
    super_window_hours: int = 168,
    existing_baseline_date: str = "2025-01-01",
    lookback_days: int = 0,  # 0 ⇒ full history sentinel
) -> bytes:
    from analysis.growth import run
    lookback = None if lookback_days <= 0 else lookback_days
    raw_dir = f"{OUT_VOL_DIR}/raw/{snapshot_date}"
    con = _open_snapshot(snapshot_date, memory_limit="56GiB")
    html, sidecar, hero_png = run(
        con, snapshot_date,
        raw_dir=raw_dir,
        at_risk_hours=at_risk_hours,
        churn_days=churn_days,
        super_threshold=super_threshold,
        super_window_hours=super_window_hours,
        existing_baseline_date=existing_baseline_date,
        lookback_days=lookback,
    )
    return _persist(snapshot_date, "growth", html, sidecar, hero_png=hero_png)


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
    "growth": {
        "fn": analyze_growth,
        "out_name": lambda d: f"growth_{d}.html",
        "vol_path": lambda d: f"/vol-out/var/analysis/{d}/growth.html",
        "kwargs": lambda d, *, at_risk_hours, churn_days, super_threshold,
                          super_window_hours, existing_baseline_date,
                          lookback_days, **rest: {
            "snapshot_date": d,
            "at_risk_hours": at_risk_hours,
            "churn_days": churn_days,
            "super_threshold": super_threshold,
            "super_window_hours": super_window_hours,
            "existing_baseline_date": existing_baseline_date,
            "lookback_days": lookback_days,
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
    background: bool = False,
) -> None:
    """Dispatch to one of the snapshot analyses.

    Args:
      analysis: which analysis to run — likes, ratio, attrition, blocks.
      snapshot_date: which snapshot in /vol-out/var/snapshot/<date>/ to read.
      window_days: time-window length for windowed analyses (ratio).
      inactivity_days: inactivity threshold for attrition.
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
