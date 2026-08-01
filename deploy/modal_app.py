"""Modal driver for at-snapshot.

Drives the at-snapshot pipeline on a Modal container with enough disk to
hold constellation's full backup. Outputs land on a persistent Modal
Volume.

Pipeline phases map 1:1 to at-snapshot subcommands:

  modal run deploy/modal_app.py                            # full build
  modal run deploy/modal_app.py --phase mirror             # just mirror
  modal run deploy/modal_app.py --phase stage              # just stage
  modal run deploy/modal_app.py --phase hydrate            # just hydrate
  modal run deploy/modal_app.py --phase build --upload     # build + upload
  modal run deploy/modal_app.py --phase upload             # upload only

Each phase commits the Volume on success so a later container can resume
from the last good state. The mirror also commits in the background
every five minutes during the long download.

Upload requires a Modal Secret named `r2-credentials` exposing
`R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY`. Non-secret R2 settings
(bucket, account_id, prefix) live in the config file the binary loads.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from multiprocessing.pool import ThreadPool
from typing import Iterable

import modal

DUCKDB_VERSION = "1.5.2"

# Two Modal Volumes, separated by lifecycle:
#   - at-snapshot-data   ("rocks volume"): the constellation mirror,
#     ~80 GB, written once by mirror and read by every stage. Almost
#     never deleted.
#   - at-snapshot-output ("output volume"): per-run build artifacts —
#     raw/<date>/*.parquet and snapshot/<date>/snapshot.duckdb. Rotates
#     constantly; old <date>/ dirs can be removed without touching rocks.
# Splitting them gives each its own quota (no more rocks + outputs
# fighting for the same Volume cap) and matches the natural read/write
# pattern of the pipeline.
ROCKS_VOL_DIR = "/vol-rocks/var"
OUT_VOL_DIR = "/vol-out/var"

# Canonical PLC shard store, on the output volume but outside any
# `<date>/` namespace. The PLC operation log is append-only and global —
# it isn't a property of a single snapshot — so shards persist here and
# every build seeds its `raw/<date>/plc/` working dir from them. Without
# this, each build would re-walk the whole ~100M-op export from scratch
# (many hours) instead of resuming at the tail (minutes).
PLC_STORE_DIR = f"{OUT_VOL_DIR}/plc"

# Ephemeral local storage. Modal's dataset-ingestion guide explicitly
# recommends `/tmp` for transform working dirs ("Transformations should
# also typically be performed against /tmp/. This is because transforms
# can be IO intensive and IO latency is lower against local SSD."). On
# debian_slim /tmp is a regular dir on the rootfs that `ephemeral_disk`
# expands, not tmpfs — so the full 1 TiB is available here.
TMP_WORK_DIR = "/tmp/var"

# ---------------------------------------------------------------------------
# Image: Debian + Rust + libduckdb + the source tree, compiled in release.
# ---------------------------------------------------------------------------

# Anything matching these globs is excluded when shipping the repo into
# the image. `target/` is huge build output, `var/` may hold an
# in-progress rocks mirror, `.cargo/config.toml` has macOS-only paths
# that would override the image's env block.
#
# `deploy/modal_app.py` is excluded deliberately: the script lives in
# this same directory and is normally part of `.`, so any tweak to a
# function decorator (cpu / memory / disk / timeout) would invalidate
# the image hash and trigger a full Rust rebuild. The script never
# runs inside the image — Modal imports it locally to discover
# functions, then ships only the function bodies + image to remote
# workers — so excluding it from the build context is safe. Runtime
# config (`deploy/at-snapshot.toml`) stays in because the binary reads
# it via `--config`.
SOURCE_IGNORE = [
    "target",
    "var",
    "analysis",
    ".git",
    ".github",
    ".cargo/config.toml",
    "__pycache__",
    "*.pyc",
    "*.parquet",
    "*.duckdb",
    ".DS_Store",
    "deploy/modal_app.py",
]

image = (
    modal.Image.debian_slim(python_version="3.12")
    .apt_install(
        "build-essential",
        "clang",
        "libclang-dev",
        "pkg-config",
        "curl",
        "ca-certificates",
        "git",
        "unzip",
        "zlib1g-dev",
    )
    .run_commands(
        "curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal",
        f"mkdir -p /opt/duckdb/lib /opt/duckdb/include && "
        f"curl -fsSL -o /tmp/libduckdb.zip "
        f"https://github.com/duckdb/duckdb/releases/download/v{DUCKDB_VERSION}/libduckdb-linux-amd64.zip && "
        f"unzip -q /tmp/libduckdb.zip -d /tmp/libduckdb && "
        f"cp /tmp/libduckdb/libduckdb.so /opt/duckdb/lib/ && "
        f"cp /tmp/libduckdb/duckdb.h /tmp/libduckdb/duckdb.hpp /opt/duckdb/include/ && "
        f"rm -rf /tmp/libduckdb /tmp/libduckdb.zip",
    )
    .env(
        {
            "DUCKDB_LIB_DIR": "/opt/duckdb/lib",
            "DUCKDB_INCLUDE_DIR": "/opt/duckdb/include",
            "LD_LIBRARY_PATH": "/opt/duckdb/lib",
            "RUSTFLAGS": "-C link-arg=-Wl,-rpath,/opt/duckdb/lib",
            "PATH": "/root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            # tangled.org (which hosts our `eat-rocks` git dep) is flaky
            # over libgit2; system-git's HTTPS handling rides through
            # transient 502s much better.
            "CARGO_NET_GIT_FETCH_WITH_CLI": "true",
            "CARGO_NET_RETRY": "10",
        }
    )
    .add_local_dir(".", remote_path="/app", copy=True, ignore=SOURCE_IGNORE)
    .run_commands(
        "cd /app && cargo build --release --bin at-snapshot",
    )
)

# /vol-rocks/var/rocks/           mirror (~80 GB, persisted across runs)
# /vol-out/var/raw/<date>/        staging parquet
# /vol-out/var/snapshot/<date>/   snapshot.duckdb + metadata
volume_rocks = modal.Volume.from_name("at-snapshot-data", create_if_missing=True)
volume_out = modal.Volume.from_name("at-snapshot-output", create_if_missing=True)

app = modal.App("at-snapshot")


def _common_args(
    *,
    backup_id: int | None,
    snapshot_date: str | None,
    mirror_concurrency: int,
    memory_limit: str,
    config: str | None,
    work_dir: str = TMP_WORK_DIR,
    window_days_back: int | None = None,
    window_days_lag: int | None = None,
    stage_drop_rocks: bool = False,
) -> list[str]:
    args = [
        "--work-dir",
        work_dir,
        "--memory-limit",
        memory_limit,
        "--mirror-concurrency",
        str(mirror_concurrency),
    ]
    if stage_drop_rocks:
        args += ["--stage-drop-rocks"]
    if backup_id is not None:
        args += ["--backup-id", str(backup_id)]
    if snapshot_date:
        args += ["--snapshot-date", snapshot_date]
    if config:
        args += ["--config", config]
    if window_days_back is not None:
        args += ["--window-days-back", str(window_days_back)]
    if window_days_lag is not None:
        args += ["--window-days-lag", str(window_days_lag)]
    return args


def _run_subcommand(subcommand: str, common: Iterable[str]) -> None:
    args = ["/app/target/release/at-snapshot", subcommand, *list(common)]
    env = {**os.environ, "RUST_LOG": "info,object_store=warn"}
    print("running:", " ".join(args), flush=True)
    # cwd=/app so relative --config paths (e.g. deploy/at-snapshot.toml)
    # resolve against the source tree shipped into the image.
    subprocess.check_call(args, env=env, cwd="/app")


def _resolve_date(snapshot_date: str | None) -> str:
    """Mirror the binary's default of today UTC so we can name copy paths."""
    return snapshot_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _dir_has_files(path: str) -> bool:
    """True if `path` exists and contains at least one regular file
    anywhere in its tree. Cheap recursive check used to short-circuit
    redundant copies."""
    if not os.path.isdir(path):
        return False
    for _, _, fnames in os.walk(path):
        if fnames:
            return True
    return False


def _rocks_looks_complete(rocks_dir: str) -> bool:
    """Mirror src/mirror.rs:existing_db_looks_complete: a rocks tree is
    considered usable if both CURRENT and .cursor are present. .cursor
    is written only after the eat-rocks restore finishes successfully,
    so it's a reliable end-of-mirror marker."""
    return os.path.exists(os.path.join(rocks_dir, "CURRENT")) and os.path.exists(
        os.path.join(rocks_dir, ".cursor")
    )


def _copy_concurrent(
    src: str,
    dst: str,
    label: str,
    max_threads: int = 24,
    skip_if_dst_populated: bool = False,
    progress_every_s: float = 5.0,
) -> None:
    """Parallel directory copy. Patterned on Modal's RoseTTAFold example
    (`copy_concurrent`); routes each file through a 24-thread shutil.copy2
    pool. Modal Volume FUSE scales with concurrent ops, so this is much
    faster than single-threaded rsync.

    Walks the source first for total bytes/file count, submits all copies
    via apply_async with success callbacks that bump shared counters, and
    prints a percent / MB-per-second heartbeat every progress_every_s
    seconds while the pool drains.

    skip_if_dst_populated=True turns this into a "stage in if missing"
    primitive — used for inbound copies where /tmp may already hold the
    artifact (e.g. mirror just wrote rocks/ to /tmp and we'd otherwise
    re-pull from the Volume). Outbound persistence copies leave it
    False so /vol always reflects the latest /tmp content.
    """
    import threading as _threading

    if not os.path.exists(src):
        print(f"[copy:{label}] skip — {src} does not exist", flush=True)
        return
    if skip_if_dst_populated and _dir_has_files(dst):
        print(
            f"[copy:{label}] skip — {dst} already populated, reusing in place",
            flush=True,
        )
        return

    # Walk source once: collect (relative_path, size). Pre-create dest
    # subdirs so worker threads don't race on mkdir.
    pairs: list[tuple[str, int]] = []
    for root, _dirs, fnames in os.walk(src):
        rel_root = os.path.relpath(root, src)
        for fname in fnames:
            full = os.path.join(root, fname)
            try:
                sz = os.path.getsize(full)
            except OSError:
                continue
            rel = fname if rel_root == "." else os.path.join(rel_root, fname)
            pairs.append((rel, sz))

    total_bytes = sum(sz for _, sz in pairs)
    total_files = len(pairs)
    os.makedirs(dst, exist_ok=True)
    for rel, _ in pairs:
        sub = os.path.dirname(rel)
        if sub:
            os.makedirs(os.path.join(dst, sub), exist_ok=True)

    print(
        f"[copy:{label}] {src} -> {dst}: {total_files} files, "
        f"{total_bytes / 1e9:.2f} GB, {max_threads} threads",
        flush=True,
    )

    if total_files == 0:
        print(f"[copy:{label}] DONE (empty source)", flush=True)
        return

    state = {"bytes": 0, "files": 0, "errors": 0}
    state_lock = _threading.Lock()
    t0 = time.time()

    def make_cb(size: int):
        def _cb(_result):
            with state_lock:
                state["bytes"] += size
                state["files"] += 1

        return _cb

    def err_cb(exc):
        with state_lock:
            state["errors"] += 1
        print(f"[copy:{label}] FAILED: {exc}", file=sys.stderr, flush=True)

    pool = ThreadPool(max_threads)
    try:
        for rel, sz in pairs:
            s = os.path.join(src, rel)
            d = os.path.join(dst, rel)
            pool.apply_async(
                shutil.copy2,
                args=(s, d),
                callback=make_cb(sz),
                error_callback=err_cb,
            )
        pool.close()

        last_print = 0.0
        while True:
            with state_lock:
                done_files = state["files"]
                done_bytes = state["bytes"]
                errors = state["errors"]
            if done_files + errors >= total_files:
                break
            now = time.time()
            if now - last_print >= progress_every_s:
                last_print = now
                elapsed = now - t0
                pct = 100.0 * done_bytes / max(1, total_bytes)
                rate = done_bytes / max(1e-3, elapsed) / 1e6
                print(
                    f"[copy:{label}] {pct:5.1f}% "
                    f"{done_bytes / 1e9:.2f}/{total_bytes / 1e9:.2f} GB, "
                    f"{done_files}/{total_files} files, {rate:.1f} MB/s",
                    flush=True,
                )
            time.sleep(0.5)
        pool.join()
    finally:
        pool.close()
        pool.join()

    elapsed = time.time() - t0
    rate = state["bytes"] / max(1e-3, elapsed) / 1e6
    suffix = f" ({state['errors']} errors)" if state["errors"] else ""
    print(
        f"[copy:{label}] DONE {state['bytes'] / 1e9:.2f} GB / "
        f"{state['files']}/{total_files} files in {elapsed:.1f}s "
        f"({rate:.1f} MB/s){suffix}",
        flush=True,
    )


def _raw_outputs_complete(raw_dir: str) -> bool:
    """True iff `raw_dir` (typically `/vol-out/var/raw/<date>`) holds
    the four parquet files a successful stage run produces. Used by
    `build` to short-circuit mirror+stage when a previous run already
    persisted raw — common after a hydrate cancellation."""
    expected = (
        "actors.parquet",
        "link_records.parquet",
        "link_record_targets.parquet",
        "targets.parquet",
    )
    for name in expected:
        p = os.path.join(raw_dir, name)
        if not os.path.isfile(p) or os.path.getsize(p) == 0:
            return False
    return True


def _plc_shards(dirpath: str) -> list[str]:
    """Sorted part-NNNNN.parquet basenames in `dirpath` (empty if absent)."""
    if not os.path.isdir(dirpath):
        return []
    return sorted(f for f in os.listdir(dirpath) if f.endswith(".parquet"))


def _read_plc_cursor(dirpath: str) -> dict | None:
    """Load a PLC cursor from `dirpath`.

    plc.rs writes `.cursor`; an earlier revision of the phase wrote
    `.cursor.json`, and the shard store on the volume still carries that
    name. Read either, preferring the current one.
    """
    for name in (".cursor", ".cursor.json"):
        path = os.path.join(dirpath, name)
        if os.path.isfile(path):
            with open(path) as fh:
                return json.load(fh)
    return None


def _copy_plc_shards(src: str, dst: str, label: str) -> int:
    """Copy `part-*.parquet` from src to dst, skipping same-size files
    already present. Returns the number of files actually copied. Small
    enough (~100 files) that the threaded copier isn't worth it."""
    os.makedirs(dst, exist_ok=True)
    copied = 0
    for name in _plc_shards(src):
        s, d = os.path.join(src, name), os.path.join(dst, name)
        if os.path.isfile(d) and os.path.getsize(d) == os.path.getsize(s):
            continue
        shutil.copy2(s, d)
        copied += 1
    print(f"[plc:{label}] {src} -> {dst}: {copied} shard(s) copied", flush=True)
    return copied


def _seed_plc_workdir(date: str) -> str:
    """Prepare `raw/<date>/plc/` so the `plc` subcommand resumes from the
    canonical store instead of re-fetching the whole export.

    The store's cursor is written back with `completed` cleared: plc.rs
    treats `completed` as terminal and short-circuits on it, but for us
    it only means "caught up as of the last run" — a new build wants to
    walk from that `after` cursor to the present tail. Shard numbering
    continues past `shards`, so existing parts are never clobbered.

    Idempotent: if the work dir already holds a cursor (a previous
    attempt on this date), it's left alone so the retry resumes there.
    """
    work = f"{OUT_VOL_DIR}/raw/{date}/plc"
    os.makedirs(work, exist_ok=True)

    if _read_plc_cursor(work) is not None:
        print(
            f"[plc:seed] {work} already has a cursor "
            f"({len(_plc_shards(work))} shards); resuming in place",
            flush=True,
        )
        return work

    cursor = _read_plc_cursor(PLC_STORE_DIR)
    if cursor is None:
        print(f"[plc:seed] no store at {PLC_STORE_DIR}; cold full export", flush=True)
        return work

    _copy_plc_shards(PLC_STORE_DIR, work, "seed")
    cursor = {**cursor, "completed": False}
    with open(os.path.join(work, ".cursor"), "w") as fh:
        json.dump(cursor, fh, indent=2)
    print(
        f"[plc:seed] resuming from after={cursor.get('after')!r} "
        f"shards={cursor.get('shards')} rows={cursor.get('rows')}",
        flush=True,
    )
    return work


def _publish_plc_store(date: str) -> None:
    """Push this build's PLC shards + cursor back to the canonical store
    so the next build resumes from today's tail rather than this one's."""
    work = f"{OUT_VOL_DIR}/raw/{date}/plc"
    cursor = _read_plc_cursor(work)
    if cursor is None:
        print(f"[plc:publish] no cursor in {work}; nothing to publish", flush=True)
        return
    _copy_plc_shards(work, PLC_STORE_DIR, "publish")
    # Write the cursor last: it's the marker the next seed keys off, so
    # a crash mid-copy leaves the store pointing at the older (still
    # valid) tail rather than claiming shards it doesn't have.
    with open(os.path.join(PLC_STORE_DIR, ".cursor"), "w") as fh:
        json.dump(cursor, fh, indent=2)
    legacy = os.path.join(PLC_STORE_DIR, ".cursor.json")
    if os.path.isfile(legacy):
        os.remove(legacy)
    print(
        f"[plc:publish] store at shards={cursor.get('shards')} "
        f"rows={cursor.get('rows')} after={cursor.get('after')!r}",
        flush=True,
    )


def _drop_local_rocks() -> None:
    """Remove /tmp/var/rocks once stage is done.

    Stage is the only phase that reads rocks; hydrate consumes raw
    parquet only. Dropping rocks before the raw-out copy reclaims ~650 GB
    on the worker's local disk so the outbound copy's Modal-Volume FUSE
    write buffer (which stages on the same disk) has headroom. Without
    this, rocks (~650 GB) + raw (~400 GB) + buffered FUSE writes (~400 GB
    while the copy runs) all compete on /tmp.
    """
    rocks_local = f"{TMP_WORK_DIR}/rocks"
    if not os.path.isdir(rocks_local):
        return
    print(f"[free] rmtree {rocks_local}", flush=True)
    t0 = time.time()
    shutil.rmtree(rocks_local)
    print(f"[free] done in {time.time() - t0:.1f}s", flush=True)


# `_ensure_rocks_on_volume` used to gate by file presence and stage
# rocks through /tmp on a cold start. The binary's mirror.rs now
# does its own size-diff against the existing rocks tree and only
# downloads delta files, so the right shape is: point the binary
# straight at /vol-rocks and let it decide whether to do work.
# Kept here as a stub so external callers don't break; new code
# should not rely on it.


# =====================================================================
# Per-phase Modal functions
#
# Each phase has its own resource shape because the workloads diverge:
# - mirror: network-bound, low CPU/RAM, big disk for rocks
# - stage:  rocks scan + DuckDB sort-merge — needs RAM for the actor
#           map (~16-20 GB at full scale) and 8 CPU for parallel passes
# - hydrate: pure DuckDB on entity parquets — RAM-hungry for aggregates,
#           no rocks needed (so smaller disk than stage)
#
# `build()` orchestrates by calling each phase function via `.remote()`,
# which lands each on its own tailored container. Don't run the whole
# pipeline on a single container sized for the worst phase; that
# over-provisions the easy phases and the cheap mirror downloads pay
# for stage's RAM and hydrate's DuckDB headroom too.
# =====================================================================


@app.function(
    image=image,
    volumes={
        "/vol-rocks": volume_rocks,
    },
    # eat-rocks streams ~80 GB compressed → ~650 GB SSTs over the
    # network; latency variability dominates wall time. 8h covers a
    # slow link with margin. Daily incremental refreshes complete
    # in minutes.
    timeout=60 * 60 * 8,
    cpu=2.0,
    # Mirror is I/O bound. 4 GiB is plenty for eat-rocks + a small
    # restore buffer; we don't open the DB or run any analytic work.
    memory=4 * 1024,
    # Writing directly to /vol-rocks (no /tmp staging) — the local
    # ephemeral disk is essentially unused, but Modal enforces a
    # 512 GiB floor on ephemeral_disk. Leave at 1 TiB so a future
    # full-rebuild path that wants to stage through /tmp still has
    # room.
    ephemeral_disk=1024 * 1024,  # 1 TiB
    retries=0,
)
def mirror_phase(
    backup_id: int | None = None,
    snapshot_date: str | None = None,
    mirror_concurrency: int = 64,
    config: str | None = None,
) -> None:
    """Mirror phase: bring /vol-rocks/var/rocks to the latest (or
    specified) constellation backup.

    The binary's mirror.rs reads the persisted `.cursor` file from
    the rocks dir, fetches the target backup's meta, and downloads
    only files whose local size doesn't match. First run is a full
    ~650 GB pull; subsequent daily refreshes touch only the SSTs
    that changed (and the MANIFEST / CURRENT), so wall time drops
    from hours to single-digit minutes.

    Writes go directly to the Modal Volume — no /tmp staging. The
    rocks tree is undated; cursor metadata records when each
    backup_id became current.
    """
    date = _resolve_date(snapshot_date)
    common = _common_args(
        backup_id=backup_id,
        snapshot_date=date,
        mirror_concurrency=mirror_concurrency,
        memory_limit="2GiB",  # mirror itself doesn't open duckdb
        config=config,
        work_dir=ROCKS_VOL_DIR,
    )
    _run_subcommand("mirror", common)
    volume_rocks.commit()


@app.function(
    image=image,
    volumes={"/vol-out": volume_out},
    # A cold full export walks ~100M ops one 1000-row page at a time;
    # a warm resume covers a few weeks in minutes. 6h absorbs either,
    # plus plc.directory's rate-limit backoff.
    timeout=60 * 60 * 6,
    # Sequential HTTP paging with a parquet writer on the end — one
    # core and a small buffer is the whole workload.
    cpu=1.0,
    memory=4 * 1024,
    ephemeral_disk=512 * 1024,  # 512 GiB (Modal minimum; unused)
    retries=0,
)
def plc_phase(
    snapshot_date: str | None = None,
    config: str | None = None,
) -> None:
    """PLC phase: plc.directory export -> raw/<date>/plc/*.parquet.

    Consumed by hydrate's `07_enrich_actors_created` stage to add
    `created_at` / `tombstoned_at` to `actors` and to insert PLC-only
    DIDs. hydrate skips that stage when the dir holds no parquet, so
    omitting this phase silently drops the column rather than failing —
    which is exactly why it belongs in `build()` and not just in the
    Rust-side `at-snapshot build`.

    Talks only to plc.directory: no rocks, no raw parquet, no ordering
    constraint against mirror/stage. `build()` runs it concurrently
    with the mirror -> stage chain and joins before hydrate.

    Runs with `--work-dir` on the output volume rather than /tmp so the
    checkpointed cursor survives a container kill mid-export.
    """
    date = _resolve_date(snapshot_date)
    _seed_plc_workdir(date)
    common = _common_args(
        backup_id=None,
        snapshot_date=date,
        mirror_concurrency=1,
        memory_limit="2GiB",  # plc doesn't open duckdb
        config=config,
        work_dir=OUT_VOL_DIR,
    )
    _run_subcommand("plc", common)
    _publish_plc_store(date)
    volume_out.commit()


@app.function(
    image=image,
    volumes={
        "/vol-rocks": volume_rocks,
        "/vol-out": volume_out,
    },
    timeout=60 * 60 * 10,
    # Pass B and Pass C run as parallel OS threads; Phase 5 spawns
    # DuckDB threads. 8 cores keeps both rocks scans saturated and
    # leaves headroom for DuckDB.
    cpu=8.0,
    # Stage v2 holds two in-memory maps during Passes B+C:
    #   did → did_id    HashMap   ~10-12 GB at 100M actors
    #   did_id → did    Vec       ~7-8 GB at 100M actors
    # Both are dropped before Phase 5. DuckDB Phase 5 then runs with
    # memory_limit=auto (capped at 80% of cgroup, hard cap 128 GiB),
    # spilling to /tmp/duckdb_tmp. 32 GiB is the right shape: covers
    # ActorMap during scans, leaves 12-16 GiB headroom for DuckDB
    # during Phase 5 sorts and pivots. If full-scale runs hit
    # ActorMap > 20 GB, bump to 48.
    memory=32 * 1024,
    # /tmp peak during stage, at 2026-07 scale:
    #   scratch lt_*.parquet + t_*_refs.parquet (~450 GB)
    #   + raw entity parquets (~400 GB)
    #   + DuckDB sort spill during Phase 5 (~300 GB on the likes sort,
    #     which is 11.8 B rows)
    # ≈ 1.2 TB, and rocks (765 GB) is NOT in that total because
    # --stage-drop-rocks deletes it after the scans, before Phase 5.
    #
    # It used to be, and that's what broke the 2026-07-31 build: rocks
    # had grown 650 -> 765 GB and every entity had roughly doubled
    # since the estimate above was written, so Phase 5 hit "No space
    # left on device" partway through the likes copy after 2.4 h of
    # scanning. 3 TiB keeps a full mirror's worth of slack on top of
    # the drop, so the next growth step doesn't repeat that.
    ephemeral_disk=3 * 1024 * 1024,  # 3 TiB
    retries=0,
)
def stage_phase(
    backup_id: int | None = None,
    snapshot_date: str | None = None,
    mirror_concurrency: int = 64,
    memory_limit: str = "auto",
    config: str | None = None,
) -> None:
    """Stage phase: rocks → entity parquets.

    Reads /vol-rocks → /tmp, runs the binary's `stage` command, then
    copies raw entity parquets to /vol-out.

    `--stage-drop-rocks` is safe here and only here: the mirror this
    reads is a container-local /tmp copy made at the top of this
    function, so deleting it costs one re-copy from the Volume rather
    than a re-download from constellation. The binary removes it once
    the scans are done, which is what keeps Phase 5's DuckDB spill
    inside the disk budget.
    """
    date = _resolve_date(snapshot_date)
    common = _common_args(
        backup_id=backup_id,
        snapshot_date=date,
        mirror_concurrency=mirror_concurrency,
        memory_limit=memory_limit,
        config=config,
        work_dir=TMP_WORK_DIR,
        stage_drop_rocks=True,
    )
    # Same staleness hazard as hydrate: pick up whatever mirror committed,
    # rather than the version this container happened to mount with.
    volume_rocks.reload()

    print("=== copy rocks: rocks volume -> /tmp ===", flush=True)
    _copy_concurrent(
        f"{ROCKS_VOL_DIR}/rocks",
        f"{TMP_WORK_DIR}/rocks",
        "rocks-in",
        skip_if_dst_populated=True,
    )
    _run_subcommand("stage", common)
    _drop_local_rocks()
    print("=== persist raw: /tmp -> output volume ===", flush=True)
    _copy_concurrent(
        f"{TMP_WORK_DIR}/raw/{date}",
        f"{OUT_VOL_DIR}/raw/{date}",
        "raw-out",
    )
    volume_out.commit()


@app.function(
    image=image,
    volumes={
        "/vol-out": volume_out,
    },
    timeout=60 * 60 * 6,
    # DuckDB benefits from threads on the chunked aggregate stages.
    # 4 cores is enough; aggregates are mostly memory-bound, not
    # CPU-bound.
    cpu=4.0,
    # Hydrate's hot moment is the chunked aggregate phase: GROUP BY
    # uri_id over likes (~5B rows × u64) and the actor_aggs joins.
    # With chunk_buckets=8, per-chunk hash table is ~5 GB; DuckDB
    # also keeps prior-chunk pages cached. 64 GiB gives memory_limit
    # of ~50 GiB resolved (80% cap), enough headroom for the joins
    # without spill thrashing.
    memory=64 * 1024,
    # /tmp peak during hydrate:
    #   raw entity parquets (~200 GB)
    #   + snapshot.duckdb (~100-150 GB)
    #   + DuckDB temp_directory spill (configurable, sized at 400 GiB
    #     in hydrate.rs) — bounded by max_temp_directory_size
    # ≈ 700-800 GiB. 1 TiB fits.
    ephemeral_disk=1024 * 1024,  # 1 TiB
    retries=0,
)
def hydrate_phase(
    backup_id: int | None = None,
    snapshot_date: str | None = None,
    memory_limit: str = "auto",
    config: str | None = None,
    window_days_back: int | None = None,
    window_days_lag: int | None = None,
) -> None:
    """Hydrate phase: entity parquets → snapshot.duckdb.

    Stages raw parquets from /vol-out → /tmp, runs the binary's
    `hydrate` command, copies snapshot.duckdb back to /vol-out.
    No rocks involvement.

    `window_days_back` / `window_days_lag` enable the hydrate-time
    window: likes / reposts / posts_from_* are filtered to
    created_at in [snapshot_date - back, snapshot_date - lag], and
    orphan likes/reposts are pruned against the windowed posts.
    Pass both or neither.
    """
    date = _resolve_date(snapshot_date)
    common = _common_args(
        backup_id=backup_id,
        snapshot_date=date,
        mirror_concurrency=1,
        memory_limit=memory_limit,
        config=config,
        work_dir=TMP_WORK_DIR,
        window_days_back=window_days_back,
        window_days_lag=window_days_lag,
    )
    # A container sees the Volume as of the moment it mounted, so a
    # hydrate that starts while stage's commit is still landing reads a
    # half-written raw/<date> — the parquet footers aren't there yet and
    # load_raw dies with "No magic bytes found at end of file". reload()
    # pulls the latest committed version before we measure or copy
    # anything. Cheap, and the only thing standing between a stale mount
    # and a 2 h hydrate on truncated input.
    volume_out.reload()

    print("=== copy raw: output volume -> /tmp ===", flush=True)
    _copy_concurrent(
        f"{OUT_VOL_DIR}/raw/{date}",
        f"{TMP_WORK_DIR}/raw/{date}",
        "raw-in",
        skip_if_dst_populated=True,
    )
    _run_subcommand("hydrate", common)
    print("=== persist snapshot: /tmp -> output volume ===", flush=True)
    _copy_concurrent(
        f"{TMP_WORK_DIR}/snapshot/{date}",
        f"{OUT_VOL_DIR}/snapshot/{date}",
        "snapshot-out",
    )
    volume_out.commit()


@app.function(
    image=image,
    # Orchestrator only — mounts /vol-out solely to check the resume
    # condition. The heavy lifting happens on the workers spawned
    # via the per-phase .remote() calls.
    volumes={"/vol-out": volume_out},
    timeout=60 * 60 * 24,
    cpu=0.5,
    memory=1 * 1024,
    # Modal enforces a 512 GiB minimum on ephemeral_disk; we don't
    # actually use it (the orchestrator does no local I/O) but we
    # have to allocate at least that. The disk is per-container and
    # ephemeral, so it's free if unused — billing is on what's
    # actually written.
    ephemeral_disk=512 * 1024,  # 512 GiB (Modal minimum)
    retries=0,
)
def build(
    backup_id: int | None = None,
    snapshot_date: str | None = None,
    mirror_concurrency: int = 64,
    memory_limit: str = "auto",
    config: str | None = None,
    upload_after: bool = False,
) -> None:
    """Orchestrate plc + mirror → stage → hydrate end-to-end.

    Each phase runs on its own purpose-sized worker via .remote(), so
    the cheap mirror download doesn't pay for stage's RAM, and hydrate
    doesn't pay for stage's 2 TiB disk. Resume-aware: if raw entity
    parquets are already on /vol-out, skip mirror+stage and go
    straight to hydrate.

    PLC mirrors the Rust `build`'s tokio::try_join: it only talks to
    plc.directory, so it's spawned up front and runs alongside the
    mirror → stage chain, then joined before hydrate (which needs its
    shards for the actors created_at enrichment). A PLC failure fails
    the build rather than silently producing a snapshot without
    created_at — mirror/stage output is already persisted at that
    point, so a rerun resumes straight into hydrate.

    `upload_after` runs the upload here, server-side, so the whole
    chain survives the local caller disconnecting.
    """
    date = _resolve_date(snapshot_date)

    print("=== phase 0: plc (spawned; joins before hydrate) ===", flush=True)
    plc_call = plc_phase.spawn(snapshot_date=date, config=config)

    raw_on_vol = f"{OUT_VOL_DIR}/raw/{date}"
    if _raw_outputs_complete(raw_on_vol):
        print(
            f"=== resume: {raw_on_vol} already complete; skipping mirror+stage ===",
            flush=True,
        )
    else:
        print("=== phase 1/3: mirror ===", flush=True)
        mirror_phase.remote(
            backup_id=backup_id,
            snapshot_date=date,
            mirror_concurrency=mirror_concurrency,
            config=config,
        )
        print("=== mirror committed ===", flush=True)

        print("=== phase 2/3: stage ===", flush=True)
        stage_phase.remote(
            backup_id=backup_id,
            snapshot_date=date,
            mirror_concurrency=mirror_concurrency,
            memory_limit=memory_limit,
            config=config,
        )
        print("=== stage committed ===", flush=True)

    print("=== joining plc ===", flush=True)
    plc_call.get()
    print("=== plc committed ===", flush=True)

    print("=== phase 3/3: hydrate ===", flush=True)
    hydrate_phase.remote(
        backup_id=backup_id,
        snapshot_date=date,
        memory_limit=memory_limit,
        config=config,
    )
    print("=== hydrate committed; snapshot ready ===", flush=True)

    if upload_after:
        print("=== upload ===", flush=True)
        upload.remote(snapshot_date=date, config=config)
        print("=== upload complete ===", flush=True)


@app.function(
    image=image,
    volumes={"/vol-rocks": volume_rocks},
    timeout=60 * 10,  # 10 min — open + property reads should take seconds
    cpu=1.0,
    memory=8 * 1024,
)
def inspect(
    config: str | None = None,
    memory_limit: str = "2GiB",
) -> None:
    """Cheap rocksdb inspection: opens /vol-rocks/var/rocks read-only
    and queries per-CF estimate-num-keys / SST sizes from the manifest.
    No scanning, no /tmp copy. Use to size pass B before kicking off a
    long stage run.
    """
    common = _common_args(
        backup_id=None,
        snapshot_date=None,
        mirror_concurrency=1,
        memory_limit=memory_limit,
        config=config,
        work_dir=ROCKS_VOL_DIR,
    )
    _run_subcommand("inspect", common)


# Lightweight image just for read-only parquet inspection. Built off
# debian_slim with the duckdb pip wheel — doesn't need the rocksdb /
# Rust toolchain in the main image, so changes here don't drag the
# Cargo cache.
inspect_image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install("duckdb==1.5.2")
)


@app.function(
    image=inspect_image,
    volumes={"/vol-out": volume_out},
    timeout=60 * 60,
    cpu=4.0,
    # likes.parquet is 162 GiB; DuckDB streams it but column stats and
    # min/max scans benefit from a few GiB of buffer space.
    memory=16 * 1024,
)
def count_parquets(snapshot_date: str = "2026-04-28") -> None:
    """Count rows + min/max created_at for each entity parquet in
    /vol-out/var/raw/<snapshot_date>/. Pure read-only volume access:
    no download, runs on Modal, exits in seconds-to-minutes.

    `actors` has no `created_at` column — only the row count is
    printed for it. All other entities have `created_at`.
    """
    import os
    import time

    import duckdb

    raw_dir = f"{OUT_VOL_DIR}/raw/{snapshot_date}"
    entities = [
        ("actors", None),
        ("blocks", "created_at"),
        ("follows", "created_at"),
        ("reposts", "created_at"),
        ("likes", "created_at"),
        ("posts_from_records", "created_at"),
        ("posts_from_targets", "created_at"),
    ]
    con = duckdb.connect()
    con.execute("SET threads=4")
    print(f"=== parquet stats: {raw_dir} ===", flush=True)
    print(f"{'table':<22}{'rows':>16}  {'min_created_at':<28}  {'max_created_at':<28}  size", flush=True)
    for name, ts_col in entities:
        path = f"{raw_dir}/{name}.parquet"
        size_gib = os.path.getsize(path) / (1024**3)
        t0 = time.time()
        if ts_col is None:
            (n,) = con.execute(
                f"SELECT COUNT(*) FROM read_parquet(?)", [path]
            ).fetchone()
            mn, mx = "-", "-"
        else:
            row = con.execute(
                f"SELECT COUNT(*), MIN({ts_col}), MAX({ts_col}) "
                f"FROM read_parquet(?)",
                [path],
            ).fetchone()
            n, mn, mx = row
            mn = "" if mn is None else str(mn)
            mx = "" if mx is None else str(mx)
        elapsed = time.time() - t0
        print(
            f"{name:<22}{n:>16,}  {mn:<28}  {mx:<28}  "
            f"{size_gib:>6.2f} GiB  ({elapsed:.1f}s)",
            flush=True,
        )


@app.function(
    image=inspect_image,
    volumes={"/vol-out": volume_out},
    timeout=60 * 60,
    cpu=4.0,
    memory=16 * 1024,
)
def plausible_count(
    snapshot_date: str = "2026-04-28",
    lo: str = "2022-01-01",
    hi: str = "2026-05-31",
) -> None:
    """For each entity parquet that carries `created_at`, count how
    many rows fall inside [lo, hi] (the atproto plausibility window)
    vs. outside. Useful to see how much of each table is TID-decode
    garbage from malformed rkeys before relying on time-window
    analytics.

    `actors` has no created_at — skipped.
    """
    import time

    import duckdb

    raw_dir = f"{OUT_VOL_DIR}/raw/{snapshot_date}"
    entities = [
        "blocks",
        "follows",
        "reposts",
        "likes",
        "posts_from_records",
        "posts_from_targets",
    ]
    con = duckdb.connect()
    con.execute("SET threads=4")
    print(
        f"=== plausibility filter: created_at in "
        f"[{lo}, {hi}] on {raw_dir} ===",
        flush=True,
    )
    print(
        f"{'table':<22}{'total':>16}  {'plausible':>16}  "
        f"{'too_early':>14}  {'too_late':>14}  {'%ok':>6}",
        flush=True,
    )
    for name in entities:
        path = f"{raw_dir}/{name}.parquet"
        t0 = time.time()
        row = con.execute(
            f"""
            SELECT
              COUNT(*) AS total,
              COUNT(*) FILTER (WHERE created_at >= ?::TIMESTAMP
                                 AND created_at <= ?::TIMESTAMP) AS ok,
              COUNT(*) FILTER (WHERE created_at <  ?::TIMESTAMP) AS early,
              COUNT(*) FILTER (WHERE created_at >  ?::TIMESTAMP) AS late
            FROM read_parquet(?)
            """,
            [lo, hi, lo, hi, path],
        ).fetchone()
        total, ok, early, late = row
        pct = 100.0 * ok / total if total else 0.0
        elapsed = time.time() - t0
        print(
            f"{name:<22}{total:>16,}  {ok:>16,}  "
            f"{early:>14,}  {late:>14,}  {pct:>5.2f}%  "
            f"({elapsed:.1f}s)",
            flush=True,
        )


@app.function(
    image=inspect_image,
    volumes={"/vol-out": volume_out},
    timeout=60 * 60,
    cpu=4.0,
    memory=16 * 1024,
)
def daily_histogram(
    snapshot_date: str = "2026-04-28",
    lo: str = "2022-01-01",
    hi: str = "2026-05-31",
    last_n_days: int = 90,
) -> None:
    """Per-month row counts (across the plausibility window) + a
    last-N-days tally. Lets us compare against jazco.dev/stats:
    20 M likes/day × 90 ≈ 1.8 B over 90 days, so any table whose
    last-90-day window matches that is consistent with the public
    chart even if its lifetime count is much larger.
    """
    import time

    import duckdb

    raw_dir = f"{OUT_VOL_DIR}/raw/{snapshot_date}"
    entities = ["follows", "reposts", "likes", "posts_from_records"]
    con = duckdb.connect()
    con.execute("SET threads=4")
    print(f"=== monthly + last-{last_n_days}d window on {raw_dir} ===", flush=True)
    for name in entities:
        path = f"{raw_dir}/{name}.parquet"
        t0 = time.time()
        last_window = con.execute(
            f"""
            SELECT COUNT(*) FROM read_parquet(?)
            WHERE created_at >= (?::TIMESTAMP - INTERVAL '{last_n_days}' DAY)
              AND created_at <= ?::TIMESTAMP
            """,
            [path, hi, hi],
        ).fetchone()[0]
        monthly = con.execute(
            f"""
            SELECT strftime(created_at, '%Y-%m') AS ym, COUNT(*) AS n
            FROM read_parquet(?)
            WHERE created_at >= ?::TIMESTAMP AND created_at <= ?::TIMESTAMP
            GROUP BY 1
            ORDER BY 1
            """,
            [path, lo, hi],
        ).fetchall()
        elapsed = time.time() - t0
        print(
            f"\n[{name}]  last {last_n_days} days ending {hi}: "
            f"{last_window:,}   (scan {elapsed:.1f}s)",
            flush=True,
        )
        print(f"  monthly rows in [{lo}, {hi}]:", flush=True)
        for ym, n in monthly:
            bar = "#" * min(60, int(n / 50_000_000))
            print(f"    {ym}  {n:>14,}  {bar}", flush=True)


@app.function(
    image=inspect_image,
    volumes={"/vol-out": volume_out},
    timeout=60 * 60,
    cpu=4.0,
    memory=16 * 1024,
)
def validate_snapshot(snapshot_date: str = "2026-04-28") -> None:
    """Battery of correctness checks against the built snapshot
    on /vol-out/var/snapshot/<date>/snapshot.duckdb. Opens read-only
    so a misbehaving check can't corrupt the file. Each check prints
    PASS / FAIL with the observed value and the expectation.

    Categories:
      1. structure: every expected table exists and is BASE TABLE
      2. counts: row counts match the build log
      3. window: time-windowed tables have created_at in [lo, hi]
      4. orphans: every likes/reposts row joins to posts
      5. uniqueness: posts.uri_id and actors.did_id are unique
      6. aggregate consistency: post_aggs / actor_aggs sums match sources
      7. data quality: no NULLs where the schema forbids them
      8. cross-table join smoke test
      9. snapshot_metadata is present and labeled correctly
    """
    import duckdb

    db_path = f"{OUT_VOL_DIR}/snapshot/{snapshot_date}/snapshot.duckdb"
    print(f"=== validate {db_path} ===", flush=True)
    con = duckdb.connect(db_path, read_only=True)
    con.execute("PRAGMA threads=4")

    expected_counts = {
        "actors": 24_424_551,
        "follows": 1_330_346_041,
        "blocks": 111_021_550,
        "likes": 586_537_270,
        "reposts": 94_753_851,
        "posts": 98_028_415,
        "actor_aggs": 24_424_551,
        "post_aggs": 98_028_415,
    }
    # Window endpoints come from snapshot_date - 45 / -15 days.
    window_lo = "2026-03-14 00:00:00"
    window_hi = "2026-04-13 23:59:59"
    windowed = ["likes", "reposts", "posts"]

    fails = 0

    def check(name: str, ok: bool, observed, expected=None) -> None:
        nonlocal fails
        marker = "PASS" if ok else "FAIL"
        if not ok:
            fails += 1
        if expected is not None:
            print(
                f"  [{marker}] {name}: observed={observed}  expected={expected}",
                flush=True,
            )
        else:
            print(f"  [{marker}] {name}: observed={observed}", flush=True)

    # 1. structure
    print("\n[1] structure", flush=True)
    rows = con.execute(
        "SELECT table_name, table_type FROM information_schema.tables "
        "WHERE table_schema='main' ORDER BY table_name"
    ).fetchall()
    by_name = {n: t for n, t in rows}
    for t in list(expected_counts.keys()) + ["snapshot_metadata"]:
        check(f"table {t} exists", t in by_name, by_name.get(t, "MISSING"))
        if t in by_name:
            check(
                f"table {t} is BASE TABLE",
                by_name[t] == "BASE TABLE",
                by_name[t],
                "BASE TABLE",
            )

    # 2. row counts
    print("\n[2] row counts", flush=True)
    for t, exp in expected_counts.items():
        (n,) = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()
        check(f"COUNT({t})", n == exp, f"{n:,}", f"{exp:,}")

    # 3. window enforcement
    print(f"\n[3] window enforcement [{window_lo}, {window_hi}]", flush=True)
    for t in windowed:
        mn, mx = con.execute(
            f"SELECT MIN(created_at), MAX(created_at) FROM {t}"
        ).fetchone()
        check(
            f"{t}.created_at >= {window_lo}",
            mn is not None and str(mn) >= window_lo,
            str(mn),
            f">= {window_lo}",
        )
        check(
            f"{t}.created_at <= {window_hi}",
            mx is not None and str(mx) <= window_hi,
            str(mx),
            f"<= {window_hi}",
        )

    # 4. orphan rate
    print("\n[4] orphan rate (must be 0 after prune)", flush=True)
    for t in ("likes", "reposts"):
        (n,) = con.execute(
            f"SELECT COUNT(*) FROM {t} l "
            f"WHERE NOT EXISTS (SELECT 1 FROM posts p WHERE p.uri_id = l.subject_uri_id)"
        ).fetchone()
        check(f"{t} rows with unresolvable subject_uri_id", n == 0, f"{n:,}", "0")

    # 5. uniqueness
    print("\n[5] uniqueness", flush=True)
    (n, d) = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT uri_id) FROM posts"
    ).fetchone()
    check("posts.uri_id distinct == total", n == d, f"{d:,}", f"{n:,}")
    (n, d) = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT did_id) FROM actors"
    ).fetchone()
    check("actors.did_id distinct == total", n == d, f"{d:,}", f"{n:,}")

    # 6. aggregate consistency
    print("\n[6] aggregate consistency", flush=True)
    for col, src in [("likes", "likes"), ("reposts", "reposts")]:
        (s,) = con.execute(f"SELECT SUM({col}) FROM post_aggs").fetchone()
        (n,) = con.execute(f"SELECT COUNT(*) FROM {src}").fetchone()
        check(f"SUM(post_aggs.{col}) == COUNT({src})", s == n, f"{s:,}", f"{n:,}")
    for col, src in [
        ("follows", "follows"),
        ("blocks_out", "blocks"),
        ("likes_out", "likes"),
        ("reposts_out", "reposts"),
        ("posts", "posts"),
    ]:
        (s,) = con.execute(f"SELECT SUM({col}) FROM actor_aggs").fetchone()
        (n,) = con.execute(f"SELECT COUNT(*) FROM {src}").fetchone()
        check(
            f"SUM(actor_aggs.{col}) == COUNT({src})",
            s == n,
            f"{s:,}",
            f"{n:,}",
        )

    # 7. data quality
    print("\n[7] data quality", flush=True)
    (n,) = con.execute("SELECT COUNT(*) FROM actors WHERE did IS NULL").fetchone()
    check("actors.did NULLs", n == 0, f"{n:,}", "0")
    (n,) = con.execute(
        "SELECT COUNT(*) FROM posts WHERE author_did_id IS NULL"
    ).fetchone()
    check("posts.author_did_id NULLs", n == 0, f"{n:,}", "0")

    # 8. cross-table join smoke test
    print("\n[8] join smoke test", flush=True)
    (n,) = con.execute(
        "SELECT COUNT(*) FROM likes l "
        "JOIN posts p ON p.uri_id = l.subject_uri_id "
        "JOIN actors a ON a.did_id = p.author_did_id "
        "WHERE p.created_at >= TIMESTAMP '2026-04-01' "
        "  AND p.created_at <  TIMESTAMP '2026-04-02'"
    ).fetchone()
    check("likes targeting 2026-04-01 posts (join works)", n >= 0, f"{n:,}")

    # 9. snapshot_metadata
    print("\n[9] snapshot_metadata", flush=True)
    row = con.execute(
        "SELECT snapshot_date, at_snapshot_version, duckdb_memory_limit "
        "FROM snapshot_metadata"
    ).fetchone()
    check(
        "snapshot_metadata.snapshot_date",
        str(row[0]) == snapshot_date,
        str(row[0]),
        snapshot_date,
    )
    print(
        f"  (version={row[1]!r}, duckdb_memory_limit={row[2]!r})",
        flush=True,
    )

    print(
        f"\n=== {'ALL CHECKS PASS' if fails == 0 else f'{fails} FAILURE(S)'} ===",
        flush=True,
    )


@app.function(
    image=image,
    volumes={"/vol-out": volume_out},
    secrets=[modal.Secret.from_name("atproto-snapshot")],
    timeout=60 * 60 * 4,
    cpu=2.0,
    memory=8 * 1024,
)
def upload(
    snapshot_date: str | None = None,
    config: str | None = None,
) -> None:
    """Push raw/<date> + snapshot/<date> to the configured object store.
    Reads from the output volume only — rocks isn't needed.

    Reads R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY from the
    `r2-credentials` Modal Secret. All other settings (bucket,
    account_id, prefix) come from the at-snapshot config file.
    """
    # Upload reads snapshot/<date> straight off the Volume, so it needs
    # the version hydrate committed, not the one this container mounted.
    volume_out.reload()
    common = _common_args(
        backup_id=None,
        snapshot_date=snapshot_date,
        mirror_concurrency=64,
        memory_limit="2GiB",
        config=config,
        work_dir=OUT_VOL_DIR,
    )
    _run_subcommand("upload", common)


@app.local_entrypoint()
def main(
    phase: str = "build",
    upload_after: bool = False,
    backup_id: int | None = None,
    snapshot_date: str | None = None,
    mirror_concurrency: int = 64,
    memory_limit: str = "auto",
    config: str | None = None,
    background: bool = False,
    window_days_back: int | None = None,
    window_days_lag: int | None = None,
) -> None:
    """Local entrypoint dispatcher.

    Args:
      phase: build | mirror | stage | plc | hydrate | upload
      upload_after: when True and phase != upload, run upload after the
        chosen phase completes. Skipped for `upload` itself. For
        `build` the upload is chained server-side, so it still happens
        under --background; for a single phase the chain is driven from
        here and needs the local process to stay alive, so --background
        is refused rather than silently racing the upload ahead of the
        phase it's supposed to follow.
      background: spawn the remote call instead of waiting on it. With
        `modal run --detach`, plain .remote() may be cancelled when the
        local caller disconnects; .spawn() returns a FunctionCall handle
        that survives. Use this for long builds you want to walk away
        from. Follow progress with: `modal app logs <fn-call-id>`.
    """
    if upload_after and background and phase not in ("build", "upload"):
        raise SystemExit(
            f"--upload-after with --background is only chained server-side for "
            f"--phase build; for --phase {phase} it would spawn the upload "
            f"immediately, ahead of the phase. Drop --background, or run "
            f"`--phase upload` separately once the phase finishes."
        )

    def _kick(fn, **kwargs):
        if background:
            call = fn.spawn(**kwargs)
            print(
                f"[spawn] FunctionCall {call.object_id} — "
                f"follow with `modal app logs at-snapshot` "
                f"or check https://modal.com/apps"
            )
            return call
        return fn.remote(**kwargs)

    if phase == "build":
        # Chained inside build() so the upload survives a local
        # disconnect under --background, instead of being spawned
        # here in parallel with the build it should follow.
        _kick(
            build,
            backup_id=backup_id,
            snapshot_date=snapshot_date,
            mirror_concurrency=mirror_concurrency,
            memory_limit=memory_limit,
            config=config,
            upload_after=upload_after,
        )
        return
    elif phase == "mirror":
        _kick(
            mirror_phase,
            backup_id=backup_id,
            snapshot_date=snapshot_date,
            mirror_concurrency=mirror_concurrency,
            config=config,
        )
    elif phase == "stage":
        _kick(
            stage_phase,
            backup_id=backup_id,
            snapshot_date=snapshot_date,
            mirror_concurrency=mirror_concurrency,
            memory_limit=memory_limit,
            config=config,
        )
    elif phase == "plc":
        _kick(plc_phase, snapshot_date=snapshot_date, config=config)
    elif phase == "hydrate":
        _kick(
            hydrate_phase,
            backup_id=backup_id,
            snapshot_date=snapshot_date,
            memory_limit=memory_limit,
            config=config,
            window_days_back=window_days_back,
            window_days_lag=window_days_lag,
        )
    elif phase == "upload":
        _kick(upload, snapshot_date=snapshot_date, config=config)
        return
    elif phase == "inspect":
        _kick(inspect, config=config)
        return
    else:
        raise SystemExit(
            f"unknown phase {phase!r}; expected "
            "build/mirror/stage/plc/hydrate/upload/inspect"
        )

    if upload_after:
        _kick(upload, snapshot_date=snapshot_date, config=config)
