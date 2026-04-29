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
# the image. `target/` is huge build output, `var/` may hold an in-progress
# rocks mirror, `.cargo/config.toml` has macOS-only paths that would
# override the image's env block.
SOURCE_IGNORE = [
    "target",
    "var",
    ".git",
    ".github",
    ".cargo/config.toml",
    "__pycache__",
    "*.pyc",
    "*.parquet",
    ".DS_Store",
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
) -> list[str]:
    args = [
        "--work-dir",
        work_dir,
        "--memory-limit",
        memory_limit,
        "--mirror-concurrency",
        str(mirror_concurrency),
    ]
    if backup_id is not None:
        args += ["--backup-id", str(backup_id)]
    if snapshot_date:
        args += ["--snapshot-date", snapshot_date]
    if config:
        args += ["--config", config]
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


def _obtain_rocks_at_tmp(common_tmp: list[str]) -> None:
    """Ensure /tmp/var/rocks holds a complete rocks tree, choosing the
    cheapest path:

      1. Already present at /tmp/var/rocks (e.g. mirror just ran in
         this container) — nothing to do.
      2. Already persisted on the rocks volume — copy
         /vol-rocks → /tmp via the parallel copier. No constellation
         download. No volume write.
      3. Neither — run the binary's mirror subcommand to download from
         constellation into /tmp, then persist /tmp → rocks volume.
    """
    if _rocks_looks_complete(f"{TMP_WORK_DIR}/rocks"):
        print(
            f"[mirror] {TMP_WORK_DIR}/rocks already complete; reusing in place",
            flush=True,
        )
        return
    if _rocks_looks_complete(f"{ROCKS_VOL_DIR}/rocks"):
        print(
            "[mirror] using existing rocks from volume (skipping download)",
            flush=True,
        )
        _copy_concurrent(
            f"{ROCKS_VOL_DIR}/rocks",
            f"{TMP_WORK_DIR}/rocks",
            "rocks-vol-to-tmp",
        )
        return
    print(
        "[mirror] no existing rocks; downloading from constellation to /tmp",
        flush=True,
    )
    _run_subcommand("mirror", common_tmp)
    print("[mirror] persisting fresh rocks: /tmp -> rocks volume", flush=True)
    _copy_concurrent(
        f"{TMP_WORK_DIR}/rocks", f"{ROCKS_VOL_DIR}/rocks", "rocks-tmp-to-vol"
    )
    volume_rocks.commit()


@app.function(
    image=image,
    volumes={
        "/vol-rocks": volume_rocks,
        "/vol-out": volume_out,
    },
    timeout=60 * 60 * 10,  # 10h ceiling
    cpu=4.0,
    memory=12 * 1024,
    ephemeral_disk=1024 * 1024,  # 1 TiB
)
def build(
    backup_id: int | None = None,
    snapshot_date: str | None = None,
    mirror_concurrency: int = 64,
    memory_limit: str = "8GiB",
    config: str | None = None,
) -> None:
    """Run mirror + stage + hydrate end-to-end on local ephemeral disk.

    Per Modal's dataset-ingestion guide: all working data lives in /tmp
    (local NVMe-class). Volumes are touched only at the persistence
    boundaries — rocks goes to the rocks volume, raw + snapshot go to
    the output volume, each via a single _copy_concurrent at the end of
    its phase.
    """
    date = _resolve_date(snapshot_date)
    common_tmp = _common_args(
        backup_id=backup_id,
        snapshot_date=date,
        mirror_concurrency=mirror_concurrency,
        memory_limit=memory_limit,
        config=config,
        work_dir=TMP_WORK_DIR,
    )

    print("=== phase 1/3: mirror ===", flush=True)
    _obtain_rocks_at_tmp(common_tmp)
    print("=== mirror committed ===", flush=True)

    print("=== phase 2/3: stage (on /tmp) ===", flush=True)
    _run_subcommand("stage", common_tmp)
    print("=== persist raw: /tmp -> output volume ===", flush=True)
    _copy_concurrent(
        f"{TMP_WORK_DIR}/raw/{date}", f"{OUT_VOL_DIR}/raw/{date}", "raw-out"
    )
    volume_out.commit()
    print("=== stage committed ===", flush=True)

    print("=== phase 3/3: hydrate (on /tmp) ===", flush=True)
    _run_subcommand("hydrate", common_tmp)
    print("=== persist snapshot: /tmp -> output volume ===", flush=True)
    _copy_concurrent(
        f"{TMP_WORK_DIR}/snapshot/{date}",
        f"{OUT_VOL_DIR}/snapshot/{date}",
        "snapshot-out",
    )
    volume_out.commit()
    print("=== hydrate committed; snapshot ready ===", flush=True)


@app.function(
    image=image,
    volumes={
        "/vol-rocks": volume_rocks,
        "/vol-out": volume_out,
    },
    timeout=60 * 60 * 10,
    cpu=4.0,
    memory=12 * 1024,
    ephemeral_disk=1024 * 1024,
)
def single_phase(
    name: str,
    backup_id: int | None = None,
    snapshot_date: str | None = None,
    mirror_concurrency: int = 64,
    memory_limit: str = "8GiB",
    config: str | None = None,
) -> None:
    """Run a single phase: mirror, stage, or hydrate.

    Mirror downloads to /tmp (per Modal's dataset-ingestion guide) then
    persists rocks/ onto the Volume. Stage and hydrate stage their inputs
    onto /tmp first via _copy_concurrent, run on local NVMe, and copy
    outputs back to the Volume. Inbound copies short-circuit when /tmp is
    already populated.
    """
    if name not in {"mirror", "stage", "hydrate"}:
        raise SystemExit(f"name must be mirror/stage/hydrate, got {name!r}")
    date = _resolve_date(snapshot_date)

    if name == "mirror":
        common = _common_args(
            backup_id=backup_id,
            snapshot_date=date,
            mirror_concurrency=mirror_concurrency,
            memory_limit=memory_limit,
            config=config,
            work_dir=TMP_WORK_DIR,
        )
        _obtain_rocks_at_tmp(common)
        return

    common_scratch = _common_args(
        backup_id=backup_id,
        snapshot_date=date,
        mirror_concurrency=mirror_concurrency,
        memory_limit=memory_limit,
        config=config,
        work_dir=TMP_WORK_DIR,
    )

    if name == "stage":
        print("=== copy rocks: rocks volume -> /tmp ===", flush=True)
        _copy_concurrent(
            f"{ROCKS_VOL_DIR}/rocks",
            f"{TMP_WORK_DIR}/rocks",
            "rocks-in",
            skip_if_dst_populated=True,
        )
        _run_subcommand("stage", common_scratch)
        print("=== persist raw: /tmp -> output volume ===", flush=True)
        _copy_concurrent(
            f"{TMP_WORK_DIR}/raw/{date}",
            f"{OUT_VOL_DIR}/raw/{date}",
            "raw-out",
        )
        volume_out.commit()
        return

    # hydrate
    print("=== copy raw: output volume -> /tmp ===", flush=True)
    _copy_concurrent(
        f"{OUT_VOL_DIR}/raw/{date}",
        f"{TMP_WORK_DIR}/raw/{date}",
        "raw-in",
        skip_if_dst_populated=True,
    )
    _run_subcommand("hydrate", common_scratch)
    print("=== persist snapshot: /tmp -> output volume ===", flush=True)
    _copy_concurrent(
        f"{TMP_WORK_DIR}/snapshot/{date}",
        f"{OUT_VOL_DIR}/snapshot/{date}",
        "snapshot-out",
    )
    volume_out.commit()


@app.function(
    image=image,
    volumes={"/vol-rocks": volume_rocks},
    timeout=60 * 10,  # 10 min — open + property reads should take seconds
    cpu=1.0,
    memory=2 * 1024,
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


@app.function(
    image=image,
    volumes={"/vol-out": volume_out},
    secrets=[modal.Secret.from_name("atproto-snapshot")],
    timeout=60 * 60 * 4,
    cpu=2.0,
    memory=4 * 1024,
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
    memory_limit: str = "8GiB",
    config: str | None = None,
    background: bool = False,
) -> None:
    """Local entrypoint dispatcher.

    Args:
      phase: build | mirror | stage | hydrate | upload
      upload_after: when True and phase != upload, run upload after the
        chosen phase completes. Skipped for `upload` itself.
      background: spawn the remote call instead of waiting on it. With
        `modal run --detach`, plain .remote() may be cancelled when the
        local caller disconnects; .spawn() returns a FunctionCall handle
        that survives. Use this for long builds you want to walk away
        from. Follow progress with: `modal app logs <fn-call-id>`.
    """

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
        _kick(
            build,
            backup_id=backup_id,
            snapshot_date=snapshot_date,
            mirror_concurrency=mirror_concurrency,
            memory_limit=memory_limit,
            config=config,
        )
    elif phase in {"mirror", "stage", "hydrate"}:
        _kick(
            single_phase,
            name=phase,
            backup_id=backup_id,
            snapshot_date=snapshot_date,
            mirror_concurrency=mirror_concurrency,
            memory_limit=memory_limit,
            config=config,
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
            "build/mirror/stage/hydrate/upload/inspect"
        )

    if upload_after:
        _kick(upload, snapshot_date=snapshot_date, config=config)
