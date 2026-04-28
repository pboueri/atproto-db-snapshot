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

# Persistent storage (Modal Volume, network-FUSE; slow random reads).
# Mirror lives here forever so we don't re-download from constellation.
VOL_WORK_DIR = "/vol/var"

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

# /vol/var/rocks/                 mirror (~600+ GB, persisted across runs)
# /vol/var/raw/<date>/            staging parquet
# /vol/var/snapshot/<date>/       snapshot.duckdb + metadata
volume = modal.Volume.from_name("at-snapshot-data", create_if_missing=True)

app = modal.App("at-snapshot")


def _common_args(
    *,
    backup_id: int | None,
    snapshot_date: str | None,
    mirror_concurrency: int,
    memory_limit: str,
    config: str | None,
    work_dir: str = VOL_WORK_DIR,
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


class _MultithreadedCopier:
    """Modal's RoseTTAFold dataset example
    (modal-examples/12_datasets/rosettafold.py): a small wrapper around a
    ThreadPool of shutil.copy2 calls. Modal Volume FUSE has high per-op
    latency but scales with concurrency, so 24 in-flight file copies
    saturate it far better than single-threaded rsync/tar."""

    def __init__(self, max_threads: int) -> None:
        self.pool = ThreadPool(max_threads)
        self.copy_jobs: list = []

    def copy(self, source: str, dest: str) -> None:
        res = self.pool.apply_async(
            shutil.copy2,
            args=(source, dest),
            error_callback=lambda exc: print(
                f"[copy] {source} failed: {exc}", file=sys.stderr, flush=True
            ),
        )
        self.copy_jobs.append(res)

    def __enter__(self) -> "_MultithreadedCopier":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.pool.close()
        self.pool.join()


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
) -> None:
    """Parallel directory copy. Patterned on Modal's RoseTTAFold example
    (`copy_concurrent`); uses shutil.copytree's directory walk while
    routing each file through a 24-thread copy pool. Significantly faster
    than single-threaded rsync against Modal Volume FUSE because Volume IO
    scales with concurrent ops.

    skip_if_dst_populated=True turns this into a "stage in if missing"
    primitive — used for inbound copies where /tmp may already hold the
    artifact (e.g. mirror just wrote rocks/ to /tmp and we'd otherwise
    re-pull from the Volume). Outbound persistence copies leave it
    False so /vol always reflects the latest /tmp content.
    """
    if not os.path.exists(src):
        print(f"[copy:{label}] skip — {src} does not exist", flush=True)
        return
    if skip_if_dst_populated and _dir_has_files(dst):
        print(
            f"[copy:{label}] skip — {dst} already populated, reusing in place",
            flush=True,
        )
        return
    os.makedirs(os.path.dirname(dst.rstrip("/")) or ".", exist_ok=True)
    t0 = time.time()
    print(
        f"[copy:{label}] {src} -> {dst} ({max_threads} threads)",
        flush=True,
    )
    with _MultithreadedCopier(max_threads=max_threads) as copier:
        shutil.copytree(
            src, dst, copy_function=copier.copy, dirs_exist_ok=True
        )
    elapsed = time.time() - t0
    # walk the destination once after the fact for a size readout
    total = 0
    for root, _, fnames in os.walk(dst):
        for f in fnames:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    rate = total / max(1, elapsed) / 1e6
    print(
        f"[copy:{label}] DONE {total / 1e9:.2f} GB in {elapsed:.1f}s "
        f"({rate:.1f} MB/s)",
        flush=True,
    )


def _obtain_rocks_at_tmp(common_tmp: list[str]) -> None:
    """Ensure /tmp/var/rocks holds a complete rocks tree, choosing the
    cheapest path:

      1. Already present at /tmp/var/rocks (e.g. mirror just ran in
         this container) — nothing to do.
      2. Already persisted on /vol/var/rocks from a prior run — copy
         volume → /tmp via the parallel copier. No constellation
         download. No /vol write.
      3. Neither — run the binary's mirror subcommand to download from
         constellation into /tmp, then persist /tmp → /vol.
    """
    if _rocks_looks_complete(f"{TMP_WORK_DIR}/rocks"):
        print(
            f"[mirror] {TMP_WORK_DIR}/rocks already complete; reusing in place",
            flush=True,
        )
        return
    if _rocks_looks_complete(f"{VOL_WORK_DIR}/rocks"):
        print(
            "[mirror] using existing rocks from volume (skipping download)",
            flush=True,
        )
        _copy_concurrent(
            f"{VOL_WORK_DIR}/rocks",
            f"{TMP_WORK_DIR}/rocks",
            "rocks-vol-to-tmp",
        )
        return
    print(
        "[mirror] no existing rocks; downloading from constellation to /tmp",
        flush=True,
    )
    _run_subcommand("mirror", common_tmp)
    print("[mirror] persisting fresh rocks: /tmp -> volume", flush=True)
    _copy_concurrent(
        f"{TMP_WORK_DIR}/rocks", f"{VOL_WORK_DIR}/rocks", "rocks-tmp-to-vol"
    )
    volume.commit()


@app.function(
    image=image,
    volumes={"/vol": volume},
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
    (local NVMe-class). The Volume is touched only as the persistence
    boundary — we _copy_concurrent the artifact at the end of each phase
    over to /vol so it survives container loss. mirror downloads
    directly to /tmp/var/rocks (no Volume IO during the long fetch),
    stage and hydrate read/write entirely on /tmp, and we batch a
    single _copy_concurrent to /vol after each phase succeeds.
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
    print("=== persist raw: /tmp -> volume ===", flush=True)
    _copy_concurrent(
        f"{TMP_WORK_DIR}/raw/{date}", f"{VOL_WORK_DIR}/raw/{date}", "raw-out"
    )
    volume.commit()
    print("=== stage committed ===", flush=True)

    print("=== phase 3/3: hydrate (on /tmp) ===", flush=True)
    _run_subcommand("hydrate", common_tmp)
    print("=== persist snapshot: /tmp -> volume ===", flush=True)
    _copy_concurrent(
        f"{TMP_WORK_DIR}/snapshot/{date}",
        f"{VOL_WORK_DIR}/snapshot/{date}",
        "snapshot-out",
    )
    volume.commit()
    print("=== hydrate committed; snapshot ready ===", flush=True)


@app.function(
    image=image,
    volumes={"/vol": volume},
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
        print("=== copy rocks: volume -> /tmp ===", flush=True)
        _copy_concurrent(
            f"{VOL_WORK_DIR}/rocks",
            f"{TMP_WORK_DIR}/rocks",
            "rocks-in",
            skip_if_dst_populated=True,
        )
        _run_subcommand("stage", common_scratch)
        print("=== persist raw: /tmp -> volume ===", flush=True)
        _copy_concurrent(
            f"{TMP_WORK_DIR}/raw/{date}",
            f"{VOL_WORK_DIR}/raw/{date}",
            "raw-out",
        )
        volume.commit()
        return

    # hydrate
    print("=== copy raw: volume -> /tmp ===", flush=True)
    _copy_concurrent(
        f"{VOL_WORK_DIR}/raw/{date}",
        f"{TMP_WORK_DIR}/raw/{date}",
        "raw-in",
        skip_if_dst_populated=True,
    )
    _run_subcommand("hydrate", common_scratch)
    print("=== persist snapshot: /tmp -> volume ===", flush=True)
    _copy_concurrent(
        f"{TMP_WORK_DIR}/snapshot/{date}",
        f"{VOL_WORK_DIR}/snapshot/{date}",
        "snapshot-out",
    )
    volume.commit()


@app.function(
    image=image,
    volumes={"/vol": volume},
    timeout=60 * 10,  # 10 min — open + property reads should take seconds
    cpu=1.0,
    memory=2 * 1024,
)
def inspect(
    config: str | None = None,
    memory_limit: str = "2GiB",
) -> None:
    """Cheap rocksdb inspection: opens /vol/var/rocks read-only and
    queries per-CF estimate-num-keys / SST sizes from the manifest. No
    scanning, no /tmp copy. Use to size pass B before kicking off a
    long stage run.
    """
    common = _common_args(
        backup_id=None,
        snapshot_date=None,
        mirror_concurrency=1,
        memory_limit=memory_limit,
        config=config,
        work_dir=VOL_WORK_DIR,
    )
    _run_subcommand("inspect", common)


@app.function(
    image=image,
    volumes={"/vol": volume},
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
