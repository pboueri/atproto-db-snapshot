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
import subprocess
import threading
import time
from datetime import datetime, timezone
from typing import Iterable

import modal

DUCKDB_VERSION = "1.5.2"

# Persistent storage (Modal Volume, network-FUSE; slow random reads).
# Mirror lives here forever so we don't re-download from constellation.
VOL_WORK_DIR = "/vol/var"

# Ephemeral local storage (declared via `ephemeral_disk` on each function;
# local NVMe-class). We rsync the rocksdb mirror here before stage so the
# random-read-heavy CF scans run against local disk, then rsync the
# resulting parquet / duckdb back to the Volume to persist them.
SCRATCH_WORK_DIR = "/scratch/var"

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
        "rsync",
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
    """Mirror the binary's default of today UTC so we can name rsync paths."""
    return snapshot_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _rsync(src: str, dst: str, label: str) -> None:
    """Bulk-copy a directory tree. Trailing slashes matter: we use 'src/' so
    rsync copies contents (not the parent) into 'dst/'."""
    if not os.path.exists(src):
        print(f"[rsync:{label}] skip — {src} does not exist", flush=True)
        return
    os.makedirs(dst, exist_ok=True)
    print(f"[rsync:{label}] {src} -> {dst}", flush=True)
    subprocess.check_call(
        [
            "rsync",
            "-a",
            "--delete",
            "--info=stats2,progress2",
            src.rstrip("/") + "/",
            dst.rstrip("/") + "/",
        ]
    )


class _PeriodicCommit:
    """Commit the Modal Volume every `interval_s` seconds in a thread.

    Used during the long mirror so partial state survives container loss.
    Modal Volumes already retain writes durably — this just makes those
    writes visible to other function calls without waiting for clean
    function exit.
    """

    def __init__(self, vol: modal.Volume, interval_s: int = 300) -> None:
        self._vol = vol
        self._interval = interval_s
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "_PeriodicCommit":
        self._thread = threading.Thread(
            target=self._loop, name="modal-commit", daemon=True
        )
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=30)

    def _loop(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                self._vol.commit()
                print(
                    f"[commit] volume committed at {time.strftime('%H:%M:%S')}",
                    flush=True,
                )
            except Exception as e:  # noqa: BLE001
                print(f"[commit] background commit failed: {e}", flush=True)


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
    """Run mirror + stage + hydrate as three separate subcommands.

    Mirror writes to the persistent Volume so the ~80GB rocksdb survives
    container restarts. Stage and hydrate run against ephemeral local disk
    (NVMe-class) instead of Volume FUSE, with rsync moving data in/out at
    the boundaries — bulk sequential transfers are dramatically faster
    than RocksDB's random reads against a network filesystem.
    """
    date = _resolve_date(snapshot_date)
    common_vol = _common_args(
        backup_id=backup_id,
        snapshot_date=date,
        mirror_concurrency=mirror_concurrency,
        memory_limit=memory_limit,
        config=config,
        work_dir=VOL_WORK_DIR,
    )
    common_scratch = _common_args(
        backup_id=backup_id,
        snapshot_date=date,
        mirror_concurrency=mirror_concurrency,
        memory_limit=memory_limit,
        config=config,
        work_dir=SCRATCH_WORK_DIR,
    )

    print("=== phase 1/3: mirror ===", flush=True)
    with _PeriodicCommit(volume, interval_s=300):
        _run_subcommand("mirror", common_vol)
    volume.commit()
    print("=== mirror committed ===", flush=True)

    print("=== copy rocks: volume -> scratch ===", flush=True)
    _rsync(f"{VOL_WORK_DIR}/rocks", f"{SCRATCH_WORK_DIR}/rocks", "rocks-in")

    print("=== phase 2/3: stage (on scratch) ===", flush=True)
    _run_subcommand("stage", common_scratch)
    print("=== persist raw: scratch -> volume ===", flush=True)
    _rsync(f"{SCRATCH_WORK_DIR}/raw/{date}", f"{VOL_WORK_DIR}/raw/{date}", "raw-out")
    volume.commit()
    print("=== stage committed ===", flush=True)

    print("=== phase 3/3: hydrate (on scratch) ===", flush=True)
    _run_subcommand("hydrate", common_scratch)
    print("=== persist snapshot: scratch -> volume ===", flush=True)
    _rsync(
        f"{SCRATCH_WORK_DIR}/snapshot/{date}",
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

    Mirror writes directly to the Volume. Stage and hydrate stage their
    inputs onto ephemeral local disk first, run there, and rsync outputs
    back. Each Modal call gets a fresh ephemeral filesystem, so we always
    have to rehydrate scratch before stage / hydrate.
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
            work_dir=VOL_WORK_DIR,
        )
        with _PeriodicCommit(volume, interval_s=300):
            _run_subcommand("mirror", common)
        volume.commit()
        return

    common_scratch = _common_args(
        backup_id=backup_id,
        snapshot_date=date,
        mirror_concurrency=mirror_concurrency,
        memory_limit=memory_limit,
        config=config,
        work_dir=SCRATCH_WORK_DIR,
    )

    if name == "stage":
        print("=== copy rocks: volume -> scratch ===", flush=True)
        _rsync(f"{VOL_WORK_DIR}/rocks", f"{SCRATCH_WORK_DIR}/rocks", "rocks-in")
        _run_subcommand("stage", common_scratch)
        print("=== persist raw: scratch -> volume ===", flush=True)
        _rsync(
            f"{SCRATCH_WORK_DIR}/raw/{date}",
            f"{VOL_WORK_DIR}/raw/{date}",
            "raw-out",
        )
        volume.commit()
        return

    # hydrate
    print("=== copy raw: volume -> scratch ===", flush=True)
    _rsync(
        f"{VOL_WORK_DIR}/raw/{date}",
        f"{SCRATCH_WORK_DIR}/raw/{date}",
        "raw-in",
    )
    _run_subcommand("hydrate", common_scratch)
    print("=== persist snapshot: scratch -> volume ===", flush=True)
    _rsync(
        f"{SCRATCH_WORK_DIR}/snapshot/{date}",
        f"{VOL_WORK_DIR}/snapshot/{date}",
        "snapshot-out",
    )
    volume.commit()


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
) -> None:
    """Local entrypoint dispatcher.

    Args:
      phase: build | mirror | stage | hydrate | upload
      upload_after: when True and phase != upload, run upload after the
        chosen phase completes. Skipped for `upload` itself.
    """
    if phase == "build":
        build.remote(
            backup_id=backup_id,
            snapshot_date=snapshot_date,
            mirror_concurrency=mirror_concurrency,
            memory_limit=memory_limit,
            config=config,
        )
    elif phase in {"mirror", "stage", "hydrate"}:
        single_phase.remote(
            name=phase,
            backup_id=backup_id,
            snapshot_date=snapshot_date,
            mirror_concurrency=mirror_concurrency,
            memory_limit=memory_limit,
            config=config,
        )
    elif phase == "upload":
        upload.remote(snapshot_date=snapshot_date, config=config)
        return
    else:
        raise SystemExit(
            f"unknown phase {phase!r}; expected build/mirror/stage/hydrate/upload"
        )

    if upload_after:
        upload.remote(snapshot_date=snapshot_date, config=config)
