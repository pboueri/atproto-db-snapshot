"""Modal driver for at-snapshot.

Runs the full pipeline (mirror + stage + hydrate) on a Modal container
with enough disk to hold constellation's full backup. Outputs land on a
persistent Modal Volume.

  modal run deploy/modal_app.py                            # full build
  modal run deploy/modal_app.py --skip-mirror              # resume
  modal run deploy/modal_app.py --backup-id 679

Each phase commits the Volume on success so a later container can resume
from the last good state. The mirror also commits in the background
every five minutes during the long download.
"""
from __future__ import annotations

import os
import subprocess
import threading
import time

import modal

DUCKDB_VERSION = "1.5.2"

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


def _run_at_snapshot(
    *,
    skip_mirror: bool,
    skip_stage: bool,
    skip_hydrate: bool,
    backup_id: int | None,
    snapshot_date: str | None,
    mirror_concurrency: int,
    memory_limit: str,
) -> None:
    args = [
        "/app/target/release/at-snapshot",
        "build",
        "--work-dir",
        "/vol/var",
        "--memory-limit",
        memory_limit,
        "--mirror-concurrency",
        str(mirror_concurrency),
    ]
    if backup_id is not None:
        args += ["--backup-id", str(backup_id)]
    if snapshot_date:
        args += ["--snapshot-date", snapshot_date]
    if skip_mirror:
        args.append("--skip-mirror")
    if skip_stage:
        args.append("--skip-stage")
    if skip_hydrate:
        args.append("--skip-hydrate")
    env = {**os.environ, "RUST_LOG": "info,object_store=warn"}
    print("running:", " ".join(args), flush=True)
    subprocess.check_call(args, env=env)


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
                print(f"[commit] volume committed at {time.strftime('%H:%M:%S')}", flush=True)
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
    skip_mirror: bool = False,
    skip_stage: bool = False,
    skip_hydrate: bool = False,
    backup_id: int | None = None,
    snapshot_date: str | None = None,
    mirror_concurrency: int = 64,
    memory_limit: str = "8GiB",
) -> None:
    common = dict(
        backup_id=backup_id,
        snapshot_date=snapshot_date,
        mirror_concurrency=mirror_concurrency,
        memory_limit=memory_limit,
    )

    if not skip_mirror:
        print("=== phase 1/3: mirror ===", flush=True)
        with _PeriodicCommit(volume, interval_s=300):
            _run_at_snapshot(
                skip_mirror=False,
                skip_stage=True,
                skip_hydrate=True,
                **common,
            )
        volume.commit()
        print("=== mirror committed ===", flush=True)

    if not skip_stage:
        print("=== phase 2/3: stage ===", flush=True)
        _run_at_snapshot(
            skip_mirror=True,
            skip_stage=False,
            skip_hydrate=True,
            **common,
        )
        volume.commit()
        print("=== stage committed ===", flush=True)

    if not skip_hydrate:
        print("=== phase 3/3: hydrate ===", flush=True)
        _run_at_snapshot(
            skip_mirror=True,
            skip_stage=True,
            skip_hydrate=False,
            **common,
        )
        volume.commit()
        print("=== hydrate committed; snapshot ready ===", flush=True)


@app.function(image=image, volumes={"/vol": volume}, timeout=60 * 60)
def upload_to_r2(snapshot_date: str) -> None:
    """Push raw/<date> + snapshot/<date> to R2."""
    src_raw = f"/vol/var/raw/{snapshot_date}"
    src_snap = f"/vol/var/snapshot/{snapshot_date}"
    if not os.path.isdir(src_raw) or not os.path.isdir(src_snap):
        raise SystemExit(f"missing snapshot artifacts for {snapshot_date}")
    raise NotImplementedError(
        "wire up rclone or boto3 with a Modal Secret for R2 before enabling"
    )


@app.local_entrypoint()
def main(
    skip_mirror: bool = False,
    skip_stage: bool = False,
    skip_hydrate: bool = False,
    backup_id: int | None = None,
    snapshot_date: str | None = None,
    mirror_concurrency: int = 64,
    memory_limit: str = "8GiB",
) -> None:
    build.remote(
        skip_mirror=skip_mirror,
        skip_stage=skip_stage,
        skip_hydrate=skip_hydrate,
        backup_id=backup_id,
        snapshot_date=snapshot_date,
        mirror_concurrency=mirror_concurrency,
        memory_limit=memory_limit,
    )
