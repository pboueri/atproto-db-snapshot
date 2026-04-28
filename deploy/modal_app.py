"""Modal driver for at-snapshot.

Runs the full pipeline (mirror + stage + hydrate) on a Modal container
with enough disk to hold constellation's full backup. Outputs land on a
persistent Modal Volume; an optional step uploads them to R2.

Run a build:

    modal run deploy/modal_app.py::build

Resume / re-run a stage only:

    modal run deploy/modal_app.py::build --skip-mirror

Open an interactive duckdb shell against the latest snapshot:

    modal run deploy/modal_app.py::shell
"""
from __future__ import annotations

import os
import subprocess

import modal

DUCKDB_VERSION = "1.5.2"
SNAPSHOT_DATE_DEFAULT = ""  # empty → at-snapshot picks today UTC

# ---------------------------------------------------------------------------
# Image: Debian + Rust + libduckdb + the source tree, compiled in release.
# ---------------------------------------------------------------------------

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
        # Rust toolchain
        "curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal",
        # libduckdb headers + shared object
        f"mkdir -p /opt/duckdb/lib /opt/duckdb/include && "
        f"curl -fsSL -o /tmp/libduckdb.zip "
        f"https://github.com/duckdb/duckdb/releases/download/v{DUCKDB_VERSION}/libduckdb-linux-amd64.zip && "
        f"unzip -q /tmp/libduckdb.zip -d /tmp/libduckdb && "
        f"cp /tmp/libduckdb/libduckdb.so /opt/duckdb/lib/ && "
        f"cp /tmp/libduckdb/duckdb.h /tmp/libduckdb/duckdb.hpp /opt/duckdb/include/ && "
        f"rm -rf /tmp/libduckdb /tmp/libduckdb.zip",
        # Persistent shell env for cargo + at-snapshot
        "echo 'export PATH=$HOME/.cargo/bin:$PATH' >> /etc/profile.d/cargo.sh",
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
    # Ship the repo inside the image and compile once at image-build time.
    # Subsequent code edits invalidate the layer; cargo's incremental build
    # caches inside the layer keep rebuilds fast.
    .add_local_dir(".", remote_path="/app", copy=True)
    .run_commands(
        "cd /app && cargo build --release --bin at-snapshot",
    )
)

# ---------------------------------------------------------------------------
# Volume layout:
#   /vol/var/rocks/...                      mirror (~600+ GB)
#   /vol/var/raw/<date>/*.parquet           staging output
#   /vol/var/snapshot/<date>/snapshot.duckdb final artifact
# ---------------------------------------------------------------------------

volume = modal.Volume.from_name("at-snapshot-data", create_if_missing=True)

app = modal.App("at-snapshot")


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

@app.function(
    image=image,
    volumes={"/vol": volume},
    timeout=60 * 60 * 8,  # up to 8h for the full pipeline
    cpu=8.0,
    memory=32 * 1024,
    ephemeral_disk=1024 * 1024,  # 1 TiB scratch; mirror peaks ~700 GB
)
def build(
    skip_mirror: bool = False,
    skip_stage: bool = False,
    skip_hydrate: bool = False,
    backup_id: int | None = None,
    snapshot_date: str | None = None,
    mirror_concurrency: int = 64,
    memory_limit: str = "24GiB",
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
    volume.commit()


@app.function(image=image, volumes={"/vol": volume}, timeout=60 * 60)
def upload_to_r2(snapshot_date: str) -> None:
    """Push raw/<date> + snapshot/<date> to R2 using rclone."""
    src_raw = f"/vol/var/raw/{snapshot_date}"
    src_snap = f"/vol/var/snapshot/{snapshot_date}"
    if not os.path.isdir(src_raw) or not os.path.isdir(src_snap):
        raise SystemExit(f"missing snapshot artifacts for {snapshot_date}")
    # Configure rclone to point at R2 via secrets at runtime; left as an
    # exercise so this script doesn't hard-code creds.
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
    memory_limit: str = "24GiB",
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
