# Modal driver

`modal_app.py` runs the at-snapshot pipeline on a Modal container so it
has the disk and bandwidth a full constellation backup needs (~650 GB
download peak, ~700 GB on disk peak).

## One-time setup

```sh
pip install modal
modal token new
```

## Build a snapshot

```sh
modal run deploy/modal_app.py
```

This calls the `build` function with defaults: latest backup, today's
date, 64 concurrent fetches, 24 GiB DuckDB memory, 800 GiB disk cap.

Pinned backup + date:

```sh
modal run deploy/modal_app.py --backup-id 679 --snapshot-date 2026-04-27
```

Skip flags resume mid-pipeline against the persisted volume:

```sh
modal run deploy/modal_app.py --skip-mirror --skip-stage   # only re-run hydrate
```

## Volume layout

The Modal Volume `at-snapshot-data` is mounted at `/vol`. Inside:

```
/vol/var/rocks/                     # constellation mirror, persistent
/vol/var/raw/<date>/*.parquet       # staging output
/vol/var/snapshot/<date>/snapshot.duckdb
/vol/var/snapshot/<date>/snapshot_metadata.json
```

Mirror state survives across runs, so the second build only downloads
new SST files since the last backup.

## Knobs (all optional)

| Flag | Default | Notes |
|---|---|---|
| `--backup-id` | latest | Pin a constellation backup id (`meta/<id>` in the bucket). |
| `--snapshot-date` | today UTC | Output namespace. |
| `--mirror-concurrency` | 64 | Drop to 8–16 if Tigris rate-limits. |
| `--memory-limit` | 24GiB | DuckDB cap. |
| `--skip-{mirror,stage,hydrate}` | — | Resume mid-pipeline. |

## R2 upload

The `upload_to_r2` function is a stub today — wire up rclone or boto3
with a Modal Secret holding R2 credentials when you're ready to publish.
