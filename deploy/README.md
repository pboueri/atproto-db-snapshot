# Modal driver

`modal_app.py` runs the at-snapshot pipeline on a Modal container so it
has the disk and bandwidth a full constellation backup needs (~600 GB
download, ~700 GB on disk peak).

## Cost & persistence

Compute, on Modal Standard (4 CPU + 12 GiB) — back-of-envelope:

| Phase | Approx wall time |
|---|---|
| Mirror     | ~1 h |
| Stage      | ~3 h |
| Hydrate    | ~1 h |
| **Total**  | ~5 h |

At Modal's compute rates that's roughly **~$8 per build**. Volume
storage charges (if any) depend on your Modal plan — see
[modal.com/pricing](https://modal.com/pricing). Constellation dedups
SSTs in `shared_checksum/`, so the second build only pulls deltas —
leaving the rocks mirror in place is the cheap path for recurring
snapshots.

Each phase calls `volume.commit()` on success, and the mirror also
commits in a background thread every 5 minutes, so a container crash
mid-mirror leaves the partial state visible to the next run.

## One-time setup

```sh
pip install modal
modal token new
```

For uploads, also create a Modal Secret with R2 credentials:

```sh
modal secret create atproto-snapshot \
  R2_ACCESS_KEY_ID=... \
  R2_SECRET_ACCESS_KEY=...
```

## Build a snapshot

Full pipeline (mirror → stage → hydrate):

```sh
modal run deploy/modal_app.py
```

Run a single phase against the persistent volume:

```sh
modal run deploy/modal_app.py --phase mirror
modal run deploy/modal_app.py --phase stage
modal run deploy/modal_app.py --phase hydrate
```

Pinned backup + date:

```sh
modal run deploy/modal_app.py --backup-id 679 --snapshot-date 2026-04-27
```

## Upload to R2

After a build completes, push raw + snapshot to R2:

```sh
modal run deploy/modal_app.py --phase upload --snapshot-date 2026-04-27 \
  --config /app/deploy/at-snapshot.toml
```

Or do it in one shot after a fresh build:

```sh
modal run deploy/modal_app.py --upload-after --config /app/deploy/at-snapshot.toml
```

The config file lives inside the image (added via `add_local_dir`), so
reference it by its `/app/...` path. R2 credentials come from the
`r2-credentials` Modal Secret; everything else (bucket, account_id,
prefix) is in the config.

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
| `--phase` | `build` | `build`, `mirror`, `stage`, `hydrate`, or `upload`. |
| `--upload-after` | `false` | When set and `--phase` ≠ `upload`, run upload after the chosen phase. |
| `--backup-id` | latest | Pin a constellation backup id (`meta/<id>` in the bucket). |
| `--snapshot-date` | today UTC | Output namespace. |
| `--mirror-concurrency` | 64 | Drop to 8–16 if Tigris rate-limits. |
| `--memory-limit` | 8GiB | DuckDB cap. |
| `--config` | — | Path to a config TOML inside the image (e.g. `/app/deploy/at-snapshot.toml`). Required for upload. |
