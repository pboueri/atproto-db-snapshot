#!/usr/bin/env python3
"""Sum bytes in the constellation S3 endpoint.

Two modes:

  global       List every key in the bucket and sum sizes. Slow for the
               full bucket (~minutes) but gives a true total including
               older backups still pinned in shared_checksum/.

  backup ID    Parse meta/<ID>, then look up each shared_checksum/*.sst
               file referenced by that manifest and sum its Size from
               the bucket listing. This is the disk a single restore of
               that backup actually needs.

Examples:

  python scripts/bucket_size.py global
  python scripts/bucket_size.py backup 679
  python scripts/bucket_size.py backup 679 --endpoint https://constellation.t3.storage.dev
"""
from __future__ import annotations

import argparse
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import defaultdict
from typing import Iterator

DEFAULT_ENDPOINT = "https://constellation.t3.storage.dev"
NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}


def list_objects(endpoint: str, prefix: str = "") -> Iterator[tuple[str, int]]:
    """Yield (key, size) for every object under `prefix`."""
    token: str | None = None
    while True:
        params = {"list-type": "2", "max-keys": "1000", "prefix": prefix}
        if token:
            params["continuation-token"] = token
        url = f"{endpoint}/?{urllib.parse.urlencode(params)}"
        with urllib.request.urlopen(url) as resp:
            body = resp.read()
        root = ET.fromstring(body)
        for c in root.findall("s3:Contents", NS):
            key = c.findtext("s3:Key", default="", namespaces=NS)
            size = int(c.findtext("s3:Size", default="0", namespaces=NS))
            yield key, size
        truncated = root.findtext("s3:IsTruncated", default="false", namespaces=NS)
        if truncated.lower() != "true":
            return
        token = root.findtext("s3:NextContinuationToken", default=None, namespaces=NS)
        if not token:
            return


def fetch_meta(endpoint: str, backup_id: int) -> tuple[int, list[str]]:
    """Return (file_count, [shared_checksum/*.sst, ...]) from meta/<id>."""
    url = f"{endpoint}/meta/{backup_id}"
    with urllib.request.urlopen(url) as resp:
        text = resp.read().decode("utf-8", errors="replace")
    lines = text.splitlines()
    if len(lines) < 3:
        raise SystemExit(f"meta/{backup_id} too short ({len(lines)} lines)")
    file_count = int(lines[2].strip())
    paths: list[str] = []
    # Format: each remaining line is "<path> crc32 <crc>".
    for ln in lines[3:]:
        parts = ln.split()
        if not parts:
            continue
        paths.append(parts[0])
    if len(paths) != file_count:
        # Tolerate; just warn.
        print(
            f"warning: meta/{backup_id} reported {file_count} files but parsed {len(paths)}",
            file=sys.stderr,
        )
    return file_count, paths


def fmt(n: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    f = float(n)
    for u in units:
        if abs(f) < 1024:
            return f"{f:8.2f} {u:>3}"
        f /= 1024
    return f"{f:8.2f} PiB"


def cmd_global(endpoint: str) -> None:
    by_top: dict[str, tuple[int, int]] = defaultdict(lambda: (0, 0))
    total_bytes = 0
    total_count = 0
    for key, size in list_objects(endpoint):
        total_bytes += size
        total_count += 1
        top = key.split("/", 1)[0] if "/" in key else key
        cur = by_top[top]
        by_top[top] = (cur[0] + size, cur[1] + 1)
        if total_count % 50_000 == 0:
            print(
                f"  ... {total_count:>10,} keys / {fmt(total_bytes)}",
                file=sys.stderr,
            )

    print(f"\n{'prefix':<24} {'bytes':>14} {'count':>10}")
    print("-" * 52)
    for prefix, (size, count) in sorted(by_top.items(), key=lambda kv: -kv[1][0]):
        print(f"{prefix:<24} {fmt(size):>14} {count:>10,}")
    print("-" * 52)
    print(f"{'TOTAL':<24} {fmt(total_bytes):>14} {total_count:>10,}")


def cmd_backup(endpoint: str, backup_id: int) -> None:
    print(f"fetching meta/{backup_id} ...", file=sys.stderr)
    declared, paths = fetch_meta(endpoint, backup_id)
    wanted = set(paths)
    print(
        f"manifest: {declared} files, listing shared_checksum/* to size them ...",
        file=sys.stderr,
    )

    seen: dict[str, int] = {}
    listed = 0
    for key, size in list_objects(endpoint, prefix="shared_checksum/"):
        listed += 1
        if key in wanted:
            seen[key] = size
        if listed % 50_000 == 0:
            print(
                f"  ... {listed:>10,} listed, {len(seen):>10,} matched / {len(wanted):>10,} wanted",
                file=sys.stderr,
            )

    missing = wanted - seen.keys()
    total = sum(seen.values())
    print()
    print(f"backup id            : {backup_id}")
    print(f"manifest entries     : {declared:>12,}")
    print(f"matched in bucket    : {len(seen):>12,}")
    print(f"missing from bucket  : {len(missing):>12,}")
    print(f"on-disk restore size : {fmt(total)}")
    if missing and len(missing) <= 5:
        print()
        for m in sorted(missing)[:5]:
            print(f"  missing: {m}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["global", "backup"])
    p.add_argument("backup_id", nargs="?", type=int)
    p.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    args = p.parse_args()

    if args.mode == "global":
        cmd_global(args.endpoint)
    else:
        if args.backup_id is None:
            p.error("backup mode needs a backup id, e.g. `bucket_size.py backup 679`")
        cmd_backup(args.endpoint, args.backup_id)


if __name__ == "__main__":
    main()
