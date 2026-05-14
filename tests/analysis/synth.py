"""Synthetic snapshot.duckdb generator for analysis tests.

Creates a DuckDB file with the same tables and column types the real
hydrate pipeline produces, populated with a small but structurally
representative dataset. Enough volume + spread that each analysis
produces non-trivial output:

  actor_aggs   500 actors with a power-law-ish likes_in / followers
  post_aggs   ~2k posts keyed on uri_id
  posts        same set, with author_did_id + created_at
  likes      ~10k like events
  follows    ~5k follow edges
  reposts    ~1k repost events
  blocks      ~500 block edges

Determinism: a fixed seed (default 0) means every test run sees the
same data, so we can assert against exact-ish numbers where useful.

The generator goes through DuckDB itself rather than constructing rows
in Python — keeps the file format identical to what hydrate produces.
"""

from __future__ import annotations

import os
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb

# Snapshot date the tests use. Picked far enough out that all synthetic
# created_at timestamps land before it.
DEFAULT_SNAPSHOT_DATE = "2026-04-28"


def make_synthetic_snapshot(
    path: str | Path,
    *,
    seed: int = 0,
    snapshot_date: str = DEFAULT_SNAPSHOT_DATE,
    n_actors: int = 500,
    n_posts: int = 2_000,
    n_likes: int = 10_000,
    n_follows: int = 5_000,
    n_reposts: int = 1_000,
    n_blocks: int = 500,
    history_days: int = 365,
) -> str:
    """Build a synthetic snapshot.duckdb at `path`. Returns the path."""
    path = str(path)
    if os.path.exists(path):
        os.remove(path)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    rng = random.Random(seed)
    snap = date.fromisoformat(snapshot_date)
    # Spread activity over `history_days` ending just before the snapshot.
    end = datetime.combine(snap, datetime.min.time()) - timedelta(hours=1)
    start = end - timedelta(days=history_days)

    def _ts(days_back_max: int) -> str:
        # Bias toward more recent activity so cohorts + windowed analyses
        # have realistic density at the recent end.
        d = rng.random() ** 1.5 * days_back_max
        ts = end - timedelta(days=d)
        return ts.strftime("%Y-%m-%d %H:%M:%S")

    # Power-law actor weights — used to pick "popular" authors / targets.
    # Without this every actor receives the same engagement, Gini → 0,
    # and the likes analysis becomes a degenerate flat distribution.
    weights = [1.0 / ((i + 1) ** 0.9) for i in range(n_actors)]
    actor_ids = list(range(n_actors))

    # ----- actor_aggs ---------------------------------------------------
    # likes_in / followers / blocks_in are concentrated in the head of
    # the distribution. The first ~5 actors are "celebrities" (high
    # everything); the next ~50 are "engaged" power users; the long tail
    # is mostly zeros — mirrors the real shape.
    actor_aggs_rows = []
    for i in actor_ids:
        rank = i + 1
        followers = max(0, int(20000 / rank ** 0.9 + rng.gauss(0, 5)))
        likes_in = max(0, int(200000 / rank ** 0.85 + rng.gauss(0, 20)))
        blocks_in = max(0, int(500 / rank ** 0.95 + rng.gauss(0, 1))) if rank <= 200 else 0
        # Most actors post; a fraction don't (lurkers).
        posts = int(max(0, 200 / rank ** 0.8 + rng.gauss(0, 2)))
        if rng.random() < 0.15:
            posts = 0  # forces some lurker tier
        likes_out = int(rng.expovariate(1 / 30))
        reposts_out = int(rng.expovariate(1 / 8))
        reposts_in = max(0, int(likes_in * 0.1 + rng.gauss(0, 2)))
        replies_out = int(posts * rng.uniform(0.0, 0.4))
        quotes_out = int(posts * rng.uniform(0.0, 0.15))
        quoted_count = int(reposts_in * 0.4)
        follows = int(rng.expovariate(1 / 50))
        blocks_out = int(rng.expovariate(1 / 5)) if rng.random() < 0.3 else 0
        actor_aggs_rows.append((
            i, follows, followers, blocks_out, blocks_in, posts,
            likes_out, likes_in, reposts_out, reposts_in,
            replies_out, quotes_out, quoted_count,
        ))

    # ----- posts + post_aggs --------------------------------------------
    # Authors picked by power-law weight so a small set of accounts owns
    # most of the corpus — necessary for the ratio analysis to produce
    # populated per-author tiers and for the leaderboard.
    posts_rows = []
    post_aggs_rows = []
    for uri_id in range(n_posts):
        author = rng.choices(actor_ids, weights=weights, k=1)[0]
        created = _ts(history_days)
        # Engagement per post: another long tail biased by the author's
        # weight. Higher-weight authors get more attention per post.
        author_rank_pull = (n_actors / (author + 1)) ** 0.7
        likes = int(rng.expovariate(1 / (1 + author_rank_pull)))
        reposts = int(likes * rng.uniform(0.0, 0.4))
        replies = int(likes * rng.uniform(0.0, 0.6))
        quotes = int(likes * rng.uniform(0.0, 0.2))
        # Bias ~3% of high-engagement posts to actually cross the ratio
        # threshold (argument > likes) so the ratio analysis has signal.
        if rng.random() < 0.03 and likes >= 10:
            replies = int(likes * 1.3) + 1
        posts_rows.append((uri_id, author, created))
        post_aggs_rows.append((uri_id, likes, reposts, replies, quotes))

    # ----- likes -------------------------------------------------------
    likes_rows = []
    for _ in range(n_likes):
        actor = rng.choice(actor_ids)
        subject_uri = rng.randrange(n_posts) if rng.random() < 0.95 else None
        likes_rows.append((actor, subject_uri, _ts(history_days)))

    # ----- follows -----------------------------------------------------
    follows_rows = set()
    while len(follows_rows) < n_follows:
        src = rng.choice(actor_ids)
        dst = rng.choices(actor_ids, weights=weights, k=1)[0]
        if src == dst:
            continue
        follows_rows.add((src, dst, _ts(history_days)))
    follows_rows = list(follows_rows)

    # ----- reposts -----------------------------------------------------
    reposts_rows = []
    for _ in range(n_reposts):
        actor = rng.choice(actor_ids)
        subject_uri = rng.randrange(n_posts)
        reposts_rows.append((actor, subject_uri, _ts(history_days)))

    # ----- blocks ------------------------------------------------------
    blocks_rows = set()
    while len(blocks_rows) < n_blocks:
        src = rng.choice(actor_ids)
        dst = rng.choices(actor_ids, weights=weights, k=1)[0]
        if src == dst:
            continue
        blocks_rows.add((src, dst))
    blocks_rows = list(blocks_rows)

    # Write everything via DuckDB so the on-disk schema matches the real
    # snapshot byte-for-byte (column order, types).
    con = duckdb.connect(path)
    con.execute("BEGIN")
    con.execute(
        """
        CREATE TABLE actor_aggs (
          did_id        BIGINT,
          follows       BIGINT,
          followers     BIGINT,
          blocks_out    BIGINT,
          blocks_in     BIGINT,
          posts         BIGINT,
          likes_out     BIGINT,
          likes_in      BIGINT,
          reposts_out   BIGINT,
          reposts_in    BIGINT,
          replies_out   BIGINT,
          quotes_out    BIGINT,
          quoted_count  BIGINT
        )
        """
    )
    con.executemany(
        "INSERT INTO actor_aggs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        actor_aggs_rows,
    )

    con.execute(
        """
        CREATE TABLE posts (
          uri_id        BIGINT,
          author_did_id BIGINT,
          created_at    TIMESTAMP,
          reply_parent_uri_id BIGINT,
          quote_uri_id  BIGINT
        )
        """
    )
    con.executemany(
        "INSERT INTO posts (uri_id, author_did_id, created_at) VALUES (?, ?, ?)",
        posts_rows,
    )

    con.execute(
        """
        CREATE TABLE post_aggs (
          uri_id  BIGINT,
          likes   BIGINT,
          reposts BIGINT,
          replies BIGINT,
          quotes  BIGINT
        )
        """
    )
    con.executemany(
        "INSERT INTO post_aggs VALUES (?, ?, ?, ?, ?)",
        post_aggs_rows,
    )

    con.execute(
        """
        CREATE TABLE likes (
          actor_did_id   BIGINT,
          subject_uri_id BIGINT,
          created_at     TIMESTAMP
        )
        """
    )
    con.executemany(
        "INSERT INTO likes VALUES (?, ?, ?)",
        likes_rows,
    )

    con.execute(
        """
        CREATE TABLE follows (
          src_did_id BIGINT,
          dst_did_id BIGINT,
          created_at TIMESTAMP
        )
        """
    )
    con.executemany(
        "INSERT INTO follows VALUES (?, ?, ?)",
        follows_rows,
    )

    con.execute(
        """
        CREATE TABLE reposts (
          actor_did_id   BIGINT,
          subject_uri_id BIGINT,
          created_at     TIMESTAMP
        )
        """
    )
    con.executemany(
        "INSERT INTO reposts VALUES (?, ?, ?)",
        reposts_rows,
    )

    con.execute(
        """
        CREATE TABLE blocks (
          src_did_id BIGINT,
          dst_did_id BIGINT,
          created_at TIMESTAMP
        )
        """
    )
    # blocks have no created_at in some snapshots, but the real schema
    # includes one. Stamp them at a random plausibility-clean ts so the
    # attrition / blocks analyses both work.
    blocks_with_ts = [(s, d, _ts(history_days)) for s, d in blocks_rows]
    con.executemany("INSERT INTO blocks VALUES (?, ?, ?)", blocks_with_ts)

    con.execute(
        """
        CREATE TABLE snapshot_metadata (
          snapshot_date       DATE,
          source_url          TEXT,
          backup_id           BIGINT,
          built_at            TIMESTAMP,
          at_snapshot_version TEXT,
          duckdb_memory_limit TEXT
        )
        """
    )
    con.execute(
        "INSERT INTO snapshot_metadata VALUES (?, ?, ?, ?, ?, ?)",
        [snapshot_date, "synthetic://test", 0,
         datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
         "test-0.0.0", "1GiB"],
    )
    con.execute("COMMIT")
    con.close()
    return path
