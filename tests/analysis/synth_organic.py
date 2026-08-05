"""Synthetic snapshot with *known* like-arrival shapes.

The generic `synth.py` fixture has random engagement timing, which is
enough to prove `analysis/organic.py` runs but proves nothing about
whether its score means anything. This builds posts whose arrival
processes are known by construction, so the tests can assert the score
ranks them the way the maths says it should.

Five families, each a deliberate point on the organic/irregular axis:

  organic    delays log-uniform over the whole window. This *is* the null
             — the log asymptote N(t) = A ln(1 + t/tau) has constant
             density in log-time — so these must score near the floor.
  spike      everything inside the first 10 minutes, then silence.
  necro      silence, then everything in the final few hours.
  two_burst  half immediately, half a day later, nothing between.
  linear     delays uniform in *linear* time. Worth its own family
             because it looks unremarkable on a linear axis yet is
             strongly back-loaded in log-time: most of a uniform day
             lands in the last log bin or two.

Only `organic` should sit below the calibrated band. The rest exist to
show the score notices different flavours of irregular, not just one.
"""

from __future__ import annotations

import math
import random
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

SNAPSHOT_DATE = "2026-04-28"

# The cohort resolves to [cut - horizon - cohort_days, cut - horizon], so
# with cut = 2026-04-28 00:00, a 24h horizon and a 14d cohort the window
# is [2026-04-13, 2026-04-27]. Posts are placed well inside it.
POST_LO = datetime(2026, 4, 14, 0, 0, 0)
POST_HI = datetime(2026, 4, 26, 0, 0, 0)
CUT = datetime(2026, 4, 28, 0, 0, 0)

HORIZON_S = 24 * 3600
FAMILIES = ("organic", "spike", "necro", "two_burst", "linear")

# uri_ids are blocked by family so a test can recover the family from the
# id alone: family i owns [BLOCK*(i+1), BLOCK*(i+1) + n_posts).
BLOCK = 1_000_000


def family_of(uri_id: int) -> str | None:
    """Inverse of the uri_id blocking scheme; None for filler posts."""
    i = uri_id // BLOCK - 1
    if 0 <= i < len(FAMILIES):
        return FAMILIES[i]
    return None


def _log_uniform(rng, lo_s: float, hi_s: float) -> float:
    return 10 ** rng.uniform(math.log10(lo_s), math.log10(hi_s))


def _delays(rng, family: str, n: int) -> list[float]:
    """n like delays in seconds, drawn from the family's arrival process."""
    if family == "organic":
        # Constant density in log-time == the log asymptote. The null.
        return [_log_uniform(rng, 60, HORIZON_S) for _ in range(n)]
    if family == "spike":
        return [_log_uniform(rng, 60, 600) for _ in range(n)]
    if family == "necro":
        return [rng.uniform(0.83 * HORIZON_S, 0.99 * HORIZON_S) for _ in range(n)]
    if family == "two_burst":
        return [
            _log_uniform(rng, 60, 300) if i % 2 == 0
            else rng.uniform(0.55 * HORIZON_S, 0.70 * HORIZON_S)
            for i in range(n)
        ]
    if family == "linear":
        return [rng.uniform(60, HORIZON_S) for _ in range(n)]
    raise ValueError(family)


def make_organic_snapshot(
    path: str | Path,
    *,
    seed: int = 0,
    posts_per_family: int = 140,
    likes_per_post: int = 160,
    n_actors: int = 4_000,
) -> Path:
    """Write a snapshot.duckdb whose like timing is known per family."""
    path = Path(path)
    if path.exists():
        path.unlink()
    rng = random.Random(seed)

    posts, aggs, likes = [], [], []
    span = (POST_HI - POST_LO).total_seconds()

    for fi, family in enumerate(FAMILIES):
        for k in range(posts_per_family):
            uri_id = BLOCK * (fi + 1) + k
            author = rng.randrange(n_actors)
            created = POST_LO + timedelta(seconds=rng.uniform(0, span))
            posts.append((uri_id, author, f"rk{uri_id}", created,
                          None, None, None, "record"))
            ds = _delays(rng, family, likes_per_post)
            aggs.append((uri_id, len(ds), 0, 0, 0))
            for d in ds:
                # Any actor but the author: self-likes are excluded by the
                # extraction, and letting them through would silently
                # shrink n for a handful of posts.
                actor = rng.randrange(n_actors)
                if actor == author:
                    actor = (actor + 1) % n_actors
                likes.append((actor, uri_id, created + timedelta(seconds=d)))

    # Filler posts at the window edge. These are what set `cut`, and
    # therefore the cohort bounds — without them `cut` would fall inside
    # the family window and censor half the cohort.
    for k in range(60):
        uri_id = BLOCK * (len(FAMILIES) + 2) + k
        posts.append((uri_id, rng.randrange(n_actors), f"rkf{uri_id}",
                      CUT - timedelta(minutes=k), None, None, None, "record"))
        aggs.append((uri_id, 0, 0, 0, 0))

    con = duckdb.connect(str(path))
    con.execute("""
        CREATE TABLE posts (
          uri_id              BIGINT,
          author_did_id       BIGINT,
          rkey                VARCHAR,
          created_at          TIMESTAMP,
          reply_root_uri_id   BIGINT,
          reply_parent_uri_id BIGINT,
          quote_uri_id        BIGINT,
          source              VARCHAR
        )
    """)
    con.executemany("INSERT INTO posts VALUES (?,?,?,?,?,?,?,?)", posts)

    con.execute("""
        CREATE TABLE post_aggs (
          uri_id BIGINT, likes BIGINT, reposts BIGINT,
          replies BIGINT, quotes BIGINT
        )
    """)
    con.executemany("INSERT INTO post_aggs VALUES (?,?,?,?,?)", aggs)

    con.execute("""
        CREATE TABLE likes (
          actor_did_id BIGINT, subject_uri_id BIGINT, created_at TIMESTAMP
        )
    """)
    con.executemany("INSERT INTO likes VALUES (?,?,?)", likes)

    # reposts exists in the real schema; organic.py only reads likes, but
    # keeping the table present means the fixture stays a drop-in.
    con.execute("""
        CREATE TABLE reposts (
          actor_did_id BIGINT, subject_uri_id BIGINT, created_at TIMESTAMP
        )
    """)
    con.close()
    return path
