"""Synthetic snapshot with *known* engagement archetypes, for testing
`analysis/lifelines.py`.

The generic `synth.py` fixture makes a structurally valid snapshot but its
engagement is scattered uniformly in time, which is exactly the thing the
lifelines analysis measures — so it cannot tell us whether the classifier
works. This builder instead plants posts whose arrival processes are drawn
from hand-specified archetype recipes, then asserts the classifier recovers
the label it was given.

That makes this a semi-synthetic injection test: the only honest way to
quote a detection rate for a taxonomy with no ground truth in the real data.
Each recipe encodes the *defining* property of its archetype and nothing
else, so a test failure points at the rule that drifted.

Unlike `synth.py` this fixture carries the full `posts` schema (`rkey`,
`source`, `reply_root_uri_id`, `quote_uri_id`) because the analysis reads
all of them.
"""

from __future__ import annotations

import math
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

SNAPSHOT_DATE = "2026-07-31"
CUT = datetime(2026, 7, 31, 0, 0, 0)
HORIZON_HOURS = 168
COHORT_DAYS = 30

# Actor id layout. Followers of an author are a contiguous block so the
# in-network / out-of-network split is easy to control per recipe.
N_AUTHORS = 12
FOLLOWER_LO, FOLLOWER_HI = 1_000, 4_000      # in-network engager pool
STRANGER_LO, STRANGER_HI = 10_000, 40_000    # out-of-network engager pool

# TID alphabet, so generated rkeys look like real ones (the analysis only
# uses rkey to build example URLs, but keeping it realistic costs nothing).
_TID_ALPHABET = "234567abcdefghijklmnopqrstuvwxyz"


def _rkey(rng: random.Random) -> str:
    return "3" + "".join(rng.choice(_TID_ALPHABET) for _ in range(12))


def _insert(con, table: str, names: list[str], columns: list, *,
            select: str | None = None) -> None:
    """Bulk-insert column-oriented data through DuckDB's numpy replacement scan.

    `executemany` binds one row at a time and costs ~100s for the ~100k rows
    this fixture generates — longer than every other analysis test combined.
    Handing DuckDB a dict of numpy arrays instead lets it ingest the whole
    batch as vectors, which is roughly four orders of magnitude faster.
    """
    import numpy as np

    payload = {}
    for name, col in zip(names, columns):
        col = list(col)
        first = next((v for v in col if v is not None), None)
        if isinstance(first, datetime):
            arr = np.array([np.datetime64(v, "us") for v in col],
                           dtype="datetime64[us]")
        elif isinstance(first, str):
            arr = np.array(col, dtype=object)
        else:
            # None -> -1 sentinel; callers unwrap it with NULLIF in `select`.
            arr = np.array([-1 if v is None else int(v) for v in col],
                           dtype=np.int64)
        payload[name] = arr
    con.register("_bulk", payload)
    try:
        con.execute(f"INSERT INTO {table} SELECT {select or '*'} FROM _bulk")
    finally:
        con.unregister("_bulk")


def _lognormal_dt(rng, median_s: float, sigma: float, horizon_s: float) -> float:
    """A delay drawn log-normally around `median_s`, clipped to the horizon."""
    v = median_s * math.exp(rng.gauss(0.0, sigma))
    return min(max(v, 1.0), horizon_s - 60.0)


# --------------------------------------------------------------------------
# archetype recipes
# --------------------------------------------------------------------------
#
# Each recipe returns a list of (dt_seconds, channel, in_network) events.
# channel: 0 like, 1 repost, 2 reply, 3 quote.
#
# `mix` weights are what the classifier's mix axis keys off; `timing` shapes
# the curve. Keeping them independent per recipe is the point — it is how we
# check the two axes don't leak into each other.

def _draw_channels(rng, n, mix):
    chans, weights = zip(*mix)
    return rng.choices(chans, weights=weights, k=n)


def _events_standard(rng, n, horizon_s):
    """Fast log-asymptotic decay, in-network, like-dominant but not pure."""
    mix = [(0, 0.80), (1, 0.10), (2, 0.08), (3, 0.02)]
    out = []
    for ch in _draw_channels(rng, n, mix):
        dt = _lognormal_dt(rng, 3600, 1.0, horizon_s)
        out.append((dt, ch, rng.random() < 0.85))
    return out


def _events_like_forward(rng, n, horizon_s):
    """Same timing as standard, but essentially nothing propagates."""
    mix = [(0, 0.96), (1, 0.02), (2, 0.015), (3, 0.005)]
    out = []
    for ch in _draw_channels(rng, n, mix):
        dt = _lognormal_dt(rng, 3600, 1.0, horizon_s)
        out.append((dt, ch, rng.random() < 0.85))
    return out


def _events_broadcast(rng, n, horizon_s):
    """Repost-heavy, and the audience turns over: followers early, strangers late."""
    mix = [(0, 0.60), (1, 0.32), (2, 0.06), (3, 0.02)]
    out = []
    for ch in _draw_channels(rng, n, mix):
        dt = _lognormal_dt(rng, 7200, 1.6, horizon_s)
        p_in = 0.90 if dt <= 3600 else (0.25 if dt > 21600 else 0.6)
        out.append((dt, ch, rng.random() < p_in))
    return out


def _events_pile_on(rng, n, horizon_s):
    """Argument arrives after the likes, from strangers. Replies outscore the post."""
    mix = [(0, 0.55), (1, 0.05), (2, 0.32), (3, 0.08)]
    out = []
    for ch in _draw_channels(rng, n, mix):
        # Likes land fast; replies and quotes lag by hours — the lag is the
        # signature, not the volume.
        dt = (_lognormal_dt(rng, 1800, 0.9, horizon_s) if ch in (0, 1)
              else _lognormal_dt(rng, 14400, 0.9, horizon_s))
        p_in = 0.75 if dt <= 3600 else 0.25
        out.append((dt, ch, rng.random() < p_in))
    return out


def _events_conversation(rng, n, horizon_s):
    """Reply-heavy like a pile-on, but in-network and the replies stay small."""
    # Reply weight sits clear of the 0.28 rule threshold rather than on it:
    # at 260 draws a 0.32 weight crosses below often enough by sampling
    # noise alone to make the test flaky, and a flaky fixture teaches
    # nothing about the classifier.
    mix = [(0, 0.56), (1, 0.05), (2, 0.36), (3, 0.03)]
    out = []
    for ch in _draw_channels(rng, n, mix):
        dt = (_lognormal_dt(rng, 1800, 0.9, horizon_s) if ch in (0, 1)
              else _lognormal_dt(rng, 14400, 0.9, horizon_s))
        out.append((dt, ch, rng.random() < 0.90))
    return out


def _events_sleeper(rng, n, horizon_s):
    """Two separated waves: a normal first hour, then a relaunch a day and a half in."""
    mix = [(0, 0.80), (1, 0.10), (2, 0.08), (3, 0.02)]
    out = []
    for ch in _draw_channels(rng, n, mix):
        if rng.random() < 0.45:
            dt = _lognormal_dt(rng, 3600, 0.8, horizon_s)
            p_in = 0.85
        else:
            dt = _lognormal_dt(rng, 129600, 0.35, horizon_s)  # ~36h
            p_in = 0.35
        out.append((dt, ch, rng.random() < p_in))
    return out


def _events_necro(rng, n, horizon_s):
    """Silence, then a tight burst on day five. Late mass AND concentrated."""
    mix = [(0, 0.80), (1, 0.10), (2, 0.08), (3, 0.02)]
    out = []
    for ch in _draw_channels(rng, n, mix):
        if rng.random() < 0.06:
            dt = _lognormal_dt(rng, 3600, 0.8, horizon_s)
            p_in = 0.85
        else:
            # ~108h and genuinely tight: sigma 0.02 keeps the burst inside a
            # ~4h span so it is a burst by the feature's own definition.
            dt = _lognormal_dt(rng, 388800, 0.02, horizon_s)
            p_in = 0.30
        out.append((dt, ch, rng.random() < p_in))
    return out


def _events_evergreen(rng, n, horizon_s):
    """Uniform accumulation over four days — late mass, but never bursty."""
    mix = [(0, 0.75), (1, 0.12), (2, 0.10), (3, 0.03)]
    out = []
    span = min(96 * 3600, horizon_s - 60)
    for ch in _draw_channels(rng, n, mix):
        dt = rng.uniform(60, span)
        out.append((dt, ch, rng.random() < 0.7))
    return out


def _events_unclassified(rng, n, horizon_s):
    """Deliberately between the rules: too slow for standard, too fast for
    evergreen, and not extreme on any mix axis. Exists to prove the
    `unclassified` bucket is reachable rather than a dead branch."""
    mix = [(0, 0.85), (1, 0.10), (2, 0.04), (3, 0.01)]
    out = []
    for ch in _draw_channels(rng, n, mix):
        dt = _lognormal_dt(rng, 43200, 1.3, horizon_s)  # median 12h
        out.append((dt, ch, rng.random() < 0.8))
    return out


RECIPES = {
    "standard":     _events_standard,
    "like_forward": _events_like_forward,
    "broadcast":    _events_broadcast,
    "pile_on":      _events_pile_on,
    "conversation": _events_conversation,
    "sleeper":      _events_sleeper,
    "necro":        _events_necro,
    "evergreen":    _events_evergreen,
    "unclassified": _events_unclassified,
}

# Likes given to each *reply* post. Only the pile-on recipe makes its replies
# outscore the post they answer — that ratio is the discriminator against
# `conversation`, so it lives here rather than in the event recipe.
REPLY_LIKES = {"pile_on": 9}
DEFAULT_REPLY_LIKES = 1


def make_lifeline_snapshot(
    path: str | Path,
    *,
    seed: int = 0,
    per_archetype: int = 14,
    events_per_post: int = 260,
) -> tuple[str, dict[int, str]]:
    """Build the fixture. Returns (path, {uri_id: intended_archetype})."""
    path = str(path)
    if os.path.exists(path):
        os.remove(path)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    rng = random.Random(seed)
    horizon_s = HORIZON_HOURS * 3600.0
    cohort_hi = CUT - timedelta(hours=HORIZON_HOURS)
    cohort_lo = cohort_hi - timedelta(days=COHORT_DAYS)
    # Keep posts a day inside each edge so bound rounding can't drop them.
    lo = cohort_lo + timedelta(days=1)
    span = (cohort_hi - timedelta(days=1) - lo).total_seconds()

    actors: dict[int, str] = {}

    def actor(did_id: int) -> int:
        actors.setdefault(did_id, f"did:plc:synth{did_id:08d}")
        return did_id

    posts = []       # (uri_id, author, rkey, created_at, root, parent, quote, source)
    likes = []       # (actor, subject, created_at)
    reposts = []
    follows = set()  # (src, dst, created_at)
    post_aggs = {}   # uri_id -> [likes, reposts, replies, quotes]
    truth: dict[int, str] = {}

    next_uri = 1
    # Follow edges are created well before the cohort so every in-network
    # engager already followed the author at engagement time.
    follow_ts = cohort_lo - timedelta(days=30)

    for a in range(N_AUTHORS):
        actor(a)
    for did in range(FOLLOWER_LO, FOLLOWER_HI):
        actor(did)

    # Every follower-pool actor follows every author. Real graphs aren't like
    # this, but the analysis only ever asks "was there an edge at time t",
    # and a dense pool makes the in-network share exactly controllable.
    for a in range(N_AUTHORS):
        for did in range(FOLLOWER_LO, FOLLOWER_HI):
            follows.add((did, a, follow_ts))

    for arch, recipe in RECIPES.items():
        for k in range(per_archetype):
            uri_id = next_uri
            next_uri += 1
            author = (uri_id + k) % N_AUTHORS
            created = lo + timedelta(seconds=rng.uniform(0, span))
            posts.append((uri_id, author, _rkey(rng), created,
                          None, None, None, "record"))
            truth[uri_id] = arch
            agg = [0, 0, 0, 0]

            for dt, ch, in_net in recipe(rng, events_per_post, horizon_s):
                ts = created + timedelta(seconds=dt)
                if in_net:
                    eng = rng.randrange(FOLLOWER_LO, FOLLOWER_HI)
                else:
                    eng = actor(rng.randrange(STRANGER_LO, STRANGER_HI))
                if eng == author:
                    continue
                if ch == 0:
                    likes.append((eng, uri_id, ts))
                    agg[0] += 1
                elif ch == 1:
                    reposts.append((eng, uri_id, ts))
                    agg[1] += 1
                else:
                    # Replies and quotes are themselves posts, with their own
                    # row and their own engagement counts.
                    child = next_uri
                    next_uri += 1
                    parent = uri_id if ch == 2 else None
                    quote = uri_id if ch == 3 else None
                    posts.append((child, eng, _rkey(rng), ts,
                                  uri_id if ch == 2 else None,
                                  parent, quote, "record"))
                    child_likes = (REPLY_LIKES.get(arch, DEFAULT_REPLY_LIKES)
                                   if ch == 2 else DEFAULT_REPLY_LIKES)
                    post_aggs[child] = [child_likes, 0, 0, 0]
                    agg[2 if ch == 2 else 3] += 1
            post_aggs[uri_id] = agg

    # Filler posts pinned at the cut so `_bounds` resolves the window from
    # real data, and outside the cohort so they can't pollute the classes.
    for j in range(20):
        uri_id = next_uri
        next_uri += 1
        posts.append((uri_id, j % N_AUTHORS, _rkey(rng),
                      CUT - timedelta(minutes=j), None, None, None, "record"))
        post_aggs[uri_id] = [1, 0, 0, 0]

    con = duckdb.connect(path)
    con.execute("BEGIN")
    con.execute("CREATE TABLE actors(did_id BIGINT, did VARCHAR, active BOOLEAN)")
    _insert(con, "actors", ["did_id", "did"],
            [list(actors.keys()), list(actors.values())],
            select="did_id, did, TRUE")

    con.execute("""CREATE TABLE posts(
        uri_id BIGINT, author_did_id BIGINT, rkey VARCHAR, created_at TIMESTAMP,
        reply_root_uri_id BIGINT, reply_parent_uri_id BIGINT,
        quote_uri_id BIGINT, source VARCHAR)""")
    cols = list(zip(*posts))
    _insert(con, "posts",
            ["uri_id", "author_did_id", "rkey", "created_at",
             "reply_root_uri_id", "reply_parent_uri_id", "quote_uri_id", "source"],
            list(cols),
            # -1 is the NULL sentinel for the three optional reference
            # columns: keeping them as plain int64 arrays is both faster and
            # avoids DuckDB's object-array inference, which fails outright on
            # a column that happens to be entirely NULL.
            select=("uri_id, author_did_id, rkey, created_at, "
                    "NULLIF(reply_root_uri_id, -1), "
                    "NULLIF(reply_parent_uri_id, -1), "
                    "NULLIF(quote_uri_id, -1), source"))

    con.execute("""CREATE TABLE post_aggs(
        uri_id BIGINT, likes BIGINT, reposts BIGINT,
        replies BIGINT, quotes BIGINT)""")
    agg_cols = list(zip(*[(u, *v) for u, v in post_aggs.items()]))
    _insert(con, "post_aggs",
            ["uri_id", "likes", "reposts", "replies", "quotes"], agg_cols)

    for name, rows in (("likes", likes), ("reposts", reposts)):
        con.execute(f"""CREATE TABLE {name}(
            actor_did_id BIGINT, subject_uri_id BIGINT, created_at TIMESTAMP)""")
        _insert(con, name, ["actor_did_id", "subject_uri_id", "created_at"],
                list(zip(*rows)))

    con.execute("""CREATE TABLE follows(
        src_did_id BIGINT, dst_did_id BIGINT, created_at TIMESTAMP)""")
    _insert(con, "follows", ["src_did_id", "dst_did_id", "created_at"],
            list(zip(*follows)))

    con.execute("""CREATE TABLE blocks(
        src_did_id BIGINT, dst_did_id BIGINT, created_at TIMESTAMP)""")
    con.execute("COMMIT")
    con.close()
    return path, truth
