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
# A small, fixed pool that works a list: the same accounts turn up on every
# fleet-boosted post, which is the correlation the authenticity axis exists
# to find. Deliberately small relative to the stranger pool.
FLEET_LO, FLEET_HI = 50_000, 50_120

# uri_ids start above int64's range on purpose. Real ones are the xxhash3-64
# of the post URI, so about half of them exceed 2**63 — and a fixture using
# small sequential ids hides every place the analysis assumes a signed 64-bit
# integer. One did: `np.array(..., dtype=np.int64)` on the id column passed
# every test here and then raised OverflowError on the first real snapshot.
URI_ID_BASE = 0xF000_0000_0000_0000

# TID alphabet, so generated rkeys look like real ones (the analysis only
# uses rkey to build example URLs, but keeping it realistic costs nothing).
_TID_ALPHABET = "234567abcdefghijklmnopqrstuvwxyz"


def _rkey(rng: random.Random) -> str:
    return "3" + "".join(rng.choice(_TID_ALPHABET) for _ in range(12))


def _insert(con, table: str, names: list[str], columns: list, *,
            select: str | None = None, ts_columns: tuple[str, ...] = (),
            u64_columns: tuple[str, ...] = ()) -> None:
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
        # `ts_columns` is needed for the entirely-NULL case: with no non-None
        # value to inspect, an all-empty timestamp column would otherwise be
        # inferred as integers and fail the cast on insert. That happens for
        # real — `tombstoned_at` is all-NULL whenever the fixture is built
        # without the fleet accounts.
        if name in ts_columns or isinstance(first, datetime):
            arr = np.array(
                [np.datetime64("NaT") if v is None else np.datetime64(v, "us")
                 for v in col], dtype="datetime64[us]")
        elif name in u64_columns:
            # uri_ids are xxhash3-64 values, so they routinely exceed int64
            # and must round-trip as unsigned. 0 is the NULL sentinel here
            # rather than -1 (which is not representable); callers unwrap it
            # with NULLIF, and no real uri_id is 0.
            arr = np.array([0 if v is None else int(v) for v in col],
                           dtype=np.uint64)
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


def _events_fleet(rng, n, horizon_s):
    """Bought amplification: a small recurring crew, on a metronome.

    Every property here is one the composite is supposed to catch, and each
    is independently defensible: the engagers come from a fixed pool (so
    they co-occur across posts), they arrive at near-constant intervals (a
    worker loop, not human attention), and the earliest reactions are
    tightly clustered in latency rather than spread over minutes.
    """
    mix = [(0, 0.72), (1, 0.24), (2, 0.03), (3, 0.01)]
    out = []
    # Near-constant spacing over ~5h with only slight jitter — the arrival
    # process a scheduler produces, and the thing human attention never does.
    step = (5 * 3600) / max(n, 1)
    for i, ch in enumerate(_draw_channels(rng, n, mix)):
        dt = max(30.0, (i + 1) * step + rng.gauss(0, step * 0.12))
        # Rounded to whole seconds: a worker loop firing on second
        # boundaries leaves the sub-second part of the TID clustered instead
        # of uniform, which is what `subsecond_phase` looks for. Without
        # this the fixture never exercises that signal at all.
        out.append((float(int(min(dt, horizon_s - 60))), ch, False, "fleet"))
    return out


def _events_discover(rng, n, horizon_s):
    """The negative control: a genuine algorithmic blowup, not a fleet.

    This is the confound the whole authenticity axis has to survive. It
    looks superficially like the fleet — a large wave of engagement from
    accounts with no prior relationship to the author — but the accounts are
    mutually *uncorrelated*: drawn from a huge pool, with spread-out
    creation dates and human-shaped, heavy-tailed reaction latencies. It
    must score LOW, or the composite is measuring reach rather than
    coordination.
    """
    mix = [(0, 0.78), (1, 0.14), (2, 0.06), (3, 0.02)]
    out = []
    for ch in _draw_channels(rng, n, mix):
        dt = _lognormal_dt(rng, 7200, 1.7, horizon_s)
        in_net = dt <= 3600 and rng.random() < 0.7
        out.append((dt, ch, in_net, "auto" if in_net else "wide"))
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

# Authenticity-axis recipes. Kept apart from RECIPES because these are not
# archetypes — a fleet-boosted post still *has* an archetype, and the point
# of the axis is that the score separates these two while the archetype
# label may not.
AUTH_RECIPES = {
    "fleet":    _events_fleet,
    "discover": _events_discover,
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
    include_auth_posts: bool = False,
) -> tuple[str, dict[int, str], dict[int, str]]:
    """Build the fixture.

    Returns (path, {uri_id: archetype}, {uri_id: authenticity_class}).

    `include_auth_posts` adds the fleet / discover posts. Off by default so
    the archetype-recovery test can assert exact per-archetype counts
    without those extra posts landing in the same buckets.
    """
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
    auth_truth: dict[int, str] = {}
    created_at: dict[int, datetime] = {}
    tombstoned: dict[int, datetime] = {}
    followers_of: dict[int, int] = {}

    next_uri = URI_ID_BASE + 1

    for a in range(N_AUTHORS):
        actor(a)
        created_at[a] = cohort_lo - timedelta(days=730)
        followers_of[a] = 3_000
    for did in range(FOLLOWER_LO, FOLLOWER_HI):
        actor(did)
        # Organic accounts are spread across three years of platform history
        # and have audiences of their own.
        created_at[did] = cohort_lo - timedelta(days=rng.uniform(30, 1095))
        followers_of[did] = int(max(5, rng.lognormvariate(4.0, 1.2)))

    # Every follower-pool actor follows every author. Real graphs aren't like
    # this, but the analysis only ever asks "was there an edge at time t",
    # and a dense pool makes the in-network share exactly controllable. The
    # edge *timestamps* are spread over two years, which matters: if every
    # organic follow shared one timestamp the follow-synchrony signal would
    # fire on all of them and measure nothing.
    for a in range(N_AUTHORS):
        for did in range(FOLLOWER_LO, FOLLOWER_HI):
            follows.add((did, a,
                         cohort_lo - timedelta(days=rng.uniform(30, 730))))

    if include_auth_posts:
        # The fleet: provisioned in a three-day batch, no audience, a quarter
        # of them since taken down, and all following the same handful of
        # authors inside one hour.
        batch = cohort_lo - timedelta(days=40)
        sync = cohort_lo - timedelta(days=35)
        for did in range(FLEET_LO, FLEET_HI):
            actor(did)
            created_at[did] = batch + timedelta(hours=rng.uniform(0, 72))
            followers_of[did] = rng.randint(0, 3)
            if rng.random() < 0.25:
                tombstoned[did] = cohort_hi + timedelta(days=1)
            for a in range(4):
                follows.add((did, a, sync + timedelta(minutes=rng.uniform(0, 60))))

    def stranger() -> int:
        did = actor(rng.randrange(STRANGER_LO, STRANGER_HI))
        if did not in created_at:
            created_at[did] = cohort_lo - timedelta(days=rng.uniform(30, 1095))
            followers_of[did] = int(max(5, rng.lognormvariate(3.6, 1.2)))
        return did

    def emit_post(label: str, recipe, into: dict, fleet_author: bool = False):
        """Generate one post plus its whole engagement stream."""
        nonlocal next_uri
        uri_id = next_uri
        next_uri += 1
        # Fleet posts are confined to the handful of authors the fleet was
        # wired to follow, so the bought-follower graph stays coherent.
        author = rng.randrange(4) if fleet_author else (uri_id - URI_ID_BASE) % N_AUTHORS
        created = lo + timedelta(seconds=rng.uniform(0, span))
        posts.append((uri_id, author, _rkey(rng), created,
                      None, None, None, "record"))
        into[uri_id] = label
        agg = [0, 0, 0, 0]

        for ev in recipe(rng, events_per_post, horizon_s):
            dt, ch, in_net = ev[0], ev[1], ev[2]
            pool = ev[3] if len(ev) > 3 else "auto"
            ts = created + timedelta(seconds=dt)
            if pool == "fleet":
                eng = rng.randrange(FLEET_LO, FLEET_HI)
            elif in_net:
                eng = rng.randrange(FOLLOWER_LO, FOLLOWER_HI)
            else:
                eng = stranger()
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
                child_likes = (REPLY_LIKES.get(label, DEFAULT_REPLY_LIKES)
                               if ch == 2 else DEFAULT_REPLY_LIKES)
                post_aggs[child] = [child_likes, 0, 0, 0]
                agg[2 if ch == 2 else 3] += 1
        post_aggs[uri_id] = agg

    for arch, recipe in RECIPES.items():
        for _k in range(per_archetype):
            emit_post(arch, recipe, truth)

    if include_auth_posts:
        for cls, recipe in AUTH_RECIPES.items():
            for _k in range(per_archetype):
                emit_post(cls, recipe, auth_truth, fleet_author=(cls == "fleet"))

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
    # Mirrors a snapshot built *with* the `plc` ETL phase: `created_at` and
    # `tombstoned_at` come from the PLC directory export and are what the
    # age-clustering and tombstone-rate signals read.
    con.execute("""CREATE TABLE actors(
        did_id UBIGINT, did VARCHAR, active BOOLEAN,
        created_at TIMESTAMP, tombstoned_at TIMESTAMP, in_microcosm BOOLEAN)""")
    ids = list(actors.keys())
    _insert(con, "actors",
            ["did_id", "did", "created_at", "tombstoned_at"],
            [ids, [actors[i] for i in ids],
             [created_at.get(i) for i in ids],
             [tombstoned.get(i) for i in ids]],
            select=("did_id, did, tombstoned_at IS NULL, "
                    "created_at, tombstoned_at, TRUE"),
            ts_columns=("created_at", "tombstoned_at"))

    con.execute("""CREATE TABLE actor_aggs(
        did_id UBIGINT, follows BIGINT, followers BIGINT, blocks_out BIGINT,
        blocks_in BIGINT, posts BIGINT, likes_out BIGINT, likes_in BIGINT,
        reposts_out BIGINT, reposts_in BIGINT, replies_out BIGINT,
        quotes_out BIGINT, quoted_count BIGINT)""")
    _insert(con, "actor_aggs", ["did_id", "followers"],
            [ids, [followers_of.get(i, 0) for i in ids]],
            select="did_id, 0, followers, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0")

    con.execute("""CREATE TABLE posts(
        uri_id UBIGINT, author_did_id UBIGINT, rkey VARCHAR, created_at TIMESTAMP,
        reply_root_uri_id UBIGINT, reply_parent_uri_id UBIGINT,
        quote_uri_id UBIGINT, source VARCHAR)""")
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
                    "NULLIF(reply_root_uri_id, 0), "
                    "NULLIF(reply_parent_uri_id, 0), "
                    "NULLIF(quote_uri_id, 0), source"),
            u64_columns=("uri_id", "author_did_id", "reply_root_uri_id",
                         "reply_parent_uri_id", "quote_uri_id"))

    con.execute("""CREATE TABLE post_aggs(
        uri_id UBIGINT, likes BIGINT, reposts BIGINT,
        replies BIGINT, quotes BIGINT)""")
    agg_cols = list(zip(*[(u, *v) for u, v in post_aggs.items()]))
    _insert(con, "post_aggs",
            ["uri_id", "likes", "reposts", "replies", "quotes"], agg_cols,
            u64_columns=("uri_id",))

    for name, rows in (("likes", likes), ("reposts", reposts)):
        con.execute(f"""CREATE TABLE {name}(
            actor_did_id UBIGINT, subject_uri_id UBIGINT, created_at TIMESTAMP)""")
        _insert(con, name, ["actor_did_id", "subject_uri_id", "created_at"],
                list(zip(*rows)),
                u64_columns=("actor_did_id", "subject_uri_id"))

    con.execute("""CREATE TABLE follows(
        src_did_id UBIGINT, dst_did_id UBIGINT, created_at TIMESTAMP)""")
    _insert(con, "follows", ["src_did_id", "dst_did_id", "created_at"],
            list(zip(*follows)))

    con.execute("""CREATE TABLE blocks(
        src_did_id UBIGINT, dst_did_id UBIGINT, created_at TIMESTAMP)""")
    con.execute("COMMIT")
    con.close()
    return path, truth, auth_truth
