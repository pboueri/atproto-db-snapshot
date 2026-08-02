"""Inauthentic-amplification signals for post engagement lifelines.

This is a *fourth axis* on top of timing / mix / audience, not a tenth
archetype. Nobody buys engagement on a post that got no organic traction —
they buy it to push something already moving — so a post is routinely a
genuine sleeper hit *and* partly amplified. Forcing that into a mutually
exclusive bucket produces a wrong answer for the common case, so what comes
out of here is a score that rides alongside the archetype label.

Three different things look alike on the surface and must be kept apart:

  bought amplification  author-purchased fleet: one author targeted
                        repeatedly, engagers correlated with each other,
                        no downstream propagation
  coordinated brigading real people acting in concert: human timing, but
                        synchronized onset; overlaps the pile_on archetype
  algorithmic reach     Discover: not artificial at all

The third is the dangerous one. A sudden wave of engagement from accounts
with no prior relationship to the author is what people reach for as bot
evidence, and it is also exactly what algorithmic distribution looks like.
Score on timing and unfamiliarity alone and you will label every Discover
hit a bot farm.

The discriminator is *correlation*, not novelty. Accounts delivered by an
algorithm are mutually uncorrelated — sampled from a huge pool, so normal
age spread, no co-engagement overlap, high variance in reaction latency. A
fleet is correlated on every one of those. So the weights below put most of
the mass on correlation-structure signals and treat timing regularity as
a cheap prefilter rather than as evidence. `score_by_archetype` in the
report is the standing check on this: if `broadcast` — the out-of-network
archetype — carries the highest mean score, the composite has drifted into
measuring Discover and the weights are wrong.

Structure. Every signal is a `Signal` in `SIGNALS`: a name, what it means,
which snapshot columns it needs, its cost, its weight, and a `compute` that
writes one row per post into a temp table. Adding a signal means appending
one entry; nothing else in the pipeline changes. Signals that need columns
the snapshot lacks (`actors.created_at` arrives only with the `plc` ETL
phase) are skipped and reported as skipped rather than failing the run.

Signals are combined by percentile rank within the analyzed cohort, so the
composite is explicitly *relative*: "unusual compared with the other posts
in this cohort", never an absolute probability that a post was boosted.
That is a real limitation and it is stated in the report.

Public entrypoint: `attach(con, ctx) -> (per_post_scores, meta)`.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable


@dataclass
class Signal:
    """One inauthenticity signal.

    `compute(con, ctx)` must create a temp table named `au_<name>` with
    columns `(uri_id, value)` covering any subset of the cohort; posts with
    no row get the neutral value. `direction` is +1 when a higher raw value
    means more suspicious and -1 when it is the other way round.
    """
    name: str
    description: str
    weight: float
    direction: int
    compute: Callable
    requires: tuple[str, ...] = ()
    cost: str = "cheap"
    default_enabled: bool = True
    family: str = "correlation"
    notes: str = ""


def _has(con, table: str, column: str) -> bool:
    try:
        rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    except Exception:
        return False
    return any(r[1] == column for r in rows)


# --------------------------------------------------------------------------
# correlation-structure signals — the load-bearing ones
# --------------------------------------------------------------------------

def _co_engagement(con, ctx):
    """Largest engager-set overlap with any other post in the cohort.

    Two random likers of an organic post have essentially nothing else in
    common. A fleet works a list, so the same crew turns up together across
    every post it touches, and the overlap between two fleet-boosted posts
    is enormous. This is the strongest signal available and the one that is
    genuinely expensive to fake — an adversary would have to buy a fresh
    account set per post.

    Cost is controlled by only considering engagers who appear on at least
    two cohort posts (which drops the overwhelming majority — most accounts
    engage with one post in any window) and by capping engager degree, since
    a handful of hyper-active accounts would otherwise dominate the
    self-join with pairs that mean nothing.
    """
    max_deg = ctx.get("coeng_max_degree", 200)
    min_shared = ctx.get("coeng_min_shared", 3)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE au_ap AS
        WITH ap AS (SELECT DISTINCT actor, uri_id FROM lf_ev),
             deg AS (SELECT actor, COUNT(*) AS d FROM ap GROUP BY 1)
        SELECT ap.actor, ap.uri_id
        FROM ap JOIN deg USING (actor)
        WHERE deg.d BETWEEN 2 AND {int(max_deg)}
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE au_overlap AS
        SELECT a.uri_id AS p, b.uri_id AS q, COUNT(*) AS shared
        FROM au_ap a JOIN au_ap b
          ON a.actor = b.actor AND a.uri_id < b.uri_id
        GROUP BY 1, 2
        HAVING COUNT(*) >= {int(min_shared)}
    """)
    # Symmetrize, then express the best overlap as a share of the post's own
    # engager count so a big post isn't flagged merely for being big.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE au_co_engagement AS
        WITH sym AS (
          SELECT p AS uri_id, shared FROM au_overlap
          UNION ALL
          SELECT q AS uri_id, shared FROM au_overlap
        ),
        best AS (SELECT uri_id, MAX(shared) AS shared FROM sym GROUP BY 1),
        sz AS (SELECT uri_id, COUNT(DISTINCT actor) AS n FROM lf_ev GROUP BY 1)
        SELECT sz.uri_id,
               COALESCE(best.shared, 0)::DOUBLE / GREATEST(sz.n, 1) AS value
        FROM sz LEFT JOIN best USING (uri_id)
    """)


def _age_clustering(con, ctx):
    """Share of engagers whose accounts were created in one 7-day window.

    Farms are provisioned in batches, so their creation dates bunch. Organic
    audiences are spread across the platform's whole history. Needs
    `actors.created_at`, which the `plc` ETL phase adds — without it the
    signal is skipped rather than guessed at.

    The window slides at daily granularity for the same reason the lifeline
    burst window does: a fixed grid splits a batch that straddles a boundary.
    """
    con.execute("""
        CREATE OR REPLACE TEMP TABLE au_age_clustering AS
        WITH ea AS (
          SELECT DISTINCT e.uri_id, e.actor, a.created_at
          FROM lf_ev e JOIN actors a ON a.did_id = e.actor
          WHERE a.created_at IS NOT NULL
        ),
        daily AS (
          SELECT uri_id, DATE_TRUNC('day', created_at) AS d, COUNT(*) AS n
          FROM ea GROUP BY 1, 2
        ),
        win AS (
          SELECT uri_id,
                 SUM(n) OVER (PARTITION BY uri_id ORDER BY d
                              RANGE BETWEEN INTERVAL 6 DAY PRECEDING
                                        AND CURRENT ROW) AS w,
                 SUM(n) OVER (PARTITION BY uri_id) AS tot
          FROM daily
        )
        SELECT uri_id, MAX(w)::DOUBLE / GREATEST(MAX(tot), 1) AS value
        FROM win GROUP BY 1
    """)


def _tombstone_rate(con, ctx):
    """Share of engagers since deleted or deactivated.

    A retroactive, partial ground truth: accounts the network itself later
    removed. Biased toward *caught* spam and therefore an undercount, but
    unlike everything else here it is not an inference.
    """
    has_tomb = _has(con, "actors", "tombstoned_at")
    dead = ("(a.tombstoned_at IS NOT NULL OR NOT a.active)" if has_tomb
            else "(NOT a.active)")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE au_tombstone_rate AS
        WITH ea AS (SELECT DISTINCT uri_id, actor FROM lf_ev)
        SELECT ea.uri_id,
               AVG(CASE WHEN {dead} THEN 1.0 ELSE 0.0 END) AS value
        FROM ea JOIN actors a ON a.did_id = ea.actor
        GROUP BY 1
    """)


def _follow_synchrony(con, ctx):
    """Share of the post's in-network engagers who followed in one 24h window.

    Real follower bases accumulate continuously. A purchased one arrives in
    a batch, and that batch is visible in `follows.created_at` long after
    the fact. Reuses `lf_follow`, which the lifeline pass already built.

    The minimum-count guard is load-bearing rather than defensive. A "max
    share in one window" statistic is inflated by small samples — a post
    with four in-network engagers can hardly help but have half of them in
    some 24h window — and posts with *few* followers engaging are exactly
    the out-of-network-heavy ones. Without the guard this signal quietly
    scores algorithmic reach as coordination, which is the specific failure
    this whole module is built to avoid.
    """
    min_n = ctx.get("synchrony_min_engagers", 20)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE au_follow_synchrony AS
        WITH fe AS (
          SELECT DISTINCT e.uri_id, f.actor, f.followed_at
          FROM lf_ev e
          JOIN lf_follow f ON f.actor = e.actor AND f.author = e.author
          WHERE f.followed_at IS NOT NULL
        ),
        hourly AS (
          SELECT uri_id, DATE_TRUNC('hour', followed_at) AS h, COUNT(*) AS n
          FROM fe GROUP BY 1, 2
        ),
        win AS (
          SELECT uri_id,
                 SUM(n) OVER (PARTITION BY uri_id ORDER BY h
                              RANGE BETWEEN INTERVAL 23 HOUR PRECEDING
                                        AND CURRENT ROW) AS w,
                 SUM(n) OVER (PARTITION BY uri_id) AS tot
          FROM hourly
        )
        SELECT uri_id, MAX(w)::DOUBLE / GREATEST(MAX(tot), 1) AS value
        FROM win GROUP BY 1
        HAVING MAX(tot) >= {int(min_n)}
    """)


def _engager_reach(con, ctx):
    """Share of engagers with essentially no audience of their own.

    Fleet accounts are cheap to make and nobody follows them. Counted from
    `actor_aggs.followers`, so it costs one join. On its own it is weak —
    plenty of real people lurk with two followers — which is why it carries
    a small weight and only matters in combination.
    """
    thresh = ctx.get("low_reach_followers", 5)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE au_engager_reach AS
        WITH ea AS (SELECT DISTINCT uri_id, actor FROM lf_ev)
        SELECT ea.uri_id,
               AVG(CASE WHEN COALESCE(ag.followers, 0) <= {int(thresh)}
                        THEN 1.0 ELSE 0.0 END) AS value
        FROM ea LEFT JOIN actor_aggs ag ON ag.did_id = ea.actor
        GROUP BY 1
    """)


# --------------------------------------------------------------------------
# timing signals — cheap prefilters, deliberately low weight
# --------------------------------------------------------------------------

def _interarrival_regularity(con, ctx):
    """How *unnaturally even* the spacing between arrivals is.

    Human attention is bursty and overdispersed: gaps between arrivals are
    heavy-tailed, so the coefficient of variation of inter-arrival times
    runs well above 1. A scheduled worker pool produces gaps that are
    near-constant (CV toward 0) or exactly Poisson (CV near 1) — and
    Poisson is itself anomalous here, because real attention is not
    memoryless. So the tell is *insufficient* burstiness, which is a
    two-sided test most bot-hunting gets wrong by only looking for spikes.

    Mapped to 1/(1+CV) so that higher means more regular means more
    suspicious, matching every other signal's direction.
    """
    con.execute("""
        CREATE OR REPLACE TEMP TABLE au_interarrival_regularity AS
        WITH gaps AS (
          SELECT uri_id,
                 dt - LAG(dt) OVER (PARTITION BY uri_id ORDER BY dt) AS g
          FROM lf_ev
        ),
        stat AS (
          SELECT uri_id, AVG(g) AS mu, STDDEV_SAMP(g) AS sd, COUNT(*) AS n
          FROM gaps WHERE g IS NOT NULL GROUP BY 1
        )
        SELECT uri_id, 1.0 / (1.0 + (sd / NULLIF(mu, 0))) AS value
        FROM stat
        WHERE n >= 20 AND mu > 0 AND sd IS NOT NULL
    """)


def _subsecond_phase(con, ctx):
    """Non-uniformity of the sub-second part of engagement timestamps.

    TIDs carry microsecond resolution, and for records written by humans
    through ordinary clients the sub-second phase is uniform noise. A client
    that batches writes, or a worker loop firing on a timer, leaves that
    phase clustered. Measured as the largest share landing in any one of ten
    sub-second deciles: 0.1 is perfectly uniform, 1.0 is a single phase.

    Forensic rather than decisive — it catches lazy automation and nothing
    else, so it carries the smallest weight in the composite.
    """
    con.execute("""
        CREATE OR REPLACE TEMP TABLE au_subsecond_phase AS
        WITH ph AS (
          SELECT uri_id,
                 (EXTRACT(microsecond FROM ts) % 1000000) / 100000 AS decile
          FROM lf_ev
        ),
        c AS (SELECT uri_id, decile, COUNT(*) AS n FROM ph GROUP BY 1, 2)
        SELECT uri_id, MAX(n)::DOUBLE / GREATEST(SUM(n), 1) AS value
        FROM c GROUP BY 1
    """)


def _onset_immediacy(con, ctx):
    """Share of the earliest engagements landing within seconds of the post.

    Firehose-triggered automation reacts in under a minute; people are
    asleep, in a meeting, or not scrolling yet.

    This was first written as the coefficient of variation of the first-K
    latencies, on the reasoning that bots react with low *variance*. That
    version inverted: the first K arrivals of any heavy-tailed organic
    process are its left tail and therefore tightly packed, so ordinary
    posts scored as more bot-like than a fleet did. Measuring the share
    inside an absolute window has no such dependence on the overall arrival
    shape.

    Targets one specific automation style — a fleet that drips engagement
    on a schedule instead of racing the firehose will not trip it, and is
    caught by `interarrival_regularity` instead. Hence the low weight: it
    is corroborating evidence, never a verdict on its own.
    """
    n_first = ctx.get("onset_n", 50)
    window_s = ctx.get("onset_window_s", 120)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE au_onset_immediacy AS
        WITH ranked AS (
          SELECT uri_id, dt,
                 ROW_NUMBER() OVER (PARTITION BY uri_id ORDER BY dt) AS rn
          FROM lf_ev
        ),
        first_k AS (SELECT uri_id, dt FROM ranked WHERE rn <= {int(n_first)})
        SELECT uri_id,
               AVG(CASE WHEN dt <= {int(window_s)} THEN 1.0 ELSE 0.0 END) AS value
        FROM first_k GROUP BY 1 HAVING COUNT(*) >= 20
    """)


# --------------------------------------------------------------------------
# heavy signals — off by default
# --------------------------------------------------------------------------

def _repost_yield(con, ctx):
    """Whether reposts actually propagated to the reposter's followers.

    Structural rather than statistical, and the hardest thing here to fake:
    a repost by an account with real reach should be followed by engagement
    *from that account's followers*. Fleets repost into the void, so their
    yield is zero no matter how many reposts they buy. Faking it requires
    buying a second, downstream layer of engagement from accounts that
    actually follow the first layer.

    Inverted (1 - yield) so higher stays "more suspicious".

    Heavy: needs a (later engager, reposter) pair join against the whole
    `follows` table, so it is capped to the `top_reposters` largest
    reposters per post and off unless asked for.
    """
    top_n = ctx.get("top_reposters", 5)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE au_rp AS
        WITH rp AS (
          SELECT e.uri_id, e.actor AS reposter, e.ts,
                 COALESCE(ag.followers, 0) AS followers,
                 ROW_NUMBER() OVER (PARTITION BY e.uri_id
                                    ORDER BY COALESCE(ag.followers, 0) DESC) AS rn
          FROM lf_ev e
          LEFT JOIN actor_aggs ag ON ag.did_id = e.actor
          WHERE e.ch = 1
        )
        SELECT uri_id, reposter, ts, followers FROM rp
        WHERE rn <= {int(top_n)} AND followers > 0
    """)
    # Candidate downstream engagers: anyone who engaged the same post after
    # the repost landed and was not already following the author.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE au_ry_pairs AS
        SELECT DISTINCT e.actor, r.reposter
        FROM au_rp r
        JOIN lf_ev e ON e.uri_id = r.uri_id AND e.ts > r.ts
        LEFT JOIN lf_follow fe ON fe.actor = e.actor AND fe.author = e.author
        WHERE fe.actor IS NULL
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE au_ry_edges AS
        SELECT p.actor, p.reposter
        FROM au_ry_pairs p
        JOIN follows f ON f.src_did_id = p.actor AND f.dst_did_id = p.reposter
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE au_repost_yield AS
        WITH downstream AS (
          SELECT r.uri_id, COUNT(DISTINCT e.actor) AS n_down
          FROM au_rp r
          JOIN lf_ev e ON e.uri_id = r.uri_id AND e.ts > r.ts
          JOIN au_ry_edges g ON g.actor = e.actor AND g.reposter = r.reposter
          GROUP BY 1
        ),
        reach AS (SELECT uri_id, SUM(followers) AS reach FROM au_rp GROUP BY 1)
        SELECT reach.uri_id,
               1.0 - LEAST(1.0,
                 COALESCE(downstream.n_down, 0)::DOUBLE
                 / GREATEST(reach.reach, 1) * 100.0) AS value
        FROM reach LEFT JOIN downstream USING (uri_id)
    """)


# --------------------------------------------------------------------------
# registry
# --------------------------------------------------------------------------
#
# Weights are the knob to turn when iterating. They deliberately concentrate
# on correlation structure: the four correlation signals carry 0.72 of the
# total against 0.18 for timing, because timing is cheap to jitter away and,
# more importantly, because timing is what makes Discover look like a bot
# farm. Re-weight here, not in the pipeline.

SIGNALS: list[Signal] = [
    Signal(
        name="co_engagement",
        description="Largest engager-set overlap with another cohort post",
        weight=0.34, direction=+1, compute=_co_engagement,
        cost="heavy", family="correlation",
        notes="Strongest single signal; expensive to fake.",
    ),
    Signal(
        name="age_clustering",
        description="Share of engagers created inside one 7-day window",
        weight=0.16, direction=+1, compute=_age_clustering,
        requires=("actors.created_at",), family="correlation",
        notes="Needs the plc ETL phase; skipped otherwise.",
    ),
    Signal(
        name="follow_synchrony",
        description="Share of in-network engagers who followed in one 24h window",
        weight=0.12, direction=+1, compute=_follow_synchrony,
        family="correlation",
    ),
    Signal(
        name="tombstone_rate",
        description="Share of engagers since deleted or deactivated",
        weight=0.10, direction=+1, compute=_tombstone_rate,
        family="correlation",
        notes="Partial retroactive ground truth; undercounts.",
    ),
    Signal(
        name="engager_reach",
        description="Share of engagers with almost no followers of their own",
        weight=0.10, direction=+1, compute=_engager_reach,
        requires=("actor_aggs.followers",), family="account",
    ),
    Signal(
        name="interarrival_regularity",
        description="Arrival spacing more even than human attention produces",
        weight=0.08, direction=+1, compute=_interarrival_regularity,
        family="timing",
    ),
    Signal(
        name="onset_immediacy",
        description="Earliest engagements landing within seconds of the post",
        weight=0.00, direction=+1, compute=_onset_immediacy,
        family="timing", default_enabled=False,
        notes="OFF: measured against a Discover-like control it scores the "
              "control *higher* than a fleet, because it is near-binary and "
              "fires on any post with unusually fast early engagement — "
              "which is what real reach looks like. Turn it on with weight "
              "when there is a firehose-bot example to calibrate against.",
    ),
    Signal(
        name="subsecond_phase",
        description="Sub-second timestamp phase clustered rather than uniform",
        weight=0.03, direction=+1, compute=_subsecond_phase,
        family="timing",
    ),
    Signal(
        name="repost_yield",
        description="Reposts that produced no engagement from the reposter's "
                    "own followers",
        weight=0.00, direction=+1, compute=_repost_yield,
        requires=("actor_aggs.followers",),
        cost="very heavy", default_enabled=False, family="correlation",
        notes="Give it weight when enabling it — structurally the best "
              "signal here, but it needs a second pair join against follows.",
    ),
]

SIGNALS_BY_NAME = {s.name: s for s in SIGNALS}


def _requirements_met(con, sig: Signal) -> bool:
    for req in sig.requires:
        table, _, column = req.partition(".")
        if column and not _has(con, table, column):
            return False
    return True


def attach(con, ctx: dict | None = None, *, enabled: list[str] | None = None,
           weights: dict[str, float] | None = None, log: bool = True):
    """Compute every enabled signal and combine into a per-post score.

    Returns `(scores, meta)` where `scores` maps uri_id to a dict of the
    composite plus each signal's percentile rank, and `meta` records which
    signals ran, which were skipped and why, and the weights used.

    Assumes the lifeline temp tables (`lf_ev`, `lf_cohort`, `lf_follow`)
    already exist — this runs as a second pass over work the lifeline
    extraction has done rather than re-deriving the event stream.
    """
    ctx = dict(ctx or {})
    w_override = dict(weights or {})

    def say(msg: str) -> None:
        if log:
            print(f"=== authenticity: {msg} ===", flush=True)

    chosen = []
    skipped = {}
    for sig in SIGNALS:
        if enabled is not None:
            if sig.name not in enabled:
                continue
        elif not sig.default_enabled:
            # Report the signal's own reason rather than a blanket "cost" —
            # some are off because they are expensive and some because they
            # are confounded, and those call for very different follow-ups.
            skipped[sig.name] = (
                f"off by default — {sig.notes}" if sig.notes
                else f"off by default (cost: {sig.cost})")
            continue
        if not _requirements_met(con, sig):
            skipped[sig.name] = f"missing {', '.join(sig.requires)}"
            continue
        chosen.append(sig)

    for name, why in skipped.items():
        say(f"skip {name}: {why}")

    ran, failed = [], {}
    for sig in chosen:
        t0 = time.time()
        try:
            sig.compute(con, ctx)
            ran.append(sig)
            say(f"{sig.name} ({time.time() - t0:.1f}s)")
        except Exception as exc:  # pragma: no cover - defensive
            # One broken signal must not lose the other eight; the report
            # says which failed so it is visible rather than silent.
            failed[sig.name] = str(exc)[:200]
            say(f"FAILED {sig.name}: {exc}")

    if not ran:
        return {}, {"signals_run": [], "skipped": skipped, "failed": failed,
                    "weights": {}, "note": "no signals available"}

    # Percentile-rank each signal across the cohort, then weight and sum.
    # Ranking rather than z-scoring because these distributions are heavily
    # skewed and a couple of extreme posts would otherwise swamp the scale.
    total_w = sum(w_override.get(s.name, s.weight) for s in ran)
    if total_w <= 0:
        total_w = 1.0

    selects, joins = [], []
    for sig in ran:
        d = "DESC" if sig.direction < 0 else "ASC"
        selects.append(
            f"COALESCE(PERCENT_RANK() OVER (ORDER BY t_{sig.name}.value {d}), 0.5) "
            f"AS r_{sig.name}"
        )
        joins.append(
            f"LEFT JOIN au_{sig.name} t_{sig.name} USING (uri_id)")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE au_ranked AS
        SELECT c.uri_id, {', '.join(selects)}
        FROM (SELECT uri_id FROM lf_cohort) c
        {' '.join(joins)}
    """)

    weight_map = {s.name: w_override.get(s.name, s.weight) / total_w for s in ran}
    composite = " + ".join(
        f"{weight_map[s.name]} * r_{s.name}" for s in ran)
    rows = con.execute(f"""
        SELECT uri_id, {composite} AS score,
               {', '.join(f'r_{s.name}' for s in ran)}
        FROM au_ranked
    """).fetchall()

    names = [s.name for s in ran]
    scores = {}
    for row in rows:
        scores[int(row[0])] = {
            "score": float(row[1]),
            **{n: float(v) for n, v in zip(names, row[2:])},
        }

    meta = {
        "signals_run": [
            {"name": s.name, "description": s.description,
             "family": s.family, "cost": s.cost,
             "weight": round(weight_map[s.name], 4), "notes": s.notes}
            for s in ran
        ],
        "skipped": skipped,
        "failed": failed,
        "weights": {n: round(weight_map[n], 4) for n in names},
    }
    say(f"composite from {len(ran)} signals over {len(scores):,} posts")
    return scores, meta
