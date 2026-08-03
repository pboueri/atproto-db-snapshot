"""Post engagement lifelines — an archetype taxonomy for how posts accumulate
likes / reposts / replies / quotes over their first hours and days.

The unit of analysis is a *lifeline*: for one post, the arrival process of
every inbound engagement event, measured as a delta from the post's own
creation time. Four things make this tractable on this snapshot:

  1. `created_at` everywhere is decoded from the record's **rkey TID**
     (see `src/stage.rs` -> `tid::decode_tid_micros`), not the client's
     self-reported `record.createdAt`. TIDs are minted at write time and
     are monotonic per-PDS, so they are a genuine microsecond-resolution
     server clock rather than a spoofable field. Minute-level lifeline
     shape is therefore meaningful.
  2. All four channels carry that clock: likes / reposts from their own
     tables, replies and quotes from `posts.reply_parent_uri_id` /
     `posts.quote_uri_id`.
  3. `follows` carries `created_at` too, so for every engagement event we
     can ask *did this account already follow the author when it engaged?*
     That is the *audience* axis — the in-network / out-of-network split
     that distinguishes "my followers saw this" from "an algorithm put
     this in front of strangers".
  4. The snapshot is windowed, so the cohort has to be chosen to give
     every post the same observation horizon (see `_bounds`).

Taxonomy is built on three orthogonal axes rather than one flat clustering,
because the categories people reach for intuitively are not commensurable:
"sleeper hit" is a *timing* statement, "like-forward" is a *mix* statement,
and "went viral on Discover" is an *audience* statement. A post can be all
three at once.

  timing   — shape of the cumulative curve (t50, late mass, wave count)
  mix      — composition across like / repost / reply / quote
  audience — in-network vs out-of-network share, and how that moves in time

Method. Shape clustering is K-Spectral Centroid (Yang & Leskovec, WSDM'11),
which is scale-invariant by construction — a 50-like post and a 50k-like
post with the same shape land together — and whose centroid update is a top
eigenvector, so it is cheap. Deliberately *not* DTW: these series are all
anchored at a known t=0, so there is no alignment to recover, and warping
would actively destroy the signal that defines a sleeper hit (a late second
wave would be stretched onto a standard post's first wave and scored as
similar).

KSC discovers shapes; the published taxonomy is a *decision list of
threshold rules* over interpretable features (`ARCHETYPE_RULES`). Clustering
to discover, rules to deploy: rules are stable across snapshots, reviewable,
and don't need re-litigating every rebuild. The cross-tab of KSC shape
against rule label is reported as a check that the rules track the shapes
the data actually contains, and the `unclassified` bucket is reported
prominently because that bucket is where the next iteration of the taxonomy
comes from.

Public entrypoint: `run(con, snapshot_date, ...) -> (html_bytes, sidecar)`.
"""

from __future__ import annotations

import math
import time

from .common import (
    BRAND, SHARED_CSS,
    built_at_utc, fig_html, fmt_int, install_template, plotlyjs_inline,
)

# Channel codes. Kept as small ints so the event table stays narrow — it is
# the widest intermediate in the pipeline (tens of millions of rows).
CH_LIKE, CH_REPOST, CH_REPLY, CH_QUOTE = 0, 1, 2, 3
CH_NAMES = {CH_LIKE: "like", CH_REPOST: "repost", CH_REPLY: "reply", CH_QUOTE: "quote"}

# Archetype presentation order + palette. `unclassified` is deliberately last
# and deliberately grey — it is a to-do list, not a finding.
ARCHETYPE_ORDER = [
    "standard", "like_forward", "broadcast", "pile_on", "conversation",
    "sleeper", "evergreen", "necro", "unclassified",
]

ARCHETYPE_COLORS = {
    "standard":     "#0085ff",
    "like_forward": "#38bdf8",
    "broadcast":    "#7c3aed",
    "pile_on":      "#ef4444",
    "conversation": "#16a34a",
    "sleeper":      "#f59e0b",
    "evergreen":    "#0d9488",
    "necro":        "#a16207",
    "unclassified": "#94a3b8",
}

ARCHETYPE_BLURB = {
    "standard": "Log-asymptotic accumulation that saturates inside a day. "
                "Mostly in-network, like-dominant. The default life of a post.",
    "like_forward": "Almost pure likes — the silent nod. Reposts, replies and "
                    "quotes are all near zero, so nothing propagates.",
    "broadcast": "Repost-forward with an out-of-network wave. Reach is "
                 "borrowed from amplifiers rather than grown by conversation.",
    "pile_on": "Replies and quotes arrive *after* the like wave and the "
               "replies outperform the post. The quantitative ratio.",
    "conversation": "Reply-heavy but in-network and sustained at a low rate, "
                    "and the replies do not outscore the post. A thread, not a dogpile.",
    "sleeper": "A second, separated wave of engagement after the first has "
               "decayed — the post gets picked up again.",
    "evergreen": "Accumulates roughly linearly instead of saturating. No "
                 "second wave, just no decay.",
    "necro": "Most of the engagement arrives after 72 hours, long past the "
             "point where a normal post is finished.",
    "unclassified": "Did not match any rule. This bucket is the input to the "
                    "next iteration of the taxonomy — inspect it, don't ignore it.",
}

# Thresholds for the rule decision list, gathered here so they are visible
# and tunable in one place rather than buried in the classifier. These are
# starting values calibrated against the shape of the distributions; expect
# to move them after eyeballing the first run's examples.
RULE_THRESHOLDS = {
    "necro_late72":        0.50,   # frac of engagement arriving after 72h
    "necro_burst_share":   0.35,   # late mass must be a *burst*, not a plateau
    "sleeper_reignition":  0.45,   # 2nd peak height / 1st peak height
    "sleeper_late24":      0.20,   # frac after 24h
    "pileon_arg_share":    0.32,   # (reply + quote) share of all engagement
    "pileon_lag_hours":    0.5,    # replies must lag likes by this much
    "pileon_outperform":   1.0,    # likes-on-replies / likes-on-post
    "pileon_oon_delta":    0.15,   # late minus early out-of-network share
    "conv_reply_share":    0.28,
    "conv_in_network":     0.55,
    "broadcast_repost":    0.22,   # repost share of all engagement
    "broadcast_oon_delta": 0.08,
    "likefwd_like_share":  0.90,
    "evergreen_t50_h":     24.0,
    "evergreen_late24":    0.45,
    "evergreen_burst_share": 0.35,  # mirror of necro_burst_share: flat, not peaked
    "standard_t50_h":      8.0,
    "standard_late24":     0.20,
}


# --------------------------------------------------------------------------
# schema probing
# --------------------------------------------------------------------------

def _columns(con, table: str) -> set[str]:
    """Column names of `table`, or an empty set if it doesn't exist.

    The published snapshots vary: the `plc` ETL phase adds `actors.created_at`
    and the synthetic fixtures carry a reduced `posts`. Probing lets one
    module run against all of them instead of hard-failing on a missing
    column, which is the same approach `graph_boosters.py` takes.
    """
    try:
        rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    except Exception:
        return set()
    return {r[1] for r in rows}


# --------------------------------------------------------------------------
# K-Spectral Centroid clustering
# --------------------------------------------------------------------------

def _ksc_distance(Xn, mu):
    """Scale-invariant distance from unit-normed rows `Xn` to unit centroid `mu`.

    KSC defines d(x, mu) = min_alpha ||x - alpha*mu|| / ||x||. Solving for
    alpha gives d = sqrt(1 - cos^2(x, mu)), i.e. the sine of the angle
    between them — so two curves with identical shape but different total
    volume are at distance zero. That scale invariance is the property we
    actually want here; it is *not* time-warping, which we explicitly avoid.
    """
    import numpy as np
    cos = Xn @ mu
    return np.sqrt(np.clip(1.0 - cos * cos, 0.0, None))


def _ksc(X, k: int, *, n_iter: int = 30, seed: int = 0):
    """Cluster non-negative curves `X` (n x d) into `k` spectral centroids.

    Returns (labels, centroids). The centroid update is the top eigenvector
    of sum_i x_i x_i^T / ||x_i||^2 — equivalently the top right singular
    vector of the row-normalized member matrix — obtained by power iteration
    so we never materialize the d x d scatter matrix.
    """
    import numpy as np

    rng = np.random.default_rng(seed)
    n, d = X.shape
    norms = np.linalg.norm(X, axis=1)
    norms[norms == 0] = 1.0
    Xn = X / norms[:, None]

    # k-means++ style seeding under the KSC metric: first centroid random,
    # each subsequent one sampled proportional to squared distance from the
    # nearest chosen centroid. Random init on angular data collapses often.
    centroids = np.empty((k, d))
    centroids[0] = Xn[rng.integers(n)]
    closest = _ksc_distance(Xn, centroids[0]) ** 2
    for j in range(1, k):
        total = closest.sum()
        if total <= 0:
            centroids[j] = Xn[rng.integers(n)]
        else:
            centroids[j] = Xn[rng.choice(n, p=closest / total)]
        closest = np.minimum(closest, _ksc_distance(Xn, centroids[j]) ** 2)

    labels = np.zeros(n, dtype=np.int32)
    for _ in range(n_iter):
        dists = np.stack([_ksc_distance(Xn, centroids[j]) for j in range(k)], axis=1)
        new_labels = dists.argmin(axis=1)
        converged = np.array_equal(new_labels, labels)
        labels = new_labels
        for j in range(k):
            members = Xn[labels == j]
            if len(members) == 0:
                centroids[j] = Xn[rng.integers(n)]
                continue
            # Power iteration for the top right singular vector of `members`.
            v = centroids[j]
            for _ in range(20):
                v = members.T @ (members @ v)
                nv = np.linalg.norm(v)
                if nv == 0:
                    v = Xn[rng.integers(n)]
                    break
                v = v / nv
            # KSC centroids are defined up to sign; pick the non-negative
            # orientation so the centroid reads as a curve, not its mirror.
            if v.sum() < 0:
                v = -v
            centroids[j] = v
        if converged:
            break
    return labels, centroids


# --------------------------------------------------------------------------
# curve-shape features
# --------------------------------------------------------------------------

def _wave_features(counts):
    """(n_waves, reignition) from one post's per-log-bin engagement counts.

    Note the input: *counts per log-spaced bin*, i.e. dN/d(ln t), not the
    per-second arrival rate. This matters and is easy to get wrong. Because
    log bins grow exponentially wide, a genuine second wave two days later
    has a per-second rate orders of magnitude below the first hour's — a
    sleeper measured on true rate looks like a rounding error. In log time,
    where attention actually lives, the same second wave is a comparable
    hump. So wave detection runs on the log density; the true rate is kept
    for the figures, where it is the honest thing to plot.

    A "wave" is a local maximum that is (a) at least 20% of the global peak
    and (b) separated from a taller peak by a trough below 50% of the
    shorter of the two. That separation test is what distinguishes a genuine
    second wave from noise riding on the shoulder of the first one.

    `reignition` is the height of the tallest *separated* later peak as a
    fraction of the first peak, so 0 means "never came back" and values near
    1 mean the post effectively relaunched.
    """
    import numpy as np

    r = np.asarray(counts, dtype=float)
    if r.max() <= 0:
        return 1, 0.0
    # 3-tap smoothing so single-bin jitter doesn't register as a wave.
    if len(r) >= 3:
        sm = np.convolve(r, np.array([0.25, 0.5, 0.25]), mode="same")
    else:
        sm = r
    peak = sm.max()
    if peak <= 0:
        return 1, 0.0

    idx = [i for i in range(len(sm))
           if sm[i] >= 0.20 * peak
           and (i == 0 or sm[i] >= sm[i - 1])
           and (i == len(sm) - 1 or sm[i] >= sm[i + 1])]
    if not idx:
        return 1, 0.0

    kept = [idx[0]]
    for i in idx[1:]:
        j = kept[-1]
        trough = sm[j:i + 1].min()
        if trough < 0.5 * min(sm[i], sm[j]):
            kept.append(i)
        elif sm[i] > sm[j]:
            kept[-1] = i

    first = kept[0]
    later = [sm[i] for i in kept[1:]]
    reignition = float(max(later) / sm[first]) if later and sm[first] > 0 else 0.0
    return len(kept), reignition


# --------------------------------------------------------------------------
# rule-based archetype assignment
# --------------------------------------------------------------------------

def _classify(f, th) -> str:
    """Assign one archetype from a feature dict. First match wins.

    Order matters and encodes precedence: the timing anomalies (necro,
    sleeper) are tested before the mix categories, because a post that is
    both like-forward *and* a sleeper is far more interestingly described as
    a sleeper. `standard` is tested last of the real rules so that it means
    "fast, saturating, and nothing else remarkable" rather than acting as a
    catch-all.
    """
    late72 = f["late72"]
    late24 = f["late24"]
    t50_h = f["t50_h"]

    # Necro and evergreen both put most of their mass late; `burst_share` —
    # the fraction landing in the single busiest 6h window — is what tells a
    # late *burst* apart from a flat week-long trickle. Without it the necro
    # rule swallows every evergreen post, since uniform accumulation over a
    # week also puts >50% of its mass past 72h.
    if (late72 >= th["necro_late72"]
            and f["burst_share"] >= th["necro_burst_share"]):
        return "necro"

    if (f["reignition"] >= th["sleeper_reignition"]
            and f["n_waves"] >= 2
            and late24 >= th["sleeper_late24"]):
        return "sleeper"

    arg_share = f["reply_share"] + f["quote_share"]
    if (arg_share >= th["pileon_arg_share"]
            and f["reply_lag_h"] >= th["pileon_lag_hours"]
            and (f["reply_outperform"] > th["pileon_outperform"]
                 or f["oon_delta"] >= th["pileon_oon_delta"])):
        return "pile_on"

    if (f["reply_share"] >= th["conv_reply_share"]
            and f["in_network_share"] >= th["conv_in_network"]
            and f["reply_outperform"] <= th["pileon_outperform"]):
        return "conversation"

    if (f["repost_share"] >= th["broadcast_repost"]
            and f["oon_delta"] >= th["broadcast_oon_delta"]):
        return "broadcast"

    if f["like_share"] >= th["likefwd_like_share"]:
        return "like_forward"

    if (t50_h >= th["evergreen_t50_h"]
            and f["n_waves"] == 1
            and f["burst_share"] < th["evergreen_burst_share"]
            and late24 >= th["evergreen_late24"]):
        return "evergreen"

    if t50_h <= th["standard_t50_h"] and late24 <= th["standard_late24"]:
        return "standard"

    return "unclassified"


# --------------------------------------------------------------------------
# cohort + event extraction
# --------------------------------------------------------------------------

def _bounds(con, snapshot_date: str, cohort_days: int, horizon_hours: int):
    """Resolve (cut, cohort_lo, cohort_hi) for the snapshot.

    Every post in the cohort must get the *same* observation horizon or the
    shape features are not comparable — a post from the last day of the
    window would look like it died young when really we just stopped
    watching. So the cohort ends one full horizon before the data does:

        cohort = [cut - horizon - cohort_days, cut - horizon]

    `cut` is the newest post we actually hold, clamped to a day past the
    nominal snapshot date so a handful of skewed TIDs can't drag it into
    the future and silently empty the cohort.
    """
    from datetime import timedelta

    guard = f"{snapshot_date} 00:00:00"
    row = con.execute(
        "SELECT MAX(created_at) FROM posts "
        "WHERE created_at <= TIMESTAMP '{}' + INTERVAL 1 DAY".format(guard)
    ).fetchone()
    cut = row[0] if row and row[0] is not None else None
    if cut is None:
        raise SystemExit("no posts in snapshot — cannot build a cohort")

    cohort_hi = cut - timedelta(hours=horizon_hours)
    cohort_lo = cohort_hi - timedelta(days=cohort_days)
    return cut, cohort_lo, cohort_hi


def _build_cohort(con, cohort_lo, cohort_hi, *, min_engagement, max_posts,
                  root_posts_only, seed, say):
    """Materialize `lf_cohort`: the posts whose lifelines we will measure.

    Filter order matters at this scale. `post_aggs` is one row per post
    (271M on the 2026-07-31 snapshot) but the engagement floor is brutally
    selective, so we apply it *first* and join the surviving handful against
    the date-filtered `posts` rather than the other way round.
    """
    post_cols = _columns(con, "posts")

    conds = ["p.created_at >= ?", "p.created_at < ?"]
    if "source" in post_cols:
        # target_only posts have NULL reply/quote refs and are usually
        # deleted-or-never-seen records; their mix features would be wrong.
        conds.append("p.source = 'record'")
    if root_posts_only and "reply_parent_uri_id" in post_cols:
        # Replies live by different rules (they inherit a thread's audience),
        # so mixing them into the taxonomy blurs every cluster.
        conds.append("p.reply_parent_uri_id IS NULL")

    extra = []
    if "rkey" in post_cols:
        extra.append("p.rkey")
    if "source" in post_cols:
        extra.append("p.source")
    extra_sel = ("," + ",".join(extra)) if extra else ""

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE lf_cohort_all AS
        WITH eligible AS (
          SELECT uri_id, likes, reposts, replies, quotes,
                 (likes + reposts + replies + quotes) AS total_eng
          FROM post_aggs
          WHERE likes + reposts + replies + quotes >= {int(min_engagement)}
        )
        SELECT p.uri_id, p.author_did_id, p.created_at,
               e.likes AS agg_likes, e.reposts AS agg_reposts,
               e.replies AS agg_replies, e.quotes AS agg_quotes,
               e.total_eng {extra_sel}
        FROM eligible e
        JOIN posts p USING (uri_id)
        WHERE {' AND '.join(conds)}
    """, [cohort_lo, cohort_hi])

    n_all = con.execute("SELECT COUNT(*) FROM lf_cohort_all").fetchone()[0]
    if n_all == 0:
        raise SystemExit(
            "cohort is empty — check min_engagement / cohort_days against "
            "the snapshot window"
        )

    if n_all > max_posts:
        say(f"cohort {n_all:,} posts -> reservoir sample {max_posts:,}")
        con.execute(
            "CREATE OR REPLACE TEMP TABLE lf_cohort AS "
            f"SELECT * FROM lf_cohort_all USING SAMPLE {int(max_posts)} ROWS "
            f"(reservoir, {int(seed)})"
        )
    else:
        con.execute(
            "CREATE OR REPLACE TEMP TABLE lf_cohort AS SELECT * FROM lf_cohort_all"
        )
    n_cohort = con.execute("SELECT COUNT(*) FROM lf_cohort").fetchone()[0]
    return n_all, n_cohort


def _build_events(con, horizon_hours: int, say):
    """Materialize `lf_ev`: one row per engagement event inside the horizon.

    Four channels unioned into a narrow table. Self-engagement (the author
    liking or replying to their own post) is excluded — an author's own
    thread continuations are authorship, not reception, and they would
    otherwise inflate the reply share of every threaded post. The count is
    kept separately so it isn't silently lost.

    The `>= c.created_at` guard drops the small number of events whose TID
    predates the post's own, which happens with clock skew across PDSes.
    """
    h = int(horizon_hours)
    post_cols = _columns(con, "posts")
    parts = [f"""
        SELECT c.uri_id, {CH_LIKE}::TINYINT AS ch, l.actor_did_id AS actor,
               c.author_did_id AS author, l.created_at AS ts,
               date_diff('second', c.created_at, l.created_at) AS dt
        FROM lf_cohort c
        JOIN likes l ON l.subject_uri_id = c.uri_id
        WHERE l.created_at >= c.created_at
          AND l.created_at < c.created_at + INTERVAL {h} HOUR
          AND l.actor_did_id <> c.author_did_id
    """, f"""
        SELECT c.uri_id, {CH_REPOST}::TINYINT, r.actor_did_id,
               c.author_did_id, r.created_at,
               date_diff('second', c.created_at, r.created_at)
        FROM lf_cohort c
        JOIN reposts r ON r.subject_uri_id = c.uri_id
        WHERE r.created_at >= c.created_at
          AND r.created_at < c.created_at + INTERVAL {h} HOUR
          AND r.actor_did_id <> c.author_did_id
    """]
    if "reply_parent_uri_id" in post_cols:
        parts.append(f"""
        SELECT c.uri_id, {CH_REPLY}::TINYINT, q.author_did_id,
               c.author_did_id, q.created_at,
               date_diff('second', c.created_at, q.created_at)
        FROM lf_cohort c
        JOIN posts q ON q.reply_parent_uri_id = c.uri_id
        WHERE q.created_at >= c.created_at
          AND q.created_at < c.created_at + INTERVAL {h} HOUR
          AND q.author_did_id <> c.author_did_id
        """)
    if "quote_uri_id" in post_cols:
        parts.append(f"""
        SELECT c.uri_id, {CH_QUOTE}::TINYINT, q.author_did_id,
               c.author_did_id, q.created_at,
               date_diff('second', c.created_at, q.created_at)
        FROM lf_cohort c
        JOIN posts q ON q.quote_uri_id = c.uri_id
        WHERE q.created_at >= c.created_at
          AND q.created_at < c.created_at + INTERVAL {h} HOUR
          AND q.author_did_id <> c.author_did_id
        """)

    con.execute("CREATE OR REPLACE TEMP TABLE lf_ev AS "
                + "\nUNION ALL\n".join(parts))
    n_ev = con.execute("SELECT COUNT(*) FROM lf_ev").fetchone()[0]
    say(f"events in horizon: {n_ev:,}")
    return n_ev


def _build_follow_edges(con, say):
    """Materialize `lf_follow`: (engager, author) -> earliest follow time.

    This is the audience axis. Rather than joining 1.4B `follows` rows
    against the event table on (src, dst), we reduce the events to their
    distinct (actor, author) pairs first — tens of millions at most — and
    join that against `follows`, which DuckDB executes as a single
    sequential pass over the big table probing a compact hash set.

    Two honest caveats, both surfaced in the report:

      * Unfollows are invisible. The snapshot holds *surviving* follow
        edges, so somebody who followed the author, engaged, then later
        unfollowed shows up here as having no edge at all. In-network is
        therefore an undercount and out-of-network an overcount.
      * A NULL `created_at` on an existing edge means we know the
        relationship exists but not when it started; we count that as
        in-network, which is the conservative direction given the above.
    """
    con.execute("""
        CREATE OR REPLACE TEMP TABLE lf_pairs AS
        SELECT DISTINCT actor, author FROM lf_ev
    """)
    n_pairs = con.execute("SELECT COUNT(*) FROM lf_pairs").fetchone()[0]
    say(f"distinct (engager, author) pairs: {n_pairs:,}")

    con.execute("""
        CREATE OR REPLACE TEMP TABLE lf_follow AS
        SELECT p.actor, p.author, MIN(f.created_at) AS followed_at
        FROM lf_pairs p
        JOIN follows f
          ON f.src_did_id = p.actor AND f.dst_did_id = p.author
        GROUP BY 1, 2
    """)
    n_edges = con.execute("SELECT COUNT(*) FROM lf_follow").fetchone()[0]
    say(f"resolved follow edges: {n_edges:,} "
        f"({100.0 * n_edges / max(n_pairs, 1):.1f}% of pairs in-network)")
    return n_pairs, n_edges


# The in-network predicate, written once and reused so the definition can't
# drift between the histogram and the scalar features.
_IN_NET = ("(fe.actor IS NOT NULL AND "
           "(fe.followed_at IS NULL OR fe.followed_at <= e.ts))")


def _build_bins(con, n_bins: int, horizon_hours: int, say):
    """Per (post, channel, log-time bin) counts, with the in-network split.

    Bins are log-spaced because engagement is violently front-loaded:
    uniform hourly bins would spend 90% of their resolution on the flat
    tail and blur the entire first hour into one point. Bin edges are
    ln(1 + dt) scaled across the horizon, so bin 0 is the first few seconds
    and the last bin is the final couple of days.

    Only non-empty (post, channel, bin) cells are emitted — the array is
    sparse and is scattered into a dense one in numpy.
    """
    horizon_s = horizon_hours * 3600
    ln_h = math.log(1.0 + horizon_s)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE lf_bins AS
        SELECT e.uri_id, e.ch,
               LEAST({int(n_bins) - 1}, GREATEST(0, CAST(FLOOR(
                 LN(1 + GREATEST(e.dt, 0)) / {ln_h} * {int(n_bins)}
               ) AS INTEGER))) AS bin,
               COUNT(*) AS n,
               COUNT(*) FILTER (WHERE {_IN_NET}) AS n_in
        FROM lf_ev e
        LEFT JOIN lf_follow fe
          ON fe.actor = e.actor AND fe.author = e.author
        GROUP BY 1, 2, 3
    """)
    n_cells = con.execute("SELECT COUNT(*) FROM lf_bins").fetchone()[0]
    say(f"sparse bin cells: {n_cells:,}")
    return n_cells


def _build_scalar_features(con, say):
    """Per-post scalar features that are cheaper to compute in SQL than numpy.

    Timing quantiles are computed on the raw event deltas rather than on the
    binned curve so they don't inherit the bins' log quantization.
    """
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE lf_scalar AS
        SELECT
          e.uri_id,
          COUNT(*)                                          AS n_total,
          COUNT(*) FILTER (WHERE e.ch = {CH_LIKE})          AS n_like,
          COUNT(*) FILTER (WHERE e.ch = {CH_REPOST})        AS n_repost,
          COUNT(*) FILTER (WHERE e.ch = {CH_REPLY})         AS n_reply,
          COUNT(*) FILTER (WHERE e.ch = {CH_QUOTE})         AS n_quote,
          MEDIAN(e.dt)                                      AS t50_s,
          QUANTILE_CONT(e.dt, 0.9)                          AS t90_s,
          MEDIAN(e.dt) FILTER (WHERE e.ch = {CH_LIKE})      AS t50_like_s,
          MEDIAN(e.dt) FILTER (WHERE e.ch IN ({CH_REPLY}, {CH_QUOTE}))
                                                            AS t50_arg_s,
          AVG(CASE WHEN e.dt > 86400  THEN 1.0 ELSE 0.0 END) AS late24,
          AVG(CASE WHEN e.dt > 259200 THEN 1.0 ELSE 0.0 END) AS late72,
          AVG(CASE WHEN {_IN_NET} THEN 1.0 ELSE 0.0 END)     AS in_network_share,
          -- Early vs late in-network share. The gap between them is the
          -- algorithmic-distribution signature: an out-of-network wave that
          -- shows up *after* the author's own followers have been served.
          COALESCE(AVG(CASE WHEN {_IN_NET} THEN 1.0 ELSE 0.0 END)
                     FILTER (WHERE e.dt <= 3600), NULL)      AS in_net_early,
          COALESCE(AVG(CASE WHEN {_IN_NET} THEN 1.0 ELSE 0.0 END)
                     FILTER (WHERE e.dt > 21600), NULL)      AS in_net_late,
          COUNT(DISTINCT e.actor)                            AS n_actors
        FROM lf_ev e
        LEFT JOIN lf_follow fe
          ON fe.actor = e.actor AND fe.author = e.author
        GROUP BY 1
    """)

    # Temporal concentration: what share of a post's engagement lands in its
    # single busiest 6-hour window. Computed on fixed-width wall-clock
    # buckets deliberately — this is the one feature that must NOT be in log
    # time, because its whole job is to say whether arrivals are spread
    # evenly across real hours or crammed into a burst.
    #
    # The window *slides* at 1h granularity rather than sitting on a fixed
    # 6h grid. With a fixed grid the measurement depends on where the burst
    # happens to fall relative to the grid phase: a four-hour burst
    # straddling a boundary reads as two half-sized ones and a genuine necro
    # gets misfiled as evergreen. The RANGE frame makes it phase-independent.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE lf_burst AS
        WITH hourly AS (
          SELECT uri_id, (dt / 3600)::INTEGER AS h, COUNT(*) AS n
          FROM lf_ev GROUP BY 1, 2
        ),
        windowed AS (
          SELECT uri_id,
                 SUM(n) OVER (PARTITION BY uri_id ORDER BY h
                              RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) AS w,
                 SUM(n) OVER (PARTITION BY uri_id) AS tot
          FROM hourly
        )
        SELECT uri_id, MAX(w)::DOUBLE / MAX(tot) AS burst_share
        FROM windowed GROUP BY 1
    """)

    # Do the replies outperform the post they answer? This is the
    # quantitative form of "getting ratio'd" and it is what separates a
    # pile-on from a busy but friendly thread.
    post_cols = _columns(con, "posts")
    if "reply_parent_uri_id" in post_cols:
        con.execute("""
            CREATE OR REPLACE TEMP TABLE lf_replyperf AS
            SELECT c.uri_id, SUM(pa.likes) AS reply_likes
            FROM lf_cohort c
            JOIN posts r ON r.reply_parent_uri_id = c.uri_id
            JOIN post_aggs pa ON pa.uri_id = r.uri_id
            WHERE r.author_did_id <> c.author_did_id
            GROUP BY 1
        """)
    else:
        con.execute("CREATE OR REPLACE TEMP TABLE lf_replyperf "
                    "AS SELECT NULL::BIGINT AS uri_id, NULL::BIGINT AS reply_likes")
    say("scalar features built")


# --------------------------------------------------------------------------
# assembly
# --------------------------------------------------------------------------

def _assemble(con, n_bins: int, horizon_hours: int, thresholds, say):
    """Pull SQL output into numpy, derive curve features, classify.

    Returns (uri_ids, cum, rate, in_share, feats, labels) where `cum` and
    `rate` are (n_posts, n_bins) all-channel curves and `feats` is a list of
    per-post feature dicts aligned with `uri_ids`.
    """
    import numpy as np

    scalar = con.execute("""
        SELECT s.uri_id, s.n_total, s.n_like, s.n_repost, s.n_reply, s.n_quote,
               s.t50_s, s.t90_s, s.t50_like_s, s.t50_arg_s,
               s.late24, s.late72, s.in_network_share,
               s.in_net_early, s.in_net_late, s.n_actors,
               c.agg_likes, COALESCE(rp.reply_likes, 0) AS reply_likes,
               COALESCE(bu.burst_share, 1.0) AS burst_share
        FROM lf_scalar s
        JOIN lf_cohort c USING (uri_id)
        LEFT JOIN lf_replyperf rp USING (uri_id)
        LEFT JOIN lf_burst bu USING (uri_id)
        ORDER BY s.uri_id
    """).fetchall()
    if not scalar:
        raise SystemExit("no posts survived event extraction")

    # Plain Python ints, not an int64 numpy array: `uri_id` is the
    # xxhash3-64 of the post URI, so roughly half of all real values exceed
    # int64's range and numpy raises OverflowError on them. Nothing here does
    # arithmetic on these — they are only ever hashed into `index` and echoed
    # back out — so the widest available integer type is the right one.
    uri_ids = [int(r[0]) for r in scalar]
    index = {u: i for i, u in enumerate(uri_ids)}
    n = len(uri_ids)
    say(f"posts with lifelines: {n:,}")

    # Scatter the sparse bin cells into dense (n_posts, n_bins) arrays.
    counts = np.zeros((n, n_bins), dtype=np.float64)
    counts_in = np.zeros((n, n_bins), dtype=np.float64)
    per_ch = np.zeros((n, 4, n_bins), dtype=np.float64)
    for uri_id, ch, b, cnt, cnt_in in con.execute(
            "SELECT uri_id, ch, bin, n, n_in FROM lf_bins").fetchall():
        i = index.get(int(uri_id))
        if i is None:
            continue
        counts[i, b] += cnt
        counts_in[i, b] += cnt_in
        per_ch[i, int(ch), b] += cnt

    # Bin widths in seconds, for converting counts to an arrival *rate*.
    # Without this the log-spaced bins make every curve look like it peaks
    # at the end, because late bins are hundreds of times wider.
    horizon_s = horizon_hours * 3600.0
    ln_h = math.log(1.0 + horizon_s)
    edges = np.array([math.exp(ln_h * b / n_bins) - 1.0 for b in range(n_bins + 1)])
    edges[-1] = horizon_s
    widths = np.maximum(np.diff(edges), 1.0)

    totals = counts.sum(axis=1)
    totals[totals == 0] = 1.0
    cum = np.cumsum(counts, axis=1) / totals[:, None]
    rate = counts / widths[None, :]

    feats = []
    for i, row in enumerate(scalar):
        (_uri, n_total, n_like, n_repost, n_reply, n_quote,
         t50_s, t90_s, t50_like_s, t50_arg_s, late24, late72,
         in_net, in_early, in_late, n_actors, agg_likes, reply_likes,
         burst_share) = row
        tot = float(n_total) or 1.0
        n_waves, reignition = _wave_features(counts[i])
        # Out-of-network delta: how much *more* out-of-network the late
        # audience is than the early one. Positive means strangers arrived
        # after the followers did.
        if in_early is None or in_late is None:
            oon_delta = 0.0
        else:
            oon_delta = float(in_early) - float(in_late)
        reply_lag_h = 0.0
        if t50_arg_s is not None and t50_like_s is not None:
            reply_lag_h = (float(t50_arg_s) - float(t50_like_s)) / 3600.0
        feats.append({
            "uri_id": int(_uri),
            "n_total": int(n_total),
            "n_like": int(n_like), "n_repost": int(n_repost),
            "n_reply": int(n_reply), "n_quote": int(n_quote),
            "n_actors": int(n_actors),
            "like_share": n_like / tot, "repost_share": n_repost / tot,
            "reply_share": n_reply / tot, "quote_share": n_quote / tot,
            "t50_h": float(t50_s or 0) / 3600.0,
            "t90_h": float(t90_s or 0) / 3600.0,
            "late24": float(late24 or 0.0), "late72": float(late72 or 0.0),
            "in_network_share": float(in_net or 0.0),
            "in_net_early": None if in_early is None else float(in_early),
            "in_net_late": None if in_late is None else float(in_late),
            "oon_delta": oon_delta,
            "reply_lag_h": reply_lag_h,
            "reply_outperform": (float(reply_likes) / float(agg_likes))
                                if agg_likes else 0.0,
            "n_waves": n_waves,
            "reignition": reignition,
            "burst_share": float(burst_share),
        })

    labels = [_classify(f, thresholds) for f in feats]
    return uri_ids, cum, counts, counts_in, rate, per_ch, feats, labels


def _fetch_examples(con, feats, labels, per_archetype, hydrate_urls, say):
    """Pick `per_archetype` representative posts per archetype, with URLs.

    Representative means closest to the archetype's own median on the
    features the rules key off, restricted to posts in the upper half of
    the archetype by engagement so the examples are actually worth looking
    at. A medoid that nobody engaged with teaches you nothing.
    """
    import numpy as np

    key_feats = ["t50_h", "late24", "late72", "like_share", "repost_share",
                 "reply_share", "quote_share", "in_network_share",
                 "oon_delta", "reignition"]
    by_arch: dict[str, list[int]] = {}
    for i, lab in enumerate(labels):
        by_arch.setdefault(lab, []).append(i)

    chosen: dict[str, list[dict]] = {}
    for arch, idxs in by_arch.items():
        M = np.array([[feats[i][k] for k in key_feats] for i in idxs], dtype=float)
        med = np.median(M, axis=0)
        spread = np.maximum(M.std(axis=0), 1e-9)
        d = np.linalg.norm((M - med) / spread, axis=1)
        eng = np.array([feats[i]["n_total"] for i in idxs], dtype=float)
        floor = np.median(eng)
        ok = np.where(eng >= floor)[0]
        if len(ok) < per_archetype:
            ok = np.arange(len(idxs))
        pick = ok[np.argsort(d[ok])[:per_archetype]]
        chosen[arch] = [dict(feats[idxs[p]]) for p in pick]

    if hydrate_urls:
        _hydrate_urls(con, [e for exs in chosen.values() for e in exs], say)
    return chosen


def _hydrate_urls(con, rows, say) -> None:
    """Fill `url` / `author_did` / `posted_at` on example dicts, in place.

    Kept separate from example selection because two callers need it and
    only one of them should ever use it: archetype examples always link
    out, while authenticity-flagged posts only do so on explicit request.
    """
    want = [r["uri_id"] for r in rows]
    if not want or "rkey" not in _columns(con, "posts"):
        return
    placeholders = ",".join("?" * len(want))
    # `post_url` is a macro defined by 06_url_macros.sql inside real
    # snapshots; synthetic fixtures don't have it, so fall back to the
    # same concatenation the macro performs.
    url_expr = "post_url(a.did, p.rkey)"
    try:
        con.execute("SELECT post_url('did:plc:x', 'abc')").fetchone()
    except Exception:
        url_expr = "'https://bsky.app/profile/' || a.did || '/post/' || p.rkey"
    found = con.execute(f"""
        SELECT p.uri_id, {url_expr}, a.did, p.created_at
        FROM posts p JOIN actors a ON a.did_id = p.author_did_id
        WHERE p.uri_id IN ({placeholders})
    """, want).fetchall()
    url_by_uri = {int(r[0]): (r[1], r[2], r[3]) for r in found}
    for r in rows:
        hit = url_by_uri.get(r["uri_id"])
        if hit:
            r["url"], r["author_did"], r["posted_at"] = hit
    say(f"hydrated {len(url_by_uri)} post URLs")


# --------------------------------------------------------------------------
# figures
# --------------------------------------------------------------------------

def _bin_midpoints_hours(n_bins: int, horizon_hours: int):
    """Bin centres in hours, for plotting on a log time axis."""
    import numpy as np
    horizon_s = horizon_hours * 3600.0
    ln_h = math.log(1.0 + horizon_s)
    edges = np.array([math.exp(ln_h * b / n_bins) - 1.0 for b in range(n_bins + 1)])
    edges[-1] = horizon_s
    mids = np.sqrt(np.maximum(edges[:-1], 1.0) * np.maximum(edges[1:], 1.0))
    return mids / 3600.0


def _x_range(counts, xs, *, floor_frac=0.001):
    """Log-axis range clipped to where the cohort actually has engagement.

    The first bins span seconds, and on a cohort with no sub-minute activity
    they are empty — leaving a third of every chart as blank axis. Start the
    axis at the first bin holding at least `floor_frac` of all engagement so
    the plotted region is the region with data. Returned in log10 units,
    which is what plotly's log axes expect.
    """
    import numpy as np

    per_bin = counts.sum(axis=0)
    total = per_bin.sum()
    if total <= 0:
        return None
    live = np.where(per_bin >= floor_frac * total)[0]
    if len(live) == 0:
        return None
    lo = max(xs[live[0]] * 0.5, 1e-4)
    return [math.log10(lo), math.log10(xs[-1] * 1.5)]


def _fig_cumulative(cum, labels, xs, present, xrange=None):
    """Mean cumulative-fraction curve per archetype — the headline chart."""
    import numpy as np
    import plotly.graph_objects as go

    fig = go.Figure()
    for arch in present:
        mask = np.array([l == arch for l in labels])
        if not mask.any():
            continue
        m = cum[mask].mean(axis=0)
        fig.add_trace(go.Scatter(
            x=xs, y=m, mode="lines", name=arch.replace("_", " "),
            line=dict(color=ARCHETYPE_COLORS[arch], width=2.5),
            hovertemplate="%{x:.2f}h — %{y:.0%}<extra>" + arch + "</extra>",
        ))
    fig.update_layout(
        template="bsky", height=440,
        xaxis=dict(type="log", title="hours since post (log)", range=xrange),
        yaxis=dict(title="share of the post's final engagement",
                   tickformat=".0%", range=[0, 1.02]),
        legend=dict(orientation="h", y=-0.22),
        margin=dict(l=70, r=20, t=20, b=80),
    )
    return fig


def _fig_in_network(counts, counts_in, labels, xs, present, *,
                    min_events=30, xrange=None):
    """In-network share of arrivals over time — the audience axis.

    Pooled across the archetype (total in-network arrivals in the bin over
    total arrivals in the bin) rather than averaged over per-post ratios.
    Averaging ratios lets a post that received two engagements in a bin
    contribute a 0% or 100% reading with the same weight as one that
    received two thousand, which turns the first few bins into noise. Bins
    holding fewer than `min_events` arrivals across the whole archetype are
    dropped rather than drawn, because there is nothing to say about them.
    """
    import numpy as np
    import plotly.graph_objects as go

    fig = go.Figure()
    for arch in present:
        mask = np.array([l == arch for l in labels])
        if not mask.any():
            continue
        tot = counts[mask].sum(axis=0)
        inn = counts_in[mask].sum(axis=0)
        y = np.where(tot >= min_events, inn / np.maximum(tot, 1), np.nan)
        fig.add_trace(go.Scatter(
            # Markers matter here: archetypes whose mass sits in one or two
            # bins (necro especially) have isolated non-null points, and a
            # lines-only trace draws nothing at all for those — the series
            # silently vanishes from the chart.
            x=xs, y=y, mode="lines+markers", name=arch.replace("_", " "),
            line=dict(color=ARCHETYPE_COLORS[arch], width=2.5),
            marker=dict(size=4, color=ARCHETYPE_COLORS[arch]),
            connectgaps=False,
            hovertemplate="%{x:.2f}h — %{y:.0%} in-network<extra>" + arch + "</extra>",
        ))
    fig.update_layout(
        template="bsky", height=420,
        xaxis=dict(type="log", title="hours since post (log)", range=xrange),
        yaxis=dict(title="share of arrivals already following the author",
                   tickformat=".0%", range=[0, 1.02]),
        legend=dict(orientation="h", y=-0.24),
        margin=dict(l=70, r=20, t=20, b=80),
    )
    return fig


def _fig_mix(feats, labels, present):
    """Stacked channel composition per archetype — the mix axis."""
    import numpy as np
    import plotly.graph_objects as go

    channels = [("like_share", "likes", "#0085ff"),
                ("repost_share", "reposts", "#7c3aed"),
                ("reply_share", "replies", "#ef4444"),
                ("quote_share", "quotes", "#f59e0b")]
    fig = go.Figure()
    names = [a.replace("_", " ") for a in present]
    for key, label, color in channels:
        vals = []
        for arch in present:
            sel = [feats[i][key] for i, l in enumerate(labels) if l == arch]
            vals.append(float(np.mean(sel)) if sel else 0.0)
        fig.add_trace(go.Bar(
            x=names, y=vals, name=label, marker_color=color,
            hovertemplate="%{x}: %{y:.0%} " + label + "<extra></extra>",
        ))
    fig.update_layout(
        template="bsky", barmode="stack", height=380,
        yaxis=dict(title="mean share of engagement", tickformat=".0%"),
        xaxis=dict(title=""),
        legend=dict(orientation="h", y=-0.28),
        margin=dict(l=70, r=20, t=20, b=90),
    )
    return fig


def _fig_shapes(centroids, shape_sizes, xs, xrange=None):
    """The KSC centroids themselves — the shapes the data actually contains."""
    import plotly.graph_objects as go

    fig = go.Figure()
    order = sorted(range(len(centroids)), key=lambda j: -shape_sizes.get(j, 0))
    palette = ["#0085ff", "#ef4444", "#f59e0b", "#16a34a", "#7c3aed",
               "#0d9488", "#ff5d8f", "#a16207"]
    for rank, j in enumerate(order):
        c = centroids[j]
        peak = max(abs(c).max(), 1e-12)
        fig.add_trace(go.Scatter(
            x=xs, y=c / peak, mode="lines",
            name=f"shape {rank + 1} (n={shape_sizes.get(j, 0):,})",
            line=dict(color=palette[rank % len(palette)], width=2.5),
        ))
    fig.update_layout(
        template="bsky", height=400,
        xaxis=dict(type="log", title="hours since post (log)", range=xrange),
        yaxis=dict(title="engagement per log-time bin (peak-normalized)"),
        legend=dict(orientation="h", y=-0.24),
        margin=dict(l=70, r=20, t=20, b=80),
    )
    return fig


def _fig_authenticity(feats, labels, present):
    """Score distribution overall, and mean score per archetype.

    The per-archetype panel is the standing confound check, not decoration.
    `broadcast` is the archetype defined by an out-of-network wave — which
    is what algorithmic distribution looks like. If it carries the top mean
    score, the composite has drifted into measuring Discover rather than
    coordination, and the weights need moving back toward the correlation
    signals. That failure mode is invisible unless it is plotted.
    """
    import numpy as np
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    vals = np.array([f["auth_score"] for f in feats
                     if f.get("auth_score") is not None])
    fig = make_subplots(
        rows=1, cols=2, column_widths=[0.45, 0.55],
        subplot_titles=("score distribution", "mean score by archetype"),
    )
    if len(vals):
        fig.add_trace(go.Histogram(
            x=vals, nbinsx=40, marker_color=BRAND, showlegend=False,
            hovertemplate="score %{x:.2f}: %{y} posts<extra></extra>",
        ), row=1, col=1)

    names, means, colors = [], [], []
    for arch in present:
        sel = [f["auth_score"] for f, l in zip(feats, labels)
               if l == arch and f.get("auth_score") is not None]
        if not sel:
            continue
        names.append(arch.replace("_", " "))
        means.append(float(np.mean(sel)))
        colors.append(ARCHETYPE_COLORS[arch])
    order = np.argsort(means)[::-1]
    fig.add_trace(go.Bar(
        x=[names[i] for i in order], y=[means[i] for i in order],
        marker_color=[colors[i] for i in order], showlegend=False,
        hovertemplate="%{x}: %{y:.3f}<extra></extra>",
    ), row=1, col=2)

    fig.update_layout(template="bsky", height=380,
                      margin=dict(l=60, r=20, t=50, b=90), bargap=0.15)
    fig.update_yaxes(title_text="posts", row=1, col=1)
    fig.update_xaxes(title_text="composite score", row=1, col=1)
    fig.update_yaxes(title_text="mean composite score", row=1, col=2)
    return fig


def _fig_crosstab(shape_labels, labels, present, n_shapes, shape_rank):
    """KSC shape x rule label — does the rule taxonomy track the real shapes?"""
    import numpy as np
    import plotly.graph_objects as go

    z = np.zeros((n_shapes, len(present)))
    for s, lab in zip(shape_labels, labels):
        if lab in present:
            z[shape_rank[int(s)], present.index(lab)] += 1
    row_tot = z.sum(axis=1, keepdims=True)
    row_tot[row_tot == 0] = 1.0
    zn = z / row_tot
    fig = go.Figure(go.Heatmap(
        z=zn, x=[a.replace("_", " ") for a in present],
        y=[f"shape {i + 1}" for i in range(n_shapes)],
        colorscale=[[0, "#ffffff"], [1, BRAND]], zmin=0, zmax=1,
        hovertemplate="%{y} x %{x}: %{z:.0%} of the shape<extra></extra>",
        colorbar=dict(title="row %", tickformat=".0%"),
    ))
    fig.update_layout(template="bsky", height=380,
                      margin=dict(l=90, r=20, t=20, b=90))
    return fig


# --------------------------------------------------------------------------
# report
# --------------------------------------------------------------------------

def _examples_html(examples, present) -> str:
    rows = []
    for arch in present:
        exs = examples.get(arch, [])
        if not exs:
            continue
        cards = []
        for e in exs:
            url = e.get("url")
            link = (f'<a href="{url}" target="_blank" rel="noopener">open post</a>'
                    if url else "<span class='muted'>uri_id "
                                f"{e['uri_id']}</span>")
            cards.append(
                "<tr>"
                f"<td>{link}</td>"
                f"<td class='num'>{fmt_int(e['n_total'])}</td>"
                f"<td class='num'>{e['like_share']:.0%}/{e['repost_share']:.0%}/"
                f"{e['reply_share']:.0%}/{e['quote_share']:.0%}</td>"
                f"<td class='num'>{e['t50_h']:.1f}h</td>"
                f"<td class='num'>{e['late24']:.0%}</td>"
                f"<td class='num'>{e['in_network_share']:.0%}</td>"
                f"<td class='num'>{e['reply_outperform']:.2f}x</td>"
                "</tr>"
            )
        rows.append(
            f"<h3 style='margin:28px 0 6px'>"
            f"<span class='dot' style='background:{ARCHETYPE_COLORS[arch]}'></span>"
            f"{arch.replace('_', ' ')}</h3>"
            f"<p class='muted' style='margin:0 0 10px'>{ARCHETYPE_BLURB[arch]}</p>"
            "<table class='ex'><thead><tr><th>post</th><th>engagements</th>"
            "<th>like/repost/reply/quote</th><th>t50</th><th>after 24h</th>"
            "<th>in-network</th><th>reply likes vs post</th></tr></thead>"
            f"<tbody>{''.join(cards)}</tbody></table>"
        )
    return "\n".join(rows)


def _authenticity_html(sidecar, figs, flagged, linked) -> str:
    """The authenticity section, or an explicit note that it did not run."""
    auth = sidecar.get("authenticity")
    if not auth or not auth.get("signals_run"):
        return ""

    sig_rows = "".join(
        f"<tr><td>{s['name'].replace('_', ' ')}</td>"
        f"<td>{s['description']}</td>"
        f"<td>{s['family']}</td>"
        f"<td class='num'>{s['weight']:.0%}</td></tr>"
        for s in auth["signals_run"]
    )
    skipped = auth.get("skipped") or {}
    skip_html = ""
    if skipped:
        items = "".join(f"<li><code>{k}</code> — {v}</li>"
                        for k, v in skipped.items())
        skip_html = (f"<p class='muted'>Signals not run: <ul class='muted'>"
                     f"{items}</ul></p>")

    fam = auth.get("family_weights", {})
    rows = []
    for e in flagged:
        cell = (f'<a href="{e["url"]}" target="_blank" rel="noopener">open post</a>'
                if linked and e.get("url") else
                f"<span class='muted'>post {e['rank']}</span>")
        rows.append(
            f"<tr><td>{cell}</td>"
            f"<td class='num'>{e['score']:.3f}</td>"
            f"<td class='num'>{fmt_int(e['n_total'])}</td>"
            f"<td>{e['archetype'].replace('_', ' ')}</td>"
            f"<td class='num'>{e['in_network_share']:.0%}</td></tr>"
        )

    return f"""
<section>
<div class="kicker">authenticity</div>
<h2>Engagement that does not look like it was earned</h2>
<p>A fourth axis, not a tenth archetype. Nobody buys engagement on a post
that got no traction — they buy it to push something already moving — so a
post is routinely a genuine sleeper hit <em>and</em> partly amplified.
Forcing that into an exclusive bucket gets the common case wrong, so this
score rides alongside the archetype label instead.</p>
<div class="figure">{figs['authenticity']}</div>
<p>The right-hand panel is a running check on the method rather than a
result. <strong>broadcast</strong> is the archetype defined by an
out-of-network wave, which is exactly what algorithmic distribution looks
like. If it ever carries the top mean score, this composite has drifted
into measuring Discover instead of coordination, and weight needs moving
back toward the correlation signals.</p>

<h3 style="margin:28px 0 6px">What it is built from</h3>
<table><thead><tr><th>signal</th><th>what it measures</th>
<th>family</th><th class="num">weight</th></tr></thead>
<tbody>{sig_rows}</tbody></table>
<p class="muted" style="margin-top:10px">Correlation-family signals carry
{fam.get('correlation', 0):.0%} of the total weight against
{fam.get('timing', 0):.0%} for timing. That split is deliberate: timing
regularity is one line of code to jitter away, and more importantly it is
what makes ordinary algorithmic reach look like a bot farm. The thing that
is genuinely hard to fake is <em>who</em> shows up — a fleet works a list,
so its accounts keep appearing together.</p>

<h3 style="margin:28px 0 6px">Highest-scoring posts</h3>
<p class="muted" style="margin:0 0 10px">These are the ten posts this
composite ranks highest, linked like every other example in the report.
Read them as <em>ranked candidates for inspection, not findings</em>: the
score is a percentile within this cohort built from correlational proxies,
and none of its inputs observes a purchase. A high rank says the engagement
pattern is unusual next to the rest of the cohort — an author with a tight
regular community can land here without having bought anything.</p>
<table><thead><tr><th>post</th><th class="num">score</th>
<th class="num">engagements</th><th>archetype</th>
<th class="num">in-network</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table>
{skip_html}
</section>
"""


def _render_html(*, snapshot_date, sidecar, figs, examples, present,
                 flagged=(), link_flagged=True) -> bytes:
    pop = sidecar["archetypes"]
    total = sidecar["cohort"]["posts_analyzed"]
    unclassified_pct = 100.0 * pop.get("unclassified", {}).get("n", 0) / max(total, 1)

    tiles = [
        (fmt_int(total), "posts with lifelines", "brand"),
        (fmt_int(sidecar["cohort"]["events"]), "engagement events", ""),
        (f"{sidecar['audience']['in_network_share']:.0%}", "in-network overall", ""),
        (f"{unclassified_pct:.0f}%", "unclassified", "bad" if unclassified_pct > 25 else ""),
    ]
    tile_html = "".join(
        f"<div class='stat'><div class='v {cls}'>{v}</div>"
        f"<div class='l'>{l}</div></div>" for v, l, cls in tiles
    )

    pop_rows = "".join(
        f"<tr><td><span class='dot' style='background:{ARCHETYPE_COLORS[a]}'></span>"
        f"{a.replace('_', ' ')}</td>"
        f"<td class='num'>{fmt_int(pop[a]['n'])}</td>"
        f"<td class='num'>{pop[a]['share']:.1%}</td>"
        f"<td class='num'>{pop[a]['median_engagement']:,.0f}</td>"
        f"<td class='num'>{pop[a]['mean_t50_h']:.1f}h</td>"
        f"<td class='num'>{pop[a]['mean_in_network']:.0%}</td></tr>"
        for a in present
    )

    c = sidecar["cohort"]
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Post engagement archetypes — {snapshot_date}</title>
<style>{SHARED_CSS}
.dot {{ display:inline-block; width:10px; height:10px; border-radius:50%;
        margin-right:8px; vertical-align:middle; }}
table {{ border-collapse: collapse; width: 100%; font-size: 14px;
         background: white; border: 1px solid var(--rule); border-radius: 10px; }}
th, td {{ padding: 9px 12px; border-bottom: 1px solid var(--rule); text-align: left; }}
th {{ font-size: 12px; text-transform: uppercase; letter-spacing: .06em;
      color: var(--muted); font-weight: 600; }}
td.num, th.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
tbody tr:last-child td {{ border-bottom: none; }}
table.ex {{ margin-bottom: 8px; }}
.muted {{ color: var(--muted); font-size: 14px; }}
.caveat {{ background:#fff8e1; border:1px solid #f0d98a; border-radius:10px;
           padding:14px 18px; margin:22px 0; font-size:14.5px; }}
.caveat h4 {{ margin:0 0 8px; font-size:14px; text-transform:uppercase;
              letter-spacing:.06em; color:#8a6d1e; }}
.caveat ul {{ margin:0; padding-left:18px; }} .caveat li {{ margin:5px 0; }}
</style>
<script>{plotlyjs_inline()}</script>
</head><body><div class="wrap">

<div class="eyebrow">atproto snapshot · {snapshot_date}</div>
<h1>The <span class="accent">life of a post</span></h1>
<p class="lede">Every post has a heartbeat: the shape in which likes, reposts,
replies and quotes arrive over its first {c['horizon_hours'] // 24} days. Those
shapes are not all alike, and the differences say something about how
information actually moved through the network — who saw it, when, and
whether they were already listening.</p>

<div class="stats">{tile_html}</div>

<section>
<div class="kicker">timing</div>
<h2>How fast the engagement arrives</h2>
<p>Each line is the mean cumulative curve for one archetype, as a share of
that post's final total inside the observation horizon. A <strong>standard</strong>
post is essentially finished within hours. The interesting archetypes are the
ones that are still climbing when everything else has flattened.</p>
<div class="figure">{figs['cumulative']}</div>
</section>

<section>
<div class="kicker">audience</div>
<h2>Who is actually showing up</h2>
<p>For every engagement we can ask whether that account already followed the
author at the moment it engaged, because <code>follows</code> carries its own
creation timestamp. A line that <strong>falls</strong> over time is the
signature of distribution beyond the author's own audience: the followers come
first, strangers arrive later.</p>
<div class="figure">{figs['in_network']}</div>
<p class="muted">Unfollows are invisible in a snapshot — we only hold surviving
follow edges — so anyone who followed, engaged, then left is counted as
out-of-network. Every number on this chart is a floor on in-network share.</p>
</section>

<section>
<div class="kicker">mix</div>
<h2>What kind of engagement it is</h2>
<p>Composition separates categories that timing alone cannot. A pile-on and a
conversation can accumulate at the same speed and mean opposite things.</p>
<div class="figure">{figs['mix']}</div>
</section>

<section>
<div class="kicker">discovered shapes</div>
<h2>What the data says without being told</h2>
<p>K-Spectral Centroid clustering on the arrival-rate curves, which is
scale-invariant by construction — a 50-like post and a 50,000-like post with
the same shape land in the same cluster. These centroids are found without
reference to the rules below.</p>
<div class="figure">{figs['shapes']}</div>
<p>The cross-tab checks that the named archetypes track the shapes the data
actually contains. Rows are discovered shapes, normalized across the row.</p>
<div class="figure">{figs['crosstab']}</div>
</section>

<section>
<div class="kicker">taxonomy</div>
<h2>The archetypes</h2>
<table><thead><tr><th>archetype</th><th class="num">posts</th>
<th class="num">share</th><th class="num">median engagements</th>
<th class="num">mean t50</th><th class="num">mean in-network</th></tr></thead>
<tbody>{pop_rows}</tbody></table>
</section>

<section>
<div class="kicker">examples</div>
<h2>Five of each, from the last {c['cohort_days']} days</h2>
<p>Picked closest to their archetype's median on the features the rules key
off, restricted to the upper half of the archetype by engagement.</p>
{_examples_html(examples, present)}
</section>

{_authenticity_html(sidecar, figs, flagged, link_flagged)}

<div class="caveat">
<h4>What this measurement cannot see</h4>
<ul>
<li><strong>No impressions.</strong> There is no view count in the snapshot, so
algorithmic distribution is inferred from the out-of-network wave, never
observed directly.</li>
<li><strong>Unfollows are invisible.</strong> In-network share is a floor.</li>
<li><strong>Engagement floor.</strong> Only posts with at least
{c['min_engagement']} engagements are analyzed —
{c['cohort_eligible']:,} qualified out of the full corpus. This taxonomy
describes the visible tail, not the median post, which gets almost nothing.</li>
<li><strong>Fixed horizon.</strong> Every post is observed for exactly
{c['horizon_hours']}h, so anything still growing at the horizon is truncated
by construction. Sleepers slower than that are counted as evergreen.</li>
<li><strong>The authenticity score is relative, not absolute.</strong> Signals
are combined by percentile rank <em>within this cohort</em>, so a high score
means "unusual compared with the other posts analyzed here" and never "this
post was boosted with probability p". A cohort with no coordination in it
still produces a top decile.</li>
<li><strong>Rules, not ground truth.</strong> The archetype labels are
thresholds over features, chosen and reviewable. The
{unclassified_pct:.0f}% unclassified bucket is the honest measure of what the
taxonomy does not yet describe.</li>
</ul>
</div>

<footer>
Built {built_at_utc()} from <code>snapshot/{snapshot_date}/snapshot.duckdb</code>.
Cohort: posts created {c['cohort_lo']} → {c['cohort_hi']}, each observed for
{c['horizon_hours']}h. Timestamps decode from record rkey TIDs.
</footer>
</div></body></html>"""


# --------------------------------------------------------------------------
# entrypoint
# --------------------------------------------------------------------------

def run(
    con,
    snapshot_date: str,
    *,
    cohort_days: int = 30,
    horizon_hours: int = 168,
    min_engagement: int = 50,
    max_posts: int = 150_000,
    n_bins: int = 24,
    n_shapes: int = 6,
    root_posts_only: bool = True,
    examples_per_archetype: int = 5,
    seed: int = 0,
    hydrate_urls: bool = True,
    thresholds: dict | None = None,
    authenticity: bool = True,
    authenticity_signals: list | None = None,
    authenticity_weights: dict | None = None,
    link_flagged_examples: bool = True,
    log: bool = True,
) -> tuple[bytes, dict]:
    """Build the engagement-archetype report.

    Args:
      cohort_days: length of the post-creation window to sample from.
      horizon_hours: observation horizon applied identically to every post.
        The cohort ends one full horizon before the data does, so no post is
        right-censored relative to another. Raising this past ~720 trades
        cohort recency for the ability to see slow sleepers.
      min_engagement: floor on total engagement for a post to be analyzed.
      max_posts: reservoir-sample down to this many posts if the cohort is
        larger, so the event extraction stays bounded.
      n_shapes: number of K-Spectral Centroid clusters for the shape axis.
      authenticity: compute the inauthentic-amplification axis. Runs as a
        second pass over the temp tables the lifeline extraction already
        built, so it costs extra query time but no extra extraction.
      authenticity_signals: explicit signal allowlist; None uses every
        signal marked default-enabled in `authenticity.SIGNALS`.
      authenticity_weights: per-signal weight overrides, renormalized.
      link_flagged_examples: link out to the posts scoring highest on the
        authenticity axis, as every other example table does. On by
        default. The score is a within-cohort percentile over correlational
        proxies and observes no purchase, so the section labels these as
        ranked candidates for inspection rather than findings; set False to
        publish the rates and shapes without the links.
    """
    import numpy as np

    install_template()
    th = dict(RULE_THRESHOLDS)
    if thresholds:
        th.update(thresholds)
    t_start = time.time()

    def say(msg: str) -> None:
        if log:
            print(f"=== {msg} ===", flush=True)

    cut, cohort_lo, cohort_hi = _bounds(con, snapshot_date, cohort_days, horizon_hours)
    say(f"window cut {cut}; cohort {cohort_lo} -> {cohort_hi} "
        f"({cohort_days}d cohort, {horizon_hours}h horizon)")

    n_eligible, n_cohort = _build_cohort(
        con, cohort_lo, cohort_hi, min_engagement=min_engagement,
        max_posts=max_posts, root_posts_only=root_posts_only, seed=seed, say=say,
    )
    n_events = _build_events(con, horizon_hours, say)
    n_pairs, n_edges = _build_follow_edges(con, say)
    _build_bins(con, n_bins, horizon_hours, say)
    _build_scalar_features(con, say)

    uri_ids, cum, counts, counts_in, rate, per_ch, feats, labels = _assemble(
        con, n_bins, horizon_hours, th, say)

    # Shape clustering runs on the per-log-bin counts for the same reason
    # wave detection does: in log time a late second wave is a comparable
    # hump, where on a per-second rate it is a rounding error. KSC is
    # scale-invariant, so the raw counts need no normalization. Guard k
    # against tiny cohorts so synthetic fixtures and dry-run slices work.
    k = max(2, min(n_shapes, len(feats)))
    shape_labels, centroids = _ksc(counts, k, seed=seed)
    shape_sizes = {j: int((shape_labels == j).sum()) for j in range(k)}
    shape_rank = {j: r for r, j in enumerate(
        sorted(range(k), key=lambda j: -shape_sizes.get(j, 0)))}
    say(f"KSC shapes: " + ", ".join(
        f"{shape_rank[j] + 1}:{shape_sizes[j]:,}" for j in sorted(shape_rank, key=shape_rank.get)))

    # Fourth axis. Deliberately a score attached to every post rather than
    # an archetype of its own: a post is routinely a genuine sleeper hit
    # *and* partly amplified, and a mutually exclusive bucket would force a
    # wrong answer on exactly the common case.
    auth_scores, auth_meta = {}, None
    if authenticity:
        from . import authenticity as auth_mod
        auth_scores, auth_meta = auth_mod.attach(
            con,
            {"cohort_days": cohort_days, "horizon_hours": horizon_hours},
            enabled=authenticity_signals,
            weights=authenticity_weights,
            log=log,
        )
        for f in feats:
            hit = auth_scores.get(f["uri_id"])
            f["auth_score"] = hit["score"] if hit else None

    present = [a for a in ARCHETYPE_ORDER if a in set(labels)]
    xs = _bin_midpoints_hours(n_bins, horizon_hours)

    # --- sidecar ----------------------------------------------------------
    arch_stats = {}
    for arch in present:
        sel = [i for i, l in enumerate(labels) if l == arch]
        arch_stats[arch] = {
            "n": len(sel),
            "share": len(sel) / max(len(labels), 1),
            "median_engagement": float(np.median([feats[i]["n_total"] for i in sel])),
            "mean_t50_h": float(np.mean([feats[i]["t50_h"] for i in sel])),
            "mean_late24": float(np.mean([feats[i]["late24"] for i in sel])),
            "mean_in_network": float(np.mean([feats[i]["in_network_share"] for i in sel])),
            "mean_oon_delta": float(np.mean([feats[i]["oon_delta"] for i in sel])),
            "mean_mix": {
                ch: float(np.mean([feats[i][f"{ch}_share"] for i in sel]))
                for ch in ("like", "repost", "reply", "quote")
            },
        }

    examples = _fetch_examples(con, feats, labels, examples_per_archetype,
                               hydrate_urls, say)

    # --- authenticity summary + flagged rows ------------------------------
    auth_summary, flagged = None, []
    if auth_meta and auth_meta.get("signals_run"):
        scored = [(i, f["auth_score"]) for i, f in enumerate(feats)
                  if f.get("auth_score") is not None]
        scored.sort(key=lambda t: -t[1])
        fam_w: dict[str, float] = {}
        for s in auth_meta["signals_run"]:
            fam_w[s["family"]] = fam_w.get(s["family"], 0.0) + s["weight"]
        vals = np.array([v for _i, v in scored]) if scored else np.array([0.0])
        cut90 = float(np.quantile(vals, 0.9)) if len(vals) else 0.0
        auth_summary = {
            **auth_meta,
            "family_weights": {k: round(v, 4) for k, v in fam_w.items()},
            "posts_scored": len(scored),
            "median_score": float(np.median(vals)),
            "p90_score": cut90,
            "mean_score_by_archetype": {
                a: float(np.mean([feats[i]["auth_score"]
                                  for i, l in enumerate(labels)
                                  if l == a
                                  and feats[i].get("auth_score") is not None]
                                 or [0.0]))
                for a in present
            },
        }
        # The ten highest-ranked posts, linked like every other example
        # table unless the caller turns links off. These are candidates for
        # inspection: the score is a within-cohort percentile over
        # correlational proxies and none of its inputs observes a purchase.
        for rank, (i, sc_val) in enumerate(scored[:10], start=1):
            row = {
                "rank": rank,
                "score": sc_val,
                "uri_id": feats[i]["uri_id"],
                "n_total": feats[i]["n_total"],
                "archetype": labels[i],
                "in_network_share": feats[i]["in_network_share"],
                **{k: v for k, v in (auth_scores.get(feats[i]["uri_id"]) or {}).items()
                   if k != "score"},
            }
            flagged.append(row)
        if link_flagged_examples and hydrate_urls:
            _hydrate_urls(con, flagged, say)
        # Carried in the sidecar too, so the ranking is reviewable without
        # re-running: per-signal percentiles show *why* each post ranked.
        auth_summary["flagged"] = flagged

    sidecar = {
        "snapshot_date": snapshot_date,
        "cohort": {
            "cohort_days": cohort_days,
            "horizon_hours": horizon_hours,
            "min_engagement": min_engagement,
            "root_posts_only": root_posts_only,
            "window_cut": cut,
            "cohort_lo": cohort_lo,
            "cohort_hi": cohort_hi,
            "cohort_eligible": n_eligible,
            "posts_sampled": n_cohort,
            "posts_analyzed": len(feats),
            "events": n_events,
            "n_bins": n_bins,
        },
        "audience": {
            "engager_author_pairs": n_pairs,
            "pairs_with_follow_edge": n_edges,
            "in_network_share": float(np.mean(
                [f["in_network_share"] for f in feats])),
            "mean_oon_delta": float(np.mean([f["oon_delta"] for f in feats])),
        },
        "shapes": {
            "k": k,
            "sizes": {str(shape_rank[j] + 1): shape_sizes[j] for j in range(k)},
        },
        "thresholds": th,
        # Deciles of every feature the decision list keys off. The thresholds
        # in RULE_THRESHOLDS are only defensible against the distribution
        # they actually face, and that distribution is not knowable from a
        # synthetic fixture — the first real run had `pile_on` at 3 posts in
        # 2,000 because the reply-share cut was set from invented data.
        # Emitting the quantiles alongside the thresholds makes the next
        # recalibration a lookup rather than another guess.
        "feature_distributions": {
            f: {q: float(np.quantile([ft[f] for ft in feats], v))
                for q, v in (("p10", .10), ("p25", .25), ("p50", .50),
                             ("p75", .75), ("p90", .90), ("p99", .99))}
            for f in ("t50_h", "t90_h", "late24", "late72", "burst_share",
                      "reignition", "n_waves", "like_share", "repost_share",
                      "reply_share", "quote_share", "reply_lag_h",
                      "reply_outperform", "oon_delta", "in_network_share")
        },
        "archetypes": arch_stats,
        "authenticity": auth_summary,
        "examples": {
            a: [{k2: v for k2, v in e.items()
                 if k2 in ("uri_id", "url", "n_total", "t50_h", "late24",
                           "in_network_share", "reply_outperform")}
                for e in exs]
            for a, exs in examples.items()
        },
        "elapsed_s": round(time.time() - t_start, 1),
    }

    xr = _x_range(counts, xs)
    figs = {
        "cumulative": fig_html(
            _fig_cumulative(cum, labels, xs, present, xr), "lf-cum"),
        "in_network": fig_html(
            _fig_in_network(counts, counts_in, labels, xs, present, xrange=xr),
            "lf-inet"),
        "mix": fig_html(_fig_mix(feats, labels, present), "lf-mix"),
        "shapes": fig_html(_fig_shapes(centroids, shape_sizes, xs, xr), "lf-shapes"),
        "crosstab": fig_html(
            _fig_crosstab(shape_labels, labels, present, k, shape_rank), "lf-xtab"),
    }
    if auth_summary:
        figs["authenticity"] = fig_html(
            _fig_authenticity(feats, labels, present), "lf-auth")
    html = _render_html(snapshot_date=snapshot_date, sidecar=sidecar,
                        figs=figs, examples=examples, present=present,
                        flagged=flagged, link_flagged=link_flagged_examples)
    say(f"done in {sidecar['elapsed_s']}s")
    return html.encode("utf-8"), sidecar
