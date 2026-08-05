"""One number: how far a post's like arrivals depart from organic decay.

This is deliberately a clean-room module. It shares no code with
`lifelines.py` and computes no archetypes, no clusters, and no composite
axes — the entire output is two numbers per post plus the plots needed to
decide whether those two numbers mean anything.

The idea in three lines
-----------------------
A post's likes, expressed as delays since the post was created, are a
distribution — not a curve. If likes accumulate on the textbook log
asymptote

    N(t) = A * ln(1 + t/tau)                 =>  rate = A / (t + tau)

then substituting u = ln t gives the density in log-time

    dN/du = rate * t = A * t / (t + tau)  ->  A   for t >> tau

so *the log asymptote is exactly "likes are uniform in log-time."* Equal
likes per doubling of age: as many between hour 1 and 2 as between 2 and
4 as between 4 and 8. Organic accumulation is a flat histogram in
log-time; irregular accumulation is a lumpy one.

That turns the whole question into one distance. Bin log10(delay) into B
equal-width bins over a fixed window; then for a post with cumulative
in-bin counts c_k out of n likes,

    D = max_k | c_k/n  -  k/B |

which is the Kolmogorov-Smirnov distance to log-uniform, evaluated at the
bin edges. Under the null the expected cumulative share at edge k is
exactly k/B, so the reference needs no fitting and the statistic has no
tunable thresholds. It reads in plain English: "at its most extreme
moment this post was D*100 percentage points off schedule."

Why the raw D is not yet the answer
-----------------------------------
Two corrections, both of which this module applies:

1. D shrinks like 1/sqrt(n), so a 50-like post looks irregular by luck
   alone. We calibrate by simulation — draw n likes from the null,
   record D, take the 95th percentile — and report

       score = D / D_95(n)

   so score > 1 means "lumpier than 95% of purely organic posts of this
   size." The calibration is a multinomial draw over the same bins, so
   discretization is baked into the null rather than approximated away.

2. The theoretical null is flat only above the knee tau. Every real post
   is suppressed below its own tau, so scoring against a flat line
   charges every post for its ignition ramp. We therefore compute D
   against *two* references and report both:

     - `loguniform`: the flat theoretical null. What "organic" means a
       priori.
     - `empirical`: the cohort's own mean per-post CDF. Absorbs the knee
       and any diurnal envelope, so it answers the narrower question
       "is this post unlike its peers."

   Which one to trust is an empirical question, and the pooled-density
   figure is what settles it. That figure is the point of this module as
   much as the score is.

Nuisance axes, kept separate on purpose
---------------------------------------
`t50_h` (median delay) is *not* folded into the score. A fast post and a
slow-burn post are both organic; they differ in tau, not in regularity.
Collapsing speed and lumpiness into one number is what forces a
taxonomy later, so speed stays a control variable you condition on, and
the report plots score against it to prove the score is not merely
re-measuring speed.

Same for hour-of-day. Likes have a 24-hour clock, so a post made at 3am
local gets a dead patch and then a morning bump: organic, but lumpy in
log-time. This is the single most likely false positive, so the report
plots score against posting hour. If that panel is not flat, the
horizon is too long or the null needs a diurnal envelope.
"""

from __future__ import annotations

import math
import time

from .common import (
    SHARED_CSS,
    BRAND,
    built_at_utc,
    fig_html,
    fmt_int,
    install_template,
    plotlyjs_inline,
)

# Delays below this are excluded from the shape measurement. The log
# asymptote says nothing about the ignition region below tau, and the
# first seconds are also where clock skew between PDSes concentrates. The
# excluded fraction is retained per post as `sub_floor_share` — it is a
# feature in its own right, not a silent drop.
FLOOR_SECONDS = 60

# Posts below this many in-window likes have no measurable shape: the
# noise floor on D swamps the signal. Kept as a hard filter rather than a
# weight so every reported number describes a post we can actually see.
MIN_LIKES = 50

NULLS = ("loguniform", "empirical")


# --------------------------------------------------------------------------
# cohort bounds
# --------------------------------------------------------------------------

def _bounds(con, snapshot_date: str, cohort_days: int, horizon_hours: int):
    """Resolve (cut, cohort_lo, cohort_hi, ends).

    Every post must get the same observation horizon or the shapes are
    not comparable — a post from the final hours of the window would look
    like it died young when in truth we stopped watching. So the cohort
    ends one full horizon before observation does.

    The subtlety is what "observation ends" means. It is *not* the last
    post: the two streams are cut independently, and on the 2026-07-31
    snapshot the last post lands 2026-07-31 23:00 while the last like
    lands 2026-07-30 21:05 — 26 hours earlier. Anchoring on the last post
    would hand the newest posts in the cohort a horizon in which no like
    could possibly have been recorded, manufacturing exactly the
    right-censoring this function exists to prevent. So `cut` is the
    earlier of the two stream ends.

    Both ends are clamped to a day past the nominal snapshot date because
    a handful of TIDs decode to 2118 and beyond (a known defect of
    deriving times from rkeys), and one of them would otherwise drag the
    window into the future and silently empty the cohort.
    """
    from datetime import timedelta

    guard = f"TIMESTAMP '{snapshot_date} 00:00:00' + INTERVAL 1 DAY"
    ends = {}
    for table in ("posts", "likes"):
        row = con.execute(
            f"SELECT MAX(created_at) FROM {table} WHERE created_at <= {guard}"
        ).fetchone()
        if not row or row[0] is None:
            raise SystemExit(f"no rows in {table} — cannot build a cohort")
        ends[table] = row[0]

    cut = min(ends["posts"], ends["likes"])
    cohort_hi = cut - timedelta(hours=horizon_hours)
    cohort_lo = cohort_hi - timedelta(days=cohort_days)
    return cut, cohort_lo, cohort_hi, ends


def _columns(con, table: str) -> set[str]:
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
        [table],
    ).fetchall()
    return {r[0] for r in rows}


# --------------------------------------------------------------------------
# extraction
# --------------------------------------------------------------------------

def _build_cohort(con, cohort_lo, cohort_hi, *, min_likes, max_posts, seed, say):
    """Materialize `og_cohort`: root posts with enough likes to have a shape.

    Filter order matters at this scale. `post_aggs` is one row per post
    (271M on the 2026-07-31 snapshot) but the like floor is brutally
    selective, so it is applied first and the survivors joined against
    the date-filtered `posts` rather than the other way round.

    The floor here uses the *aggregate* like count as a cheap prefilter.
    It is an upper bound on the in-window count, so the exact in-window
    floor is re-applied after delays are extracted.
    """
    post_cols = _columns(con, "posts")

    conds = ["p.created_at >= ?", "p.created_at < ?"]
    if "source" in post_cols:
        # target_only posts are known only via inbound links; their own
        # created_at is the least trustworthy field in the snapshot.
        conds.append("p.source = 'record'")
    if "reply_parent_uri_id" in post_cols:
        # Replies inherit a thread's audience and so follow different
        # timing rules. Mixing them in blurs the reference curve.
        conds.append("p.reply_parent_uri_id IS NULL")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE og_cohort_all AS
        WITH eligible AS (
          SELECT uri_id, likes FROM post_aggs WHERE likes >= {int(min_likes)}
        )
        SELECT p.uri_id, p.author_did_id, p.created_at, e.likes AS agg_likes
        FROM eligible e
        JOIN posts p USING (uri_id)
        WHERE {' AND '.join(conds)}
    """, [cohort_lo, cohort_hi])

    n_all = con.execute("SELECT COUNT(*) FROM og_cohort_all").fetchone()[0]
    if n_all == 0:
        raise SystemExit(
            "cohort is empty — check min_likes / cohort_days against the "
            "snapshot's activity window"
        )

    if n_all > max_posts:
        say(f"cohort {n_all:,} posts -> reservoir sample {max_posts:,}")
        con.execute(
            "CREATE OR REPLACE TEMP TABLE og_cohort AS SELECT * FROM og_cohort_all "
            f"USING SAMPLE {int(max_posts)} ROWS (reservoir, {int(seed)})"
        )
    else:
        con.execute(
            "CREATE OR REPLACE TEMP TABLE og_cohort AS SELECT * FROM og_cohort_all"
        )
    n_cohort = con.execute("SELECT COUNT(*) FROM og_cohort").fetchone()[0]
    say(f"cohort: {n_all:,} eligible -> {n_cohort:,} analyzed")
    return n_all, n_cohort


def _build_delays(con, horizon_hours: int, say):
    """Materialize `og_delay`: one row per in-horizon like, with delay.

    Self-likes are excluded — an author liking their own post is
    authorship, not reception. The `dt >= 0` guard drops likes whose TID
    predates the post's own, which happens with clock skew across PDSes.
    """
    h = int(horizon_hours)
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE og_delay AS
        SELECT c.uri_id,
               date_diff('second', c.created_at, l.created_at) AS dt,
               l.created_at AS ts
        FROM og_cohort c
        JOIN likes l ON l.subject_uri_id = c.uri_id
        WHERE l.created_at >= c.created_at
          AND l.created_at < c.created_at + INTERVAL {h} HOUR
          AND l.actor_did_id <> c.author_did_id
    """)
    n = con.execute("SELECT COUNT(*) FROM og_delay").fetchone()[0]
    say(f"delays: {n:,} likes extracted ({time.time() - t0:.0f}s)")
    return n


def _build_features(con, n_bins: int, horizon_hours: int, min_likes: int, say):
    """Per-post binned counts plus the nuisance/diagnostic scalars.

    Binning happens in SQL so only `n_posts * n_bins` values cross into
    Python, not one row per like. Bin index is floor over log10(dt) on a
    fixed window [FLOOR_SECONDS, horizon]; equal width in log space is
    what makes the flat null exact.
    """
    lo = math.log10(FLOOR_SECONDS)
    hi = math.log10(horizon_hours * 3600.0)
    width = (hi - lo) / n_bins

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE og_bin AS
        SELECT uri_id,
               LEAST({n_bins - 1},
                     CAST(FLOOR((LOG10(GREATEST(dt, 1)) - {lo}) / {width}) AS INT)
               ) AS b,
               COUNT(*) AS c
        FROM og_delay
        WHERE dt >= {FLOOR_SECONDS}
        GROUP BY 1, 2
    """)

    # Scalars. `t50_h` and `tie_share` come from the *in-window* likes so
    # they describe the same sample the shape score does. `sub_floor_share`
    # and `hour_utc` describe what the shape score deliberately ignores.
    # All three per-post aggregates are computed as independent grouped
    # scans and then joined on uri_id. A correlated subquery for the
    # sub-floor count would be the obvious way to write this and would
    # quietly become the slowest part of the run at 60k posts.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE og_scalar AS
        WITH inw AS (
          SELECT uri_id,
                 COUNT(*) AS n_in,
                 QUANTILE_CONT(dt, 0.5) / 3600.0 AS t50_h,
                 QUANTILE_CONT(dt, 0.9) / 3600.0 AS t90_h
          FROM og_delay WHERE dt >= {FLOOR_SECONDS} GROUP BY 1
        ),
        ties AS (
          -- Likes sharing an identical second are a firehose/backfill
          -- artifact, not a burst of human attention. Tracked so a post
          -- whose "lumpiness" is really write batching can be spotted.
          SELECT uri_id, MAX(k) AS max_tie FROM (
            SELECT uri_id, ts, COUNT(*) AS k
            FROM og_delay WHERE dt >= {FLOOR_SECONDS} GROUP BY 1, 2
          ) GROUP BY 1
        ),
        below AS (
          SELECT uri_id, COUNT(*) AS n_below
          FROM og_delay WHERE dt < {FLOOR_SECONDS} GROUP BY 1
        )
        SELECT c.uri_id,
               c.created_at,
               EXTRACT(hour FROM c.created_at) AS hour_utc,
               i.n_in,
               i.t50_h,
               i.t90_h,
               COALESCE(t.max_tie, 0) / i.n_in::DOUBLE AS tie_share,
               COALESCE(b.n_below, 0) / i.n_in::DOUBLE AS sub_floor_share
        FROM og_cohort c
        JOIN inw i USING (uri_id)
        LEFT JOIN ties t USING (uri_id)
        LEFT JOIN below b USING (uri_id)
        WHERE i.n_in >= {int(min_likes)}
    """)
    n = con.execute("SELECT COUNT(*) FROM og_scalar").fetchone()[0]
    say(f"features: {n:,} posts clear the {min_likes}-like in-window floor")
    if n == 0:
        raise SystemExit("no post clears the in-window like floor")
    return n


def _assemble(con, n_bins: int, say):
    """Pull the per-post bin matrix and scalars into numpy.

    Returns (uri_ids, counts[n_posts, n_bins], scalars dict-of-arrays).
    """
    import numpy as np

    rows = con.execute("""
        SELECT uri_id, created_at, hour_utc, n_in, t50_h, t90_h,
               tie_share, sub_floor_share
        FROM og_scalar ORDER BY uri_id
    """).fetchall()
    # Kept as plain Python ints, not a numpy integer array. Real uri_ids
    # are xxhash3-64 values, so they fill the full 64-bit range and land
    # in BIGINT as negatives about half the time — casting those to
    # uint64 raises, and casting to int64 in the other direction would
    # too. Nothing here needs them to be numeric.
    uri_ids = [int(r[0]) for r in rows]
    index = {u: i for i, u in enumerate(uri_ids)}

    counts = np.zeros((len(uri_ids), n_bins), dtype=np.float64)
    for uri_id, b, c in con.execute(
        "SELECT b.uri_id, b.b, b.c FROM og_bin b "
        "SEMI JOIN og_scalar s ON s.uri_id = b.uri_id"
    ).fetchall():
        i = index.get(int(uri_id))
        if i is not None:
            counts[i, int(b)] = c

    scalars = {
        "created_at": [r[1] for r in rows],
        "hour_utc": np.array([r[2] for r in rows], dtype=np.float64),
        "n": np.array([r[3] for r in rows], dtype=np.float64),
        "t50_h": np.array([r[4] for r in rows], dtype=np.float64),
        "t90_h": np.array([r[5] for r in rows], dtype=np.float64),
        "tie_share": np.array([r[6] for r in rows], dtype=np.float64),
        "sub_floor_share": np.array([r[7] for r in rows], dtype=np.float64),
    }
    say(f"assembled {len(uri_ids):,} x {n_bins} bin matrix")
    return uri_ids, counts, scalars


# --------------------------------------------------------------------------
# the statistic
# --------------------------------------------------------------------------

def _ks_to_reference(counts, ref_cdf):
    """Signed and unsigned KS distance from each row's CDF to `ref_cdf`.

    `counts` is [n_posts, n_bins] of per-log-bin like counts; `ref_cdf` is
    the reference cumulative share at each of the n_bins right-hand edges
    (so ref_cdf[-1] == 1).

    Returns (d_unsigned, d_signed). The signed value is the deviation at
    the point of maximum absolute deviation, so its sign says *which way*:
    positive means the post ran ahead of schedule (mass arrived earlier
    than the reference), negative means it ran behind.
    """
    import numpy as np

    n = counts.sum(axis=1, keepdims=True)
    cdf = np.cumsum(counts, axis=1) / np.maximum(n, 1)
    dev = cdf - ref_cdf[None, :]
    k = np.argmax(np.abs(dev), axis=1)
    d_signed = dev[np.arange(len(dev)), k]
    return np.abs(d_signed), d_signed


def _loguniform_cdf(n_bins: int):
    import numpy as np
    return np.arange(1, n_bins + 1, dtype=np.float64) / n_bins


def _empirical_cdf(counts):
    """The cohort's mean per-post CDF — every post weighted equally.

    Deliberately not the pooled CDF over all likes: that would let the
    handful of 50k-like posts define what "typical" means for the
    hundred-like majority.
    """
    import numpy as np

    n = counts.sum(axis=1, keepdims=True)
    return (np.cumsum(counts, axis=1) / np.maximum(n, 1)).mean(axis=0)


def _calibrate(ref_pmf, n_grid, *, n_sims: int, pct: float, seed: int, say):
    """Noise floor for D as a function of n, by simulation under the null.

    For each n in `n_grid`, draw `n_sims` multinomial samples of n likes
    over the reference bin probabilities, compute D exactly as the real
    path does, and take the `pct` percentile. Simulating the multinomial
    rather than using the asymptotic KS formula means the calibration
    carries the same discretization the measurement has, so the two are
    directly comparable at small n where it matters most.
    """
    import numpy as np

    rng = np.random.default_rng(seed)
    ref_cdf = np.cumsum(ref_pmf)
    ref_cdf = ref_cdf / ref_cdf[-1]
    out = []
    for n in n_grid:
        draws = rng.multinomial(int(n), ref_pmf, size=n_sims).astype(np.float64)
        d, _ = _ks_to_reference(draws, ref_cdf)
        out.append(float(np.percentile(d, pct)))
    say(f"calibrated D_{pct:g} over n in [{n_grid[0]}, {n_grid[-1]}]: "
        f"{out[0]:.3f} -> {out[-1]:.3f}")
    return np.asarray(out, dtype=np.float64)


def _score(d, n, n_grid, d_crit):
    """score = D / D_crit(n), interpolated in log n and clamped at the ends."""
    import numpy as np

    crit = np.interp(np.log(np.maximum(n, 1)), np.log(n_grid), d_crit)
    return d / np.maximum(crit, 1e-9)


def _n_grid(n_min: int, n_max: float, *, points: int = 14):
    import numpy as np
    hi = max(float(n_max), n_min * 2.0)
    return np.unique(np.round(np.geomspace(n_min, hi, points)).astype(int))


# --------------------------------------------------------------------------
# figures
# --------------------------------------------------------------------------

def _bin_edges(n_bins: int, horizon_hours: int):
    """(left_edges_seconds, right_edges_seconds, centers_log10) for the bins."""
    import numpy as np

    lo = math.log10(FLOOR_SECONDS)
    hi = math.log10(horizon_hours * 3600.0)
    e = np.linspace(lo, hi, n_bins + 1)
    return 10 ** e[:-1], 10 ** e[1:], (e[:-1] + e[1:]) / 2


def _hours_label(seconds):
    s = float(seconds)
    if s < 3600:
        return f"{s / 60:.0f}m"
    if s < 86400:
        return f"{s / 3600:.0f}h"
    return f"{s / 86400:.1f}d"


def _fig_pooled_density(counts, n_bins, horizon_hours):
    """Increment zero: is the mean post's log-time density actually flat?

    Two series, because they answer different questions. The equal-weight
    mean is the reference the score uses. The like-weighted pooled
    density is what the firehose looks like, and if the two diverge
    sharply then post size and post shape are entangled.
    """
    import numpy as np
    import plotly.graph_objects as go

    _, _, ctr = _bin_edges(n_bins, horizon_hours)
    n = counts.sum(axis=1, keepdims=True)
    per_post = (counts / np.maximum(n, 1)).mean(axis=0)
    pooled = counts.sum(axis=0) / counts.sum()

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=ctr, y=per_post, name="mean post (equal weight)",
        marker_color=BRAND, opacity=0.85,
        hovertemplate="%{y:.3f} of a post's likes<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=ctr, y=pooled, name="pooled over all likes",
        mode="lines+markers", line=dict(color="#ff5d8f", width=2),
    ))
    fig.add_trace(go.Scatter(
        x=ctr, y=np.full(n_bins, 1.0 / n_bins), name="organic null (flat)",
        mode="lines", line=dict(color="#1d2433", width=2, dash="dash"),
    ))
    ticks = list(range(int(math.floor(ctr[0])), int(math.ceil(ctr[-1])) + 1))
    fig.update_layout(
        template="bsky", height=420, bargap=0.06,
        title="Share of a post's likes per log-time bin",
        xaxis=dict(title="age of post when the like arrived",
                   tickvals=ticks,
                   ticktext=[_hours_label(10 ** t) for t in ticks]),
        yaxis=dict(title="share of the post's likes", rangemode="tozero"),
        legend=dict(orientation="h", y=1.12, x=0),
    )
    return fig


def _fig_score_hist(scores, label):
    import numpy as np
    import plotly.graph_objects as go

    s = np.clip(scores, 0, 4)
    fig = go.Figure()
    fig.add_trace(go.Histogram(
        x=s, nbinsx=80, marker_color=BRAND, opacity=0.85,
        hovertemplate="score %{x:.2f}: %{y} posts<extra></extra>",
    ))
    fig.add_vline(x=1.0, line=dict(color="#1d2433", width=2, dash="dash"),
                  annotation_text="95th pct of pure-organic", annotation_position="top right")
    over = float((scores > 1.0).mean())
    fig.update_layout(
        template="bsky", height=380,
        title=f"Irregularity score vs {label} null — {over * 100:.1f}% above 1.0",
        xaxis=dict(title="score = D / D₉₅(n)   (clipped at 4)"),
        yaxis=dict(title="posts"),
    )
    return fig


def _fig_deciles(counts, scores, n_bins, horizon_hours, *, per_decile, seed):
    """The eyeball test. If the top decile is not obviously weird and the
    bottom decile is not obviously boring, the score is wrong.
    """
    import numpy as np
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    rng = np.random.default_rng(seed)
    _, _, ctr = _bin_edges(n_bins, horizon_hours)
    order = np.argsort(scores)
    chunks = np.array_split(order, 10)

    fig = make_subplots(
        rows=2, cols=5, shared_xaxes=True, shared_yaxes=True,
        vertical_spacing=0.13, horizontal_spacing=0.03,
        subplot_titles=[
            f"decile {i + 1} · score {np.median(scores[c]):.2f}"
            for i, c in enumerate(chunks)
        ],
    )
    n_post = counts.sum(axis=1, keepdims=True)
    cdf = np.cumsum(counts, axis=1) / np.maximum(n_post, 1)
    for i, chunk in enumerate(chunks):
        r, c = divmod(i, 5)
        pick = rng.choice(chunk, size=min(per_decile, len(chunk)), replace=False)
        for j in pick:
            fig.add_trace(go.Scatter(
                x=ctr, y=cdf[j], mode="lines", showlegend=False,
                line=dict(color=BRAND, width=1), opacity=0.45,
                hoverinfo="skip",
            ), row=r + 1, col=c + 1)
        fig.add_trace(go.Scatter(
            x=[ctr[0], ctr[-1]], y=[1.0 / n_bins, 1.0], mode="lines",
            showlegend=False, line=dict(color="#1d2433", width=1.5, dash="dash"),
            hoverinfo="skip",
        ), row=r + 1, col=c + 1)

    ticks = list(range(int(math.floor(ctr[0])), int(math.ceil(ctr[-1])) + 1))
    fig.update_xaxes(tickvals=ticks, ticktext=[_hours_label(10 ** t) for t in ticks])
    fig.update_yaxes(range=[0, 1.02])
    for ann in fig.layout.annotations:
        ann.font.size = 11
    fig.update_layout(
        template="bsky", height=560,
        title=f"{per_decile} real posts per score decile — cumulative likes in log-time "
              "(dashed = organic null)",
        margin=dict(l=50, r=20, t=70, b=50),
    )
    return fig


def _fig_control(scores, values, *, title, xtitle, log_x=False, n_bucket=12):
    """Score against a nuisance variable, as bucketed median + IQR band.

    The point of these panels is falsification: if the score tracks
    posting speed or posting hour, it is measuring that instead of
    regularity, and no amount of downstream taxonomy will fix it.
    """
    import numpy as np
    import plotly.graph_objects as go

    v = np.asarray(values, dtype=np.float64)
    ok = np.isfinite(v) & np.isfinite(scores)
    if log_x:
        ok &= v > 0
    v, s = v[ok], scores[ok]
    if len(v) < n_bucket * 5:
        n_bucket = max(2, len(v) // 5)

    qs = np.quantile(v, np.linspace(0, 1, n_bucket + 1))
    qs = np.unique(qs)
    idx = np.clip(np.searchsorted(qs, v, side="right") - 1, 0, len(qs) - 2)

    xs, med, q1, q3 = [], [], [], []
    for b in range(len(qs) - 1):
        m = idx == b
        if m.sum() < 5:
            continue
        xs.append(float(np.median(v[m])))
        med.append(float(np.median(s[m])))
        q1.append(float(np.quantile(s[m], 0.25)))
        q3.append(float(np.quantile(s[m], 0.75)))

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=xs + xs[::-1], y=q3 + q1[::-1], fill="toself",
        fillcolor="rgba(0,133,255,0.13)", line=dict(width=0),
        name="IQR", hoverinfo="skip",
    ))
    fig.add_trace(go.Scatter(
        x=xs, y=med, mode="lines+markers", name="median score",
        line=dict(color=BRAND, width=2.5),
    ))
    fig.add_hline(y=1.0, line=dict(color="#1d2433", width=1.5, dash="dash"))
    fig.update_layout(
        template="bsky", height=360, title=title,
        xaxis=dict(title=xtitle, type="log" if log_x else "linear"),
        yaxis=dict(title="irregularity score", rangemode="tozero"),
        showlegend=False,
    )
    return fig


# --------------------------------------------------------------------------
# report
# --------------------------------------------------------------------------

def _render_html(*, snapshot_date, sidecar, figs) -> bytes:
    s = sidecar
    ref = s["reference"]["headline"]
    flat = s["flatness"]
    corr = s["controls"]

    def pct(x):
        return f"{x * 100:.1f}%"

    stats = [
        (fmt_int(s["cohort"]["n_analyzed"]), "posts measured",
         f"of {fmt_int(s['cohort']['n_eligible'])} eligible"),
        (fmt_int(s["cohort"]["n_likes"]), "likes placed",
         f"≥{s['cohort']['min_likes']} per post, in-window"),
        (f"{flat['mean_post_max_dev']:.3f}", "mean-post deviation from flat",
         "0 would mean the log null holds exactly"),
        (pct(s["scores"][ref]["share_above_1"]), "above the organic band",
         f"vs {ref} null"),
    ]
    stat_html = "".join(
        f'<div class="stat"><div class="v">{v}</div>'
        f'<div class="l">{l}</div><div class="sub">{sub}</div></div>'
        for v, l, sub in stats
    )

    verdict = (
        "flat enough that the theoretical null is usable"
        if flat["mean_post_max_dev"] < 0.10 else
        "far enough from flat that the empirical null is the honest reference"
    )
    speed_note = (
        "essentially flat — the score is not just re-measuring speed"
        if abs(corr["spearman_t50"]) < 0.15 else
        f"sloped (Spearman {corr['spearman_t50']:+.2f}) — the score is partly "
        "tracking how fast the post moved, which needs fixing before the "
        "score can be called an irregularity measure"
    )
    hour_note = (
        "flat — posting hour is not driving the score at this horizon"
        if corr["hour_spread"] < 0.15 else
        f"varies by {corr['hour_spread']:.2f} across posting hours — the "
        "diurnal clock is leaking into the score; shorten the horizon or give "
        "the null a diurnal envelope"
    )
    emp_note = (
        "The cohort has a dominant shared shape, so its mean curve is a "
        f"legitimate second reference — it flags "
        f"{pct(s['reference']['empirical_share_above_1'])} of posts, against "
        f"{pct(s['scores']['loguniform']['share_above_1'])} for the flat null. "
        "The gap between the two is the part of the flat null's verdict that "
        "was really about the ignition knee rather than about lumpiness."
        if s["reference"]["empirical_usable"] else
        "<strong>The empirical reference is not usable on this cohort.</strong> "
        f"It flags {pct(s['reference']['empirical_share_above_1'])} of posts, "
        "which does not mean they are all irregular — it means the cohort has "
        "no dominant shape, so its mean curve is a shape no post actually "
        "has and every post is far from it. Read the flat null here."
    )

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Organic vs irregular accumulation · {snapshot_date}</title>
<style>{SHARED_CSS}
.mono {{ font-variant-numeric: tabular-nums; }}
table.k {{ border-collapse: collapse; width: 100%; font-size: 14px; margin-top: 14px; }}
table.k th, table.k td {{ text-align: right; padding: 7px 10px;
  border-bottom: 1px solid var(--rule); }}
table.k th:first-child, table.k td:first-child {{ text-align: left; }}
table.k th {{ color: var(--muted); font-weight: 600; font-size: 12.5px;
  text-transform: uppercase; letter-spacing: 0.06em; }}
.note {{ background: white; border: 1px solid var(--rule); border-left: 3px solid {BRAND};
  border-radius: 0 8px 8px 0; padding: 14px 18px; margin: 22px 0; font-size: 14.5px;
  color: var(--muted); }}
.note strong {{ color: var(--ink); }}
</style>
<script>{plotlyjs_inline()}</script>
</head><body><div class="wrap">

<div class="eyebrow">Bluesky snapshot · {snapshot_date}</div>
<h1>Organic or <span class="accent">irregular</span>?</h1>
<p class="lede">One number per post. The textbook log asymptote is exactly
&ldquo;likes arrive uniformly in log-time&rdquo; — equal likes per doubling of
age — so organic accumulation is a flat histogram and irregular accumulation
is a lumpy one. Everything here is the distance between those two, plus the
checks that decide whether that distance means anything.</p>

<div class="stats">{stat_html}</div>

<section>
<div class="kicker">Increment zero</div>
<h2>Is the null even true?</h2>
<p>Before scoring a single post, the cohort's own average has to be checked
against the flat line. If the mean post's log-time density is not flat, then
scoring posts against flatness charges every one of them for the same
systematic gap.</p>
<div class="figure">{figs['pooled']}</div>
<div class="note">The mean post's largest cumulative deviation from flat is
<strong>{flat['mean_post_max_dev']:.3f}</strong> — {verdict}. The suppression
at the left edge is the ignition knee <em>&tau;</em>: below it the rate is
still climbing, so no post can look log-uniform there. That is why the score
is also computed against the cohort's own mean curve, which absorbs the knee.
</div>
</section>

<section>
<div class="kicker">The spectrum</div>
<h2>One number, calibrated for size</h2>
<p>For each post, <span class="mono">D = max<sub>k</sub> |c<sub>k</sub>/n
&minus; k/B|</span> over the cumulative share in each of the
{s['params']['n_bins']} log bins. Because <span class="mono">D</span> shrinks
like <span class="mono">1/&radic;n</span>, a small post looks irregular by
luck, so it is divided by the simulated 95th percentile of
<span class="mono">D</span> at that post's own <span class="mono">n</span>.
A score of 1.0 means &ldquo;as lumpy as the lumpiest 5% of purely organic
posts this size.&rdquo;</p>
<div class="figure">{figs['hist_loguniform']}</div>
<div class="figure">{figs['hist_empirical']}</div>
<div class="note">{emp_note}</div>
<table class="k">
<tr><th>null</th><th>median score</th><th>p90</th><th>p99</th>
    <th>share &gt; 1.0</th></tr>
{"".join(
    f"<tr><td>{k}</td><td>{v['median']:.2f}</td><td>{v['p90']:.2f}</td>"
    f"<td>{v['p99']:.2f}</td><td>{pct(v['share_above_1'])}</td></tr>"
    for k, v in s["scores"].items())}
</table>
</section>

<section>
<div class="kicker">The eyeball test</div>
<h2>Does the score sort posts the way a human would?</h2>
<p>Real cumulative curves, {s['params']['examples_per_decile']} sampled from
each score decile. This is the check that matters most: if the last panel is
not obviously strange and the first is not obviously dull, the number is not
measuring what it claims and nothing built on top of it will be worth
reading.</p>
<div class="figure">{figs['deciles']}</div>
</section>

<section>
<div class="kicker">Falsification</div>
<h2>Is it secretly measuring something else?</h2>
<p>Two things could masquerade as irregularity. <strong>Speed</strong>: a fast
post and a slow burn are both organic, differing in <em>&tau;</em>, not in
regularity — so the score must not track median delay.
<strong>Posting hour</strong>: likes follow a 24-hour clock, so a post made
while its audience sleeps gets a gap and then a bump. That is the most likely
false positive in the whole exercise.</p>
<div class="figure">{figs['ctrl_t50']}</div>
<div class="figure">{figs['ctrl_hour']}</div>
<div class="note">Against median delay the score is {speed_note}. Across
posting hour it is {hour_note}.</div>
</section>

<section>
<div class="kicker">Known artifacts</div>
<h2>What is still in the data</h2>
<p>Timestamps are decoded from record keys, which is why the cohort is clamped
to one day past the nominal snapshot date and delays below zero are dropped:
a small number of TIDs decode to 1970 or to 2118 and beyond. Likes arriving in
the first {FLOOR_SECONDS} seconds are excluded from the shape and tracked
separately — median <span class="mono">{pct(flat['median_sub_floor'])}</span>
of a post's likes. Identical-second like clusters, which are write batching
rather than attention, reach a median
<span class="mono">{pct(flat['median_tie_share'])}</span> of a post's likes.</p>
</section>

<footer>
Cohort: root posts created {s['cohort']['lo']} → {s['cohort']['hi']}, each
observed for a full {s['params']['horizon_hours']}h so no post is censored
relative to another. Observation closes at {s['cohort']['cut']} — the earlier
of the two stream ends (last post {s['cohort']['posts_end']}, last like
{s['cohort']['likes_end']}, {s['cohort']['stream_lag_hours']:.1f}h apart).
Built {built_at_utc()} · <code>analysis/organic.py</code> ·
{s['timing']['total_s']:.0f}s
</footer>
</div></body></html>""".encode()


# --------------------------------------------------------------------------
# entrypoint
# --------------------------------------------------------------------------

def run(
    con,
    snapshot_date: str,
    *,
    cohort_days: int = 14,
    horizon_hours: int = 24,
    min_likes: int = MIN_LIKES,
    max_posts: int = 60_000,
    n_bins: int = 18,
    examples_per_decile: int = 20,
    n_sims: int = 4_000,
    crit_pct: float = 95.0,
    headline_null: str = "loguniform",
    seed: int = 0,
    log: bool = True,
) -> tuple[bytes, dict]:
    """Build the organic-vs-irregular report.

    Args:
      cohort_days: length of the post-creation window sampled from.
      horizon_hours: identical observation horizon for every post. Kept at
        24 by default so one diurnal cycle cannot masquerade as
        irregularity; raise it only after the posting-hour panel is
        confirmed flat.
      n_bins: log-time bins between FLOOR_SECONDS and the horizon. More
        bins resolve narrower bursts but raise D's noise floor, which the
        calibration then has to absorb.
      n_sims: multinomial draws per grid point when calibrating D's noise
        floor. 4k is enough for a 95th percentile.
      headline_null: which reference drives the summary numbers. Both are
        always computed; the pooled-density figure is what justifies the
        choice.

    Returns (html_bytes, sidecar_dict). The sidecar carries the per-post
    table so scores can be joined back without re-running the extraction.
    """
    import numpy as np

    if headline_null not in NULLS:
        raise SystemExit(f"headline_null must be one of {NULLS}")

    install_template()
    t_start = time.time()

    def say(msg: str) -> None:
        if log:
            print(f"=== {msg} ===", flush=True)

    cut, lo, hi, ends = _bounds(con, snapshot_date, cohort_days, horizon_hours)
    say(f"stream ends: posts {ends['posts']}, likes {ends['likes']}")
    say(f"window cut {cut}; cohort {lo} -> {hi} "
        f"({cohort_days}d cohort, {horizon_hours}h horizon)")
    lag_h = abs((ends["posts"] - ends["likes"]).total_seconds()) / 3600.0
    if lag_h > 1.0:
        say(f"note: the two streams end {lag_h:.1f}h apart — cut anchored on "
            "the earlier one so no post is censored")

    n_eligible, n_cohort = _build_cohort(
        con, lo, hi, min_likes=min_likes, max_posts=max_posts, seed=seed, say=say)
    n_likes_raw = _build_delays(con, horizon_hours, say)
    _build_features(con, n_bins, horizon_hours, min_likes, say)
    uri_ids, counts, sc = _assemble(con, n_bins, say)

    # Two references. The flat one is what "organic" means a priori; the
    # empirical one is the cohort's own mean curve, which absorbs the
    # ignition knee and any diurnal envelope.
    refs = {
        "loguniform": _loguniform_cdf(n_bins),
        "empirical": _empirical_cdf(counts),
    }
    n_arr = counts.sum(axis=1)
    grid = _n_grid(min_likes, float(n_arr.max()))

    results, figs_hist = {}, {}
    for name, ref_cdf in refs.items():
        pmf = np.diff(np.concatenate([[0.0], ref_cdf]))
        pmf = np.maximum(pmf, 1e-12)
        pmf = pmf / pmf.sum()
        d_crit = _calibrate(pmf, grid, n_sims=n_sims, pct=crit_pct,
                            seed=seed, say=say)
        d, d_signed = _ks_to_reference(counts, ref_cdf)
        results[name] = {
            "d": d, "d_signed": d_signed,
            "score": _score(d, n_arr, grid, d_crit),
            "d_crit": d_crit,
        }
        figs_hist[name] = _fig_score_hist(results[name]["score"], name)

    head = results[headline_null]["score"]

    # Falsification diagnostics. Spearman via ranks — no scipy dependency.
    def spearman(a, b):
        ra = np.argsort(np.argsort(a)).astype(float)
        rb = np.argsort(np.argsort(b)).astype(float)
        ra -= ra.mean(); rb -= rb.mean()
        den = math.sqrt(float((ra ** 2).sum()) * float((rb ** 2).sum()))
        return float((ra * rb).sum() / den) if den > 0 else 0.0

    hour_med = [float(np.median(head[sc["hour_utc"] == h]))
                for h in range(24) if (sc["hour_utc"] == h).sum() >= 5]
    controls = {
        "spearman_t50": spearman(head, sc["t50_h"]),
        "spearman_n": spearman(head, n_arr),
        "hour_spread": (max(hour_med) - min(hour_med)) if hour_med else 0.0,
    }

    # Is the empirical reference a shape anybody actually has? The mean of
    # a heterogeneous cohort is a curve no member resembles, and scoring
    # against it then flags nearly everyone as "unlike average" — which
    # says the reference is unusable, not that the posts are irregular.
    # Cheap tell: if most posts clear the band against their own cohort
    # mean, the mean is a fiction and only the flat null can be trusted.
    emp_share = float((results["empirical"]["score"] > 1.0).mean())
    reference_validity = {
        "empirical_median_d": float(np.median(results["empirical"]["d"])),
        "empirical_share_above_1": emp_share,
        "empirical_usable": bool(emp_share < 0.5),
    }
    if not reference_validity["empirical_usable"]:
        say(f"WARNING: empirical null flags {emp_share * 100:.0f}% of posts — "
            "the cohort has no dominant shape, so its mean curve is not a "
            "usable reference. Trust the loguniform null here.")

    mean_post_cdf = _empirical_cdf(counts)
    flat = {
        "mean_post_max_dev": float(np.max(np.abs(
            mean_post_cdf - _loguniform_cdf(n_bins)))),
        "mean_post_cdf": [float(x) for x in mean_post_cdf],
        "median_sub_floor": float(np.median(sc["sub_floor_share"])),
        "median_tie_share": float(np.median(sc["tie_share"])),
    }

    figs = {
        "pooled": fig_html(_fig_pooled_density(counts, n_bins, horizon_hours),
                           "og-pooled"),
        "hist_loguniform": fig_html(figs_hist["loguniform"], "og-hist-lu"),
        "hist_empirical": fig_html(figs_hist["empirical"], "og-hist-emp"),
        "deciles": fig_html(_fig_deciles(counts, head, n_bins, horizon_hours,
                                         per_decile=examples_per_decile,
                                         seed=seed), "og-deciles"),
        "ctrl_t50": fig_html(_fig_control(
            head, sc["t50_h"], log_x=True,
            title="Score vs how fast the post moved",
            xtitle="median like delay (hours, log)"), "og-t50"),
        "ctrl_hour": fig_html(_fig_control(
            head, sc["hour_utc"], n_bucket=24,
            title="Score vs hour of day the post was made",
            xtitle="post created_at hour (UTC)"), "og-hour"),
    }

    def summarize(x):
        return {
            "median": float(np.median(x)), "p90": float(np.percentile(x, 90)),
            "p99": float(np.percentile(x, 99)),
            "share_above_1": float((x > 1.0).mean()),
        }

    sidecar = {
        "snapshot_date": snapshot_date,
        "params": {
            "cohort_days": cohort_days, "horizon_hours": horizon_hours,
            "min_likes": min_likes, "max_posts": max_posts, "n_bins": n_bins,
            "floor_seconds": FLOOR_SECONDS, "n_sims": n_sims,
            "crit_pct": crit_pct, "examples_per_decile": examples_per_decile,
            "seed": seed,
        },
        "cohort": {
            "cut": str(cut), "lo": str(lo), "hi": str(hi),
            "posts_end": str(ends["posts"]), "likes_end": str(ends["likes"]),
            "stream_lag_hours": lag_h,
            "n_eligible": n_eligible, "n_sampled": n_cohort,
            "n_analyzed": int(len(uri_ids)),
            "n_likes": int(n_arr.sum()), "n_likes_raw": n_likes_raw,
            "min_likes": min_likes,
        },
        "reference": {
            "headline": headline_null,
            "loguniform_cdf": [float(x) for x in refs["loguniform"]],
            "empirical_cdf": [float(x) for x in refs["empirical"]],
            **reference_validity,
        },
        "calibration": {
            "n_grid": [int(x) for x in grid],
            **{f"d_crit_{k}": [float(x) for x in v["d_crit"]]
               for k, v in results.items()},
        },
        "flatness": flat,
        "controls": controls,
        "scores": {k: summarize(v["score"]) for k, v in results.items()},
        "deciles": {
            k: [float(x) for x in np.quantile(v["score"], np.linspace(0, 1, 11))]
            for k, v in results.items()
        },
        "timing": {"total_s": time.time() - t_start},
    }

    # Per-post table, for joining scores back to posts without re-extracting.
    sidecar["posts"] = {
        "uri_id": [int(u) for u in uri_ids],
        "n": [int(x) for x in n_arr],
        "t50_h": [float(x) for x in sc["t50_h"]],
        "t90_h": [float(x) for x in sc["t90_h"]],
        "hour_utc": [int(x) for x in sc["hour_utc"]],
        "tie_share": [float(x) for x in sc["tie_share"]],
        "sub_floor_share": [float(x) for x in sc["sub_floor_share"]],
        **{f"d_{k}": [float(x) for x in v["d"]] for k, v in results.items()},
        **{f"d_signed_{k}": [float(x) for x in v["d_signed"]]
           for k, v in results.items()},
        **{f"score_{k}": [float(x) for x in v["score"]]
           for k, v in results.items()},
    }

    say(f"done in {sidecar['timing']['total_s']:.0f}s — "
        f"median score {sidecar['scores'][headline_null]['median']:.2f}, "
        f"{sidecar['scores'][headline_null]['share_above_1'] * 100:.1f}% above 1")
    return _render_html(snapshot_date=snapshot_date, sidecar=sidecar,
                        figs=figs), sidecar


def main(argv=None) -> int:
    """Local CLI: open a snapshot read-only and write html + json + parquet."""
    import argparse
    import json
    import os

    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--db", required=True, help="path to snapshot.duckdb")
    p.add_argument("--snapshot-date", required=True, help="e.g. 2026-07-31")
    p.add_argument("--out-dir", default=".")
    p.add_argument("--cohort-days", type=int, default=14)
    p.add_argument("--horizon-hours", type=int, default=24)
    p.add_argument("--min-likes", type=int, default=MIN_LIKES)
    p.add_argument("--max-posts", type=int, default=60_000)
    p.add_argument("--n-bins", type=int, default=18)
    p.add_argument("--headline-null", choices=NULLS, default="loguniform")
    p.add_argument("--memory-limit", default="20GB")
    p.add_argument("--threads", type=int, default=6)
    p.add_argument("--temp-dir", default=None)
    p.add_argument("--seed", type=int, default=0)
    a = p.parse_args(argv)

    import duckdb

    con = duckdb.connect(a.db, read_only=True)
    con.execute(f"SET memory_limit='{a.memory_limit}'")
    con.execute(f"SET threads={int(a.threads)}")
    # Nothing here depends on row order, and keeping it costs enough memory
    # on a 1.6B-row likes scan to OOM a 32 GB machine outright.
    con.execute("SET preserve_insertion_order=false")
    if a.temp_dir:
        con.execute(f"SET temp_directory='{a.temp_dir}'")

    html, sidecar = run(
        con, a.snapshot_date, cohort_days=a.cohort_days,
        horizon_hours=a.horizon_hours, min_likes=a.min_likes,
        max_posts=a.max_posts, n_bins=a.n_bins,
        headline_null=a.headline_null, seed=a.seed,
    )

    os.makedirs(a.out_dir, exist_ok=True)
    base = f"{a.out_dir}/organic_{a.snapshot_date}"
    with open(f"{base}.html", "wb") as f:
        f.write(html)

    posts = sidecar.pop("posts")
    with open(f"{base}.json", "w") as f:
        json.dump(sidecar, f, indent=2, default=str)

    # The per-post table goes to parquet rather than into the json: it is
    # the thing you actually want to slice, and at 60k rows it would
    # dominate a sidecar meant to be read by eye.
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        # uri_id is xxhash3-64 and lands in the snapshot as UBIGINT, so
        # real values routinely exceed 2^63. Letting pyarrow infer the type
        # gets int64 and raises on the first such row, so it is declared.
        cols = {}
        for name, vals in posts.items():
            if name == "uri_id":
                cols[name] = pa.array(vals, type=pa.uint64())
            else:
                cols[name] = pa.array(vals)
        pq.write_table(pa.table(cols), f"{base}_posts.parquet")
        wrote = f"{base}_posts.parquet"
    except ImportError:
        import csv
        wrote = f"{base}_posts.csv"
        cols = list(posts)
        with open(wrote, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(zip(*(posts[c] for c in cols)))

    print(f"\nwrote {base}.html\n      {base}.json\n      {wrote}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
