"""Distribution of *accounts followed since 2025* per account.

The out-degree companion to `followers.py`. Where that module bins each
account by inbound edges (followers), this one bins by **outbound** edges
— how many other accounts each user follows — using the `follows` column
of `actor_aggs`.

The same 2025-01-01 cutoff caveat applies, and arguably even more
cleanly: the snapshot's `follows` table only contains edges created on or
after 2025-01-01, so this column is the count of *follow actions an
account has taken since the start of 2025* — i.e. accounts it started
following since then, not its lifetime following total. Unlike the
follower side (where capture depends on whether your *audience* is post-
cutoff), out-degree is undercounted uniformly by account: every account
simply loses the accounts it followed before 2025. An account created
after the cutoff has its full following captured.

Bins `log10(follows)` on a log10 scale, same machinery as followers.py.

Public entrypoint: `run(con, snapshot_date) -> (html_bytes, sidecar_dict)`.
"""

from __future__ import annotations

import math

from .common import (
    BRAND, SHARED_CSS,
    built_at_utc, fig_html, fmt_int, install_template,
    plotlyjs_inline, timed_query,
)
# Reuse the binning / smoothing / mode-detection machinery and the shared
# constants so the two distribution reports stay in lockstep.
from .followers import (
    SINCE_DATE, BINS_PER_DECADE, SMOOTH_SIGMA_BINS, MIN_MODE_SEP_DEX,
    _human, _smooth, _find_modes,
)


def run(con, snapshot_date: str, *, log: bool = True) -> tuple[bytes, dict]:
    import plotly.graph_objects as go

    if log:
        print("=== run queries ===", flush=True)

    def q(sql, params=None):
        return timed_query(con, sql, params, log=log)

    bpd = BINS_PER_DECADE

    # --- headline numbers -------------------------------------------------
    headline = q(
        """
        SELECT
          COUNT(*)                            AS total_actors,
          COUNT(*) FILTER (WHERE follows > 0) AS with_follows,
          COUNT(*) FILTER (WHERE follows = 0) AS zero_follows,
          MEDIAN(follows)                     AS median_all,
          AVG(follows)::DOUBLE                AS mean_all,
          QUANTILE_CONT(follows, 0.99)        AS p99_all,
          MAX(follows)                        AS max_f,
          (SELECT MEDIAN(follows) FROM actor_aggs WHERE follows > 0)
                                              AS median_pos
        FROM actor_aggs
        """
    )[0]
    (total_actors, with_follows, zero_follows, median_all, mean_all,
     p99_all, max_f, median_pos) = headline

    # --- log10 histogram (positive follows only) --------------------------
    # FLOOR(log10(follows) * bpd) gives an integer bin id; bin b covers
    # follows in [10^(b/bpd), 10^((b+1)/bpd)).
    hist_rows = q(
        f"""
        SELECT
          CAST(FLOOR(LOG10(follows) * {bpd}) AS BIGINT) AS bin,
          COUNT(*) AS n
        FROM actor_aggs
        WHERE follows > 0
        GROUP BY 1
        ORDER BY 1
        """
    )

    # --- by order of magnitude (clean, labelled secondary view) -----------
    # "1" is split out from "2–9": an account following exactly one other
    # account is almost certainly just sitting on the signup default (the
    # auto-followed bsky.app), not actively curating — worth distinguishing
    # from accounts that have made ≥2 deliberate follows.
    decade_rows = q(
        """
        SELECT bucket, n FROM (
          SELECT
            CASE
              WHEN follows = 0          THEN '0'
              WHEN follows = 1          THEN '1 (default)'
              WHEN follows < 10         THEN '2–9'
              WHEN follows < 100        THEN '10–99'
              WHEN follows < 1000       THEN '100–999'
              WHEN follows < 10000      THEN '1K–9.9K'
              WHEN follows < 100000     THEN '10K–99K'
              WHEN follows < 1000000    THEN '100K–999K'
              ELSE '1M+'
            END AS bucket,
            CASE
              WHEN follows = 0          THEN 0
              WHEN follows = 1          THEN 1
              WHEN follows < 10         THEN 2
              WHEN follows < 100        THEN 3
              WHEN follows < 1000       THEN 4
              WHEN follows < 10000      THEN 5
              WHEN follows < 100000     THEN 6
              WHEN follows < 1000000    THEN 7
              ELSE 8
            END AS ord,
            COUNT(*) AS n
          FROM actor_aggs
          GROUP BY 1, 2
        ) ORDER BY ord
        """
    )

    # Densify the histogram: fill empty bins between the min and max bin
    # so the bars / smoothed line are evenly spaced in log space.
    if hist_rows:
        counts_by_bin = {int(b): int(n) for b, n in hist_rows}
        bmin = min(counts_by_bin)
        bmax = max(counts_by_bin)
        bins = list(range(bmin, bmax + 1))
        counts = [counts_by_bin.get(b, 0) for b in bins]
        centers_dex = [(b + 0.5) / bpd for b in bins]
        centers_follows = [10 ** d for d in centers_dex]
    else:  # pragma: no cover - empty snapshot
        bins, counts, centers_dex, centers_follows = [], [], [], []

    smooth = _smooth([float(c) for c in counts]) if counts else []
    modes = _find_modes(centers_dex, smooth)
    is_multimodal = len(modes) >= 2

    # ---------------------------------------------------------------------
    # Plotly figures
    # ---------------------------------------------------------------------
    install_template()

    if centers_dex:
        tick_lo = int(math.floor(min(centers_dex)))
        tick_hi = int(math.ceil(max(centers_dex)))
    else:
        tick_lo, tick_hi = 0, 1
    tickvals = list(range(tick_lo, tick_hi + 1))
    ticktext = [_human(10 ** v) for v in tickvals]

    bar_width = 1.0 / bpd  # bars touch in log space

    fig_hist = go.Figure()
    fig_hist.add_trace(go.Bar(
        x=centers_dex, y=counts, width=bar_width,
        marker=dict(color=BRAND, line=dict(width=0)),
        name="accounts",
        customdata=centers_follows,
        hovertemplate="≈%{customdata:,.0f} accounts followed<br>"
                      "%{y:,} accounts<extra></extra>",
    ))
    fig_hist.add_trace(go.Scatter(
        x=centers_dex, y=smooth, mode="lines",
        line=dict(color="#1d2433", width=2),
        name="smoothed",
        hoverinfo="skip",
    ))
    if modes:
        mode_dex = [math.log10(f) for f, _h in modes]
        mode_h = [h for _f, h in modes]
        fig_hist.add_trace(go.Scatter(
            x=mode_dex, y=mode_h, mode="markers",
            marker=dict(color="#ff5d8f", size=10, symbol="circle",
                        line=dict(color="white", width=1.5)),
            name="mode",
            customdata=[f for f, _h in modes],
            hovertemplate="mode ≈ %{customdata:,.0f} accounts followed<extra></extra>",
        ))
    fig_hist.update_layout(
        template="bsky",
        title=dict(
            text=f"<b>Accounts followed since {SINCE_DATE[:4]}, per account</b>  ·  "
                 "log10 scale, " + (
                     "multi-modal" if is_multimodal else "single mode"),
            x=0.02, xanchor="left"),
        xaxis=dict(title=f"Accounts followed since {SINCE_DATE[:4]} (log scale)",
                   tickmode="array", tickvals=tickvals, ticktext=ticktext),
        yaxis=dict(title="Number of accounts"),
        bargap=0.0,
        height=460,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.0,
                    xanchor="right", x=1.0),
    )

    d_buckets = [r[0] for r in decade_rows]
    d_counts = [r[1] for r in decade_rows]
    d_pct = [100 * c / total_actors for c in d_counts] if total_actors else []
    # Red for the zero bucket, amber for the "1 (default)" bucket (signup
    # default, not active curation), brand blue for everyone with ≥2 follows.
    d_colors = []
    for b in d_buckets:
        if b == "0":
            d_colors.append("#ef4444")
        elif b.startswith("1 ("):
            d_colors.append("#f59e0b")
        else:
            d_colors.append(BRAND)
    fig_decade = go.Figure(go.Bar(
        x=d_buckets, y=d_counts,
        marker=dict(color=d_colors),
        text=[f"{c:,}<br>{p:.1f}%" for c, p in zip(d_counts, d_pct)],
        textposition="outside",
        hovertemplate="%{x} accounts followed<br>%{y:,} accounts<extra></extra>",
    ))
    fig_decade.update_layout(
        template="bsky",
        title=dict(text=f"<b>Accounts by order of magnitude of accounts followed since {SINCE_DATE[:4]}</b>",
                   x=0.02, xanchor="left"),
        xaxis=dict(title=f"Accounts followed since {SINCE_DATE[:4]}"),
        yaxis=dict(title="Number of accounts (log)", type="log"),
        height=400,
    )

    plot_html = {
        "hist": fig_html(fig_hist, "fig_hist"),
        "decade": fig_html(fig_decade, "fig_decade"),
    }
    plotlyjs = plotlyjs_inline()

    pct_zero = 100 * zero_follows / total_actors if total_actors else 0.0
    # Accounts following exactly one other account — the signup default
    # (auto-followed bsky.app), a proxy for "registered but never curated".
    default_follow = next(
        (n for b, n in decade_rows if str(b).startswith("1 (")), 0
    )
    pct_default = 100 * default_follow / total_actors if total_actors else 0.0
    built_at = built_at_utc()

    if is_multimodal:
        m0, m1 = modes[0][0], modes[1][0]
        lo, hi = sorted((m0, m1))
        mode_sentence = (
            f"The distribution is <strong>multi-modal</strong>: a peak near "
            f"<strong>{_human(round(lo))}</strong> accounts followed and a "
            f"second near <strong>{_human(round(hi))}</strong>."
        )
        verdict = "bimodal"
    elif modes:
        mode_sentence = (
            f"The distribution is <strong>single-peaked</strong>, cresting "
            f"around <strong>{_human(round(modes[0][0]))}</strong> accounts followed."
        )
        verdict = "unimodal"
    else:  # pragma: no cover - empty snapshot
        mode_sentence = ("No accounts followed anyone since "
                         f"{SINCE_DATE} in this snapshot.")
        verdict = "empty"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Distribution of accounts followed since {SINCE_DATE[:4]}</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>{SHARED_CSS}</style>
<script>{plotlyjs}</script>
</head>
<body>
<div class="wrap">

<div class="eyebrow">An analysis · snapshot {snapshot_date}</div>
<h1>Accounts followed <span class="accent">since {SINCE_DATE[:4]}</span>, per account.</h1>
<p class="lede">
  The out-degree companion to the follower distribution: how many other
  accounts each user follows. This snapshot's follow graph only contains
  edges created on or after <strong>{SINCE_DATE}</strong>, so each count is
  the number of <strong>accounts a user has started following since the
  start of {SINCE_DATE[:4]}</strong> — not their lifetime following total.
  Binned on a log10 scale so the low-activity mass doesn't collapse the
  chart into a single spike. {mode_sentence}
</p>

<div class="stats">
  <div class="stat">
    <div class="v">{fmt_int(total_actors)}</div>
    <div class="l">accounts in the snapshot</div>
    <div class="sub">{fmt_int(with_follows)} followed ≥1 since {SINCE_DATE[:4]}</div>
  </div>
  <div class="stat">
    <div class="v bad">{pct_zero:.1f}%</div>
    <div class="l">followed nobody since {SINCE_DATE[:4]}</div>
    <div class="sub">{fmt_int(zero_follows)} accounts</div>
  </div>
  <div class="stat">
    <div class="v">{fmt_int(median_all or 0)}</div>
    <div class="l">median accounts followed (all)</div>
    <div class="sub">{fmt_int(median_pos or 0)} among those with ≥1</div>
  </div>
  <div class="stat">
    <div class="v brand">{fmt_int(max_f or 0)}</div>
    <div class="l">most accounts followed (since {SINCE_DATE[:4]})</div>
    <div class="sub">mean {(mean_all or 0):,.0f} · p99 {fmt_int(p99_all or 0)}</div>
  </div>
</div>

<section>
  <div class="kicker">The main event</div>
  <h2>Accounts-followed-since-{SINCE_DATE[:4]} on a log10 scale.</h2>
  <p>
    Each bar is a 0.1-decade slice of the axis, so bars are equal width in
    log space and any two peaks are directly comparable in height. The dark
    line is a Gaussian-smoothed guide; pink dots mark detected local maxima
    (modes). Accounts that followed nobody since {SINCE_DATE}
    ({pct_zero:.1f}% of the total) have no log10 and are excluded from this
    chart — see the breakdown below for them.
  </p>
  <div class="figure">{plot_html["hist"]}</div>
</section>

<section>
  <div class="kicker">For reference</div>
  <h2>Accounts by order of magnitude.</h2>
  <p>
    The same population bucketed into clean powers of ten (accounts followed
    since {SINCE_DATE[:4]}). The zero group is red; the
    <strong>"1 (default)"</strong> bucket is amber and split out on purpose:
    an account following exactly one other account is almost certainly just
    sitting on the signup default — Bluesky auto-follows its own
    <code>bsky.app</code> account on sign-up — rather than actively curating
    a feed. That single bucket holds <strong>{fmt_int(default_follow)}</strong>
    accounts ({pct_default:.1f}% of all accounts). Together with the
    {pct_zero:.1f}% who follow nobody, that's a large slice of the network
    that has made effectively zero deliberate follow decisions since
    {SINCE_DATE[:4]}. The y-axis is logarithmic so the smaller high-activity
    tiers stay visible next to the low-activity masses.
  </p>
  <div class="figure">{plot_html["decade"]}</div>
</section>

<footer>
  <p>
    <strong>What this measures.</strong> The snapshot's <code>follows</code>
    table only contains edges created on or after <code>{SINCE_DATE}</code>
    — verified from its <code>created_at</code> histogram, where pre-{SINCE_DATE[:4]}
    months hold a handful of backdated rows each and {SINCE_DATE[:7]} jumps to
    ~13M edges. So the <code>follows</code> column of <code>actor_aggs</code>
    counts <strong>accounts followed since {SINCE_DATE}</strong>, not lifetime
    following. Unlike the follower (in-degree) side, out-degree is truncated
    uniformly per account — every account simply loses whoever it followed
    before the cutoff — so an account created after {SINCE_DATE[:4]} has its
    full following captured.
  </p>
  <p>
    <strong>Methodology.</strong> Counts come from the <code>follows</code>
    column of <code>actor_aggs</code> in the at-snapshot Bluesky DuckDB build,
    snapshot date <code>{snapshot_date}</code>. The headline histogram bins
    <code>log10(follows)</code> at {bpd} bins per decade over accounts that
    followed at least one account since {SINCE_DATE[:4]}; the zero group is
    reported separately. Mode detection takes local maxima of a Gaussian-
    smoothed (σ ≈ {SMOOTH_SIGMA_BINS} bins) histogram, keeping peaks ≥5% of the
    global maximum and merging any within {MIN_MODE_SEP_DEX} dex of a taller
    one. Verdict: <strong>{verdict}</strong>. Built {built_at}.
  </p>
  <p>
    <strong>Caveats.</strong> Read the shape as "recent following activity,"
    not lifetime following. Suspended/deactivated accounts still appear if
    their edges remain indexed; counts are not de-spammed. Aggressive
    follow-bots and follow-back rings, if present, are not separated from
    organic following.
  </p>
</footer>

</div>
</body>
</html>
"""

    sidecar = {
        "snapshot_date": snapshot_date,
        "built_at_utc": built_at,
        "metric": "accounts_followed_since",
        "since_date": SINCE_DATE,
        "bins_per_decade": bpd,
        "total_actors": int(total_actors or 0),
        "with_follows": int(with_follows or 0),
        "zero_follows": int(zero_follows or 0),
        "pct_zero_follows": pct_zero,
        "median_follows_all": int(median_all or 0),
        "median_follows_positive": int(median_pos or 0),
        "mean_follows": float(mean_all or 0),
        "p99_follows": int(p99_all or 0),
        "max_follows": int(max_f or 0),
        "default_follow_only": int(default_follow),
        "pct_default_follow_only": pct_default,
        "is_multimodal": is_multimodal,
        "verdict": verdict,
        "modes": [
            {"follows": float(f), "smoothed_height": float(h)}
            for f, h in modes
        ],
        "histogram": {
            "bin_ids": bins,
            "bin_center_follows": centers_follows,
            "counts": counts,
        },
        "decade_buckets": [
            {"bucket": b, "n": int(n)} for b, n in decade_rows
        ],
    }
    return html.encode("utf-8"), sidecar
