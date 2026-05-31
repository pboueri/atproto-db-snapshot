"""Follower-count distribution analysis.

Reads the `actor_aggs` table from a snapshot.duckdb connection and asks a
single question: when you line every account up by how many followers it
has and bin them on a **log10 scale**, what shape does the distribution
take? The hope is to surface a bimodal (or multi-modal) structure —
e.g. a hump of low-follower / dormant accounts and a separate hump of
"established" accounts — that a linear-scale histogram would smear into
an unreadable spike at zero.

Binning is done in log space (a fixed number of bins per decade) so the
bars have equal width in log10 and modes are directly comparable. The
x-axis is plotted in log10-dex with tick labels rendered back in human
follower counts (1, 10, 100, 1K, …). Accounts with zero followers have
no log10 and are reported separately as a headline number rather than
forced into the first bin.

Public entrypoint: `run(con, snapshot_date) -> (html_bytes, sidecar_dict)`.
"""

from __future__ import annotations

import math

from .common import (
    BRAND, SHARED_CSS,
    built_at_utc, fig_html, fmt_int, install_template,
    plotlyjs_inline, timed_query,
)

# Bins per decade for the log10 histogram. 10 ⇒ 0.1-dex bars, fine
# enough to resolve separate modes without turning into noise.
BINS_PER_DECADE = 10


def _human(n: float) -> str:
    """Render a follower count compactly: 1, 10, 1K, 1M, …."""
    n = float(n)
    if n < 1000:
        # Drop a trailing .0 so axis ticks read "1", "100" not "1.0".
        return f"{n:.0f}" if n == int(n) else f"{n:g}"
    if n < 1_000_000:
        return f"{n / 1000:g}K"
    if n < 1_000_000_000:
        return f"{n / 1_000_000:g}M"
    return f"{n / 1_000_000_000:g}B"


def _smooth(counts: list[float], sigma_bins: float = 1.6) -> list[float]:
    """Gaussian-smooth a histogram (reflect at the edges).

    Used only to draw a guide line over the bars and to make local-maxima
    detection robust to single-bin jitter; the bars themselves stay raw.
    """
    radius = max(1, int(math.ceil(sigma_bins * 3)))
    kernel = [math.exp(-0.5 * (i / sigma_bins) ** 2)
              for i in range(-radius, radius + 1)]
    ksum = sum(kernel)
    kernel = [k / ksum for k in kernel]
    n = len(counts)
    out = []
    for i in range(n):
        acc = 0.0
        for j, k in enumerate(kernel):
            idx = i + (j - radius)
            # Reflect indices that fall off either end.
            if idx < 0:
                idx = -idx
            elif idx >= n:
                idx = 2 * n - 2 - idx
            idx = min(max(idx, 0), n - 1)
            acc += counts[idx] * k
        out.append(acc)
    return out


def _find_modes(centers_dex: list[float], smooth: list[float],
                *, min_rel_height: float = 0.05) -> list[tuple[float, float]]:
    """Local maxima of the smoothed curve, tall enough to count as modes.

    Returns (follower_count, smoothed_height) pairs sorted tallest-first.
    `min_rel_height` filters out ripples below 5% of the global peak.
    """
    if not smooth:
        return []
    peak = max(smooth)
    floor = peak * min_rel_height
    modes = []
    for i in range(len(smooth)):
        left = smooth[i - 1] if i > 0 else -1.0
        right = smooth[i + 1] if i < len(smooth) - 1 else -1.0
        if smooth[i] >= left and smooth[i] >= right and smooth[i] >= floor:
            modes.append((10 ** centers_dex[i], smooth[i]))
    modes.sort(key=lambda m: m[1], reverse=True)
    return modes


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
          COUNT(*)                              AS total_actors,
          COUNT(*) FILTER (WHERE followers > 0) AS with_followers,
          COUNT(*) FILTER (WHERE followers = 0) AS zero_followers,
          MEDIAN(followers)                     AS median_all,
          AVG(followers)::DOUBLE                AS mean_all,
          QUANTILE_CONT(followers, 0.99)        AS p99_all,
          MAX(followers)                        AS max_f,
          (SELECT MEDIAN(followers) FROM actor_aggs WHERE followers > 0)
                                                AS median_pos
        FROM actor_aggs
        """
    )[0]
    (total_actors, with_followers, zero_followers, median_all, mean_all,
     p99_all, max_f, median_pos) = headline

    # --- log10 histogram (positive followers only) ------------------------
    # FLOOR(log10(followers) * bpd) gives an integer bin id; bin b covers
    # followers in [10^(b/bpd), 10^((b+1)/bpd)).
    hist_rows = q(
        f"""
        SELECT
          CAST(FLOOR(LOG10(followers) * {bpd}) AS BIGINT) AS bin,
          COUNT(*) AS n
        FROM actor_aggs
        WHERE followers > 0
        GROUP BY 1
        ORDER BY 1
        """
    )

    # --- by order of magnitude (clean, labelled secondary view) -----------
    decade_rows = q(
        """
        SELECT bucket, n FROM (
          SELECT
            CASE
              WHEN followers = 0          THEN '0'
              WHEN followers < 10         THEN '1–9'
              WHEN followers < 100        THEN '10–99'
              WHEN followers < 1000       THEN '100–999'
              WHEN followers < 10000      THEN '1K–9.9K'
              WHEN followers < 100000     THEN '10K–99K'
              WHEN followers < 1000000    THEN '100K–999K'
              ELSE '1M+'
            END AS bucket,
            CASE
              WHEN followers = 0          THEN 0
              WHEN followers < 10         THEN 1
              WHEN followers < 100        THEN 2
              WHEN followers < 1000       THEN 3
              WHEN followers < 10000      THEN 4
              WHEN followers < 100000     THEN 5
              WHEN followers < 1000000    THEN 6
              ELSE 7
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
        # Bin centre in dex (log10 followers) and the human follower count.
        centers_dex = [(b + 0.5) / bpd for b in bins]
        centers_followers = [10 ** d for d in centers_dex]
    else:  # pragma: no cover - empty snapshot
        bins, counts, centers_dex, centers_followers = [], [], [], []

    smooth = _smooth([float(c) for c in counts]) if counts else []
    modes = _find_modes(centers_dex, smooth)
    is_multimodal = len(modes) >= 2

    # ---------------------------------------------------------------------
    # Plotly figures
    # ---------------------------------------------------------------------
    install_template()

    # Integer-dex tick marks (1, 10, 100, …) spanning the data range.
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
        customdata=centers_followers,
        hovertemplate="≈%{customdata:,.0f} followers<br>"
                      "%{y:,} accounts<extra></extra>",
    ))
    fig_hist.add_trace(go.Scatter(
        x=centers_dex, y=smooth, mode="lines",
        line=dict(color="#1d2433", width=2),
        name="smoothed",
        hoverinfo="skip",
    ))
    # Drop a marker on each detected mode.
    if modes:
        mode_dex = [math.log10(f) for f, _h in modes]
        mode_h = [h for _f, h in modes]
        fig_hist.add_trace(go.Scatter(
            x=mode_dex, y=mode_h, mode="markers",
            marker=dict(color="#ff5d8f", size=10, symbol="circle",
                        line=dict(color="white", width=1.5)),
            name="mode",
            customdata=[f for f, _h in modes],
            hovertemplate="mode ≈ %{customdata:,.0f} followers<extra></extra>",
        ))
    fig_hist.update_layout(
        template="bsky",
        title=dict(
            text="<b>Follower distribution across all accounts</b>  ·  "
                 "log10 scale, " + (
                     "multi-modal" if is_multimodal else "single mode"),
            x=0.02, xanchor="left"),
        xaxis=dict(title="Followers (log scale)",
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
    fig_decade = go.Figure(go.Bar(
        x=d_buckets, y=d_counts,
        marker=dict(color=["#ef4444"] + [BRAND] * (len(d_buckets) - 1)),
        text=[f"{c:,}<br>{p:.1f}%" for c, p in zip(d_counts, d_pct)],
        textposition="outside",
        hovertemplate="%{x} followers<br>%{y:,} accounts<extra></extra>",
    ))
    fig_decade.update_layout(
        template="bsky",
        title=dict(text="<b>Accounts by order of magnitude of followers</b>",
                   x=0.02, xanchor="left"),
        xaxis=dict(title="Follower count"),
        yaxis=dict(title="Number of accounts (log)", type="log"),
        height=400,
    )

    plot_html = {
        "hist": fig_html(fig_hist, "fig_hist"),
        "decade": fig_html(fig_decade, "fig_decade"),
    }
    plotlyjs = plotlyjs_inline()

    pct_zero = 100 * zero_followers / total_actors if total_actors else 0.0
    built_at = built_at_utc()

    # Human-readable mode summary for the prose.
    if is_multimodal:
        m0, m1 = modes[0][0], modes[1][0]
        lo, hi = sorted((m0, m1))
        mode_sentence = (
            f"The distribution is <strong>multi-modal</strong>: a peak near "
            f"<strong>{_human(round(lo))}</strong> followers and a second "
            f"near <strong>{_human(round(hi))}</strong>."
        )
        verdict = "bimodal"
    elif modes:
        mode_sentence = (
            f"The distribution is <strong>single-peaked</strong>, cresting "
            f"around <strong>{_human(round(modes[0][0]))}</strong> followers."
        )
        verdict = "unimodal"
    else:  # pragma: no cover - empty snapshot
        mode_sentence = "No accounts with followers were found in this snapshot."
        verdict = "empty"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>The shape of Bluesky's follower distribution</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>{SHARED_CSS}</style>
<script>{plotlyjs}</script>
</head>
<body>
<div class="wrap">

<div class="eyebrow">An analysis · snapshot {snapshot_date}</div>
<h1>The shape of Bluesky's <span class="accent">follower distribution</span>.</h1>
<p class="lede">
  Every account on Bluesky, binned by follower count on a log10 scale.
  Plotting in log space stops the enormous pile of low-follower accounts
  from collapsing the chart into a single spike — and lets any separate
  "humps" in the population stand out. {mode_sentence}
</p>

<div class="stats">
  <div class="stat">
    <div class="v">{fmt_int(total_actors)}</div>
    <div class="l">accounts in the snapshot</div>
    <div class="sub">{fmt_int(with_followers)} have ≥1 follower</div>
  </div>
  <div class="stat">
    <div class="v bad">{pct_zero:.1f}%</div>
    <div class="l">have zero followers</div>
    <div class="sub">{fmt_int(zero_followers)} accounts</div>
  </div>
  <div class="stat">
    <div class="v">{fmt_int(median_all or 0)}</div>
    <div class="l">median followers (all accounts)</div>
    <div class="sub">{fmt_int(median_pos or 0)} among those with ≥1</div>
  </div>
  <div class="stat">
    <div class="v brand">{fmt_int(max_f or 0)}</div>
    <div class="l">most-followed account</div>
    <div class="sub">mean {(mean_all or 0):,.0f} · p99 {fmt_int(p99_all or 0)}</div>
  </div>
</div>

<section>
  <div class="kicker">The main event</div>
  <h2>Follower counts on a log10 scale.</h2>
  <p>
    Each bar is a 0.1-decade slice of the follower axis, so bars are equal
    width in log space and any two peaks are directly comparable in height.
    The dark line is a Gaussian-smoothed guide; pink dots mark detected
    local maxima (modes). Accounts with exactly zero followers
    ({pct_zero:.1f}% of the total) have no log10 and are excluded from this
    chart — see the breakdown below for them.
  </p>
  <div class="figure">{plot_html["hist"]}</div>
</section>

<section>
  <div class="kicker">For reference</div>
  <h2>Accounts by order of magnitude.</h2>
  <p>
    The same population bucketed into clean powers of ten, including the
    zero-follower group in red. The y-axis is logarithmic so the smaller
    high-follower tiers stay visible next to the low-follower masses.
  </p>
  <div class="figure">{plot_html["decade"]}</div>
</section>

<footer>
  <p>
    <strong>Methodology.</strong> Follower counts come from the
    <code>followers</code> column of the <code>actor_aggs</code> table in
    the at-snapshot Bluesky DuckDB build, snapshot date
    <code>{snapshot_date}</code> — each account's count of inbound edges in
    the <code>follows</code> graph. The headline histogram bins
    <code>log10(followers)</code> at {bpd} bins per decade over accounts
    with at least one follower; zero-follower accounts are reported
    separately. Mode detection takes local maxima of a Gaussian-smoothed
    (σ ≈ 1.6 bins) version of the histogram, keeping peaks at least 5% as
    tall as the global maximum. Verdict: <strong>{verdict}</strong>.
    Built {built_at}.
  </p>
  <p>
    <strong>Caveats.</strong> The snapshot reflects the follow graph as
    crawled by constellation; very recent follows lag. Suspended or
    deactivated accounts still appear if their edges remain indexed, which
    inflates the low-follower mass. Follower counts are not de-spammed.
  </p>
</footer>

</div>
</body>
</html>
"""

    sidecar = {
        "snapshot_date": snapshot_date,
        "built_at_utc": built_at,
        "bins_per_decade": bpd,
        "total_actors": int(total_actors or 0),
        "with_followers": int(with_followers or 0),
        "zero_followers": int(zero_followers or 0),
        "pct_zero_followers": pct_zero,
        "median_followers_all": int(median_all or 0),
        "median_followers_positive": int(median_pos or 0),
        "mean_followers": float(mean_all or 0),
        "p99_followers": int(p99_all or 0),
        "max_followers": int(max_f or 0),
        "is_multimodal": is_multimodal,
        "verdict": verdict,
        "modes": [
            {"followers": float(f), "smoothed_height": float(h)}
            for f, h in modes
        ],
        "histogram": {
            "bin_ids": bins,
            "bin_center_followers": centers_followers,
            "counts": counts,
        },
        "decade_buckets": [
            {"bucket": b, "n": int(n)} for b, n in decade_rows
        ],
    }
    return html.encode("utf-8"), sidecar
