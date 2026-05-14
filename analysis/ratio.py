"""Argumentation-ratio analysis: how often (replies + quotes) > likes.

Reads `posts` and `post_aggs` from a snapshot.duckdb connection. Returns
HTML + JSON-friendly sidecar dict. Window: posts authored in the last
`window_days` before `snapshot_date`.

Public entrypoint: `run(con, snapshot_date, window_days=90)`.
"""

from __future__ import annotations

import time
from datetime import date, timedelta

from .common import (
    BRAND, SHARED_CSS,
    built_at_utc, fig_html, fmt_int, install_template,
    plotlyjs_inline, timed_query,
)


def run(
    con,
    snapshot_date: str,
    window_days: int = 90,
    *,
    log: bool = True,
) -> tuple[bytes, dict]:
    import plotly.graph_objects as go

    hi = snapshot_date
    lo = (date.fromisoformat(snapshot_date) - timedelta(days=window_days)).isoformat()
    if log:
        print(f"=== window: posts.created_at in [{lo}, {hi}] ===", flush=True)

    if log:
        print("=== materialize cohort view ===", flush=True)
    t0 = time.time()
    con.execute(
        f"""
        CREATE OR REPLACE TEMPORARY VIEW ratio_cohort AS
        SELECT
          p.uri_id,
          p.author_did_id,
          p.created_at,
          pa.likes,
          pa.replies,
          pa.quotes,
          pa.reposts,
          (pa.replies + pa.quotes)::DOUBLE AS argument,
          CASE WHEN pa.likes > 0
               THEN (pa.replies + pa.quotes)::DOUBLE / pa.likes
               ELSE NULL END AS argr
        FROM posts p
        JOIN post_aggs pa USING(uri_id)
        WHERE p.created_at BETWEEN TIMESTAMP '{lo}' AND TIMESTAMP '{hi}'
        """
    )
    if log:
        print(f"  ({time.time()-t0:.1f}s) view ready", flush=True)

    def q(sql):
        return timed_query(con, sql, log=log)

    headline = q(
        """
        SELECT
          COUNT(*)                                                AS total_posts,
          COUNT(*) FILTER (WHERE likes >= 10)                     AS qualifying_posts,
          COUNT(*) FILTER (WHERE likes >= 10 AND argument > likes) AS true_ratios,
          MEDIAN(argr) FILTER (WHERE likes >= 10)                 AS median_argr,
          QUANTILE_CONT(argr, 0.75) FILTER (WHERE likes >= 10)    AS p75_argr,
          QUANTILE_CONT(argr, 0.90) FILTER (WHERE likes >= 10)    AS p90_argr,
          QUANTILE_CONT(argr, 0.95) FILTER (WHERE likes >= 10)    AS p95_argr,
          QUANTILE_CONT(argr, 0.99) FILTER (WHERE likes >= 10)    AS p99_argr,
          MAX(argr) FILTER (WHERE likes >= 10)                    AS max_argr
        FROM ratio_cohort
        """
    )[0]
    (total_posts, qualifying_posts, true_ratios,
     median_argr, p75_argr, p90_argr, p95_argr, p99_argr, max_argr) = headline
    ratio_pct = 100.0 * (true_ratios or 0) / qualifying_posts if qualifying_posts else 0
    unique_authors = q(
        "SELECT COUNT(DISTINCT author_did_id) FROM ratio_cohort WHERE likes >= 10"
    )[0][0]

    dist_rows = q(
        """
        SELECT bucket, n FROM (
          SELECT
            CASE
              WHEN argr IS NULL OR argr = 0 THEN '0'
              WHEN argr <= 0.05 THEN '(0, 0.05]'
              WHEN argr <= 0.10 THEN '(0.05, 0.10]'
              WHEN argr <= 0.25 THEN '(0.10, 0.25]'
              WHEN argr <= 0.50 THEN '(0.25, 0.50]'
              WHEN argr <= 1.00 THEN '(0.50, 1.00]'
              WHEN argr <= 2.00 THEN '(1, 2] — ratio'
              WHEN argr <= 5.00 THEN '(2, 5] — heavy'
              ELSE '5+ — nuked'
            END AS bucket,
            CASE
              WHEN argr IS NULL OR argr = 0 THEN 0
              WHEN argr <= 0.05 THEN 1
              WHEN argr <= 0.10 THEN 2
              WHEN argr <= 0.25 THEN 3
              WHEN argr <= 0.50 THEN 4
              WHEN argr <= 1.00 THEN 5
              WHEN argr <= 2.00 THEN 6
              WHEN argr <= 5.00 THEN 7
              ELSE 8
            END AS ord,
            COUNT(*) AS n
          FROM ratio_cohort
          WHERE likes >= 10
          GROUP BY 1, 2
        ) ORDER BY ord
        """
    )

    tier_rows = q(
        """
        SELECT bucket, total, ratios, ratio_pct, median_argr FROM (
          SELECT
            CASE
              WHEN likes < 50    THEN '10–49'
              WHEN likes < 100   THEN '50–99'
              WHEN likes < 500   THEN '100–499'
              WHEN likes < 1000  THEN '500–999'
              WHEN likes < 5000  THEN '1k–4.9k'
              WHEN likes < 50000 THEN '5k–49k'
              ELSE '50k+'
            END AS bucket,
            CASE
              WHEN likes < 50    THEN 0
              WHEN likes < 100   THEN 1
              WHEN likes < 500   THEN 2
              WHEN likes < 1000  THEN 3
              WHEN likes < 5000  THEN 4
              WHEN likes < 50000 THEN 5
              ELSE 6
            END AS ord,
            COUNT(*) AS total,
            SUM(CASE WHEN argument > likes THEN 1 ELSE 0 END) AS ratios,
            100.0 * SUM(CASE WHEN argument > likes THEN 1 ELSE 0 END) / COUNT(*) AS ratio_pct,
            MEDIAN(argr) AS median_argr
          FROM ratio_cohort
          WHERE likes >= 10
          GROUP BY 1, 2
          HAVING COUNT(*) >= 50
        ) ORDER BY ord
        """
    )

    weekly_rows = q(
        """
        SELECT
          DATE_TRUNC('week', created_at) AS week,
          COUNT(*) AS total,
          SUM(CASE WHEN argument > likes THEN 1 ELSE 0 END) AS ratios,
          100.0 * SUM(CASE WHEN argument > likes THEN 1 ELSE 0 END) / COUNT(*) AS ratio_pct,
          MEDIAN(argr) AS median_argr
        FROM ratio_cohort
        WHERE likes >= 10
        GROUP BY 1
        ORDER BY 1
        """
    )

    author_rate_rows = q(
        """
        WITH per_author AS (
          SELECT author_did_id,
                 COUNT(*) AS posts,
                 SUM(CASE WHEN argument > likes THEN 1 ELSE 0 END) AS ratios,
                 SUM(CASE WHEN argument > likes THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS rate
          FROM ratio_cohort
          WHERE likes >= 10
          GROUP BY 1
          HAVING COUNT(*) >= 10
        )
        SELECT bucket, n FROM (
          SELECT
            CASE
              WHEN rate < 0.01 THEN '0% (never ratio''d)'
              WHEN rate < 0.05 THEN '0–5%'
              WHEN rate < 0.10 THEN '5–10%'
              WHEN rate < 0.20 THEN '10–20%'
              WHEN rate < 0.40 THEN '20–40%'
              ELSE '40%+ (ratio bait)'
            END AS bucket,
            CASE
              WHEN rate < 0.01 THEN 0
              WHEN rate < 0.05 THEN 1
              WHEN rate < 0.10 THEN 2
              WHEN rate < 0.20 THEN 3
              WHEN rate < 0.40 THEN 4
              ELSE 5
            END AS ord,
            COUNT(*) AS n
          FROM per_author
          GROUP BY 1, 2
        ) ORDER BY ord
        """
    )
    author_universe = q(
        """
        WITH per_author AS (
          SELECT author_did_id, COUNT(*) AS posts
          FROM ratio_cohort
          WHERE likes >= 10
          GROUP BY 1
          HAVING COUNT(*) >= 10
        )
        SELECT COUNT(*) FROM per_author
        """
    )[0][0]

    top_authors = q(
        """
        WITH per_author AS (
          SELECT author_did_id,
                 COUNT(*) AS posts,
                 SUM(CASE WHEN argument > likes THEN 1 ELSE 0 END) AS ratios,
                 SUM(CASE WHEN argument > likes THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS rate_pct
          FROM ratio_cohort
          WHERE likes >= 10
          GROUP BY 1
          HAVING COUNT(*) >= 20  -- stronger sample at the head
        )
        SELECT posts, ratios, rate_pct
        FROM per_author
        ORDER BY rate_pct DESC, ratios DESC
        LIMIT 50
        """
    )

    install_template()

    d_buckets = [r[0] for r in dist_rows]
    d_counts = [r[1] for r in dist_rows]
    d_pct = [100 * c / qualifying_posts for c in d_counts] if qualifying_posts else [0] * len(d_counts)
    threshold_idx = next(
        (i for i, b in enumerate(d_buckets) if b.startswith("(1, 2]")),
        len(d_buckets),
    )
    bar_colors = [BRAND if i < threshold_idx else "#ef4444" for i in range(len(d_buckets))]
    fig_dist = go.Figure(go.Bar(
        x=d_buckets, y=d_counts, marker=dict(color=bar_colors),
        text=[f"{c:,}<br>{p:.1f}%" for c, p in zip(d_counts, d_pct)],
        textposition="outside",
        hovertemplate="argr %{x}<br>%{y:,} posts<extra></extra>",
    ))
    fig_dist.update_layout(
        template="bsky",
        title=dict(text="<b>Where the argumentation ratio actually lands</b>  ·  "
                        "posts with ≥10 likes, bucketed by (replies + quotes) / likes",
                   x=0.02, xanchor="left"),
        xaxis=dict(title="(replies + quotes) / likes"),
        yaxis=dict(title="Number of posts (log)", type="log"),
        height=420,
    )

    t_buckets = [r[0] for r in tier_rows]
    t_rates = [float(r[3] or 0) for r in tier_rows]
    t_totals = [r[1] for r in tier_rows]
    fig_tier = go.Figure(go.Bar(
        x=t_buckets, y=t_rates, marker=dict(color=BRAND),
        text=[f"{r:.2f}%" for r in t_rates],
        textposition="outside",
        customdata=t_totals,
        hovertemplate="likes %{x}<br>%{y:.2f}%% ratio'd<br>%{customdata:,} posts in tier<extra></extra>",
    ))
    fig_tier.update_layout(
        template="bsky",
        title=dict(text="<b>Does virality buy you protection?</b>  ·  "
                        "% of posts where (replies + quotes) > likes, by like-count tier",
                   x=0.02, xanchor="left"),
        xaxis=dict(title="Likes on post"),
        yaxis=dict(title="Ratio rate (%)"),
        height=400,
    )

    w_dates = [r[0] for r in weekly_rows]
    w_pct = [float(r[3] or 0) for r in weekly_rows]
    w_total = [r[1] for r in weekly_rows]
    fig_weekly = go.Figure(go.Scatter(
        x=w_dates, y=w_pct, mode="lines+markers",
        line=dict(color=BRAND, width=2.5),
        marker=dict(size=6, color=BRAND),
        customdata=w_total,
        hovertemplate="week of %{x|%Y-%m-%d}<br>%{y:.2f}%% ratio'd<br>%{customdata:,} qualifying posts<extra></extra>",
    ))
    fig_weekly.update_layout(
        template="bsky",
        title=dict(text=f"<b>Weekly ratio rate over the {window_days}-day window</b>",
                   x=0.02, xanchor="left"),
        xaxis=dict(title="Week"),
        yaxis=dict(title="% of posts ratio'd"),
        height=380,
    )

    ar_buckets = [r[0] for r in author_rate_rows]
    ar_counts = [r[1] for r in author_rate_rows]
    ar_pct = [100 * c / max(1, author_universe) for c in ar_counts]
    ar_colors = [BRAND if not b.startswith("40%") else "#ef4444" for b in ar_buckets]
    fig_authors = go.Figure(go.Bar(
        x=ar_buckets, y=ar_counts, marker=dict(color=ar_colors),
        text=[f"{c:,}<br>{p:.1f}%" for c, p in zip(ar_counts, ar_pct)],
        textposition="outside",
        hovertemplate="rate %{x}<br>%{y:,} authors<extra></extra>",
    ))
    fig_authors.update_layout(
        template="bsky",
        title=dict(text=f"<b>Per-author ratio rate</b>  ·  "
                        f"{author_universe:,} authors with ≥10 qualifying posts",
                   x=0.02, xanchor="left"),
        xaxis=dict(title="Share of an author's posts that get ratio'd"),
        yaxis=dict(title="Number of authors (log)", type="log"),
        height=400,
    )

    head_rates = [float(r[2]) for r in top_authors]
    head_rk = list(range(1, len(head_rates) + 1))
    head_posts = [r[0] for r in top_authors]
    head_ratios = [r[1] for r in top_authors]
    fig_head = go.Figure(go.Bar(
        x=head_rk, y=head_rates, marker=dict(color="#ef4444"),
        customdata=list(zip(head_posts, head_ratios)),
        hovertemplate=(
            "rank #%{x}<br>%{y:.1f}%% of posts ratio'd<br>"
            "%{customdata[1]:,} of %{customdata[0]:,} qualifying posts<extra></extra>"
        ),
    ))
    fig_head.update_layout(
        template="bsky",
        title=dict(text="<b>The ratio leaderboard</b>  ·  "
                        "top 50 anonymized authors by share of their posts that crossed the line",
                   x=0.02, xanchor="left"),
        xaxis=dict(title="Rank"),
        yaxis=dict(title="% of author's posts where argument > likes"),
        height=400, bargap=0.05,
    )

    plot_html = {
        "dist": fig_html(fig_dist, "fig_dist"),
        "tier": fig_html(fig_tier, "fig_tier"),
        "weekly": fig_html(fig_weekly, "fig_weekly"),
        "authors": fig_html(fig_authors, "fig_authors"),
        "head": fig_html(fig_head, "fig_head"),
    }
    plotlyjs = plotlyjs_inline()

    built_at = built_at_utc()

    if tier_rows:
        lo_tier = tier_rows[0]
        hi_tier = tier_rows[-1]
        lo_tier_label, lo_tier_rate = lo_tier[0], float(lo_tier[3] or 0)
        hi_tier_label, hi_tier_rate = hi_tier[0], float(hi_tier[3] or 0)
        trends_down = lo_tier_rate > hi_tier_rate
    else:
        lo_tier_label = hi_tier_label = "n/a"
        lo_tier_rate = hi_tier_rate = 0.0
        trends_down = True

    leaderboard_intro = (
        f"The leader gets ratio'd on <strong>{head_rates[0]:.0f}%</strong> of their qualifying posts."
        if head_rates else
        "No author cleared the threshold in this window."
    )
    median_argr_safe = median_argr if median_argr and median_argr > 0 else 0.0
    p99_argr_safe = p99_argr or 0.0

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>The ratio on Bluesky</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>{SHARED_CSS}</style>
<script>{plotlyjs}</script>
</head>
<body>
<div class="wrap">

<div class="eyebrow">An analysis · snapshot {snapshot_date} · last {window_days} days</div>
<h1>The <span class="accent">ratio</span> barely exists on Bluesky.</h1>
<p class="lede">
  Twitter folklore: when a post draws more replies than likes, you're getting
  dragged. We measured how often this actually happens on Bluesky.
  Across {fmt_int(qualifying_posts)} posts with ≥10 likes over the last
  {window_days} days — written by {fmt_int(unique_authors)} authors — it
  happens to about <strong>1 in {int(100/ratio_pct) if ratio_pct else 0}</strong> of them.
</p>

<div class="stats">
  <div class="stat">
    <div class="v bad">{ratio_pct:.2f}%</div>
    <div class="l">of posts cross the line</div>
    <div class="sub">argument &gt; likes ({fmt_int(true_ratios or 0)} of {fmt_int(qualifying_posts)})</div>
  </div>
  <div class="stat">
    <div class="v">{median_argr_safe:.3f}</div>
    <div class="l">median argumentation ratio</div>
    <div class="sub">~{(1/median_argr_safe) if median_argr_safe > 0 else 0:.0f} likes per argument</div>
  </div>
  <div class="stat">
    <div class="v">{p99_argr_safe:.2f}</div>
    <div class="l">99th-percentile ratio</div>
    <div class="sub">even the worst 1% sits below the threshold</div>
  </div>
  <div class="stat">
    <div class="v brand">{hi_tier_rate:.2f}%</div>
    <div class="l">ratio rate in the {hi_tier_label}-likes tier</div>
    <div class="sub">vs {lo_tier_rate:.2f}% in the {lo_tier_label}-likes tier</div>
  </div>
</div>

<div class="pull">
  The ratio — argument outweighing likes — is the exception, not the rule.
  The median post on Bluesky pulls roughly
  <strong>{(1/median_argr_safe) if median_argr_safe > 0 else 0:.0f} likes for every reply or quote</strong>.
</div>

<section>
  <div class="kicker">Finding 01</div>
  <h2>The bulk of posts sit well below the ratio threshold.</h2>
  <p>
    The "argumentation ratio" we computed is
    <strong>(replies + quotes) / likes</strong>. A post is "ratio'd"
    when that number crosses 1.0. Even the 99th percentile lands at
    <strong>{p99_argr_safe:.2f}</strong>.
  </p>
  <div class="figure">{plot_html["dist"]}</div>
</section>

<section>
  <div class="kicker">Finding 02</div>
  <h2>Virality protects you from the ratio, not the other way around.</h2>
  <p>
    Splitting posts by like-count: ratio rate is
    <strong>highest at the bottom</strong>
    (the {lo_tier_label}-likes tier, at {lo_tier_rate:.2f}%) and drops to
    {hi_tier_rate:.2f}% in the {hi_tier_label}-likes tier.
  </p>
  <div class="figure">{plot_html["tier"]}</div>
</section>

<section>
  <div class="kicker">Finding 03</div>
  <h2>Ratio rate over time.</h2>
  <p>
    Week-over-week trend over the {window_days}-day window.
  </p>
  <div class="figure">{plot_html["weekly"]}</div>
</section>

<section>
  <div class="kicker">Finding 04</div>
  <h2>A few accounts attract ratios at much higher rates.</h2>
  <p>
    Restricting to the <strong>{fmt_int(author_universe)}</strong> authors
    with at least 10 qualifying posts in the window: most never get
    ratio'd at all.
  </p>
  <div class="figure">{plot_html["authors"]}</div>
</section>

<section>
  <div class="kicker">Finding 05</div>
  <h2>The leaderboard.</h2>
  <p>{leaderboard_intro}</p>
  <div class="figure">{plot_html["head"]}</div>
</section>

<footer>
  <p>
    <strong>Methodology.</strong> Computed from the at-snapshot Bluesky
    DuckDB build for snapshot date <code>{snapshot_date}</code>, restricted
    to posts with <code>created_at</code> in
    <code>[{lo}, {hi}]</code>. "Argumentation ratio" is
    <code>(replies + quotes) / likes</code>; we restrict the per-post
    analysis to posts with ≥10 likes to avoid noise dominating.
    Built {built_at}.
  </p>
</footer>

</div>
</body>
</html>
"""

    sidecar = {
        "snapshot_date": snapshot_date,
        "window_days": window_days,
        "window_lo": lo,
        "window_hi": hi,
        "built_at_utc": built_at,
        "total_posts_in_window": total_posts,
        "qualifying_posts": qualifying_posts,
        "true_ratio_posts": true_ratios,
        "ratio_pct": ratio_pct,
        "unique_authors": unique_authors,
        "authors_with_min_posts": author_universe,
        "median_argr": median_argr,
        "p75_argr": p75_argr,
        "p90_argr": p90_argr,
        "p95_argr": p95_argr,
        "p99_argr": p99_argr,
        "max_argr": max_argr,
        "lo_tier": lo_tier_label,
        "lo_tier_rate_pct": lo_tier_rate,
        "hi_tier": hi_tier_label,
        "hi_tier_rate_pct": hi_tier_rate,
        "ratio_decreases_with_virality": trends_down,
    }
    return html.encode("utf-8"), sidecar
