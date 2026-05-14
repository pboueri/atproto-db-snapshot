"""Like-concentration analysis.

Reads `actor_aggs` and `post_aggs` from a snapshot.duckdb connection,
produces a self-contained HTML report on how like-receipt is distributed
across posters, and a JSON sidecar of the headline numbers.

Public entrypoint: `run(con, snapshot_date) -> (html_bytes, sidecar_dict)`.
"""

from __future__ import annotations

from .common import (
    BRAND, SHARED_CSS,
    built_at_utc, fig_html, fmt_int, install_template,
    plotlyjs_inline, timed_query,
)


def run(con, snapshot_date: str, *, log: bool = True) -> tuple[bytes, dict]:
    import plotly.graph_objects as go

    if log:
        print("=== run queries ===", flush=True)

    def q(sql, params=None):
        return timed_query(con, sql, params, log=log)

    # --- headline numbers -------------------------------------------------
    headline = q(
        """
        WITH posters AS (SELECT * FROM actor_aggs WHERE posts > 0)
        SELECT
          (SELECT COUNT(*) FROM actor_aggs)               AS total_actors,
          (SELECT COUNT(*) FROM posters)                  AS total_posters,
          (SELECT SUM(likes_in) FROM posters)             AS total_likes_in,
          (SELECT COUNT(*) FROM post_aggs)                AS total_posts,
          (SELECT COUNT(*) FROM post_aggs WHERE likes=0)  AS posts_zero_likes,
          (SELECT COUNT(*) FROM post_aggs
             WHERE likes=0 AND reposts=0
               AND replies=0 AND quotes=0)                AS posts_zero_eng,
          (SELECT COUNT(*) FROM posters WHERE likes_in=0) AS posters_zero,
          (SELECT MEDIAN(likes_in) FROM posters)          AS median_li,
          (SELECT AVG(likes_in)::DOUBLE FROM posters)     AS mean_li,
          (SELECT MAX(likes_in) FROM posters)             AS max_li,
          (SELECT QUANTILE_CONT(likes_in, 0.99) FROM posters) AS p99_li
        """
    )[0]
    (total_actors, total_posters, total_likes_in, total_posts,
     posts_zero_likes, posts_zero_eng, posters_zero,
     median_li, mean_li, max_li, p99_li) = headline

    # --- Lorenz / Gini via NTILE bucketing --------------------------------
    # Sort posters once, bucket into 500 quantiles, aggregate per bucket.
    lorenz_rows = q(
        """
        WITH posters AS (SELECT likes_in FROM actor_aggs WHERE posts > 0),
        bucketed AS (
          SELECT NTILE(500) OVER (ORDER BY likes_in) AS bucket, likes_in
          FROM posters
        )
        SELECT bucket,
               COUNT(*) AS n,
               SUM(likes_in) AS sum_likes,
               MIN(likes_in) AS min_likes,
               MAX(likes_in) AS max_likes
        FROM bucketed
        GROUP BY bucket
        ORDER BY bucket
        """
    )
    cum_n = 0
    cum_likes = 0
    total_n_lorenz = sum(r[1] for r in lorenz_rows) or 1
    total_likes_lorenz = sum(r[2] or 0 for r in lorenz_rows) or 1
    lorenz_x = [0.0]
    lorenz_y = [0.0]
    for _bucket, n, sum_l, _mn, _mx in lorenz_rows:
        cum_n += n
        cum_likes += (sum_l or 0)
        lorenz_x.append(cum_n / total_n_lorenz)
        lorenz_y.append(cum_likes / total_likes_lorenz)
    # Gini = 1 - 2 * area-under-Lorenz (trapezoid).
    area = 0.0
    for i in range(1, len(lorenz_x)):
        area += 0.5 * (lorenz_y[i] + lorenz_y[i - 1]) * (
            lorenz_x[i] - lorenz_x[i - 1]
        )
    gini = 1.0 - 2.0 * area

    # Top-N shares: precise direct query.
    top_share_row = q(
        """
        WITH agg AS (
          SELECT COUNT(*) AS n, SUM(likes_in)::DOUBLE AS tot
          FROM actor_aggs WHERE posts > 0
        ),
        ranked AS (
          SELECT likes_in, ROW_NUMBER() OVER (ORDER BY likes_in DESC) AS rk
          FROM actor_aggs WHERE posts > 0
        )
        SELECT
          (SUM(likes_in) FILTER (WHERE rk <= GREATEST(1, (SELECT n FROM agg) * 0.0001)))::DOUBLE / (SELECT tot FROM agg) AS top_001,
          (SUM(likes_in) FILTER (WHERE rk <= GREATEST(1, (SELECT n FROM agg) * 0.001 )))::DOUBLE / (SELECT tot FROM agg) AS top_01,
          (SUM(likes_in) FILTER (WHERE rk <= GREATEST(1, (SELECT n FROM agg) * 0.01  )))::DOUBLE / (SELECT tot FROM agg) AS top_1,
          (SUM(likes_in) FILTER (WHERE rk <= GREATEST(1, (SELECT n FROM agg) * 0.10  )))::DOUBLE / (SELECT tot FROM agg) AS top_10,
          (SUM(likes_in) FILTER (WHERE rk <= GREATEST(1, (SELECT n FROM agg) * 0.50  )))::DOUBLE / (SELECT tot FROM agg) AS top_50
        FROM ranked
        """
    )[0]
    top_buckets = [
        ("Top 0.01%", 0.0001, float(top_share_row[0] or 0)),
        ("Top 0.1%",  0.001,  float(top_share_row[1] or 0)),
        ("Top 1%",    0.01,   float(top_share_row[2] or 0)),
        ("Top 10%",   0.10,   float(top_share_row[3] or 0)),
        ("Top 50%",   0.50,   float(top_share_row[4] or 0)),
    ]
    top_shares = [(label, share, frac) for label, frac, share in top_buckets]
    bottom50_share = max(0.0, 1.0 - top_buckets[4][2])

    # --- posters by likes-received tier ----------------------------------
    poster_tier_rows = q(
        """
        SELECT bucket, n FROM (
          SELECT
            CASE
              WHEN likes_in = 0          THEN '0'
              WHEN likes_in < 10         THEN '1–9'
              WHEN likes_in < 100        THEN '10–99'
              WHEN likes_in < 1000       THEN '100–999'
              WHEN likes_in < 10000      THEN '1k–9.9k'
              WHEN likes_in < 100000     THEN '10k–99k'
              WHEN likes_in < 1000000    THEN '100k–999k'
              ELSE '1M+'
            END AS bucket,
            CASE
              WHEN likes_in = 0          THEN 0
              WHEN likes_in < 10         THEN 1
              WHEN likes_in < 100        THEN 2
              WHEN likes_in < 1000       THEN 3
              WHEN likes_in < 10000      THEN 4
              WHEN likes_in < 100000     THEN 5
              WHEN likes_in < 1000000    THEN 6
              ELSE 7
            END AS ord,
            COUNT(*) AS n
          FROM actor_aggs WHERE posts > 0
          GROUP BY 1, 2
        ) ORDER BY ord
        """
    )

    post_tier_rows = q(
        """
        SELECT bucket, n FROM (
          SELECT
            CASE
              WHEN likes = 0       THEN '0'
              WHEN likes < 10      THEN '1–9'
              WHEN likes < 100     THEN '10–99'
              WHEN likes < 1000    THEN '100–999'
              WHEN likes < 10000   THEN '1k–9.9k'
              WHEN likes < 100000  THEN '10k–99k'
              ELSE '100k+'
            END AS bucket,
            CASE
              WHEN likes = 0       THEN 0
              WHEN likes < 10      THEN 1
              WHEN likes < 100     THEN 2
              WHEN likes < 1000    THEN 3
              WHEN likes < 10000   THEN 4
              WHEN likes < 100000  THEN 5
              ELSE 6
            END AS ord,
            COUNT(*) AS n
          FROM post_aggs
          GROUP BY 1, 2
        ) ORDER BY ord
        """
    )

    follower_rows = q(
        """
        SELECT bucket, posters, total_posts, total_likes,
               total_likes::DOUBLE / NULLIF(total_posts, 0) AS likes_per_post
        FROM (
          SELECT
            CASE
              WHEN followers = 0          THEN '0'
              WHEN followers < 10         THEN '1–9'
              WHEN followers < 100        THEN '10–99'
              WHEN followers < 1000       THEN '100–999'
              WHEN followers < 10000      THEN '1k–9.9k'
              WHEN followers < 100000     THEN '10k–99k'
              WHEN followers < 1000000    THEN '100k–999k'
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
            COUNT(*) AS posters,
            SUM(posts) AS total_posts,
            SUM(likes_in) AS total_likes
          FROM actor_aggs WHERE posts > 0
          GROUP BY 1, 2
        ) ORDER BY ord
        """
    )

    ladder_rows = q(
        """
        SELECT likes_in
        FROM actor_aggs
        WHERE posts > 0 AND likes_in > 0
        ORDER BY likes_in DESC
        LIMIT 1000
        """
    )

    # ---------------------------------------------------------------------
    # Plotly figures
    # ---------------------------------------------------------------------
    install_template()

    def _fmt_pct(v: float) -> str:
        return f"{v * 100:.1f}%"

    fig_lorenz = go.Figure()
    fig_lorenz.add_trace(go.Scatter(
        x=[0, 1], y=[0, 1], mode="lines",
        line=dict(dash="dash", color="#9ca3af", width=1),
        name="Perfect equality", hoverinfo="skip",
    ))
    fig_lorenz.add_trace(go.Scatter(
        x=lorenz_x, y=lorenz_y, mode="lines",
        line=dict(color=BRAND, width=2.5),
        name="Bluesky likes",
        hovertemplate="bottom %{x:.1%} of posters<br>get %{y:.1%} of likes<extra></extra>",
        fill="tozeroy", fillcolor="rgba(0,133,255,0.08)",
    ))
    fig_lorenz.update_layout(
        template="bsky",
        title=dict(text=f"<b>Lorenz curve of likes received per poster</b>  ·  Gini = {gini:.3f}",
                   x=0.02, xanchor="left"),
        xaxis=dict(title="Cumulative share of posters (poorest → richest)",
                   tickformat=".0%", range=[0, 1]),
        yaxis=dict(title="Cumulative share of all likes received",
                   tickformat=".0%", range=[0, 1]),
        height=440,
    )

    labels = [b[0] for b in top_shares] + ["Bottom 50%"]
    values = [b[1] for b in top_shares] + [bottom50_share]
    fig_topn = go.Figure(go.Bar(
        x=values, y=labels, orientation="h",
        marker=dict(color=[BRAND, BRAND, BRAND, BRAND, "#9ca3af", "#d1d5db"]),
        text=[_fmt_pct(v) for v in values],
        textposition="outside",
        hovertemplate="%{y}: %{x:.2%} of all likes<extra></extra>",
    ))
    fig_topn.update_layout(
        template="bsky",
        title=dict(text="<b>Where the likes go</b>  ·  share of all likes received, by poster percentile",
                   x=0.02, xanchor="left"),
        xaxis=dict(title="Share of all likes received", tickformat=".0%",
                   range=[0, max(values) * 1.15 if values else 1]),
        yaxis=dict(autorange="reversed"),
        height=380,
    )

    p_buckets = [r[0] for r in poster_tier_rows]
    p_counts = [r[1] for r in poster_tier_rows]
    p_pct = [100 * c / total_posters for c in p_counts] if total_posters else [0] * len(p_counts)
    fig_p_tier = go.Figure(go.Bar(
        x=p_buckets, y=p_counts,
        marker=dict(color=[BRAND if i > 0 else "#ef4444" for i in range(len(p_buckets))]),
        text=[f"{c:,}<br>{p:.1f}%" for c, p in zip(p_counts, p_pct)],
        textposition="outside",
        hovertemplate="%{x} likes received<br>%{y:,} posters<extra></extra>",
    ))
    fig_p_tier.update_layout(
        template="bsky",
        title=dict(text="<b>How many likes does the average poster receive?</b>  ·  posters bucketed by lifetime likes_in",
                   x=0.02, xanchor="left"),
        xaxis=dict(title="Lifetime likes received"),
        yaxis=dict(title="Number of posters (log)", type="log"),
        height=400,
    )

    po_buckets = [r[0] for r in post_tier_rows]
    po_counts = [r[1] for r in post_tier_rows]
    po_pct = [100 * c / total_posts for c in po_counts] if total_posts else [0] * len(po_counts)
    fig_po_tier = go.Figure(go.Bar(
        x=po_buckets, y=po_counts,
        marker=dict(color=["#ef4444"] + [BRAND] * (len(po_buckets) - 1)),
        text=[f"{c:,}<br>{p:.1f}%" for c, p in zip(po_counts, po_pct)],
        textposition="outside",
        hovertemplate="%{x} likes<br>%{y:,} posts<extra></extra>",
    ))
    fig_po_tier.update_layout(
        template="bsky",
        title=dict(text="<b>How many likes does the average post receive?</b>  ·  posts bucketed by likes",
                   x=0.02, xanchor="left"),
        xaxis=dict(title="Likes on post"),
        yaxis=dict(title="Number of posts (log)", type="log"),
        height=400,
    )

    f_buckets = [r[0] for r in follower_rows]
    f_lpp = [float(r[4]) if r[4] is not None else 0 for r in follower_rows]
    f_posters = [r[1] for r in follower_rows]
    fig_f = go.Figure(go.Bar(
        x=f_buckets, y=f_lpp,
        marker=dict(color=BRAND),
        text=[f"{v:.2f}" for v in f_lpp],
        textposition="outside",
        customdata=f_posters,
        hovertemplate="followers %{x}<br>%{y:.2f} likes per post<br>%{customdata:,} posters<extra></extra>",
    ))
    fig_f.update_layout(
        template="bsky",
        title=dict(text="<b>The follower premium</b>  ·  average likes-per-post by author follower count",
                   x=0.02, xanchor="left"),
        xaxis=dict(title="Author followers"),
        yaxis=dict(title="Average likes per post (log)", type="log"),
        height=400,
    )

    ladder_y = [r[0] for r in ladder_rows]
    ladder_x = list(range(1, len(ladder_y) + 1))
    fig_ladder = go.Figure(go.Bar(
        x=ladder_x, y=ladder_y,
        marker=dict(color=BRAND),
        hovertemplate="rank #%{x}<br>%{y:,} likes received<extra></extra>",
    ))
    fig_ladder.update_layout(
        template="bsky",
        title=dict(text="<b>The head of the distribution</b>  ·  top 1,000 posters by lifetime likes received",
                   x=0.02, xanchor="left"),
        xaxis=dict(title="Rank (most-liked → 1,000th)"),
        yaxis=dict(title="Lifetime likes received (log)", type="log"),
        height=380,
        bargap=0.0,
    )

    plot_html = {
        "lorenz": fig_html(fig_lorenz, "fig_lorenz"),
        "topn": fig_html(fig_topn, "fig_topn"),
        "p_tier": fig_html(fig_p_tier, "fig_p_tier"),
        "po_tier": fig_html(fig_po_tier, "fig_po_tier"),
        "f_premium": fig_html(fig_f, "fig_f"),
        "ladder": fig_html(fig_ladder, "fig_ladder"),
    }
    plotlyjs = plotlyjs_inline()

    pct_posts_zero = 100 * posts_zero_likes / total_posts if total_posts else 0
    pct_posts_zero_eng = 100 * posts_zero_eng / total_posts if total_posts else 0

    built_at = built_at_utc()

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Bluesky's like economy is winner-take-all</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>{SHARED_CSS}</style>
<script>{plotlyjs}</script>
</head>
<body>
<div class="wrap">

<div class="eyebrow">An analysis · snapshot {snapshot_date}</div>
<h1>Bluesky's like economy is <span class="accent">winner-take-all</span>.</h1>
<p class="lede">
  Across {fmt_int(total_posters)} accounts that have ever posted on Bluesky,
  likes received are concentrated to a degree more extreme than nearly any
  studied wealth distribution — and a large fraction of posts get no
  engagement at all.
</p>

<div class="stats">
  <div class="stat">
    <div class="v brand">{top_shares[2][1]*100:.0f}%</div>
    <div class="l">of all likes go to the top 1% of posters</div>
    <div class="sub">{int(top_shares[2][2] * total_posters):,} accounts</div>
  </div>
  <div class="stat">
    <div class="v bad">{pct_posts_zero:.1f}%</div>
    <div class="l">of posts receive zero likes</div>
    <div class="sub">{fmt_int(posts_zero_likes)} of {fmt_int(total_posts)} posts</div>
  </div>
  <div class="stat">
    <div class="v">{fmt_int(median_li or 0)}</div>
    <div class="l">median lifetime likes per poster</div>
    <div class="sub">vs. mean of {(mean_li or 0):,.0f}</div>
  </div>
  <div class="stat">
    <div class="v brand">{gini:.3f}</div>
    <div class="l">Gini coefficient of likes received</div>
    <div class="sub">U.S. wealth Gini ≈ 0.85</div>
  </div>
</div>

<div class="pull">
  Half of all Bluesky posters share less than {bottom50_share*100:.2f}% of all
  likes between them. The top 0.1% — {int(top_shares[1][2] * total_posters):,}
  accounts — receive {top_shares[1][1]*100:.0f}%.
</div>

<section>
  <div class="kicker">Finding 01</div>
  <h2>Likes are distributed more unequally than wealth.</h2>
  <p>
    The Lorenz curve below sorts posters from fewest to most likes received,
    then plots the cumulative share of all likes those posters command. A
    perfectly equal distribution would lie on the dashed diagonal. Bluesky's
    likes hug the bottom-right corner: the bottom 90% of posters together
    receive less than {(1 - top_buckets[3][2])*100:.1f}% of all likes. The Gini coefficient
    is <strong>{gini:.3f}</strong> — for reference, U.S. household income
    sits around 0.49 and household wealth around 0.85.
  </p>
  <div class="figure">{plot_html["lorenz"]}</div>
</section>

<section>
  <div class="kicker">Finding 02</div>
  <h2>The top 1% take {top_shares[2][1]*100:.0f}% of all likes.</h2>
  <p>
    Slicing the same distribution by percentile makes the head heaviness
    obvious. The top 0.01% of posters — roughly
    {int(top_shares[0][2] * total_posters):,} accounts — pull in
    <strong>{top_shares[0][1]*100:.1f}%</strong> of every like ever sent on
    Bluesky. The bottom 50% combined: <strong>{bottom50_share*100:.2f}%</strong>.
  </p>
  <div class="figure">{plot_html["topn"]}</div>
</section>

<section>
  <div class="kicker">Finding 03</div>
  <h2>The median post gets ~1 like. {pct_posts_zero:.0f}% get none.</h2>
  <p>
    Bucketing every post by the number of likes it received shows a
    distribution heavily anchored at zero.
    <strong>{fmt_int(posts_zero_likes)} posts ({pct_posts_zero:.1f}%)</strong>
    received not a single like; <strong>{pct_posts_zero_eng:.1f}%</strong>
    received no engagement of any kind (no likes, reposts, replies, or quotes).
    The y-axis is logarithmic; without it the leftmost bar would dwarf the
    rest of the chart.
  </p>
  <div class="figure">{plot_html["po_tier"]}</div>
</section>

<section>
  <div class="kicker">Finding 04</div>
  <h2>For most posters, lifetime likes are a small number.</h2>
  <p>
    Aggregating to the poster level: the median Bluesky poster has
    accumulated <strong>{fmt_int(median_li or 0)}</strong> total likes across
    every post they have ever made. The 99th percentile is
    {fmt_int(p99_li or 0)}; the maximum is {fmt_int(max_li or 0)}.
  </p>
  <div class="figure">{plot_html["p_tier"]}</div>
</section>

<section>
  <div class="kicker">Finding 05</div>
  <h2>Audience scales engagement super-linearly.</h2>
  <p>
    Average likes per post grows by orders of magnitude as follower count
    rises. Posters with under 10 followers get a tiny fraction of a like
    per post on average; six-figure-follower accounts get hundreds. A
    typical post's expected like count is overwhelmingly determined by who
    wrote it, not by the post's content.
  </p>
  <div class="figure">{plot_html["f_premium"]}</div>
</section>

<section>
  <div class="kicker">Finding 06</div>
  <h2>The head of the curve.</h2>
  <p>
    Sorted bar chart of the top 1,000 posters by lifetime likes received,
    log scale. The drop-off across just the top 1,000 is itself two orders
    of magnitude — and the top 1,000 is roughly the top
    {(100 * 1000 / total_posters) if total_posters else 0:.4f}% of posters.
  </p>
  <div class="figure">{plot_html["ladder"]}</div>
</section>

<footer>
  <p>
    <strong>Methodology.</strong> All counts are derived from the
    <code>actor_aggs</code> and <code>post_aggs</code> tables of the
    at-snapshot Bluesky DuckDB build, snapshot date <code>{snapshot_date}</code>.
    "Posters" are accounts with at least one post in the snapshot.
    "Likes received" is each post's lifetime like count summed across the
    poster's posts, regardless of when those likes happened. The Gini
    coefficient is computed by trapezoid integration of a 500-quantile
    Lorenz curve. Built {built_at}.
  </p>
  <p>
    <strong>Caveats.</strong> The snapshot reflects engagement that has
    been crawled by constellation; very recent likes lag. Suspended /
    deactivated accounts still appear if their data is still indexed.
    Spam-network like-rings, if present, are not separated from organic
    likes.
  </p>
</footer>

</div>
</body>
</html>
"""

    sidecar = {
        "snapshot_date": snapshot_date,
        "built_at_utc": built_at,
        "gini": gini,
        "total_actors": total_actors,
        "total_posters": total_posters,
        "total_posts": total_posts,
        "total_likes_in": int(total_likes_in or 0),
        "posts_zero_likes": posts_zero_likes,
        "posts_zero_eng": posts_zero_eng,
        "posters_zero_likes": posters_zero,
        "median_likes_in": int(median_li or 0),
        "mean_likes_in": float(mean_li or 0),
        "max_likes_in": int(max_li or 0),
        "p99_likes_in": int(p99_li or 0),
        "top_shares": [
            {"label": l, "frac": f, "share": s} for l, s, f in top_shares
        ],
        "bottom50_share": bottom50_share,
    }
    return html.encode("utf-8"), sidecar
