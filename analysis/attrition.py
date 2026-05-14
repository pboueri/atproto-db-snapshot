"""Regrettable-attrition analysis.

Reads `likes`, `posts`, `follows`, `reposts`, and `actor_aggs` from a
snapshot.duckdb connection. Computes per-actor first_seen / last_seen,
buckets actors by engagement tier, and measures inactivity rate.

Public entrypoint: `run(con, snapshot_date, inactivity_days=30)`.
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
    inactivity_days: int = 30,
    *,
    log: bool = True,
) -> tuple[bytes, dict]:
    import plotly.graph_objects as go

    snap = snapshot_date
    inactive_cutoff = (date.fromisoformat(snap) - timedelta(days=inactivity_days)).isoformat()
    plausibility_lo = "2022-01-01"
    if log:
        print(
            f"=== inactivity cutoff: last_seen < {inactive_cutoff} "
            f"(={inactivity_days}d before {snap}) ===",
            flush=True,
        )

    if log:
        print("=== materialize actor_activity ===", flush=True)
    t0 = time.time()
    con.execute(
        f"""
        CREATE OR REPLACE TEMPORARY TABLE actor_activity AS
        WITH per_table AS (
          SELECT actor_did_id AS did_id,
                 MIN(created_at) AS mn, MAX(created_at) AS mx, COUNT(*) AS n
          FROM likes
          WHERE created_at BETWEEN TIMESTAMP '{plausibility_lo}'
                               AND TIMESTAMP '{snap}'
          GROUP BY 1
          UNION ALL
          SELECT author_did_id, MIN(created_at), MAX(created_at), COUNT(*)
          FROM posts
          WHERE created_at BETWEEN TIMESTAMP '{plausibility_lo}'
                               AND TIMESTAMP '{snap}'
          GROUP BY 1
          UNION ALL
          SELECT src_did_id, MIN(created_at), MAX(created_at), COUNT(*)
          FROM follows
          WHERE created_at BETWEEN TIMESTAMP '{plausibility_lo}'
                               AND TIMESTAMP '{snap}'
          GROUP BY 1
          UNION ALL
          SELECT actor_did_id, MIN(created_at), MAX(created_at), COUNT(*)
          FROM reposts
          WHERE created_at BETWEEN TIMESTAMP '{plausibility_lo}'
                               AND TIMESTAMP '{snap}'
          GROUP BY 1
        )
        SELECT did_id,
               MIN(mn) AS first_seen,
               MAX(mx) AS last_seen,
               SUM(n)::BIGINT AS total_actions
        FROM per_table
        GROUP BY 1
        """
    )
    if log:
        print(f"  ({time.time()-t0:.1f}s) actor_activity materialized", flush=True)

    if log:
        print("=== materialize actor_with_tier ===", flush=True)
    t0 = time.time()
    con.execute(
        f"""
        CREATE OR REPLACE TEMPORARY TABLE actor_with_tier AS
        SELECT
          a.did_id,
          a.first_seen,
          a.last_seen,
          a.total_actions,
          COALESCE(aa.posts, 0)     AS posts,
          COALESCE(aa.likes_in, 0)  AS likes_in,
          COALESCE(aa.followers, 0) AS followers,
          CASE
            WHEN COALESCE(aa.posts, 0) >= 100
              OR COALESCE(aa.likes_in, 0) >= 10000
              OR COALESCE(aa.followers, 0) >= 1000 THEN 'power'
            WHEN COALESCE(aa.posts, 0) >= 10
              OR COALESCE(aa.likes_in, 0) >= 100
              OR COALESCE(aa.followers, 0) >= 10 THEN 'engaged'
            WHEN COALESCE(aa.posts, 0) >= 1
              OR COALESCE(aa.followers, 0) >= 1 THEN 'casual'
            ELSE 'lurker'
          END AS tier,
          (a.last_seen < TIMESTAMP '{inactive_cutoff}') AS inactive
        FROM actor_activity a
        LEFT JOIN actor_aggs aa USING(did_id)
        """
    )
    if log:
        print(f"  ({time.time()-t0:.1f}s) actor_with_tier materialized", flush=True)

    def q(sql):
        return timed_query(con, sql, log=log)

    headline = q(
        """
        SELECT
          COUNT(*)                                          AS total,
          COUNT(*) FILTER (WHERE tier IN ('engaged','power')) AS engaged_pop,
          COUNT(*) FILTER (WHERE tier IN ('engaged','power') AND inactive) AS engaged_inactive,
          COUNT(*) FILTER (WHERE tier='power')              AS power_pop,
          COUNT(*) FILTER (WHERE tier='power' AND inactive) AS power_inactive,
          COUNT(*) FILTER (WHERE inactive)                  AS total_inactive
        FROM actor_with_tier
        """
    )[0]
    (total_actors, engaged_pop, engaged_inactive,
     power_pop, power_inactive, total_inactive) = headline
    regret_rate = 100.0 * engaged_inactive / engaged_pop if engaged_pop else 0
    power_regret = 100.0 * power_inactive / power_pop if power_pop else 0
    overall_inactive_pct = 100.0 * total_inactive / total_actors if total_actors else 0

    tier_rows = q(
        """
        SELECT tier, n, inactive, 100.0 * inactive / n AS inactive_pct FROM (
          SELECT
            tier,
            CASE tier
              WHEN 'lurker' THEN 0
              WHEN 'casual' THEN 1
              WHEN 'engaged' THEN 2
              WHEN 'power' THEN 3
            END AS ord,
            COUNT(*) AS n,
            SUM(CASE WHEN inactive THEN 1 ELSE 0 END) AS inactive
          FROM actor_with_tier
          GROUP BY 1, 2
        ) ORDER BY ord
        """
    )

    cohort_rows = q(
        f"""
        WITH cohorts AS (
          SELECT
            DATE_TRUNC('month', first_seen) AS cohort,
            tier,
            COUNT(*) AS n,
            SUM(CASE WHEN inactive THEN 0 ELSE 1 END) AS still_active
          FROM actor_with_tier
          WHERE first_seen >= TIMESTAMP '{plausibility_lo}'
          GROUP BY 1, 2
        )
        SELECT
          cohort,
          SUM(n) AS cohort_size,
          SUM(CASE WHEN tier IN ('engaged','power') THEN n ELSE 0 END) AS engaged,
          SUM(CASE WHEN tier IN ('engaged','power') THEN still_active ELSE 0 END) AS engaged_active,
          SUM(still_active) AS active_now,
          100.0 * SUM(still_active) / SUM(n) AS active_pct,
          CASE WHEN SUM(CASE WHEN tier IN ('engaged','power') THEN n ELSE 0 END) > 0
               THEN 100.0 * SUM(CASE WHEN tier IN ('engaged','power') THEN still_active ELSE 0 END)
                          / SUM(CASE WHEN tier IN ('engaged','power') THEN n ELSE 0 END)
               ELSE NULL END AS engaged_active_pct
        FROM cohorts
        GROUP BY 1
        ORDER BY 1
        """
    )

    timeline_rows = q(
        f"""
        SELECT
          DATE_TRUNC('week', last_seen) AS week,
          COUNT(*) AS n
        FROM actor_with_tier
        WHERE tier IN ('engaged','power')
          AND last_seen < TIMESTAMP '{inactive_cutoff}'
          AND last_seen >= TIMESTAMP '{plausibility_lo}'
        GROUP BY 1
        ORDER BY 1
        """
    )

    inactive_dist = q(
        f"""
        SELECT bucket, n FROM (
          SELECT
            CASE
              WHEN DATEDIFF('day', last_seen, TIMESTAMP '{snap}') <= 7   THEN '≤7d (active)'
              WHEN DATEDIFF('day', last_seen, TIMESTAMP '{snap}') <= 30  THEN '8–30d'
              WHEN DATEDIFF('day', last_seen, TIMESTAMP '{snap}') <= 90  THEN '31–90d'
              WHEN DATEDIFF('day', last_seen, TIMESTAMP '{snap}') <= 180 THEN '91–180d'
              WHEN DATEDIFF('day', last_seen, TIMESTAMP '{snap}') <= 365 THEN '181–365d'
              ELSE '365d+'
            END AS bucket,
            CASE
              WHEN DATEDIFF('day', last_seen, TIMESTAMP '{snap}') <= 7   THEN 0
              WHEN DATEDIFF('day', last_seen, TIMESTAMP '{snap}') <= 30  THEN 1
              WHEN DATEDIFF('day', last_seen, TIMESTAMP '{snap}') <= 90  THEN 2
              WHEN DATEDIFF('day', last_seen, TIMESTAMP '{snap}') <= 180 THEN 3
              WHEN DATEDIFF('day', last_seen, TIMESTAMP '{snap}') <= 365 THEN 4
              ELSE 5
            END AS ord,
            COUNT(*) AS n
          FROM actor_with_tier
          WHERE tier IN ('engaged','power')
          GROUP BY 1, 2
        ) ORDER BY ord
        """
    )

    install_template()

    t_labels = [r[0] for r in tier_rows]
    t_total = [r[1] for r in tier_rows]
    t_pct = [float(r[3] or 0) for r in tier_rows]
    tier_colors = {"lurker": "#9ca3af", "casual": "#cbd5e1",
                   "engaged": BRAND, "power": "#ef4444"}
    fig_tier = go.Figure(go.Bar(
        x=t_labels, y=t_pct,
        marker=dict(color=[tier_colors.get(l, BRAND) for l in t_labels]),
        text=[f"{p:.1f}%" for p in t_pct],
        textposition="outside",
        customdata=t_total,
        hovertemplate="tier %{x}<br>%{y:.1f}%% inactive<br>%{customdata:,} accounts<extra></extra>",
    ))
    fig_tier.update_layout(
        template="bsky",
        title=dict(text=f"<b>Inactivity rate by engagement tier</b>  ·  "
                        f"no activity in last {inactivity_days} days",
                   x=0.02, xanchor="left"),
        xaxis=dict(title="Engagement tier"),
        yaxis=dict(title=f"% inactive ({inactivity_days}d+)"),
        height=400,
    )

    c_months = [r[0] for r in cohort_rows]
    c_size = [r[1] for r in cohort_rows]
    c_active_pct = [float(r[5] or 0) for r in cohort_rows]
    c_eng_pct = [float(r[6] or 0) if r[6] is not None else None for r in cohort_rows]
    fig_cohort = go.Figure()
    fig_cohort.add_trace(go.Scatter(
        x=c_months, y=c_active_pct, mode="lines+markers",
        name="All accounts",
        line=dict(color="#9ca3af", width=2),
        marker=dict(size=5),
        customdata=c_size,
        hovertemplate="cohort %{x|%Y-%m}<br>%{y:.1f}%% still active<br>%{customdata:,} in cohort<extra></extra>",
    ))
    fig_cohort.add_trace(go.Scatter(
        x=c_months, y=c_eng_pct, mode="lines+markers",
        name="Engaged + power tier",
        line=dict(color=BRAND, width=2.5),
        marker=dict(size=5),
        hovertemplate="cohort %{x|%Y-%m}<br>%{y:.1f}%% of engaged still active<extra></extra>",
    ))
    fig_cohort.update_layout(
        template="bsky",
        title=dict(text="<b>Cohort retention</b>  ·  share of each monthly signup cohort "
                        f"still active in the last {inactivity_days} days",
                   x=0.02, xanchor="left"),
        xaxis=dict(title="First-seen cohort (month)"),
        yaxis=dict(title=f"% active in last {inactivity_days}d", range=[0, 100]),
        height=420, legend=dict(orientation="h", y=-0.18),
    )

    w_dates = [r[0] for r in timeline_rows]
    w_counts = [r[1] for r in timeline_rows]
    fig_timeline = go.Figure(go.Bar(
        x=w_dates, y=w_counts, marker=dict(color="#ef4444"),
        hovertemplate="week of %{x|%Y-%m-%d}<br>%{y:,} engaged accounts went quiet<extra></extra>",
    ))
    fig_timeline.update_layout(
        template="bsky",
        title=dict(text="<b>When did the dropouts happen?</b>  ·  "
                        "weekly count of engaged+ accounts whose last action falls in this week "
                        f"(and never came back through {snap})",
                   x=0.02, xanchor="left"),
        xaxis=dict(title="Week of last activity"),
        yaxis=dict(title="Engaged accounts going quiet that week"),
        height=380,
    )

    i_labels = [r[0] for r in inactive_dist]
    i_counts = [r[1] for r in inactive_dist]
    i_pct = [100.0 * c / engaged_pop for c in i_counts] if engaged_pop else [0] * len(i_counts)
    i_colors = ["#16a34a" if "active" in l else "#ef4444" if "365" in l else BRAND for l in i_labels]
    fig_dist = go.Figure(go.Bar(
        x=i_labels, y=i_counts, marker=dict(color=i_colors),
        text=[f"{c:,}<br>{p:.1f}%" for c, p in zip(i_counts, i_pct)],
        textposition="outside",
        hovertemplate="%{x}<br>%{y:,} engaged accounts<extra></extra>",
    ))
    fig_dist.update_layout(
        template="bsky",
        title=dict(text="<b>How long since the engaged base last did anything?</b>  ·  "
                        f"engaged + power tier ({engaged_pop:,} accounts)",
                   x=0.02, xanchor="left"),
        xaxis=dict(title="Days since last action"),
        yaxis=dict(title="Number of accounts (log)", type="log"),
        height=400,
    )

    plot_html = {
        "tier": fig_html(fig_tier, "fig_tier"),
        "cohort": fig_html(fig_cohort, "fig_cohort"),
        "timeline": fig_html(fig_timeline, "fig_timeline"),
        "dist": fig_html(fig_dist, "fig_dist"),
    }
    plotlyjs = plotlyjs_inline()

    built_at = built_at_utc()

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Regrettable attrition on Bluesky</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>{SHARED_CSS}</style>
<script>{plotlyjs}</script>
</head>
<body>
<div class="wrap">

<div class="eyebrow">An analysis · snapshot {snapshot_date} · {inactivity_days}d inactivity threshold</div>
<h1>Bluesky's <span class="accent">regrettable attrition</span>.</h1>
<p class="lede">
  Headline user counts are misleading. The harder question is: of the
  people who actually engaged on Bluesky — posted, were liked, built an
  audience — how many are still showing up?
  Across {fmt_int(total_actors)} accounts with any outbound activity,
  {fmt_int(engaged_pop)} cleared the "engaged" bar (≥10 posts OR ≥100
  lifetime likes received OR ≥10 followers). Of those,
  <strong>{regret_rate:.1f}% have gone quiet</strong> for more than
  {inactivity_days} days.
</p>

<div class="stats">
  <div class="stat">
    <div class="v bad">{regret_rate:.1f}%</div>
    <div class="l">engaged accounts now inactive</div>
    <div class="sub">{fmt_int(engaged_inactive)} of {fmt_int(engaged_pop)}</div>
  </div>
  <div class="stat">
    <div class="v bad">{power_regret:.1f}%</div>
    <div class="l">"power-tier" accounts now inactive</div>
    <div class="sub">≥100 posts or ≥10k likes_in or ≥1k followers</div>
  </div>
  <div class="stat">
    <div class="v">{overall_inactive_pct:.0f}%</div>
    <div class="l">of all accounts inactive {inactivity_days}d+</div>
    <div class="sub">{fmt_int(total_inactive)} of {fmt_int(total_actors)}</div>
  </div>
  <div class="stat">
    <div class="v brand">{fmt_int(engaged_pop - engaged_inactive)}</div>
    <div class="l">engaged accounts still active</div>
    <div class="sub">the actually-living core of the network</div>
  </div>
</div>

<div class="pull">
  Roughly {regret_rate:.0f} out of every 100 accounts that ever cleared
  the engagement bar have stopped posting, liking, following, or
  reposting for at least {inactivity_days} days.
</div>

<section>
  <div class="kicker">Finding 01</div>
  <h2>Higher-engagement tiers stick better — but the dropout rate is still substantial.</h2>
  <p>
    Inactivity rate falls as engagement rises. Even in the "power" tier,
    <strong>{power_regret:.1f}%</strong> have gone quiet for more than
    {inactivity_days} days.
  </p>
  <div class="figure">{plot_html["tier"]}</div>
</section>

<section>
  <div class="kicker">Finding 02</div>
  <h2>Retention varies sharply by signup cohort.</h2>
  <p>
    Splitting accounts by the month they first did anything on Bluesky.
  </p>
  <div class="figure">{plot_html["cohort"]}</div>
</section>

<section>
  <div class="kicker">Finding 03</div>
  <h2>When did the engaged accounts go quiet?</h2>
  <p>
    Bucketing engaged accounts by the week of their final outbound action.
  </p>
  <div class="figure">{plot_html["timeline"]}</div>
</section>

<section>
  <div class="kicker">Finding 04</div>
  <h2>How long is the engaged base "stale"?</h2>
  <p>
    Distribution of days-since-last-action across the
    {fmt_int(engaged_pop)} engaged accounts.
  </p>
  <div class="figure">{plot_html["dist"]}</div>
</section>

<footer>
  <p>
    <strong>Methodology.</strong> Computed from the at-snapshot Bluesky
    DuckDB build for snapshot date <code>{snapshot_date}</code>.
    Per-actor <code>first_seen</code> and <code>last_seen</code> are
    derived from <code>MIN/MAX(created_at)</code> across <code>likes</code>,
    <code>posts</code>, <code>follows</code>, and <code>reposts</code>,
    plausibility-filtered to <code>created_at &gt; 2022-01-01</code>.
    "Engaged" = ≥10 posts OR ≥100 lifetime likes_in OR ≥10 followers.
    "Power" raises those thresholds 10×. "Inactive" =
    <code>last_seen &lt; snapshot_date − {inactivity_days} days</code>.
    Built {built_at}.
  </p>
</footer>

</div>
</body>
</html>
"""

    sidecar = {
        "snapshot_date": snapshot_date,
        "inactivity_days": inactivity_days,
        "inactive_cutoff": inactive_cutoff,
        "built_at_utc": built_at,
        "total_actors": total_actors,
        "engaged_pop": engaged_pop,
        "engaged_inactive": engaged_inactive,
        "regret_rate_pct": regret_rate,
        "power_pop": power_pop,
        "power_inactive": power_inactive,
        "power_regret_pct": power_regret,
        "total_inactive": total_inactive,
        "overall_inactive_pct": overall_inactive_pct,
        "tier_breakdown": [
            {"tier": r[0], "n": r[1], "inactive": r[2], "inactive_pct": float(r[3] or 0)}
            for r in tier_rows
        ],
    }
    return html.encode("utf-8"), sidecar
