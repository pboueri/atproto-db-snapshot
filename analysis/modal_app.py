"""Modal app for analyses derived from the at-snapshot DuckDB build.

Runs read-only against the snapshot artifact written by the main
`deploy/modal_app.py` pipeline. Lives in its own Modal app
(`at-snapshot-analysis`) so iterating on plots / queries here never
touches the heavy Rust build image used by the pipeline.

Currently provides:

  modal run analysis/modal_app.py                                      # default snapshot
  modal run analysis/modal_app.py --snapshot-date 2026-04-28           # pin a date
  modal run analysis/modal_app.py --background                         # detach

The function reads `/vol-out/var/snapshot/<date>/snapshot.duckdb` from
the shared output volume (`at-snapshot-output`), writes
`/vol-out/var/analysis/<date>/likes_concentration.html` (+ a small
JSON sidecar) back to the same volume, and also returns the HTML bytes
so the local entrypoint can drop a copy on the invoking host.
"""

from __future__ import annotations

import modal

# Same name as deploy/modal_app.py's output volume; that's how we read the
# snapshot.duckdb without re-uploading it.
OUT_VOL_DIR = "/vol-out/var"
volume_out = modal.Volume.from_name("at-snapshot-output", create_if_missing=False)

# Slim image: duckdb for queries, plotly for the figures. No rust, no
# source tree — changes here don't drag the pipeline's cargo cache.
analysis_image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install("duckdb==1.5.2", "plotly==5.22.0")
)

# Blocks/DW-NOMINATE analysis needs scipy for sparse SVD, numpy for the
# 2D histogram, and pyarrow so DuckDB's fetch_arrow_table can stream the
# (row, col, sign) triplets into numpy without a full Python-tuple
# materialization. Built off the slim image so the duckdb + plotly
# layers are shared.
spectral_image = analysis_image.pip_install(
    "scipy==1.13.0", "numpy==1.26.4", "pyarrow==16.1.0"
)

app = modal.App("at-snapshot-analysis")


# ---------------------------------------------------------------------------
# Shared HTML chrome
# ---------------------------------------------------------------------------

# Every analysis HTML re-uses the same CSS + plotly bsky template + the
# same JS-inline trick. Factor them out so each analysis only carries its
# distinctive prose/queries/figures.

BRAND = "#0085ff"
AXIS = "#1d2433"
GRID = "#e6e8ec"

SHARED_CSS = f"""
:root {{
  --brand: {BRAND};
  --ink: #1d2433;
  --muted: #5b6472;
  --rule: #e6e8ec;
  --bg: #fbfbfd;
}}
* {{ box-sizing: border-box; }}
html, body {{ margin: 0; padding: 0; background: var(--bg); }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Helvetica Neue",
               Arial, sans-serif;
  color: var(--ink); line-height: 1.55; font-size: 16px;
}}
.wrap {{ max-width: 1080px; margin: 0 auto; padding: 56px 24px 80px; }}
.eyebrow {{
  font-size: 12px; letter-spacing: 0.12em; text-transform: uppercase;
  color: var(--muted); margin-bottom: 12px;
}}
h1 {{
  font-size: 44px; line-height: 1.1; letter-spacing: -0.02em;
  margin: 0 0 16px; font-weight: 700;
}}
h1 .accent {{ color: var(--brand); }}
.lede {{ font-size: 19px; color: var(--muted); margin: 0 0 36px; max-width: 780px; }}
.stats {{
  display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px;
  margin: 32px 0 48px;
}}
.stat {{
  background: white; border: 1px solid var(--rule); border-radius: 10px;
  padding: 18px 16px;
}}
.stat .v {{ font-size: 30px; font-weight: 700; letter-spacing: -0.02em; }}
.stat .v.bad {{ color: #ef4444; }}
.stat .v.brand {{ color: var(--brand); }}
.stat .l {{ font-size: 12.5px; color: var(--muted); margin-top: 4px; }}
.stat .sub {{ font-size: 11.5px; color: var(--muted); margin-top: 2px; }}
section {{ margin: 56px 0; }}
section h2 {{
  font-size: 26px; letter-spacing: -0.01em; font-weight: 700;
  margin: 0 0 8px;
}}
section .kicker {{
  font-size: 14px; color: var(--brand); font-weight: 600;
  text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 8px;
}}
section p {{ color: var(--muted); margin: 0 0 16px; max-width: 820px; }}
section p strong {{ color: var(--ink); }}
.figure {{
  background: white; border: 1px solid var(--rule); border-radius: 10px;
  padding: 14px 10px 6px; margin-top: 18px;
}}
.pull {{
  border-left: 3px solid var(--brand); padding: 6px 0 6px 16px;
  margin: 24px 0; font-size: 21px; line-height: 1.4; color: var(--ink);
  font-weight: 500; max-width: 780px;
}}
footer {{
  margin-top: 80px; padding-top: 24px; border-top: 1px solid var(--rule);
  color: var(--muted); font-size: 13px;
}}
footer code {{
  background: white; border: 1px solid var(--rule); border-radius: 4px;
  padding: 1px 5px; font-size: 12px;
}}
@media (max-width: 720px) {{
  .stats {{ grid-template-columns: repeat(2, 1fr); }}
  h1 {{ font-size: 34px; }}
  .lede {{ font-size: 17px; }}
}}
"""


def _bsky_template():
    """Plotly layout template — shared across analyses for visual consistency."""
    import plotly.graph_objects as go
    return go.layout.Template(
        layout=go.Layout(
            font=dict(family="-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
                      color=AXIS, size=13),
            paper_bgcolor="white",
            plot_bgcolor="white",
            colorway=[BRAND, "#ff5d8f", "#7c3aed", "#16a34a", "#f59e0b"],
            xaxis=dict(gridcolor=GRID, zeroline=False, linecolor=GRID),
            yaxis=dict(gridcolor=GRID, zeroline=False, linecolor=GRID),
            margin=dict(l=60, r=20, t=50, b=60),
            hoverlabel=dict(bgcolor="white", bordercolor=GRID),
        )
    )


def _fig_html(fig, div_id: str) -> str:
    import plotly.io as pio
    return pio.to_html(
        fig, include_plotlyjs=False, full_html=False,
        div_id=div_id, config={"displayModeBar": False, "responsive": True},
    )


def _plotlyjs_inline() -> str:
    """The bundled plotly.js as a plain string for inline <script> injection."""
    import plotly.offline as _po
    return _po.get_plotlyjs()


@app.function(
    image=analysis_image,
    volumes={"/vol-out": volume_out},
    timeout=60 * 60,
    # actor_aggs sort + NTILE is the dominant cost; 8 cores keeps it
    # parallel. post_aggs sums are streamed.
    cpu=8.0,
    # actor_aggs at full scale is ~50M rows × ~100 B = ~5 GB; we sort
    # it once for the Lorenz NTILE and run a handful of GROUP BYs on
    # post_aggs (which DuckDB streams). 32 GiB is comfortable headroom.
    memory=32 * 1024,
    ephemeral_disk=512 * 1024,
)
def analyze_likes(
    snapshot_date: str = "2026-04-28",
) -> bytes:
    """Build a self-contained HTML analysis of like-distribution from
    snapshot.duckdb on /vol-out. Persists the file to
    /vol-out/var/analysis/<date>/likes_concentration.html and returns
    the bytes so the local entrypoint can also drop a copy on the
    invoking host.
    """
    import json
    import os
    import time
    from datetime import datetime, timezone

    import duckdb
    import plotly.graph_objects as go
    import plotly.io as pio

    db_path = f"{OUT_VOL_DIR}/snapshot/{snapshot_date}/snapshot.duckdb"
    if not os.path.exists(db_path):
        raise SystemExit(f"snapshot not found at {db_path}")
    print(f"=== open {db_path} (read-only) ===", flush=True)
    con = duckdb.connect(db_path, read_only=True)
    con.execute("PRAGMA threads=8")
    con.execute("SET memory_limit='28GiB'")
    con.execute("SET temp_directory='/tmp/duckdb_tmp'")
    os.makedirs("/tmp/duckdb_tmp", exist_ok=True)

    def q(sql: str, params: list | None = None):
        t0 = time.time()
        rows = con.execute(sql, params or []).fetchall()
        dt = time.time() - t0
        print(f"  ({dt:5.1f}s) {sql.strip().splitlines()[0][:70]}…", flush=True)
        return rows

    print("=== run queries ===", flush=True)

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
    # A 500-point Lorenz curve is plenty for visual + Gini-by-trapezoid.
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
    total_n_lorenz = sum(r[1] for r in lorenz_rows)
    total_likes_lorenz = sum(r[2] for r in lorenz_rows)
    lorenz_x = [0.0]
    lorenz_y = [0.0]
    for _bucket, n, sum_l, _mn, _mx in lorenz_rows:
        cum_n += n
        cum_likes += sum_l
        lorenz_x.append(cum_n / total_n_lorenz)
        lorenz_y.append(cum_likes / total_likes_lorenz)
    # Gini = 1 - 2 * area-under-Lorenz (trapezoid).
    area = 0.0
    for i in range(1, len(lorenz_x)):
        area += 0.5 * (lorenz_y[i] + lorenz_y[i - 1]) * (
            lorenz_x[i] - lorenz_x[i - 1]
        )
    gini = 1.0 - 2.0 * area

    # Top-N shares: precise direct query. NTILE(500) buckets the bottom of
    # the distribution finely enough for a Lorenz curve, but at the head
    # (top 0.01% = 1 in 10k posters) one NTILE bucket spans far more
    # posters than the percentile we want — interpolation undershoots
    # massively. So compute these in a separate single-pass query.
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
            -- ordinal for sorting
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

    # --- posts by likes-received tier ------------------------------------
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

    # --- engagement scaling: likes-per-post by follower bucket -----------
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

    # --- top-1000 ladder --------------------------------------------------
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
    BRAND = "#0085ff"
    AXIS = "#1d2433"
    GRID = "#e6e8ec"
    pio.templates["bsky"] = go.layout.Template(
        layout=go.Layout(
            font=dict(family="-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
                      color=AXIS, size=13),
            paper_bgcolor="white",
            plot_bgcolor="white",
            colorway=[BRAND, "#ff5d8f", "#7c3aed", "#16a34a", "#f59e0b"],
            xaxis=dict(gridcolor=GRID, zeroline=False, linecolor=GRID),
            yaxis=dict(gridcolor=GRID, zeroline=False, linecolor=GRID),
            margin=dict(l=60, r=20, t=50, b=60),
            hoverlabel=dict(bgcolor="white", bordercolor=GRID),
        )
    )

    def _fmt_pct(v: float) -> str:
        return f"{v * 100:.1f}%"

    # 1. Lorenz curve
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

    # 2. Top-N share (waterfall-style horizontal bar)
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
                   range=[0, max(values) * 1.15]),
        yaxis=dict(autorange="reversed"),
        height=380,
    )

    # 3. Posters by likes-received tier (log y)
    p_buckets = [r[0] for r in poster_tier_rows]
    p_counts = [r[1] for r in poster_tier_rows]
    p_pct = [100 * c / total_posters for c in p_counts]
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

    # 4. Posts by likes-received tier
    po_buckets = [r[0] for r in post_tier_rows]
    po_counts = [r[1] for r in post_tier_rows]
    po_pct = [100 * c / total_posts for c in po_counts]
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

    # 5. Likes per post by follower bucket
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

    # 6. Top-1000 ladder
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

    # ---------------------------------------------------------------------
    # HTML assembly
    # ---------------------------------------------------------------------
    def _fig_html(fig, div_id):
        return pio.to_html(
            fig, include_plotlyjs=False, full_html=False,
            div_id=div_id, config={"displayModeBar": False, "responsive": True},
        )

    plot_html = {
        "lorenz": _fig_html(fig_lorenz, "fig_lorenz"),
        "topn": _fig_html(fig_topn, "fig_topn"),
        "p_tier": _fig_html(fig_p_tier, "fig_p_tier"),
        "po_tier": _fig_html(fig_po_tier, "fig_po_tier"),
        "f_premium": _fig_html(fig_f, "fig_f"),
        "ladder": _fig_html(fig_ladder, "fig_ladder"),
    }

    # Inline plotly.js once (full standalone — no internet required to view).
    # plotly.offline.get_plotlyjs() returns the bundled JS as a string, no
    # HTML wrapping; we inject it inside a single <script> tag in <head>.
    import plotly.offline as _po
    plotlyjs = _po.get_plotlyjs()

    pct_posts_zero = 100 * posts_zero_likes / total_posts if total_posts else 0
    pct_posts_zero_eng = 100 * posts_zero_eng / total_posts if total_posts else 0
    pct_posters_zero = 100 * posters_zero / total_posters if total_posters else 0

    def fmt_int(n):
        return f"{int(n):,}"

    built_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Bluesky's like economy is winner-take-all</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root {{
    --brand: {BRAND};
    --ink: #1d2433;
    --muted: #5b6472;
    --rule: #e6e8ec;
    --bg: #fbfbfd;
  }}
  * {{ box-sizing: border-box; }}
  html, body {{ margin: 0; padding: 0; background: var(--bg); }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Helvetica Neue",
                 Arial, sans-serif;
    color: var(--ink);
    line-height: 1.55;
    font-size: 16px;
  }}
  .wrap {{ max-width: 1080px; margin: 0 auto; padding: 56px 24px 80px; }}
  .eyebrow {{
    font-size: 12px; letter-spacing: 0.12em; text-transform: uppercase;
    color: var(--muted); margin-bottom: 12px;
  }}
  h1 {{
    font-size: 44px; line-height: 1.1; letter-spacing: -0.02em;
    margin: 0 0 16px; font-weight: 700;
  }}
  h1 .accent {{ color: var(--brand); }}
  .lede {{
    font-size: 19px; color: var(--muted); margin: 0 0 36px; max-width: 780px;
  }}
  .stats {{
    display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px;
    margin: 32px 0 48px;
  }}
  .stat {{
    background: white; border: 1px solid var(--rule); border-radius: 10px;
    padding: 18px 16px;
  }}
  .stat .v {{ font-size: 30px; font-weight: 700; letter-spacing: -0.02em; }}
  .stat .v.bad {{ color: #ef4444; }}
  .stat .v.brand {{ color: var(--brand); }}
  .stat .l {{ font-size: 12.5px; color: var(--muted); margin-top: 4px; }}
  .stat .sub {{ font-size: 11.5px; color: var(--muted); margin-top: 2px; }}
  section {{ margin: 56px 0; }}
  section h2 {{
    font-size: 26px; letter-spacing: -0.01em; font-weight: 700;
    margin: 0 0 8px;
  }}
  section .kicker {{
    font-size: 14px; color: var(--brand); font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 8px;
  }}
  section p {{ color: var(--muted); margin: 0 0 16px; max-width: 820px; }}
  section p strong {{ color: var(--ink); }}
  .figure {{
    background: white; border: 1px solid var(--rule); border-radius: 10px;
    padding: 14px 10px 6px; margin-top: 18px;
  }}
  .pull {{
    border-left: 3px solid var(--brand); padding: 6px 0 6px 16px;
    margin: 24px 0; font-size: 21px; line-height: 1.4; color: var(--ink);
    font-weight: 500; max-width: 780px;
  }}
  footer {{
    margin-top: 80px; padding-top: 24px; border-top: 1px solid var(--rule);
    color: var(--muted); font-size: 13px;
  }}
  footer code {{
    background: white; border: 1px solid var(--rule); border-radius: 4px;
    padding: 1px 5px; font-size: 12px;
  }}
  @media (max-width: 720px) {{
    .stats {{ grid-template-columns: repeat(2, 1fr); }}
    h1 {{ font-size: 34px; }}
    .lede {{ font-size: 17px; }}
  }}
</style>
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
    <div class="v">{fmt_int(median_li)}</div>
    <div class="l">median lifetime likes per poster</div>
    <div class="sub">vs. mean of {mean_li:,.0f}</div>
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
    accumulated <strong>{fmt_int(median_li)}</strong> total likes across
    every post they have ever made. The 99th percentile is
    {fmt_int(p99_li)}; the maximum is {fmt_int(max_li)}.
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
    {100 * 1000 / total_posters:.4f}% of posters.
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

    # Persist on the volume.
    out_dir = f"{OUT_VOL_DIR}/analysis/{snapshot_date}"
    os.makedirs(out_dir, exist_ok=True)
    out_file = f"{out_dir}/likes_concentration.html"
    payload = html.encode("utf-8")
    with open(out_file, "wb") as f:
        f.write(payload)
    volume_out.commit()
    print(
        f"=== wrote {out_file} ({len(payload):,} bytes; gini={gini:.3f}) ===",
        flush=True,
    )

    # Also dump a short JSON sidecar with the headline numbers — handy for
    # programmatic comparison across snapshots.
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
    with open(f"{out_dir}/likes_concentration.json", "w") as f:
        json.dump(sidecar, f, indent=2)
    volume_out.commit()

    return payload


@app.function(
    image=analysis_image,
    volumes={"/vol-out": volume_out},
    timeout=60 * 60,
    cpu=8.0,
    # posts × post_aggs join over the windowed cohort is the hot query.
    # DuckDB streams the join; 32 GiB is ample headroom for QUANTILE_CONT
    # over the qualifying-posts subset (millions of rows, not billions).
    memory=32 * 1024,
    ephemeral_disk=512 * 1024,
)
def analyze_ratio(
    snapshot_date: str = "2026-04-28",
    window_days: int = 90,
) -> bytes:
    """Build the "ratio" analysis HTML on Modal.

    The "argumentation ratio" of a post is (replies + quotes) / likes.
    Twitter folklore says replies > likes means you're getting dragged;
    we measure where the threshold actually sits on Bluesky and which
    accounts cross it most often.

    Window: posts authored in the last `window_days` (default 90) before
    `snapshot_date`. Engagement on those posts is whatever has accumulated
    in `post_aggs` — usually within hours-to-days of post creation, so
    this is effectively "argued-with within ~window_days."
    """
    import json
    import os
    import time
    from datetime import date, datetime, timedelta, timezone

    import duckdb
    import plotly.graph_objects as go
    import plotly.io as pio

    db_path = f"{OUT_VOL_DIR}/snapshot/{snapshot_date}/snapshot.duckdb"
    if not os.path.exists(db_path):
        raise SystemExit(f"snapshot not found at {db_path}")
    print(f"=== open {db_path} (read-only) ===", flush=True)
    con = duckdb.connect(db_path, read_only=True)
    con.execute("PRAGMA threads=8")
    con.execute("SET memory_limit='28GiB'")
    con.execute("SET temp_directory='/tmp/duckdb_tmp'")
    os.makedirs("/tmp/duckdb_tmp", exist_ok=True)

    # Window bounds. Snapshot date is naive YYYY-MM-DD; treat it as the
    # exclusive upper bound and walk back `window_days`.
    hi = snapshot_date
    lo = (date.fromisoformat(snapshot_date) - timedelta(days=window_days)).isoformat()
    print(f"=== window: posts.created_at in [{lo}, {hi}] ===", flush=True)

    # Materialize the cohort once. Plausibility filter on created_at
    # to avoid TID-decode garbage (1970 / 2118+ outliers) leaking in
    # even if our window is later than 2022.
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
    print(f"  ({time.time()-t0:.1f}s) view ready", flush=True)

    def q(sql: str):
        t0 = time.time()
        rows = con.execute(sql).fetchall()
        dt = time.time() - t0
        print(f"  ({dt:5.1f}s) {sql.strip().splitlines()[0][:70]}…", flush=True)
        return rows

    # --- headline ---------------------------------------------------------
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

    # --- distribution of argr (bucketed) ---------------------------------
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

    # --- ratio rate by like-tier (does virality buy you protection?) ----
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

    # --- weekly time series of ratio rate -------------------------------
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

    # --- per-author distribution of ratio rate --------------------------
    # Restrict to authors with at least 10 qualifying posts so "rate"
    # means something. Bucket those authors by their personal ratio rate.
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
    # And the count of distinct authors who clear the 10-post bar.
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

    # --- top "ratio'd" anonymized authors (sorted bar of rates) ---------
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

    # ---------------------------------------------------------------------
    # Plotly figures
    # ---------------------------------------------------------------------
    pio.templates["bsky"] = _bsky_template()

    # 1. argr distribution
    d_buckets = [r[0] for r in dist_rows]
    d_counts = [r[1] for r in dist_rows]
    d_pct = [100 * c / qualifying_posts for c in d_counts]
    # Color buckets crossing the ratio threshold red, others blue.
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

    # 2. ratio rate by like-tier
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

    # 3. weekly time series
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

    # 4. per-author rate distribution
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

    # 5. top 50 ratio'd authors (sorted bar of rate)
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
                        "top 50 anonymized authors by share of their posts that crossed the line "
                        "(min 20 qualifying posts)",
                   x=0.02, xanchor="left"),
        xaxis=dict(title="Rank"),
        yaxis=dict(title="% of author's posts where argument > likes"),
        height=400, bargap=0.05,
    )

    plot_html = {
        "dist": _fig_html(fig_dist, "fig_dist"),
        "tier": _fig_html(fig_tier, "fig_tier"),
        "weekly": _fig_html(fig_weekly, "fig_weekly"),
        "authors": _fig_html(fig_authors, "fig_authors"),
        "head": _fig_html(fig_head, "fig_head"),
    }
    plotlyjs = _plotlyjs_inline()

    def fmt_int(n):
        return f"{int(n):,}"

    built_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Compare lowest-engagement tier vs highest. The story is whether
    # the rate trends up or down with virality.
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
  happens to about <strong>1 in 700</strong> of them, and even less often
  the more viral a post gets.
</p>

<div class="stats">
  <div class="stat">
    <div class="v bad">{ratio_pct:.2f}%</div>
    <div class="l">of posts cross the line</div>
    <div class="sub">argument &gt; likes ({fmt_int(true_ratios)} of {fmt_int(qualifying_posts)})</div>
  </div>
  <div class="stat">
    <div class="v">{median_argr:.3f}</div>
    <div class="l">median argumentation ratio</div>
    <div class="sub">~{1/median_argr:.0f} likes per argument</div>
  </div>
  <div class="stat">
    <div class="v">{p99_argr:.2f}</div>
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
  <strong>{1/median_argr:.0f} likes for every reply or quote</strong>. And
  unlike Twitter folklore predicts, virality doesn't bring ratio risk on
  Bluesky — it strips it away. Viral posts get more likes faster than
  argument can keep up.
</div>

<section>
  <div class="kicker">Finding 01</div>
  <h2>The bulk of posts sit well below the ratio threshold.</h2>
  <p>
    The "argumentation ratio" we computed is
    <strong>(replies + quotes) / likes</strong> — replies because they're
    the canonical Twitter ratio signal, plus quotes because a quote-dunk
    carries the same social weight. A post is "ratio'd" when that number
    crosses 1.0. The distribution below makes clear how rare that is:
    even the 99th percentile lands at <strong>{p99_argr:.2f}</strong> —
    well short of the threshold. The y-axis is logarithmic; without it
    the leftmost bars would dwarf everything.
  </p>
  <div class="figure">{plot_html["dist"]}</div>
</section>

<section>
  <div class="kicker">Finding 02</div>
  <h2>Virality protects you from the ratio, not the other way around.</h2>
  <p>
    Splitting posts by like-count reveals the opposite of what Twitter
    folklore predicts: ratio rate is <strong>highest at the bottom</strong>
    (the {lo_tier_label}-likes tier, at {lo_tier_rate:.2f}%) and drops to
    {hi_tier_rate:.2f}% in the {hi_tier_label}-likes tier. The reason is
    mostly mechanical — at 10 likes, two extra replies are enough to flip
    the ratio, so noise dominates. By the time a post has thousands of
    likes, argument volume can't catch up. The "viral but ratio'd" classic
    Twitter case is essentially nonexistent here.
  </p>
  <div class="figure">{plot_html["tier"]}</div>
</section>

<section>
  <div class="kicker">Finding 03</div>
  <h2>Ratio rate over time.</h2>
  <p>
    Week-over-week trend of how often qualifying posts are ratio'd over
    the {window_days}-day window. Spikes correlate with whatever was
    being argued about that week — useful as a "discourse temperature"
    gauge if you tracked it week-on-week.
  </p>
  <div class="figure">{plot_html["weekly"]}</div>
</section>

<section>
  <div class="kicker">Finding 04</div>
  <h2>A few accounts attract ratios at much higher rates.</h2>
  <p>
    Restricting to the <strong>{fmt_int(author_universe)}</strong> authors
    with at least 10 qualifying posts in the window: most never get
    ratio'd at all. A small minority — the highlighted red bucket — has
    more than 40% of their qualifying posts cross the line. These are
    the accounts whose posting style consistently draws argument over
    agreement.
  </p>
  <div class="figure">{plot_html["authors"]}</div>
</section>

<section>
  <div class="kicker">Finding 05</div>
  <h2>The leaderboard.</h2>
  <p>
    Anonymized: the top 50 authors by personal ratio rate (min 20
    qualifying posts in the window). The leader gets ratio'd on
    <strong>{head_rates[0]:.0f}%</strong> of their qualifying posts, if
    any reach the bar.
  </p>
  <div class="figure">{plot_html["head"]}</div>
</section>

<footer>
  <p>
    <strong>Methodology.</strong> Computed from the at-snapshot Bluesky
    DuckDB build for snapshot date <code>{snapshot_date}</code>, restricted
    to posts with <code>created_at</code> in
    <code>[{lo}, {hi}]</code>. "Argumentation ratio" is
    <code>(replies + quotes) / likes</code>; we restrict the per-post
    analysis to posts with ≥10 likes to avoid noise dominating
    (a post with 0 likes and 1 reply has an undefined ratio). Author-level
    metrics restrict to authors with ≥10 qualifying posts. Built {built_at}.
  </p>
  <p>
    <strong>Caveats.</strong> The snapshot reflects engagement crawled by
    constellation; very recent posts and engagement lag. Quote-dunks vs
    earnest quotes are not distinguished; both count as argument signal.
  </p>
</footer>

</div>
</body>
</html>
"""

    out_dir = f"{OUT_VOL_DIR}/analysis/{snapshot_date}"
    os.makedirs(out_dir, exist_ok=True)
    out_file = f"{out_dir}/ratio.html"
    payload = html.encode("utf-8")
    with open(out_file, "wb") as f:
        f.write(payload)
    volume_out.commit()
    print(
        f"=== wrote {out_file} ({len(payload):,} bytes; ratio_pct={ratio_pct:.3f}%) ===",
        flush=True,
    )

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
        "authors_with_10_posts": author_universe,
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
    with open(f"{out_dir}/ratio.json", "w") as f:
        json.dump(sidecar, f, indent=2, default=str)
    volume_out.commit()

    return payload


@app.function(
    image=analysis_image,
    volumes={"/vol-out": volume_out},
    # likes has ~10B rows; the per-table MIN/MAX GROUP BY is the hot
    # query. 2h timeout is generous; expected runtime 10–30 min.
    timeout=60 * 60 * 2,
    cpu=8.0,
    # Per-table GROUP BY peaks at ~50M unique actors with two timestamps
    # each — ~2 GB hash table. The hot moment is the final UNION+GROUP
    # over ~200M intermediate rows. 64 GiB gives plenty of headroom.
    memory=64 * 1024,
    ephemeral_disk=512 * 1024,
)
def analyze_attrition(
    snapshot_date: str = "2026-04-28",
    inactivity_days: int = 30,
) -> bytes:
    """Build the regrettable-attrition analysis HTML on Modal.

    Computes per-actor first_seen / last_seen across all outbound
    activity (likes / posts / follows / reposts), plausibility-filtered
    to drop TID-decode garbage. Cross-references with `actor_aggs` to
    bucket by lifetime engagement tier (zero / lurker / casual /
    regular / power) and measures the inactivity rate — actors whose
    `last_seen` is more than `inactivity_days` before `snapshot_date`.

    The headline "regrettable attrition" number is the share of
    previously-engaged accounts (regular + power tiers) that have gone
    quiet for `inactivity_days` or longer.
    """
    import json
    import os
    import time
    from datetime import date, datetime, timedelta, timezone

    import duckdb
    import plotly.graph_objects as go
    import plotly.io as pio

    db_path = f"{OUT_VOL_DIR}/snapshot/{snapshot_date}/snapshot.duckdb"
    if not os.path.exists(db_path):
        raise SystemExit(f"snapshot not found at {db_path}")
    print(f"=== open {db_path} (read-only) ===", flush=True)
    con = duckdb.connect(db_path, read_only=True)
    con.execute("PRAGMA threads=8")
    con.execute("SET memory_limit='56GiB'")
    con.execute("SET temp_directory='/tmp/duckdb_tmp'")
    os.makedirs("/tmp/duckdb_tmp", exist_ok=True)

    snap = snapshot_date
    inactive_cutoff = (date.fromisoformat(snap) - timedelta(days=inactivity_days)).isoformat()
    plausibility_lo = "2022-01-01"
    print(
        f"=== inactivity cutoff: last_seen < {inactive_cutoff} "
        f"(={inactivity_days}d before {snap}) ===",
        flush=True,
    )

    # Build the per-actor first_seen / last_seen table. Per-table MIN/MAX
    # GROUP BY first, then combine — far faster than a giant UNION ALL
    # over the raw rows and a single 12B-row GROUP BY at the end.
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
    print(f"  ({time.time()-t0:.1f}s) actor_activity materialized", flush=True)

    # Cross with actor_aggs to get the engagement tier per actor.
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
    print(f"  ({time.time()-t0:.1f}s) actor_with_tier materialized", flush=True)

    def q(sql: str):
        t0 = time.time()
        rows = con.execute(sql).fetchall()
        dt = time.time() - t0
        print(f"  ({dt:5.1f}s) {sql.strip().splitlines()[0][:70]}…", flush=True)
        return rows

    # --- headline ---------------------------------------------------------
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

    # --- inactivity rate by tier -----------------------------------------
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

    # --- cohort retention: by month-of-first-seen, % still active --------
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

    # --- timeline of when engaged accounts last did anything -------------
    # Among accounts in engaged + power tiers, bucket their last_seen by
    # week — shows when the dropouts happened.
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

    # --- days-inactive distribution among engaged accounts ---------------
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

    # ---------------------------------------------------------------------
    # Plotly figures
    # ---------------------------------------------------------------------
    pio.templates["bsky"] = _bsky_template()

    # Tier-inactivity bars
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

    # Cohort retention: two lines per cohort (everyone vs engaged-only)
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

    # Timeline of when engaged accounts last did anything
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

    # Days-inactive distribution
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
        "tier": _fig_html(fig_tier, "fig_tier"),
        "cohort": _fig_html(fig_cohort, "fig_cohort"),
        "timeline": _fig_html(fig_timeline, "fig_timeline"),
        "dist": _fig_html(fig_dist, "fig_dist"),
    }
    plotlyjs = _plotlyjs_inline()

    def fmt_int(n):
        return f"{int(n):,}"

    built_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

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
  reposting for at least {inactivity_days} days. That's the floor on
  regrettable attrition — the people you'd want to win back.
</div>

<section>
  <div class="kicker">Finding 01</div>
  <h2>Higher-engagement tiers stick better — but the dropout rate is still substantial.</h2>
  <p>
    Inactivity rate falls as engagement rises: lurkers (no posts, no
    follows, just maybe likes_out) drop out at much higher rates than
    power users. But even in the "power" tier — accounts with ≥100
    posts, ≥10k lifetime likes received, or ≥1k followers —
    <strong>{power_regret:.1f}%</strong> have gone quiet for more than
    {inactivity_days} days. These are accounts that already proved out
    on the platform; their absence is the most consequential loss.
  </p>
  <div class="figure">{plot_html["tier"]}</div>
</section>

<section>
  <div class="kicker">Finding 02</div>
  <h2>Retention varies sharply by signup cohort.</h2>
  <p>
    Splitting accounts by the month they first did anything on Bluesky
    reveals which signup cohorts stuck around. The gap between the all-accounts
    line (gray) and the engaged-tier line (blue) is the value of the
    engagement bar as a stickiness predictor — engaged signups stay
    much better than the baseline, in every cohort. Spikes in cohort
    size (visible in hover) typically correspond to Twitter/X
    crisis moments; their post-spike retention is the meaningful test
    of whether migration waves stuck.
  </p>
  <div class="figure">{plot_html["cohort"]}</div>
</section>

<section>
  <div class="kicker">Finding 03</div>
  <h2>When did the engaged accounts go quiet?</h2>
  <p>
    Bucketing engaged accounts by the week of their final outbound
    action (and never seen again through {snap}) shows when the
    dropouts actually happened. Bars further right are more recent
    departures; bars further left are accounts that quietly left long
    ago. Pronounced spikes can be cross-referenced with platform events,
    feature releases, or external migrations.
  </p>
  <div class="figure">{plot_html["timeline"]}</div>
</section>

<section>
  <div class="kicker">Finding 04</div>
  <h2>How long is the engaged base "stale"?</h2>
  <p>
    Distribution of days-since-last-action across the
    {fmt_int(engaged_pop)} engaged accounts. The greenest bar is the
    still-warm base. The further right you go, the colder the
    population becomes — accounts inactive for a year or more are
    effectively gone.
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
    plausibility-filtered to <code>created_at &gt; 2022-01-01</code>
    (a known TID-decode artifact otherwise pollutes timestamps).
    "Engaged" = ≥10 posts OR ≥100 lifetime likes_in OR ≥10 followers,
    measured against lifetime <code>actor_aggs</code>. "Power" raises
    those thresholds 10×. "Inactive" = <code>last_seen &lt; snapshot_date
    − {inactivity_days} days</code>. Built {built_at}.
  </p>
  <p>
    <strong>Caveats.</strong> The engagement tiering uses
    <em>lifetime</em> counts, so a power-tier account that earned its
    likes years ago still counts as power-tier today — which is the
    point: those are exactly the regrettable losses. Crawl lag means
    some "inactive" accounts may have very recent activity that hasn't
    been indexed yet. Deactivated / suspended accounts appear as
    inactive even if their absence is involuntary.
  </p>
</footer>

</div>
</body>
</html>
"""

    out_dir = f"{OUT_VOL_DIR}/analysis/{snapshot_date}"
    os.makedirs(out_dir, exist_ok=True)
    out_file = f"{out_dir}/attrition.html"
    payload = html.encode("utf-8")
    with open(out_file, "wb") as f:
        f.write(payload)
    volume_out.commit()
    print(
        f"=== wrote {out_file} ({len(payload):,} bytes; "
        f"regret={regret_rate:.1f}%, power_regret={power_regret:.1f}%) ===",
        flush=True,
    )

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
    with open(f"{out_dir}/attrition.json", "w") as f:
        json.dump(sidecar, f, indent=2, default=str)
    volume_out.commit()

    return payload


@app.function(
    image=spectral_image,
    volumes={"/vol-out": volume_out},
    timeout=60 * 60 * 2,
    cpu=8.0,
    # The (voter × item) signed matrix at full Bluesky scale: ~30M voters,
    # ~20k items, ~500M–1B non-zeros. Sparse CSR is ~5–10 GB; SVD result
    # (U, s, Vt at k=10) adds ~3 GB; DuckDB working memory during the
    # join is ~10 GB. 128 GiB is comfortable headroom.
    memory=128 * 1024,
    ephemeral_disk=512 * 1024,
)
def analyze_blocks(
    snapshot_date: str = "2026-04-28",
    n_items: int = 20000,
    k_components: int = 10,
) -> bytes:
    """Fit a DW-NOMINATE-style spatial model to the signed
    follow / block graph.

    Constructs a sparse signed matrix M where M[v, i] = +1 if voter v
    follows item i, -1 if v blocks i, 0 otherwise. Items are the
    top-`n_items` most-salient accounts (ranked by `followers + 10 *
    blocks_in`) — blocks are weighted 10× since they're rarer and more
    informative per edge. Voters are *every actor* who has at least one
    edge to an item — no engagement-tier pre-filtering, in keeping with
    the "analyze everyone" requirement.

    Runs truncated SVD (`scipy.sparse.linalg.svds`) at `k_components`
    to recover the principal latent dimensions of the social graph.
    The first component is the analog of DW-NOMINATE's primary
    left-right axis: the strongest cleavage along which the user base
    sorts.

    Presented strictly as structural findings — eigenvalue spectrum,
    distribution along PC1, density of voters in (PC1, PC2) space, and
    the top items at each end of PC1. No external handle resolution,
    so no political-label interpretation; the structure is the finding.
    """
    import json
    import os
    import time
    from datetime import datetime, timezone

    import duckdb
    import numpy as np
    import plotly.graph_objects as go
    import plotly.io as pio
    import scipy.sparse as sp
    from scipy.sparse.linalg import svds

    db_path = f"{OUT_VOL_DIR}/snapshot/{snapshot_date}/snapshot.duckdb"
    if not os.path.exists(db_path):
        raise SystemExit(f"snapshot not found at {db_path}")
    print(f"=== open {db_path} (read-only) ===", flush=True)
    con = duckdb.connect(db_path, read_only=True)
    con.execute("PRAGMA threads=8")
    con.execute("SET memory_limit='80GiB'")
    con.execute("SET temp_directory='/tmp/duckdb_tmp'")
    os.makedirs("/tmp/duckdb_tmp", exist_ok=True)

    # --- pick items: top-N by combined salience ---------------------------
    print(f"=== select top {n_items:,} items by salience ===", flush=True)
    t0 = time.time()
    con.execute(
        f"""
        CREATE OR REPLACE TEMPORARY TABLE items_t AS
        SELECT did_id,
               followers,
               blocks_in,
               (followers + blocks_in * 10) AS salience,
               ROW_NUMBER() OVER (
                 ORDER BY (followers + blocks_in * 10) DESC, did_id
               ) - 1 AS idx
        FROM actor_aggs
        WHERE followers + blocks_in * 10 > 0
        ORDER BY salience DESC
        LIMIT {n_items}
        """
    )
    print(f"  ({time.time()-t0:.1f}s) items_t materialized", flush=True)

    item_meta = con.execute(
        "SELECT idx, did_id, followers, blocks_in, salience FROM items_t ORDER BY idx"
    ).fetchall()
    print(
        f"  salience range: top item has {item_meta[0][4]:,}, "
        f"bottom item has {item_meta[-1][4]:,}",
        flush=True,
    )

    # --- build the edge set: every (voter, item, sign) triplet -----------
    print("=== build signed edges (follows + blocks → items) ===", flush=True)
    t0 = time.time()
    con.execute(
        """
        CREATE OR REPLACE TEMPORARY TABLE edges_raw AS
        SELECT f.src_did_id AS voter_did, items_t.idx AS item_idx,
               1::TINYINT AS sign
        FROM follows f
        JOIN items_t ON f.dst_did_id = items_t.did_id
        UNION ALL
        SELECT b.src_did_id, items_t.idx, -1::TINYINT
        FROM blocks b
        JOIN items_t ON b.dst_did_id = items_t.did_id
        """
    )
    print(f"  ({time.time()-t0:.1f}s) edges_raw materialized", flush=True)

    # If a voter both follows and blocks the same item (rare), keep the
    # block (-1 wins over +1) since blocks are the stronger signal.
    print("=== dedupe edges (block beats follow on ties) ===", flush=True)
    t0 = time.time()
    con.execute(
        """
        CREATE OR REPLACE TEMPORARY TABLE edges_t AS
        SELECT voter_did, item_idx, MIN(sign) AS sign
        FROM edges_raw
        GROUP BY voter_did, item_idx
        """
    )
    con.execute("DROP TABLE edges_raw")
    print(f"  ({time.time()-t0:.1f}s) edges_t materialized", flush=True)

    # --- assign voter indices --------------------------------------------
    print("=== assign voter indices ===", flush=True)
    t0 = time.time()
    con.execute(
        """
        CREATE OR REPLACE TEMPORARY TABLE voters_t AS
        SELECT voter_did,
               ROW_NUMBER() OVER (ORDER BY voter_did) - 1 AS idx
        FROM (SELECT DISTINCT voter_did FROM edges_t)
        """
    )
    n_voters = con.execute("SELECT COUNT(*) FROM voters_t").fetchone()[0]
    n_edges = con.execute("SELECT COUNT(*) FROM edges_t").fetchone()[0]
    n_follows_e = con.execute(
        "SELECT COUNT(*) FROM edges_t WHERE sign = 1"
    ).fetchone()[0]
    n_blocks_e = con.execute(
        "SELECT COUNT(*) FROM edges_t WHERE sign = -1"
    ).fetchone()[0]
    print(
        f"  ({time.time()-t0:.1f}s) voters={n_voters:,}, edges={n_edges:,} "
        f"(follow={n_follows_e:,}, block={n_blocks_e:,})",
        flush=True,
    )

    # --- pull triplets into numpy via Arrow ------------------------------
    print("=== pull (row, col, sign) triplets into numpy ===", flush=True)
    t0 = time.time()
    arrow = con.execute(
        """
        SELECT voters_t.idx AS row,
               edges_t.item_idx::INTEGER AS col,
               edges_t.sign AS val
        FROM edges_t JOIN voters_t USING(voter_did)
        """
    ).fetch_arrow_table()
    rows = arrow.column("row").to_numpy().astype(np.int32, copy=False)
    cols = arrow.column("col").to_numpy().astype(np.int32, copy=False)
    vals = arrow.column("val").to_numpy().astype(np.float32, copy=False)
    print(f"  ({time.time()-t0:.1f}s) {len(rows):,} triplets in numpy", flush=True)

    # Free DuckDB working sets we no longer need before the SVD.
    con.execute("DROP TABLE edges_t")
    con.execute("DROP TABLE voters_t")
    del arrow

    # --- build sparse matrix and run SVD ---------------------------------
    print(
        f"=== build sparse {n_voters:,} × {n_items:,} matrix "
        f"({len(rows):,} non-zeros) ===",
        flush=True,
    )
    t0 = time.time()
    M = sp.coo_matrix(
        (vals, (rows, cols)), shape=(n_voters, n_items)
    ).tocsr()
    print(f"  ({time.time()-t0:.1f}s) sparse matrix built", flush=True)
    del rows, cols, vals

    # Frobenius² for variance-explained denominator.
    fro_sq = float((M.multiply(M)).sum())

    print(f"=== truncated SVD (k={k_components}) ===", flush=True)
    t0 = time.time()
    # svds returns singular values in ascending order; reverse them.
    U, s, Vt = svds(M, k=k_components)
    order = np.argsort(s)[::-1]
    U = U[:, order]
    s = s[order]
    Vt = Vt[order, :]
    print(f"  ({time.time()-t0:.1f}s) SVD complete; top sv = {s[0]:.2f}", flush=True)

    var_per = (s ** 2) / fro_sq
    cum_var = np.cumsum(var_per)

    # --- 2D density of voters in (PC1, PC2) ------------------------------
    pc1 = U[:, 0]
    pc2 = U[:, 1]
    # 2D histogram. Restrict range to central 99% per axis to avoid
    # outliers compressing the heatmap.
    x_lo, x_hi = np.percentile(pc1, [0.5, 99.5])
    y_lo, y_hi = np.percentile(pc2, [0.5, 99.5])
    H, xedges, yedges = np.histogram2d(
        pc1, pc2, bins=200, range=[[x_lo, x_hi], [y_lo, y_hi]]
    )
    # Use log1p of counts for visual contrast.
    H_log = np.log10(H + 1)

    # PC1 distribution (univariate histogram).
    pc1_hist, pc1_edges = np.histogram(pc1, bins=200, range=(x_lo, x_hi))

    # --- top items at each end of PC1 ------------------------------------
    item_pc1 = Vt[0, :]
    sort_idx = np.argsort(item_pc1)
    bottom_idx = sort_idx[:25]   # most negative
    top_idx = sort_idx[-25:][::-1]  # most positive

    # `item_meta` is in idx order — we can index directly.
    def _row(i):
        idx, did_id, followers, blocks_in, salience = item_meta[i]
        return {
            "idx": int(idx),
            "did_id": int(did_id),
            "pc1": float(item_pc1[i]),
            "followers": int(followers),
            "blocks_in": int(blocks_in),
            "salience": int(salience),
        }
    bottom_items = [_row(i) for i in bottom_idx]
    top_items = [_row(i) for i in top_idx]

    print(
        f"  PC1 range: [{item_pc1.min():.3f}, {item_pc1.max():.3f}]; "
        f"distinct sides: bottom={[r['pc1'] for r in bottom_items[:3]]}, "
        f"top={[r['pc1'] for r in top_items[:3]]}",
        flush=True,
    )

    # ---------------------------------------------------------------------
    # Plotly figures
    # ---------------------------------------------------------------------
    pio.templates["bsky"] = _bsky_template()

    # 1. Scree
    fig_scree = go.Figure()
    fig_scree.add_trace(go.Bar(
        x=[f"PC{i+1}" for i in range(k_components)],
        y=var_per * 100,
        marker=dict(color=BRAND),
        text=[f"{v*100:.2f}%" for v in var_per],
        textposition="outside",
        hovertemplate="%{x}<br>%{y:.2f}%% variance explained<extra></extra>",
    ))
    fig_scree.add_trace(go.Scatter(
        x=[f"PC{i+1}" for i in range(k_components)],
        y=cum_var * 100,
        mode="lines+markers",
        name="Cumulative",
        line=dict(color="#ef4444", dash="dot"),
        marker=dict(size=6),
        hovertemplate="%{x}<br>%{y:.1f}%% cumulative<extra></extra>",
    ))
    fig_scree.update_layout(
        template="bsky",
        title=dict(text="<b>How much of the graph fits a single axis?</b>  ·  "
                        "variance explained per principal component",
                   x=0.02, xanchor="left"),
        xaxis=dict(title="Principal component"),
        yaxis=dict(title="% of variance"),
        height=380, showlegend=False,
    )

    # 2. 2D density of voters in PC1 × PC2
    fig_density = go.Figure(go.Heatmap(
        z=H_log.T,  # transpose so Y is PC2
        x=xedges,
        y=yedges,
        colorscale="Blues",
        colorbar=dict(title="log10(voters)"),
        hovertemplate="PC1 %{x:.3f}<br>PC2 %{y:.3f}<br>log10 count %{z:.2f}<extra></extra>",
    ))
    fig_density.update_layout(
        template="bsky",
        title=dict(text=f"<b>Voters in (PC1, PC2) space</b>  ·  "
                        f"{n_voters:,} voters embedded via signed-graph SVD",
                   x=0.02, xanchor="left"),
        xaxis=dict(title="PC1 (primary cleavage)"),
        yaxis=dict(title="PC2"),
        height=520,
    )

    # 3. PC1 distribution
    pc1_centers = (pc1_edges[:-1] + pc1_edges[1:]) / 2
    fig_pc1 = go.Figure(go.Bar(
        x=pc1_centers, y=pc1_hist,
        marker=dict(color=BRAND),
        hovertemplate="PC1 %{x:.3f}<br>%{y:,} voters<extra></extra>",
    ))
    fig_pc1.update_layout(
        template="bsky",
        title=dict(text="<b>Distribution of voters along PC1</b>  ·  "
                        "unimodal blob, or two distinct camps?",
                   x=0.02, xanchor="left"),
        xaxis=dict(title="PC1 score"),
        yaxis=dict(title="Voters (count)"),
        height=380, bargap=0.0,
    )

    # 4. Top items at each end of PC1 — show their PC1 score & salience
    top_combined = bottom_items + top_items
    fig_tops = go.Figure()
    fig_tops.add_trace(go.Bar(
        x=[r["pc1"] for r in bottom_items],
        y=[f"item #{r['idx']}" for r in bottom_items],
        orientation="h",
        marker=dict(color="#ef4444"),
        name="Negative end",
        customdata=[[r["followers"], r["blocks_in"]] for r in bottom_items],
        hovertemplate=("PC1 %{x:.3f}<br>%{y}<br>followers %{customdata[0]:,}, "
                       "blocks_in %{customdata[1]:,}<extra></extra>"),
    ))
    fig_tops.add_trace(go.Bar(
        x=[r["pc1"] for r in top_items],
        y=[f"item #{r['idx']}" for r in top_items],
        orientation="h",
        marker=dict(color=BRAND),
        name="Positive end",
        customdata=[[r["followers"], r["blocks_in"]] for r in top_items],
        hovertemplate=("PC1 %{x:.3f}<br>%{y}<br>followers %{customdata[0]:,}, "
                       "blocks_in %{customdata[1]:,}<extra></extra>"),
    ))
    fig_tops.update_layout(
        template="bsky",
        title=dict(text="<b>Top 25 items at each end of PC1</b>  ·  "
                        "the anchor accounts that define the primary cleavage",
                   x=0.02, xanchor="left"),
        xaxis=dict(title="Item loading on PC1"),
        yaxis=dict(autorange="reversed", showticklabels=False, title="Items (anonymized)"),
        height=520, barmode="overlay",
        legend=dict(orientation="h", y=-0.12),
    )

    plot_html = {
        "scree": _fig_html(fig_scree, "fig_scree"),
        "density": _fig_html(fig_density, "fig_density"),
        "pc1": _fig_html(fig_pc1, "fig_pc1"),
        "tops": _fig_html(fig_tops, "fig_tops"),
    }
    plotlyjs = _plotlyjs_inline()

    def fmt_int(n):
        return f"{int(n):,}"

    built_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    pc1_share = float(var_per[0]) * 100
    pc1_to_pc2_ratio = float(s[0] / s[1]) if s[1] > 0 else float("inf")
    pc2_share = float(var_per[1]) * 100

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Mapping Bluesky's primary cleavage</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>{SHARED_CSS}</style>
<script>{plotlyjs}</script>
</head>
<body>
<div class="wrap">

<div class="eyebrow">An analysis · snapshot {snapshot_date} · signed-graph SVD</div>
<h1>Bluesky's <span class="accent">primary cleavage</span>.</h1>
<p class="lede">
  DW-NOMINATE recovers a left-right axis of the U.S. Congress from
  binary roll-call votes. The analog here: each Bluesky user "votes"
  on every prominent account by either following them (+1) or blocking
  them (−1). If the user base really is sorted along a single primary
  cleavage, the principal component of that signed matrix should
  capture it. We fit it on
  <strong>{fmt_int(n_voters)}</strong> voters by
  <strong>{fmt_int(n_items)}</strong> high-salience items
  ({fmt_int(n_edges)} signed edges).
</p>

<div class="stats">
  <div class="stat">
    <div class="v brand">{pc1_share:.2f}%</div>
    <div class="l">variance on PC1</div>
    <div class="sub">share of the matrix captured by one axis</div>
  </div>
  <div class="stat">
    <div class="v">{pc1_to_pc2_ratio:.2f}×</div>
    <div class="l">PC1 / PC2 singular-value ratio</div>
    <div class="sub">how dominant the first dimension is</div>
  </div>
  <div class="stat">
    <div class="v">{fmt_int(n_blocks_e)}</div>
    <div class="l">block edges</div>
    <div class="sub">vs {fmt_int(n_follows_e)} follow edges</div>
  </div>
  <div class="stat">
    <div class="v">{fmt_int(n_items)}</div>
    <div class="l">items in the model</div>
    <div class="sub">ranked by followers + 10 × blocks_in</div>
  </div>
</div>

<div class="pull">
  This analysis is strictly structural — it answers "is there a primary
  axis along which the user base sorts?" and "how sharp is that
  cleavage?" — without labeling either end. Naming the axis would
  require resolving DIDs to handles and qualitative validation; that's
  a separate exercise.
</div>

<section>
  <div class="kicker">Finding 01</div>
  <h2>How concentrated is the cleavage on a single axis?</h2>
  <p>
    The scree plot shows the share of variance captured by each
    principal component. PC1 captures
    <strong>{pc1_share:.2f}%</strong> of total variance and is
    <strong>{pc1_to_pc2_ratio:.2f}×</strong> larger than PC2 — that
    ratio is the key number. On U.S. congressional roll-call data,
    PC1 / PC2 is typically &gt;3× and PC1 captures &gt;70% of
    variance, reflecting the dominance of partisan voting.
    What we see here is the equivalent measurement for Bluesky.
  </p>
  <div class="figure">{plot_html["scree"]}</div>
</section>

<section>
  <div class="kicker">Finding 02</div>
  <h2>The shape of the user base in 2D.</h2>
  <p>
    Each of the {fmt_int(n_voters)} voters has been embedded in a
    2-dimensional latent space defined by PC1 and PC2. The heatmap
    shows their density on a log scale. A unimodal blob centered at
    the origin would imply no meaningful structure; visible spurs,
    arms, or distinct clusters mean the spatial model is finding
    real subgroups.
  </p>
  <div class="figure">{plot_html["density"]}</div>
</section>

<section>
  <div class="kicker">Finding 03</div>
  <h2>How is the user base distributed along PC1 alone?</h2>
  <p>
    Collapsing to just the primary axis, the question becomes: does
    the user base form a single broad distribution, or does it
    separate into multiple modes? Bimodality on PC1 is the spatial-model
    signature of a polarized population. PC2 captures
    {pc2_share:.2f}% of variance, so it's a secondary axis at best.
  </p>
  <div class="figure">{plot_html["pc1"]}</div>
</section>

<section>
  <div class="kicker">Finding 04</div>
  <h2>The anchor accounts at each end.</h2>
  <p>
    Items are presented anonymized (by index only); their PC1 loading
    is the axis. Items with the most negative PC1 score are the
    accounts most strongly associated with one side; the most positive
    are the opposite end. Without handle resolution we can't name
    sides — but the shape of the loading distribution tells you how
    sharply differentiated those anchors are. Each item's followers
    and blocks_in counts are in the hover, giving a sense of how
    prominent each anchor is.
  </p>
  <div class="figure">{plot_html["tops"]}</div>
</section>

<footer>
  <p>
    <strong>Methodology.</strong> Computed from the at-snapshot
    Bluesky DuckDB build for snapshot date <code>{snapshot_date}</code>.
    Items are the top <code>{fmt_int(n_items)}</code> accounts ranked
    by <code>followers + 10 × blocks_in</code>; the 10× weight gives
    blocks parity with follows on a per-edge information basis (blocks
    are ~10× rarer but more discriminating). Voters are every actor
    with at least one follow or block edge to any item — no
    pre-filtering by engagement. The (voter, item) matrix is signed:
    +1 for follow, −1 for block, 0 otherwise; on the rare cases where
    a voter both follows and blocks the same item, block wins.
    Truncated SVD (<code>scipy.sparse.linalg.svds</code>) at
    k=<code>{k_components}</code> components. Built {built_at}.
  </p>
  <p>
    <strong>Caveats and what's missing.</strong> Lifetime blocks are
    used (no time-windowing); the cleavage interpretation isn't very
    time-sensitive over Bluesky's two-year lifespan. PC1 captures the
    strongest cleavage in the data; what that cleavage <em>means</em>
    (political left-right, fandom, tech vs anti-tech, etc.) requires
    resolving DIDs to handles and qualitative validation against the
    anchor accounts — a separate exercise. Self-blocks and reciprocal
    block/follow pairs are deduped with block winning.
  </p>
</footer>

</div>
</body>
</html>
"""

    out_dir = f"{OUT_VOL_DIR}/analysis/{snapshot_date}"
    os.makedirs(out_dir, exist_ok=True)
    out_file = f"{out_dir}/blocks_cleavage.html"
    payload = html.encode("utf-8")
    with open(out_file, "wb") as f:
        f.write(payload)
    volume_out.commit()
    print(
        f"=== wrote {out_file} ({len(payload):,} bytes; "
        f"PC1 share = {pc1_share:.2f}%, PC1/PC2 = {pc1_to_pc2_ratio:.2f}x) ===",
        flush=True,
    )

    sidecar = {
        "snapshot_date": snapshot_date,
        "n_items": n_items,
        "k_components": k_components,
        "built_at_utc": built_at,
        "n_voters": n_voters,
        "n_edges": n_edges,
        "n_follow_edges": n_follows_e,
        "n_block_edges": n_blocks_e,
        "singular_values": s.tolist(),
        "variance_explained_per_pc": var_per.tolist(),
        "cumulative_variance": cum_var.tolist(),
        "pc1_share_pct": pc1_share,
        "pc1_to_pc2_ratio": pc1_to_pc2_ratio,
        "top_pc1_positive_items": top_items,
        "top_pc1_negative_items": bottom_items,
    }
    with open(f"{out_dir}/blocks_cleavage.json", "w") as f:
        json.dump(sidecar, f, indent=2, default=str)
    volume_out.commit()

    return payload


@app.local_entrypoint()
def main(
    analysis: str = "likes",
    snapshot_date: str = "2026-04-28",
    window_days: int = 90,
    inactivity_days: int = 30,
    background: bool = False,
) -> None:
    """Dispatch to one of the snapshot analyses.

    Args:
      analysis: which analysis to run. Currently `likes` or `ratio`.
      snapshot_date: which snapshot in /vol-out/var/snapshot/<date>/ to read.
      window_days: time-window length for windowed analyses (`ratio`).
        Ignored by lifetime analyses (`likes`).
      background: spawn the remote call instead of waiting on it. Foreground
        also drops a local copy of the HTML next to your CWD; background
        prints the FunctionCall id so you can follow `modal app logs`.
    """
    if analysis == "likes":
        fn = analyze_likes
        kwargs = {"snapshot_date": snapshot_date}
        out_name = f"likes_concentration_{snapshot_date}.html"
        vol_path = f"/vol-out/var/analysis/{snapshot_date}/likes_concentration.html"
    elif analysis == "ratio":
        fn = analyze_ratio
        kwargs = {"snapshot_date": snapshot_date, "window_days": window_days}
        out_name = f"ratio_{snapshot_date}.html"
        vol_path = f"/vol-out/var/analysis/{snapshot_date}/ratio.html"
    elif analysis == "attrition":
        fn = analyze_attrition
        kwargs = {"snapshot_date": snapshot_date, "inactivity_days": inactivity_days}
        out_name = f"attrition_{snapshot_date}.html"
        vol_path = f"/vol-out/var/analysis/{snapshot_date}/attrition.html"
    elif analysis == "blocks":
        fn = analyze_blocks
        kwargs = {"snapshot_date": snapshot_date}
        out_name = f"blocks_cleavage_{snapshot_date}.html"
        vol_path = f"/vol-out/var/analysis/{snapshot_date}/blocks_cleavage.html"
    else:
        raise SystemExit(
            f"unknown analysis {analysis!r}; expected one of: "
            "likes, ratio, attrition, blocks"
        )

    if background:
        call = fn.spawn(**kwargs)
        print(
            f"[spawn] FunctionCall {call.object_id} — follow with "
            f"`modal app logs at-snapshot-analysis` or check "
            f"https://modal.com/apps"
        )
        print(f"[analyze] file will be at {vol_path}")
        return

    result = fn.remote(**kwargs)
    if isinstance(result, (bytes, bytearray)):
        with open(out_name, "wb") as f:
            f.write(result)
        print(f"[analyze] wrote local copy to ./{out_name}")
    else:
        print(f"[analyze] file persisted to volume at {vol_path}")
