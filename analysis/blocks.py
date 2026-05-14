"""Signed-graph DW-NOMINATE-style analysis.

Builds a sparse signed matrix M[v, i] = +1 (follow) / -1 (block) over
the top-n_items most-salient accounts and runs truncated SVD to recover
latent dimensions of the social graph.

Public entrypoint: `run(con, snapshot_date, n_items=20000, k_components=10)`.
"""

from __future__ import annotations

import time

from .common import (
    BRAND, SHARED_CSS,
    built_at_utc, fig_html, fmt_int, install_template,
    plotlyjs_inline,
)


def run(
    con,
    snapshot_date: str,
    n_items: int = 20000,
    k_components: int = 10,
    *,
    log: bool = True,
) -> tuple[bytes, dict]:
    import numpy as np
    import plotly.graph_objects as go
    import scipy.sparse as sp
    from scipy.sparse.linalg import svds

    if log:
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
    if log:
        print(f"  ({time.time()-t0:.1f}s) items_t materialized", flush=True)

    item_meta = con.execute(
        "SELECT idx, did_id, followers, blocks_in, salience FROM items_t ORDER BY idx"
    ).fetchall()
    if not item_meta:
        raise RuntimeError("blocks analysis: no items with followers + blocks_in*10 > 0")
    if log:
        print(
            f"  salience range: top item has {item_meta[0][4]:,}, "
            f"bottom item has {item_meta[-1][4]:,}",
            flush=True,
        )

    if log:
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
    if log:
        print(f"  ({time.time()-t0:.1f}s) edges_raw materialized", flush=True)

    if log:
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
    if log:
        print(f"  ({time.time()-t0:.1f}s) edges_t materialized", flush=True)

    if log:
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
    if log:
        print(
            f"  ({time.time()-t0:.1f}s) voters={n_voters:,}, edges={n_edges:,} "
            f"(follow={n_follows_e:,}, block={n_blocks_e:,})",
            flush=True,
        )

    actual_items = len(item_meta)
    max_k = max(1, min(n_voters, actual_items) - 1)
    if k_components > max_k:
        if log:
            print(
                f"  reducing k from {k_components} to {max_k} (limited by matrix shape)",
                flush=True,
            )
        k_components = max_k

    if log:
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
    if log:
        print(f"  ({time.time()-t0:.1f}s) {len(rows):,} triplets in numpy", flush=True)

    con.execute("DROP TABLE edges_t")
    con.execute("DROP TABLE voters_t")
    del arrow

    if log:
        print(
            f"=== build sparse {n_voters:,} × {actual_items:,} matrix "
            f"({len(rows):,} non-zeros) ===",
            flush=True,
        )
    t0 = time.time()
    M = sp.coo_matrix(
        (vals, (rows, cols)), shape=(n_voters, actual_items)
    ).tocsr()
    if log:
        print(f"  ({time.time()-t0:.1f}s) sparse matrix built", flush=True)
    del rows, cols, vals

    fro_sq = float((M.multiply(M)).sum())

    if log:
        print(f"=== truncated SVD (k={k_components}) ===", flush=True)
    t0 = time.time()
    U, s, Vt = svds(M, k=k_components)
    order = np.argsort(s)[::-1]
    U = U[:, order]
    s = s[order]
    Vt = Vt[order, :]
    if log:
        print(f"  ({time.time()-t0:.1f}s) SVD complete; top sv = {s[0]:.2f}", flush=True)

    var_per = (s ** 2) / fro_sq if fro_sq else (s ** 2) * 0
    cum_var = np.cumsum(var_per)

    pc1 = U[:, 0]
    pc2 = U[:, 1] if U.shape[1] > 1 else np.zeros_like(pc1)
    x_lo, x_hi = np.percentile(pc1, [0.5, 99.5])
    y_lo, y_hi = np.percentile(pc2, [0.5, 99.5])
    # When the spread along an axis is zero (tiny synthetic data), widen
    # the bounds slightly so histogram2d doesn't reject the range.
    if x_lo == x_hi:
        x_lo, x_hi = x_lo - 1e-6, x_hi + 1e-6
    if y_lo == y_hi:
        y_lo, y_hi = y_lo - 1e-6, y_hi + 1e-6
    H, xedges, yedges = np.histogram2d(
        pc1, pc2, bins=min(200, max(10, n_voters // 4)),
        range=[[x_lo, x_hi], [y_lo, y_hi]],
    )
    H_log = np.log10(H + 1)

    pc1_hist, pc1_edges = np.histogram(pc1, bins=min(200, max(10, n_voters // 4)), range=(x_lo, x_hi))

    item_pc1 = Vt[0, :]
    sort_idx = np.argsort(item_pc1)
    head_n = min(25, len(item_pc1) // 2 or 1)
    bottom_idx = sort_idx[:head_n]
    top_idx = sort_idx[-head_n:][::-1]

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

    if log:
        print(
            f"  PC1 range: [{item_pc1.min():.3f}, {item_pc1.max():.3f}]",
            flush=True,
        )

    install_template()

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

    fig_density = go.Figure(go.Heatmap(
        z=H_log.T,
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

    pc1_centers = (pc1_edges[:-1] + pc1_edges[1:]) / 2
    fig_pc1 = go.Figure(go.Bar(
        x=pc1_centers, y=pc1_hist,
        marker=dict(color=BRAND),
        hovertemplate="PC1 %{x:.3f}<br>%{y:,} voters<extra></extra>",
    ))
    fig_pc1.update_layout(
        template="bsky",
        title=dict(text="<b>Distribution of voters along PC1</b>",
                   x=0.02, xanchor="left"),
        xaxis=dict(title="PC1 score"),
        yaxis=dict(title="Voters (count)"),
        height=380, bargap=0.0,
    )

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
        title=dict(text=f"<b>Top {head_n} items at each end of PC1</b>",
                   x=0.02, xanchor="left"),
        xaxis=dict(title="Item loading on PC1"),
        yaxis=dict(autorange="reversed", showticklabels=False, title="Items (anonymized)"),
        height=520, barmode="overlay",
        legend=dict(orientation="h", y=-0.12),
    )

    plot_html = {
        "scree": fig_html(fig_scree, "fig_scree"),
        "density": fig_html(fig_density, "fig_density"),
        "pc1": fig_html(fig_pc1, "fig_pc1"),
        "tops": fig_html(fig_tops, "fig_tops"),
    }
    plotlyjs = plotlyjs_inline()

    built_at = built_at_utc()
    pc1_share = float(var_per[0]) * 100
    pc1_to_pc2_ratio = float(s[0] / s[1]) if len(s) > 1 and s[1] > 0 else float("inf")
    pc2_share = float(var_per[1]) * 100 if len(var_per) > 1 else 0.0

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
  binary roll-call votes. Here each Bluesky user "votes" on every
  prominent account by either following them (+1) or blocking them (−1).
  We fit it on <strong>{fmt_int(n_voters)}</strong> voters by
  <strong>{fmt_int(actual_items)}</strong> high-salience items
  ({fmt_int(n_edges)} signed edges).
</p>

<div class="stats">
  <div class="stat">
    <div class="v brand">{pc1_share:.2f}%</div>
    <div class="l">variance on PC1</div>
    <div class="sub">share captured by one axis</div>
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
    <div class="v">{fmt_int(actual_items)}</div>
    <div class="l">items in the model</div>
    <div class="sub">ranked by followers + 10 × blocks_in</div>
  </div>
</div>

<section>
  <div class="kicker">Finding 01</div>
  <h2>How concentrated is the cleavage on a single axis?</h2>
  <p>
    PC1 captures <strong>{pc1_share:.2f}%</strong> of total variance and is
    <strong>{pc1_to_pc2_ratio:.2f}×</strong> larger than PC2.
  </p>
  <div class="figure">{plot_html["scree"]}</div>
</section>

<section>
  <div class="kicker">Finding 02</div>
  <h2>The shape of the user base in 2D.</h2>
  <p>
    Each voter has been embedded in a 2-dimensional latent space defined by
    PC1 and PC2.
  </p>
  <div class="figure">{plot_html["density"]}</div>
</section>

<section>
  <div class="kicker">Finding 03</div>
  <h2>How is the user base distributed along PC1 alone?</h2>
  <p>
    PC2 captures {pc2_share:.2f}% of variance — a secondary axis at best.
  </p>
  <div class="figure">{plot_html["pc1"]}</div>
</section>

<section>
  <div class="kicker">Finding 04</div>
  <h2>The anchor accounts at each end.</h2>
  <p>
    Items are presented anonymized by index. PC1 loading is the axis.
  </p>
  <div class="figure">{plot_html["tops"]}</div>
</section>

<footer>
  <p>
    <strong>Methodology.</strong> Computed from the at-snapshot Bluesky
    DuckDB build for snapshot date <code>{snapshot_date}</code>. Items are
    the top <code>{fmt_int(actual_items)}</code> accounts ranked by
    <code>followers + 10 × blocks_in</code>. Truncated SVD
    (<code>scipy.sparse.linalg.svds</code>) at
    k=<code>{k_components}</code> components. Built {built_at}.
  </p>
</footer>

</div>
</body>
</html>
"""

    sidecar = {
        "snapshot_date": snapshot_date,
        "n_items": actual_items,
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
    return html.encode("utf-8"), sidecar
