"""Booster-account graph analysis.

Quantifies what share of (recently-created) Bluesky accounts are *boosters*:
accounts that exist essentially to follow a single account — or a couple of
*related* accounts — and do nothing else (no posts/replies/reposts/quotes).

Definitions (all tunable):
  - We strip the one platform-default follow every signup gets: `bsky.app`
    (did:plc:z72i7hdynmk6r22z27h6tvur). "Out-degree" below is *post-strip*.
  - booster = created >= `created_after` AND post-strip out-degree in
    [1, `booster_max_outdeg`] AND zero authored content.
  - A target's `booster_ratio` = (booster followers) / (all followers).
  - Farms = communities in the boosted-target co-follow graph (igraph), i.e.
    clusters of related accounts the same booster population follows together.

Account creation dates come from the PLC directory export. They're read from
`actors.created_at` when the snapshot was built with the `plc` ETL phase;
otherwise we read the PLC parquet shards directly (`plc_glob`) and join by DID,
so this analysis runs against an un-enriched published snapshot too.

Public entrypoint: `run(con, snapshot_date, ...) -> (html_bytes, sidecar)`.
"""

from __future__ import annotations

import time

from .common import (
    BRAND, SHARED_CSS,
    built_at_utc, fmt_int, install_template,
)

BSKY_APP_DID = "did:plc:z72i7hdynmk6r22z27h6tvur"


def _has_plc_shards(con, plc_glob: str) -> bool:
    if not plc_glob:
        return False
    try:
        con.execute(f"SELECT 1 FROM read_parquet('{plc_glob}') LIMIT 1").fetchone()
        return True
    except Exception:
        return False


def run(
    con,
    snapshot_date: str,
    *,
    created_after: str = "2025-01-01",
    booster_max_outdeg: int = 3,
    min_target_support: int = 5,
    top_targets: int = 100,
    plc_glob: str = "/vol-out/var/plc/*.parquet",
    build_full_graph: bool = False,
    hydrate_handles: bool = True,
    log: bool = True,
) -> tuple[bytes, dict]:
    import plotly.graph_objects as go

    install_template()
    t_start = time.time()

    def say(msg: str) -> None:
        if log:
            print(f"=== {msg} ===", flush=True)

    # --- Stage 1: resolve bsky.app + creation-date source --------------------
    row = con.execute("SELECT did_id FROM actors WHERE did = ?", [BSKY_APP_DID]).fetchone()
    if not row:
        raise RuntimeError(f"bsky.app ({BSKY_APP_DID}) not found in actors")
    bsky_id = row[0]
    say(f"bsky.app did_id = {bsky_id}")

    actor_cols = {r[0] for r in con.execute("DESCRIBE actors").fetchall()}
    have_baked = "created_at" in actor_cols
    plc_built = False

    if have_baked:
        say("using baked actors.created_at (snapshot built with plc phase)")
        in_microcosm_expr = "COALESCE(a.in_microcosm, TRUE)"
        tomb_expr = "a.tombstoned_at" if "tombstoned_at" in actor_cols else "CAST(NULL AS TIMESTAMP)"
        pds_expr = "a.pds" if "pds" in actor_cols else "CAST(NULL AS VARCHAR)"
        handle_expr = "a.handle" if "handle" in actor_cols else "CAST(NULL AS VARCHAR)"
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE acct AS
              SELECT a.did_id, a.did, a.created_at, {tomb_expr} AS tombstoned_at,
                     {in_microcosm_expr} AS in_microcosm,
                     {pds_expr} AS pds, {handle_expr} AS handle
              FROM actors a
        """)
        age_enabled = True
    elif _has_plc_shards(con, plc_glob):
        say(f"reading PLC shards from {plc_glob}")
        plc_built = True
        # pds/handle: latest op that carries one (arg_max over ts) -> current.
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE plc_acct AS
              SELECT did,
                     MIN(ts) FILTER (WHERE kind='create')          AS created_at,
                     MAX(ts) FILTER (WHERE kind='tombstone')        AS tombstoned_at,
                     arg_max(pds, ts)    FILTER (WHERE pds IS NOT NULL)    AS pds,
                     arg_max(handle, ts) FILTER (WHERE handle IS NOT NULL) AS handle
              FROM read_parquet('{plc_glob}')
              GROUP BY did
        """)
        con.execute("""
            CREATE OR REPLACE TEMP TABLE acct AS
              SELECT a.did_id, a.did, p.created_at, p.tombstoned_at, TRUE AS in_microcosm,
                     p.pds, p.handle
              FROM actors a LEFT JOIN plc_acct p ON p.did = a.did
        """)
        age_enabled = True
    else:
        say("no created_at source — age filter DISABLED (dry-run mode)")
        con.execute("""
            CREATE OR REPLACE TEMP TABLE acct AS
              SELECT a.did_id, a.did, CAST(NULL AS TIMESTAMP) AS created_at,
                     CAST(NULL AS TIMESTAMP) AS tombstoned_at, TRUE AS in_microcosm,
                     CAST(NULL AS VARCHAR) AS pds, CAST(NULL AS VARCHAR) AS handle
              FROM actors a
        """)
        age_enabled = False

    # --- Stage 2: per-account node table (post-strip out-degree) -------------
    say("building follows_bsky + node table")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE follows_bsky AS
          SELECT DISTINCT src_did_id AS did_id FROM follows WHERE dst_did_id = {bsky_id}
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE node AS
          SELECT ac.did_id, ac.did,
                 COALESCE(ag.follows, 0)   AS follows,
                 COALESCE(ag.followers, 0) AS followers,
                 COALESCE(ag.posts + ag.replies_out + ag.reposts_out + ag.quotes_out, 0) AS content,
                 COALESCE(ag.likes_out, 0) AS likes_out,
                 COALESCE(ag.follows, 0) - (CASE WHEN fb.did_id IS NOT NULL THEN 1 ELSE 0 END) AS adj_out,
                 ac.created_at, ac.tombstoned_at, ac.in_microcosm, ac.pds, ac.handle
          FROM acct ac
          LEFT JOIN actor_aggs ag USING(did_id)
          LEFT JOIN follows_bsky fb ON fb.did_id = ac.did_id
    """)

    cutoff = created_after
    in_pop = (f"(created_at IS NOT NULL AND created_at >= TIMESTAMP '{cutoff}')"
              if age_enabled else "TRUE")
    booster = f"({in_pop} AND adj_out BETWEEN 1 AND {booster_max_outdeg} AND content = 0)"

    # --- Stage 3: headline classification ------------------------------------
    say("classifying boosters")
    (n_pop, n_boost, b1, b2, b3, b_strict, n_pop_micro, n_tomb) = con.execute(f"""
        SELECT
          count(*) FILTER (WHERE {in_pop})                                   AS n_pop,
          count(*) FILTER (WHERE {booster})                                  AS n_boost,
          count(*) FILTER (WHERE {booster} AND adj_out = 1)                  AS b1,
          count(*) FILTER (WHERE {booster} AND adj_out = 2)                  AS b2,
          count(*) FILTER (WHERE {booster} AND adj_out = 3)                  AS b3,
          count(*) FILTER (WHERE {booster} AND likes_out = 0)                AS b_strict,
          count(*) FILTER (WHERE {in_pop} AND in_microcosm)                  AS n_pop_micro,
          count(*) FILTER (WHERE {booster} AND tombstoned_at IS NOT NULL)    AS n_tomb
        FROM node
    """).fetchone()

    # PLC-only accounts (created>=cutoff) that microcosm never indexed.
    if have_baked:
        n_plc_only = con.execute(f"""
            SELECT count(*) FROM acct
            WHERE NOT in_microcosm AND created_at IS NOT NULL
              AND created_at >= TIMESTAMP '{cutoff}'
        """).fetchone()[0]
        n_total_pop = n_pop                      # already includes PLC-only
    elif plc_built:
        n_plc_only = con.execute(f"""
            SELECT count(*) FROM plc_acct p
            LEFT JOIN actors a ON a.did = p.did
            WHERE a.did IS NULL AND p.created_at IS NOT NULL
              AND p.created_at >= TIMESTAMP '{cutoff}'
        """).fetchone()[0]
        n_total_pop = n_pop + n_plc_only         # node excludes PLC-only
    else:
        n_plc_only = 0
        n_total_pop = n_pop

    pct_total = (100.0 * n_boost / n_total_pop) if n_total_pop else 0.0
    pct_micro = (100.0 * n_boost / n_pop_micro) if n_pop_micro else 0.0
    say(f"boosters={n_boost:,} / pop={n_total_pop:,} ({pct_total:.1f}%)")

    # --- Stage 4: per-target concentration -----------------------------------
    say("aggregating per-target booster concentration")
    con.execute(f"CREATE OR REPLACE TEMP TABLE booster_ids AS SELECT did_id FROM node WHERE {booster}")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE target_stats AS
          SELECT f.dst_did_id AS did_id, count(*) AS booster_followers
          FROM follows f JOIN booster_ids b ON f.src_did_id = b.did_id
          WHERE f.dst_did_id <> {bsky_id}
          GROUP BY 1
    """)
    n_targets = con.execute(
        f"SELECT count(*) FROM target_stats WHERE booster_followers >= {min_target_support}"
    ).fetchone()[0]
    top_rows = con.execute(f"""
        SELECT t.did_id, a.did, t.booster_followers,
               COALESCE(ag.followers, 0) AS total_followers,
               t.booster_followers::DOUBLE / NULLIF(ag.followers, 0) AS booster_ratio
        FROM target_stats t
        JOIN actors a USING(did_id)
        LEFT JOIN actor_aggs ag USING(did_id)
        WHERE t.booster_followers >= {min_target_support}
        ORDER BY t.booster_followers DESC
        LIMIT {top_targets}
    """).fetchall()

    # --- Stage 5: farm structure (igraph community detection) ----------------
    say("building boosted-target co-follow graph (farms)")
    pairs = con.execute(f"""
        WITH bt AS (
          SELECT f.src_did_id AS b, f.dst_did_id AS t
          FROM follows f
          JOIN booster_ids bi ON bi.did_id = f.src_did_id
          JOIN target_stats q ON q.did_id = f.dst_did_id AND q.booster_followers >= {min_target_support}
          WHERE f.dst_did_id <> {bsky_id}
        )
        SELECT x.t AS t1, y.t AS t2, count(*) AS w
        FROM bt x JOIN bt y ON x.b = y.b AND x.t < y.t
        GROUP BY 1, 2
        HAVING count(*) >= {min_target_support}
    """).fetchall()

    farms_summary: list[dict] = []
    n_farms = 0
    largest_farm = 0
    try:
        import igraph as ig
        if pairs:
            tids = sorted({t for p in pairs for t in (p[0], p[1])})
            idx = {t: i for i, t in enumerate(tids)}
            edges = [(idx[a], idx[b]) for a, b, _ in pairs]
            weights = [int(w) for _, _, w in pairs]
            g = ig.Graph(n=len(tids), edges=edges)
            g.es["weight"] = weights
            comms = g.community_multilevel(weights="weight")
            sizes = sorted((len(c) for c in comms), reverse=True)
            n_farms = len([s for s in sizes if s >= 2])
            largest_farm = sizes[0] if sizes else 0
            # describe the biggest few farms by their highest-support members
            did_for_idx = {i: t for t, i in idx.items()}
            bf = {r[0]: r[2] for r in top_rows}  # did_id -> booster_followers (top only)
            for c in sorted(comms, key=len, reverse=True)[:10]:
                if len(c) < 2:
                    continue
                members = [did_for_idx[i] for i in c]
                support = sum(bf.get(m, 0) for m in members)
                farms_summary.append({"size": len(c), "top_support": support,
                                      "member_did_ids": members[:25]})
        say(f"farms: {n_farms} (largest {largest_farm} accounts)")
    except ImportError:
        say("igraph not available — skipping farm detection")

    # --- Stage 5b (optional): full-graph igraph build + degree cross-check ---
    full_graph_stats = None
    if build_full_graph:
        full_graph_stats = _build_full_graph_and_crosscheck(con, bsky_id, say)

    # --- Stage 6: hydrate handles + out-degree distribution ------------------
    handles = {}
    if hydrate_handles and top_rows:
        handles = _resolve_handles([r[1] for r in top_rows[:top_targets]], say)

    say("out-degree distribution")
    dist = con.execute(f"""
        WITH b AS (SELECT follows, adj_out FROM node WHERE {in_pop})
        SELECT bucket, sum(pre) AS pre, sum(post) AS post FROM (
          SELECT CASE WHEN follows=0 THEN '0' WHEN follows=1 THEN '1' WHEN follows=2 THEN '2'
                      WHEN follows=3 THEN '3' WHEN follows BETWEEN 4 AND 5 THEN '4-5'
                      WHEN follows BETWEEN 6 AND 10 THEN '6-10' WHEN follows BETWEEN 11 AND 50 THEN '11-50'
                      WHEN follows BETWEEN 51 AND 200 THEN '51-200' ELSE '200+' END AS bucket,
                 1 AS pre, 0 AS post FROM b
          UNION ALL
          SELECT CASE WHEN adj_out<=0 THEN '0' WHEN adj_out=1 THEN '1' WHEN adj_out=2 THEN '2'
                      WHEN adj_out=3 THEN '3' WHEN adj_out BETWEEN 4 AND 5 THEN '4-5'
                      WHEN adj_out BETWEEN 6 AND 10 THEN '6-10' WHEN adj_out BETWEEN 11 AND 50 THEN '11-50'
                      WHEN adj_out BETWEEN 51 AND 200 THEN '51-200' ELSE '200+' END AS bucket,
                 0 AS pre, 1 AS post FROM b
        ) GROUP BY bucket
    """).fetchall()
    order = ['0', '1', '2', '3', '4-5', '6-10', '11-50', '51-200', '200+']
    dmap = {r[0]: (r[1], r[2]) for r in dist}
    pre_vals = [dmap.get(o, (0, 0))[0] for o in order]
    post_vals = [dmap.get(o, (0, 0))[1] for o in order]

    # --- PDS facet: standard (bsky-hosted) vs self-hosted -------------------
    say("pds facet")
    STD = "(pds IS NOT NULL AND (pds LIKE '%bsky.network%' OR pds LIKE '%bsky.social%'))"
    SELF = "(pds IS NOT NULL AND NOT (pds LIKE '%bsky.network%' OR pds LIKE '%bsky.social%'))"
    (sh_acct, sh_boost, sh_pds_n, std_acct, no_pds) = con.execute(f"""
        SELECT
          count(*) FILTER (WHERE {in_pop} AND {SELF})      AS sh_acct,
          count(*) FILTER (WHERE {booster} AND {SELF})     AS sh_boost,
          count(DISTINCT pds) FILTER (WHERE {SELF})        AS sh_pds_n,
          count(*) FILTER (WHERE {in_pop} AND {STD})       AS std_acct,
          count(*) FILTER (WHERE {in_pop} AND pds IS NULL) AS no_pds
        FROM node
    """).fetchone()
    pds_top = con.execute(f"""
        SELECT pds,
               count(*) FILTER (WHERE {in_pop})  AS accounts,
               count(*) FILTER (WHERE {booster}) AS boosters
        FROM node WHERE {SELF}
        GROUP BY pds
        HAVING count(*) FILTER (WHERE {in_pop}) > 0
        ORDER BY accounts DESC
        LIMIT 40
    """).fetchall()
    say(f"self-hosted PDSes: {sh_pds_n:,} hosts, {sh_acct:,} accounts, {sh_boost:,} boosters")

    # --- render --------------------------------------------------------------
    sidecar = {
        "snapshot_date": snapshot_date,
        "created_after": cutoff if age_enabled else None,
        "age_filter_enabled": age_enabled,
        "created_at_source": ("baked" if have_baked else "plc_shards" if plc_built else "none"),
        "bsky_app_did_id": int(bsky_id),
        "booster_max_outdeg": booster_max_outdeg,
        "population_total": int(n_total_pop),
        "population_microcosm": int(n_pop_micro),
        "population_plc_only": int(n_plc_only),
        "boosters": int(n_boost),
        "boosters_outdeg_1": int(b1),
        "boosters_outdeg_2": int(b2),
        "boosters_outdeg_3": int(b3),
        "boosters_no_likes_either": int(b_strict),
        "boosters_tombstoned": int(n_tomb),
        "booster_pct_of_total_pop": round(pct_total, 3),
        "booster_pct_of_microcosm_pop": round(pct_micro, 3),
        "boosted_targets_ge_support": int(n_targets),
        "min_target_support": min_target_support,
        "farms": int(n_farms),
        "largest_farm": int(largest_farm),
        "pds_breakdown": {
            "self_hosted_accounts": int(sh_acct),
            "self_hosted_boosters": int(sh_boost),
            "self_hosted_pds_count": int(sh_pds_n),
            "standard_accounts": int(std_acct),
            "no_pds_accounts": int(no_pds),
            "top_self_hosted": [
                {"pds": r[0], "accounts": int(r[1]), "boosters": int(r[2]),
                 "booster_share": (round(r[2] / r[1], 4) if r[1] else None)}
                for r in pds_top
            ],
        },
        "full_graph": full_graph_stats,
        "top_targets": [
            {"did_id": int(r[0]), "did": r[1], "handle": handles.get(r[1]),
             "booster_followers": int(r[2]), "total_followers": int(r[3]),
             "booster_ratio": (round(r[4], 4) if r[4] is not None else None)}
            for r in top_rows
        ],
        "farms_detail": farms_summary,
        "elapsed_secs": round(time.time() - t_start, 1),
    }

    html = _render_html(snapshot_date, sidecar, order, pre_vals, post_vals,
                        top_rows, handles, b1, b2, b3, pds_top, go)
    say(f"done in {sidecar['elapsed_secs']}s")
    return html, sidecar


def _build_full_graph_and_crosscheck(con, bsky_id, say):
    """Optional: materialize the full 1.33B-edge directed graph in igraph and
    cross-check out-degrees against the DuckDB adj_out. EXPENSIVE — needs a
    high-memory container; off by default."""
    import os

    import igraph as ig

    say("FULL GRAPH: exporting stripped edgelist (this is the expensive path)")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE remap AS
          SELECT did_id, (ROW_NUMBER() OVER (ORDER BY did_id) - 1) AS idx FROM actors
    """)
    edge_path = "/tmp/edges.txt"
    con.execute(f"""
        COPY (
          SELECT s.idx, d.idx
          FROM follows f
          JOIN remap s ON s.did_id = f.src_did_id
          JOIN remap d ON d.did_id = f.dst_did_id
          WHERE f.dst_did_id <> {bsky_id}
        ) TO '{edge_path}' (FORMAT CSV, DELIMITER ' ', HEADER false)
    """)
    say(f"FULL GRAPH: Read_Edgelist from {edge_path} ({os.path.getsize(edge_path)/1e9:.1f} GB)")
    g = ig.Graph.Read_Edgelist(edge_path, directed=True)
    n_actors = con.execute("SELECT count(*) FROM actors").fetchone()[0]
    if g.vcount() < n_actors:
        g.add_vertices(n_actors - g.vcount())
    comps = g.connected_components(mode="weak")
    sizes = sorted((len(c) for c in comps), reverse=True)
    say(f"FULL GRAPH: |V|={g.vcount():,} |E|={g.ecount():,} giant_component={sizes[0]:,}")
    return {"vcount": g.vcount(), "ecount": g.ecount(),
            "giant_component": sizes[0] if sizes else 0,
            "n_components": len(sizes)}


def _resolve_handles(dids: list[str], say) -> dict:
    """Resolve did -> handle via the bsky public AppView getProfiles (25/call)."""
    import json
    import urllib.parse
    import urllib.request

    base = "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfiles"
    out: dict[str, str] = {}
    say(f"resolving {len(dids)} handles via getProfiles")
    for i in range(0, len(dids), 25):
        batch = dids[i:i + 25]
        qs = "&".join("actors=" + urllib.parse.quote(d) for d in batch)
        try:
            with urllib.request.urlopen(f"{base}?{qs}", timeout=30) as resp:
                data = json.load(resp)
            for p in data.get("profiles", []):
                out[p["did"]] = p.get("handle")
        except Exception as e:
            say(f"  handle batch failed: {e}")
    return out


def _png_img(fig, width: int, height: int) -> str:
    """Render a plotly figure to a static base64 PNG <img>. Self-contained and
    JS-free, so it displays in any HTML viewer (incl. ones that don't run
    JavaScript). Falls back to a note if kaleido is unavailable."""
    import base64
    try:
        png = fig.to_image(format="png", width=width, height=height, scale=2)
    except Exception as e:  # kaleido missing / render error
        return f"<div class='sub' style='color:var(--muted)'>[chart unavailable: {e}]</div>"
    b64 = base64.b64encode(png).decode("ascii")
    return f'<img alt="chart" style="width:100%;height:auto" src="data:image/png;base64,{b64}">'


def _render_html(snapshot_date, sc, order, pre_vals, post_vals, top_rows, handles,
                 b1, b2, b3, pds_top, go) -> bytes:
    pct = sc["booster_pct_of_total_pop"]
    age_note = (f"created on/after {sc['created_after']}" if sc["age_filter_enabled"]
                else "ALL ages (age filter disabled — PLC dates unavailable)")

    fig_dist = go.Figure()
    fig_dist.add_bar(x=order, y=pre_vals, name="raw follows", marker_color="#cbd5e1")
    fig_dist.add_bar(x=order, y=post_vals, name="post-bsky.app strip", marker_color=BRAND)
    fig_dist.update_layout(barmode="group", height=380,
                           xaxis_title="out-degree", yaxis_title="accounts",
                           legend=dict(orientation="h", y=1.1))

    fig_split = go.Figure()
    fig_split.add_bar(x=["follows 1", "follows 2", "follows 3"], y=[b1, b2, b3],
                      marker_color=[BRAND, "#ff5d8f", "#7c3aed"])
    fig_split.update_layout(height=360, yaxis_title="booster accounts",
                            xaxis_title="post-strip out-degree")

    n_top = min(20, len(top_rows))
    labels = [handles.get(r[1]) or r[1][:24] for r in top_rows[:n_top]][::-1]
    vals = [r[2] for r in top_rows[:n_top]][::-1]
    fig_top = go.Figure()
    fig_top.add_bar(x=vals, y=labels, orientation="h", marker_color=BRAND)
    fig_top.update_layout(height=560, xaxis_title="booster followers",
                          margin=dict(l=220))

    rows_html = "\n".join(
        f"<tr><td>{i+1}</td><td>{(handles.get(r[1]) or '')}</td>"
        f"<td class='did'>{r[1]}</td><td>{fmt_int(r[2])}</td>"
        f"<td>{fmt_int(r[3])}</td>"
        f"<td>{(f'{r[4]*100:.1f}%' if r[4] is not None else '—')}</td></tr>"
        for i, r in enumerate(top_rows[:50])
    )

    # PDS facet — only render the section when there are self-hosted hosts.
    pb = sc["pds_breakdown"]
    pds_section = ""
    if pds_top:
        n_pds = min(15, len(pds_top))
        p_labels = [r[0].replace("https://", "").replace("http://", "")[:40]
                    for r in pds_top[:n_pds]][::-1]
        p_acct = [r[1] for r in pds_top[:n_pds]][::-1]
        p_boost = [r[2] for r in pds_top[:n_pds]][::-1]
        fig_pds = go.Figure()
        fig_pds.add_bar(x=p_acct, y=p_labels, orientation="h", name="accounts",
                        marker_color="#94a3b8")
        fig_pds.add_bar(x=p_boost, y=p_labels, orientation="h", name="boosters",
                        marker_color="#ef4444")
        fig_pds.update_layout(height=560, barmode="overlay", margin=dict(l=270),
                              xaxis_title="accounts on host (self-hosted PDSes)",
                              legend=dict(orientation="h", y=1.07))
        pds_rows_html = "\n".join(
            f"<tr><td>{i+1}</td><td class='did'>{r[0]}</td><td>{fmt_int(r[1])}</td>"
            f"<td>{fmt_int(r[2])}</td>"
            f"<td>{(f'{r[2]/r[1]*100:.0f}%' if r[1] else '—')}</td></tr>"
            for i, r in enumerate(pds_top[:50])
        )
        pds_section = f"""
<section>
<div class="kicker">Hosting</div>
<h2>Self-hosted PDSes</h2>
<p><strong>{fmt_int(pb['self_hosted_accounts'])}</strong> accounts in the
population run on <strong>{fmt_int(pb['self_hosted_pds_count'])}</strong>
self-hosted PDSes (not <code>*.bsky.network</code> / <code>bsky.social</code>),
including <strong>{fmt_int(pb['self_hosted_boosters'])}</strong> boosters.
Self-hosting is legitimate for many users — this surfaces hosts for review,
ranked by the accounts (grey) and boosters (red) they carry.</p>
<div class="figure">{_png_img(fig_pds, 1040, 580)}</div>
<table><thead><tr><th>#</th><th>PDS</th><th>accounts</th><th>boosters</th>
<th>booster share</th></tr></thead>
<tbody>{pds_rows_html}</tbody></table>
</section>"""

    def stat(v, label, sub="", cls=""):
        return (f"<div class='stat'><div class='v {cls}'>{v}</div>"
                f"<div class='l'>{label}</div>"
                + (f"<div class='sub'>{sub}</div>" if sub else "") + "</div>")

    cards = "".join([
        stat(fmt_int(sc["population_total"]), "accounts in population", age_note),
        stat(fmt_int(sc["boosters"]), "booster accounts",
             f"out-degree 1–{sc['booster_max_outdeg']}, no content", cls="brand"),
        stat(f"{pct:.1f}%", "of population are boosters",
             f"{sc['booster_pct_of_microcosm_pop']:.1f}% of indexed-only", cls="bad"),
        stat(fmt_int(sc["largest_farm"]), "largest farm (accounts)",
             f"{fmt_int(sc['farms'])} farms total"),
    ])

    body = f"""<!doctype html><html><head><meta charset="utf-8">
<title>Booster accounts — {snapshot_date}</title>
<style>{SHARED_CSS}
table {{ width:100%; border-collapse:collapse; font-size:13px; margin-top:10px; }}
th,td {{ text-align:left; padding:6px 10px; border-bottom:1px solid var(--rule); }}
td.did {{ font-family:ui-monospace,monospace; color:var(--muted); font-size:11px; }}
</style></head>
<body><div class="wrap">
<div class="eyebrow">atproto snapshot · {snapshot_date}</div>
<h1>The <span class="accent">booster</span> accounts</h1>
<p class="lede">Accounts that exist to follow a single account — or a couple of
related accounts — and do nothing else. Out-degree is measured after stripping
the default <code>bsky.app</code> follow. Population: accounts {age_note}.
Creation dates from the PLC directory ({sc['created_at_source']}).</p>
<div class="stats">{cards}</div>

<section>
<div class="kicker">Out-degree</div>
<h2>Stripping the default follow</h2>
<p>Raw follow counts vs. post-strip out-degree across the population. Removing
the universal <strong>bsky.app</strong> follow shifts a large mass into the
0/1/2 buckets — the booster zone.</p>
<div class="figure">{_png_img(fig_dist, 1040, 400)}</div>
</section>

<section>
<div class="kicker">Booster shape</div>
<h2>One account, or a couple of related ones</h2>
<p><strong>{fmt_int(sc['boosters'])}</strong> boosters split by how many
non-default accounts they follow. {fmt_int(sc['boosters_no_likes_either'])} of
them have never even liked a post; {fmt_int(sc['boosters_tombstoned'])} are
already deactivated (PLC tombstone).</p>
<div class="figure">{_png_img(fig_split, 1040, 380)}</div>
</section>

<section>
<div class="kicker">Targets</div>
<h2>Who the boosters boost</h2>
<p>Accounts ranked by how many booster accounts follow them
(≥ {sc['min_target_support']} support; {fmt_int(sc['boosted_targets_ge_support'])}
qualify).</p>
<div class="figure">{_png_img(fig_top, 1040, 580)}</div>
<table><thead><tr><th>#</th><th>handle</th><th>did</th>
<th>booster followers</th><th>total followers</th><th>booster ratio</th></tr></thead>
<tbody>{rows_html}</tbody></table>
</section>
{pds_section}
<p class="sub" style="color:var(--muted);margin-top:40px">
Built {built_at_utc()} · {sc['elapsed_secs']}s ·
boosters out-deg 1/2/3 = {fmt_int(b1)}/{fmt_int(b2)}/{fmt_int(b3)} ·
PLC-only inert accounts in population: {fmt_int(sc['population_plc_only'])}</p>
</div></body></html>"""
    return body.encode("utf-8")
