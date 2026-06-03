"""Follow-booster analysis: who is propped up by content-less follow accounts.

A large share of Bluesky accounts produce no original content (no posts,
replies, or quotes) and exist mainly to *follow* (and sometimes repost) other
accounts -- inflating their targets' follower counts. This analysis measures,
per target account, what fraction of its followers are such "boosters", and in
particular what fraction follow essentially *only* that account.

Definitions (all derived from `actor_aggs`):
  content   = posts + replies_out + quotes_out      (original authored material)
  booster   = an account with content = 0 that follows >= 1 account
              (reposts/likes don't count as content -- they're amplification)
  silent    = booster with reposts_out = 0 AND likes_out = 0 (pure follow)
  amplifier = booster with reposts_out > 0              (follows + reposts)

Per target T (over the follow graph, with `bsky` follows removed -- see below):
  followers_clean(T)   = # accounts that follow T
  booster_followers(T) = # of those that are boosters
  dedicated_followers(T) = # of those whose *only* follow (after removing the
                           excluded bsky accounts) is T  -> out_follows == 1
  booster_share(T)     = booster_followers / followers_clean
  dedicated_share(T)   = dedicated_followers / followers_clean   <- the headline

"Remove bsky follows": follow edges whose destination is an official Bluesky
account (bsky.app / safety / moderation) are near-universal onboarding artifacts.
We drop them so they neither (a) dominate the population edge-share nor (b)
inflate every booster's out-degree (an account that follows only `bsky.app` + T
should read as "dedicated to T", out_follows == 1).

`run(con, snapshot_date, ...)` -> (html_bytes, sidecar_dict), matching the other
analyses so the Modal dispatcher and the test harness can call it uniformly.
"""

from __future__ import annotations

import time

from analysis.common import (
    SHARED_CSS,
    built_at_utc,
    fig_html,
    fmt_int,
    install_template,
    plotlyjs_inline,
)

# Official Bluesky accounts whose follows are onboarding/default artifacts.
# Tunable via the `exclude_dids` arg (e.g. to also drop starter-pack celebs).
DEFAULT_EXCLUDE_DIDS = [
    "did:plc:z72i7hdynmk6r22z27h6tvur",  # bsky.app
    "did:plc:eon2iu7v3x2ukgxkqaf7e5np",  # safety.bsky.app
    "did:plc:ar7c4by46qjdydhdevvrndac",  # moderation.bsky.app
]


def _table_exists(con, name: str) -> bool:
    return con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
        [name],
    ).fetchone()[0] > 0


def run(
    con,
    snapshot_date: str,
    *,
    min_followers: int = 100,
    top_n: int = 200,
    exclude_dids: list[str] | None = None,
    log: bool = True,
) -> tuple[bytes, dict]:
    exclude_dids = DEFAULT_EXCLUDE_DIDS if exclude_dids is None else exclude_dids
    has_actors = _table_exists(con, "actors")
    has_reposts = _table_exists(con, "reposts") and _table_exists(con, "posts")

    def step(msg, t0):
        if log:
            print(f"  ({time.time()-t0:5.1f}s) {msg}", flush=True)

    # ---- 1. resolve the bsky exclusion set --------------------------------
    if has_actors and exclude_dids:
        ph = ",".join(["?"] * len(exclude_dids))
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE excl AS "
            f"SELECT did_id FROM actors WHERE did IN ({ph})",
            exclude_dids,
        )
    else:
        con.execute("CREATE OR REPLACE TEMP TABLE excl(did_id UBIGINT)")
    n_excl = con.execute("SELECT count(*) FROM excl").fetchone()[0]

    # ---- 2. classify every actor as booster / silent / amplifier ----------
    t0 = time.time()
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE actor_class AS
        SELECT
            did_id,
            ((posts + replies_out + quotes_out) = 0)                       AS contentless,
            ((posts + replies_out + quotes_out) = 0 AND reposts_out > 0)   AS amplifier,
            ((posts + replies_out + quotes_out) = 0
             AND reposts_out = 0 AND likes_out = 0)                        AS silent
        FROM actor_aggs
        """
    )
    step("actor_class built", t0)

    # ---- 3. cleaned follow edges (drop edges INTO the excluded accounts) ---
    t0 = time.time()
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE edges AS
        SELECT f.src_did_id, f.dst_did_id
        FROM follows f
        WHERE f.dst_did_id NOT IN (SELECT did_id FROM excl)
        """
    )
    n_edges = con.execute("SELECT count(*) FROM edges").fetchone()[0]
    step(f"cleaned edges = {n_edges:,}", t0)

    # ---- 4. per-source out-degree on the cleaned graph --------------------
    t0 = time.time()
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE outdeg AS
        SELECT src_did_id, count(*) AS out_follows
        FROM edges GROUP BY src_did_id
        """
    )
    step("outdeg built", t0)

    # ---- 5. per-target follower-quality aggregation -----------------------
    t0 = time.time()
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE target_q AS
        SELECT
            e.dst_did_id AS did_id,
            count(*)                                            AS followers_clean,
            sum(c.contentless::INT)                             AS booster_followers,
            sum(c.silent::INT)                                  AS silent_followers,
            sum(c.amplifier::INT)                               AS amplifier_followers,
            sum((od.out_follows = 1)::INT)                      AS dedicated_followers,
            sum((c.contentless AND od.out_follows <= 3)::INT)   AS tight_booster_followers
        FROM edges e
        JOIN actor_class c ON c.did_id = e.src_did_id
        JOIN outdeg     od ON od.src_did_id = e.src_did_id
        GROUP BY e.dst_did_id
        """
    )
    step("target_q built", t0)

    # ---- 6. population-level headline numbers -----------------------------
    booster_edges = con.execute(
        "SELECT count(*) FROM edges e JOIN actor_class c ON c.did_id = e.src_did_id "
        "WHERE c.contentless"
    ).fetchone()[0]
    dedicated_edges = con.execute(
        "SELECT count(*) FROM edges e JOIN outdeg od ON od.src_did_id = e.src_did_id "
        "WHERE od.out_follows = 1"
    ).fetchone()[0]
    n_actors = con.execute("SELECT count(*) FROM actor_aggs").fetchone()[0]
    n_boosters = con.execute(
        "SELECT count(*) FROM actor_class c JOIN outdeg od ON od.src_did_id = c.did_id "
        "WHERE c.contentless"
    ).fetchone()[0]

    sidecar: dict = {
        "snapshot_date": snapshot_date,
        "excluded_dids": exclude_dids,
        "n_excluded_resolved": n_excl,
        "min_followers": min_followers,
        "n_actors": n_actors,
        "n_clean_follow_edges": n_edges,
        "booster_edge_share": booster_edges / n_edges if n_edges else 0.0,
        "dedicated_edge_share": dedicated_edges / n_edges if n_edges else 0.0,
        "n_booster_accounts": n_boosters,
    }

    # ---- 7. rankings ------------------------------------------------------
    did_sel = "a.did," if has_actors else ""
    did_join = "LEFT JOIN actors a USING (did_id)" if has_actors else ""

    def ranking(order_col):
        rows = con.execute(
            f"""
            SELECT {did_sel} t.did_id, t.followers_clean,
                   t.booster_followers, t.silent_followers, t.amplifier_followers,
                   t.dedicated_followers,
                   t.booster_followers::DOUBLE / t.followers_clean   AS booster_share,
                   t.dedicated_followers::DOUBLE / t.followers_clean AS dedicated_share
            FROM target_q t {did_join}
            WHERE t.followers_clean >= {min_followers}
            ORDER BY {order_col} DESC, t.followers_clean DESC
            LIMIT {top_n}
            """
        )
        cols = [d[0] for d in rows.description]
        return [dict(zip(cols, r)) for r in rows.fetchall()]

    top_by_booster = ranking("booster_share")
    top_by_dedicated = ranking("dedicated_share")
    sidecar["top_by_booster_share"] = _jsonify(top_by_booster)
    sidecar["top_by_dedicated_share"] = _jsonify(top_by_dedicated)

    # distribution of booster_share across eligible targets (for the histogram)
    dist = con.execute(
        f"""
        SELECT booster_followers::DOUBLE / followers_clean AS s
        FROM target_q WHERE followers_clean >= {min_followers}
        """
    ).fetchall()
    booster_shares = [r[0] for r in dist]
    sidecar["n_targets_ranked"] = len(booster_shares)

    # ---- 8. optional: repost amplification (reposts of T's posts) ---------
    top_by_repost = []
    if has_reposts:
        t0 = time.time()
        try:
            rows = con.execute(
                f"""
                WITH ra AS (
                    SELECT p.author_did_id AS did_id,
                           count(*) AS reposts_total,
                           sum(c.contentless::INT) AS booster_reposts
                    FROM reposts r
                    JOIN posts p ON p.uri_id = r.subject_uri_id
                    JOIN actor_class c ON c.did_id = r.actor_did_id
                    WHERE p.author_did_id IS NOT NULL
                    GROUP BY p.author_did_id
                )
                SELECT {did_sel} ra.did_id, ra.reposts_total, ra.booster_reposts,
                       ra.booster_reposts::DOUBLE / ra.reposts_total AS booster_repost_share
                FROM ra {did_join.replace('USING (did_id)', 'ON a.did_id = ra.did_id')}
                WHERE ra.reposts_total >= {min_followers}
                ORDER BY booster_repost_share DESC, ra.reposts_total DESC
                LIMIT {top_n}
                """
            )
            cols = [d[0] for d in rows.description]
            top_by_repost = [dict(zip(cols, r)) for r in rows.fetchall()]
            step("repost amplification built", t0)
        except Exception as e:  # windowed/absent reposts shouldn't kill the run
            if log:
                print(f"  repost amplification skipped: {e}", flush=True)
    sidecar["top_by_repost_share"] = _jsonify(top_by_repost)

    html = _render_html(snapshot_date, sidecar, booster_shares,
                        top_by_booster, top_by_dedicated, top_by_repost)
    return html.encode("utf-8"), sidecar


def _jsonify(rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        out.append({k: (str(v) if isinstance(v, int) and abs(v) > 2**53 else v)
                    for k, v in r.items()})
    return out


# ----------------------------------------------------------------------------
# rendering
# ----------------------------------------------------------------------------

def _render_html(snapshot_date, sc, booster_shares,
                 top_booster, top_dedicated, top_repost) -> str:
    install_template()
    import plotly.graph_objects as go

    pct = lambda x: f"{100*x:.1f}%"

    # histogram of booster_share across ranked targets
    hist = go.Figure(go.Histogram(x=[100 * s for s in booster_shares], nbinsx=50,
                                   marker_color="#0085ff"))
    hist.update_layout(
        template="bsky", height=340,
        xaxis_title="% of an account's followers that are content-less boosters",
        yaxis_title="accounts", bargap=0.02,
        title=f"Booster-follower share across {sc['n_targets_ranked']:,} "
              f"accounts with ≥{sc['min_followers']} followers",
    )

    def table(rows, share_key, share_label, extra=None):
        if not rows:
            return "<p class='muted'>(none)</p>"
        head = ["account", "followers", share_label, "dedicated%", "silent", "amplifier"]
        trs = []
        for r in rows[:30]:
            who = r.get("did") or str(r.get("did_id"))
            link = (f"<a href='https://bsky.app/profile/{who}' target='_blank'>{who}</a>"
                    if r.get("did") else who)
            trs.append(
                "<tr>"
                f"<td class='mono'>{link}</td>"
                f"<td>{fmt_int(r['followers_clean'])}</td>"
                f"<td><b>{pct(r[share_key])}</b></td>"
                f"<td>{pct(r['dedicated_share'])}</td>"
                f"<td>{fmt_int(r['silent_followers'])}</td>"
                f"<td>{fmt_int(r['amplifier_followers'])}</td>"
                "</tr>"
            )
        return ("<table class='t'><thead><tr>"
                + "".join(f"<th>{h}</th>" for h in head)
                + "</tr></thead><tbody>" + "".join(trs) + "</tbody></table>")

    repost_table = ""
    if top_repost:
        trs = []
        for r in top_repost[:30]:
            who = r.get("did") or str(r.get("did_id"))
            link = (f"<a href='https://bsky.app/profile/{who}' target='_blank'>{who}</a>"
                    if r.get("did") else who)
            trs.append(
                f"<tr><td class='mono'>{link}</td>"
                f"<td>{fmt_int(r['reposts_total'])}</td>"
                f"<td><b>{pct(r['booster_repost_share'])}</b></td>"
                f"<td>{fmt_int(r['booster_reposts'])}</td></tr>"
            )
        repost_table = (
            "<section><h2>Repost amplification</h2>"
            "<p>Accounts whose reposts come disproportionately from content-less "
            "booster accounts (windowed activity).</p>"
            "<table class='t'><thead><tr><th>account</th><th>reposts</th>"
            "<th>booster%</th><th>booster reposts</th></tr></thead><tbody>"
            + "".join(trs) + "</tbody></table></section>"
        )

    excl = ", ".join(sc["excluded_dids"]) or "(none)"
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>Follow boosters — {snapshot_date}</title>
<script>{plotlyjs_inline()}</script>
<style>{SHARED_CSS}
.t{{border-collapse:collapse;width:100%;font-size:13px;margin-top:10px}}
.t th,.t td{{border-bottom:1px solid var(--rule);padding:5px 8px;text-align:right}}
.t th:first-child,.t td:first-child{{text-align:left}}
.t .mono,.mono{{font-family:ui-monospace,Menlo,monospace;font-size:11.5px}}
.muted{{color:var(--muted)}}
</style></head><body><div class="wrap">
<div class="eyebrow">snapshot {snapshot_date} · built {built_at_utc()}</div>
<h1>Follow <span class="accent">boosters</span></h1>
<p class="lede">How much of the follow graph is content-less "booster" accounts —
and which targets are propped up by followers that do nothing but follow them.
Follows of official Bluesky accounts ({sc['n_excluded_resolved']} resolved) are
removed.</p>
<div class="stats">
  <div class="stat"><div class="v">{pct(sc['booster_edge_share'])}</div>
    <div class="l">of all follow-edges come from content-less accounts</div></div>
  <div class="stat"><div class="v bad">{pct(sc['dedicated_edge_share'])}</div>
    <div class="l">of all follow-edges come from accounts that follow exactly 1 target</div></div>
  <div class="stat"><div class="v">{fmt_int(sc['n_booster_accounts'])}</div>
    <div class="l">booster accounts (no content, follow ≥1)</div></div>
  <div class="stat"><div class="v">{fmt_int(sc['n_clean_follow_edges'])}</div>
    <div class="l">follow-edges analysed (bsky removed)</div></div>
</div>
<section><div class="figure">{fig_html(hist, "hist")}</div></section>
<section><h2>Most booster-propped accounts</h2>
<p>Of accounts with ≥{sc['min_followers']} followers, those whose audience is most
dominated by content-less boosters. <b>dedicated%</b> = followers who (after
removing bsky) follow <i>only</i> this account.</p>
{table(top_booster, "booster_share", "booster%")}</section>
<section><h2>Most "dedicated-follower" accounts</h2>
<p>Ranked by the share of followers who follow essentially nothing else —
the strongest "made to follow this account" signal.</p>
{table(top_dedicated, "dedicated_share", "dedicated%")}</section>
{repost_table}
<footer>excluded accounts: <code>{excl}</code></footer>
</div></body></html>"""
