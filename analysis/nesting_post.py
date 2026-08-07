"""Russian nesting posts: quote towers where every quote out-likes what it quotes.

A *tower* is a chain of posts p1 <- p2 <- ... <- pn where each p(k+1) quotes
p(k), and:

  * likes(p(k+1)) > likes(p(k)) for every step — the doll grows outward, and
  * p1 is a genuine ORIGINAL post: a post we have the record for, whose own
    `quote_uri_id` is NULL. A chain that bottoms out on another quote post is
    not a tower, it is the middle of one.

Because likes strictly increase along every kept edge the subgraph is acyclic,
and "ascending likes" is a valid topological order — so one linear pass answers
both the tallest tower (longest path) and the heaviest (largest total likes,
per height).

Each post carries at most one `quote_uri_id`, so the quote graph is a functional
graph: every node has ≤1 parent. That makes the unconstrained question — the
deepest quote stack on the site, ignoring likes entirely — a memoised pointer
walk rather than a topological sort.

Reads `posts` + `post_aggs`. Post text is not in the snapshot, so the renderer
hydrates text/handles from the public Bluesky appview (skippable, offline-safe).

Public entrypoint: `run(con, snapshot_date)`.
Also runnable directly:

    .venv/bin/python -m analysis.nesting_post \
        --db analysis/snapshot/snapshot_2026-07-31.duckdb --out nesting_post.html
"""

from __future__ import annotations

import html as _html
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import numpy as np

APPVIEW = "https://public.api.bsky.app/xrpc/app.bsky.feed.getPosts"
MAXH = 12          # taller than the increasing-likes DAG can produce
UA = "at-snapshot-analysis/1.0 (+https://github.com/pboueri/atproto-db-snapshot)"

# ---------------------------------------------------------------- ordinal ramp
# Likes along a tower are an ORDERED magnitude, not identities, so the encoding
# is a single-hue ordinal ramp (dataviz skill: sequential/ordinal, never
# categorical). Three steps, not one-per-post: the blue ramp's legal ordinal
# band (light end no lighter than step 250, dark end no darker than step 600)
# only spans ~0.43 in OKLab L, so seven distinct steps would sit 0.047 apart and
# fail the adjacent-lightness gate. Three tiers clear it with room (>=0.06), and
# nothing is lost — band *width* already carries the exact like count, with the
# number direct-labelled beside it. Color's job here is just the arc:
# origin -> relay -> peak.
RAMP_LIGHT = ["#86b6ef", "#256abf", "#0d366b"]
RAMP_DARK = ["#184f95", "#5598e7", "#cde2fb"]
TIER_NAMES = ("the original", "relayed", "the peak")


def _tier(rank: int, n: int) -> int:
    """0 = the original post, 2 = the outermost quote, 1 = everything between."""
    if rank == 1:
        return 0
    if rank == n:
        return 2
    return 1


# --------------------------------------------------------------------- edges
# Every quote edge, with the like count on both ends. The increasing-likes
# filter is applied later in numpy so the same scan also feeds the
# deepest-stack question, which ignores likes.
EDGE_SQL = """
SELECT p.uri_id              AS child,
       p.quote_uri_id        AS parent,
       COALESCE(ca.likes, 0) AS clikes,
       COALESCE(pa.likes, 0) AS plikes
FROM posts p
LEFT JOIN post_aggs ca ON ca.uri_id = p.uri_id
LEFT JOIN post_aggs pa ON pa.uri_id = p.quote_uri_id
WHERE p.quote_uri_id IS NOT NULL
  AND p.quote_uri_id <> p.uri_id
"""


def _to_arrow(res):
    """An arrow Table from a duckdb result, across API versions.

    `.arrow()` hands back a RecordBatchReader on some builds, which has no
    `.num_rows` / `.column()`; the explicit table accessors do not.
    """
    for name in ("to_arrow_table", "fetch_arrow_table"):
        fn = getattr(res, name, None)
        if fn is not None:
            return fn()
    out = res.arrow()
    return out.read_all() if hasattr(out, "read_all") else out


def _edges(con, cache: str | None, *, log: bool):
    """All quote edges as an arrow table, optionally parquet-cached.

    The scan is the expensive part (~4 min over 271M posts), so the cache makes
    every later experiment on the same snapshot instant.
    """
    import duckdb

    if cache and os.path.exists(cache):
        if log:
            print(f"  (cache) {cache}", flush=True)
        return duckdb.sql(f"SELECT * FROM '{cache}'").to_arrow_table()
    t0 = time.time()
    if cache:
        os.makedirs(os.path.dirname(cache) or ".", exist_ok=True)
        con.execute(f"COPY ({EDGE_SQL}) TO '{cache}' (FORMAT PARQUET)")
        tbl = duckdb.sql(f"SELECT * FROM '{cache}'").to_arrow_table()
    else:
        tbl = _to_arrow(con.execute(EDGE_SQL))
    if log:
        print(f"  ({time.time()-t0:5.1f}s) {tbl.num_rows:,} quote edges", flush=True)
    return tbl


def _root_ok(con, node_ids, *, log: bool):
    """Which of `node_ids` are genuine ORIGINAL posts.

    A root must be a post we actually hold the record for (`source='record'`)
    whose own `quote_uri_id` is NULL. `target_only` rows are excluded on
    purpose: we saw them only as somebody else's target, so their quote status
    is unknown and we must not assume "no quote" from a NULL we never observed.
    """
    import pyarrow as pa

    t0 = time.time()
    # Register the candidates as an arrow table rather than INSERTing millions
    # of rows one at a time — this is a join, not a load.
    cand = pa.table({"uri_id": pa.array(np.asarray(node_ids, dtype="uint64"))})
    con.register("_np_cand", cand)
    out = con.execute("""
        SELECT p.uri_id FROM _np_cand c JOIN posts p USING (uri_id)
        WHERE p.quote_uri_id IS NULL AND p.source = 'record'
    """).fetchnumpy()["uri_id"]
    con.unregister("_np_cand")
    if log:
        print(f"  ({time.time()-t0:5.1f}s) {len(out):,} of {len(node_ids):,} "
              f"candidates are true original posts", flush=True)
    return np.asarray(out, dtype="uint64")


# ------------------------------------------------------------------ the DAGs
def _index(child, parent):
    nodes, inv = np.unique(np.concatenate([child, parent]), return_inverse=True)
    return nodes, inv[: len(child)], inv[len(child):]


def _deepest_stack(nodes, ci, pi, *, log: bool):
    """Longest quote-of-a-quote chain, ignoring likes entirely.

    Each post quotes at most one post, so the graph is functional: one parent
    pointer per node. Depth is a memoised walk with an explicit path stack,
    which also catches the (illegal but possible) cycle.
    """
    n = len(nodes)
    par = np.full(n, -1, dtype=np.int64)
    par[ci] = pi                              # child -> the post it quotes

    t0 = time.time()
    depth = np.zeros(n, dtype=np.int32)       # 0 == not yet computed
    state = np.zeros(n, dtype=np.int8)        # 1 == on the current path
    cycles = 0
    for start in range(n):
        if depth[start]:
            continue
        path = []
        v = start
        while v != -1 and depth[v] == 0 and state[v] == 0:
            state[v] = 1
            path.append(v)
            v = par[v]
        if v != -1 and state[v] == 1:         # walked into a cycle
            cycles += 1
            base = 0                          # refuse to count cyclic depth
        else:
            base = depth[v] if v != -1 else 0
        for u in reversed(path):
            base += 1
            depth[u] = base
            state[u] = 0
    if log:
        print(f"  ({time.time()-t0:5.1f}s) deepest quote stack = {int(depth.max())}"
              f"{f', {cycles} cyclic starts skipped' if cycles else ''}", flush=True)

    v = int(np.argmax(depth))
    chain = []
    while v != -1:
        chain.append(int(nodes[v]))
        v = int(par[v])
    return int(depth.max()), chain, cycles     # chain: top -> bottom


def _solve(nodes, ci, pi, likes, root_mask, *, log: bool):
    """Both increasing-likes DPs in one ascending-likes pass, root-anchored.

    A chain may only *start* at a true original post, so a non-root node has no
    height-1 base case: it can extend a tower but never found one.
    """
    n = len(nodes)
    NEG = -1
    H = np.where(root_mask, 1, 0).astype(np.int32)   # 0 == cannot found a tower
    hprev = np.full(n, -1, dtype=np.int64)
    S = np.full((MAXH + 1, n), NEG, dtype=np.int64)
    sprev = np.full((MAXH + 1, n), -1, dtype=np.int64)
    S[1] = np.where(root_mask, likes, NEG)

    inc = likes[ci] > likes[pi]                      # the strictly-increasing edges
    ci, pi = ci[inc], pi[inc]
    order = np.argsort(likes[ci], kind="stable")     # ascending == topological

    t0 = time.time()
    for e in order:
        c, p = ci[e], pi[e]
        if H[p] and H[p] + 1 > H[c]:
            H[c], hprev[c] = H[p] + 1, p
        lc = likes[c]
        for h in range(2, MAXH + 1):
            sp = S[h - 1][p]
            if sp == NEG:
                # `continue`, not `break`: the achievable heights at p are not
                # contiguous. A non-root p has no height-1 tower (S[1] == NEG)
                # yet can still have a valid height-2 one, so breaking here
                # would silently erase every height >= 3 from the results.
                continue
            if sp + lc > S[h][c]:
                S[h][c], sprev[h][c] = sp + lc, p
    if log:
        print(f"  ({time.time()-t0:5.1f}s) DP over {len(order):,} increasing "
              f"edges, {n:,} posts", flush=True)
    return H, hprev, S, sprev


def _chain_tall(nodes, hprev, v):
    out = []
    while v != -1:
        out.append(int(nodes[v]))
        v = int(hprev[v])
    return out                                  # top (most likes) -> bottom


def _chain_total(nodes, sprev, v, h):
    out = []
    while v != -1:
        out.append(int(nodes[v]))
        v, h = int(sprev[h][v]), h - 1
    return out


# ------------------------------------------------------------------- metadata
def _meta(con, uri_ids):
    con.execute("CREATE OR REPLACE TEMPORARY TABLE _np_ids(uri_id UBIGINT)")
    con.executemany("INSERT INTO _np_ids VALUES (?)", [(int(u),) for u in uri_ids])
    rows = con.execute("""
        SELECT p.uri_id, a.did, p.rkey, p.created_at,
               COALESCE(g.likes,0), COALESCE(g.reposts,0),
               COALESCE(g.replies,0), COALESCE(g.quotes,0),
               p.author_did_id, p.source, p.quote_uri_id
        FROM _np_ids i JOIN posts p USING (uri_id)
        LEFT JOIN actors a ON a.did_id = p.author_did_id
        LEFT JOIN post_aggs g ON g.uri_id = p.uri_id
    """).fetchall()
    return {r[0]: dict(uri_id=r[0], did=r[1], rkey=r[2], created_at=str(r[3]),
                       likes=r[4], reposts=r[5], replies=r[6], quotes=r[7],
                       author_did_id=r[8], source=r[9],
                       is_quote=r[10] is not None,
                       uri=f"at://{r[1]}/app.bsky.feed.post/{r[2]}") for r in rows}


def _hydrate(posts, *, log: bool):
    """Attach handle/text from the public appview. Best-effort; never fatal."""
    uris = sorted({p["uri"] for p in posts if p.get("uri")})
    got = {}
    for i in range(0, len(uris), 25):
        batch = uris[i:i + 25]
        url = APPVIEW + "?" + urllib.parse.urlencode([("uris", u) for u in batch])
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            for p in json.load(urllib.request.urlopen(req, timeout=30)).get("posts", []):
                got[p["uri"]] = p
        except Exception as exc:                # offline / rate-limited / deleted
            if log:
                print(f"  (appview) batch {i//25} failed: {exc}", flush=True)
        time.sleep(0.3)
    if log:
        print(f"  (appview) hydrated {len(got)}/{len(uris)} posts", flush=True)
    for p in posts:
        h = got.get(p.get("uri"), {})
        a = h.get("author") or {}
        p["handle"] = a.get("handle")
        p["display_name"] = a.get("displayName")
        p["text"] = (h.get("record") or {}).get("text")
        p["live_likes"] = h.get("likeCount")
        p["bsky_url"] = (f"https://bsky.app/profile/{a.get('handle') or p.get('did')}"
                         f"/post/{p.get('rkey')}") if p.get("rkey") else None
    return posts


def _tower(meta, chain, *, strict=True):
    """Build a tower record from a top->bottom uri_id chain. Bottom-up order.

    `strict` drops the tower if any post is missing from `posts` (used for the
    funnels, where a hole would break the encoding). The deepest-stack view
    passes strict=False: there the chain *length* is the finding, so an
    unresolvable link is shown as a gap rather than silently shortening it.
    """
    posts = []
    for u in chain:
        m = meta.get(u)
        if m is None:
            if strict:
                return None
            posts.append(dict(uri_id=u, did=None, rkey=None, created_at=None,
                              likes=0, reposts=0, replies=0, quotes=0,
                              author_did_id=None, source=None, is_quote=None,
                              uri=None, missing=True))
        else:
            posts.append(dict(m, missing=False))
    posts.reverse()                             # original first
    ts = [datetime.fromisoformat(p["created_at"]) for p in posts if p["created_at"]]
    prev_t = None
    for i, p in enumerate(posts):
        p["rank"] = i + 1
        t = datetime.fromisoformat(p["created_at"]) if p["created_at"] else None
        p["gap_min"] = (round((t - prev_t).total_seconds() / 60)
                        if t and prev_t else None)
        prev_t = t or prev_t
    return dict(height=len(posts),
                total_likes=sum(p["likes"] for p in posts),
                authors=len({p["author_did_id"] for p in posts
                             if p["author_did_id"] is not None}),
                span_min=(round((ts[-1] - ts[0]).total_seconds() / 60)
                          if len(ts) > 1 else 0),
                posts=posts)


def _sample_stack(stack, *, head=3, tail=3):
    """Keep a stack's full statistics but only its two ends for rendering.

    The deepest stack runs to thousands of posts; rendering a card each would
    be a multi-megabyte page nobody scrolls. Stats stay computed over the whole
    chain — only the display is sampled, and the elided count is shown.
    """
    posts = stack["posts"]
    by_author = {}
    for p in posts:                              # keyed by did — handles are
        k = p.get("did") or "?"                  # only known after hydration
        by_author[k] = by_author.get(k, 0) + 1
    out = dict(stack)
    out["max_likes"] = max(p["likes"] for p in posts) or 1
    out["switches"] = sum(1 for a, b in zip(posts, posts[1:])
                          if a["author_did_id"] != b["author_did_id"])
    out["steps"] = len(posts) - 1
    out["zero_likes"] = sum(1 for p in posts if p["likes"] == 0)
    out["author_counts"] = sorted(by_author.items(), key=lambda kv: -kv[1])
    if len(posts) > head + tail:
        out["posts"] = (posts[:head]
                        + [dict(gapmark=True, omitted=len(posts) - head - tail)]
                        + posts[-tail:])
    return out


def _pick(meta, cands, chain_fn, *, k=25):
    """First candidate whose chain fully resolves against `posts`.

    Without this a single orphan node (a `quote_uri_id` pointing at a row we
    never captured) silently deletes the whole height class from the output —
    which is how the height-7 row went missing in the first draft.
    """
    for v in cands[:k]:
        t = _tower(meta, chain_fn(int(v)), strict=True)
        if t:
            return t
    return None


# ------------------------------------------------------------------ rendering
def _esc(s) -> str:
    return _html.escape(str(s if s is not None else ""))


def _fmt(n) -> str:
    return f"{int(n):,}"


def _dur(mins) -> str:
    if mins is None:
        return "—"
    m = int(mins)
    if m < 60:
        return f"{m}m"
    if m < 60 * 24:
        return f"{m//60}h {m%60:02d}m"
    return f"{m//1440}d {(m%1440)//60}h"


CARD_H = 130                   # px; a band is exactly as tall as its card
ROW_GAP = 2                    # px; surface gap between adjacent fills
ROW_H = CARD_H + ROW_GAP       # px; pitch — must match the card's box + margin
LBL_W = 74                     # px; the direct-label gutter left of the funnel
FUN_W = 226                    # px; funnel drawing width
MIN_W = 8                      # px; a band never disappears entirely


def _funnel_svg(tower, ident: str) -> str:
    """Proportional funnel: band width is strictly linear in like count.

    Top band = the outermost quote (most likes); bottom = the original post.
    No min-width fudging beyond MIN_W, so the silhouette is honest — when two
    posts have similar like counts the funnel legitimately looks flat.
    """
    posts = list(reversed(tower["posts"]))       # most likes first == top
    n = len(posts)
    mx = max(p["likes"] for p in posts) or 1
    cx = LBL_W + FUN_W / 2
    hgt = n * ROW_H - ROW_GAP                    # last row carries no trailing gap

    def w(i):
        return max(MIN_W, FUN_W * posts[i]["likes"] / mx)

    out = [f'<svg class="funnel" width="{LBL_W+FUN_W}" height="{hgt}" '
           f'viewBox="0 0 {LBL_W+FUN_W} {hgt}" role="img" '
           f'aria-label="Funnel of like counts, {n} posts, '
           f'{_fmt(posts[0]["likes"])} likes at the outermost quote down to '
           f'{_fmt(posts[-1]["likes"])} at the original post.">']
    for i, p in enumerate(posts):
        y0, y1 = i * ROW_H, i * ROW_H + CARD_H
        wt = w(i)
        wb = w(i + 1) if i < n - 1 else wt        # taper toward the next band
        pts = (f"{cx-wt/2:.1f},{y0:.1f} {cx+wt/2:.1f},{y0:.1f} "
               f"{cx+wb/2:.1f},{y1:.1f} {cx-wb/2:.1f},{y1:.1f}")
        tier = _tier(p["rank"], n)
        out.append(
            f'<polygon class="band" points="{pts}" fill="var(--step-{tier})" '
            f'data-tower="{ident}" data-i="{p["rank"]}" tabindex="0">'
            f'<title>#{p["rank"]} ({TIER_NAMES[tier]}) — {_fmt(p["likes"])} likes'
            f'</title></polygon>')
        out.append(
            f'<text class="blabel" x="{LBL_W-12}" y="{(y0+y1)/2:.1f}" '
            f'text-anchor="end" dominant-baseline="central">{_fmt(p["likes"])}</text>')
    out.append("</svg>")
    return "".join(out)


def _card(p, n, ident, mx, *, fixed=True):
    if p.get("gapmark"):
        return (f'<li class="card gapmark"><span>⋮</span>'
                f'<span>{_fmt(p["omitted"])} more posts in the chain, '
                f'not shown</span><span>⋮</span></li>')
    if p.get("missing"):
        return (f'<li class="card missing"{"" if fixed else " style=height:auto"}>'
                f'<div class="chead"><span class="rank">#{p["rank"]}</span>'
                f'<span class="badge">link not in snapshot</span></div>'
                f'<p class="ctext">This post is referenced as a quote target but '
                f'its own row was never captured, so the chain continues through '
                f'a gap.</p></li>')
    who = p["handle"] or p["did"]
    txt = p["text"] or "— post text unavailable (deleted, or not served by the appview) —"
    badge = ("<span class='badge origin'>the original</span>" if p["rank"] == 1
             else f"<span class='badge'>quotes #{p['rank']-1}</span>")
    gap = (f"<span class='gap'>+{_dur(p['gap_min'])} later</span>"
           if p["gap_min"] is not None else "")
    pct = 100 * p["likes"] / mx if mx else 0
    return f"""
<li class="card" data-tower="{ident}" data-i="{p['rank']}">
  <div class="chead">
    <span class="rank">#{p['rank']}</span>
    <a class="who" href="{_esc(p['bsky_url'])}" target="_blank" rel="noopener">@{_esc(who)}</a>
    {badge}{gap}
  </div>
  <p class="ctext">{_esc(txt)}</p>
  <div class="cfoot">
    <span class="likes">{_fmt(p['likes'])} likes</span>
    <span class="sub">{_fmt(p['reposts'])} reposts · {_fmt(p['replies'])} replies
      · {_fmt(p['quotes'])} quotes</span>
  </div>
  <div class="minibar" aria-hidden="true"><i style="width:{pct:.1f}%"></i></div>
</li>"""


def _cards_html(tower, ident: str, *, fixed=True) -> str:
    posts = list(reversed(tower["posts"]))       # match the funnel, top-down
    n = len(posts)
    # For a sampled stack the bar scale must stay the WHOLE chain's max, or the
    # six posts we happen to show would silently rescale to look important.
    mx = tower.get("max_likes") or max(p["likes"] for p in posts) or 1
    body = "".join(_card(p, n, ident, mx, fixed=fixed) for p in posts)
    # --card-h / --row-gap come from the same constants the SVG is drawn with,
    # so a band always lines up with its card.
    cls = "cards" if fixed else "cards loose"
    return (f'<ol class="{cls}" style="--card-h:{CARD_H}px;--row-gap:{ROW_GAP}px">'
            + body + "</ol>")


def _table_html(tower) -> str:
    rows = "".join(
        (f"<tr><td class='num'>⋮</td><td colspan='7'>{_fmt(p['omitted'])} more "
         f"posts elided</td></tr>") if p.get("gapmark") else
        f"<tr><td class='num'>{p['rank']}</td>"
        f"<td>{'—' if p.get('missing') else '@' + _esc(p['handle'] or p['did'])}</td>"
        f"<td class='num'>{_fmt(p['likes'])}</td><td class='num'>{_fmt(p['reposts'])}</td>"
        f"<td class='num'>{_fmt(p['replies'])}</td><td class='num'>{_fmt(p['quotes'])}</td>"
        f"<td>{_esc((p['created_at'] or '')[:16])}</td>"
        f"<td>{'' if p.get('missing') else f'''<a href="{_esc(p['bsky_url'])}" target="_blank" rel="noopener">open</a>'''}</td></tr>"
        for p in tower["posts"])
    return f"""<details class="tableview"><summary>Table view</summary>
<div class="scroll"><table><thead><tr><th class="num">#</th><th>author</th>
<th class="num">likes</th><th class="num">reposts</th><th class="num">replies</th>
<th class="num">quotes</th><th>posted (UTC)</th><th></th></tr></thead>
<tbody>{rows}</tbody></table></div></details>"""


def _ramp_style(ident: str) -> str:
    lvars = ";".join(f"--step-{i}:{c}" for i, c in enumerate(RAMP_LIGHT))
    dvars = ";".join(f"--step-{i}:{c}" for i, c in enumerate(RAMP_DARK))
    return (f"<style>#{ident}{{{lvars}}}"
            f'@media (prefers-color-scheme:dark){{:root:where(:not([data-theme="light"]))'
            f" #{ident}{{{dvars}}}}}"
            f':root[data-theme="dark"] #{ident}{{{dvars}}}</style>')


def _stats(items) -> str:
    return ('<div class="stats">' + "".join(
        f'<div class="stat"><div class="v">{v}</div><div class="l">{l}</div></div>'
        for v, l in items) + "</div>")


def _tower_block(tower, ident, *, eyebrow, title, lede) -> str:
    return f"""
{_ramp_style(ident)}
<section class="tower-sec" id="{ident}">
  <div class="kicker">{_esc(eyebrow)}</div>
  <h2>{_esc(title)}</h2>
  <p class="lede2">{lede}</p>
  {_stats([(tower["height"], "posts deep"),
           (_fmt(tower["total_likes"]), "likes across the tower"),
           (tower["authors"], "distinct authors"),
           (_dur(tower["span_min"]), "original to outermost")])}
  <figure class="tower">
    {_funnel_svg(tower, ident)}
    {_cards_html(tower, ident)}
  </figure>
  <figcaption class="cap">Band width is linear in like count; the widest band is
    the outermost quote. Reading down the funnel is reading <em>inward</em> —
    each post is nested inside the one above it, and the bottom band is a real
    original post, not another quote.</figcaption>
  {_table_html(tower)}
</section>"""


def _stack_block(tower, cycles, ident="deepest") -> str:
    """The deepest quote stack ignoring likes.

    Deliberately NOT a funnel: likes are not monotone here, so a funnel
    silhouette would imply an ordering the data does not have. Per-card bars on
    a shared scale, and the depth itself is the hero number.
    """
    mx = tower["max_likes"]
    alt = (tower["switches"] == tower["steps"] and tower["authors"] == 2)
    who = ", ".join(f"@{_esc(h)} ({_fmt(c)})" for h, c in tower["author_counts"][:3])
    return f"""
<section class="tower-sec" id="{ident}">
  <div class="kicker">ignoring likes entirely</div>
  <h2>The deepest quote stack on the site is {_fmt(tower['height'])} posts</h2>
  <p class="lede2">Drop the like rule and just ask how deep quote-of-a-quote
    goes. The answer is not a viral cascade at all — it is
    <strong>two people writing a story to each other</strong>. Every post quotes
    the one before it, {"strictly alternating between the two accounts on "
    f"all {_fmt(tower['steps'])} handoffs" if alt else
    f"across {tower['authors']} accounts"}, for
    {_dur(tower['span_min'])} straight. Engagement is beside the point: the most
    liked post in the entire chain got <strong>{_fmt(mx)} likes</strong>, and
    {_fmt(tower['zero_likes'])} of the {_fmt(tower['height'])} got none at all.
    They are using quote posts as a threading mechanism, and it builds by far
    the deepest structure in the snapshot.</p>
  {_stats([(_fmt(tower["height"]), "posts deep"),
           (tower["authors"], "authors, taking turns"),
           (_fmt(tower["total_likes"]), "likes in the whole chain"),
           (_dur(tower["span_min"]), "first to last")])}
  <p class="lede2 who">{who}</p>
  <figure class="tower stackfig">{_cards_html(tower, ident, fixed=False)}</figure>
  <figcaption class="cap">The two ends of the chain; the middle is elided. Bars
    are like counts against the whole chain's maximum ({_fmt(mx)}). Unlike the
    towers above this chain is <em>not</em> monotone in likes — it is simply the
    deepest.{f' {cycles:,} chains were skipped for containing a quote cycle.'
              if cycles else ''}
  </figcaption>
  {_table_html(tower)}
</section>"""


def _barrow(cells, value, pct) -> str:
    return ("<tr>" + "".join(f"<td class='{c}'>{v}</td>" for c, v in cells) +
            f"<td class='barcell'><i style='width:{pct:.1f}%'></i></td>"
            f"<td class='num strong'>{value}</td></tr>")


def _scale(values):
    """Bar widths as percentages, plus a label for the scale used.

    Log only when the spread actually needs it (>100x). Log-scaling a set that
    spans 7x squashes every bar to near-identical length and quietly hides the
    very differences the chart exists to show.
    """
    vals = list(values)
    lo, hi = min(vals), max(vals)
    if lo > 0 and hi / lo > 100:
        return ([100 * np.log10(v + 1) / np.log10(hi + 1) for v in vals],
                ' <span class="mut">(log-scaled)</span>')
    return [100 * v / hi for v in vals], ""


def _ladder_html(ladder) -> str:
    """Best-total tower at each height — the depth/reach tradeoff. The exact
    number sits in its own column so it can never wrap under the bar."""
    pcts, note = _scale([r["total_likes"] for r in ladder])
    rows = []
    for r, pct in zip(ladder, pcts):
        top = r["posts"][-1]
        chain = " → ".join(_fmt(p["likes"]) for p in r["posts"])
        rows.append(_barrow(
            [("num", r["height"]), ("num", r["authors"]),
             ("chain", chain),
             ("", f"<a href='{_esc(top['bsky_url'])}' target='_blank' "
                  f"rel='noopener'>@{_esc(top['handle'] or top['did'])}</a>")],
            _fmt(r["total_likes"]), pct))
    return f"""<table class="ladder"><thead><tr>
<th class="num">height</th><th class="num">authors</th>
<th>likes, original → outermost</th><th>top post</th>
<th>total likes{note}</th><th class="num">total</th>
</tr></thead><tbody>{''.join(rows)}</tbody></table>"""


def _hist_html(hist) -> str:
    vals = {h: v for h, v in hist.items() if h >= 2}     # a 1-post "tower" is not one
    ks = sorted(vals)
    pcts, note = _scale([vals[h] for h in ks])
    rows = [_barrow([("num", h)], _fmt(vals[h]), pct) for h, pct in zip(ks, pcts)]
    return f"""<table class="ladder"><thead><tr><th class="num">height</th>
<th>towers this tall{note}</th>
<th class="num">count</th></tr></thead><tbody>{''.join(rows)}</tbody></table>"""


CSS = """
:root{color-scheme:light dark}
*{box-sizing:border-box}
html,body{margin:0;padding:0}
body{
  background:var(--plane);color:var(--ink);
  font-family:system-ui,-apple-system,"Segoe UI",sans-serif;
  line-height:1.55;font-size:16px;
  --plane:#f9f9f7; --surface:#fcfcfb; --ink:#0b0b0b; --ink2:#52514e;
  --mut:#898781; --rule:#e1e0d9; --ring:rgba(11,11,11,.10); --accent:#2a78d6;
}
@media (prefers-color-scheme:dark){:root:where(:not([data-theme="light"])) body{
  --plane:#0d0d0d; --surface:#1a1a19; --ink:#fff; --ink2:#c3c2b7;
  --mut:#898781; --rule:#2c2c2a; --ring:rgba(255,255,255,.10); --accent:#3987e5;}}
:root[data-theme="dark"] body{
  --plane:#0d0d0d; --surface:#1a1a19; --ink:#fff; --ink2:#c3c2b7;
  --mut:#898781; --rule:#2c2c2a; --ring:rgba(255,255,255,.10); --accent:#3987e5;}

.wrap{max-width:1000px;margin:0 auto;padding:56px 24px 96px}
.eyebrow{font-size:12px;letter-spacing:.12em;text-transform:uppercase;
  color:var(--mut);margin-bottom:14px}
h1{font-size:46px;line-height:1.06;letter-spacing:-.025em;margin:0 0 18px;font-weight:700}
h1 em{font-style:normal;color:var(--accent)}
.lede{font-size:19px;color:var(--ink2);margin:0 0 8px;max-width:760px}
.lede strong{color:var(--ink)}
.tower-sec{margin:76px 0 0;padding-top:44px;border-top:1px solid var(--rule)}
.kicker{font-size:12.5px;font-weight:650;color:var(--accent);
  text-transform:uppercase;letter-spacing:.09em;margin-bottom:8px}
h2{font-size:29px;letter-spacing:-.015em;margin:0 0 10px;font-weight:700}
.lede2{color:var(--ink2);margin:0 0 26px;max-width:760px}
.lede2 strong{color:var(--ink)}
p.note{color:var(--ink2);max-width:760px}
p.note strong{color:var(--ink)}
a{color:var(--accent)}
code{font-size:13px;background:var(--surface);border:1px solid var(--ring);
  border-radius:4px;padding:1px 5px}

.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:0 0 30px}
.stat{background:var(--surface);border:1px solid var(--ring);border-radius:10px;
  padding:15px 16px}
.stat .v{font-size:29px;font-weight:700;letter-spacing:-.02em}
.stat .l{font-size:12.5px;color:var(--ink2);margin-top:2px}

.tower{display:grid;grid-template-columns:300px 1fr;gap:0 22px;margin:0;
  align-items:start}
.stackfig{grid-template-columns:1fr;max-width:720px}
.funnel{display:block;overflow:visible}
.band{stroke:var(--surface);stroke-width:2;transition:filter .12s;cursor:default}
.band:hover,.band:focus,.band.on{filter:brightness(1.12);outline:none}
.blabel{font-size:15px;font-weight:650;fill:var(--ink);
  font-variant-numeric:tabular-nums}

.cards{list-style:none;margin:0;padding:0}
.card{height:var(--card-h);padding:11px 16px;background:var(--surface);
  border:1px solid var(--ring);border-radius:10px;margin-bottom:var(--row-gap);
  display:flex;flex-direction:column;gap:4px;overflow:hidden;
  transition:border-color .12s}
.card:last-child{margin-bottom:0}
.cards.loose .card{height:auto;min-height:0;margin-bottom:6px;padding:12px 16px}
.cards.loose .minibar{display:block}
.card.on{border-color:var(--accent)}
.card.missing{border-style:dashed;color:var(--mut)}
.card.gapmark{height:auto;background:none;border:1px dashed var(--rule);
  align-items:center;justify-content:center;gap:2px;padding:14px;
  color:var(--mut);font-size:13px;text-align:center}
.card.gapmark span:first-child,.card.gapmark span:last-child{
  font-size:17px;line-height:.8;letter-spacing:.2em}
p.who{font-size:14px;color:var(--mut);margin:-14px 0 22px;
  font-variant-numeric:tabular-nums}
.chead{display:flex;align-items:baseline;gap:9px;flex-wrap:wrap;font-size:13px}
.rank{font-weight:700;color:var(--mut);font-variant-numeric:tabular-nums}
.who{font-weight:650;text-decoration:none}
.who:hover{text-decoration:underline}
.badge{font-size:11px;letter-spacing:.04em;text-transform:uppercase;
  color:var(--ink2);border:1px solid var(--rule);border-radius:999px;padding:1px 8px}
.badge.origin{border-color:var(--accent);color:var(--accent)}
.gap{font-size:12px;color:var(--mut)}
.ctext{margin:0;font-size:14.5px;color:var(--ink);overflow:hidden;
  display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;flex:1 1 auto}
.card.missing .ctext{color:var(--mut);font-style:italic}
.cfoot{display:flex;align-items:baseline;gap:10px;flex-wrap:wrap;margin-top:auto}
.likes{font-weight:700;font-variant-numeric:tabular-nums}
.sub{font-size:12px;color:var(--mut)}
.minibar{display:none;height:4px;background:var(--rule);border-radius:2px;
  margin-top:6px}
.minibar i{display:block;height:100%;background:var(--accent);border-radius:0 2px 2px 0}
.cap{font-size:13px;color:var(--mut);margin:14px 0 0;max-width:820px}

.tableview{margin:22px 0 0}
.tableview summary{cursor:pointer;color:var(--ink2);font-size:13px}
table{border-collapse:collapse;width:100%;margin-top:12px;font-size:13.5px}
th,td{text-align:left;padding:7px 10px;border-bottom:1px solid var(--rule);
  vertical-align:middle}
th{font-size:12px;text-transform:uppercase;letter-spacing:.06em;color:var(--mut);
  font-weight:650}
td.num,th.num{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}
td.strong{font-weight:700}
.mut{color:var(--mut);text-transform:none;letter-spacing:0;font-weight:400}
.ladder .barcell{width:26%;min-width:120px;padding-right:0}
.ladder td a{white-space:nowrap}
.ladder .barcell i{display:block;height:11px;background:var(--accent);
  border-radius:0 4px 4px 0;opacity:.85}
.ladder .chain{font-variant-numeric:tabular-nums;color:var(--ink2);font-size:12.5px}
.scroll{overflow-x:auto}

.foot{margin-top:72px;padding-top:22px;border-top:1px solid var(--rule);
  font-size:13px;color:var(--mut)}

@media (max-width:820px){
  .tower{grid-template-columns:1fr}
  .funnel{display:none}
  .card{height:auto;min-height:0}
  .minibar{display:block}
  .stats{grid-template-columns:repeat(2,1fr)}
  h1{font-size:34px}
}
"""

JS = """
// Hover layer: a band and its card highlight together, in both directions.
for (const el of document.querySelectorAll('.band,.card')) {
  if (!el.dataset.tower) continue;
  const mates = () => document.querySelectorAll(
      `[data-tower="${el.dataset.tower}"][data-i="${el.dataset.i}"]`);
  const set = on => mates().forEach(m => m.classList.toggle('on', on));
  el.addEventListener('mouseenter', () => set(true));
  el.addEventListener('mouseleave', () => set(false));
  el.addEventListener('focus', () => set(true));
  el.addEventListener('blur', () => set(false));
}
"""


def _authorship(tower) -> str:
    """One clause describing who is in a tower — a self-quoting thread that got
    picked up reads very differently from N strangers relaying each other."""
    n, a = tower["height"], tower["authors"]
    if a == 1:
        return "It is one account re-quoting itself the whole way up."
    counts = {}
    for p in tower["posts"]:
        counts[p["author_did_id"]] = counts.get(p["author_did_id"], 0) + 1
    top = max(counts.values())
    if a == n:
        return f"All {n} posts are by different people."
    if top > 1:
        return (f"{top} of the {n} are one account continuing its own thread, "
                f"and the rest are other people picking it up.")
    return f"{a} accounts across {n} posts."


def render(tallest, heaviest, stack, ladder, hist, meta) -> bytes:
    t_top = tallest["posts"][-1]
    h_top = heaviest["posts"][-1]
    body = f"""
<div class="wrap">
<div class="eyebrow">at-proto snapshot · {_esc(meta['snapshot_date'])}</div>
<h1>The <em>Russian Nesting Post</em></h1>
<p class="lede">A quote post sits inside the post it quotes. Chain those together
and you get a tower — and the interesting towers are the ones where
<strong>every quote out-likes the thing it quotes</strong>, so the doll gets
bigger with every layer. The bottom has to be a real original post, not another
quote. Across {_fmt(meta['edges'])} quote links in this snapshot,
{_fmt(meta['inc_edges'])} of them increasing, here are the extremes: the tower
that goes <strong>deepest</strong>, the one carrying the <strong>most
likes</strong>, and — dropping the like rule entirely — the deepest quote stack
on the site.</p>

{_tower_block(tallest, "tallest", eyebrow="the tallest tower",
  lede=f"{tallest['height']} posts, and the like count goes up at every single "
       f"step — {_fmt(tallest['posts'][0]['likes'])} likes at the original, "
       f"{_fmt(tallest['posts'][-1]['likes'])} at the outermost quote. "
       f"{_authorship(tallest)} This is the deepest strictly-increasing quote "
       f"chain in the snapshot that bottoms out on a genuine original post "
       f"rather than on somebody else's quote — no chain of "
       f"{tallest['height']+1} exists.",
  title=f"{tallest['height']} deep, and it never once goes down")}

{_tower_block(heaviest, "heaviest", eyebrow="the heaviest tower",
  lede=f"Depth and reach pull against each other. The tower carrying the most "
       f"likes is only {heaviest['height']} posts tall — "
       f"{_fmt(heaviest['total_likes'])} likes between them — because at this "
       f"altitude the post you would have to beat is already enormous. Every "
       f"extra layer has to clear a bar the previous layer just raised.",
  title=f"{_fmt(heaviest['total_likes'])} likes, and only "
        f"{heaviest['height']} layers")}

{_stack_block(stack, meta['cycles'])}

<section class="tower-sec">
  <div class="kicker">the tradeoff</div>
  <h2>You can go deep, or you can go big</h2>
  <p class="lede2">The best a tower can do at each height. Roughly every extra
    layer halves the total likes a tower can carry: the ceiling falls from
    {_fmt(ladder[0]['total_likes'])} at {ladder[0]['height']} posts to
    {_fmt(ladder[-1]['total_likes'])} at {ladder[-1]['height']}. The ratchet
    that makes a tower interesting is the same one that starves it.</p>
  <div class="scroll">{_ladder_html(ladder)}</div>
</section>

<section class="tower-sec">
  <div class="kicker">how rare is this</div>
  <h2>Almost every tower is two posts</h2>
  <p class="lede2">Counting each post by the tallest root-anchored tower ending
    at it. Two layers is the overwhelming default.</p>
  <div class="scroll">{_hist_html(hist)}</div>
</section>

<section class="tower-sec">
  <div class="kicker">method</div>
  <h2>How this was computed</h2>
  <p class="note">Every row of <code>posts</code> with a non-null
    <code>quote_uri_id</code> becomes an edge <em>child → the post it quotes</em>,
    joined to <code>post_aggs</code> for the like count on both ends:
    {_fmt(meta['edges'])} edges over {_fmt(meta['nodes'])} posts. Keep only the
    edges where the quoting post has strictly more likes
    ({_fmt(meta['inc_edges'])} of them). Because likes then strictly increase
    along every surviving edge the graph is acyclic, and sorting edges by the
    quoting post's like count is a topological order — so one linear pass gives
    both the longest path and the largest-sum path at each height. A chain may
    only <em>start</em> at a post whose own <code>quote_uri_id</code> is NULL and
    whose <code>source</code> is <code>record</code>: a genuine original we hold
    the body for. The deepest-stack section drops the like rule and walks the
    raw quote graph, which is a forest — each post quotes at most one post — so
    depth is a memoised pointer walk.</p>
  <p class="note"><strong>Caveats.</strong> Like counts are the snapshot's, over
    a {_esc(meta['window'])} activity window — a like cast outside it is not
    counted, so a tower spanning the window edge can be understated.
    <code>target_only</code> posts are never treated as roots: we saw them only
    as somebody else's quote target, so a NULL <code>quote_uri_id</code> there
    means "unobserved", not "not a quote". The snapshot stores structure, not
    content, so post text and handles were hydrated live from the public Bluesky
    appview at render time and may have moved since; like counts shown are the
    snapshot's, not live. Ties form no edge, so equal like counts never extend a
    tower.</p>
</section>

<div class="foot">
  Tallest: <a href="{_esc(t_top['bsky_url'])}" target="_blank" rel="noopener">
  @{_esc(t_top['handle'] or t_top['did'])}</a> ·
  Heaviest: <a href="{_esc(h_top['bsky_url'])}" target="_blank" rel="noopener">
  @{_esc(h_top['handle'] or h_top['did'])}</a><br>
  Built {_esc(meta['built_at'])} from snapshot {_esc(meta['snapshot_date'])}.
</div>
</div>
<script>{JS}</script>"""
    return (f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>The Russian Nesting Post — at-proto snapshot</title>
<style>{CSS}</style></head><body>{body}</body></html>""").encode()


# ------------------------------------------------------------------- pipeline
def run(con, snapshot_date: str, *, log: bool = True,
        edges_cache: str | None = None, hydrate: bool = True,
        window: str = "90-day") -> tuple[bytes, dict]:
    if log:
        print("=== nesting_post: scan quote edges ===", flush=True)
    tbl = _edges(con, edges_cache, log=log)
    child = tbl.column("child").to_numpy(zero_copy_only=False)
    parent = tbl.column("parent").to_numpy(zero_copy_only=False)
    clikes = tbl.column("clikes").to_numpy(zero_copy_only=False)
    plikes = tbl.column("plikes").to_numpy(zero_copy_only=False)
    nodes, ci, pi = _index(child, parent)
    likes = np.zeros(len(nodes), dtype=np.int64)
    likes[ci] = clikes
    likes[pi] = plikes

    if log:
        print("=== nesting_post: deepest raw quote stack ===", flush=True)
    depth, stack_chain, cycles = _deepest_stack(nodes, ci, pi, log=log)

    # Only nodes that are never a child can possibly be originals; asking the DB
    # about the rest is wasted work.
    if log:
        print("=== nesting_post: identify true original posts ===", flush=True)
    is_child = np.zeros(len(nodes), dtype=bool)
    is_child[ci] = True
    cand = nodes[~is_child]
    roots = _root_ok(con, cand, log=log)
    root_mask = np.isin(nodes, roots)

    if log:
        print("=== nesting_post: tallest + heaviest DP ===", flush=True)
    H, hprev, S, sprev = _solve(nodes, ci, pi, likes, root_mask, log=log)

    hist = {int(h): int((H == h).sum()) for h in np.unique(H) if h >= 2}
    hmax = int(H.max())

    # Candidate tops, best-first, so one orphan node can't delete a whole class.
    tall_c = np.where(H == hmax)[0]
    tall_c = tall_c[np.argsort(-likes[tall_c])]
    ladder_c = {h: np.argsort(-S[h])[:25] for h in range(2, MAXH + 1)
                if S[h].max() > 0}

    ids = {u for v in tall_c[:25] for u in _chain_tall(nodes, hprev, int(v))}
    for h, vs in ladder_c.items():
        for v in vs:
            ids.update(_chain_total(nodes, sprev, int(v), h))
    ids.update(stack_chain)
    meta_by_id = _meta(con, sorted(ids))

    tallest = _pick(meta_by_id, tall_c, lambda v: _chain_tall(nodes, hprev, v))
    ladder = []
    for h, vs in sorted(ladder_c.items()):
        t = _pick(meta_by_id, vs, lambda v, h=h: _chain_total(nodes, sprev, v, h))
        if t:
            ladder.append(t)
        elif log:
            print(f"  (warn) no height-{h} tower resolved to full metadata", flush=True)
    heaviest = max(ladder, key=lambda t: t["total_likes"])
    # Full stats over the whole chain, then sample the ends for display.
    stack = _sample_stack(_tower(meta_by_id, stack_chain, strict=False))

    if hydrate:
        if log:
            print("=== nesting_post: hydrate text from appview ===", flush=True)
        # Pass EVERY post dict, not one per uri_id: the same post can appear in
        # several towers as distinct dicts, and _hydrate already dedupes the
        # URIs it fetches. Deduping here would leave the other copies bare.
        todo = [p for t in ladder + [tallest, stack] for p in t["posts"]
                if not p.get("missing") and not p.get("gapmark")]
        _hydrate(todo, log=log)
    else:
        for t in ladder + [tallest, stack]:
            for p in t["posts"]:
                if p.get("gapmark"):
                    continue
                p.setdefault("handle", None)
                p.setdefault("text", None)
                p["bsky_url"] = (f"https://bsky.app/profile/{p['did']}/post/{p['rkey']}"
                                 if p.get("rkey") else None)

    # author_counts was keyed by did (handles arrive with hydration); relabel.
    did2handle = {p.get("did"): p.get("handle") for p in stack["posts"]
                  if p.get("handle")}
    stack["author_counts"] = [(did2handle.get(d, d), c)
                              for d, c in stack["author_counts"]]

    meta = dict(snapshot_date=snapshot_date, window=window,
                edges=int(tbl.num_rows), inc_edges=int((clikes > plikes).sum()),
                nodes=int(len(nodes)), deepest_stack=depth, cycles=cycles,
                tallest_height=hmax,
                built_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    html = render(tallest, heaviest, stack, ladder, hist, meta)
    sidecar = dict(meta=meta, height_histogram=hist, tallest=tallest,
                   heaviest=heaviest, deepest_stack=stack, ladder=ladder,
                   deepest_stack_chain=stack_chain)   # full uri_id chain, bottom-last
    return html, sidecar


def main():
    import argparse
    import duckdb

    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="analysis/snapshot/snapshot_2026-07-31.duckdb")
    ap.add_argument("--out", default="nesting_post.html")
    ap.add_argument("--edges-cache", default=None,
                    help="parquet path to cache the (expensive) edge scan")
    ap.add_argument("--snapshot-date", default=None)
    ap.add_argument("--no-hydrate", action="store_true")
    a = ap.parse_args()

    date = a.snapshot_date or os.path.basename(a.db).replace(
        "snapshot_", "").replace(".duckdb", "")
    con = duckdb.connect(a.db, read_only=True)
    html, sidecar = run(con, date, edges_cache=a.edges_cache,
                        hydrate=not a.no_hydrate)
    with open(a.out, "wb") as f:
        f.write(html)
    with open(a.out.replace(".html", ".json"), "w") as f:
        json.dump(sidecar, f, indent=2, default=str)
    print(f"wrote {a.out} ({len(html)/1024:.0f} KB) + sidecar", file=sys.stderr)


if __name__ == "__main__":
    main()
