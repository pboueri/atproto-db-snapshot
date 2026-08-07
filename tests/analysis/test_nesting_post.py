"""Quote-tower analysis on a hand-built graph with known answers.

The shared synthetic snapshot has no `quote_uri_id` data and no `source`
column, and the properties worth pinning here are structural rather than
statistical — so this builds its own tiny snapshot where every tower is known
by construction.

The fixture deliberately encodes the two ways this DP has actually broken:

  * a chain whose bottom post is itself a quote must not count as a tower
    (the "root must be an original" rule), and
  * a tower of height 3 whose middle post is *not* a root must still be found
    — the achievable heights at a node are not contiguous, so the height loop
    has to skip a missing height rather than stop at it.
"""

from __future__ import annotations

import duckdb
import pytest

from analysis.nesting_post import run

SNAPSHOT_DATE = "2026-07-31"

# uri_id, author, likes, quote_uri_id, source
POSTS = [
    # Tower A — a clean height-3 tower on a genuine original.
    (100, 1, 10, None, "record"),
    (101, 2, 20, 100, "record"),
    (102, 3, 30, 101, "record"),

    # Case B — 201 is out-liked by what it quotes, so 202<-201 is increasing but
    # bottoms out on a QUOTE post. Must produce no tower at all. Sized so that
    # without the root rule it would win the "heaviest" slot outright, which is
    # what makes its absence detectable rather than merely unranked.
    (200, 4, 100_000, None, "record"),
    (201, 5, 5_000, 200, "record"),
    (202, 6, 90_000, 201, "record"),

    # Case C — the deepest raw stack: 4 long, likes NOT monotone.
    (300, 7, 1, None, "record"),
    (301, 8, 0, 300, "record"),
    (302, 9, 0, 301, "record"),
    (303, 1, 2, 302, "record"),

    # Case D — the heaviest tower, and only 2 tall.
    (400, 2, 1000, None, "record"),
    (401, 3, 2000, 400, "record"),
]


@pytest.fixture(scope="module")
def quote_snapshot(tmp_path_factory):
    path = tmp_path_factory.mktemp("qsnap") / "snapshot.duckdb"
    con = duckdb.connect(str(path))
    con.execute("""CREATE TABLE actors(
        did_id UBIGINT, did VARCHAR, active BOOLEAN,
        created_at TIMESTAMP, tombstoned_at TIMESTAMP, in_microcosm BOOLEAN)""")
    con.executemany("INSERT INTO actors VALUES (?, ?, true, NULL, NULL, true)",
                    [(a, f"did:plc:test{a}") for a in range(1, 10)])
    con.execute("""CREATE TABLE posts(
        uri_id UBIGINT, author_did_id UBIGINT, rkey VARCHAR, created_at TIMESTAMP,
        reply_root_uri_id UBIGINT, reply_parent_uri_id UBIGINT,
        quote_uri_id UBIGINT, source VARCHAR)""")
    con.executemany(
        "INSERT INTO posts VALUES (?, ?, ?, ?, NULL, NULL, ?, ?)",
        [(u, a, f"rkey{u}", f"2026-07-0{1 + i % 9} 12:00:00", q, s)
         for i, (u, a, _, q, s) in enumerate(POSTS)])
    con.execute("""CREATE TABLE post_aggs(
        uri_id UBIGINT, likes BIGINT, reposts BIGINT, replies BIGINT, quotes BIGINT)""")
    con.executemany("INSERT INTO post_aggs VALUES (?, ?, 0, 0, 0)",
                    [(u, likes) for u, _, likes, _, _ in POSTS])
    con.close()
    return path


@pytest.fixture()
def result(quote_snapshot):
    con = duckdb.connect(str(quote_snapshot), read_only=True)
    html, sidecar = run(con, SNAPSHOT_DATE, log=False, hydrate=False)
    con.close()
    return html, sidecar


def _ids(tower):
    return [p["uri_id"] for p in tower["posts"]]


def test_tallest_tower_is_the_root_anchored_chain(result):
    _, s = result
    assert _ids(s["tallest"]) == [100, 101, 102]      # bottom-up
    assert s["tallest"]["height"] == 3
    assert s["tallest"]["total_likes"] == 60


def test_chain_bottoming_out_on_a_quote_post_is_not_a_tower(result):
    """202 <- 201 is strictly increasing and would be the heaviest tower by far
    (95,000 likes), but 201 quotes 200 — it is the middle of a chain, not the
    bottom of one — so nothing in the output may reference it."""
    _, s = result
    every = {u for t in s["ladder"] + [s["tallest"], s["heaviest"]] for u in _ids(t)}
    assert 202 not in every
    assert 201 not in every
    assert s["heaviest"]["total_likes"] == 3000     # not 95,000


def test_height_three_survives_a_non_root_middle_post(result):
    """Regression: 101 is not a root, so its height-1 entry is empty. A height
    loop that stopped at the first empty height erased every tower of 3+."""
    _, s = result
    by_h = {t["height"]: t for t in s["ladder"]}
    assert sorted(by_h) == [2, 3]
    assert by_h[3]["total_likes"] == 60
    assert s["height_histogram"] == {3: 1, 2: 2}


def test_heaviest_tower_is_not_the_tallest(result):
    _, s = result
    assert s["heaviest"]["height"] == 2
    assert s["heaviest"]["total_likes"] == 3000
    assert _ids(s["heaviest"]) == [400, 401]


def test_deepest_stack_ignores_likes(result):
    """The 300-chain is 4 long with a 0-like middle — deeper than any tower."""
    _, s = result
    assert s["meta"]["deepest_stack"] == 4
    assert s["meta"]["cycles"] == 0
    assert _ids(s["deepest_stack"]) == [300, 301, 302, 303]
    assert s["deepest_stack"]["max_likes"] == 2


def test_renders_html(result):
    html, s = result
    assert isinstance(html, bytes) and len(html) > 5_000
    assert b"<!doctype html>" in html
    assert b"Russian Nesting Post" in html
    assert s["meta"]["snapshot_date"] == SNAPSHOT_DATE
