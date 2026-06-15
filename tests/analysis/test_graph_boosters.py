"""End-to-end test for the booster-account graph analysis on synthetic data.

Builds a tiny in-memory snapshot with a known booster population and asserts the
classification, the 1/2/3 out-degree split (after stripping the default
`bsky.app` follow), and per-target concentration. Farm detection (igraph) is
exercised only when igraph is importable; the analysis degrades gracefully
otherwise, so this test does not require it.
"""

from __future__ import annotations

import duckdb
import pytest

from analysis.graph_boosters import BSKY_APP_DID, run


def _build_con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("CREATE TABLE actors(did_id BIGINT, did VARCHAR, active BOOLEAN)")
    actors = [(0, BSKY_APP_DID), (1, "did:plc:target1"), (2, "did:plc:target2"),
              (30, "did:plc:normal")]
    actors += [(i, f"did:plc:booster{i}") for i in range(10, 15)]
    actors.append((20, "did:plc:booster20"))
    con.executemany("INSERT INTO actors VALUES (?,?,TRUE)", actors)

    con.execute("""CREATE TABLE actor_aggs(
        did_id BIGINT, follows BIGINT, followers BIGINT, posts BIGINT,
        replies_out BIGINT, reposts_out BIGINT, quotes_out BIGINT, likes_out BIGINT)""")
    aggs = [
        (0, 0, 12, 0, 0, 0, 0, 0),     # bsky.app
        (1, 1, 6, 100, 0, 0, 0, 0),    # target1 (popular)
        (2, 1, 1, 50, 0, 0, 0, 0),     # target2 (related)
        (30, 50, 3, 10, 2, 0, 0, 5),   # normal: content>0 -> not a booster
    ]
    aggs += [(i, 2, 0, 0, 0, 0, 0, 0) for i in range(10, 15)]  # follow bsky+t1 -> adj_out 1
    aggs.append((20, 3, 0, 0, 0, 0, 0, 0))                     # follow bsky+t1+t2 -> adj_out 2
    con.executemany("INSERT INTO actor_aggs VALUES (?,?,?,?,?,?,?,?)", aggs)

    con.execute("CREATE TABLE follows(src_did_id BIGINT, dst_did_id BIGINT, created_at TIMESTAMP)")
    edges = []
    for i in range(10, 15):
        edges += [(i, 0), (i, 1)]            # boosters of target1 (+ default bsky.app)
    edges += [(20, 0), (20, 1), (20, 2)]     # booster following the target1+target2 farm
    edges += [(30, 1)] + [(30, k) for k in range(100, 149)]  # normal: 50 follows, has content
    con.executemany("INSERT INTO follows VALUES (?,?,NULL)", edges)
    return con


def test_booster_classification_and_concentration():
    con = _build_con()
    # plc_glob=None disables the age filter (no PLC dates in this synthetic db),
    # so the whole population is in scope.
    html, sc = run(con, "synthetic", plc_glob=None, hydrate_handles=False,
                   min_target_support=1, build_full_graph=False, top_targets=10,
                   log=False)

    # 5 single-target boosters + 1 two-target booster; the 50-follow account
    # with content is excluded.
    assert sc["boosters"] == 6
    assert (sc["boosters_outdeg_1"], sc["boosters_outdeg_2"], sc["boosters_outdeg_3"]) == (5, 1, 0)
    assert sc["population_total"] == 6 + 1 + 1 + 1 + 1  # boosters + targets + normal + bsky

    by_did = {t["did"]: t for t in sc["top_targets"]}
    assert by_did["did:plc:target1"]["booster_followers"] == 6
    assert by_did["did:plc:target2"]["booster_followers"] == 1
    # target1 ratio = 6 booster followers / 6 total followers = 1.0
    assert by_did["did:plc:target1"]["booster_ratio"] == pytest.approx(1.0)

    assert isinstance(html, bytes) and len(html) > 1000
    assert sc["bsky_app_did_id"] == 0
    assert sc["created_at_source"] == "none"


def test_pds_facet_flags_self_hosted():
    # Baked-style snapshot: actors already carry created_at + pds + handle, so
    # the analysis uses them directly and computes the PDS facet.
    con = duckdb.connect()
    con.execute("""CREATE TABLE actors(
        did_id BIGINT, did VARCHAR, active BOOLEAN, created_at TIMESTAMP,
        tombstoned_at TIMESTAMP, in_microcosm BOOLEAN, pds VARCHAR, handle VARCHAR)""")
    BNET = "https://shard.us-west.host.bsky.network"
    EVIL = "https://pds.evil.example"
    rows = [
        (0, BSKY_APP_DID, BNET, "bsky.app"),
        (1, "did:plc:target1", BNET, "target1"),
        (30, "did:plc:normal", BNET, "normal"),
    ]
    rows += [(i, f"did:plc:booster{i}", EVIL, f"booster{i}") for i in range(10, 15)]
    con.executemany(
        "INSERT INTO actors VALUES (?,?,TRUE,TIMESTAMP '2025-03-01',NULL,TRUE,?,?)",
        rows,
    )
    con.execute("""CREATE TABLE actor_aggs(
        did_id BIGINT, follows BIGINT, followers BIGINT, posts BIGINT,
        replies_out BIGINT, reposts_out BIGINT, quotes_out BIGINT, likes_out BIGINT)""")
    aggs = [(0, 0, 5, 0, 0, 0, 0, 0), (1, 1, 6, 100, 0, 0, 0, 0),
            (30, 50, 3, 10, 2, 0, 0, 5)]
    aggs += [(i, 2, 0, 0, 0, 0, 0, 0) for i in range(10, 15)]
    con.executemany("INSERT INTO actor_aggs VALUES (?,?,?,?,?,?,?,?)", aggs)
    con.execute("CREATE TABLE follows(src_did_id BIGINT, dst_did_id BIGINT, created_at TIMESTAMP)")
    edges = []
    for i in range(10, 15):
        edges += [(i, 0), (i, 1)]
    edges += [(30, 1)]
    con.executemany("INSERT INTO follows VALUES (?,?,NULL)", edges)

    _, sc = run(con, "synthetic", hydrate_handles=False, min_target_support=1,
                build_full_graph=False, log=False)
    pb = sc["pds_breakdown"]
    # the 5 boosters sit on the self-hosted host; bsky.network ones don't count.
    assert pb["self_hosted_boosters"] == 5
    assert pb["self_hosted_accounts"] == 5
    assert pb["self_hosted_pds_count"] == 1
    top = {r["pds"]: r for r in pb["top_self_hosted"]}
    assert EVIL in top and top[EVIL]["accounts"] == 5
    assert BNET not in top  # bsky.network is "standard", not self-hosted


def test_farms_detected_when_igraph_present():
    igraph = pytest.importorskip("igraph")
    assert igraph  # silence unused
    con = _build_con()
    _, sc = run(con, "synthetic", plc_glob=None, hydrate_handles=False,
                min_target_support=1, build_full_graph=False, log=False)
    # booster20 co-follows target1 & target2 -> one 2-node farm.
    assert sc["largest_farm"] == 2
    assert sc["farms"] >= 1
