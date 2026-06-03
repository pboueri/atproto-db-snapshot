"""End-to-end test: follow-booster analysis on the synthetic snapshot.

The synthetic fixture has no `actors` table (so the bsky exclusion resolves to
empty) but does have follows / actor_aggs / posts / reposts, which is enough to
exercise every code path. We use a small min_followers so the rankings are
non-empty on the modest synthetic graph.
"""

import pytest

pytest.importorskip("plotly")

from analysis.boosters import run  # noqa: E402


def test_boosters_runs_and_produces_html(synthetic_con, snapshot_date):
    html, sidecar = run(
        synthetic_con, snapshot_date,
        min_followers=2, top_n=10, log=False,
    )

    assert isinstance(html, bytes) and len(html) > 10_000
    assert b"<!DOCTYPE html>" in html
    assert b"boosters" in html

    assert sidecar["snapshot_date"] == snapshot_date
    assert sidecar["n_actors"] > 0
    assert sidecar["n_clean_follow_edges"] > 0
    # no actors table in the fixture -> nothing resolved to exclude
    assert sidecar["n_excluded_resolved"] == 0
    # shares are well-formed fractions
    assert 0.0 <= sidecar["booster_edge_share"] <= 1.0
    assert 0.0 <= sidecar["dedicated_edge_share"] <= 1.0
    assert sidecar["n_booster_accounts"] >= 0

    # rankings are lists of dicts with the expected derived fields
    for key in ("top_by_booster_share", "top_by_dedicated_share"):
        assert isinstance(sidecar[key], list)
        for row in sidecar[key]:
            assert row["followers_clean"] >= 2
            assert 0.0 <= row["booster_share"] <= 1.0
            assert 0.0 <= row["dedicated_share"] <= 1.0
            # a follower can't be "dedicated" (follows only T) without being
            # counted among T's followers
            assert row["dedicated_followers"] <= row["followers_clean"]
            assert row["booster_followers"] <= row["followers_clean"]


def test_booster_share_matches_manual_count(synthetic_con, snapshot_date):
    """Cross-check one target's booster_share against a hand-written query."""
    _, sidecar = run(synthetic_con, snapshot_date, min_followers=2, top_n=50, log=False)
    if not sidecar["top_by_booster_share"]:
        pytest.skip("no ranked targets in synthetic graph")
    t = sidecar["top_by_booster_share"][0]
    did_id = int(t["did_id"])
    # recompute booster_followers for this target straight from the tables
    manual = synthetic_con.execute(
        """
        SELECT count(*) FILTER (WHERE (a.posts+a.replies_out+a.quotes_out)=0)
        FROM follows f
        JOIN actor_aggs a ON a.did_id = f.src_did_id
        WHERE f.dst_did_id = ?
        """,
        [did_id],
    ).fetchone()[0]
    assert manual == t["booster_followers"]
