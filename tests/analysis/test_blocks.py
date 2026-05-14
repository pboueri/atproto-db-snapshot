"""End-to-end test: signed-graph SVD analysis on synthetic data.

Uses small n_items + k_components so svds works on the modest matrix
the synthetic generator produces.
"""

import pytest

pytest.importorskip("scipy")
pytest.importorskip("numpy")
pytest.importorskip("pyarrow")

from analysis.blocks import run  # noqa: E402


def test_blocks_runs_and_produces_html(synthetic_con, snapshot_date):
    html, sidecar = run(
        synthetic_con, snapshot_date,
        n_items=30, k_components=3, log=False,
    )

    assert isinstance(html, bytes) and len(html) > 10_000
    assert b"<!DOCTYPE html>" in html
    assert b"primary cleavage" in html

    assert sidecar["snapshot_date"] == snapshot_date
    assert sidecar["n_items"] <= 30
    assert sidecar["k_components"] >= 1
    assert sidecar["n_voters"] > 0
    assert sidecar["n_edges"] > 0
    assert sidecar["n_follow_edges"] + sidecar["n_block_edges"] == sidecar["n_edges"]
    assert len(sidecar["singular_values"]) == sidecar["k_components"]
    assert 0.0 <= sidecar["pc1_share_pct"] <= 100.0
