"""End-to-end test: likes-concentration analysis on synthetic data."""

from analysis.likes import run


def test_likes_runs_and_produces_html(synthetic_con, snapshot_date):
    html, sidecar = run(synthetic_con, snapshot_date, log=False)

    assert isinstance(html, bytes) and len(html) > 10_000
    assert b"<!DOCTYPE html>" in html
    assert b"winner-take-all" in html
    assert b"Lorenz curve" in html
    assert b"Gini" in html

    # Sidecar shape + sanity bounds.
    assert sidecar["snapshot_date"] == snapshot_date
    assert sidecar["total_actors"] > 0
    assert sidecar["total_posters"] > 0
    assert sidecar["total_posts"] > 0
    assert 0.0 <= sidecar["gini"] <= 1.0
    # Concentration in the synthetic data is non-trivial — Gini should
    # be well off zero given the power-law weights in synth.py.
    assert sidecar["gini"] > 0.3, sidecar["gini"]

    # Top-share buckets should be a list of five (top 0.01% … top 50%).
    assert len(sidecar["top_shares"]) == 5
    shares = [b["share"] for b in sidecar["top_shares"]]
    # Shares are monotonic-non-decreasing as the bucket widens.
    assert shares == sorted(shares)
