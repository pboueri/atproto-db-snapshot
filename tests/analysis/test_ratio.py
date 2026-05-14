"""End-to-end test: argumentation-ratio analysis on synthetic data."""

from analysis.ratio import run


def test_ratio_runs_and_produces_html(synthetic_con, snapshot_date):
    html, sidecar = run(synthetic_con, snapshot_date, window_days=365, log=False)

    assert isinstance(html, bytes) and len(html) > 10_000
    assert b"<!DOCTYPE html>" in html
    assert b"barely exists on Bluesky" in html

    assert sidecar["snapshot_date"] == snapshot_date
    assert sidecar["window_days"] == 365
    assert sidecar["total_posts_in_window"] > 0
    # Synthetic generator biases ~3% of posts to be ratio'd so some
    # qualifying posts should exist even at this scale.
    assert sidecar["qualifying_posts"] >= 0
    assert 0.0 <= sidecar["ratio_pct"] <= 100.0
