"""End-to-end test: regrettable-attrition analysis on synthetic data."""

from analysis.attrition import run


def test_attrition_runs_and_produces_html(synthetic_con, snapshot_date):
    html, sidecar = run(synthetic_con, snapshot_date, inactivity_days=30, log=False)

    assert isinstance(html, bytes) and len(html) > 10_000
    assert b"<!DOCTYPE html>" in html
    assert b"regrettable attrition" in html

    assert sidecar["snapshot_date"] == snapshot_date
    assert sidecar["inactivity_days"] == 30
    assert sidecar["total_actors"] > 0
    assert sidecar["engaged_pop"] >= 0
    assert sidecar["engaged_inactive"] <= sidecar["engaged_pop"]
    assert 0.0 <= sidecar["regret_rate_pct"] <= 100.0

    # Tier breakdown should cover at least one of the four tiers.
    tiers = {row["tier"] for row in sidecar["tier_breakdown"]}
    assert tiers.issubset({"lurker", "casual", "engaged", "power"})
    assert tiers, "expected at least one tier in the breakdown"
