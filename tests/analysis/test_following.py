"""End-to-end test: accounts-followed (out-degree) distribution on synthetic data."""

from analysis.following import run


def test_following_runs_and_produces_html(synthetic_con, snapshot_date):
    html, sidecar = run(synthetic_con, snapshot_date, log=False)

    assert isinstance(html, bytes) and len(html) > 10_000
    assert b"<!DOCTYPE html>" in html
    # Reframed around "accounts followed since <year>", out-degree.
    assert b"followed" in html.lower() and b"since 2025" in html.lower()
    # The log10 framing is the whole point of the chart.
    assert b"log10" in html or b"log scale" in html

    # Sidecar shape + sanity bounds.
    assert sidecar["snapshot_date"] == snapshot_date
    assert sidecar["metric"] == "accounts_followed_since"
    assert sidecar["since_date"] == "2025-01-01"
    assert sidecar["total_actors"] > 0
    assert sidecar["with_follows"] > 0
    assert sidecar["with_follows"] <= sidecar["total_actors"]
    assert sidecar["zero_follows"] >= 0
    assert (sidecar["with_follows"] + sidecar["zero_follows"]
            == sidecar["total_actors"])
    assert 0.0 <= sidecar["pct_zero_follows"] <= 100.0
    assert sidecar["max_follows"] >= sidecar["median_follows_positive"]
    assert sidecar["bins_per_decade"] == 10

    # Histogram densely filled (no gaps); positive-bin total matches headline.
    hist = sidecar["histogram"]
    assert len(hist["counts"]) == len(hist["bin_ids"])
    assert len(hist["bin_center_follows"]) == len(hist["counts"])
    assert sum(hist["counts"]) == sidecar["with_follows"]

    assert sidecar["verdict"] in {"unimodal", "bimodal", "empty"}
    if sidecar["is_multimodal"]:
        assert len(sidecar["modes"]) >= 2
        assert sidecar["verdict"] == "bimodal"
