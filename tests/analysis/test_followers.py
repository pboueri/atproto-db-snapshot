"""End-to-end test: follower-distribution analysis on synthetic data."""

from analysis.followers import run


def test_followers_runs_and_produces_html(synthetic_con, snapshot_date):
    html, sidecar = run(synthetic_con, snapshot_date, log=False)

    assert isinstance(html, bytes) and len(html) > 10_000
    assert b"<!DOCTYPE html>" in html
    assert b"follower distribution" in html.lower()
    # The log10 framing is the whole point of the chart.
    assert b"log10" in html or b"log scale" in html

    # Sidecar shape + sanity bounds.
    assert sidecar["snapshot_date"] == snapshot_date
    assert sidecar["total_actors"] > 0
    assert sidecar["with_followers"] > 0
    assert sidecar["with_followers"] <= sidecar["total_actors"]
    assert sidecar["zero_followers"] >= 0
    assert (sidecar["with_followers"] + sidecar["zero_followers"]
            == sidecar["total_actors"])
    assert 0.0 <= sidecar["pct_zero_followers"] <= 100.0
    assert sidecar["max_followers"] >= sidecar["median_followers_positive"]
    assert sidecar["bins_per_decade"] == 10

    # The histogram should be densely filled (no gaps): one count per bin
    # id between min and max, and the total over positive bins matches the
    # with_followers headline.
    hist = sidecar["histogram"]
    assert len(hist["counts"]) == len(hist["bin_ids"])
    assert len(hist["bin_center_followers"]) == len(hist["counts"])
    assert sum(hist["counts"]) == sidecar["with_followers"]

    # Verdict is one of the known states and is consistent with modes.
    assert sidecar["verdict"] in {"unimodal", "bimodal", "empty"}
    if sidecar["is_multimodal"]:
        assert len(sidecar["modes"]) >= 2
        assert sidecar["verdict"] == "bimodal"
