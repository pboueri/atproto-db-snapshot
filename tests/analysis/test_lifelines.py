"""Tests for the post engagement lifeline / archetype analysis.

The central test is a semi-synthetic injection: `synth_lifelines.py` plants
posts whose arrival processes are drawn from hand-specified archetype
recipes, and we assert the classifier recovers the label each post was built
with. There is no ground truth for engagement archetypes in the real data,
so injection is the only way to put a number on whether the rules work — and
it doubles as a regression guard, since a threshold that drifts shows up
immediately as a confusion between two specific archetypes rather than as a
vague change in the population mix.
"""

from __future__ import annotations

import duckdb
import pytest

from analysis.lifelines import (
    RULE_THRESHOLDS, _classify, _ksc, _wave_features, run,
)
from synth_lifelines import SNAPSHOT_DATE, make_lifeline_snapshot


@pytest.fixture(scope="module")
def lifeline_snapshot(tmp_path_factory):
    path, truth, _auth = make_lifeline_snapshot(
        tmp_path_factory.mktemp("lf") / "snapshot.duckdb")
    return path, truth


@pytest.fixture(scope="module")
def report(lifeline_snapshot):
    path, truth = lifeline_snapshot
    con = duckdb.connect(path, read_only=True)
    html, sidecar = run(con, SNAPSHOT_DATE, max_posts=100_000, log=False)
    return html, sidecar, truth


def test_cohort_respects_the_horizon(report):
    """No post may be closer to the data's end than one full horizon.

    This is the guard against right-censoring: if the cohort ran up to the
    cut, recent posts would look like they died young when in fact we just
    stopped watching, and every shape feature would be garbage.
    """
    _html, sc, _truth = report
    c = sc["cohort"]
    gap_hours = (c["window_cut"] - c["cohort_hi"]).total_seconds() / 3600
    assert gap_hours >= c["horizon_hours"] - 1
    assert (c["cohort_hi"] - c["cohort_lo"]).days == c["cohort_days"]


def test_every_planted_archetype_is_recovered(report):
    """Each recipe's posts land in the archetype they were built as."""
    _html, sc, _truth = report
    counts = {a: v["n"] for a, v in sc["archetypes"].items()}
    # Nine recipes x 14 posts each, and each should be recovered whole.
    assert sc["cohort"]["posts_analyzed"] == 126
    for arch in ("standard", "like_forward", "broadcast", "pile_on",
                 "conversation", "sleeper", "evergreen", "necro",
                 "unclassified"):
        assert counts.get(arch, 0) == 14, (arch, counts)


def test_audience_axis_separates_in_and_out_of_network(report):
    """Archetypes built with a late stranger wave must show it, and only them.

    `oon_delta` is (in-network share of the first hour) minus (in-network
    share after six hours), so a positive value means the followers came
    first and strangers arrived later — the algorithmic-distribution
    signature.

    `necro` is deliberately absent from both lists. It puts ~94% of its
    engagement in a burst four days out and lands single digits in its first
    hour, so there is no early bucket to compare a late one against, and
    `MIN_AUDIENCE_BUCKET` makes it report no turnover rather than a figure
    derived from a handful of events. That is the correct answer for a post
    shaped like this, and asserting a turnover for it would be asserting
    that noise gets through.
    """
    _html, sc, _truth = report
    a = sc["archetypes"]
    for arch in ("broadcast", "pile_on", "sleeper"):
        assert a[arch]["mean_oon_delta"] > 0.25, (arch, a[arch])
    for arch in ("standard", "conversation", "evergreen", "like_forward"):
        assert abs(a[arch]["mean_oon_delta"]) < 0.15, (arch, a[arch])
    assert a["necro"]["mean_oon_delta"] == 0.0
    # Conversation was built in-network; pile-on was built from strangers.
    assert a["conversation"]["mean_in_network"] > a["pile_on"]["mean_in_network"]


def test_mix_axis_matches_the_recipes(report):
    _html, sc, _truth = report
    a = sc["archetypes"]
    assert a["like_forward"]["mean_mix"]["like"] > 0.90
    assert a["broadcast"]["mean_mix"]["repost"] > 0.22
    assert (a["pile_on"]["mean_mix"]["reply"]
            + a["pile_on"]["mean_mix"]["quote"]) > 0.32
    assert a["conversation"]["mean_mix"]["reply"] > 0.28


def test_necro_and_evergreen_are_distinguished_by_burstiness(report):
    """Both put most of their mass late; only necro is concentrated.

    This pair is the one the taxonomy gets wrong if `burst_share` is dropped
    or its window stops sliding, so it is asserted explicitly.
    """
    _html, sc, _truth = report
    assert sc["archetypes"]["necro"]["mean_t50_h"] > 72
    assert sc["archetypes"]["evergreen"]["mean_t50_h"] > 24
    assert (sc["archetypes"]["necro"]["mean_t50_h"]
            > sc["archetypes"]["evergreen"]["mean_t50_h"])


def test_examples_are_produced_with_urls(report):
    _html, sc, _truth = report
    for arch, exs in sc["examples"].items():
        assert len(exs) == 5, (arch, len(exs))
        for e in exs:
            assert e["url"].startswith("https://bsky.app/profile/did:plc:")


def test_report_is_self_contained_html(report):
    html, sc, _truth = report
    assert html.startswith(b"<!doctype html>")
    # Plotly is inlined, not fetched — the reports get published standalone,
    # so the page must not reach out to any host at render time. Checked by
    # the absence of external references rather than of the string
    # "cdn.plot.ly", which appears inside the inlined bundle's own source.
    assert b"<script src=" not in html
    assert b"<link " not in html
    assert b"<title>Post engagement archetypes" in html
    # The caveats are load-bearing, not decoration.
    assert b"Unfollows are invisible" in html
    assert str(sc["cohort"]["min_engagement"]).encode() in html


def test_thresholds_are_reported_for_reproducibility(report):
    """The sidecar must carry the exact thresholds a run used.

    The taxonomy is a decision list, so a report without its thresholds is
    not reproducible — a later rebuild with different values would produce
    different populations with no way to tell why.
    """
    _html, sc, _truth = report
    assert sc["thresholds"] == RULE_THRESHOLDS


def test_reservoir_sampling_bounds_the_cohort(lifeline_snapshot):
    """Cohorts above `max_posts` are sampled down, not silently truncated.

    On the real snapshot the eligible cohort is far larger than what the
    event extraction should pull, so this branch always runs in production
    and never runs in the other tests — worth covering explicitly.
    """
    path, _truth = lifeline_snapshot
    con = duckdb.connect(path, read_only=True)
    _html, sc = run(con, SNAPSHOT_DATE, max_posts=40, log=False)
    assert sc["cohort"]["cohort_eligible"] == 126
    assert sc["cohort"]["posts_sampled"] == 40
    assert sc["cohort"]["posts_analyzed"] == 40


def test_wave_features_find_a_separated_second_wave():
    """A two-humped log-density curve reads as two waves; one hump as one."""
    single = [0, 0, 1, 5, 20, 12, 4, 1, 0, 0, 0, 0]
    n_waves, reignition = _wave_features(single)
    assert n_waves == 1
    assert reignition == 0.0

    double = [0, 0, 1, 5, 20, 12, 2, 0, 0, 3, 18, 6]
    n_waves, reignition = _wave_features(double)
    assert n_waves == 2
    assert reignition > 0.45


def test_ksc_is_scale_invariant():
    """Two curves of the same shape at wildly different volume must cluster
    together — that scale invariance is the entire reason KSC was chosen
    over clustering the raw curves."""
    import numpy as np

    shape_a = np.array([0.0, 1, 8, 20, 9, 3, 1, 0])
    shape_b = np.array([0.0, 0, 1, 2, 4, 9, 18, 7])
    X = np.stack([shape_a, shape_a * 1000.0, shape_b, shape_b * 500.0])
    labels, _centroids = _ksc(X, 2, seed=0)
    assert labels[0] == labels[1]
    assert labels[2] == labels[3]
    assert labels[0] != labels[2]


def test_classify_is_a_total_function():
    """Every feature vector gets a label; nothing falls off the decision list."""
    base = {
        "late24": 0.0, "late72": 0.0, "t50_h": 1.0, "burst_share": 0.9,
        "reignition": 0.0, "n_waves": 1, "reply_share": 0.0,
        "quote_share": 0.0, "like_share": 0.5, "repost_share": 0.5,
        "reply_lag_h": 0.0, "reply_outperform": 0.0, "oon_delta": 0.0,
        "in_network_share": 0.5,
    }
    assert _classify(base, RULE_THRESHOLDS)
    extreme = dict(base, late72=1.0, late24=1.0, t50_h=150.0)
    assert _classify(extreme, RULE_THRESHOLDS) == "necro"
