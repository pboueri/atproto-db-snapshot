"""Does the irregularity score actually measure irregularity?

The interesting assertions are the ranking ones. `analysis/organic.py`
claims a single number separates organic log-asymptote accumulation from
irregular accumulation; these tests build posts whose arrival processes
are known and check the claim holds, rather than only checking the run
produces bytes.
"""

from __future__ import annotations

import math

import duckdb
import numpy as np
import pytest

from analysis.organic import (
    _calibrate,
    _empirical_cdf,
    _ks_to_reference,
    _loguniform_cdf,
    _score,
    run,
)
from synth_organic import (  # noqa: E402  (conftest puts this dir on sys.path)
    FAMILIES,
    SNAPSHOT_DATE,
    family_of,
    make_organic_snapshot,
)


# --------------------------------------------------------------------------
# the statistic, in isolation
# --------------------------------------------------------------------------

def test_flat_counts_have_zero_distance_to_loguniform():
    """A perfectly flat log-time histogram *is* the null: D must be 0."""
    counts = np.full((1, 18), 7.0)
    d, signed = _ks_to_reference(counts, _loguniform_cdf(18))
    assert d[0] == pytest.approx(0.0, abs=1e-12)
    assert signed[0] == pytest.approx(0.0, abs=1e-12)


def test_front_and_back_loading_get_opposite_signs():
    """The sign says which way, which is the whole point of keeping it."""
    n_bins = 18
    ref = _loguniform_cdf(n_bins)

    front = np.zeros((1, n_bins)); front[0, 0] = 100.0
    back = np.zeros((1, n_bins)); back[0, -1] = 100.0

    d_f, s_f = _ks_to_reference(front, ref)
    d_b, s_b = _ks_to_reference(back, ref)

    # Everything in bin 0 means the post is maximally ahead of schedule.
    assert s_f[0] > 0 and s_b[0] < 0
    # Both are near-maximal departures from flat.
    assert d_f[0] > 0.9 and d_b[0] > 0.9


def test_loguniform_cdf_is_a_cdf():
    c = _loguniform_cdf(18)
    assert c[-1] == pytest.approx(1.0)
    assert np.all(np.diff(c) > 0)


def test_empirical_reference_weights_posts_equally():
    """One 10k-like post must not outvote many small ones."""
    n_bins = 6
    small = np.zeros((20, n_bins)); small[:, 0] = 50.0     # all mass, bin 0
    huge = np.zeros((1, n_bins)); huge[0, -1] = 10_000.0   # all mass, last bin
    ref = _empirical_cdf(np.vstack([small, huge]))
    # 20 of 21 posts are entirely in bin 0, so the mean CDF starts high.
    assert ref[0] == pytest.approx(20 / 21, abs=1e-9)


def test_calibration_floor_falls_with_n():
    """D's noise floor must shrink roughly like 1/sqrt(n), or the score
    would systematically flag small posts."""
    n_bins = 18
    pmf = np.full(n_bins, 1.0 / n_bins)
    grid = np.array([50, 200, 800, 3200])
    crit = _calibrate(pmf, grid, n_sims=1500, pct=95.0, seed=0,
                      say=lambda m: None)

    assert np.all(np.diff(crit) < 0), "floor must decrease with n"
    # Quadrupling n should roughly halve the floor.
    for a, b in zip(crit, crit[1:]):
        assert 1.6 < a / b < 2.6


def test_score_is_one_at_the_calibrated_percentile():
    grid = np.array([50, 500])
    crit = np.array([0.20, 0.10])
    n = np.array([50.0, 500.0])
    s = _score(np.array([0.20, 0.10]), n, grid, crit)
    assert s == pytest.approx([1.0, 1.0])


def test_purely_organic_draws_sit_below_the_band():
    """The calibration must be self-consistent: sampling from the null and
    scoring against it should put ~5% above 1.0, by construction."""
    n_bins, n = 18, 200
    pmf = np.full(n_bins, 1.0 / n_bins)
    grid = np.array([50, 200, 800])
    crit = _calibrate(pmf, grid, n_sims=4000, pct=95.0, seed=1,
                      say=lambda m: None)

    rng = np.random.default_rng(7)
    draws = rng.multinomial(n, pmf, size=3000).astype(float)
    d, _ = _ks_to_reference(draws, _loguniform_cdf(n_bins))
    s = _score(d, np.full(3000, float(n)), grid, crit)

    assert 0.02 < float((s > 1.0).mean()) < 0.10
    assert float(np.median(s)) < 0.75


# --------------------------------------------------------------------------
# end to end, against known shapes
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def organic_con(tmp_path_factory):
    db = tmp_path_factory.mktemp("organic") / "snapshot.duckdb"
    make_organic_snapshot(db)
    con = duckdb.connect(str(db), read_only=True)
    yield con
    con.close()


@pytest.fixture(scope="module")
def result(organic_con):
    return run(organic_con, SNAPSHOT_DATE, cohort_days=14, horizon_hours=24,
               min_likes=50, n_bins=18, n_sims=2000,
               examples_per_decile=5, log=False)


def _by_family(sidecar, key="score_loguniform"):
    """Median of `key` per synthetic family."""
    posts = sidecar["posts"]
    out = {}
    for fam in FAMILIES:
        vals = [v for u, v in zip(posts["uri_id"], posts[key])
                if family_of(u) == fam]
        assert vals, f"no posts recovered for family {fam}"
        out[fam] = float(np.median(vals))
    return out


def test_runs_and_renders(result):
    html, sidecar = result
    assert isinstance(html, bytes) and len(html) > 50_000
    assert b"<!doctype html>" in html.lower()
    assert sidecar["snapshot_date"] == SNAPSHOT_DATE

    # Not exactly 5*140: the fixture's like stream ends before its filler
    # posts do, so `cut` anchors on the likes and a couple of posts at the
    # very top of the creation window fall outside the cohort. That is the
    # censoring guard working, so the assertion is a floor, not equality.
    planned = len(FAMILIES) * 140
    assert 0.97 * planned <= sidecar["cohort"]["n_analyzed"] <= planned


def test_organic_family_scores_lowest(result):
    """The load-bearing assertion: the family drawn from the null must be
    the least irregular of the five, by a clear margin."""
    _, sidecar = result
    med = _by_family(sidecar)
    others = [v for k, v in med.items() if k != "organic"]
    assert med["organic"] < min(others), med
    # Not a photo-finish: every irregular family at least doubles it.
    assert min(others) > 2 * med["organic"], med


def test_organic_family_sits_inside_the_band(result):
    """Score < 1 means "no lumpier than the lumpiest 5% of pure-organic"."""
    _, sidecar = result
    med = _by_family(sidecar)
    assert med["organic"] < 1.0, med


def test_every_irregular_family_is_flagged(result):
    """Each flavour of irregular must clear the band, not just one — a
    score that only catches late bursts is not a spectrum."""
    _, sidecar = result
    med = _by_family(sidecar)
    for fam in ("spike", "necro", "two_burst", "linear"):
        assert med[fam] > 1.0, (fam, med)


def test_sign_separates_early_from_late(result):
    """spike runs ahead of schedule; necro and linear run behind."""
    _, sidecar = result
    signed = _by_family(sidecar, "d_signed_loguniform")
    assert signed["spike"] > 0.2, signed
    assert signed["necro"] < -0.2, signed
    assert signed["linear"] < 0.0, signed


def test_speed_is_not_what_is_being_measured(result):
    """spike and necro have wildly different t50 but both score high; if
    the score were a speed proxy one of them would be clean."""
    _, sidecar = result
    t50 = _by_family(sidecar, "t50_h")
    med = _by_family(sidecar)
    assert t50["spike"] < 1.0 and t50["necro"] > 15.0, t50
    assert med["spike"] > 1.0 and med["necro"] > 1.0, med


def test_empirical_reference_is_detected_as_unusable_on_a_mixture(result):
    """The empirical null has a failure mode, and the module must say so.

    This fixture is an equal mixture of five unlike shapes, so its mean
    CDF is a curve *no* family resembles. Scoring against it flags nearly
    every post — which is a statement about the reference, not about the
    posts. The `empirical_usable` flag exists to keep that from being read
    as "98% of Bluesky is inorganic", so pin it here where the mixture is
    pathological by construction.
    """
    _, sidecar = result
    ref = sidecar["reference"]
    assert ref["empirical_share_above_1"] > 0.5
    assert ref["empirical_usable"] is False
    # And the warning has to reach the page, not just the sidecar.
    html, _ = result
    assert b"not usable on this cohort" in html


def test_loguniform_null_still_ranks_correctly_on_the_mixture(result):
    """The flat null has no such failure mode: it is fixed a priori, so a
    heterogeneous cohort cannot corrupt it. That asymmetry is why it is
    the default headline."""
    _, sidecar = result
    med = _by_family(sidecar, "score_loguniform")
    assert med["organic"] < 1.0 < min(
        v for k, v in med.items() if k != "organic"), med


def test_sidecar_carries_a_complete_per_post_table(result):
    _, sidecar = result
    posts = sidecar["posts"]
    n = sidecar["cohort"]["n_analyzed"]
    for col in ("uri_id", "n", "t50_h", "hour_utc", "tie_share",
                "sub_floor_share", "d_loguniform", "score_loguniform",
                "d_empirical", "score_empirical"):
        assert len(posts[col]) == n, col
    assert all(x >= 50 for x in posts["n"])
    assert all(0.0 <= x <= 1.0 for x in posts["d_loguniform"])


def test_no_post_is_right_censored(result):
    """Cohort must end a full horizon before the data does, else late
    shapes are an artifact of when we stopped looking."""
    _, sidecar = result
    from datetime import datetime, timedelta
    cut = datetime.fromisoformat(sidecar["cohort"]["cut"])
    hi = datetime.fromisoformat(sidecar["cohort"]["hi"])
    assert cut - hi >= timedelta(hours=sidecar["params"]["horizon_hours"])


def test_cut_anchors_on_whichever_stream_ends_first(tmp_path):
    """Posts and likes are cut independently, so the horizon must be
    measured from the *earlier* stream end.

    On the real 2026-07-31 snapshot the last post is 26h newer than the
    last like. Anchoring on the last post gives the newest posts in the
    cohort a horizon in which no like could have been recorded, which
    fabricates late-arriving shapes out of pure censoring. Reproduced here
    by truncating the like stream well before the post stream.
    """
    from datetime import datetime, timedelta

    from analysis.organic import _bounds

    db = tmp_path / "truncated.duckdb"
    make_organic_snapshot(db, posts_per_family=20, likes_per_post=60)
    con = duckdb.connect(str(db))
    likes_end = datetime(2026, 4, 24, 12, 0, 0)
    con.execute("DELETE FROM likes WHERE created_at > ?", [likes_end])
    con.close()

    con = duckdb.connect(str(db), read_only=True)
    try:
        posts_end = con.execute("SELECT MAX(created_at) FROM posts").fetchone()[0]
        cut, lo, hi, ends = _bounds(con, SNAPSHOT_DATE, 14, 24)

        # The post stream really does outlast the like stream here.
        assert ends["likes"] <= likes_end < posts_end
        # And the cut follows the likes, not the posts.
        assert cut == ends["likes"]
        assert hi == cut - timedelta(hours=24)
        # No cohort post can be created after likes stopped being recorded
        # a full horizon later.
        assert hi <= ends["likes"] - timedelta(hours=24)
    finally:
        con.close()


def test_flatness_diagnostic_detects_the_mixture(result):
    """The cohort is 4/5 irregular by construction, so its mean curve must
    *not* look flat. This is the increment-zero check working."""
    _, sidecar = result
    assert sidecar["flatness"]["mean_post_max_dev"] > 0.05
