"""Tests for the inauthentic-amplification axis.

The fixture plants two classes that a naive detector confuses:

  fleet     bought amplification — a small recurring crew of batch-created,
            audience-less accounts delivering engagement on a metronome
  discover  a genuine algorithmic blowup — a large wave of engagement from
            accounts with no prior relationship to the author, but mutually
            *uncorrelated*: spread-out creation dates, human-shaped latency

Both look "inorganic" on the surface. Only the first one is. The test that
matters most here is not that fleets score high — it is that the discover
control does *not*, because scoring reach as coordination is the specific
failure mode this module exists to avoid, and it is the one that would
survive review unnoticed.
"""

from __future__ import annotations

import duckdb
import pytest

from analysis import authenticity
from analysis.lifelines import run
from synth_lifelines import SNAPSHOT_DATE, make_lifeline_snapshot


@pytest.fixture(scope="module")
def scored(tmp_path_factory):
    path, truth, auth_truth = make_lifeline_snapshot(
        tmp_path_factory.mktemp("auth") / "snapshot.duckdb",
        include_auth_posts=True)
    con = duckdb.connect(path, read_only=True)
    _html, sidecar = run(con, SNAPSHOT_DATE, max_posts=100_000, log=False)
    # The lifeline temp tables are still live on this connection, which is
    # exactly the contract `attach` relies on.
    scores, meta = authenticity.attach(con, {}, log=False)
    by_class = {}
    for uri_id, cls in auth_truth.items():
        if uri_id in scores:
            by_class.setdefault(cls, []).append(scores[uri_id]["score"])
    for uri_id in truth:
        if uri_id in scores:
            by_class.setdefault("organic", []).append(scores[uri_id]["score"])
    return scores, meta, by_class, sidecar


def _mean(xs):
    return sum(xs) / len(xs)


def test_fleet_scores_far_above_organic(scored):
    _scores, _meta, by_class, _sc = scored
    assert _mean(by_class["fleet"]) > 0.8
    assert _mean(by_class["fleet"]) > _mean(by_class["organic"]) + 0.35


def test_discover_control_is_not_flagged(scored):
    """The load-bearing test: algorithmic reach must not read as coordination.

    A discover post has the same surface features people use as bot
    evidence — a wave of strangers with no relationship to the author. If
    this assertion ever fails, the composite has started measuring reach,
    and the fix is to move weight back toward the correlation signals rather
    than to relax the threshold here.
    """
    scores, _meta, by_class, _sc = scored
    assert _mean(by_class["discover"]) < _mean(by_class["fleet"]) - 0.4
    # And not merely lower on average — none of them in the top decile.
    ranked = sorted((s["score"] for s in scores.values()), reverse=True)
    cut = ranked[int(len(ranked) * 0.1)]
    assert all(s < cut for s in by_class["discover"])
    assert sum(1 for s in by_class["fleet"] if s >= cut) == len(by_class["fleet"])


def test_correlation_signals_carry_the_weight(scored):
    """Timing must stay a prefilter, not the basis of the score.

    Timing regularity is one line of code for an adversary to jitter away,
    and it is what makes ordinary algorithmic reach look automated. The
    weighting is a deliberate design decision, so it is asserted rather than
    left to drift.
    """
    _scores, _meta, _by_class, sc = scored
    fam = sc["authenticity"]["family_weights"]
    assert fam["correlation"] > 0.7
    assert fam.get("timing", 0) < 0.25


def test_confound_check_is_reported(scored):
    """`broadcast` topping the score table is the drift alarm; assert it isn't.

    broadcast is the archetype *defined* by an out-of-network wave. If it
    carries the highest mean authenticity score, the composite has collapsed
    into measuring Discover.
    """
    _scores, _meta, _by_class, sc = scored
    means = sc["authenticity"]["mean_score_by_archetype"]
    assert means
    assert max(means, key=means.get) != "broadcast"


def test_signals_skip_cleanly_when_columns_are_missing():
    """A snapshot built without the `plc` phase must lose one signal, not the run.

    Published snapshots vary in whether `actors.created_at` exists, so this
    degradation path is normal operation rather than an error case.
    """
    con = duckdb.connect()
    con.execute("CREATE TEMP TABLE lf_cohort AS SELECT 1::BIGINT AS uri_id")
    con.execute("""CREATE TEMP TABLE lf_ev AS
        SELECT 1::BIGINT AS uri_id, 0::TINYINT AS ch, 2::BIGINT AS actor,
               3::BIGINT AS author, NOW()::TIMESTAMP AS ts, 10::BIGINT AS dt""")
    con.execute("""CREATE TEMP TABLE lf_follow AS
        SELECT 2::BIGINT AS actor, 3::BIGINT AS author,
               NOW()::TIMESTAMP AS followed_at""")
    con.execute("CREATE TABLE actors(did_id BIGINT, did VARCHAR, active BOOLEAN)")
    _scores, meta = authenticity.attach(con, {}, log=False)
    assert "age_clustering" in meta["skipped"]
    assert "engager_reach" in meta["skipped"]
    # Whatever survived did so without raising.
    assert not meta["failed"]


def test_a_broken_signal_does_not_lose_the_others():
    """One signal raising must not cost the other seven.

    Signals run against a schema that varies across snapshots; the registry
    is only useful if a new or broken one degrades to a reported failure
    instead of taking the report down with it.
    """
    con = duckdb.connect()
    con.execute("CREATE TEMP TABLE lf_cohort AS SELECT 1::BIGINT AS uri_id")
    con.execute("""CREATE TEMP TABLE lf_ev AS
        SELECT 1::BIGINT AS uri_id, 0::TINYINT AS ch, 2::BIGINT AS actor,
               3::BIGINT AS author, NOW()::TIMESTAMP AS ts, 10::BIGINT AS dt""")
    con.execute("""CREATE TEMP TABLE lf_follow AS
        SELECT 2::BIGINT AS actor, 3::BIGINT AS author,
               NOW()::TIMESTAMP AS followed_at""")
    # No `actors` table at all: tombstone_rate blows up, the rest carry on.
    _scores, meta = authenticity.attach(
        con, {}, enabled=["tombstone_rate", "co_engagement",
                          "interarrival_regularity"], log=False)
    assert "tombstone_rate" in meta["failed"]
    assert {s["name"] for s in meta["signals_run"]} == {
        "co_engagement", "interarrival_regularity"}


def test_weights_can_be_overridden_and_are_renormalized(tmp_path_factory):
    """Reweighting is the iteration knob, so it must work from the caller.

    Also pins the renormalization: weights are supplied as relative
    importances and must sum to 1 across whichever signals actually ran, so
    that a skipped signal redistributes its share rather than silently
    shrinking every score.
    """
    path, _truth, _auth = make_lifeline_snapshot(
        tmp_path_factory.mktemp("w") / "snapshot.duckdb", include_auth_posts=True)
    con = duckdb.connect(path, read_only=True)
    run(con, SNAPSHOT_DATE, max_posts=100_000, authenticity=False, log=False)

    _base, meta = authenticity.attach(
        con, {}, enabled=["co_engagement", "tombstone_rate"], log=False)
    assert meta["weights"] == pytest.approx(
        {"co_engagement": 0.34 / 0.44, "tombstone_rate": 0.10 / 0.44}, abs=1e-3)

    _tuned, meta2 = authenticity.attach(
        con, {}, enabled=["co_engagement", "tombstone_rate"],
        weights={"co_engagement": 3.0, "tombstone_rate": 1.0}, log=False)
    assert meta2["weights"] == pytest.approx(
        {"co_engagement": 0.75, "tombstone_rate": 0.25}, abs=1e-3)


def test_flagged_examples_are_linked_only_on_explicit_opt_in(tmp_path_factory):
    """Naming accounts we are implicitly accusing takes an explicit flag.

    Every other archetype in the report links to real posts. This one links
    only when the caller sets `link_flagged_examples`, because "bought
    engagement" is an accusation and the inference is probabilistic.
    """
    path, _truth, _auth = make_lifeline_snapshot(
        tmp_path_factory.mktemp("link") / "snapshot.duckdb",
        include_auth_posts=True)
    con = duckdb.connect(path, read_only=True)
    html, _sc = run(con, SNAPSHOT_DATE, max_posts=100_000,
                    link_flagged_examples=True, log=False)
    section = html.split(b"Highest-scoring posts")[1]
    assert b"open post" in section


def test_report_renders_the_authenticity_section(tmp_path_factory):
    path, _truth, _auth = make_lifeline_snapshot(
        tmp_path_factory.mktemp("authhtml") / "snapshot.duckdb",
        include_auth_posts=True)
    con = duckdb.connect(path, read_only=True)
    html, _sc = run(con, SNAPSHOT_DATE, max_posts=100_000, log=False)
    assert b"Engagement that does not look like it was earned" in html
    assert b"co engagement" in html
    # The relativity caveat and the redaction notice are both load-bearing.
    assert b"relative, not absolute" in html
    assert b"Redacted deliberately" in html
    # Redacted by default: no post links inside the authenticity table.
    auth_section = html.split(b"Highest-scoring posts")[1]
    assert b"open post" not in auth_section
