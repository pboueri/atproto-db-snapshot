"""End-to-end test: growth state-machine analysis on synthetic data.

Exercises both:
- the per-actor state machine directly (`process_actor`) so we can
  assert exact transition counts on tiny hand-built timelines, and
- the full `run(con, ...)` pipeline against the synthetic snapshot
  fixture, asserting the HTML + sidecar contract holds.
"""

from __future__ import annotations

from collections import Counter, defaultdict

import numpy as np

from analysis.growth import (
    N_STATES, STATE_ACTIVE, STATE_AT_RISK, STATE_CHURNED, STATE_NAMES,
    STATE_NEW, STATE_SUPER,
    process_actor, run,
)


# ---------------------------------------------------------------------------
# Unit tests for process_actor — hand-built timelines, exact assertions
# ---------------------------------------------------------------------------


def _state_machine_outputs(events, *, at_risk_h=48, churn_h=14*24,
                           super_h=7*24, super_thr=50,
                           baseline_h=10_000, end_h=20_000,
                           is_existing=False, cohort_ym=202503):
    """Run process_actor on a list of (hour, n_actions) events.

    Returns (pop_delta, transitions, cohort_outcomes, cohort_size).
    """
    hours = np.array([h for h, _ in events], dtype=np.int64)
    counts = np.array([n for _, n in events], dtype=np.int32)
    pop_delta = np.zeros((N_STATES, end_h + 2), dtype=np.int64)
    transitions: Counter = Counter()
    cohort_outcomes = defaultdict(Counter)
    cohort_size: Counter = Counter()
    process_actor(
        hours, counts,
        at_risk_h=at_risk_h, churn_h=churn_h,
        super_h=super_h, super_thr=super_thr,
        baseline_h=baseline_h, end_h=end_h,
        is_existing=is_existing,
        pop_delta=pop_delta,
        transitions=transitions,
        cohort_outcomes=cohort_outcomes,
        cohort_size=cohort_size,
        cohort_ym=cohort_ym,
    )
    return pop_delta, transitions, cohort_outcomes, cohort_size


def test_new_actor_one_event_churns_by_end():
    # Single action at hour 11_000 (after baseline). Window: 48h to
    # at_risk, then 14d (336h) total to churn.
    _, transitions, outcomes, sizes = _state_machine_outputs(
        [(11_000, 1)],
        baseline_h=10_000, end_h=20_000,
        is_existing=False, cohort_ym=202503,
    )
    # NEW -> ACTIVE at 11_000 + 48 = 11_048
    # ACTIVE -> AT_RISK also at 11_048 (last_active is 11_000)
    # AT_RISK -> CHURNED at 11_000 + 336 = 11_336
    by_to = Counter()
    for (h, fr, to), c in transitions.items():
        by_to[(fr, to)] += c
    assert by_to[(STATE_NEW, STATE_ACTIVE)] == 1
    assert by_to[(STATE_ACTIVE, STATE_AT_RISK)] == 1
    assert by_to[(STATE_AT_RISK, STATE_CHURNED)] == 1
    # Cohort outcome should be CHURNED at end_h
    assert outcomes[202503][STATE_CHURNED] == 1
    assert sizes[202503] == 1


def test_new_actor_two_events_within_48h_stays_new_then_active():
    # Two events 24h apart. After the 2nd event, last_active is at h+24.
    # At hour first + 48 they're still NEW (since last_active = first+24
    # means idle is only 24h at the grad moment) — they transition
    # NEW -> ACTIVE at the grad boundary.
    _, transitions, _, _ = _state_machine_outputs(
        [(11_000, 1), (11_024, 1)],
        baseline_h=10_000, end_h=20_000,
        is_existing=False,
    )
    by_to = Counter()
    for (h, fr, to), c in transitions.items():
        by_to[(fr, to)] += c
    assert by_to[(STATE_NEW, STATE_ACTIVE)] == 1
    # last_active = 11_024, so AT_RISK fires at 11_024 + 48 = 11_072
    assert by_to[(STATE_ACTIVE, STATE_AT_RISK)] == 1
    assert by_to[(STATE_AT_RISK, STATE_CHURNED)] == 1


def test_super_promotion_and_demotion():
    # Burst 60 actions in one hour at h=12_000. Above threshold (50)
    # → SUPER. Then quiet until h=12_000+168=12_168 when those 60 expire
    # from the trailing window, demoting back to ACTIVE.
    # But ACTIVE will already have transitioned to AT_RISK at h=12_000+48=12_048
    # because no further action.
    _, transitions, _, _ = _state_machine_outputs(
        [(12_000, 60)],
        baseline_h=10_000, end_h=20_000,
        is_existing=True,  # bypass NEW state
    )
    by_to = Counter()
    for (h, fr, to), c in transitions.items():
        by_to[(fr, to)] += c
    # Existing actor starts ACTIVE at baseline.
    # At 12_000: actually wait — pre-event, no fires (last_active is -1
    # for is_existing with no events yet, baseline=10_000 so at_risk_fire
    # would be 10_048).
    # The code uses baseline_h + at_risk_h as the fire when last_active < 0
    # and is_existing — so AT_RISK at 10_048, CHURNED at 10_000 + 336.
    # That fires BEFORE the 12_000 event.
    # So: ACTIVE -> AT_RISK at 10_048, AT_RISK -> CHURNED at 10_336.
    # Then 12_000 event: CHURNED -> ACTIVE (resurrection).
    # Then SUPER promotion at 12_000 (60 ≥ 50).
    # No further events. SUPER decay at 12_000 + 168 = 12_168.
    # But wait — SUPER -> AT_RISK fires at 12_000 + 48 = 12_048 first,
    # before the SUPER decay at 12_168. So we go SUPER -> AT_RISK at 12_048.
    # Then AT_RISK -> CHURNED at 12_000 + 336 = 12_336.
    assert by_to[(STATE_ACTIVE, STATE_AT_RISK)] >= 1
    assert by_to[(STATE_AT_RISK, STATE_CHURNED)] >= 1
    assert by_to[(STATE_CHURNED, STATE_ACTIVE)] == 1
    assert by_to[(STATE_ACTIVE, STATE_SUPER)] == 1
    assert by_to[(STATE_SUPER, STATE_AT_RISK)] == 1


def test_resurrection_after_churn():
    # Event, long silence past churn, then another event.
    _, transitions, _, _ = _state_machine_outputs(
        [(11_000, 1), (15_000, 1)],
        baseline_h=10_000, end_h=20_000,
        is_existing=False,
    )
    by_to = Counter()
    for (h, fr, to), c in transitions.items():
        by_to[(fr, to)] += c
    # 11_000 first event → NEW
    # 11_048 NEW → ACTIVE → AT_RISK (both at same hour, single event)
    # 11_336 AT_RISK → CHURNED
    # 15_000 CHURNED → ACTIVE (resurrection)
    # 15_048 ACTIVE → AT_RISK; 15_336 → CHURNED
    assert by_to[(STATE_CHURNED, STATE_ACTIVE)] == 1
    assert by_to[(STATE_NEW, STATE_ACTIVE)] == 1


def test_existing_actor_seeded_active_at_baseline():
    # Existing actor with first event well after baseline. They should
    # be seeded ACTIVE at baseline_h, then transition normally.
    pop_delta, transitions, _, _ = _state_machine_outputs(
        [(12_000, 1)],
        baseline_h=10_000, end_h=20_000,
        is_existing=True,
    )
    # +1 ACTIVE at start_h (which is baseline_h since first event > baseline_h)
    # We can verify by cumsum: at hour 10_000, ACTIVE pop should be 1.
    pop = np.cumsum(pop_delta, axis=1)
    assert pop[STATE_ACTIVE, 10_000] >= 1


def test_post_end_transitions_not_emitted():
    # Actor with last event close to end_h. Any deadlines beyond end_h
    # should not appear in transitions.
    _, transitions, _, _ = _state_machine_outputs(
        [(19_900, 1)],
        baseline_h=10_000, end_h=20_000,
        is_existing=False,
    )
    for (h, _fr, _to), _c in transitions.items():
        assert h <= 20_000, f"transition emitted past end_h: {h}"


# ---------------------------------------------------------------------------
# End-to-end test against the synthetic snapshot
# ---------------------------------------------------------------------------


def test_growth_runs_and_produces_html(synthetic_con, snapshot_date):
    # Synthetic snapshot covers ~1 year ending just before 2026-04-28.
    # Use a baseline of 2025-12-01 so most synthetic actors land as
    # "existing" — exercises both seeding paths.
    html, sidecar, _hero = run(
        synthetic_con, snapshot_date,
        raw_dir=None,  # use in-DB tables
        at_risk_hours=48,
        churn_days=14,
        super_threshold=50,
        existing_baseline_date="2025-12-01",
        log=False,
    )

    assert isinstance(html, bytes) and len(html) > 10_000
    assert b"<!DOCTYPE html>" in html
    assert b"growing or shrinking" in html

    assert sidecar["snapshot_date"] == snapshot_date
    assert sidecar["at_risk_hours"] == 48
    assert sidecar["churn_days"] == 14
    assert sidecar["super_threshold"] == 50
    assert sidecar["n_actors"] > 0
    assert sidecar["n_events"] > 0

    # Final composition is a dict over all 5 state names.
    fp = sidecar["final_population"]
    assert set(fp.keys()) == set(STATE_NAMES)
    assert sum(fp.values()) <= sidecar["n_actors"]

    # Cohort outcomes is a non-empty list and entries are well-formed.
    assert len(sidecar["cohort_outcomes"]) >= 1
    for row in sidecar["cohort_outcomes"]:
        assert 0.0 <= row["active_pct"] <= 100.0
        assert 0.0 <= row["churned_pct"] <= 100.0
        assert row["size"] >= 1


def test_growth_lookback_filter_drops_events(synthetic_con, snapshot_date):
    # With a tiny lookback (1 day), the synthetic dataset's 365-day spread
    # should leave only a sliver of events.
    _html, sidecar, _hero = run(
        synthetic_con, snapshot_date,
        raw_dir=None,
        lookback_days=1,
        log=False,
    )
    _html_all, sidecar_all, _hero_all = run(
        synthetic_con, snapshot_date,
        raw_dir=None,
        lookback_days=None,
        log=False,
    )
    assert sidecar["n_events"] < sidecar_all["n_events"]


def test_growth_markov_and_regimes(synthetic_con, snapshot_date):
    """The Markov section and regime classification show up in the
    sidecar, and the steady-state is a proper probability distribution.
    """
    _html, sidecar, _hero = run(
        synthetic_con, snapshot_date,
        raw_dir=None,
        existing_baseline_date="2025-06-01",
        log=False,
    )
    # Markov stationary is a proper probability distribution.
    pi = sidecar["markov"]["steady_state"]
    assert set(pi.keys()) == set(STATE_NAMES)
    s = sum(pi.values())
    assert 0.99 < s < 1.01, f"steady-state must sum to ~1, got {s}"
    for v in pi.values():
        assert -1e-9 <= v <= 1.0

    # P is a stochastic matrix: rows sum to 1.
    import numpy as _np
    P = _np.array(sidecar["markov"]["P"])
    assert P.shape == (5, 5)
    row_sums = P.sum(axis=1)
    assert _np.allclose(row_sums, 1.0, atol=1e-6), f"P rows not stochastic: {row_sums}"

    # Regimes is a list of per-week labels with valid labels only.
    valid = {"growth", "no_new", "leaky_onboarding", "churning_active"}
    assert len(sidecar["regimes"]) >= 1
    for r in sidecar["regimes"]:
        assert r["regime"] in valid
        assert isinstance(r["week"], str)
    assert sidecar["current_regime"] in valid


def test_growth_hero_png_present(synthetic_con, snapshot_date):
    """If kaleido is available, the hero PNG should be valid bytes;
    if not, an empty bytes object is returned (and the rest still works).
    """
    _html, _sc, hero = run(
        synthetic_con, snapshot_date,
        raw_dir=None,
        existing_baseline_date="2025-06-01",
        log=False,
    )
    # Either kaleido produced a real PNG (starts with the PNG magic bytes),
    # or it was unavailable and we got empty bytes — both are acceptable.
    if hero:
        assert hero[:8] == b"\x89PNG\r\n\x1a\n"


# ---------------------------------------------------------------------------
# Vectorized state-machine driver vs per-actor reference path
# ---------------------------------------------------------------------------


def test_vec_matches_serial_on_synthetic(synthetic_con, snapshot_date):
    """The vectorized driver (n_workers=-1) must produce bitwise-identical
    pop_delta and equal counters to the per-actor serial driver."""
    from datetime import datetime

    from analysis import growth as g

    g._materialize_per_hour(
        synthetic_con,
        raw_dir=None,
        snap_ts=f"{snapshot_date} 23:59:59",
        plausible_lo_ts="2022-01-01 00:00:00",
        lookback_lo_ts="2022-01-01 00:00:00",
        log=False,
    )
    snap_h = g._to_hour_index(datetime.fromisoformat(f"{snapshot_date}T23:59:59"))
    baseline_h = g._to_hour_index(datetime.fromisoformat("2025-01-01T00:00:00"))

    kwargs = dict(
        at_risk_h=48, churn_h=14 * 24,
        super_h=168, super_thr=50,
        baseline_h=baseline_h, end_h=snap_h, log=False,
    )
    serial = g._run_state_machine(synthetic_con, n_workers=1, **kwargs)
    vec = g._run_state_machine(synthetic_con, n_workers=-1, **kwargs)

    # pop_delta arrays equal element-by-element
    assert np.array_equal(serial[0], vec[0]), \
        "vec pop_delta differs from serial reference"
    # transitions Counter (after dict-comparable cast)
    assert dict(serial[1]) == dict(vec[1]), \
        "vec transitions differ from serial reference"
    # cohort_outcomes structures equal
    s_co = {k: dict(v) for k, v in serial[2].items()}
    v_co = {k: dict(v) for k, v in vec[2].items()}
    assert s_co == v_co
    assert dict(serial[3]) == dict(vec[3])
    assert dict(serial[4]) == dict(vec[4])
    assert serial[5] == vec[5]
    assert serial[6] == vec[6]


def test_numba_matches_serial_on_synthetic(synthetic_con, snapshot_date):
    """The Numba kernel (n_workers=-2) must produce bitwise-identical
    pop_delta and equal counters to the per-actor serial driver.

    Skipped where numba isn't importable (e.g. a py3.14 dev box); the
    Modal image ships py3.12 + numba so it runs there and in CI on a
    compatible interpreter.
    """
    import pytest

    from analysis import growth as g

    if not g._HAVE_NUMBA:
        pytest.skip("numba not available in this interpreter")

    from datetime import datetime

    g._materialize_per_hour(
        synthetic_con,
        raw_dir=None,
        snap_ts=f"{snapshot_date} 23:59:59",
        plausible_lo_ts="2022-01-01 00:00:00",
        lookback_lo_ts="2022-01-01 00:00:00",
        log=False,
    )
    snap_h = g._to_hour_index(datetime.fromisoformat(f"{snapshot_date}T23:59:59"))
    baseline_h = g._to_hour_index(datetime.fromisoformat("2025-01-01T00:00:00"))

    # super_thr=5 forces meaningful SUPER activity so the kernel's super
    # promotion/decay branches are exercised, not just the 4-state base.
    kwargs = dict(
        at_risk_h=48, churn_h=14 * 24,
        super_h=168, super_thr=5,
        baseline_h=baseline_h, end_h=snap_h, log=False,
    )
    serial = g._run_state_machine(synthetic_con, n_workers=1, **kwargs)
    numba = g._run_state_machine(synthetic_con, n_workers=-2, **kwargs)

    assert np.array_equal(serial[0], numba[0]), \
        "numba pop_delta differs from serial reference"
    assert dict(serial[1]) == dict(numba[1]), \
        "numba transitions differ from serial reference"
    s_co = {k: dict(v) for k, v in serial[2].items()}
    n_co = {k: dict(v) for k, v in numba[2].items()}
    assert s_co == n_co
    assert dict(serial[3]) == dict(numba[3])
    assert dict(serial[4]) == dict(numba[4])
    assert serial[5] == numba[5]
    assert serial[6] == numba[6]


def test_state_log_reconstructs_pop_delta(synthetic_con, snapshot_date, tmp_path):
    """The per-user state-interval log must reconstruct exactly the same
    pop_delta the kernel produced — cross-validates `_sm_state_log_kernel`
    against `_sm_kernel`. Forward-filling the (did, hour, state) records
    is the inverse of the population deltas."""
    import pytest

    from analysis import growth as g

    if not g._HAVE_NUMBA:
        pytest.skip("numba not available in this interpreter")

    from datetime import datetime

    import pyarrow.parquet as pq

    g._materialize_per_hour(
        synthetic_con, raw_dir=None,
        snap_ts=f"{snapshot_date} 23:59:59",
        plausible_lo_ts="2022-01-01 00:00:00",
        lookback_lo_ts="2022-01-01 00:00:00",
        log=False,
    )
    snap_h = g._to_hour_index(datetime.fromisoformat(f"{snapshot_date}T23:59:59"))
    baseline_h = g._to_hour_index(datetime.fromisoformat("2025-01-01T00:00:00"))

    log_path = str(tmp_path / "state_log.parquet")
    kwargs = dict(
        at_risk_h=48, churn_h=14 * 24, super_h=168, super_thr=5,
        baseline_h=baseline_h, end_h=snap_h, log=False,
    )
    numba = g._run_state_machine(
        synthetic_con, n_workers=-2, state_log_path=log_path, **kwargs,
    )
    pop_delta = numba[0]

    tbl = pq.read_table(log_path)
    did = tbl.column("did_id").to_numpy()
    hour = tbl.column("hour_idx").to_numpy()
    state = tbl.column("state").to_numpy()

    # Reconstruct pop_delta by forward-filling each actor's intervals.
    recon = np.zeros_like(pop_delta)
    prev_state = -1
    prev_did = None
    for i in range(len(did)):
        d, h, s = int(did[i]), int(hour[i]), int(state[i])
        if d != prev_did:  # first record for this actor → initial entry
            recon[s, h] += 1
            prev_did = d
        else:  # transition: exit prev state, enter new state at this hour
            recon[prev_state, h] -= 1
            recon[s, h] += 1
        prev_state = s

    assert np.array_equal(recon, pop_delta), \
        "state-log forward-fill does not reconstruct kernel pop_delta"

    # state values are valid; records sorted by (did, hour) within actor.
    assert set(np.unique(state).tolist()).issubset(set(range(N_STATES)))
