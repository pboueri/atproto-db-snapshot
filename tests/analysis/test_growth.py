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
    html, sidecar = run(
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
    _html, sidecar = run(
        synthetic_con, snapshot_date,
        raw_dir=None,
        lookback_days=1,
        log=False,
    )
    _html_all, sidecar_all = run(
        synthetic_con, snapshot_date,
        raw_dir=None,
        lookback_days=None,
        log=False,
    )
    assert sidecar["n_events"] < sidecar_all["n_events"]
