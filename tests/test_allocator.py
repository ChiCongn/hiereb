"""
Unit tests for src/ml_hiereb/allocator.py

Tests the core HierEB water-filling and censored correction logic.
No Kafka or Docker required.

Run: pytest tests/test_allocator.py -v
"""
from __future__ import annotations

import math

import pytest

from src.ml_hiereb.allocator import HierEBAllocator, PlugVarianceState, WelfordState


# ─── WelfordState ─────────────────────────────────────────────────────────────

def test_welford_single_sample_variance_zero():
    w = WelfordState()
    w.update(10.0)
    assert w.variance == pytest.approx(0.0)
    assert w.std == pytest.approx(0.0)


def test_welford_two_samples():
    w = WelfordState()
    w.update(10.0)
    w.update(20.0)
    # Sample variance of [10, 20] = 50
    assert w.variance == pytest.approx(50.0)
    assert w.std == pytest.approx(math.sqrt(50.0))


def test_welford_constant_series():
    w = WelfordState()
    for _ in range(100):
        w.update(42.0)
    assert w.variance == pytest.approx(0.0)
    assert w.mean == pytest.approx(42.0)


def test_welford_known_std():
    """std of [0, 10, 20, 30, 40] = 15.81..."""
    w = WelfordState()
    for v in [0, 10, 20, 30, 40]:
        w.update(float(v))
    assert w.std == pytest.approx(15.811, abs=0.01)


# ─── HierEBAllocator – basic initialisation ───────────────────────────────────

def test_allocator_negative_epsilon_raises():
    with pytest.raises(ValueError, match="positive"):
        HierEBAllocator(epsilon_h=-0.1)


def test_allocator_get_delta_unknown_plug():
    alloc = HierEBAllocator(epsilon_h=0.05)
    assert alloc.get_delta(999) == math.inf


# ─── Water-filling correctness ────────────────────────────────────────────────

# Simple 1-house, 1-household structure
SIMPLE_STRUCTURE = {
    1: {
        0: [101, 102, 103]   # house 1, hh 0, 3 plugs
    }
}


def seed_allocator(
    alloc: HierEBAllocator,
    plug_sigmas: dict[int, float],
) -> None:
    """
    Seed effective rolling variance samples.

    A repeated residual with absolute value s gives rolling variance s² and
    sigma s under the allocator's zero-mean effective-residual contract.
    """
    for plug_uid, sigma in plug_sigmas.items():
        state = alloc.get_or_create_plug(plug_uid)
        for _ in range(alloc.min_variance_samples):
            state.record_transmitted(sigma)


def test_water_fill_proportional_to_sigma():
    """
    Plug with higher sigma must get higher delta (water-filling).
    σ_101=10, σ_102=30, σ_103=60 → δ_103 > δ_102 > δ_101
    """
    alloc = HierEBAllocator(epsilon_h=100.0)  # large epsilon for easy inspection
    seed_allocator(alloc, {101: 10.0, 102: 30.0, 103: 60.0})

    deltas = alloc.reallocate(SIMPLE_STRUCTURE)

    assert deltas[103] > deltas[102] > deltas[101]


def test_water_fill_sums_to_epsilon():
    """
    For a single household: sum of deltas should equal epsilon_h.
    No positive min-delta clipping is allowed in the baseline.
    """
    epsilon_h = 100.0
    alloc = HierEBAllocator(epsilon_h=epsilon_h)
    seed_allocator(alloc, {101: 10.0, 102: 20.0, 103: 30.0})

    deltas = alloc.reallocate(SIMPLE_STRUCTURE)

    total = sum(deltas.values())
    assert total == pytest.approx(epsilon_h, abs=0.01)


def test_water_fill_equal_sigma_equal_delta():
    """When all plugs have equal sigma, deltas must be equal."""
    epsilon_h = 90.0
    alloc = HierEBAllocator(epsilon_h=epsilon_h)
    seed_allocator(alloc, {101: 5.0, 102: 5.0, 103: 5.0})

    deltas = alloc.reallocate(SIMPLE_STRUCTURE)

    assert deltas[101] == pytest.approx(30.0, abs=0.01)
    assert deltas[102] == pytest.approx(30.0, abs=0.01)
    assert deltas[103] == pytest.approx(30.0, abs=0.01)


def test_no_min_delta_floor_keeps_tiny_budget_bound():
    """A positive min delta would blow this budget; baseline must not use one."""
    structure = {1: {0: list(range(1000, 1100))}}
    alloc = HierEBAllocator(epsilon_h=0.001, min_variance_samples=1)
    seed_allocator(alloc, {plug_uid: 0.0001 for plug_uid in structure[1][0]})

    deltas = alloc.reallocate(structure)

    assert sum(deltas.values()) <= 0.001 + 1e-12
    assert max(deltas.values()) < 0.1


# ─── Two-stage allocation ─────────────────────────────────────────────────────

TWO_HH_STRUCTURE = {
    1: {
        0: [101, 102],   # hh 0: 2 plugs, low sigma
        1: [201, 202],   # hh 1: 2 plugs, high sigma
    }
}


def test_two_stage_hh_budget_proportional_to_sigma():
    """
    HH with higher total sigma should receive more of epsilon_h.
    hh0: σ_101=5, σ_102=5 → total 10
    hh1: σ_201=20, σ_202=20 → total 40
    hh1 should receive 4× the budget of hh0.
    """
    epsilon_h = 100.0
    alloc = HierEBAllocator(epsilon_h=epsilon_h)
    seed_allocator(alloc, {101: 5.0, 102: 5.0, 201: 20.0, 202: 20.0})

    deltas = alloc.reallocate(TWO_HH_STRUCTURE)

    # HH0 budget = 100 × 10/50 = 20 → each plug gets 10
    # HH1 budget = 100 × 40/50 = 80 → each plug gets 40
    assert deltas[101] == pytest.approx(10.0, abs=0.1)
    assert deltas[102] == pytest.approx(10.0, abs=0.1)
    assert deltas[201] == pytest.approx(40.0, abs=0.1)
    assert deltas[202] == pytest.approx(40.0, abs=0.1)


def test_two_stage_total_budget_conserved():
    """Sum of all deltas should equal epsilon_h."""
    epsilon_h = 50.0
    alloc = HierEBAllocator(epsilon_h=epsilon_h)
    seed_allocator(alloc, {101: 5.0, 102: 15.0, 201: 10.0, 202: 20.0})

    deltas = alloc.reallocate(TWO_HH_STRUCTURE)

    assert sum(deltas.values()) == pytest.approx(epsilon_h, abs=0.1)


# ─── Censored correction ──────────────────────────────────────────────────────

def test_censored_correction_increases_sigma():
    """
    Adding censored observations should never collapse sigma to 0.
    A plug with no transmitted obs but many censored ones should still
    have a positive sigma estimate.
    """
    alloc = HierEBAllocator(epsilon_h=0.05)
    state = alloc.get_or_create_plug(plug_uid=1)

    # No transmitted, only suppressed
    for _ in range(100):
        state.record_suppressed(delta_used=10.0)

    sigma = state.estimate_sigma()
    assert state.variance == pytest.approx((10.0 ** 2) / 3.0)
    assert sigma == pytest.approx(math.sqrt((10.0 ** 2) / 3.0))


def test_transmitted_updates_welford():
    alloc = HierEBAllocator(epsilon_h=0.05)
    alloc.update_from_transmitted(plug_uid=1, residual=5.0)
    alloc.update_from_transmitted(plug_uid=1, residual=15.0)

    state = alloc._plug_states[1]
    assert state.welford.n == 2
    assert state.welford.mean == pytest.approx(10.0)


def test_censored_buffer_clears_after_realloc():
    alloc = HierEBAllocator(epsilon_h=0.05)
    state = alloc.get_or_create_plug(plug_uid=1)
    state.record_suppressed(10.0)
    state.record_suppressed(10.0)

    assert state.effective_sample_count == 2
    alloc.reallocate({0: {0: [1]}})
    assert state.effective_sample_count == 2


# ─── Delta persistence ────────────────────────────────────────────────────────

def test_get_delta_after_realloc():
    alloc = HierEBAllocator(epsilon_h=100.0)
    seed_allocator(alloc, {101: 10.0, 102: 10.0})
    alloc.reallocate({1: {0: [101, 102]}})

    assert alloc.get_delta(101) == pytest.approx(50.0, abs=0.1)
    assert alloc.get_delta(102) == pytest.approx(50.0, abs=0.1)


def test_sigma_floor_fixed_from_warmup_and_zero_variance_gets_weight():
    alloc = HierEBAllocator(
        epsilon_h=30.0,
        sigma_floor_by_house={1: 2.5},
        min_variance_samples=30,
    )
    for plug_uid in [101, 102, 103]:
        for _ in range(30):
            alloc.update_from_transmitted(plug_uid, 0.0)

    deltas = alloc.reallocate(SIMPLE_STRUCTURE)

    assert alloc.sigma_floor_for_house(1) == pytest.approx(2.5)
    assert deltas[101] == pytest.approx(10.0)
    assert deltas[102] == pytest.approx(10.0)
    assert deltas[103] == pytest.approx(10.0)

    alloc.update_from_transmitted(101, 100.0)
    alloc.reallocate(SIMPLE_STRUCTURE)
    assert alloc.sigma_floor_for_house(1) == pytest.approx(2.5)


def test_inactive_plugs_get_zero_and_do_not_consume_budget():
    alloc = HierEBAllocator(epsilon_h=90.0, min_variance_samples=1)
    seed_allocator(alloc, {101: 1.0, 102: 1.0, 103: 1.0})

    deltas = alloc.reallocate(
        SIMPLE_STRUCTURE,
        active_plug_uids_by_house={1: {101, 103}},
    )

    assert deltas[101] == pytest.approx(45.0)
    assert deltas[102] == pytest.approx(0.0)
    assert deltas[103] == pytest.approx(45.0)
    assert sum(deltas.values()) == pytest.approx(90.0)


def test_cold_start_uses_household_then_house_median_weight():
    structure = {1: {0: [101, 102], 1: [201]}}
    alloc = HierEBAllocator(epsilon_h=90.0, min_variance_samples=30)
    for _ in range(30):
        alloc.update_from_transmitted(101, 4.0)

    deltas = alloc.reallocate(structure)

    assert deltas[101] == pytest.approx(30.0)
    assert deltas[102] == pytest.approx(30.0)
    assert deltas[201] == pytest.approx(30.0)


def test_censored_contribution_uses_old_delta_used_squared_over_three():
    state = PlugVarianceState(plug_uid=1)

    state.record_suppressed(6.0)

    assert state.effective_sample_count == 1
    assert state.variance == pytest.approx(12.0)
    assert state.estimate_sigma() == pytest.approx(math.sqrt(12.0))


def test_rolling_window_keeps_1000_effective_samples():
    state = PlugVarianceState(plug_uid=1, rolling_window_size=1000)

    for _ in range(1000):
        state.record_transmitted(1.0)
    for _ in range(5):
        state.record_transmitted(3.0)

    assert state.effective_sample_count == 1000
    assert state.variance == pytest.approx(((995 * 1.0) + (5 * 9.0)) / 1000)


def test_threshold_trace_records_timing_and_version():
    alloc = HierEBAllocator(epsilon_h=10.0, min_variance_samples=1)
    seed_allocator(alloc, {101: 1.0, 102: 1.0, 103: 1.0})

    alloc.reallocate(
        SIMPLE_STRUCTURE,
        allocation_time=2000,
        effective_after_time=2000,
        threshold_version=7,
    )

    trace = alloc.latest_trace_by_house[1]
    assert trace.allocation_time == 2000
    assert trace.effective_after_time == 2000
    assert trace.threshold_version == 7
    assert trace.budget_sum <= 10.0 + 1e-12
