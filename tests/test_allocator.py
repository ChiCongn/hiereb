"""
Unit tests for src/ml_hiereb/allocator.py

Tests the core HierEB water-filling and censored correction logic.
No Kafka or Docker required.

Run: pytest tests/test_allocator.py -v
"""
from __future__ import annotations

import math

import pytest

from src.ml_hiereb.allocator import HierEBAllocator, WelfordState


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
    Seed sigma estimates via Welford state.

    For a target sigma s, we add two Welford samples [0, s*√2].
    This gives: mean=s*√2/2, M2=s², sample_var=s², std=s. Exact.
    """
    import math
    for plug_uid, sigma in plug_sigmas.items():
        state = alloc.get_or_create_plug(plug_uid)
        # Two samples that produce exactly sigma
        state.welford.update(0.0)
        state.welford.update(sigma * math.sqrt(2))


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
    (Assuming sigma >> minimum floor 0.1W so no clipping.)
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


def test_water_fill_floor_at_0_1():
    """Very small sigma still gets minimum delta of 0.1W."""
    alloc = HierEBAllocator(epsilon_h=0.001)  # tiny epsilon
    seed_allocator(alloc, {101: 0.0001, 102: 0.0001, 103: 0.0001})

    deltas = alloc.reallocate(SIMPLE_STRUCTURE)

    for delta in deltas.values():
        assert delta >= 0.1


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
        state.record_suppressed(delta=10.0)

    sigma = state.estimate_sigma()
    assert sigma > 0.0


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
    state.sigma = 5.0
    state.record_suppressed(10.0)
    state.record_suppressed(10.0)

    assert len(state.censored_deltas) == 2
    alloc.reallocate({0: {0: [1]}})
    assert len(state.censored_deltas) == 0


# ─── Delta persistence ────────────────────────────────────────────────────────

def test_get_delta_after_realloc():
    alloc = HierEBAllocator(epsilon_h=100.0)
    seed_allocator(alloc, {101: 10.0, 102: 10.0})
    alloc.reallocate({1: {0: [101, 102]}})

    assert alloc.get_delta(101) == pytest.approx(50.0, abs=0.1)
    assert alloc.get_delta(102) == pytest.approx(50.0, abs=0.1)