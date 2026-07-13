from __future__ import annotations

from datetime import UTC, datetime

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from hiereb.allocator.base import AllocationRequest, allocate
from hiereb.config import AppConfig
from hiereb.residual.state import ResidualState


def _request(
    config: AppConfig,
    scores: dict[tuple[int, int], float],
    budget: float = 10.0,
    caps: dict[tuple[int, int], float] | None = None,
) -> AllocationRequest:
    return AllocationRequest(
        timestamp=datetime(2020, 1, 1, tzinfo=UTC),
        house_budget=budget,
        active_plugs=tuple(sorted(scores)),
        scores=scores,
        hierarchy=config.true_hierarchy,
        caps=caps or {},
        tolerance=1e-9,
    )


def test_exact_and_proxy_suppressed_branches() -> None:
    exact = ResidualState("exact", 10, 0.1)
    exact.observe_suppressed((0, 0), 3.0, exact_residual=2.0)
    assert exact.score((0, 0)) == 2.0
    proxy = ResidualState("uniform_proxy", 10, 0.1)
    proxy.observe_suppressed((0, 0), 3.0)
    assert proxy.score((0, 0)) == pytest.approx(3.0 / (3.0**0.5))
    with pytest.raises(ValueError):
        exact.observe_suppressed((0, 0), 1.0)


def test_baseline_formulas_and_legacy_equivalence(config: AppConfig) -> None:
    request = _request(config, {(0, 0): 1.0, (0, 1): 3.0, (1, 0): 6.0})
    uniform = allocate("uniform", request)
    flat = allocate("flat_variance", request)
    legacy = allocate("legacy_two_stage", request)
    assert all(value == pytest.approx(10.0 / 3.0) for value in uniform.thresholds.values())
    assert flat.thresholds == pytest.approx(legacy.thresholds, rel=1e-12, abs=1e-12)
    assert sum(flat.thresholds.values()) == pytest.approx(10.0)


def test_true_hierarchy_is_group_sensitive(config: AppConfig) -> None:
    request = _request(config, {(0, 0): 1.0, (0, 1): 1.0, (1, 0): 4.0})
    flat = allocate("flat_variance", request)
    hierarchical = allocate("true_hierarchical", request)
    assert hierarchical.thresholds != flat.thresholds
    assert sum(hierarchical.thresholds.values()) == pytest.approx(10.0)


def test_cap_redistribution_and_explicit_unused(config: AppConfig) -> None:
    scores = {(0, 0): 1.0, (0, 1): 3.0, (1, 0): 6.0}
    caps = {(0, 0): 1.0, (0, 1): 2.0, (1, 0): 3.0}
    result = allocate("true_hierarchical_cap", _request(config, scores, caps=caps))
    assert all(result.thresholds[p] <= caps[p] + 1e-9 for p in scores)
    assert result.used_budget == pytest.approx(6.0)
    assert result.unused_budget == pytest.approx(4.0)
    assert result.used_budget + result.unused_budget == pytest.approx(10.0)
    assert result.redistribution_rounds <= len(scores) * 5 + 20


@given(
    st.lists(
        st.floats(min_value=0.0, max_value=1e6, allow_nan=False, allow_infinity=False),
        min_size=1,
        max_size=20,
    ),
    st.floats(min_value=0.0, max_value=1e6, allow_nan=False, allow_infinity=False),
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_allocator_properties(config: AppConfig, values: list[float], budget: float) -> None:
    scores = {(index % 3, index): value for index, value in enumerate(values)}
    request = _request(config, scores, budget)
    flat = allocate("flat_variance", request)
    legacy = allocate("legacy_two_stage", request)
    assert flat.thresholds == pytest.approx(legacy.thresholds, rel=1e-12, abs=1e-12)
    assert all(value >= 0 for value in flat.thresholds.values())
    assert sum(flat.thresholds.values()) <= budget + max(1e-7, budget * 1e-12)


@given(
    st.lists(st.floats(min_value=0.01, max_value=100), min_size=1, max_size=12),
    st.floats(min_value=0.0, max_value=1000),
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_capped_properties(config: AppConfig, values: list[float], budget: float) -> None:
    scores = {(index % 3, index): value for index, value in enumerate(values)}
    caps = {key: value * 0.1 for key, value in scores.items()}
    result = allocate("true_hierarchical_cap", _request(config, scores, budget, caps))
    assert all(0 <= result.thresholds[p] <= caps[p] + 1e-7 for p in scores)
    assert result.used_budget <= budget + 1e-7
    assert result.used_budget + result.unused_budget == pytest.approx(budget, abs=1e-7)
