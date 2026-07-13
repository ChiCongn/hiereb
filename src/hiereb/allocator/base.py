"""Common allocator interface and dispatch."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from hiereb.config import Mode, TrueHierarchyConfig
from hiereb.domain.models import PlugKey


@dataclass(frozen=True, slots=True)
class AllocationRequest:
    """Immutable allocation snapshot; allocators cannot inspect raw future events."""

    timestamp: datetime
    house_budget: float
    active_plugs: tuple[PlugKey, ...]
    scores: dict[PlugKey, float]
    hierarchy: TrueHierarchyConfig
    caps: dict[PlugKey, float] = field(default_factory=dict)
    tolerance: float = 1e-9
    max_iterations_extra: int = 10


@dataclass(frozen=True, slots=True)
class AllocationResult:
    thresholds: dict[PlugKey, float]
    household_budgets: dict[int, float]
    caps: dict[PlugKey, float]
    used_budget: float
    unused_budget: float
    cap_hit_count: int
    redistribution_rounds: int
    effective_after: datetime


def allocate(mode: Mode, request: AllocationRequest) -> AllocationResult:
    """Dispatch through the shared allocator interface."""
    from hiereb.allocator.baselines import flat_variance, full_tx, legacy_two_stage, uniform
    from hiereb.allocator.hierarchical import true_hierarchical

    if mode == "full_tx":
        return full_tx(request)
    if mode == "uniform":
        return uniform(request)
    if mode == "flat_variance":
        return flat_variance(request)
    if mode == "legacy_two_stage":
        return legacy_two_stage(request)
    if mode == "true_hierarchical":
        return true_hierarchical(request, capped=False)
    if mode == "true_hierarchical_cap":
        return true_hierarchical(request, capped=True)
    raise ValueError(f"unknown mode: {mode}")


def result_from_thresholds(
    request: AllocationRequest,
    thresholds: dict[PlugKey, float],
    household_budgets: dict[int, float] | None = None,
    *,
    caps: dict[PlugKey, float] | None = None,
    cap_hit_count: int = 0,
    redistribution_rounds: int = 0,
) -> AllocationResult:
    used = sum(thresholds.values())
    unused = max(request.house_budget - used, 0.0)
    return AllocationResult(
        thresholds=thresholds,
        household_budgets=household_budgets or _household_totals(thresholds),
        caps=caps or {},
        used_budget=used,
        unused_budget=unused,
        cap_hit_count=cap_hit_count,
        redistribution_rounds=redistribution_rounds,
        effective_after=request.timestamp,
    )


def _household_totals(thresholds: dict[PlugKey, float]) -> dict[int, float]:
    totals: dict[int, float] = {}
    for (household, _), value in thresholds.items():
        totals[household] = totals.get(household, 0.0) + value
    return totals

