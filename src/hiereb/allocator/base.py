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
    max_iterations_extra: int = 10  # Additional loops for water-filling.


@dataclass(frozen=True, slots=True)
class AllocationResult:
    thresholds: dict[PlugKey, float]
    household_budgets: dict[int, float]
    caps: dict[PlugKey, float]
    used_budget: float
    unused_budget: float
    cap_hit_count: int
    redistribution_rounds: int  # Total number of water-filling cycles completed.
    # Budget moved from capped plugs to other plugs in the same household.
    within_household_redistributed_budget: float
    # Budget moved from one household to another after local redistribution.
    cross_household_spill_budget: float
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
    within_household_redistributed_budget: float = 0.0,
    cross_household_spill_budget: float = 0.0,
) -> AllocationResult:
    """
    Construct a normalized AllocationResult from final plug thresholds.

    This helper centralizes result bookkeeping so individual allocators only
    need to compute their thresholds and allocator-specific metadata.
    """

    # The used budget is the sum of all final plug thresholds.
    used_budget = sum(thresholds.values())

    # Clamp at zero to protect against tiny negative values caused by
    # floating-point rounding, for example:
    #
    # used_budget = 100.00000000000001
    # house_budget = 100.0
    unused_budget = max(request.house_budget - used_budget, 0.0)

    # Use explicitly supplied household budgets when available.
    #
    # Otherwise, derive household totals by grouping the final thresholds.
    resolved_household_budgets = (
        household_budgets if household_budgets is not None else _household_totals(thresholds)
    )

    # Allocation modes that do not use caps expose an empty cap mapping.
    resolved_caps = caps if caps is not None else {}

    return AllocationResult(
        thresholds=thresholds,
        household_budgets=resolved_household_budgets,
        caps=resolved_caps,
        used_budget=used_budget,
        unused_budget=unused_budget,
        cap_hit_count=cap_hit_count,
        redistribution_rounds=redistribution_rounds,
        within_household_redistributed_budget=within_household_redistributed_budget,
        cross_household_spill_budget=cross_household_spill_budget,
        effective_after=request.timestamp,
    )


def _household_totals(thresholds: dict[PlugKey, float]) -> dict[int, float]:
    """Sum final plug thresholds by household."""
    totals: dict[int, float] = {}
    for (household, _), value in thresholds.items():
        totals[household] = totals.get(household, 0.0) + value
    return totals
