"""Full transmission and flat baseline allocations (MATH_SPEC sections 6-8)."""

from __future__ import annotations

from hiereb.allocator.base import AllocationRequest, AllocationResult, result_from_thresholds


def full_tx(request: AllocationRequest) -> AllocationResult:
    """Assign the threshold to 0 for all plugs"""
    return result_from_thresholds(request, {plug: 0.0 for plug in request.active_plugs})


def uniform(request: AllocationRequest) -> AllocationResult:
    n = len(request.active_plugs)
    value = request.house_budget / n if n else 0.0
    return result_from_thresholds(request, {plug: value for plug in request.active_plugs})


def flat_variance(request: AllocationRequest) -> AllocationResult:
    total = sum(request.scores[plug] for plug in request.active_plugs)
    if total <= 0:
        return uniform(request)
    thresholds = {
        plug: request.house_budget * (request.scores[plug] / total)
        for plug in request.active_plugs
    }
    return result_from_thresholds(request, thresholds)


def legacy_two_stage(request: AllocationRequest) -> AllocationResult:
    """Algebraically flat allocation, deliberately preserving the legacy grouping view."""
    group_weights: dict[int, float] = {}

    for plug in request.active_plugs:
        group_weights[plug[0]] = group_weights.get(plug[0], 0.0) + request.scores[plug]

    house_weight = sum(group_weights.values())

    if house_weight <= 0:
        return uniform(request)

    # MATH_SPEC section 8 simplifies the two stages to this exact flat expression.
    thresholds = {
        plug: request.house_budget * (request.scores[plug] / house_weight)
        for plug in request.active_plugs
    }

    return result_from_thresholds(request, thresholds)
