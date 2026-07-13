"""True household-aware hierarchy and capped redistribution."""

from __future__ import annotations

from collections import defaultdict

import numpy as np

from hiereb.allocator.base import AllocationRequest, AllocationResult, result_from_thresholds
from hiereb.allocator.redistribution import waterfill
from hiereb.domain.models import PlugKey


def hierarchy_shares(
    request: AllocationRequest,
) -> tuple[dict[int, float], dict[PlugKey, float], dict[int, float]]:
    """Compute household, conditional plug, and household-risk shares (MATH_SPEC section 9)."""
    groups: defaultdict[int, list[PlugKey]] = defaultdict(list)
    for plug in request.active_plugs:
        groups[plug[0]].append(plug)
    households = sorted(groups)
    if not households:
        return {}, {}, {}
    median_house = float(np.median([request.scores[p] for p in request.active_plugs]))
    lambdas = request.hierarchy.household_score
    risk: dict[int, float] = {}
    for household in households:
        scores = [request.scores[p] for p in groups[household]]
        count_score = float(np.log1p(len(scores))) * median_house
        risk[household] = (
            lambdas.lambda_mean * float(np.mean(scores))
            + lambdas.lambda_max * max(scores)
            + lambdas.lambda_count * count_score
        )
    risk_total = sum(risk.values())
    risk_share = (
        {h: risk[h] / risk_total for h in households}
        if risk_total > 0
        else {h: 1.0 / len(households) for h in households}
    )
    alpha = request.hierarchy.household_fairness_alpha
    household_share = {
        h: alpha / len(households) + (1.0 - alpha) * risk_share[h] for h in households
    }
    beta = request.hierarchy.plug_fairness_beta
    plug_share: dict[PlugKey, float] = {}
    for household in households:
        plugs = groups[household]
        score_sum = sum(request.scores[p] for p in plugs)
        for plug in plugs:
            variance_share = request.scores[plug] / score_sum if score_sum > 0 else 1.0 / len(plugs)
            plug_share[plug] = beta / len(plugs) + (1.0 - beta) * variance_share
    return household_share, plug_share, risk_share


def true_hierarchical(request: AllocationRequest, *, capped: bool) -> AllocationResult:
    household_share, plug_share, risk_share = hierarchy_shares(request)
    household_budgets = {h: request.house_budget * share for h, share in household_share.items()}
    raw = {
        plug: household_budgets[plug[0]] * plug_share[plug] for plug in request.active_plugs
    }
    if not capped:
        return result_from_thresholds(request, raw, household_budgets)
    return _capped(request, raw, household_budgets, plug_share, risk_share)


def _capped(
    request: AllocationRequest,
    raw: dict[PlugKey, float],
    household_budgets: dict[int, float],
    plug_share: dict[PlugKey, float],
    risk_share: dict[int, float],
) -> AllocationResult:
    missing = set(request.active_plugs) - set(request.caps)
    if missing:
        raise ValueError(f"missing caps for active plugs: {sorted(missing)}")
    thresholds = {plug: min(raw[plug], request.caps[plug]) for plug in request.active_plugs}
    groups: defaultdict[int, list[PlugKey]] = defaultdict(list)
    for plug in request.active_plugs:
        groups[plug[0]].append(plug)
    max_rounds = len(request.active_plugs) + request.max_iterations_extra
    rounds = 0

    # First keep each household's original budget local.
    for household in sorted(groups):
        plugs = sorted(groups[household])
        local_missing = household_budgets[household] - sum(thresholds[p] for p in plugs)
        _, used_rounds = waterfill(
            local_missing,
            plugs,
            plug_share,
            thresholds,
            request.caps,
            request.tolerance,
            max_rounds,
        )
        rounds += used_rounds

    # Then spill globally to households with remaining aggregate capacity.
    used = sum(thresholds.values())
    spill = max(request.house_budget - used, 0.0)
    households = sorted(groups)
    group_current = {h: 0.0 for h in households}
    group_caps = {
        h: sum(request.caps[p] - thresholds[p] for p in groups[h]) for h in households
    }
    remaining, group_rounds = waterfill(
        spill,
        households,
        risk_share,
        group_current,
        group_caps,
        request.tolerance,
        len(households) + request.max_iterations_extra,
    )
    rounds += group_rounds
    for household in households:
        plugs = sorted(groups[household])
        leftover, used_rounds = waterfill(
            group_current[household],
            plugs,
            plug_share,
            thresholds,
            request.caps,
            request.tolerance,
            max_rounds,
        )
        rounds += used_rounds
        remaining += leftover

    # `result_from_thresholds` recomputes unused from the final used budget.
    _ = remaining
    cap_hits = sum(
        1
        for plug, threshold in thresholds.items()
        if request.caps[plug] - threshold <= request.tolerance
    )
    return result_from_thresholds(
        request,
        thresholds,
        caps=dict(request.caps),
        cap_hit_count=cap_hits,
        redistribution_rounds=rounds,
    )


def build_caps(
    active_plugs: tuple[PlugKey, ...],
    warmup_absolute_residuals: dict[PlugKey, tuple[float, ...]],
    house_budget: float,
    sigma_floor: float,
    quantile: float,
    multiplier: float,
    max_house_fraction: float,
    min_samples: int,
) -> dict[PlugKey, float]:
    """Build per-plug warm-up-only caps (MATH_SPEC section 10)."""
    fallback = max_house_fraction * house_budget
    caps: dict[PlugKey, float] = {}
    for plug in active_plugs:
        samples = warmup_absolute_residuals.get(plug, ())
        if len(samples) >= min_samples:
            quantile_value = float(np.quantile(samples, quantile))
            caps[plug] = min(fallback, max(multiplier * quantile_value, sigma_floor))
        else:
            caps[plug] = fallback
    return caps
