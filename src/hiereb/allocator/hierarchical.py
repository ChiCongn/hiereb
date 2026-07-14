"""True household-aware hierarchy and capped redistribution."""

from __future__ import annotations

from collections import defaultdict

import numpy as np

from hiereb.allocator.base import (
    AllocationRequest,
    AllocationResult,
    result_from_thresholds,
)
from hiereb.allocator.redistribution import waterfill
from hiereb.domain.models import PlugKey


def hierarchy_shares(
    request: AllocationRequest,
) -> tuple[dict[int, float], dict[PlugKey, float], dict[int, float]]:
    """Compute household, conditional plug, and household-risk shares (MATH_SPEC section 9)."""

    # Group active plugs by their household ID.
    groups: defaultdict[int, list[PlugKey]] = defaultdict(list)
    for plug in request.active_plugs:
        groups[plug[0]].append(plug)

    # Sort household IDs to make the allocation determini
    households = sorted(groups)
    if not households:
        return {}, {}, {}

    # Use the median score across all active plugs as a scale factor
    # for the household plug-count term.
    #
    # Multiplying log(1 + number_of_plugs) by this median keeps the
    # count-based score on roughly the same scale as the plug scores.
    median_house = float(np.median([request.scores[p] for p in request.active_plugs]))
    household_score_weights = request.hierarchy.household_score
    household_risk: dict[int, float] = {}

    for household in households:
        household_scores = [request.scores[plug] for plug in groups[household]]
        plug_count_score = float(np.log1p(len(household_scores))) * median_house

        household_risk[household] = (
            household_score_weights.lambda_mean * float(np.mean(household_scores))
            + household_score_weights.lambda_max * max(household_scores)
            + household_score_weights.lambda_count * plug_count_score
        )

    # Normalize household risks so that they form a probability-like
    # distribution whose values sum to 1.
    total_household_risk = sum(household_risk.values())
    if total_household_risk > 0:
        risk_share = {
            household: household_risk[household] / total_household_risk for household in households
        }
    else:
        # If all household risks are zero, no household can be preferred
        # based on risk, so fall back to equal shares.
        equal_household_share = 1.0 / len(households)
        risk_share = {household: equal_household_share for household in households}

    # Mix equal household fairness with risk-based allocation.
    #
    # alpha = 0:
    #     Allocation is entirely risk-based.
    #
    # alpha = 1:
    #     Every household receives an equal share.
    #
    # 0 < alpha < 1:
    #     Convex combination of fairness and risk.
    alpha = request.hierarchy.household_fairness_alpha
    equal_household_share = 1.0 / len(households)
    household_share = {
        household: alpha * equal_household_share + (1.0 - alpha) * risk_share[household]
        for household in households
    }

    # Compute the conditional share of each plug inside its household.
    #
    # beta behaves similarly to alpha:
    #
    # beta = 0:
    #     Plug allocation is entirely proportional to plug scores.
    #
    # beta = 1:
    #     All plugs in the same household receive equal shares.
    beta = request.hierarchy.plug_fairness_beta
    plug_share: dict[PlugKey, float] = {}

    for household in households:
        plugs = groups[household]
        household_score_sum = sum(request.scores[p] for p in plugs)

        equal_plug_share = 1.0 / len(plugs)
        for plug in plugs:
            if household_score_sum > 0:
                score_based_share = request.scores[plug] / household_score_sum
            else:
                # If all plug scores in this household are zero,
                # fall back to equal allocation.
                score_based_share = equal_plug_share

            plug_share[plug] = beta * equal_plug_share + (1.0 - beta) * score_based_share
    return household_share, plug_share, risk_share


def true_hierarchical(request: AllocationRequest, *, capped: bool) -> AllocationResult:
    """
    Allocate the total house budget through a true two-level hierarchy.

    Allocation flow:
    total house budget -> household budgets -> plug thresholds

    When capped=False:
        Return the raw hierarchical allocation.

    When capped=True:
        Apply per-plug caps and redistribute unused budget:
            1. Within the original household.
            2. Across households when local redistribution is insufficient.
    """

    # Compute the household-level and plug-level allocation shares.
    household_share, plug_share, risk_share = hierarchy_shares(request)

    # Convert household shares into absolute household budgets.
    # B_h = total_budget * household_share[h]
    household_budgets = {
        household: request.house_budget * share for household, share in household_share.items()
    }

    # Allocate each household's budget to its plugs.
    #
    # raw_threshold[i] = household_budget[household(i)] * conditional_plug_share[i]
    raw_thresholds = {
        plug: household_budgets[plug[0]] * plug_share[plug] for plug in request.active_plugs
    }

    # Without caps, the raw hierarchical thresholds are final.
    if not capped:
        return result_from_thresholds(request, raw_thresholds, household_budgets)

    # With caps enabled, clamp thresholds and redistribute
    return _capped(request, raw_thresholds, household_budgets, plug_share, risk_share)


def _capped(
    request: AllocationRequest,
    raw: dict[PlugKey, float],
    household_budgets: dict[int, float],
    plug_share: dict[PlugKey, float],
    risk_share: dict[int, float],
) -> AllocationResult:
    """
    Apply per-plug caps and redistribute unused budget hierarchically.

    Redistribution order:
        1. Clamp every raw threshold by its plug cap.
        2. Redistribute missing budget inside each household.
        3. Spill any globally unused budget across households.
        4. Redistribute each household's spill allocation to its plugs.

    The threshold dictionary is updated in place by waterfill().
    """

    # Every active plug must have a corresponding cap.
    #
    # Failing early avoids silently applying an undefined or inconsistent
    # default cap to an active plug.
    missing_caps = set(request.active_plugs) - set(request.caps)
    if missing_caps:
        raise ValueError(f"missing caps for active plugs: {sorted(missing_caps)}")

    # Apply the initial per-plug cap:
    #
    # threshold_i = min(raw_threshold_i, cap_i)
    thresholds = {plug: min(raw[plug], request.caps[plug]) for plug in request.active_plugs}

    # Record how much budget remains in use immediately after clamping.
    #
    # This value is later used to measure how much budget was recovered
    # by within-household redistribution.
    initial_capped_used = sum(thresholds.values())

    # Group plugs by household for local redistribution.
    groups: defaultdict[int, list[PlugKey]] = defaultdict(list)
    for plug in request.active_plugs:
        household_id = plug[0]
        groups[household_id].append(plug)

    # Water-filling should normally converge after plugs successively
    # reach their caps. The extra rounds provide a small safety margin.
    max_plug_rounds = len(request.active_plugs) + request.max_iterations_extra
    total_redistribution_rounds = 0

    # ------------------------------------------------------------------
    # Phase 1: redistribute unused budget inside each household.
    # ------------------------------------------------------------------
    #
    # A household should first retain ownership of its original budget.
    # If one plug is capped below its raw allocation, another plug in the
    # same household receives the unused portion whenever capacity exists.
    for household in sorted(groups):
        plugs = sorted(groups[household])
        current_household_usage = sum(thresholds[plug] for plug in plugs)

        local_missing_budget = household_budgets[household] - current_household_usage

        # waterfill() mutates `thresholds` in place.
        # The remaining local budget is intentionally ignored here.
        # Any budget that cannot remain local will be recomputed later
        # as part of the global spill amount.
        _, used_rounds = waterfill(
            local_missing_budget,
            plugs,
            plug_share,
            thresholds,
            request.caps,
            request.tolerance,
            max_plug_rounds,
        )
        total_redistribution_rounds += used_rounds

    # Total budget in use after all local household redistributions.
    after_within_used = sum(thresholds.values())

    # ------------------------------------------------------------------
    # Phase 2: spill unused budget across households.
    # ------------------------------------------------------------------
    current_total_used = sum(thresholds.values())

    # Floating-point arithmetic may produce a tiny negative value when
    # usage is numerically slightly larger than the budget, so clamp at 0.
    global_spill_budget = max(request.house_budget - current_total_used, 0.0)

    households = sorted(groups)

    # At the household spill level, `group_current[h]` represents only
    # the additional spill assigned to household h, not its original
    # household budget.
    group_current = {household: 0.0 for household in households}

    # Aggregate remaining capacity of each household.
    # This is the maximum additional spill that the household can accept
    # without any of its plugs exceeding their caps.
    group_remaining_capacity = {
        household: sum(request.caps[p] - thresholds[p] for p in groups[household])
        for household in households
    }

    # Allocate global spill across households according to pure risk share.
    # The initial household allocation already includes fairness via alpha.
    # Using pure risk directs extra capacity toward households with the
    # greatest measured need.
    remaining_spill, household_rounds = waterfill(
        global_spill_budget,
        households,
        risk_share,
        group_current,
        group_remaining_capacity,
        request.tolerance,
        len(households) + request.max_iterations_extra,
    )

    total_redistribution_rounds += household_rounds

    # Distribute each household's assigned spill among its plugs.
    for household in households:
        plugs = sorted(groups[household])

        plug_level_leftover, used_rounds = waterfill(
            group_current[household],
            plugs,
            plug_share,
            thresholds,
            request.caps,
            request.tolerance,
            max_plug_rounds,
        )

        total_redistribution_rounds += used_rounds
        # In theory, the household-level capacity calculation should make
        # this leftover approximately zero. Keep it for defensive handling
        # of numerical tolerance or iteration-limit effects.
        remaining_spill += plug_level_leftover

    # result_from_thresholds() recomputes the final unused budget directly
    # from the final thresholds, which is more reliable than accumulated
    # intermediate leftovers.
    _ = remaining_spill

    # Count plugs that are at or sufficiently close to their caps.
    #
    # Tolerance is used instead of exact equality because thresholds are
    # floating-point values.
    cap_hit_count = sum(
        1
        for plug, threshold in thresholds.items()
        if request.caps[plug] - threshold <= request.tolerance
    )

    final_used_budget = sum(thresholds.values())

    return result_from_thresholds(
        request,
        thresholds,
        caps=dict(request.caps),
        cap_hit_count=cap_hit_count,
        redistribution_rounds=total_redistribution_rounds,
        # Budget recovered by moving capped allocations to other plugs
        # within the same household.
        within_household_redistributed_budget=max(after_within_used - initial_capped_used, 0.0),
        # Additional budget recovered only after allowing budget to move
        # from one household to another.
        cross_household_spill_budget=max(final_used_budget - after_within_used, 0.0),
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
    precomputed_quantiles: dict[PlugKey, float] | None = None,
    sample_counts: dict[PlugKey, int] | None = None,
) -> dict[PlugKey, float]:
    """
    Build per-plug caps using warm-up absolute residuals only.

    For a plug with enough warm-up samples:

        cap_i = min(
            max_house_fraction * house_budget,
            max(
                multiplier * residual_quantile_i,
                sigma_floor,
            ),
        )

    For a plug with insufficient samples:
        cap_i = max_house_fraction * house_budget

    Using warm-up-only information prevents evaluation-data leakage.

    Mathematical reference:
        MATH_SPEC section 10.
    """

    # This value serves two purposes:
    #   1. It is the fallback cap when a plug lacks enough samples.
    #   2. It is the maximum cap allowed for any individual plug.
    fallback_cap = max_house_fraction * house_budget

    caps: dict[PlugKey, float] = {}
    for plug in active_plugs:
        # Missing residual history is treated as an empty sequence.
        samples = warmup_absolute_residuals.get(plug, ())

        # sample_counts can be supplied separately when the pipeline stores
        # only precomputed statistics rather than every raw residual.
        if sample_counts is not None:
            sample_count = sample_counts.get(plug, len(samples))
        else:
            sample_count = len(samples)

        if sample_count >= min_samples:
            # Prefer a precomputed quantile when one is available.
            #
            # This supports streaming or memory-efficient pipelines that
            # do not retain the full residual sequence.
            if precomputed_quantiles is not None and plug in precomputed_quantiles:
                residual_quantile = precomputed_quantiles[plug]
            else:
                residual_quantile = float(np.quantile(samples, quantile))

            # Apply:
            # - multiplier: safety margin around the residual quantile;
            # - sigma_floor: minimum usable cap;
            # - fallback_cap: maximum fraction of total house budget.
            caps[plug] = min(
                fallback_cap,
                max(
                    multiplier * residual_quantile,
                    sigma_floor,
                ),
            )
        else:
            # With too few samples, an empirical quantile may be unstable.
            # Use the configured house-budget fraction instead.
            caps[plug] = fallback_cap

    return caps
