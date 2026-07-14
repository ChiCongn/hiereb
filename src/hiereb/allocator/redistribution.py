"""Deterministic capped weighted water filling (MATH_SPEC section 11)."""

from __future__ import annotations

from collections.abc import Hashable, Mapping, Sequence


def waterfill[K: Hashable](
    amount: float,
    keys: Sequence[K],  # plugs_of_one_household
    weights: Mapping[K, float],  # plug_share
    current: dict[K, float],  # thresholds
    caps: Mapping[K, float],  # plug_caps
    tolerance: float,
    max_rounds: int,
) -> tuple[float, int]:
    """Add at most `amount` without exceeding caps; order is caller-provided and stable."""
    remaining = max(amount, 0.0)
    rounds = 0

    while remaining > tolerance and rounds < max_rounds:
        eligible = [key for key in keys if caps[key] - current[key] > tolerance]

        # No remaining capacity means the undistributed amount must be
        # returned to the caller.
        if not eligible:
            break

        # Ignore negative weights and treat missing weights as zero.
        #
        # Weight normalization is performed only over currently eligible
        # keys because capped keys must no longer receive allocation.
        total_weight = sum(max(weights.get(key, 0.0), 0.0) for key in eligible)
        if total_weight > 0:
            # Normalize eligible weights into shares that sum to one.
            shares = {key: (max(weights.get(key, 0.0), 0.0) / total_weight) for key in eligible}
        else:
            # If all eligible weights are zero, no weighted preference is
            # available, so distribute equally among eligible keys.
            equal_share = 1.0 / len(eligible)
            shares = {key: equal_share for key in eligible}

        # Track the actual progress made during this round.
        distributed = 0.0
        round_start_remaining = remaining

        for key in eligible:
            # Amount this key would receive if no cap restricted it.
            weighted_request = round_start_remaining * shares[key]

            # Maximum amount the key can still accept.
            remaining_capacity = caps[key] - current[key]

            # Never exceed the key's cap.
            addition = min(
                weighted_request,
                remaining_capacity,
            )

            current[key] += addition
            distributed += addition

        # Remove the amount successfully allocated in this round.
        #
        # Clamp at zero to avoid tiny negative values from floating-point arithmetic.
        remaining = max(remaining - distributed, 0.0)
        rounds += 1

        if distributed <= tolerance:
            break

    return remaining, rounds
