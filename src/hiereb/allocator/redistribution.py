"""Deterministic capped weighted water filling (MATH_SPEC section 11)."""

from __future__ import annotations

from collections.abc import Hashable, Mapping, Sequence


def waterfill[K: Hashable](
    amount: float,
    keys: Sequence[K],
    weights: Mapping[K, float],
    current: dict[K, float],
    caps: Mapping[K, float],
    tolerance: float,
    max_rounds: int,
) -> tuple[float, int]:
    """Add at most `amount` without exceeding caps; order is caller-provided and stable."""
    remaining = max(amount, 0.0)
    rounds = 0
    while remaining > tolerance and rounds < max_rounds:
        eligible = [key for key in keys if caps[key] - current[key] > tolerance]
        if not eligible:
            break
        total_weight = sum(max(weights.get(key, 0.0), 0.0) for key in eligible)
        shares = (
            {key: max(weights.get(key, 0.0), 0.0) / total_weight for key in eligible}
            if total_weight > 0
            else {key: 1.0 / len(eligible) for key in eligible}
        )
        distributed = 0.0
        before = remaining
        for key in eligible:
            addition = min(before * shares[key], caps[key] - current[key])
            current[key] += addition
            distributed += addition
        remaining = max(remaining - distributed, 0.0)
        rounds += 1
        if distributed <= tolerance:
            break
    return remaining, rounds
