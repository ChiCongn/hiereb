"""Robust warm-up scale and edge-side drift detectors for Direction 3."""

from __future__ import annotations

from dataclasses import dataclass

from hiereb.config import DriftConfig
from hiereb.domain.models import PlugKey


@dataclass(frozen=True, slots=True)
class DriftScaleModel:
    per_plug: dict[PlugKey, float]
    per_household: dict[int, float]
    house_scale: float
    sigma_floor: float
    min_scale: float

    def scale(self, plug: PlugKey) -> float:
        """Apply the mandatory plug -> household -> house -> floor fallback."""
        candidates = (
            self.per_plug.get(plug),
            self.per_household.get(plug[0]),
            self.house_scale,
            self.sigma_floor,
            1.0,
        )
        for candidate in candidates:
            if candidate is not None and candidate > 0:
                return max(float(candidate), self.min_scale)
        return max(1.0, self.min_scale)


@dataclass(slots=True)
class DriftRuntimeState:
    consecutive_large_residuals: int = 0
    cusum_positive: float = 0.0
    cusum_negative: float = 0.0
    cooldown_remaining: int = 0


@dataclass(frozen=True, slots=True)
class DriftEvaluation:
    trigger: bool
    standardized_innovation: float
    next_consecutive: int
    next_cusum_positive: float
    next_cusum_negative: float
    next_cooldown: int
    suppressed_by_cooldown: bool


def evaluate_drift(
    state: DriftRuntimeState,
    innovation: float,
    scale: float,
    config: DriftConfig,
    *,
    enabled: bool,
) -> DriftEvaluation:
    """Evaluate from pre-event state; the caller commits only after the atomic batch."""
    standardized = innovation / scale
    absolute_standardized = abs(standardized)
    consecutive = state.consecutive_large_residuals
    positive = state.cusum_positive
    negative = state.cusum_negative
    potential_trigger = False
    if config.detector == "zscore_consecutive":
        consecutive = consecutive + 1 if absolute_standardized >= config.zscore.threshold else 0
        potential_trigger = consecutive >= config.zscore.consecutive_count
    elif config.detector == "cusum":
        positive = max(0.0, positive + standardized - config.cusum.kappa)
        negative = max(0.0, negative - standardized - config.cusum.kappa)
        potential_trigger = max(positive, negative) >= config.cusum.threshold_h

    cooldown_active = state.cooldown_remaining > 0
    trigger = enabled and not cooldown_active and potential_trigger
    if trigger or (cooldown_active and potential_trigger):
        consecutive = 0
        positive = 0.0
        negative = 0.0
    next_cooldown = config.cooldown_events if trigger else max(state.cooldown_remaining - 1, 0)
    return DriftEvaluation(
        trigger=trigger,
        standardized_innovation=absolute_standardized,
        next_consecutive=consecutive,
        next_cusum_positive=positive,
        next_cusum_negative=negative,
        next_cooldown=next_cooldown,
        suppressed_by_cooldown=enabled and cooldown_active and potential_trigger,
    )


def commit_drift(state: DriftRuntimeState, evaluation: DriftEvaluation) -> None:
    state.consecutive_large_residuals = evaluation.next_consecutive
    state.cusum_positive = evaluation.next_cusum_positive
    state.cusum_negative = evaluation.next_cusum_negative
    state.cooldown_remaining = evaluation.next_cooldown
