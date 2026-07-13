"""Exact and uniform-censor rolling residual state (MATH_SPEC section 5)."""

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Mapping

import numpy as np

from hiereb.domain.models import Event, PlugKey
from hiereb.predictor.base import Predictor


class ResidualState:
    """Per-plug rolling second moments with an explicit censored branch."""

    def __init__(
        self,
        estimator: str,
        window_size: int,
        sigma_floor: float,
        initial_squared: Mapping[PlugKey, tuple[float, ...]] | None = None,
    ) -> None:
        if estimator not in {"exact", "uniform_proxy"}:
            raise ValueError(f"unknown estimator: {estimator}")
        if window_size <= 0 or sigma_floor <= 0:
            raise ValueError("window_size and sigma_floor must be positive")
        self.estimator = estimator
        self.window_size = window_size
        self.sigma_floor = sigma_floor
        self._values: dict[PlugKey, deque[float]] = defaultdict(
            lambda: deque(maxlen=self.window_size)
        )
        if initial_squared:
            for plug, values in initial_squared.items():
                self._values[plug].extend(values[-window_size:])

    def observe_transmitted(self, plug: PlugKey, residual: float) -> None:
        self._values[plug].append(residual * residual)

    def observe_suppressed(
        self, plug: PlugKey, threshold_used: float, *, exact_residual: float | None = None
    ) -> None:
        if self.estimator == "exact":
            if exact_residual is None:
                raise ValueError("exact estimator requires residual for suppressed event")
            value = exact_residual * exact_residual
        else:
            value = threshold_used * threshold_used / 3.0
        self._values[plug].append(value)

    def score(self, plug: PlugKey) -> float:
        values = self._values.get(plug)
        if not values:
            return self.sigma_floor
        second_moment = sum(values) / len(values)
        return max(float(np.sqrt(max(second_moment, 0.0))), self.sigma_floor)

    def scores(self, plugs: tuple[PlugKey, ...]) -> dict[PlugKey, float]:
        return {plug: self.score(plug) for plug in plugs}


def fit_warmup_residuals(
    events: tuple[Event, ...],
    predictor: Predictor,
    floor_percentile: float,
) -> tuple[float, dict[PlugKey, tuple[float, ...]], dict[PlugKey, tuple[float, ...]]]:
    """Fit floor and retain exact signed/absolute warm-up residuals for cap statistics."""
    residuals: defaultdict[PlugKey, list[float]] = defaultdict(list)
    for event in events:
        prediction = predictor.predict(event.plug_key, event.timestamp)
        if prediction is not None:
            residuals[event.plug_key].append(event.value - prediction)
    sigmas = [float(np.sqrt(np.mean(np.square(values)))) for values in residuals.values()]
    positive = [sigma for sigma in sigmas if sigma > 0]
    sigma_floor = float(np.percentile(positive, floor_percentile)) if positive else 1.0
    squared = {plug: tuple(value * value for value in values) for plug, values in residuals.items()}
    absolute = {plug: tuple(abs(value) for value in values) for plug, values in residuals.items()}
    return sigma_floor, squared, absolute

