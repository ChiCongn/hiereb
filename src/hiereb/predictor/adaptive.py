"""Deployable dual-predictor EWMA state for Direction 3."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from hiereb.config import PredictorMode
from hiereb.domain.models import PlugKey
from hiereb.predictor.base import Predictor

DEPLOYABLE_PREDICTOR_MODES: tuple[PredictorMode, ...] = (
    "slot_median_frozen",
    "slot_median_ewma",
    "slot_median_ewma_periodic_sync",
    "slot_median_ewma_drift",
    "slot_median_ewma_drift_periodic_sync",
)
COMPARISON_PREDICTOR_MODES: tuple[PredictorMode, ...] = DEPLOYABLE_PREDICTOR_MODES


@dataclass(slots=True)
class PredictorRuntimeState:
    """Explicit edge/server state and synchronization diagnostics for one plug."""

    edge_bias: float = 0.0
    server_bias: float = 0.0
    warmup_initial_bias: float = 0.0
    last_prediction: float | None = None
    last_innovation: float | None = None
    last_transmitted_timestamp: datetime | None = None
    consecutive_suppressions: int = 0
    predictor_update_count: int = 0
    drift_trigger_count: int = 0
    periodic_sync_count: int = 0
    periodic_sync_time_count: int = 0
    last_drift_timestamp: datetime | None = None
    version: int = 0


def ewma_bias_transition(bias: float, innovation: float, alpha: float) -> float:
    """Apply the shared deterministic Direction 3 EWMA transition."""
    if not 0.0 <= alpha <= 1.0:
        raise ValueError("alpha must be in [0, 1]")
    return bias + alpha * innovation


def mode_uses_adaptation(mode: PredictorMode) -> bool:
    return mode != "slot_median_frozen"


def mode_uses_drift(mode: PredictorMode) -> bool:
    return mode in {
        "slot_median_ewma_drift",
        "slot_median_ewma_drift_periodic_sync",
    }


def mode_uses_periodic_sync(mode: PredictorMode) -> bool:
    return mode in {
        "slot_median_ewma_periodic_sync",
        "slot_median_ewma_drift_periodic_sync",
    }


def mode_is_oracle(mode: PredictorMode) -> bool:
    return mode == "slot_median_ewma_local_oracle"


class DualPredictorRuntime:
    """Seasonal baseline plus explicit edge/server EWMA biases."""

    def __init__(
        self,
        seasonal: Predictor,
        mode: PredictorMode,
        alpha: float,
        initial_biases: dict[PlugKey, float] | None = None,
    ) -> None:
        self.seasonal = seasonal
        self.mode = mode
        self.alpha = alpha
        self._initial_biases = initial_biases or {}
        self.states: dict[PlugKey, PredictorRuntimeState] = {}

    @property
    def deployable(self) -> bool:
        return not mode_is_oracle(self.mode)

    def state(self, plug: PlugKey) -> PredictorRuntimeState:
        if plug not in self.states:
            initial = self._initial_biases.get(plug, 0.0)
            self.states[plug] = PredictorRuntimeState(
                edge_bias=initial,
                server_bias=initial,
                warmup_initial_bias=initial,
            )
        return self.states[plug]

    def predict(self, plug: PlugKey, timestamp: datetime) -> tuple[float | None, float | None]:
        baseline = self.seasonal.predict(plug, timestamp)
        if baseline is None:
            return None, None
        state = self.state(plug)
        if not mode_uses_adaptation(self.mode):
            return baseline, baseline
        return baseline + state.edge_bias, baseline + state.server_bias

    def commit(
        self,
        plug: PlugKey,
        innovation: float,
        *,
        transmitted: bool,
        timestamp: datetime,
    ) -> bool:
        """Commit once after a batch decision; return whether a shared update occurred."""
        state = self.state(plug)
        should_update_edge = transmitted or mode_is_oracle(self.mode)
        should_update_server = transmitted
        if mode_uses_adaptation(self.mode) and should_update_edge:
            state.edge_bias = ewma_bias_transition(state.edge_bias, innovation, self.alpha)
        if mode_uses_adaptation(self.mode) and should_update_server:
            state.server_bias = ewma_bias_transition(state.server_bias, innovation, self.alpha)
            state.predictor_update_count += 1
            state.version += 1
        state.last_innovation = innovation
        if transmitted:
            state.last_transmitted_timestamp = timestamp
            state.consecutive_suppressions = 0
        else:
            state.consecutive_suppressions += 1
        return mode_uses_adaptation(self.mode) and should_update_server

    def divergence_count(self, tolerance: float) -> int:
        return sum(
            abs(state.edge_bias - state.server_bias) > tolerance for state in self.states.values()
        )
