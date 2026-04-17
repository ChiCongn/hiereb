"""
Per-plug runtime state for HierEB simulator.
PlugState holds predictions and delta.
Week 1: delta = inf → always transmit (full_tx mode).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict

import structlog

log = structlog.get_logger(__name__)


@dataclass
class PlugState:
    """
    Runtime state for one smart plug.
    The should_transmit() method is the single source of truth for suppression.
    """
    plug_uid: int
    house_id: int
    household_id: int
    plug_id: int

    # Week 2+: populated from hiereb.predictions topic
    predictions: Dict[int, float] = field(default_factory=dict)

    # Week 1: math.inf = full_tx (always transmit)
    # Week 2+: set by water-filling allocator
    delta: float = field(default=math.inf)

    def get_prediction(self, timestamp: int) -> float:
        """Return predicted load or 0.0 if not available (graceful fallback)."""
        return self.predictions.get(timestamp, 0.0)

    def update_predictions(self, predictions: Dict[int, float]) -> None:
        """Replace current prediction batch (called when receiving new batch from ML)."""
        self.predictions = dict(predictions)  # copy
        log.debug("predictions_updated", plug_uid=self.plug_uid, count=len(predictions))

    def set_delta(self, delta: float) -> None:
        """Update suppression threshold from allocator."""
        if delta < 0:
            raise ValueError(f"delta must be non-negative, got {delta}")
        self.delta = delta
        log.debug("delta_updated", plug_uid=self.plug_uid, new_delta=delta)

    def should_transmit(self, actual: float, timestamp: int) -> bool:
        """
        Core suppression decision (b_p(t) = 1 if |x - x̂| > δ).
        Exactly equal to delta → suppress (important boundary behavior).
        """
        if self.delta == math.inf:          # full_tx mode
            return True

        predicted = self.get_prediction(timestamp)
        diff = abs(actual - predicted)

        return diff > self.delta


@dataclass
class HouseState:
    """
    All plugs belonging to one house.
    """
    house_id: int
    plugs: Dict[int, PlugState] = field(default_factory=dict)  # plug_uid → PlugState

    def get_or_create_plug(
        self, plug_uid: int, household_id: int, plug_id: int
    ) -> PlugState:
        """Get existing plug or create new one with default settings."""
        if plug_uid not in self.plugs:
            self.plugs[plug_uid] = PlugState(
                plug_uid=plug_uid,
                house_id=self.house_id,
                household_id=household_id,
                plug_id=plug_id,
            )
        return self.plugs[plug_uid]

    def update_deltas(self, deltas: Dict[int, float]) -> None:
        """Apply new deltas received from hiereb.thresholds topic."""
        for plug_uid, delta in deltas.items():
            if plug_uid in self.plugs:
                self.plugs[plug_uid].set_delta(delta)
            else:
                log.warning("unknown_plug_for_delta", plug_uid=plug_uid, house_id=self.house_id)

    @property
    def plug_count(self) -> int:
        return len(self.plugs)