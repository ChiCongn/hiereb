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

from config.settings import settings

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
    prediction_cache_max_seconds: int = field(
        default_factory=lambda: settings.PREDICTION_CACHE_MAX_SECONDS
    )

    # Last observed source-side value. Used as persistence fallback when the
    # async prediction stream is temporarily late or ahead of replay time.
    last_value: float | None = None
    last_timestamp: int | None = None

    # Week 1: math.inf = full_tx (always transmit)
    # Week 2+: set by water-filling allocator
    delta: float = field(default=math.inf)

    def get_prediction(self, timestamp: int) -> float:
        """Return prediction for timestamp, falling back to last observed value."""
        if timestamp in self.predictions:
            return self.predictions[timestamp]
        if self.last_value is not None:
            return self.last_value
        return 0.0

    def update_predictions(self, predictions: Dict[int, float]) -> None:
        """Merge a prediction batch into the cache."""
        self.predictions.update(predictions)
        self._prune_predictions()
        log.debug(
            "predictions_updated",
            plug_uid=self.plug_uid,
            added=len(predictions),
            cached=len(self.predictions),
        )

    def observe(self, value: float, timestamp: int) -> None:
        """Record the latest source-side value and prune stale/far-future predictions."""
        self.last_value = value
        self.last_timestamp = timestamp
        self._prune_predictions()

    def _prune_predictions(self) -> None:
        if self.last_timestamp is None or not self.predictions:
            return

        keep_from = self.last_timestamp - self.prediction_cache_max_seconds
        keep_to = self.last_timestamp + self.prediction_cache_max_seconds
        self.predictions = {
            ts: value
            for ts, value in self.predictions.items()
            if keep_from <= ts <= keep_to
        }

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

    def update_deltas(self, deltas: Dict[int, float], create_missing: bool = False) -> None:
        """Apply new deltas received from hiereb.thresholds topic."""
        for plug_uid, delta in deltas.items():
            if plug_uid in self.plugs:
                self.plugs[plug_uid].set_delta(delta)
            elif create_missing:
                decoded_house_id = plug_uid // 100_000
                if decoded_house_id != self.house_id:
                    log.warning(
                        "delta_house_mismatch",
                        plug_uid=plug_uid,
                        decoded_house_id=decoded_house_id,
                        house_id=self.house_id,
                    )
                    continue

                remainder = plug_uid % 100_000
                household_id = remainder // 1_000
                plug_id = remainder % 1_000
                self.get_or_create_plug(plug_uid, household_id, plug_id).set_delta(delta)
            else:
                log.warning("unknown_plug_for_delta", plug_uid=plug_uid, house_id=self.house_id)

    @property
    def plug_count(self) -> int:
        return len(self.plugs)
