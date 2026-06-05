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


@dataclass(frozen=True)
class TransmissionDecision:
    """Decision metadata for one plug event."""
    transmitted: bool
    predicted: float | None
    residual: float | None
    abs_residual: float | None
    reason: str
    plug_status: str
    is_forced_transmit: bool


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

    # Last observed source-side value is debug/state metadata only. It must not
    # be used as prediction fallback for the suppression metric path.
    last_value: float | None = None
    last_timestamp: int | None = None

    # Week 1: math.inf = full_tx (always transmit)
    # Week 2+: set by water-filling allocator
    delta: float = field(default=math.inf)

    def get_prediction(self, timestamp: int) -> float | None:
        """Return cached prediction for timestamp, or None if it is missing."""
        return self.predictions.get(timestamp)

    def update_predictions(self, predictions: Dict[int, float]) -> None:
        """Merge a prediction batch into the cache."""
        self.predictions.update(predictions)
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

    def decide_transmission(
        self,
        actual: float,
        timestamp: int,
        *,
        bypass_suppression: bool = False,
        plug_status: str = "active",
    ) -> TransmissionDecision:
        """
        Core suppression decision with audit metadata.

        Missing prediction is a forced transmit in suppression modes. A full_tx
        caller can pass bypass_suppression=True so no threshold rule is applied.
        """
        predicted = self.get_prediction(timestamp)
        residual = actual - predicted if predicted is not None else None
        abs_residual = abs(residual) if residual is not None else None

        if bypass_suppression:
            return TransmissionDecision(
                transmitted=True,
                predicted=predicted,
                residual=residual,
                abs_residual=abs_residual,
                reason="normal",
                plug_status=plug_status,
                is_forced_transmit=False,
            )

        if predicted is None:
            return TransmissionDecision(
                transmitted=True,
                predicted=None,
                residual=None,
                abs_residual=None,
                reason="missing_prediction",
                plug_status=plug_status,
                is_forced_transmit=True,
            )

        if plug_status == "reactivated":
            return TransmissionDecision(
                transmitted=True,
                predicted=predicted,
                residual=residual,
                abs_residual=abs_residual,
                reason="inactive_reactivation",
                plug_status=plug_status,
                is_forced_transmit=True,
            )

        if self.delta == math.inf:
            return TransmissionDecision(
                transmitted=True,
                predicted=predicted,
                residual=residual,
                abs_residual=abs_residual,
                reason="normal",
                plug_status=plug_status,
                is_forced_transmit=False,
            )

        transmitted = abs_residual > self.delta
        return TransmissionDecision(
            transmitted=transmitted,
            predicted=predicted,
            residual=residual,
            abs_residual=abs_residual,
            reason="normal",
            plug_status=plug_status,
            is_forced_transmit=False,
        )

    def should_transmit(self, actual: float, timestamp: int) -> bool:
        """
        Core suppression decision (b_p(t) = 1 if |x - xhat| > delta).
        Exactly equal to delta → suppress (important boundary behavior).
        """
        return self.decide_transmission(actual, timestamp).transmitted


@dataclass
class HouseState:
    """
    All plugs belonging to one house.
    """
    house_id: int
    plugs: Dict[int, PlugState] = field(default_factory=dict)  # plug_uid → PlugState
    uniform_allocation_time: int | None = None
    uniform_pending_allocation_time: int | None = None
    uniform_pending_deltas: Dict[int, float] = field(default_factory=dict)
    uniform_pending_active_count: int = 0
    uniform_pending_delta_per_active: float = 0.0
    uniform_active_count: int = 0
    uniform_delta_per_active: float = 0.0

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

    def plug_status_for_event(
        self,
        plug: PlugState,
        timestamp: int,
        active_window_seconds: int,
    ) -> str:
        """
        Return active/reactivated status for an observed event.

        A plug becomes reactivated only after it had prior history and then
        stayed silent for more than the active budget window.
        """
        if active_window_seconds < 0:
            raise ValueError("active_window_seconds must be non-negative")
        if plug.last_timestamp is None:
            return "active"
        if timestamp - plug.last_timestamp > active_window_seconds:
            return "reactivated"
        return "active"

    def stage_uniform_allocation(
        self,
        *,
        delta_h: float,
        allocation_time: int,
        active_window_seconds: int,
    ) -> tuple[int, float]:
        """
        Compute a pending uniform allocation for one house.

        The staged deltas become decision-effective only after
        activate_due_uniform_allocation() sees timestamp > allocation_time.
        """
        if delta_h < 0:
            raise ValueError(f"delta_h must be non-negative, got {delta_h}")
        if active_window_seconds < 0:
            raise ValueError("active_window_seconds must be non-negative")

        active_plug_uids = [
            plug_uid
            for plug_uid, plug in self.plugs.items()
            if plug.last_timestamp is None
            or allocation_time - plug.last_timestamp <= active_window_seconds
        ]
        active_count = len(active_plug_uids)
        delta_per_active = delta_h / active_count if active_count else 0.0
        active_set = set(active_plug_uids)

        self.uniform_pending_deltas = {
            plug_uid: delta_per_active if plug_uid in active_set else 0.0
            for plug_uid in self.plugs
        }
        self.uniform_pending_allocation_time = allocation_time
        self.uniform_pending_active_count = active_count
        self.uniform_pending_delta_per_active = delta_per_active
        log.debug(
            "uniform_allocation_staged",
            house_id=self.house_id,
            allocation_time=allocation_time,
            active_count=active_count,
            delta_per_active=delta_per_active,
        )
        return active_count, delta_per_active

    def activate_due_uniform_allocation(self, timestamp: int) -> bool:
        """Apply pending uniform deltas if this event is after allocation_time."""
        if self.uniform_pending_allocation_time is None:
            return False
        if timestamp <= self.uniform_pending_allocation_time:
            return False

        for plug_uid, delta in self.uniform_pending_deltas.items():
            plug = self.plugs.get(plug_uid)
            if plug is not None:
                plug.set_delta(delta)

        self.uniform_allocation_time = self.uniform_pending_allocation_time
        self.uniform_active_count = self.uniform_pending_active_count
        self.uniform_delta_per_active = self.uniform_pending_delta_per_active
        self.uniform_pending_allocation_time = None
        self.uniform_pending_deltas = {}
        self.uniform_pending_active_count = 0
        self.uniform_pending_delta_per_active = 0.0
        log.debug(
            "uniform_allocation_activated",
            house_id=self.house_id,
            timestamp=timestamp,
            allocation_time=self.uniform_allocation_time,
            active_count=self.uniform_active_count,
            delta_per_active=self.uniform_delta_per_active,
        )
        return True

    def ensure_initial_uniform_allocation(
        self,
        *,
        timestamp: int,
        delta_h: float,
        active_window_seconds: int,
    ) -> None:
        """Create and activate the first uniform allocation for current replay."""
        if (
            self.uniform_allocation_time is not None
            or self.uniform_pending_allocation_time is not None
        ):
            return
        self.stage_uniform_allocation(
            delta_h=delta_h,
            allocation_time=timestamp - 1,
            active_window_seconds=active_window_seconds,
        )
        self.activate_due_uniform_allocation(timestamp)

    def stage_uniform_allocation_if_due(
        self,
        *,
        timestamp: int,
        delta_h: float,
        active_window_seconds: int,
        allocation_period_seconds: int,
    ) -> bool:
        """Stage the next uniform allocation after the configured event-time period."""
        if allocation_period_seconds <= 0:
            raise ValueError("allocation_period_seconds must be positive")
        if self.uniform_pending_allocation_time is not None:
            return False
        if self.uniform_allocation_time is None:
            return False
        if timestamp - self.uniform_allocation_time < allocation_period_seconds:
            return False
        self.stage_uniform_allocation(
            delta_h=delta_h,
            allocation_time=timestamp,
            active_window_seconds=active_window_seconds,
        )
        return True

    @property
    def plug_count(self) -> int:
        return len(self.plugs)
