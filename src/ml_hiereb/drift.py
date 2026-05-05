"""
Page-Hinkley drift detector.

Detects concept drift in plug load patterns (e.g., new appliance plugged in,
seasonal change, occupancy shift).

When drift is detected:
  - Plug is forced to transmit for a window of FORCED_TX_SECONDS
  - Predictor's training data for that plug becomes stale → flag for refit
  - Drift event is logged (and written to threshold_log in Week 3+)

Algorithm (CUSUM with restart-on-alarm):
  - Maintains two CUSUM statistics clamped to 0:
      cusum_up   = max(0, cusum_up + centered - λ)   detects positive shift
      cusum_down = max(0, cusum_down - centered - λ)  detects negative shift
  - Alarm when either exceeds ξ (xi threshold)
  - Clamping to 0 prevents false alarms on stable zero-mean signals.

References:
  Page, E.S. (1954). "Continuous inspection schemes."
  Biometrika 41(1-2): 100-115.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field

import structlog

log = structlog.get_logger(__name__)

# ── Default parameters ────────────────────────────────────────────────────────
DEFAULT_LAMBDA = 5.0    # minimum mean shift to detect (Watts)
DEFAULT_XI = 50.0       # alarm threshold for CUSUM statistic

# Window after drift: forced transmission (data-time seconds)
FORCED_TX_SECONDS = 10


@dataclass
class PageHinkleyState:
    """
    CUSUM-based drift detector for one plug.

    Both up and down shifts are tracked independently.
    Restart-on-alarm: CUSUMs reset to 0 after each alarm.
    """
    plug_uid: int
    lambda_: float = DEFAULT_LAMBDA
    xi: float = DEFAULT_XI

    # CUSUM accumulators (clamped to >= 0)
    _cusum_up: float = 0.0      # detects positive mean shift
    _cusum_down: float = 0.0    # detects negative mean shift

    # Running mean (for centering each residual)
    _n: int = 0
    _mean: float = 0.0

    # Forced TX state after drift detection
    _forced_tx_remaining: int = 0
    drift_count: int = 0

    def update(self, residual: float) -> bool:
        """
        Update CUSUM with new residual and check for drift.

        Args:
            residual: actual - predicted (Watts)

        Returns:
            True if drift detected at this timestep (alarm raised)
        """
        # Update running mean
        self._n += 1
        self._mean += (residual - self._mean) / self._n
        centered = residual - self._mean

        # Clamped CUSUM (restart-on-alarm semantics)
        self._cusum_up = max(0.0, self._cusum_up + centered - self.lambda_)
        self._cusum_down = max(0.0, self._cusum_down - centered - self.lambda_)

        drift_detected = self._cusum_up > self.xi or self._cusum_down > self.xi

        if drift_detected:
            self.drift_count += 1
            self._forced_tx_remaining = FORCED_TX_SECONDS
            # Reset CUSUMs after alarm (restart-on-alarm)
            self._cusum_up = 0.0
            self._cusum_down = 0.0
            log.info(
                "drift_detected",
                plug_uid=self.plug_uid,
                residual=round(residual, 2),
                drift_count=self.drift_count,
            )

        return drift_detected

    def tick(self) -> None:
        """Advance forced-TX countdown. Call once per data-time second."""
        if self._forced_tx_remaining > 0:
            self._forced_tx_remaining -= 1

    @property
    def in_forced_tx(self) -> bool:
        return self._forced_tx_remaining > 0


# ─── Drift manager ────────────────────────────────────────────────────────────

class DriftDetector:
    """
    Manages CUSUM detectors for all plugs.

    Usage:
        detector = DriftDetector()
        if detector.update(plug_uid, residual) or detector.in_forced_tx(plug_uid):
            transmit()
        detector.tick(plug_uid)
    """

    def __init__(
        self,
        lambda_: float = DEFAULT_LAMBDA,
        xi: float = DEFAULT_XI,
    ) -> None:
        self.lambda_ = lambda_
        self.xi = xi
        self._states: dict[int, PageHinkleyState] = {}

    def get_or_create(self, plug_uid: int) -> PageHinkleyState:
        if plug_uid not in self._states:
            self._states[plug_uid] = PageHinkleyState(
                plug_uid=plug_uid,
                lambda_=self.lambda_,
                xi=self.xi,
            )
        return self._states[plug_uid]

    def update(self, plug_uid: int, residual: float) -> bool:
        """Update detector for plug_uid. Returns True if drift detected."""
        return self.get_or_create(plug_uid).update(residual)

    def tick(self, plug_uid: int) -> None:
        """Advance forced-TX countdown. Call every timestep."""
        self.get_or_create(plug_uid).tick()

    def in_forced_tx(self, plug_uid: int) -> bool:
        if plug_uid not in self._states:
            return False
        return self._states[plug_uid].in_forced_tx

    def total_drift_events(self) -> int:
        return sum(s.drift_count for s in self._states.values())

    def drift_summary(self) -> dict[int, int]:
        return {
            uid: s.drift_count
            for uid, s in self._states.items()
            if s.drift_count > 0
        }