"""
Simulation statistics using Welford online algorithm + Transmission Rate tracking.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict

import structlog

log = structlog.get_logger(__name__)


@dataclass
class WelfordState:
    """Online mean & variance calculator (used for sigma_p in allocator)."""
    count: int = 0
    mean: float = 0.0
    m2: float = 0.0

    def update(self, x: float) -> None:
        self.count += 1
        delta = x - self.mean
        self.mean += delta / self.count
        delta2 = x - self.mean
        self.m2 += delta * delta2

    def variance(self) -> float:
        if self.count < 2:
            return 0.0
        return self.m2 / self.count

    def std(self) -> float:
        return self.variance() ** 0.5


@dataclass
class SimStats:
    """Per-house simulation statistics."""
    house_id: int | None = None

    variance_states: Dict[int, WelfordState] = field(default_factory=dict)
    transmitted: int = 0
    suppressed: int = 0
    total_timesteps: int = 0

    def get_or_create_variance(self, plug_uid: int) -> WelfordState:
        if plug_uid not in self.variance_states:
            self.variance_states[plug_uid] = WelfordState()
        return self.variance_states[plug_uid]

    def record_transmit(self, plug_uid: int, error: float) -> None:
        """Record a transmitted reading."""
        self.transmitted += 1
        self.total_timesteps += 1
        self.get_or_create_variance(plug_uid).update(error)

    def record_suppress(self, plug_uid: int) -> None:
        """Record a suppressed reading (censored)."""
        self.suppressed += 1
        self.total_timesteps += 1
        # Censored variance correction will be handled in allocator (Week 2)

    def transmission_rate(self) -> float:
        """Current Transmission Rate (TR)."""
        if self.total_timesteps == 0:
            return 0.0
        return self.transmitted / self.total_timesteps

    @property
    def overall_tr(self) -> float:
        """Alias used by simulator progress logs."""
        return self.transmission_rate()

    def summary(self) -> dict:
        return {
            "house_id": self.house_id,
            "tr": round(self.transmission_rate(), 4),
            "transmitted": self.transmitted,
            "suppressed": self.suppressed,
            "total_timesteps": self.total_timesteps,
            "active_plugs": len(self.variance_states),
        }
