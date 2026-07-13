"""Threshold versions with explicit event-time effectiveness."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from hiereb.allocator.base import AllocationResult
from hiereb.domain.models import PlugKey


@dataclass(slots=True)
class ThresholdTimeline:
    current: AllocationResult | None = None

    def publish(self, result: AllocationResult) -> None:
        self.current = result

    def threshold(self, plug: PlugKey, timestamp: datetime) -> float:
        if self.current is None or timestamp <= self.current.effective_after:
            return 0.0
        return self.current.thresholds.get(plug, 0.0)

