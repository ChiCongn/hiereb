"""Immutable domain records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

PlugKey = tuple[int, int]


@dataclass(frozen=True, slots=True)
class Event:
    """One valid smart-plug observation after filtering and stable sorting."""

    timestamp: datetime
    value: float
    property_value: int
    plug_id: int
    household_id: int
    house_id: int
    original_row_index: int
    event_id: str | int

    @property
    def plug_key(self) -> PlugKey:
        """Return the house-local `(household_id, plug_id)` identity."""
        return (self.household_id, self.plug_id)


@dataclass(frozen=True, slots=True)
class SplitEvents:
    """Chronologically separated experiment partitions."""

    warmup: tuple[Event, ...]
    validation: tuple[Event, ...]
    evaluation: tuple[Event, ...]
