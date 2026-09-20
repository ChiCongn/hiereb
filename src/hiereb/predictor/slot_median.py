"""Warm-up-only time-slice median predictor."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from datetime import datetime

import numpy as np

from hiereb.domain.models import Event, PlugKey

Slot = tuple[int, int]


class TimeSliceMedianPredictor:
    """Plug-slot median with plug-global fallback (PRD Predictor)."""

    def __init__(self, slot_seconds: int) -> None:
        if slot_seconds <= 0:
            raise ValueError("slot_seconds must be positive")
        self.slot_seconds = slot_seconds
        self._slot_medians: dict[tuple[PlugKey, Slot], float] = {}
        self._plug_medians: dict[PlugKey, float] = {}
        self._fitted = False

    @staticmethod
    def _slot(timestamp: datetime, slot_seconds: int) -> Slot:
        seconds = timestamp.hour * 3600 + timestamp.minute * 60 + timestamp.second
        return (timestamp.weekday(), seconds // slot_seconds)

    def fit(self, events: tuple[Event, ...]) -> None:
        by_slot: defaultdict[tuple[PlugKey, Slot], list[float]] = defaultdict(list)
        by_plug: defaultdict[PlugKey, list[float]] = defaultdict(list)
        for event in events:
            by_slot[(event.plug_key, self._slot(event.timestamp, self.slot_seconds))].append(
                event.value
            )
            by_plug[event.plug_key].append(event.value)
        self._slot_medians = {key: float(np.median(values)) for key, values in by_slot.items()}
        self._plug_medians = {key: float(np.median(values)) for key, values in by_plug.items()}
        self._fitted = True

    def fit_statistics(
        self,
        slot_medians: Mapping[tuple[PlugKey, Slot], float],
        plug_medians: Mapping[PlugKey, float],
    ) -> None:
        """Fit from exact externally aggregated warm-up medians."""
        if not plug_medians:
            raise ValueError("predictor statistics must contain at least one plug")
        self._slot_medians = dict(slot_medians)
        self._plug_medians = dict(plug_medians)
        self._fitted = True

    def predict(self, plug: PlugKey, timestamp: datetime) -> float | None:
        if not self._fitted:
            raise RuntimeError("predictor must be fit before predict")
        return self._slot_medians.get(
            (plug, self._slot(timestamp, self.slot_seconds)), self._plug_medians.get(plug)
        )
