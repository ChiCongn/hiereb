"""Predictor protocol."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

from hiereb.domain.models import Event, PlugKey


class Predictor(Protocol):
    def fit(self, events: tuple[Event, ...]) -> None: ...

    def predict(self, plug: PlugKey, timestamp: datetime) -> float | None: ...

