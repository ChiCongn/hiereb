"""Event-time active/inactive state."""

from __future__ import annotations

from datetime import datetime, timedelta

from hiereb.domain.models import Event, PlugKey


class ActiveSet:
    def __init__(self, warmup_events: tuple[Event, ...], window_seconds: int) -> None:
        self.window = timedelta(seconds=window_seconds)
        self.last_seen: dict[PlugKey, datetime] = {}
        for event in warmup_events:
            self.last_seen[event.plug_key] = event.timestamp

    def is_reactivation(self, plug: PlugKey, timestamp: datetime) -> bool:
        previous = self.last_seen.get(plug)
        return previous is None or timestamp - previous > self.window

    def observe_batch(self, events: tuple[Event, ...]) -> None:
        for event in events:
            self.last_seen[event.plug_key] = event.timestamp

    def active_at(self, timestamp: datetime) -> tuple[PlugKey, ...]:
        # MATH_SPEC section 4 uses the open lower endpoint `(B-T_active, B]`.
        return tuple(
            sorted(
                plug
                for plug, last in self.last_seen.items()
                if timestamp - self.window < last <= timestamp
            )
        )

