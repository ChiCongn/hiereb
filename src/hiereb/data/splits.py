"""Leakage-safe event partitioning."""

from __future__ import annotations

from hiereb.config import SplitsConfig
from hiereb.domain.models import Event, SplitEvents


def split_events(events: tuple[Event, ...], config: SplitsConfig) -> SplitEvents:
    """Partition already sorted events into non-overlapping configured intervals."""
    warmup = tuple(e for e in events if config.warmup.start <= e.timestamp <= config.warmup.end)
    evaluation = tuple(
        e for e in events if config.evaluation.start <= e.timestamp <= config.evaluation.end
    )
    validation: tuple[Event, ...] = ()
    if config.validation.enabled:
        assert config.validation.start is not None and config.validation.end is not None
        validation = tuple(
            e
            for e in events
            if config.validation.start <= e.timestamp <= config.validation.end
        )
    if not warmup:
        raise ValueError("warm-up split is empty")
    if not evaluation:
        raise ValueError("evaluation split is empty")
    return SplitEvents(warmup=warmup, validation=validation, evaluation=evaluation)


def mean_event_aligned_house_load(events: tuple[Event, ...]) -> float:
    """Mean of per-timestamp observed house sums, per MATH_SPEC section 3."""
    sums: dict[object, float] = {}
    for event in events:
        sums[event.timestamp] = sums.get(event.timestamp, 0.0) + event.value
    return sum(sums.values()) / len(sums)

