"""Opt-in elapsed-time progress reporting outside algorithm decisions."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from time import perf_counter

ProgressCallback = Callable[[str], None]


@dataclass(slots=True)
class ProgressReporter:
    """Prefix progress messages with elapsed runtime and send them to a caller sink."""

    sink: ProgressCallback
    started: float = field(default_factory=perf_counter)

    def __call__(self, message: str) -> None:
        self.sink(f"[hiereb +{perf_counter() - self.started:8.1f}s] {message}")


def notify_progress(progress: ProgressCallback | None, message: str) -> None:
    """Emit a message only when the caller opted into progress reporting."""
    if progress is not None:
        progress(message)
