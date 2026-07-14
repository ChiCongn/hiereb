"""Bounded-memory replay over complete timestamp batches."""

from __future__ import annotations

import hashlib
import heapq
from array import array
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from hiereb.config import AppConfig, Mode
from hiereb.domain.models import Event, PlugKey
from hiereb.predictor.base import Predictor
from hiereb.residual.state import ResidualState
from hiereb.simulation.active_set import ActiveSet
from hiereb.simulation.replay import _publish_allocation
from hiereb.simulation.threshold_timeline import ThresholdTimeline
from hiereb.utils.progress import ProgressCallback, notify_progress


@dataclass(frozen=True, slots=True)
class StreamingReplayResult:
    """Compact replay output independent of the number of raw events."""

    mode: Mode
    valid_events: int
    timestamp_batches: int
    transmitted: int
    errors: array[float]
    allocated_budgets: array[float]
    threshold_rows: tuple[dict[str, Any], ...]
    forced_transmit: dict[str, int]
    bound_violation_count: int
    suppression_violation_count: int
    maximum_consecutive_suppression: int
    daily_rows: tuple[dict[str, Any], ...]
    household_rows: tuple[dict[str, Any], ...]
    plug_rows: tuple[dict[str, Any], ...]
    top_outlier_rows: tuple[dict[str, Any], ...]
    plot_timestamps: tuple[datetime, ...]
    plot_errors: tuple[float, ...]
    decision_sha256: str


class _Hasher(Protocol):
    def update(self, data: bytes) -> None: ...

    def hexdigest(self) -> str: ...


@dataclass(slots=True)
class _EventAggregate:
    events: int = 0
    transmitted: int = 0
    absolute_error_sum: float = 0.0
    squared_error_sum: float = 0.0
    threshold_sum: float = 0.0
    threshold_max: float = 0.0

    def observe(self, reconstruction_error: float, threshold: float, transmitted: bool) -> None:
        self.events += 1
        self.transmitted += int(transmitted)
        self.absolute_error_sum += abs(reconstruction_error)
        self.squared_error_sum += reconstruction_error * reconstruction_error
        self.threshold_sum += threshold
        self.threshold_max = max(self.threshold_max, threshold)


@dataclass(slots=True)
class _BatchAggregate:
    batches: int = 0
    absolute_error_sum: float = 0.0
    squared_error_sum: float = 0.0
    maximum: float = 0.0
    violations: int = 0

    def observe(self, error: float, violation: bool) -> None:
        self.batches += 1
        self.absolute_error_sum += abs(error)
        self.squared_error_sum += error * error
        self.maximum = max(self.maximum, abs(error))
        self.violations += int(violation)


@dataclass(slots=True)
class _CompactCollector:
    mode: Mode
    top_k: int
    total_events_hint: int
    errors: array[float] = field(default_factory=lambda: array("d"))
    allocated: array[float] = field(default_factory=lambda: array("d"))
    valid_events: int = 0
    transmitted: int = 0
    bound_violations: int = 0
    suppression_violations: int = 0
    maximum_streak: int = 0
    daily: defaultdict[str, _BatchAggregate] = field(
        default_factory=lambda: defaultdict(_BatchAggregate)
    )
    households: defaultdict[int, _EventAggregate] = field(
        default_factory=lambda: defaultdict(_EventAggregate)
    )
    plugs: defaultdict[PlugKey, _EventAggregate] = field(
        default_factory=lambda: defaultdict(_EventAggregate)
    )
    outlier_heap: list[tuple[float, int, list[dict[str, Any]]]] = field(default_factory=list)
    plot_timestamps: list[datetime] = field(default_factory=list)
    plot_errors: list[float] = field(default_factory=list)
    decision_hasher: _Hasher = field(default_factory=hashlib.sha256)

    def observe(
        self,
        batch_row: dict[str, Any],
        event_rows: list[dict[str, Any]],
        tolerance: float,
    ) -> None:
        error = float(batch_row["house_error"])
        allocated = float(batch_row["allocated_budget"])
        timestamp = batch_row["timestamp"]
        assert isinstance(timestamp, datetime)
        self.errors.append(error)
        self.allocated.append(allocated)
        self.valid_events += len(event_rows)
        self.transmitted += sum(bool(row["transmitted"]) for row in event_rows)
        self.decision_hasher.update(
            bytes(1 if bool(row["transmitted"]) else 0 for row in event_rows)
        )
        violation = bool(batch_row["bound_violation"])
        self.bound_violations += int(violation)
        self.daily[timestamp.date().isoformat()].observe(error, violation)
        for row in event_rows:
            household = int(row["household_id"])
            plug = (household, int(row["plug_id"]))
            reconstruction_error = float(row["reconstruction_error"])
            threshold = float(row["threshold_used"])
            transmitted = bool(row["transmitted"])
            self.households[household].observe(reconstruction_error, threshold, transmitted)
            self.plugs[plug].observe(reconstruction_error, threshold, transmitted)
            self.maximum_streak = max(self.maximum_streak, int(row["consecutive_suppression"]))
            if (
                bool(row["suppressed"])
                and row["residual"] is not None
                and abs(float(row["residual"])) > threshold + tolerance
            ):
                self.suppression_violations += 1
        self._observe_outlier(timestamp, abs(error), event_rows)
        stride = max(self.total_events_hint // 10_000, 1)
        if len(self.errors) == 1 or len(self.errors) % stride == 0:
            self.plot_timestamps.append(timestamp)
            self.plot_errors.append(error)

    def finish(
        self,
        threshold_rows: list[dict[str, Any]],
        forced: dict[str, int],
    ) -> StreamingReplayResult:
        budgets: defaultdict[int, list[float]] = defaultdict(list)
        for row in threshold_rows:
            budgets[int(row["household_id"])].append(float(row["household_budget"]))
        daily_rows = tuple(
            {
                "date": day,
                "mode": self.mode,
                "batches": stats.batches,
                "mae": stats.absolute_error_sum / stats.batches,
                "rmse": (stats.squared_error_sum / stats.batches) ** 0.5,
                "max": stats.maximum,
                "bound_violation_count": stats.violations,
            }
            for day, stats in sorted(self.daily.items())
        )
        household_rows = tuple(
            {
                "household_id": household,
                "mode": self.mode,
                "events": stats.events,
                "transmission_ratio": stats.transmitted / stats.events,
                "mae": stats.absolute_error_sum / stats.events,
                "rmse": (stats.squared_error_sum / stats.events) ** 0.5,
                "mean_budget": (
                    sum(budgets[household]) / len(budgets[household]) if budgets[household] else 0.0
                ),
            }
            for household, stats in sorted(self.households.items())
        )
        plug_rows = tuple(
            {
                "household_id": plug[0],
                "plug_id": plug[1],
                "mode": self.mode,
                "events": stats.events,
                "transmission_ratio": stats.transmitted / stats.events,
                "mae": stats.absolute_error_sum / stats.events,
                "rmse": (stats.squared_error_sum / stats.events) ** 0.5,
                "threshold_mean": stats.threshold_sum / stats.events,
                "threshold_max": stats.threshold_max,
            }
            for plug, stats in sorted(self.plugs.items())
        )
        outlier_rows: list[dict[str, Any]] = []
        selected = sorted(self.outlier_heap, key=lambda item: (-item[0], -item[1]))
        for _absolute_error, _negative_timestamp, rows in selected:
            rows.sort(
                key=lambda row: (
                    -abs(float(row["reconstruction_error"])),
                    int(row["household_id"]),
                    int(row["plug_id"]),
                    int(row["original_row_index"]),
                )
            )
            outlier_rows.extend(rows)
        return StreamingReplayResult(
            mode=self.mode,
            valid_events=self.valid_events,
            timestamp_batches=len(self.errors),
            transmitted=self.transmitted,
            errors=self.errors,
            allocated_budgets=self.allocated,
            threshold_rows=tuple(threshold_rows),
            forced_transmit=dict(forced),
            bound_violation_count=self.bound_violations,
            suppression_violation_count=self.suppression_violations,
            maximum_consecutive_suppression=self.maximum_streak,
            daily_rows=daily_rows,
            household_rows=household_rows,
            plug_rows=plug_rows,
            top_outlier_rows=tuple(outlier_rows[: self.top_k]),
            plot_timestamps=tuple(self.plot_timestamps),
            plot_errors=tuple(self.plot_errors),
            decision_sha256=self.decision_hasher.hexdigest(),
        )

    def _observe_outlier(
        self,
        timestamp: datetime,
        absolute_error: float,
        event_rows: list[dict[str, Any]],
    ) -> None:
        negative_timestamp = -_timestamp_microseconds(timestamp)
        item = (absolute_error, negative_timestamp, event_rows)
        if len(self.outlier_heap) < self.top_k:
            heapq.heappush(self.outlier_heap, item)
        elif item[:2] > self.outlier_heap[0][:2]:
            heapq.heapreplace(self.outlier_heap, item)


def replay_streaming(
    mode: Mode,
    evaluation_batches: Iterable[tuple[Event, ...]],
    predictor: Predictor,
    config: AppConfig,
    house_budget: float,
    sigma_floor: float,
    initial_squared_residuals: dict[PlugKey, tuple[float, ...]],
    initial_last_seen: dict[PlugKey, datetime],
    evaluation_event_count: int,
    precomputed_cap_quantiles: dict[PlugKey, float] | None = None,
    cap_sample_counts: dict[PlugKey, int] | None = None,
    progress: ProgressCallback | None = None,
) -> StreamingReplayResult:
    """Replay a sorted event stream while retaining only compact diagnostics."""
    state = ResidualState(
        config.residual.estimator,
        config.residual.rolling_window_size,
        sigma_floor,
        initial_squared_residuals,
    )
    active = ActiveSet((), config.replay.active_window_seconds)
    active.last_seen.update(initial_last_seen)
    timeline = ThresholdTimeline()
    threshold_rows: list[dict[str, Any]] = []
    forced = {"full_tx": 0, "missing_prediction": 0, "inactive_reactivation": 0}
    suppression_streaks: dict[PlugKey, int] = {}
    cap_cache: dict[PlugKey, float] = {}
    collector = _CompactCollector(mode, config.replay.top_k_outliers, evaluation_event_count)
    processed_events = 0
    next_progress_percent = 10

    eval_start = config.splits.evaluation.start
    initial_time = eval_start - timedelta(microseconds=1)
    _publish_allocation(
        mode,
        initial_time,
        active,
        state,
        timeline,
        threshold_rows,
        config,
        house_budget,
        sigma_floor,
        {},
        cap_cache,
        precomputed_cap_quantiles,
        cap_sample_counts,
    )
    next_allocation = eval_start + timedelta(seconds=config.replay.allocation_period_seconds)

    for batch in evaluation_batches:
        if not batch:
            continue
        timestamp = batch[0].timestamp
        pending: list[
            tuple[
                Event,
                float | None,
                float,
                float,
                bool,
                str | None,
                float,
                float | None,
                datetime | None,
                int,
            ]
        ] = []
        actual_house = 0.0
        reconstructed_house = 0.0
        transmitted_count = 0
        for event in batch:
            if event.timestamp != timestamp:
                raise ValueError("streaming replay received a non-atomic timestamp batch")
            prediction = predictor.predict(event.plug_key, timestamp)
            threshold = timeline.threshold(event.plug_key, timestamp)
            residual_score = state.score(event.plug_key)
            cap = timeline.current.caps.get(event.plug_key) if timeline.current else None
            last_seen = active.last_seen.get(event.plug_key)
            reason: str | None = None
            if mode == "full_tx":
                reason = "full_tx"
            elif prediction is None:
                reason = "missing_prediction"
            elif active.is_reactivation(event.plug_key, timestamp):
                reason = "inactive_reactivation"
            residual = event.value - prediction if prediction is not None else 0.0
            transmitted = reason is not None or abs(residual) > threshold
            reconstructed = event.value if transmitted or prediction is None else prediction
            if reason is not None:
                forced[reason] += 1
            transmitted_count += int(transmitted)
            actual_house += event.value
            reconstructed_house += reconstructed
            consecutive = 0 if transmitted else suppression_streaks.get(event.plug_key, 0) + 1
            suppression_streaks[event.plug_key] = consecutive
            pending.append(
                (
                    event,
                    prediction,
                    residual,
                    threshold,
                    transmitted,
                    reason,
                    residual_score,
                    cap,
                    last_seen,
                    consecutive,
                )
            )

        house_error = actual_house - reconstructed_house
        current = timeline.current
        allocated_budget = current.used_budget if current is not None else 0.0
        bound_violation = abs(house_error) > house_budget + config.replay.float_tolerance
        batch_row: dict[str, Any] = {
            "timestamp": timestamp,
            "mode": mode,
            "actual_house": actual_house,
            "reconstructed_house": reconstructed_house,
            "house_error": house_error,
            "abs_house_error": abs(house_error),
            "event_count": len(batch),
            "transmitted_count": transmitted_count,
            "allocated_budget": allocated_budget,
            "budget_utilization": (allocated_budget / house_budget if house_budget > 0 else 0.0),
            "bound_utilization": abs(house_error) / house_budget if house_budget > 0 else 0.0,
            "bound_violation": bound_violation,
        }
        event_rows: list[dict[str, Any]] = []
        for (
            event,
            prediction,
            residual,
            threshold,
            transmitted,
            reason,
            residual_score,
            cap,
            last_seen,
            consecutive,
        ) in pending:
            reconstructed = event.value if transmitted or prediction is None else prediction
            event_rows.append(
                {
                    "event_id": event.event_id,
                    "timestamp": timestamp,
                    "house_id": event.house_id,
                    "household_id": event.household_id,
                    "plug_id": event.plug_id,
                    "original_row_index": event.original_row_index,
                    "actual": event.value,
                    "prediction": prediction,
                    "residual": residual if prediction is not None else None,
                    "threshold_used": threshold,
                    "transmitted": transmitted,
                    "suppressed": not transmitted,
                    "reconstructed": reconstructed,
                    "reconstruction_error": event.value - reconstructed,
                    "forced_reason": reason,
                    "forced_transmit_reason": reason,
                    "decision": "transmit" if transmitted else "suppress",
                    "residual_score": residual_score,
                    "cap": cap,
                    "consecutive_suppression": consecutive,
                    "last_seen_timestamp": last_seen,
                    "house_error": house_error,
                    "abs_house_error": abs(house_error),
                    "bound_utilization": (
                        abs(house_error) / house_budget if house_budget > 0 else 0.0
                    ),
                    "mode": mode,
                }
            )
        collector.observe(batch_row, event_rows, config.replay.float_tolerance)

        for event, prediction, residual, threshold, transmitted, *_diagnostics in pending:
            if prediction is None:
                continue
            if transmitted:
                state.observe_transmitted(event.plug_key, residual)
            else:
                state.observe_suppressed(
                    event.plug_key,
                    threshold,
                    exact_residual=residual if config.residual.estimator == "exact" else None,
                )
        active.observe_batch(batch)
        processed_events += len(batch)
        while (
            next_progress_percent <= 100
            and processed_events * 100 >= evaluation_event_count * next_progress_percent
        ):
            notify_progress(
                progress,
                f"mode {mode}: streaming replay {next_progress_percent}% "
                f"({processed_events:,}/{evaluation_event_count:,} events)",
            )
            next_progress_percent += 10

        if timestamp >= next_allocation:
            _publish_allocation(
                mode,
                timestamp,
                active,
                state,
                timeline,
                threshold_rows,
                config,
                house_budget,
                sigma_floor,
                {},
                cap_cache,
                precomputed_cap_quantiles,
                cap_sample_counts,
            )
            while next_allocation <= timestamp:
                next_allocation += timedelta(seconds=config.replay.allocation_period_seconds)

    if processed_events != evaluation_event_count:
        raise ValueError(
            "streaming evaluation count changed between preparation and replay: "
            f"expected {evaluation_event_count}, observed {processed_events}"
        )
    return collector.finish(threshold_rows, forced)


def _timestamp_microseconds(timestamp: datetime) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = timestamp.astimezone(UTC) - epoch
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
