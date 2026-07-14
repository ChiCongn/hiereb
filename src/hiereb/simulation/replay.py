"""Atomic timestamp-batch replay engine (ARCHITECTURE ReplayEngine order)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import groupby
from typing import Any

from hiereb.allocator.base import AllocationRequest, allocate
from hiereb.allocator.hierarchical import build_caps
from hiereb.config import AppConfig, Mode
from hiereb.domain.models import Event, PlugKey
from hiereb.predictor.base import Predictor
from hiereb.residual.state import ResidualState
from hiereb.simulation.active_set import ActiveSet
from hiereb.simulation.threshold_timeline import ThresholdTimeline
from hiereb.utils.progress import ProgressCallback, notify_progress


@dataclass(frozen=True, slots=True)
class ReplayResult:
    mode: Mode
    event_rows: tuple[dict[str, Any], ...]
    batch_rows: tuple[dict[str, Any], ...]
    threshold_rows: tuple[dict[str, Any], ...]
    forced_transmit: dict[str, int]


def replay(
    mode: Mode,
    evaluation_events: tuple[Event, ...],
    warmup_events: tuple[Event, ...],
    predictor: Predictor,
    config: AppConfig,
    house_budget: float,
    sigma_floor: float,
    initial_squared_residuals: dict[PlugKey, tuple[float, ...]],
    warmup_absolute_residuals: dict[PlugKey, tuple[float, ...]],
    precomputed_cap_quantiles: dict[PlugKey, float] | None = None,
    cap_sample_counts: dict[PlugKey, int] | None = None,
    progress: ProgressCallback | None = None,
) -> ReplayResult:
    """Replay one mode without wall-clock access or future raw-event inspection."""
    state = ResidualState(
        config.residual.estimator,
        config.residual.rolling_window_size,
        sigma_floor,
        initial_squared_residuals,
    )
    active = ActiveSet(warmup_events, config.replay.active_window_seconds)
    timeline = ThresholdTimeline()
    event_rows: list[dict[str, Any]] = []
    batch_rows: list[dict[str, Any]] = []
    threshold_rows: list[dict[str, Any]] = []
    forced = {"full_tx": 0, "missing_prediction": 0, "inactive_reactivation": 0}
    suppression_streaks: dict[PlugKey, int] = {}
    cap_cache: dict[PlugKey, float] = {}
    total_events = len(evaluation_events)
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
        warmup_absolute_residuals,
        cap_cache,
        precomputed_cap_quantiles,
        cap_sample_counts,
    )
    next_allocation = eval_start + timedelta(seconds=config.replay.allocation_period_seconds)

    for timestamp, group in groupby(evaluation_events, key=lambda event: event.timestamp):
        batch = tuple(group)
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

        # Every decision reads the same pre-batch threshold and pre-batch active state.
        for event in batch:
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
        batch_rows.append(
            {
                "timestamp": timestamp,
                "mode": mode,
                "actual_house": actual_house,
                "reconstructed_house": reconstructed_house,
                "house_error": house_error,
                "abs_house_error": abs(house_error),
                "event_count": len(batch),
                "transmitted_count": transmitted_count,
                "allocated_budget": allocated_budget,
                "budget_utilization": allocated_budget / house_budget if house_budget > 0 else 0.0,
                "bound_utilization": abs(house_error) / house_budget if house_budget > 0 else 0.0,
                "bound_violation": bound_violation,
            }
        )
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

        # State changes occur only after all batch decisions are complete.
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
            and processed_events * 100 >= total_events * next_progress_percent
        ):
            notify_progress(
                progress,
                f"mode {mode}: replay {next_progress_percent}% "
                f"({processed_events:,}/{total_events:,} events)",
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
                warmup_absolute_residuals,
                cap_cache,
                precomputed_cap_quantiles,
                cap_sample_counts,
            )
            while next_allocation <= timestamp:
                next_allocation += timedelta(seconds=config.replay.allocation_period_seconds)

    return ReplayResult(mode, tuple(event_rows), tuple(batch_rows), tuple(threshold_rows), forced)


def _publish_allocation(
    mode: Mode,
    timestamp: datetime,
    active: ActiveSet,
    state: ResidualState,
    timeline: ThresholdTimeline,
    trace: list[dict[str, Any]],
    config: AppConfig,
    house_budget: float,
    sigma_floor: float,
    warmup_absolute_residuals: dict[PlugKey, tuple[float, ...]],
    cap_cache: dict[PlugKey, float],
    precomputed_cap_quantiles: dict[PlugKey, float] | None,
    cap_sample_counts: dict[PlugKey, int] | None,
) -> None:
    active_plugs = active.active_at(timestamp)
    scores = state.scores(active_plugs)
    if mode == "true_hierarchical_cap":
        missing_caps = tuple(plug for plug in active_plugs if plug not in cap_cache)
        cap_cache.update(
            build_caps(
                missing_caps,
                warmup_absolute_residuals,
                house_budget,
                sigma_floor,
                config.cap.quantile,
                config.cap.multiplier,
                config.cap.max_house_budget_fraction,
                config.cap.min_samples,
                precomputed_cap_quantiles,
                cap_sample_counts,
            )
        )
        caps = {plug: cap_cache[plug] for plug in active_plugs}
    else:
        caps = {}
    result = allocate(
        mode,
        AllocationRequest(
            timestamp=timestamp,
            house_budget=house_budget,
            active_plugs=active_plugs,
            scores=scores,
            hierarchy=config.true_hierarchy,
            caps=caps,
            tolerance=config.cap.redistribution_tolerance,
            max_iterations_extra=config.cap.max_iterations_extra,
        ),
    )
    timeline.publish(result)
    for plug in active_plugs:
        trace.append(
            {
                "allocation_timestamp": timestamp,
                "effective_after": result.effective_after,
                "household_id": plug[0],
                "plug_id": plug[1],
                "score": scores[plug],
                "threshold": result.thresholds[plug],
                "cap": result.caps.get(plug),
                "household_budget": result.household_budgets.get(plug[0], 0.0),
                "used_budget": result.used_budget,
                "unused_budget": result.unused_budget,
                "cap_hit_count": result.cap_hit_count,
                "redistribution_rounds": result.redistribution_rounds,
                "within_household_redistributed_budget": (
                    result.within_household_redistributed_budget
                ),
                "cross_household_spill_budget": result.cross_household_spill_budget,
                "mode": mode,
            }
        )
