"""Direction 3 atomic dual-predictor replay with drift and resynchronization."""

from __future__ import annotations

import heapq
import math
from array import array
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np

from hiereb.config import AppConfig, Mode, PredictorMode
from hiereb.domain.models import Event, PlugKey
from hiereb.predictor.adaptive import (
    DualPredictorRuntime,
    mode_is_oracle,
    mode_uses_drift,
    mode_uses_periodic_sync,
)
from hiereb.predictor.base import Predictor
from hiereb.predictor.drift import (
    DriftEvaluation,
    DriftRuntimeState,
    DriftScaleModel,
    commit_drift,
    evaluate_drift,
)
from hiereb.residual.state import ResidualState
from hiereb.simulation.active_set import ActiveSet
from hiereb.simulation.replay import _publish_allocation
from hiereb.simulation.streaming_replay import StreamingReplayResult, _CompactCollector
from hiereb.simulation.threshold_timeline import ThresholdTimeline
from hiereb.utils.progress import ProgressCallback, notify_progress


@dataclass(slots=True)
class _PredictionAggregate:
    capacity: int
    seed: int
    count: int = 0
    signed_sum: float = 0.0
    absolute_sum: float = 0.0
    squared_sum: float = 0.0
    sample: array[float] = field(default_factory=lambda: array("d"))

    def observe(self, error: float) -> None:
        self.count += 1
        self.signed_sum += error
        self.absolute_sum += abs(error)
        self.squared_sum += error * error
        if len(self.sample) < self.capacity:
            self.sample.append(abs(error))
            return
        index = ((self.count * 1_103_515_245 + self.seed) & 0x7FFFFFFF) % self.count
        if index < self.capacity:
            self.sample[index] = abs(error)

    def metrics(self) -> dict[str, float | int]:
        values = np.frombuffer(self.sample, dtype=np.float64)
        squared = np.square(values)
        squared_total = float(np.sum(squared))
        return {
            "events": self.count,
            "prediction_mae": self.absolute_sum / self.count if self.count else 0.0,
            "prediction_rmse": (self.squared_sum / self.count) ** 0.5 if self.count else 0.0,
            "prediction_p95": float(np.quantile(values, 0.95)) if values.size else 0.0,
            "prediction_p99": float(np.quantile(values, 0.99)) if values.size else 0.0,
            "prediction_cvar95": _sample_cvar(values, 0.95),
            "prediction_cvar99": _sample_cvar(values, 0.99),
            "prediction_bias": self.signed_sum / self.count if self.count else 0.0,
            "prediction_quantile_sample_size": len(self.sample),
            "top_1_percent_squared_error_share": _top_fraction_share(squared, squared_total, 0.01),
            "top_5_percent_squared_error_share": _top_fraction_share(squared, squared_total, 0.05),
        }


@dataclass(slots=True)
class _RecoveryEpisode:
    plug: PlugKey
    trigger_timestamp: datetime
    pre_trigger_absolute_error: float
    event_count: int = 0
    error_after_1: float | None = None
    error_after_5: float | None = None
    error_after_10: float | None = None
    recovered_timestamp: datetime | None = None
    recovery_events: int | None = None

    def observe(self, timestamp: datetime, absolute_error: float, scale: float) -> None:
        if timestamp <= self.trigger_timestamp:
            return
        self.event_count += 1
        if self.event_count == 1:
            self.error_after_1 = absolute_error
        elif self.event_count == 5:
            self.error_after_5 = absolute_error
        elif self.event_count == 10:
            self.error_after_10 = absolute_error
        if self.recovered_timestamp is None and absolute_error <= scale:
            self.recovered_timestamp = timestamp
            self.recovery_events = self.event_count

    def row(self) -> dict[str, Any]:
        recovery_seconds = (
            (self.recovered_timestamp - self.trigger_timestamp).total_seconds()
            if self.recovered_timestamp is not None
            else None
        )
        return {
            "household_id": self.plug[0],
            "plug_id": self.plug[1],
            "trigger_timestamp": self.trigger_timestamp,
            "recovered_timestamp": self.recovered_timestamp,
            "recovery_time_after_drift": recovery_seconds,
            "recovery_events_after_drift": self.recovery_events,
            "pre_trigger_absolute_prediction_error": self.pre_trigger_absolute_error,
            "post_trigger_prediction_error_1": self.error_after_1,
            "post_trigger_prediction_error_5": self.error_after_5,
            "post_trigger_prediction_error_10": self.error_after_10,
            "unresolved": self.recovered_timestamp is None,
        }


@dataclass(frozen=True, slots=True)
class AdaptiveReplayResult:
    base: StreamingReplayResult
    predictor_mode: PredictorMode
    deployable: bool
    predictor_summary: dict[str, Any]
    predictor_plug_rows: tuple[dict[str, Any], ...]
    predictor_update_rows: tuple[dict[str, Any], ...]
    drift_event_rows: tuple[dict[str, Any], ...]
    resynchronization_rows: tuple[dict[str, Any], ...]
    recovery_rows: tuple[dict[str, Any], ...]
    bias_trace_rows: tuple[dict[str, Any], ...]
    top_prediction_outlier_rows: tuple[dict[str, Any], ...]


@dataclass(frozen=True, slots=True)
class _PendingDecision:
    event: Event
    edge_prediction: float | None
    server_prediction: float | None
    innovation: float
    threshold: float
    transmitted: bool
    reason: str | None
    normal_would_transmit: bool
    drift: DriftEvaluation | None
    standardized_innovation: float | None
    residual_score: float
    cap: float | None
    last_seen: datetime | None
    prior_suppressions: int
    prior_last_transmitted: datetime | None
    prior_edge_bias: float
    prior_server_bias: float


def replay_adaptive_streaming(
    allocator_mode: Mode,
    predictor_mode: PredictorMode,
    evaluation_batches: Iterable[tuple[Event, ...]],
    seasonal_predictor: Predictor,
    config: AppConfig,
    house_budget: float,
    sigma_floor: float,
    initial_squared_residuals: dict[PlugKey, tuple[float, ...]],
    initial_last_seen: dict[PlugKey, datetime],
    evaluation_event_count: int,
    scale_model: DriftScaleModel,
    initial_biases: dict[PlugKey, float],
    precomputed_cap_quantiles: dict[PlugKey, float] | None = None,
    cap_sample_counts: dict[PlugKey, int] | None = None,
    progress: ProgressCallback | None = None,
) -> AdaptiveReplayResult:
    """Replay Direction 3 without hidden updates or free control transmissions."""
    runtime = DualPredictorRuntime(
        seasonal_predictor,
        predictor_mode,
        config.predictor.adaptive.alpha,
        initial_biases,
    )
    drift_states: defaultdict[PlugKey, DriftRuntimeState] = defaultdict(DriftRuntimeState)
    residual_state = ResidualState(
        config.residual.estimator,
        config.residual.rolling_window_size,
        sigma_floor,
        initial_squared_residuals,
    )
    active = ActiveSet((), config.replay.active_window_seconds)
    active.last_seen.update(initial_last_seen)
    timeline = ThresholdTimeline()
    threshold_rows: list[dict[str, Any]] = []
    cap_cache: dict[PlugKey, float] = {}
    collector = _CompactCollector(
        allocator_mode, config.replay.top_k_outliers, evaluation_event_count
    )
    prediction = _PredictionAggregate(200_000, config.experiment.seed)
    per_plug_prediction: defaultdict[PlugKey, _PredictionAggregate] = defaultdict(
        lambda: _PredictionAggregate(10_000, config.experiment.seed)
    )
    forced = {
        "invalid_value": 0,
        "full_tx": 0,
        "missing_prediction": 0,
        "inactive_reactivation": 0,
        "drift_detected": 0,
        "periodic_sync_count": 0,
        "periodic_sync_time": 0,
    }
    update_rows: list[dict[str, Any]] = []
    drift_rows: list[dict[str, Any]] = []
    resync_rows: list[dict[str, Any]] = []
    bias_rows: list[dict[str, Any]] = []
    prediction_outliers: list[tuple[float, int, int, dict[str, Any]]] = []
    completed_episodes: list[_RecoveryEpisode] = []
    active_episodes: dict[PlugKey, _RecoveryEpisode] = {}
    trigger_standardized: list[float] = []
    silence_durations: list[float] = []
    processed_events = 0
    next_progress_percent = 10
    redundant_drift = 0
    additional_drift = 0
    additional_periodic = 0
    cooldown_suppressed = 0
    divergence_count = 0
    maximum_suppressions = 0
    trace_config = config.artifacts.predictor_trace

    evaluation_start = config.splits.evaluation.start
    initial_time = evaluation_start - timedelta(microseconds=1)
    _publish_allocation(
        allocator_mode,
        initial_time,
        active,
        residual_state,
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
    next_allocation = evaluation_start + timedelta(seconds=config.replay.allocation_period_seconds)

    for batch in evaluation_batches:
        if not batch:
            continue
        timestamp = batch[0].timestamp
        pending: list[_PendingDecision] = []
        actual_house = 0.0
        reconstructed_house = 0.0
        transmitted_count = 0
        for event in batch:
            if event.timestamp != timestamp:
                raise ValueError("adaptive replay received a non-atomic timestamp batch")
            edge_prediction, server_prediction = runtime.predict(event.plug_key, timestamp)
            threshold = timeline.threshold(event.plug_key, timestamp)
            score = residual_state.score(event.plug_key)
            cap = timeline.current.caps.get(event.plug_key) if timeline.current else None
            last_seen = active.last_seen.get(event.plug_key)
            predictor_state = runtime.state(event.plug_key)
            decision_prediction = (
                edge_prediction if mode_is_oracle(predictor_mode) else server_prediction
            )
            innovation = event.value - edge_prediction if edge_prediction is not None else 0.0
            normal_would_transmit = edge_prediction is not None and abs(innovation) > threshold
            drift_evaluation: DriftEvaluation | None = None
            if edge_prediction is not None:
                drift_evaluation = evaluate_drift(
                    drift_states[event.plug_key],
                    innovation,
                    scale_model.scale(event.plug_key),
                    config.predictor.adaptive.drift,
                    enabled=mode_uses_drift(predictor_mode),
                )
            reason = _decision_reason(
                event=event,
                prediction=edge_prediction,
                timestamp=timestamp,
                evaluation_start=evaluation_start,
                active=active,
                predictor_state=predictor_state,
                drift=drift_evaluation,
                normal_would_transmit=normal_would_transmit,
                predictor_mode=predictor_mode,
                config=config,
                allocator_mode=allocator_mode,
            )
            transmitted = reason is not None
            reconstructed = (
                event.value if transmitted or decision_prediction is None else decision_prediction
            )
            if reason in forced:
                forced[reason] += 1
            actual_house += event.value
            reconstructed_house += reconstructed
            transmitted_count += int(transmitted)
            pending.append(
                _PendingDecision(
                    event=event,
                    edge_prediction=edge_prediction,
                    server_prediction=server_prediction,
                    innovation=innovation,
                    threshold=threshold,
                    transmitted=transmitted,
                    reason=reason,
                    normal_would_transmit=normal_would_transmit,
                    drift=drift_evaluation,
                    standardized_innovation=(
                        drift_evaluation.standardized_innovation
                        if drift_evaluation is not None
                        else None
                    ),
                    residual_score=score,
                    cap=cap,
                    last_seen=last_seen,
                    prior_suppressions=predictor_state.consecutive_suppressions,
                    prior_last_transmitted=predictor_state.last_transmitted_timestamp,
                    prior_edge_bias=predictor_state.edge_bias,
                    prior_server_bias=predictor_state.server_bias,
                )
            )

        house_error = actual_house - reconstructed_house
        current = timeline.current
        allocated_budget = current.used_budget if current is not None else 0.0
        bound_violation = abs(house_error) > house_budget + config.replay.float_tolerance
        batch_row: dict[str, Any] = {
            "timestamp": timestamp,
            "mode": allocator_mode,
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
        for item in pending:
            event = item.event
            decision_prediction = (
                item.edge_prediction if mode_is_oracle(predictor_mode) else item.server_prediction
            )
            reconstructed = (
                event.value
                if item.transmitted or decision_prediction is None
                else decision_prediction
            )
            consecutive = 0 if item.transmitted else item.prior_suppressions + 1
            event_rows.append(
                {
                    "event_id": event.event_id,
                    "timestamp": timestamp,
                    "house_id": event.house_id,
                    "household_id": event.household_id,
                    "plug_id": event.plug_id,
                    "original_row_index": event.original_row_index,
                    "actual": event.value,
                    "prediction": decision_prediction,
                    "residual": item.innovation if item.edge_prediction is not None else None,
                    "threshold_used": item.threshold,
                    "transmitted": item.transmitted,
                    "suppressed": not item.transmitted,
                    "reconstructed": reconstructed,
                    "reconstruction_error": event.value - reconstructed,
                    "forced_reason": item.reason,
                    "forced_transmit_reason": item.reason,
                    "decision": "transmit" if item.transmitted else "suppress",
                    "residual_score": item.residual_score,
                    "cap": item.cap,
                    "consecutive_suppression": consecutive,
                    "last_seen_timestamp": item.last_seen,
                    "house_error": house_error,
                    "abs_house_error": abs(house_error),
                    "bound_utilization": (
                        abs(house_error) / house_budget if house_budget > 0 else 0.0
                    ),
                    "mode": allocator_mode,
                    "predictor_mode": predictor_mode,
                }
            )
        collector.observe(batch_row, event_rows, config.replay.float_tolerance)

        for item, _event_row in zip(pending, event_rows, strict=True):
            event = item.event
            valid_actual = math.isfinite(event.value) and event.value >= 0.0
            if item.edge_prediction is not None and valid_actual:
                prediction.observe(item.innovation)
                per_plug_prediction[event.plug_key].observe(item.innovation)
                _observe_prediction_outlier(
                    prediction_outliers,
                    config.replay.top_k_outliers,
                    item,
                    predictor_mode,
                )
                episode = active_episodes.get(event.plug_key)
                if episode is not None:
                    episode.observe(
                        timestamp, abs(item.innovation), scale_model.scale(event.plug_key)
                    )

            predictor_state = runtime.state(event.plug_key)
            if item.reason == "drift_detected":
                predictor_state.drift_trigger_count += 1
                predictor_state.last_drift_timestamp = timestamp
                trigger_standardized.append(float(item.standardized_innovation or 0.0))
                redundant_drift += int(item.normal_would_transmit)
                additional_drift += int(not item.normal_would_transmit)
                previous_episode = active_episodes.get(event.plug_key)
                if previous_episode is not None:
                    completed_episodes.append(previous_episode)
                active_episodes[event.plug_key] = _RecoveryEpisode(
                    event.plug_key, timestamp, abs(item.innovation)
                )
                drift_rows.append(
                    _trigger_row(item, predictor_mode, scale_model.scale(event.plug_key))
                )
            elif item.reason in {"periodic_sync_count", "periodic_sync_time"}:
                additional_periodic += int(not item.normal_would_transmit)
                if item.reason == "periodic_sync_count":
                    predictor_state.periodic_sync_count += 1
                else:
                    predictor_state.periodic_sync_time_count += 1
                resync_rows.append(_resynchronization_row(item, predictor_mode))
            if item.drift is not None:
                cooldown_suppressed += int(item.drift.suppressed_by_cooldown)
                commit_drift(drift_states[event.plug_key], item.drift)

            before_edge = predictor_state.edge_bias
            before_server = predictor_state.server_bias
            updated = False
            if item.edge_prediction is not None and valid_actual:
                updated = runtime.commit(
                    event.plug_key,
                    item.innovation,
                    transmitted=item.transmitted,
                    timestamp=timestamp,
                )
                if item.transmitted:
                    anchor = item.prior_last_transmitted or evaluation_start
                    silence_durations.append(max((timestamp - anchor).total_seconds(), 0.0))
                if item.transmitted:
                    residual_state.observe_transmitted(event.plug_key, item.innovation)
                else:
                    residual_state.observe_suppressed(
                        event.plug_key,
                        item.threshold,
                        exact_residual=(
                            item.innovation if config.residual.estimator == "exact" else None
                        ),
                    )
            predictor_state.last_prediction = item.server_prediction
            maximum_suppressions = max(
                maximum_suppressions, predictor_state.consecutive_suppressions
            )
            if updated:
                update_rows.append(
                    {
                        "timestamp": timestamp,
                        "event_id": event.event_id,
                        "household_id": event.household_id,
                        "plug_id": event.plug_id,
                        "predictor_mode": predictor_mode,
                        "reason": item.reason,
                        "innovation": item.innovation,
                        "edge_bias_before": before_edge,
                        "edge_bias_after": predictor_state.edge_bias,
                        "server_bias_before": before_server,
                        "server_bias_after": predictor_state.server_bias,
                        "predictor_state_version": predictor_state.version,
                    }
                )
            if _include_bias_trace(
                processed_events + 1,
                updated,
                item.reason,
                trace_config,
            ):
                bias_rows.append(
                    {
                        "timestamp": timestamp,
                        "event_id": event.event_id,
                        "household_id": event.household_id,
                        "plug_id": event.plug_id,
                        "predictor_mode": predictor_mode,
                        "edge_bias": predictor_state.edge_bias,
                        "server_bias": predictor_state.server_bias,
                        "warmup_initial_bias": predictor_state.warmup_initial_bias,
                        "prediction": item.edge_prediction,
                        "innovation": item.innovation,
                        "transmitted": item.transmitted,
                        "reason": item.reason,
                        "predictor_state_version": predictor_state.version,
                    }
                )
            processed_events += 1
        active.observe_batch(batch)
        if runtime.deployable:
            divergence_count += runtime.divergence_count(config.replay.float_tolerance)

        while (
            next_progress_percent <= 100
            and processed_events * 100 >= evaluation_event_count * next_progress_percent
        ):
            notify_progress(
                progress,
                f"predictor {predictor_mode}: replay {next_progress_percent}% "
                f"({processed_events:,}/{evaluation_event_count:,} events)",
            )
            next_progress_percent += 10
        if timestamp >= next_allocation:
            _publish_allocation(
                allocator_mode,
                timestamp,
                active,
                residual_state,
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
            f"adaptive replay expected {evaluation_event_count} events, got {processed_events}"
        )
    completed_episodes.extend(active_episodes.values())
    base = collector.finish(threshold_rows, forced)
    predictor_summary, plug_rows = _build_predictor_summary(
        predictor_mode,
        runtime,
        prediction,
        per_plug_prediction,
        base,
        drift_rows,
        resync_rows,
        completed_episodes,
        trigger_standardized,
        silence_durations,
        redundant_drift,
        additional_drift,
        additional_periodic,
        cooldown_suppressed,
        maximum_suppressions,
        divergence_count,
    )
    return AdaptiveReplayResult(
        base=base,
        predictor_mode=predictor_mode,
        deployable=runtime.deployable,
        predictor_summary=predictor_summary,
        predictor_plug_rows=tuple(plug_rows),
        predictor_update_rows=tuple(update_rows),
        drift_event_rows=tuple(drift_rows),
        resynchronization_rows=tuple(resync_rows),
        recovery_rows=tuple(episode.row() for episode in completed_episodes),
        bias_trace_rows=tuple(bias_rows),
        top_prediction_outlier_rows=tuple(
            row
            for _error, _timestamp_key, _row_index, row in sorted(prediction_outliers, reverse=True)
        ),
    )


def _decision_reason(
    *,
    event: Event,
    prediction: float | None,
    timestamp: datetime,
    evaluation_start: datetime,
    active: ActiveSet,
    predictor_state: Any,
    drift: DriftEvaluation | None,
    normal_would_transmit: bool,
    predictor_mode: PredictorMode,
    config: AppConfig,
    allocator_mode: Mode = "uniform",
) -> str | None:
    if not math.isfinite(event.value) or event.value < 0:
        return "invalid_value"
    if prediction is None:
        return "missing_prediction"
    if active.is_reactivation(event.plug_key, timestamp):
        return "inactive_reactivation"
    if drift is not None and drift.trigger:
        return "drift_detected"
    resync = config.predictor.adaptive.resynchronization
    if mode_uses_periodic_sync(predictor_mode):
        maximum = resync.max_consecutive_suppressions
        if maximum is not None and predictor_state.consecutive_suppressions >= maximum:
            return "periodic_sync_count"
        maximum_silence = resync.max_silence_seconds
        anchor = predictor_state.last_transmitted_timestamp or evaluation_start
        if maximum_silence is not None and (timestamp - anchor).total_seconds() >= maximum_silence:
            return "periodic_sync_time"
    if allocator_mode == "full_tx":
        return "full_tx"
    if normal_would_transmit:
        return "normal_deadband"
    return None


def _build_predictor_summary(
    mode: PredictorMode,
    runtime: DualPredictorRuntime,
    prediction: _PredictionAggregate,
    per_plug: dict[PlugKey, _PredictionAggregate],
    base: StreamingReplayResult,
    drift_rows: list[dict[str, Any]],
    resync_rows: list[dict[str, Any]],
    episodes: list[_RecoveryEpisode],
    trigger_standardized: list[float],
    silence_durations: list[float],
    redundant_drift: int,
    additional_drift: int,
    additional_periodic: int,
    cooldown_suppressed: int,
    maximum_suppressions: int,
    divergence_count: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    summary: dict[str, Any] = dict(prediction.metrics())
    states = list(runtime.states.values())
    absolute_biases = [abs(state.server_bias) for state in states]
    updates = sum(state.predictor_update_count for state in states)
    sync_count = sum(state.periodic_sync_count for state in states)
    sync_time = sum(state.periodic_sync_time_count for state in states)
    recovery_times = [
        float(row["recovery_time_after_drift"])
        for episode in episodes
        if (row := episode.row())["recovery_time_after_drift"] is not None
    ]
    resync_error_reductions = [
        episode.pre_trigger_absolute_error - episode.error_after_1
        for episode in episodes
        if episode.error_after_1 is not None
    ]
    trigger_values = np.asarray(trigger_standardized, dtype=float)
    silence = np.asarray(silence_durations, dtype=float)
    summary.update(
        {
            "predictor_mode": mode,
            "deployable": runtime.deployable,
            "oracle_diagnostic": mode_is_oracle(mode),
            "predictor_update_count": updates,
            "predictor_update_rate": updates / base.valid_events if base.valid_events else 0.0,
            "mean_abs_adaptive_bias": float(np.mean(absolute_biases)) if absolute_biases else 0.0,
            "max_abs_adaptive_bias": max(absolute_biases, default=0.0),
            "predictor_state_divergence_count": divergence_count,
            "drift_trigger_count": len(drift_rows),
            "drift_trigger_rate": len(drift_rows) / base.valid_events if base.valid_events else 0.0,
            "drift_triggers_by_plug": _count_rows_by_plug(drift_rows),
            "redundant_drift_trigger_count": redundant_drift,
            "additional_drift_transmission_count": additional_drift,
            "median_standardized_innovation_at_trigger": (
                float(np.median(trigger_values)) if trigger_values.size else 0.0
            ),
            "max_standardized_innovation_at_trigger": (
                float(np.max(trigger_values)) if trigger_values.size else 0.0
            ),
            "detector_cooldown_suppression_count": cooldown_suppressed,
            "periodic_sync_count_trigger_count": sync_count,
            "periodic_sync_time_trigger_count": sync_time,
            "additional_periodic_transmission_count": additional_periodic,
            "maximum_consecutive_suppressions_observed": maximum_suppressions,
            "mean_silence_seconds": float(np.mean(silence)) if silence.size else 0.0,
            "p95_silence_seconds": (float(np.quantile(silence, 0.95)) if silence.size else 0.0),
            "max_silence_seconds": float(np.max(silence)) if silence.size else 0.0,
            "transmission_increase_due_to_synchronization": (
                additional_periodic / base.valid_events if base.valid_events else 0.0
            ),
            "median_recovery_time_after_drift": (
                float(np.median(recovery_times)) if recovery_times else 0.0
            ),
            "p95_recovery_time_after_drift": (
                float(np.quantile(recovery_times, 0.95)) if recovery_times else 0.0
            ),
            "max_recovery_time_after_drift": max(recovery_times, default=0.0),
            "mean_error_reduction_after_resync": (
                float(np.mean(resync_error_reductions)) if resync_error_reductions else 0.0
            ),
            "number_of_unresolved_drift_episodes": sum(
                episode.recovered_timestamp is None for episode in episodes
            ),
        }
    )
    plug_rows: list[dict[str, Any]] = []
    for plug, aggregate in sorted(per_plug.items()):
        row = dict(aggregate.metrics())
        state = runtime.state(plug)
        row.update(
            {
                "household_id": plug[0],
                "plug_id": plug[1],
                "predictor_mode": mode,
                "warmup_initial_bias": state.warmup_initial_bias,
                "final_edge_bias": state.edge_bias,
                "final_server_bias": state.server_bias,
                "predictor_update_count": state.predictor_update_count,
                "drift_trigger_count": state.drift_trigger_count,
                "periodic_sync_count": state.periodic_sync_count,
                "periodic_sync_time_count": state.periodic_sync_time_count,
            }
        )
        plug_rows.append(row)
    return summary, plug_rows


def _trigger_row(item: _PendingDecision, mode: PredictorMode, scale: float) -> dict[str, Any]:
    return {
        "timestamp": item.event.timestamp,
        "event_id": item.event.event_id,
        "household_id": item.event.household_id,
        "plug_id": item.event.plug_id,
        "predictor_mode": mode,
        "prior_consecutive_suppressions": item.prior_suppressions,
        "innovation": item.innovation,
        "absolute_innovation": abs(item.innovation),
        "robust_scale": scale,
        "standardized_innovation": item.standardized_innovation,
        "normal_would_transmit": item.normal_would_transmit,
        "additional_transmission": not item.normal_would_transmit,
        "cooldown_before": None,
    }


def _resynchronization_row(item: _PendingDecision, mode: PredictorMode) -> dict[str, Any]:
    return {
        "timestamp": item.event.timestamp,
        "event_id": item.event.event_id,
        "household_id": item.event.household_id,
        "plug_id": item.event.plug_id,
        "predictor_mode": mode,
        "reason": item.reason,
        "prior_consecutive_suppressions": item.prior_suppressions,
        "prior_last_transmitted_timestamp": item.prior_last_transmitted,
        "normal_would_transmit": item.normal_would_transmit,
        "additional_transmission": not item.normal_would_transmit,
    }


def _observe_prediction_outlier(
    heap: list[tuple[float, int, int, dict[str, Any]]],
    top_k: int,
    item: _PendingDecision,
    mode: PredictorMode,
) -> None:
    timestamp_key = -_timestamp_microseconds(item.event.timestamp)
    row = {
        "timestamp": item.event.timestamp,
        "event_id": item.event.event_id,
        "household_id": item.event.household_id,
        "plug_id": item.event.plug_id,
        "actual": item.event.value,
        "prediction": item.edge_prediction,
        "innovation": item.innovation,
        "absolute_prediction_error": abs(item.innovation),
        "threshold_used": item.threshold,
        "transmitted": item.transmitted,
        "reason": item.reason,
        "predictor_mode": mode,
        "prior_consecutive_suppressions": item.prior_suppressions,
        "standardized_innovation": item.standardized_innovation,
    }
    candidate = (
        abs(item.innovation),
        timestamp_key,
        -item.event.original_row_index,
        row,
    )
    if len(heap) < top_k:
        heapq.heappush(heap, candidate)
    elif candidate[:3] > heap[0][:3]:
        heapq.heapreplace(heap, candidate)


def _include_bias_trace(
    event_index: int,
    updated: bool,
    reason: str | None,
    config: Any,
) -> bool:
    if not config.enabled:
        return False
    if event_index % config.sample_every_n_events == 0:
        return True
    if updated and config.always_include_updates:
        return True
    if reason == "drift_detected" and config.always_include_drift:
        return True
    return reason in {"periodic_sync_count", "periodic_sync_time"} and config.always_include_resync


def _count_rows_by_plug(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: defaultdict[str, int] = defaultdict(int)
    for row in rows:
        counts[f"{row['household_id']}:{row['plug_id']}"] += 1
    return dict(sorted(counts.items()))


def _sample_cvar(values: np.ndarray, quantile: float) -> float:
    if not values.size:
        return 0.0
    threshold = np.quantile(values, quantile)
    return float(np.mean(values[values >= threshold]))


def _top_fraction_share(squared_values: np.ndarray, total: float, fraction: float) -> float:
    if not squared_values.size or total <= 0:
        return 0.0
    count = max(int(np.ceil(squared_values.size * fraction)), 1)
    largest = np.partition(squared_values, squared_values.size - count)[-count:]
    return float(np.sum(largest) / total)


def _timestamp_microseconds(timestamp: datetime) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = timestamp.astimezone(UTC) - epoch
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
