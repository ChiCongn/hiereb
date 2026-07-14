"""End-to-end experiment orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import polars as pl

from hiereb.config import AppConfig, Mode
from hiereb.data.loader import LoadedData, load_events
from hiereb.data.splits import mean_event_aligned_house_load, split_events
from hiereb.data.streaming import (
    StreamingPreparedExperiment,
    iter_evaluation_batches,
    prepare_streaming,
)
from hiereb.domain.models import Event
from hiereb.evaluation.artifacts import (
    file_sha256,
    write_artifacts,
    write_streaming_artifacts,
)
from hiereb.evaluation.metrics import evaluate, evaluate_streaming
from hiereb.predictor.slot_median import TimeSliceMedianPredictor
from hiereb.residual.state import fit_warmup_residuals
from hiereb.simulation.replay import ReplayResult, replay
from hiereb.simulation.streaming_replay import StreamingReplayResult, replay_streaming
from hiereb.utils.progress import ProgressCallback, notify_progress


@dataclass(frozen=True, slots=True)
class PreparedExperiment:
    loaded: LoadedData
    warmup: tuple[Event, ...]
    evaluation: tuple[Event, ...]
    validation: tuple[Event, ...]
    predictor: TimeSliceMedianPredictor
    house_budget: float
    sigma_floor: float
    squared_residuals: dict[tuple[int, int], tuple[float, ...]]
    absolute_residuals: dict[tuple[int, int], tuple[float, ...]]
    mean_warmup_house_load: float
    input_sha256: str
    cap_quantiles: dict[float, dict[tuple[int, int], float]]
    cap_sample_counts: dict[tuple[int, int], int]


def prepare(config: AppConfig, progress: ProgressCallback | None = None) -> PreparedExperiment:
    notify_progress(progress, "prepare: loading and filtering input")
    loaded = load_events(config.data, config.splits, progress)
    notify_progress(progress, "prepare: creating warm-up/validation/evaluation splits")
    splits = split_events(loaded.events, config.splits)
    notify_progress(
        progress,
        f"prepare: split sizes warmup={len(splits.warmup):,}, "
        f"validation={len(splits.validation):,}, evaluation={len(splits.evaluation):,}",
    )
    notify_progress(progress, "prepare: fitting time-slice median predictor")
    predictor = TimeSliceMedianPredictor(config.predictor.slot_seconds)
    predictor.fit(splits.warmup)
    notify_progress(progress, "prepare: fitting residual floor and warm-up cap statistics")
    sigma_floor, squared, absolute = fit_warmup_residuals(
        splits.warmup,
        predictor,
        config.residual.sigma_floor_percentile,
        config.residual.rolling_window_size,
    )
    mean_load = mean_event_aligned_house_load(splits.warmup)
    budget = config.budget.epsilon_ratio * mean_load
    quantiles = {0.90, 0.95, 0.99, config.cap.quantile}
    cap_quantiles = {
        quantile: {plug: float(np.quantile(values, quantile)) for plug, values in absolute.items()}
        for quantile in quantiles
    }
    cap_sample_counts = {plug: len(values) for plug, values in absolute.items()}
    compact_loaded = LoadedData((), loaded.filter_counts, loaded.input_path)
    notify_progress(progress, f"prepare: hashing input {config.data.path}")
    input_sha256 = file_sha256(config.data.path)
    notify_progress(
        progress,
        f"prepare: ready; house_budget={budget:.6f}, sigma_floor={sigma_floor:.6f}",
    )
    return PreparedExperiment(
        compact_loaded,
        splits.warmup,
        splits.evaluation,
        splits.validation,
        predictor,
        budget,
        sigma_floor,
        squared,
        {},
        mean_load,
        input_sha256,
        cap_quantiles,
        cap_sample_counts,
    )


def execute_mode(
    config: AppConfig,
    mode: Mode,
    prepared: PreparedExperiment | None = None,
    *,
    write: bool = True,
    progress: ProgressCallback | None = None,
) -> tuple[ReplayResult, dict[str, Any]]:
    prepared = prepared or prepare(config, progress)
    notify_progress(
        progress,
        f"mode {mode}: replay start ({len(prepared.evaluation):,} evaluation events)",
    )
    result = replay(
        mode,
        prepared.evaluation,
        prepared.warmup,
        prepared.predictor,
        config,
        prepared.house_budget,
        prepared.sigma_floor,
        prepared.squared_residuals,
        prepared.absolute_residuals,
        prepared.cap_quantiles.get(config.cap.quantile),
        prepared.cap_sample_counts,
        progress,
    )
    notify_progress(progress, f"mode {mode}: computing metrics")
    summary = evaluate(result, prepared.house_budget)
    if write:
        output_dir = config.experiment.output_dir / mode
        notify_progress(progress, f"mode {mode}: writing artifacts to {output_dir}")
        write_artifacts(
            output_dir,
            config,
            result,
            summary,
            prepared.loaded.filter_counts,
            prepared.input_sha256,
        )
    notify_progress(
        progress,
        f"mode {mode}: done; TR={summary['transmission_ratio']:.6f}, "
        f"RMSE={summary['rmse']:.6f}, Max={summary['max']:.6f}",
    )
    return result, summary


def execute_mode_streaming(
    config: AppConfig,
    mode: Mode,
    prepared: StreamingPreparedExperiment | None = None,
    *,
    write: bool = True,
    progress: ProgressCallback | None = None,
) -> tuple[StreamingReplayResult, dict[str, Any]]:
    """Execute one mode with bounded-memory input and replay state."""
    prepared = prepared or prepare_streaming(config, progress)
    notify_progress(
        progress,
        f"mode {mode}: bounded-memory replay start "
        f"({prepared.evaluation_event_count:,} evaluation events)",
    )
    result = replay_streaming(
        mode,
        iter_evaluation_batches(prepared, config),
        prepared.predictor,
        config,
        prepared.house_budget,
        prepared.sigma_floor,
        prepared.squared_residuals,
        prepared.initial_last_seen,
        prepared.evaluation_event_count,
        prepared.cap_quantiles.get(config.cap.quantile),
        prepared.cap_sample_counts,
        progress,
    )
    notify_progress(progress, f"mode {mode}: computing compact metrics")
    summary = evaluate_streaming(result, prepared.house_budget)
    if write:
        output_dir = config.experiment.output_dir / mode
        notify_progress(progress, f"mode {mode}: writing artifacts to {output_dir}")
        write_streaming_artifacts(
            output_dir,
            config,
            result,
            summary,
            prepared.filter_counts,
            prepared.input_sha256,
        )
    notify_progress(
        progress,
        f"mode {mode}: done; TR={summary['transmission_ratio']:.6f}, "
        f"RMSE={summary['rmse']:.6f}, Max={summary['max']:.6f}",
    )
    return result, summary


def compare_modes(
    config: AppConfig, progress: ProgressCallback | None = None
) -> list[dict[str, Any]]:
    if config.data.streaming.enabled:
        return _compare_modes_streaming(config, progress)
    notify_progress(
        progress,
        f"run: preparing once for {len(config.experiment.modes)} configured modes",
    )
    prepared = prepare(config, progress)
    summaries: list[dict[str, Any]] = []
    for index, mode in enumerate(config.experiment.modes, start=1):
        notify_progress(
            progress,
            f"run: mode {index}/{len(config.experiment.modes)} -> {mode}",
        )
        summaries.append(execute_mode(config, mode, prepared, progress=progress)[1])
    config.experiment.output_dir.mkdir(parents=True, exist_ok=True)
    flat = [{k: v for k, v in row.items() if not isinstance(v, dict)} for row in summaries]
    pl.DataFrame(flat).write_csv(config.experiment.output_dir / "comparison.csv")
    notify_progress(
        progress,
        f"run: comparison written to {config.experiment.output_dir / 'comparison.csv'}",
    )
    return summaries


def _compare_modes_streaming(
    config: AppConfig, progress: ProgressCallback | None
) -> list[dict[str, Any]]:
    notify_progress(
        progress,
        f"run: streaming preparation once for {len(config.experiment.modes)} configured modes",
    )
    prepared = prepare_streaming(config, progress)
    summaries: list[dict[str, Any]] = []
    for index, mode in enumerate(config.experiment.modes, start=1):
        notify_progress(
            progress,
            f"run: streaming mode {index}/{len(config.experiment.modes)} -> {mode}",
        )
        summaries.append(execute_mode_streaming(config, mode, prepared, progress=progress)[1])
    config.experiment.output_dir.mkdir(parents=True, exist_ok=True)
    flat = [
        {key: value for key, value in row.items() if not isinstance(value, dict)}
        for row in summaries
    ]
    pl.DataFrame(flat).write_csv(config.experiment.output_dir / "comparison.csv")
    notify_progress(
        progress,
        f"run: comparison written to {config.experiment.output_dir / 'comparison.csv'}",
    )
    return summaries


def verify_legacy_equivalence(
    config: AppConfig, tolerance: float | None = None
) -> dict[str, float | int | bool]:
    if config.data.streaming.enabled:
        return _verify_legacy_equivalence_streaming(config, tolerance)
    prepared = prepare(config)
    flat, _ = execute_mode(config, "flat_variance", prepared, write=False)
    legacy, _ = execute_mode(config, "legacy_two_stage", prepared, write=False)
    tolerance = tolerance if tolerance is not None else config.replay.float_tolerance
    if len(flat.threshold_rows) != len(legacy.threshold_rows):
        return {
            "equivalent": False,
            "threshold_rows": len(flat.threshold_rows),
            "max_difference": float("inf"),
        }
    max_difference = max(
        (
            abs(float(left["threshold"]) - float(right["threshold"]))
            for left, right in zip(flat.threshold_rows, legacy.threshold_rows, strict=True)
        ),
        default=0.0,
    )
    decisions_equal = [row["transmitted"] for row in flat.event_rows] == [
        row["transmitted"] for row in legacy.event_rows
    ]
    return {
        "equivalent": max_difference <= tolerance and decisions_equal,
        "threshold_rows": len(flat.threshold_rows),
        "max_difference": max_difference,
        "decisions_equal": decisions_equal,
    }


def _verify_legacy_equivalence_streaming(
    config: AppConfig, tolerance: float | None
) -> dict[str, float | int | bool]:
    prepared = prepare_streaming(config)
    flat, _ = execute_mode_streaming(config, "flat_variance", prepared, write=False)
    legacy, _ = execute_mode_streaming(config, "legacy_two_stage", prepared, write=False)
    tolerance = tolerance if tolerance is not None else config.replay.float_tolerance
    if len(flat.threshold_rows) != len(legacy.threshold_rows):
        return {
            "equivalent": False,
            "threshold_rows": len(flat.threshold_rows),
            "max_difference": float("inf"),
            "decisions_equal": False,
        }
    max_difference = max(
        (
            abs(float(left["threshold"]) - float(right["threshold"]))
            for left, right in zip(flat.threshold_rows, legacy.threshold_rows, strict=True)
        ),
        default=0.0,
    )
    decisions_equal = (
        flat.valid_events == legacy.valid_events and flat.decision_sha256 == legacy.decision_sha256
    )
    return {
        "equivalent": max_difference <= tolerance and decisions_equal,
        "threshold_rows": len(flat.threshold_rows),
        "max_difference": max_difference,
        "decisions_equal": decisions_equal,
    }
