"""End-to-end experiment orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl

from hiereb.config import AppConfig, Mode
from hiereb.data.loader import LoadedData, load_events
from hiereb.data.splits import mean_event_aligned_house_load, split_events
from hiereb.domain.models import Event
from hiereb.evaluation.artifacts import write_artifacts
from hiereb.evaluation.metrics import evaluate
from hiereb.predictor.slot_median import TimeSliceMedianPredictor
from hiereb.residual.state import fit_warmup_residuals
from hiereb.simulation.replay import ReplayResult, replay


@dataclass(frozen=True, slots=True)
class PreparedExperiment:
    loaded: LoadedData
    warmup: tuple[Event, ...]
    evaluation: tuple[Event, ...]
    predictor: TimeSliceMedianPredictor
    house_budget: float
    sigma_floor: float
    squared_residuals: dict[tuple[int, int], tuple[float, ...]]
    absolute_residuals: dict[tuple[int, int], tuple[float, ...]]


def prepare(config: AppConfig) -> PreparedExperiment:
    loaded = load_events(config.data, config.splits)
    splits = split_events(loaded.events, config.splits)
    predictor = TimeSliceMedianPredictor(config.predictor.slot_seconds)
    predictor.fit(splits.warmup)
    sigma_floor, squared, absolute = fit_warmup_residuals(
        splits.warmup, predictor, config.residual.sigma_floor_percentile
    )
    budget = config.budget.epsilon_ratio * mean_event_aligned_house_load(splits.warmup)
    return PreparedExperiment(
        loaded,
        splits.warmup,
        splits.evaluation,
        predictor,
        budget,
        sigma_floor,
        squared,
        absolute,
    )


def execute_mode(
    config: AppConfig,
    mode: Mode,
    prepared: PreparedExperiment | None = None,
    *,
    write: bool = True,
) -> tuple[ReplayResult, dict[str, Any]]:
    prepared = prepared or prepare(config)
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
    )
    summary = evaluate(result, prepared.house_budget)
    if write:
        write_artifacts(
            config.experiment.output_dir / mode,
            config,
            result,
            summary,
            prepared.loaded.filter_counts,
        )
    return result, summary


def compare_modes(config: AppConfig) -> list[dict[str, Any]]:
    prepared = prepare(config)
    summaries = [execute_mode(config, mode, prepared)[1] for mode in config.experiment.modes]
    config.experiment.output_dir.mkdir(parents=True, exist_ok=True)
    flat = [{k: v for k, v in row.items() if not isinstance(v, dict)} for row in summaries]
    pl.DataFrame(flat).write_csv(config.experiment.output_dir / "comparison.csv")
    return summaries


def verify_legacy_equivalence(
    config: AppConfig, tolerance: float | None = None
) -> dict[str, float | int | bool]:
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
