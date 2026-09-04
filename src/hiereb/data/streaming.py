"""Bounded-memory staging, warm-up fitting, and timestamp-batch iteration."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict, deque
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from hiereb.config import AppConfig
from hiereb.data.loader import event_from_row, filtered_event_frame
from hiereb.domain.models import Event, PlugKey
from hiereb.evaluation.artifacts import file_sha256
from hiereb.predictor.adaptive import ewma_bias_transition
from hiereb.predictor.slot_median import TimeSliceMedianPredictor
from hiereb.utils.progress import ProgressCallback, notify_progress

SORT_COLUMNS = ["timestamp", "household_id", "plug_id", "original_row_index"]


@dataclass(frozen=True, slots=True)
class StreamingPreparedExperiment:
    """Small warm-up state plus a reusable sorted on-disk event source."""

    stage_path: Path
    filter_counts: dict[str, int]
    input_sha256: str
    predictor: TimeSliceMedianPredictor
    mean_warmup_house_load: float
    house_budget: float
    sigma_floor: float
    squared_residuals: dict[PlugKey, tuple[float, ...]]
    cap_quantiles: dict[float, dict[PlugKey, float]]
    cap_sample_counts: dict[PlugKey, int]
    warmup_initial_biases: dict[PlugKey, float]
    drift_scales: dict[PlugKey, float]
    household_drift_scales: dict[int, float]
    house_drift_scale: float
    initial_last_seen: dict[PlugKey, datetime]
    warmup_event_count: int
    validation_event_count: int
    evaluation_event_count: int


def prepare_streaming(
    config: AppConfig, progress: ProgressCallback | None = None
) -> StreamingPreparedExperiment:
    """Prepare exact warm-up statistics without materializing raw events."""
    notify_progress(progress, f"streaming: hashing input {config.data.path}")
    input_sha256 = file_sha256(config.data.path)
    notify_progress(progress, f"streaming: input sha256={input_sha256[:12]}...")
    frame, filter_counts = filtered_event_frame(config.data, config.splits, progress)
    stage_path = _stage_path(config, input_sha256)
    stage_path.parent.mkdir(parents=True, exist_ok=True)
    if config.data.streaming.reuse_staging and stage_path.is_file():
        notify_progress(progress, f"streaming: reusing sorted staging file {stage_path}")
    else:
        notify_progress(
            progress,
            "streaming: external sort and Parquet staging "
            f"(chunk_rows={config.data.streaming.chunk_rows:,})",
        )
        frame.sort(SORT_COLUMNS, maintain_order=True).sink_parquet(
            stage_path,
            row_group_size=config.data.streaming.chunk_rows,
            maintain_order=True,
            mkdir=True,
            engine="streaming",
        )
        notify_progress(progress, f"streaming: staged accepted events at {stage_path}")

    staged = pl.scan_parquet(stage_path)
    coverage = staged.select(
        pl.col("timestamp").min().alias("minimum"),
        pl.col("timestamp").max().alias("maximum"),
    ).collect(engine="streaming")
    minimum_timestamp = _as_datetime(coverage["minimum"].item())
    maximum_timestamp = _as_datetime(coverage["maximum"].item())
    if config.splits.validation.enabled and minimum_timestamp > config.splits.warmup.start:
        raise ValueError(
            "input coverage starts after warm-up start: "
            f"{minimum_timestamp.isoformat()} > {config.splits.warmup.start.isoformat()}"
        )
    if config.splits.validation.enabled and maximum_timestamp < config.splits.evaluation.end:
        raise ValueError(
            "input coverage ends before evaluation end: "
            f"{maximum_timestamp.isoformat()} < {config.splits.evaluation.end.isoformat()}"
        )
    warmup = staged.filter(
        pl.col("timestamp").is_between(
            config.splits.warmup.start,
            config.splits.warmup.end,
            closed="both",
        )
    )
    evaluation = staged.filter(
        pl.col("timestamp").is_between(
            config.splits.evaluation.start,
            config.splits.evaluation.end,
            closed="both",
        )
    )
    warmup_count = _row_count(warmup)
    evaluation_count = _row_count(evaluation)
    validation_count = 0
    if config.splits.validation.enabled:
        assert config.splits.validation.start is not None
        assert config.splits.validation.end is not None
        validation_count = _row_count(
            staged.filter(
                pl.col("timestamp").is_between(
                    config.splits.validation.start,
                    config.splits.validation.end,
                    closed="both",
                )
            )
        )
    if warmup_count == 0:
        raise ValueError("warm-up split is empty")
    if config.splits.validation.enabled and validation_count == 0:
        raise ValueError("validation split is empty")
    if evaluation_count == 0:
        raise ValueError("evaluation split is empty")
    notify_progress(
        progress,
        f"streaming: split sizes warmup={warmup_count:,}, "
        f"validation={validation_count:,}, evaluation={evaluation_count:,}",
    )

    predictor, residual_frame = _fit_predictor(warmup, config, progress)
    (
        sigma_floor,
        squared,
        cap_quantiles,
        cap_counts,
        drift_scales,
        household_drift_scales,
        house_drift_scale,
    ) = _fit_residual_statistics(residual_frame, config, progress)
    initial_biases = initialize_adaptive_biases(stage_path, predictor, config)
    mean_load = float(
        warmup.group_by("timestamp", maintain_order=True)
        .agg(pl.col("value").sum().alias("house_load"))
        .select(pl.col("house_load").mean())
        .collect(engine="streaming")
        .item()
    )
    last_seen_rows = (
        warmup.group_by("household_id", "plug_id")
        .agg(pl.col("timestamp").max().alias("last_seen"))
        .collect(engine="streaming")
    )
    initial_last_seen = {
        (int(row["household_id"]), int(row["plug_id"])): _as_datetime(row["last_seen"])
        for row in last_seen_rows.iter_rows(named=True)
    }
    house_budget = config.budget.epsilon_ratio * mean_load
    notify_progress(
        progress,
        f"streaming: warm-up ready; house_budget={house_budget:.6f}, sigma_floor={sigma_floor:.6f}",
    )
    return StreamingPreparedExperiment(
        stage_path=stage_path,
        filter_counts=filter_counts,
        input_sha256=input_sha256,
        predictor=predictor,
        mean_warmup_house_load=mean_load,
        house_budget=house_budget,
        sigma_floor=sigma_floor,
        squared_residuals=squared,
        cap_quantiles=cap_quantiles,
        cap_sample_counts=cap_counts,
        warmup_initial_biases=initial_biases,
        drift_scales=drift_scales,
        household_drift_scales=household_drift_scales,
        house_drift_scale=house_drift_scale,
        initial_last_seen=initial_last_seen,
        warmup_event_count=warmup_count,
        validation_event_count=validation_count,
        evaluation_event_count=evaluation_count,
    )


def iter_evaluation_batches(
    prepared: StreamingPreparedExperiment,
    config: AppConfig,
) -> Iterator[tuple[Event, ...]]:
    """Yield complete, globally sorted timestamp batches across chunk boundaries."""
    query = pl.scan_parquet(prepared.stage_path).filter(
        pl.col("timestamp").is_between(
            config.splits.evaluation.start,
            config.splits.evaluation.end,
            closed="both",
        )
    )
    pending: list[Event] = []
    pending_timestamp: datetime | None = None
    for frame in query.collect_batches(
        chunk_size=config.data.streaming.chunk_rows,
        maintain_order=True,
        engine="streaming",
    ):
        for row in frame.iter_rows(named=True):
            event = event_from_row(row)
            if pending_timestamp is not None and event.timestamp != pending_timestamp:
                yield tuple(pending)
                pending = []
            pending.append(event)
            pending_timestamp = event.timestamp
    if pending:
        yield tuple(pending)


def _fit_predictor(
    warmup: pl.LazyFrame,
    config: AppConfig,
    progress: ProgressCallback | None,
) -> tuple[TimeSliceMedianPredictor, pl.LazyFrame]:
    notify_progress(progress, "streaming: fitting exact warm-up medians")
    slotted = _add_slot_columns(warmup, config.predictor.slot_seconds)
    slot_keys = ["household_id", "plug_id", "_weekday", "_slot"]
    slot_stats = (
        slotted.group_by(slot_keys)
        .agg(pl.col("value").median().alias("_slot_median"))
        .collect(engine="streaming")
    )
    plug_stats = (
        warmup.group_by("household_id", "plug_id")
        .agg(pl.col("value").median().alias("_plug_median"))
        .collect(engine="streaming")
    )
    slot_medians = {
        (
            (int(row["household_id"]), int(row["plug_id"])),
            (int(row["_weekday"]), int(row["_slot"])),
        ): float(row["_slot_median"])
        for row in slot_stats.iter_rows(named=True)
    }
    plug_medians = {
        (int(row["household_id"]), int(row["plug_id"])): float(row["_plug_median"])
        for row in plug_stats.iter_rows(named=True)
    }
    predictor = TimeSliceMedianPredictor(config.predictor.slot_seconds)
    predictor.fit_statistics(slot_medians, plug_medians)
    residual_frame = (
        slotted.join(slot_stats.lazy(), on=slot_keys, how="left")
        .join(plug_stats.lazy(), on=["household_id", "plug_id"], how="left")
        .with_columns(pl.coalesce("_slot_median", "_plug_median").alias("_prediction"))
        .with_columns((pl.col("value") - pl.col("_prediction")).alias("_residual"))
    )
    return predictor, residual_frame


def _fit_residual_statistics(
    residual_frame: pl.LazyFrame,
    config: AppConfig,
    progress: ProgressCallback | None,
) -> tuple[
    float,
    dict[PlugKey, tuple[float, ...]],
    dict[float, dict[PlugKey, float]],
    dict[PlugKey, int],
    dict[PlugKey, float],
    dict[int, float],
    float,
]:
    notify_progress(progress, "streaming: fitting residual floor, rolling state, and caps")
    sigmas = (
        residual_frame.group_by("household_id", "plug_id")
        .agg((pl.col("_residual").pow(2).mean().sqrt()).alias("sigma"))
        .collect(engine="streaming")
    )
    positive = [float(value) for value in sigmas["sigma"].to_list() if float(value) > 0]
    sigma_floor = (
        float(np.percentile(positive, config.residual.sigma_floor_percentile)) if positive else 1.0
    )
    quantiles = sorted({0.90, 0.95, 0.99, config.cap.quantile})
    expressions: list[pl.Expr] = [pl.len().alias("sample_count")]
    expressions.extend(
        pl.col("_residual").abs().quantile(q, interpolation="linear").alias(f"q_{index}")
        for index, q in enumerate(quantiles)
    )
    cap_rows = (
        residual_frame.group_by("household_id", "plug_id")
        .agg(expressions)
        .collect(engine="streaming")
    )
    cap_counts: dict[PlugKey, int] = {}
    cap_quantiles: dict[float, dict[PlugKey, float]] = {quantile: {} for quantile in quantiles}
    for row in cap_rows.iter_rows(named=True):
        plug = (int(row["household_id"]), int(row["plug_id"]))
        cap_counts[plug] = int(row["sample_count"])
        for index, quantile in enumerate(quantiles):
            cap_quantiles[quantile][plug] = float(row[f"q_{index}"])

    residual_medians = (
        residual_frame.group_by("household_id", "plug_id")
        .agg(pl.col("_residual").median().alias("_residual_median"))
        .collect(engine="streaming")
    )
    mad_rows = (
        residual_frame.join(residual_medians.lazy(), on=["household_id", "plug_id"], how="left")
        .with_columns((pl.col("_residual") - pl.col("_residual_median")).abs().alias("_deviation"))
        .group_by("household_id", "plug_id")
        .agg(pl.col("_deviation").median().alias("mad"))
        .collect(engine="streaming")
    )
    drift_scales: dict[PlugKey, float] = {}
    scales_by_household: defaultdict[int, list[float]] = defaultdict(list)
    for row in mad_rows.iter_rows(named=True):
        plug = (int(row["household_id"]), int(row["plug_id"]))
        scale = max(
            1.4826 * float(row["mad"]),
            sigma_floor,
            config.predictor.adaptive.drift.min_scale,
        )
        drift_scales[plug] = scale
        scales_by_household[plug[0]].append(scale)
    household_scales = {
        household: float(np.median(scales)) for household, scales in scales_by_household.items()
    }
    house_scale = (
        float(np.median(list(drift_scales.values())))
        if drift_scales
        else max(sigma_floor, config.predictor.adaptive.drift.min_scale, 1.0)
    )

    rolling: defaultdict[PlugKey, deque[float]] = defaultdict(
        lambda: deque(maxlen=config.residual.rolling_window_size)
    )
    ordered = residual_frame.select(
        "household_id", "plug_id", "timestamp", "original_row_index", "_residual"
    ).sort(SORT_COLUMNS, maintain_order=True)
    for frame in ordered.collect_batches(
        chunk_size=config.data.streaming.chunk_rows,
        maintain_order=True,
        engine="streaming",
    ):
        for row in frame.iter_rows(named=True):
            plug = (int(row["household_id"]), int(row["plug_id"]))
            residual = float(row["_residual"])
            rolling[plug].append(residual * residual)
    squared = {plug: tuple(values) for plug, values in rolling.items()}
    return (
        sigma_floor,
        squared,
        cap_quantiles,
        cap_counts,
        drift_scales,
        household_scales,
        house_scale,
    )


def initialize_adaptive_biases(
    stage_path: Path,
    predictor: TimeSliceMedianPredictor,
    config: AppConfig,
) -> dict[PlugKey, float]:
    """Replay warm-up exactly to initialize synchronized EWMA bias state."""
    if not (
        config.predictor.adaptive.enabled and config.predictor.adaptive.initialize_bias_from_warmup
    ):
        return {}
    warmup = pl.scan_parquet(stage_path).filter(
        pl.col("timestamp").is_between(
            config.splits.warmup.start,
            config.splits.warmup.end,
            closed="both",
        )
    )
    biases: dict[PlugKey, float] = {}
    for frame in warmup.collect_batches(
        chunk_size=config.data.streaming.chunk_rows,
        maintain_order=True,
        engine="streaming",
    ):
        for row in frame.iter_rows(named=True):
            event = event_from_row(row)
            baseline = predictor.predict(event.plug_key, event.timestamp)
            if baseline is None:
                continue
            bias = biases.get(event.plug_key, 0.0)
            innovation = event.value - (baseline + bias)
            biases[event.plug_key] = ewma_bias_transition(
                bias, innovation, config.predictor.adaptive.alpha
            )
    return biases


def _add_slot_columns(frame: pl.LazyFrame, slot_seconds: int) -> pl.LazyFrame:
    seconds = (
        pl.col("timestamp").dt.hour() * 3600
        + pl.col("timestamp").dt.minute() * 60
        + pl.col("timestamp").dt.second()
    )
    return frame.with_columns(
        (pl.col("timestamp").dt.weekday() - 1).cast(pl.Int8).alias("_weekday"),
        (seconds // slot_seconds).cast(pl.Int32).alias("_slot"),
    )


def _stage_path(config: AppConfig, input_sha256: str) -> Path:
    payload = {
        "input_sha256": input_sha256,
        "house_id": config.data.house_id,
        "property": config.data.load_property_value,
        "timestamp_unit": config.data.timestamp_unit,
        "columns": config.data.columns.model_dump(mode="json"),
        "start": config.splits.warmup.start.isoformat(),
        "end": config.splits.evaluation.end.isoformat(),
    }
    key = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:20]
    return config.data.streaming.staging_dir / f"house-{config.data.house_id}-{key}.parquet"


def _row_count(frame: pl.LazyFrame) -> int:
    return int(frame.select(pl.len()).collect(engine="streaming").item())


def _as_datetime(value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError("streaming timestamp conversion failed")
    return value
