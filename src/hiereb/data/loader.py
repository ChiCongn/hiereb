"""CSV/Parquet ingestion with deterministic filtering and ordering."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

import polars as pl

from hiereb.config import DataConfig, SplitsConfig
from hiereb.domain.models import Event
from hiereb.utils.progress import ProgressCallback, notify_progress


@dataclass(frozen=True, slots=True)
class LoadedData:
    events: tuple[Event, ...]
    filter_counts: dict[str, int]
    input_path: Path


def _scan(config: DataConfig) -> pl.LazyFrame:
    """Lazily scan the configured CSV or Parquet file."""
    if not config.path.is_file():
        raise FileNotFoundError(f"input does not exist: {config.path}")

    if config.format == "csv":
        frame = pl.scan_csv(config.path)
    else:
        frame = pl.scan_parquet(config.path)

    # Add the source-file row position before applying any transformations.
    return frame.with_row_index("original_row_index")


def _timestamp_expression(name: str, dtype: pl.DataType, unit: str) -> pl.Expr:
    if dtype.is_temporal():
        expr = pl.col(name).cast(pl.Datetime("us"))
    elif dtype.is_numeric():
        epoch_unit = cast(
            Literal["s", "ms", "us"],
            {"seconds": "s", "milliseconds": "ms", "microseconds": "us"}[unit],
        )
        expr = pl.from_epoch(pl.col(name), time_unit=epoch_unit)
    else:
        expr = pl.col(name).str.to_datetime(strict=False, time_zone="UTC")
    return expr.dt.replace_time_zone("UTC").alias("timestamp")


def filtered_event_frame(
    config: DataConfig,
    splits: SplitsConfig,
    progress: ProgressCallback | None = None,
) -> tuple[pl.LazyFrame, dict[str, int]]:
    """Build the normalized filtered lazy frame shared by both loader paths."""
    notify_progress(progress, f"input: scan {config.path} ({config.format})")
    frame = _scan(config)
    c = config.columns
    required = {c.timestamp, c.value, c.property, c.plug_id, c.household_id, c.house_id}
    input_schema = frame.collect_schema()
    missing = required - set(input_schema.names())
    if missing:
        raise ValueError(f"missing required columns: {sorted(missing)}")

    rename = {
        c.value: "value",
        c.property: "property",
        c.plug_id: "plug_id",
        c.household_id: "household_id",
        c.house_id: "house_id",
    }
    if c.id in input_schema.names():
        rename[c.id] = "event_id"
    frame = frame.rename(rename).with_columns(
        _timestamp_expression(c.timestamp, input_schema[c.timestamp], config.timestamp_unit),
        pl.col("value").cast(pl.Float64, strict=False),
        pl.col("property").cast(pl.Int64, strict=False),
        pl.col("plug_id").cast(pl.Int64, strict=False),
        pl.col("household_id").cast(pl.Int64, strict=False),
        pl.col("house_id").cast(pl.Int64, strict=False),
    )
    if c.id not in input_schema.names():
        frame = frame.with_columns(pl.col("original_row_index").alias("event_id"))

    house_match = (pl.col("house_id") == config.house_id).fill_null(False)
    property_match = (pl.col("property") == config.load_property_value).fill_null(False)
    valid_value = (
        pl.col("value").is_not_null() & pl.col("value").is_finite() & (pl.col("value") >= 0)
    ).fill_null(False)
    in_range = (
        pl.col("timestamp").is_not_null()
        & (pl.col("timestamp") >= splits.warmup.start)
        & (pl.col("timestamp") <= splits.evaluation.end)
    ).fill_null(False)
    accepted = house_match & property_match & valid_value & in_range
    notify_progress(progress, "input: computing filter counts")
    count_row = (
        frame.select(
            pl.len().alias("input_rows"),
            (~house_match).sum().alias("wrong_house"),
            (house_match & ~property_match).sum().alias("wrong_property"),
            (house_match & property_match & ~valid_value).sum().alias("invalid_value"),
            (house_match & property_match & valid_value & ~in_range)
            .sum()
            .alias("outside_experiment_range"),
            accepted.sum().alias("accepted_rows"),
        )
        .collect(engine="streaming")
        .row(0, named=True)
    )
    counts = {name: int(value) for name, value in count_row.items()}
    notify_progress(
        progress,
        "input: filter counts "
        f"input={counts['input_rows']:,}, accepted={counts['accepted_rows']:,}, "
        f"outside_range={counts['outside_experiment_range']:,}",
    )
    if counts["input_rows"] - counts["wrong_house"] == 0:
        raise ValueError(f"selected house {config.house_id} has no rows")
    if counts["accepted_rows"] == 0:
        raise ValueError("no valid rows remain after filtering")

    filtered = frame.filter(accepted).select(
        "event_id",
        "timestamp",
        "value",
        "property",
        "plug_id",
        "household_id",
        "house_id",
        "original_row_index",
    )
    return filtered, counts


def load_events(
    config: DataConfig,
    splits: SplitsConfig,
    progress: ProgressCallback | None = None,
) -> LoadedData:
    """Load, filter, and stable-sort events for one selected house."""
    frame, counts = filtered_event_frame(config, splits, progress)
    notify_progress(progress, "input: materializing and stable-sorting accepted rows")
    collected = frame.sort(
        ["timestamp", "household_id", "plug_id", "original_row_index"],
        maintain_order=True,
    ).collect()
    events = tuple(event_from_row(row) for row in collected.iter_rows(named=True))
    notify_progress(progress, f"input: materialized {len(events):,} events")
    return LoadedData(events=events, filter_counts=counts, input_path=config.path)


def event_from_row(row: dict[str, Any]) -> Event:
    timestamp = row["timestamp"]
    if not isinstance(timestamp, datetime):
        raise ValueError("timestamp conversion failed")
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    return Event(
        timestamp=timestamp,
        value=float(row["value"]),
        property_value=int(row["property"]),
        plug_id=int(row["plug_id"]),
        household_id=int(row["household_id"]),
        house_id=int(row["house_id"]),
        original_row_index=int(row["original_row_index"]),
        event_id=(
            row["event_id"] if isinstance(row["event_id"], (str, int)) else str(row["event_id"])
        ),
    )
