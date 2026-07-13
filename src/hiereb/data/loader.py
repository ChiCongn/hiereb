"""CSV/Parquet ingestion with deterministic filtering and ordering."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

import polars as pl

from hiereb.config import DataConfig, SplitsConfig
from hiereb.domain.models import Event


@dataclass(frozen=True, slots=True)
class LoadedData:
    events: tuple[Event, ...]
    filter_counts: dict[str, int]
    input_path: Path


def _scan(config: DataConfig) -> pl.LazyFrame:
    if not config.path.is_file():
        raise FileNotFoundError(f"input does not exist: {config.path}")
    frame = pl.scan_csv(config.path) if config.format == "csv" else pl.scan_parquet(config.path)
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


def load_events(config: DataConfig, splits: SplitsConfig) -> LoadedData:
    """Load, filter, and stable-sort events for one selected house."""
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
        frame = frame.with_columns(pl.col("original_row_index").cast(pl.String).alias("event_id"))
    else:
        frame = frame.with_columns(pl.col("event_id").cast(pl.String))

    def row_count(lazy: pl.LazyFrame) -> int:
        return int(lazy.select(pl.len()).collect().item())

    counts: dict[str, int] = {"input_rows": row_count(frame)}

    def apply_filter(name: str, predicate: pl.Expr) -> None:
        nonlocal frame
        before = row_count(frame)
        frame = frame.filter(predicate)
        counts[name] = before - row_count(frame)

    apply_filter("wrong_house", pl.col("house_id") == config.house_id)
    if row_count(frame) == 0:
        raise ValueError(f"selected house {config.house_id} has no rows")
    apply_filter("wrong_property", pl.col("property") == config.load_property_value)
    apply_filter(
        "invalid_value",
        pl.col("value").is_not_null() & pl.col("value").is_finite() & (pl.col("value") >= 0),
    )
    overall_start = splits.warmup.start
    overall_end = splits.evaluation.end
    apply_filter(
        "outside_experiment_range",
        pl.col("timestamp").is_not_null()
        & (pl.col("timestamp") >= overall_start)
        & (pl.col("timestamp") <= overall_end),
    )
    counts["accepted_rows"] = row_count(frame)
    if counts["accepted_rows"] == 0:
        raise ValueError("no valid rows remain after filtering")

    collected = frame.sort(
        ["timestamp", "household_id", "plug_id", "original_row_index"], maintain_order=True
    ).collect()
    events = tuple(_row_to_event(row) for row in collected.iter_rows(named=True))
    return LoadedData(events=events, filter_counts=counts, input_path=config.path)


def _row_to_event(row: dict[str, Any]) -> Event:
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
        event_id=str(row["event_id"]),
    )
