"""
DEBS 2014 CSV loader.
Responsibilities:
- Load and filter raw CSV (Property=0 only, specified houses only)
- Build plug_uid as a globally unique integer per plug
- Yield TimestepBatch objects in ascending timestamp order

NOT responsible for: Kafka publishing, Suppression logic, Prediction lookup
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Generator

import pandas as pd
import structlog

log = structlog.get_logger(__name__)

# DEBS 2014 CSV column names (no header row)
_DEBS_COLUMNS = ["id", "timestamp", "value", "property", "plug_id", "household_id", "house_id"]
_DEBS_DTYPES = {
    "id": int,
    "timestamp": int,
    "value": float,
    "property": int,
    "plug_id": int,
    "household_id": int,
    "house_id": int,
}


@dataclass(frozen=True)
class PlugReading:
    """Single plug measurement at one timestamp."""
    plug_uid: int      # globally unique: house*100000 + household*1000 + plug
    timestamp: int
    value: float       # Watts (Property=0 only)


@dataclass(frozen=True)
class TimestepBatch:
    """
    All plug readings at a single timestamp for one house.
    This maps 1:1 to one Kafka message (key = house_id).
    """
    house_id: int
    timestamp: int
    readings: tuple[PlugReading, ...]


def make_plug_uid(house_id: int, household_id: int, plug_id: int) -> int:
    """Compute globally unique plug ID."""
    return house_id * 100_000 + household_id * 1_000 + plug_id


def load_debs(
    data_file: Path,
    house_ids: list[int],
    property_filter: int = 0,
) -> pd.DataFrame:
    """
    Load DEBS 2014 CSV, filter Property and house_ids.
    Returns DataFrame with columns: timestamp, value, plug_uid, house_id, household_id, plug_id
    """
    if not data_file.exists():
        raise FileNotFoundError(f"DEBS data file not found: {data_file}")

    log.info("loading_debs_csv", file=str(data_file), house_ids=house_ids, property=property_filter)

    df = pd.read_csv(
        data_file,
        names=_DEBS_COLUMNS,
        dtype=_DEBS_DTYPES,
        header=None,
        engine="c",
        low_memory=False,
    )

    original_count = len(df)

    df = df[
        (df["property"] == property_filter) &
        (df["house_id"].isin(house_ids))
    ].copy()

    if df.empty:
        raise ValueError(
            f"No data after filtering: property={property_filter}, house_ids={house_ids}. "
            f"Original file had {original_count} rows. "
            f"Check HOUSE_IDS in .env"
        )

    # Compute globally unique plug_uid
    df["plug_uid"] = df.apply(
        lambda row: make_plug_uid(
            int(row["house_id"]),
            int(row["household_id"]),
            int(row["plug_id"])
        ),
        axis=1
    )

    # Keep only needed columns + sort
    df = df[["timestamp", "value", "plug_uid", "house_id", "household_id", "plug_id"]]
    df = df.sort_values("timestamp").reset_index(drop=True)

    n_plugs = df["plug_uid"].nunique()
    n_timestamps = df["timestamp"].nunique()
    duration_hours = (df["timestamp"].max() - df["timestamp"].min()) / 3600

    log.info(
        "debs_loaded",
        records=len(df),
        houses=df["house_id"].nunique(),
        plugs=n_plugs,
        timestamps=n_timestamps,
        duration_hours=round(duration_hours, 1),
    )

    return df


def iter_timestep_batches(df: pd.DataFrame) -> Generator[TimestepBatch, None, None]:
    """
    Yield TimestepBatch per (house, timestamp) in ascending order.
    """
    if df.empty:
        return

    for (house_id, ts), group in df.groupby(["house_id", "timestamp"], sort=True):
        readings = []
        for _, row in group.iterrows():
            readings.append(
                PlugReading(
                    plug_uid=int(row["plug_uid"]),
                    timestamp=int(ts),
                    value=float(row["value"]),
                )
            )

        yield TimestepBatch(
            house_id=int(house_id),
            timestamp=int(ts),
            readings=tuple(readings),
        )