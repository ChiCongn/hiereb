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
from typing import Generator, Iterable

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
_READ_CHUNK_SIZE = 500_000

_DATASET_PRESET_ALIASES = {
    "custom": "custom",
    "one_house": "one_house",
    "one_houses": "one_house",
    "single_house": "one_house",
    "1_house": "one_house",
    "five_house": "five_houses",
    "five_houses": "five_houses",
    "5_house": "five_houses",
    "5_houses": "five_houses",
    "all_house": "all_house",
    "all_houses": "all_house",
}

_WINDOW_SECONDS = {
    "one_day": 24 * 3600,
    "1_day": 24 * 3600,
    "1d": 24 * 3600,
    "day": 24 * 3600,
    "a_day": 24 * 3600,
    "five_days": 5 * 24 * 3600,
    "5d": 5 * 24 * 3600,
    "a_week": 7 * 24 * 3600,
    "one_week": 7 * 24 * 3600,
    "week": 7 * 24 * 3600,
    "7d": 7 * 24 * 3600,
    "7_days": 7 * 24 * 3600,
    "all": None,
}

_WINDOW_FILE = {
    "one_day": "one-day.csv",
    "1_day": "one-day.csv",
    "1d": "one-day.csv",
    "day": "one-day.csv",
    "a_day": "one-day.csv",
    "five_days": "five-days.csv",
    "5d": "five-days.csv",
    "a_week": "a-week.csv",
    "one_week": "a-week.csv",
    "week": "a-week.csv",
    "7d": "a-week.csv",
    "7_days": "a-week.csv",
}


@dataclass(frozen=True)
class PlugReading:
    """Single plug measurement at one timestamp."""
    plug_uid: int      # globally unique: house*100000 + household*1000 + plug
    household_id: int
    plug_id: int
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


def decode_plug_uid(plug_uid: int) -> tuple[int, int, int]:
    """Decode plug_uid back into (house_id, household_id, plug_id)."""
    house_id = plug_uid // 100_000
    remainder = plug_uid % 100_000
    household_id = remainder // 1_000
    plug_id = remainder % 1_000
    return house_id, household_id, plug_id


def normalize_dataset_preset(dataset_preset: str) -> str:
    """Normalize DATASET_PRESET aliases."""
    preset = dataset_preset.strip().lower().replace("-", "_").replace(" ", "_")
    if preset not in _DATASET_PRESET_ALIASES:
        valid = ", ".join(sorted(_DATASET_PRESET_ALIASES))
        raise ValueError(f"Unknown DATASET_PRESET={dataset_preset!r}. Valid values: {valid}")
    return _DATASET_PRESET_ALIASES[preset]


def normalize_data_window(data_window: str) -> str:
    """Normalize DATA_WINDOW aliases."""
    window = data_window.strip().lower().replace("-", "_").replace(" ", "_")
    if window not in _WINDOW_SECONDS:
        valid = ", ".join(sorted(_WINDOW_SECONDS))
        raise ValueError(f"Unknown DATA_WINDOW={data_window!r}. Valid values: {valid}")
    return window


def data_window_seconds(data_window: str) -> int | None:
    """Return max duration in seconds for a DATA_WINDOW value."""
    return _WINDOW_SECONDS[normalize_data_window(data_window)]


def resolve_house_ids(
    dataset_preset: str,
    *,
    house_ids: list[int],
    one_house_id: int,
    five_house_ids: list[int],
) -> list[int] | None:
    """
    Resolve configured house selection.

    Returns None for all houses/no house filter.
    """
    preset = normalize_dataset_preset(dataset_preset)
    if preset == "custom":
        return house_ids or None
    if preset == "one_house":
        return [one_house_id]
    if preset == "five_houses":
        return five_house_ids
    if preset == "all_house":
        return None
    raise ValueError(
        "Unknown DATASET_PRESET="
        f"{dataset_preset!r}. Valid values: custom, one_house, five_houses, all_house"
    )


def resolve_data_files(
    data_path: str,
    *,
    dataset_preset: str,
    data_window: str,
    data_file: str,
    data_glob: str,
    one_house_id: int,
    five_house_ids: list[int],
) -> list[Path]:
    """
    Resolve the CSV file(s) for the configured experiment preset.

    Preset layout supported by the current repository data:
      - data/house-<id>/{one-day,five-days,a-week}.csv
      - data/five-houses/a-day.csv
      - data/all-house/{one-day,five-days,a-week}.csv
      - data/house-<id>.csv for full-history runs
    """
    base = Path(data_path)
    preset = normalize_dataset_preset(dataset_preset)
    window = normalize_data_window(data_window)

    def _existing(paths: Iterable[Path]) -> list[Path]:
        files = [p for p in paths if p.is_file()]
        if not files:
            raise FileNotFoundError(
                "No DEBS data files matched DATASET_PRESET="
                f"{dataset_preset!r}, DATA_WINDOW={data_window!r}, DATA_PATH={data_path!r}"
            )
        return files

    if preset == "custom":
        if data_glob.strip():
            return _existing(sorted(base.glob(data_glob.strip())))
        raw_files = [part.strip() for part in data_file.split(",") if part.strip()]
        return _existing(base / part for part in raw_files)

    if preset == "one_house":
        root_house_file = base / f"house-{one_house_id}.csv"
        if root_house_file.is_file():
            return [root_house_file]
        if window != "all":
            candidate = base / f"house-{one_house_id}" / _WINDOW_FILE[window]
            if candidate.is_file():
                return [candidate]
        return _existing([root_house_file])

    if preset == "five_houses":
        if window != "all":
            candidate = base / "all-house" / _WINDOW_FILE[window]
            if candidate.is_file():
                return [candidate]
        if window in {"one_day", "1d", "day", "a_day"}:
            candidate = base / "five-houses" / "a-day.csv"
            if candidate.is_file():
                return [candidate]
        return _existing(base / f"house-{house_id}.csv" for house_id in five_house_ids)

    if preset == "all_house":
        if window != "all":
            candidate = base / "all-house" / _WINDOW_FILE[window]
            if candidate.is_file():
                return [candidate]
        return _existing(sorted(base.glob("house-*.csv")))

    raise ValueError(
        "Unknown DATASET_PRESET="
        f"{dataset_preset!r}. Valid values: custom, one_house, five_houses, all_house"
    )


def _csv_has_header(data_file: Path) -> bool:
    with data_file.open("r", encoding="utf-8", errors="ignore") as handle:
        first_field = handle.readline().split(",", 1)[0].strip()
    return not first_field.lstrip("-").isdigit()


def _iter_raw_debs_chunks(data_file: Path) -> Iterable[pd.DataFrame]:
    has_header = _csv_has_header(data_file)
    common_kwargs = {
        "dtype": _DEBS_DTYPES,
        "engine": "c",
        "chunksize": _READ_CHUNK_SIZE,
    }

    if has_header:
        yield from pd.read_csv(
            data_file,
            header=0,
            usecols=_DEBS_COLUMNS,
            **common_kwargs,
        )
    else:
        yield from pd.read_csv(
            data_file,
            names=_DEBS_COLUMNS,
            header=None,
            usecols=list(range(len(_DEBS_COLUMNS))),
            **common_kwargs,
        )


def load_debs(
    data_file: Path,
    house_ids: list[int] | None,
    property_filter: int = 0,
    max_duration_seconds: int | None = None,
) -> pd.DataFrame:
    """
    Load DEBS 2014 CSV, filter Property and house_ids.
    Returns DataFrame with columns: timestamp, value, plug_uid, house_id, household_id, plug_id
    """
    if not data_file.exists():
        raise FileNotFoundError(f"DEBS data file not found: {data_file}")

    log.info(
        "loading_debs_csv",
        file=str(data_file),
        house_ids=house_ids if house_ids is not None else "all",
        property=property_filter,
        max_duration_seconds=max_duration_seconds,
    )

    frames: list[pd.DataFrame] = []
    original_count = 0
    ts_start: int | None = None
    ts_stop: int | None = None

    for chunk in _iter_raw_debs_chunks(data_file):
        original_count += len(chunk)

        mask = chunk["property"] == property_filter
        if house_ids is not None:
            mask &= chunk["house_id"].isin(house_ids)

        filtered = chunk[mask].copy()
        if filtered.empty:
            continue

        if max_duration_seconds is not None:
            if ts_start is None:
                ts_start = int(filtered["timestamp"].min())
                ts_stop = ts_start + max_duration_seconds
            filtered = filtered[filtered["timestamp"] < ts_stop].copy()
            if filtered.empty:
                continue

        # Compute globally unique plug_uid vectorized; row-wise apply is too slow
        # on the multi-GB DEBS CSV files.
        filtered["plug_uid"] = (
            filtered["house_id"].astype("int64") * 100_000
            + filtered["household_id"].astype("int64") * 1_000
            + filtered["plug_id"].astype("int64")
        )

        frames.append(
            filtered[["timestamp", "value", "plug_uid", "house_id", "household_id", "plug_id"]]
        )

    if frames:
        df = pd.concat(frames, ignore_index=True)
    else:
        df = pd.DataFrame(columns=["timestamp", "value", "plug_uid", "house_id", "household_id", "plug_id"])

    if df.empty:
        raise ValueError(
            f"No data after filtering: property={property_filter}, house_ids={house_ids}. "
            f"Original file had {original_count} rows. "
            f"Check HOUSE_IDS in .env"
        )

    # Keep only needed columns + sort
    df = df.sort_values(["timestamp", "house_id", "household_id", "plug_id"]).reset_index(drop=True)

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


def load_debs_many(
    data_files: list[Path],
    house_ids: list[int] | None,
    property_filter: int = 0,
    max_duration_seconds: int | None = None,
) -> pd.DataFrame:
    """Load one or more DEBS CSV files and return one sorted DataFrame."""
    if not data_files:
        raise ValueError("No data files configured")

    frames = [
        load_debs(
            data_file,
            house_ids=house_ids,
            property_filter=property_filter,
            max_duration_seconds=max_duration_seconds,
        )
        for data_file in data_files
    ]

    if len(frames) == 1:
        return frames[0]

    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values(["timestamp", "house_id", "household_id", "plug_id"]).reset_index(drop=True)

    log.info(
        "debs_loaded_many",
        files=len(data_files),
        records=len(df),
        houses=df["house_id"].nunique(),
        plugs=df["plug_uid"].nunique(),
        timestamps=df["timestamp"].nunique(),
    )
    return df


def iter_timestep_batches(df: pd.DataFrame) -> Generator[TimestepBatch, None, None]:
    """
    Yield TimestepBatch per (house, timestamp) in ascending order.
    """
    if df.empty:
        return

    for (ts, house_id), group in df.groupby(["timestamp", "house_id"], sort=True):
        readings = []
        for row in group.itertuples(index=False):
            readings.append(
                PlugReading(
                    plug_uid=int(row.plug_uid),
                    household_id=int(row.household_id),
                    plug_id=int(row.plug_id),
                    timestamp=int(ts),
                    value=float(row.value),
                )
            )

        yield TimestepBatch(
            house_id=int(house_id),
            timestamp=int(ts),
            readings=tuple(readings),
        )
