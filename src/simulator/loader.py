"""
DEBS 2014 CSV loader.
Responsibilities:
- Load and filter raw CSV (DEBS property=1 load, specified houses/time windows)
- Build plug_uid as a globally unique integer per plug
- Yield TimestepBatch objects in ascending timestamp order

NOT responsible for: Kafka publishing, Suppression logic, Prediction lookup
"""
from __future__ import annotations

from dataclasses import dataclass
import heapq
import math
from pathlib import Path
from typing import Generator, Iterable, Iterator

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
_STREAM_READ_CHUNK_SIZE = 10_000

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
    source_id: int
    property: int
    plug_uid: int      # globally unique: house*100000 + household*1000 + plug
    household_id: int
    plug_id: int
    timestamp: int
    value: float       # Watts (DEBS property=1 load)


@dataclass(frozen=True)
class DebsLoadStats:
    """Audit counters collected while filtering DEBS rows."""
    original_rows: int = 0
    property_filtered_rows: int = 0
    house_filtered_rows: int = 0
    time_filtered_rows: int = 0
    negative_value_rows: int = 0
    non_finite_value_rows: int = 0
    duplicate_rows: int = 0
    rows_after_filter: int = 0
    rows_loaded: int = 0


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


def partition_window_dir_name(data_window: str) -> str:
    """Return the directory name used under data/partitioned for a window."""
    window = normalize_data_window(data_window)
    if window == "all":
        return "all"
    return Path(_WINDOW_FILE[window]).stem


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

    Preferred partition layout for bounded-memory multi-house runs:
      - data/partitioned/<window>/house-<id>.csv

    Legacy input layouts also supported where they do not select one combined
    all-house file for multi-house runtime:
      - data/house-<id>/{one-day,five-days,a-week}.csv
      - data/house-<id>.csv for full-history runs
    """
    base = Path(data_path)
    preset = normalize_dataset_preset(dataset_preset)
    window = normalize_data_window(data_window)
    partition_dir = base / "partitioned" / partition_window_dir_name(window)

    def _existing(paths: Iterable[Path]) -> list[Path]:
        files = [p for p in paths if p.is_file()]
        if not files:
            raise FileNotFoundError(
                "No DEBS data files matched DATASET_PRESET="
                f"{dataset_preset!r}, DATA_WINDOW={data_window!r}, DATA_PATH={data_path!r}"
            )
        return files

    def _all_required(paths: Iterable[Path]) -> list[Path]:
        candidates = list(paths)
        missing = [path for path in candidates if not path.is_file()]
        if missing:
            raise FileNotFoundError(
                "Missing configured per-house data files: "
                + ", ".join(str(path) for path in missing)
                + ". Create partitions or change the selected house IDs."
            )
        return candidates

    def _partition_files_for_ids(ids: list[int]) -> list[Path] | None:
        candidates = [partition_dir / f"house-{house_id}.csv" for house_id in ids]
        if candidates and all(path.is_file() for path in candidates):
            return candidates
        return None

    if preset == "custom":
        if data_glob.strip():
            return _existing(sorted(base.glob(data_glob.strip())))
        raw_files = [part.strip() for part in data_file.split(",") if part.strip()]
        return _existing(base / part for part in raw_files)

    if preset == "one_house":
        if window != "all":
            partitioned = _partition_files_for_ids([one_house_id])
            if partitioned:
                return partitioned
            candidate = base / f"house-{one_house_id}" / _WINDOW_FILE[window]
            if candidate.is_file():
                return [candidate]
        root_house_file = base / f"house-{one_house_id}.csv"
        if root_house_file.is_file():
            return [root_house_file]
        return _existing([root_house_file])

    if preset == "five_houses":
        if window != "all":
            partitioned = _partition_files_for_ids(five_house_ids)
            if partitioned:
                return partitioned
        return _all_required(base / f"house-{house_id}.csv" for house_id in five_house_ids)

    if preset == "all_house":
        if window != "all":
            partitioned = sorted(partition_dir.glob("house-*.csv"))
            if partitioned:
                return partitioned
            combined_file = base / "all-house" / _WINDOW_FILE[window]
            if combined_file.is_file():
                raise FileNotFoundError(
                    f"Partitioned data not found in {partition_dir}. "
                    "Split the combined file before running all_house: "
                    f"python scripts/partition_debs_by_house.py --input {combined_file} "
                    f"--output-dir {partition_dir} --property 1"
                )
        return _existing(sorted(base.glob("house-*.csv")))

    raise ValueError(
        "Unknown DATASET_PRESET="
        f"{dataset_preset!r}. Valid values: custom, one_house, five_houses, all_house"
    )


def _csv_has_header(data_file: Path) -> bool:
    with data_file.open("r", encoding="utf-8", errors="ignore") as handle:
        first_field = handle.readline().split(",", 1)[0].strip()
    return not first_field.lstrip("-").isdigit()


def _iter_raw_debs_chunks(
    data_file: Path,
    chunk_size: int = _READ_CHUNK_SIZE,
) -> Iterable[pd.DataFrame]:
    has_header = _csv_has_header(data_file)
    common_kwargs = {
        "dtype": _DEBS_DTYPES,
        "engine": "c",
        "chunksize": chunk_size,
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


_SORT_COLUMNS = ["timestamp", "house_id", "household_id", "plug_id", "property", "id"]
_DEDUP_COLUMNS = ["timestamp", "house_id", "household_id", "plug_id", "property"]
_OUTPUT_COLUMNS = [
    "id",
    "timestamp",
    "value",
    "property",
    "plug_uid",
    "house_id",
    "household_id",
    "plug_id",
]


def _finite_value_mask(values: pd.Series) -> pd.Series:
    return values.map(math.isfinite)


def _add_plug_uid(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["plug_uid"] = (
        df["house_id"].astype("int64") * 100_000
        + df["household_id"].astype("int64") * 1_000
        + df["plug_id"].astype("int64")
    )
    return df


def _deduplicate_and_sort(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if df.empty:
        return df[_OUTPUT_COLUMNS].copy(), 0

    ordered = df.sort_values(_SORT_COLUMNS, kind="mergesort")
    deduped = ordered.drop_duplicates(subset=_DEDUP_COLUMNS, keep="last")
    duplicate_rows = len(ordered) - len(deduped)
    deduped = deduped.sort_values(_SORT_COLUMNS, kind="mergesort").reset_index(drop=True)
    return deduped[_OUTPUT_COLUMNS], duplicate_rows


def load_debs_with_stats(
    data_file: Path,
    house_ids: list[int] | None,
    property_filter: int = 1,
    max_duration_seconds: int | None = None,
    timestamp_start: int | None = None,
    timestamp_end: int | None = None,
) -> tuple[pd.DataFrame, DebsLoadStats]:
    """
    Load DEBS 2014 CSV, filter by property/house/time/value, and return stats.

    Duplicate keys `(timestamp, house_id, household_id, plug_id, property)` keep
    the row with the largest source `id`. The returned DataFrame keeps `id` and
    `property` so later stages can audit the exact source event.
    """
    if not data_file.exists():
        raise FileNotFoundError(f"DEBS data file not found: {data_file}")
    if timestamp_start is not None and timestamp_end is not None and timestamp_start > timestamp_end:
        raise ValueError("timestamp_start must be <= timestamp_end")

    log.info(
        "loading_debs_csv",
        file=str(data_file),
        house_ids=house_ids if house_ids is not None else "all",
        property=property_filter,
        max_duration_seconds=max_duration_seconds,
        timestamp_start=timestamp_start,
        timestamp_end=timestamp_end,
    )

    frames: list[pd.DataFrame] = []
    original_count = 0
    property_filtered_rows = 0
    house_filtered_rows = 0
    time_filtered_rows = 0
    negative_value_rows = 0
    non_finite_value_rows = 0
    ts_start: int | None = None
    ts_stop: int | None = None

    for chunk in _iter_raw_debs_chunks(data_file):
        original_count += len(chunk)

        property_mask = chunk["property"] == property_filter
        property_filtered_rows += int((~property_mask).sum())

        house_mask = pd.Series(True, index=chunk.index)
        if house_ids is not None:
            house_mask = chunk["house_id"].isin(house_ids)
            house_filtered_rows += int((property_mask & ~house_mask).sum())

        base_mask = property_mask & house_mask

        if timestamp_start is not None:
            before = base_mask
            base_mask = base_mask & (chunk["timestamp"] >= timestamp_start)
            time_filtered_rows += int((before & ~base_mask).sum())
        if timestamp_end is not None:
            before = base_mask
            base_mask = base_mask & (chunk["timestamp"] <= timestamp_end)
            time_filtered_rows += int((before & ~base_mask).sum())

        if max_duration_seconds is not None:
            duration_source = chunk[base_mask]
            if ts_start is None and not duration_source.empty:
                ts_start = (
                    timestamp_start
                    if timestamp_start is not None
                    else int(duration_source["timestamp"].min())
                )
                ts_stop = ts_start + max_duration_seconds
            if ts_stop is not None:
                before = base_mask
                base_mask = base_mask & (chunk["timestamp"] < ts_stop)
                time_filtered_rows += int((before & ~base_mask).sum())

        finite_mask = _finite_value_mask(chunk["value"])
        negative_mask = finite_mask & (chunk["value"] < 0)
        negative_value_rows += int((base_mask & negative_mask).sum())
        non_finite_value_rows += int((base_mask & ~finite_mask).sum())

        valid_mask = base_mask & finite_mask & ~negative_mask
        filtered = chunk[valid_mask].copy()
        if filtered.empty:
            continue

        # Compute globally unique plug_uid vectorized; row-wise apply is too slow
        # on the multi-GB DEBS CSV files.
        frames.append(_add_plug_uid(filtered))

    if frames:
        filtered_df = pd.concat(frames, ignore_index=True)
        rows_after_filter = len(filtered_df)
        df, duplicate_rows = _deduplicate_and_sort(filtered_df)
    else:
        rows_after_filter = 0
        duplicate_rows = 0
        df = pd.DataFrame(columns=_OUTPUT_COLUMNS)

    if df.empty:
        raise ValueError(
            f"No data after filtering: property={property_filter}, house_ids={house_ids}. "
            f"Original file had {original_count} rows. "
            f"Check HOUSE_IDS in .env"
        )

    n_plugs = df["plug_uid"].nunique()
    n_timestamps = df["timestamp"].nunique()
    duration_hours = (df["timestamp"].max() - df["timestamp"].min()) / 3600
    stats = DebsLoadStats(
        original_rows=original_count,
        property_filtered_rows=property_filtered_rows,
        house_filtered_rows=house_filtered_rows,
        time_filtered_rows=time_filtered_rows,
        negative_value_rows=negative_value_rows,
        non_finite_value_rows=non_finite_value_rows,
        duplicate_rows=duplicate_rows,
        rows_after_filter=rows_after_filter,
        rows_loaded=len(df),
    )

    log.info(
        "debs_loaded",
        records=len(df),
        houses=df["house_id"].nunique(),
        plugs=n_plugs,
        timestamps=n_timestamps,
        duration_hours=round(duration_hours, 1),
        invalid_negative=negative_value_rows,
        invalid_non_finite=non_finite_value_rows,
        duplicates_dropped=duplicate_rows,
    )

    return df, stats


def load_debs(
    data_file: Path,
    house_ids: list[int] | None,
    property_filter: int = 1,
    max_duration_seconds: int | None = None,
    timestamp_start: int | None = None,
    timestamp_end: int | None = None,
) -> pd.DataFrame:
    """
    Load DEBS 2014 CSV and return filtered load events.

    Returns columns: id, timestamp, value, property, plug_uid, house_id,
    household_id, plug_id.
    """
    df, _ = load_debs_with_stats(
        data_file,
        house_ids=house_ids,
        property_filter=property_filter,
        max_duration_seconds=max_duration_seconds,
        timestamp_start=timestamp_start,
        timestamp_end=timestamp_end,
    )
    return df


def load_debs_many(
    data_files: list[Path],
    house_ids: list[int] | None,
    property_filter: int = 1,
    max_duration_seconds: int | None = None,
    timestamp_start: int | None = None,
    timestamp_end: int | None = None,
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
            timestamp_start=timestamp_start,
            timestamp_end=timestamp_end,
        )
        for data_file in data_files
    ]

    if len(frames) == 1:
        return frames[0]

    df, duplicate_rows = _deduplicate_and_sort(pd.concat(frames, ignore_index=True))

    log.info(
        "debs_loaded_many",
        files=len(data_files),
        records=len(df),
        houses=df["house_id"].nunique(),
        plugs=df["plug_uid"].nunique(),
        timestamps=df["timestamp"].nunique(),
        duplicates_dropped=duplicate_rows,
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
                    source_id=int(row.id),
                    property=int(row.property),
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


def _reading_sort_key(reading: PlugReading) -> tuple[int, int, int, int]:
    return (reading.household_id, reading.plug_id, reading.property, reading.source_id)


def _dedupe_readings(readings: Iterable[PlugReading]) -> tuple[PlugReading, ...]:
    latest: dict[tuple[int, int, int, int], PlugReading] = {}
    for reading in readings:
        key = (reading.timestamp, reading.household_id, reading.plug_id, reading.property)
        current = latest.get(key)
        if current is None or reading.source_id > current.source_id:
            latest[key] = reading
    return tuple(sorted(latest.values(), key=_reading_sort_key))


def _batch_from_pending(
    house_id: int,
    timestamp: int,
    readings: list[PlugReading],
) -> TimestepBatch:
    return TimestepBatch(
        house_id=house_id,
        timestamp=timestamp,
        readings=_dedupe_readings(readings),
    )


def _iter_timestep_batches_from_file(
    data_file: Path,
    house_ids: list[int] | None,
    property_filter: int,
    read_chunk_size: int,
    timestamp_start: int | None = None,
    timestamp_end: int | None = None,
) -> Iterator[TimestepBatch]:
    """
    Yield batches from one timestamp-sorted CSV without loading the whole file.

    Files produced by scripts/partition_debs_by_house.py preserve timestamp
    ordering. Legacy per-house DEBS files are expected to be timestamp sorted.
    """
    pending_key: tuple[int, int] | None = None
    pending_readings: list[PlugReading] = []
    previous_key: tuple[int, int] | None = None

    for chunk in _iter_raw_debs_chunks(data_file, chunk_size=read_chunk_size):
        mask = chunk["property"] == property_filter
        if house_ids is not None:
            mask &= chunk["house_id"].isin(house_ids)
        if timestamp_start is not None:
            mask &= chunk["timestamp"] >= timestamp_start
        if timestamp_end is not None:
            mask &= chunk["timestamp"] <= timestamp_end
        mask &= _finite_value_mask(chunk["value"])
        mask &= chunk["value"] >= 0
        filtered = chunk[mask].copy()
        if filtered.empty:
            continue

        filtered = _add_plug_uid(filtered)
        filtered = filtered.sort_values(_SORT_COLUMNS, kind="mergesort")

        for row in filtered.itertuples(index=False):
            key = (int(row.timestamp), int(row.house_id))
            if previous_key is not None and key < previous_key:
                raise ValueError(
                    f"Stream input is not timestamp sorted: {data_file}. "
                    "Create sorted per-house partitions before replay."
                )
            previous_key = key

            reading = PlugReading(
                source_id=int(row.id),
                property=int(row.property),
                plug_uid=int(row.plug_uid),
                household_id=int(row.household_id),
                plug_id=int(row.plug_id),
                timestamp=key[0],
                value=float(row.value),
            )

            if pending_key is None:
                pending_key = key
            elif key != pending_key:
                yield _batch_from_pending(
                    house_id=pending_key[1],
                    timestamp=pending_key[0],
                    readings=pending_readings,
                )
                pending_key = key
                pending_readings = []
            pending_readings.append(reading)

    if pending_key is not None:
        yield _batch_from_pending(
            house_id=pending_key[1],
            timestamp=pending_key[0],
            readings=pending_readings,
        )


def iter_timestep_batches_from_files(
    data_files: list[Path],
    house_ids: list[int] | None,
    property_filter: int = 1,
    max_duration_seconds: int | None = None,
    read_chunk_size: int = _STREAM_READ_CHUNK_SIZE,
    timestamp_start: int | None = None,
    timestamp_end: int | None = None,
) -> Iterator[TimestepBatch]:
    """
    Stream and merge multiple timestamp-sorted files with bounded memory.

    Each source iterator keeps at most one small CSV chunk in memory. Adjacent
    batches with the same (timestamp, house_id) are combined so the Kafka
    message contract remains one complete house snapshot per timestamp.
    """
    if not data_files:
        raise ValueError("No data files configured")
    if read_chunk_size <= 0:
        raise ValueError("read_chunk_size must be positive")

    streams = [
        _iter_timestep_batches_from_file(
            data_file,
            house_ids=house_ids,
            property_filter=property_filter,
            read_chunk_size=read_chunk_size,
            timestamp_start=timestamp_start,
            timestamp_end=timestamp_end,
        )
        for data_file in data_files
    ]
    merged = heapq.merge(*streams, key=lambda batch: (batch.timestamp, batch.house_id))

    try:
        current = next(merged)
    except StopIteration as exc:
        raise ValueError(
            f"No data after filtering: property={property_filter}, house_ids={house_ids}"
        ) from exc

    start_timestamp = current.timestamp
    stop_timestamp = (
        (timestamp_start if timestamp_start is not None else start_timestamp)
        + max_duration_seconds
        if max_duration_seconds is not None
        else None
    )

    for batch in merged:
        if stop_timestamp is not None and batch.timestamp >= stop_timestamp:
            break
        if (batch.timestamp, batch.house_id) == (current.timestamp, current.house_id):
            current = TimestepBatch(
                house_id=current.house_id,
                timestamp=current.timestamp,
                readings=_dedupe_readings(current.readings + batch.readings),
            )
            continue
        yield current
        current = batch

    if stop_timestamp is None or current.timestamp < stop_timestamp:
        yield current


def discover_stream_house_ids(
    data_files: list[Path],
    house_ids: list[int] | None,
    property_filter: int = 1,
    timestamp_start: int | None = None,
    timestamp_end: int | None = None,
) -> list[int]:
    """
    Resolve house states before replay starts without materializing all rows.

    Preset-based multi-house runs use one partition file per house. If
    HOUSE_IDS is already specified it is authoritative; otherwise one first
    matching chunk from each partition is sufficient to identify houses.
    """
    if house_ids is not None:
        return sorted(set(house_ids))

    discovered: set[int] = set()
    for data_file in data_files:
        for chunk in _iter_raw_debs_chunks(data_file, chunk_size=_STREAM_READ_CHUNK_SIZE):
            mask = chunk["property"] == property_filter
            if timestamp_start is not None:
                mask &= chunk["timestamp"] >= timestamp_start
            if timestamp_end is not None:
                mask &= chunk["timestamp"] <= timestamp_end
            mask &= _finite_value_mask(chunk["value"])
            mask &= chunk["value"] >= 0
            filtered = chunk[mask]
            if not filtered.empty:
                discovered.update(int(value) for value in filtered["house_id"].unique())
                break
    if not discovered:
        raise ValueError(f"No houses found in configured data files: {data_files}")
    return sorted(discovered)
