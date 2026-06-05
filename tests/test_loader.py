"""
Unit tests for src/simulator/loader.py
Run: pytest tests/test_loader.py -v
"""
from __future__ import annotations

from pathlib import Path
import textwrap

import pandas as pd
import pytest

from config.settings import Settings
from src.simulator.loader import (
    DebsLoadStats,
    PlugReading,
    TimestepBatch,
    data_window_seconds,
    decode_plug_uid,
    discover_stream_house_ids,
    iter_timestep_batches,
    iter_timestep_batches_from_files,
    load_debs,
    load_debs_with_stats,
    make_plug_uid,
    partition_window_dir_name,
    resolve_data_files,
    resolve_house_ids,
)


# ─── Fixtures ────────────────────────────────────────────────────────────────
_SAMPLE_CSV = textwrap.dedent("""\
1,1000,50.0,1,0,0,1
2,1000,30.0,1,1,0,1
3,1000,20.0,0,2,0,1
4,1001,55.0,1,0,0,1
5,1001,32.0,1,1,0,1
6,1002,60.0,1,0,0,1
7,1002,90.0,1,0,0,2
""")


@pytest.fixture
def sample_csv(tmp_path: Path) -> Path:
    p = tmp_path / "test_data.csv"
    p.write_text(_SAMPLE_CSV)
    return p


@pytest.fixture
def sample_csv_with_header(tmp_path: Path) -> Path:
    p = tmp_path / "test_data_with_header.csv"
    p.write_text(
        "id,timestamp,value,property,plug_id,household_id,house_id,datetime\n"
        "1,1000,50.0,1,0,0,1,2013-01-01 00:00:00+00:00\n"
        "2,1001,20.0,0,0,0,1,2013-01-01 00:00:01+00:00\n"
        "3,1002,70.0,1,0,0,2,2013-01-01 00:00:02+00:00\n"
    )
    return p


# ─── make_plug_uid tests ─────────────────────────────────────────────────────
def test_make_plug_uid_unique():
    assert make_plug_uid(1, 0, 0) != make_plug_uid(1, 0, 1)
    assert make_plug_uid(1, 0, 0) != make_plug_uid(1, 1, 0)
    assert make_plug_uid(1, 0, 0) != make_plug_uid(2, 0, 0)


def test_decode_plug_uid_roundtrip():
    uid = make_plug_uid(12, 3, 45)
    assert decode_plug_uid(uid) == (12, 3, 45)


# ─── load_debs tests ─────────────────────────────────────────────────────────
def test_load_debs_basic(sample_csv):
    df = load_debs(sample_csv, house_ids=[1])
    assert len(df) == 5
    assert "id" in df.columns
    assert "property" in df.columns
    assert "plug_uid" in df.columns


def test_settings_default_property_is_load():
    assert Settings(_env_file=None).PROPERTY_FILTER == 1


def test_load_debs_filters_property_1_by_default(sample_csv):
    df = load_debs(sample_csv, house_ids=[1])
    assert len(df) == 5
    assert set(df["property"].unique()) == {1}


def test_load_debs_ignores_property_0_in_main_experiment(sample_csv):
    df = load_debs(sample_csv, house_ids=[1])
    assert 2 not in set(df["plug_id"])


def test_load_debs_filters_house(sample_csv):
    df = load_debs(sample_csv, house_ids=[1])
    assert df["house_id"].unique().tolist() == [1]


def test_load_debs_multi_house(sample_csv):
    df = load_debs(sample_csv, house_ids=[1, 2])
    assert set(df["house_id"].unique()) == {1, 2}


def test_load_debs_sorted_by_timestamp(sample_csv):
    df = load_debs(sample_csv, house_ids=[1])
    assert df["timestamp"].is_monotonic_increasing


def test_load_debs_plug_uid(sample_csv):
    df = load_debs(sample_csv, house_ids=[1])
    assert df["plug_uid"].nunique() == 2


def test_load_debs_file_not_found():
    with pytest.raises(FileNotFoundError):
        load_debs(Path("/non/existent.csv"), [1])


def test_load_debs_raises_on_empty_after_filter(sample_csv):
    with pytest.raises(ValueError, match="No data after filtering"):
        load_debs(sample_csv, house_ids=[99])


def test_load_debs_column_types(sample_csv):
    df = load_debs(sample_csv, house_ids=[1])
    assert pd.api.types.is_integer_dtype(df["id"])
    assert pd.api.types.is_integer_dtype(df["timestamp"])
    assert pd.api.types.is_float_dtype(df["value"])
    assert pd.api.types.is_integer_dtype(df["property"])
    assert pd.api.types.is_integer_dtype(df["plug_uid"])


def test_load_debs_accepts_header_and_extra_datetime_column(sample_csv_with_header):
    df = load_debs(sample_csv_with_header, house_ids=[1])
    assert len(df) == 1
    assert df.iloc[0]["timestamp"] == 1000
    assert "datetime" not in df.columns


def test_load_debs_limits_duration(sample_csv):
    df = load_debs(sample_csv, house_ids=[1], max_duration_seconds=2)
    assert df["timestamp"].max() < df["timestamp"].min() + 2
    assert set(df["timestamp"].unique()) == {1000, 1001}


def test_load_debs_filters_absolute_time_window(sample_csv):
    df = load_debs(sample_csv, house_ids=[1], timestamp_start=1001, timestamp_end=1002)
    assert set(df["timestamp"].unique()) == {1001, 1002}


def test_load_debs_warmup_and_eval_windows_do_not_overlap():
    settings = Settings(_env_file=None)
    assert settings.WARMUP_END < settings.EVAL_START
    assert settings.WARMUP_START <= settings.WARMUP_END
    assert settings.EVAL_START <= settings.EVAL_END


def test_load_debs_filters_invalid_values_and_reports_stats(tmp_path: Path):
    source = tmp_path / "invalid.csv"
    source.write_text(
        "1,1000,10.0,1,0,0,1\n"
        "2,1001,-1.0,1,0,0,1\n"
        "3,1002,nan,1,0,0,1\n"
        "4,1003,inf,1,0,0,1\n"
        "5,1004,20.0,1,0,0,1\n"
    )

    df, stats = load_debs_with_stats(source, house_ids=[1])

    assert isinstance(stats, DebsLoadStats)
    assert df["value"].tolist() == [10.0, 20.0]
    assert stats.negative_value_rows == 1
    assert stats.non_finite_value_rows == 2
    assert stats.rows_loaded == 2


def test_load_debs_deduplicates_key_by_largest_source_id(tmp_path: Path):
    source = tmp_path / "dupes.csv"
    source.write_text(
        "10,1000,10.0,1,0,0,1\n"
        "12,1000,12.0,1,0,0,1\n"
        "11,1000,11.0,1,0,0,1\n"
        "13,1000,99.0,1,1,0,1\n"
    )

    df, stats = load_debs_with_stats(source, house_ids=[1])

    first_plug = df[df["plug_id"] == 0].iloc[0]
    assert int(first_plug["id"]) == 12
    assert float(first_plug["value"]) == 12.0
    assert stats.duplicate_rows == 2


def test_load_debs_sort_order_uses_all_contract_columns(tmp_path: Path):
    source = tmp_path / "unsorted.csv"
    source.write_text(
        "4,1000,40.0,1,2,0,1\n"
        "3,1000,30.0,1,1,0,1\n"
        "2,999,20.0,1,9,1,1\n"
        "1,1000,10.0,1,0,0,1\n"
    )

    df = load_debs(source, house_ids=[1])

    assert df[["timestamp", "house_id", "household_id", "plug_id", "property", "id"]].to_records(index=False).tolist() == [
        (999, 1, 1, 9, 1, 2),
        (1000, 1, 0, 0, 1, 1),
        (1000, 1, 0, 1, 1, 3),
        (1000, 1, 0, 2, 1, 4),
    ]


def test_data_window_seconds_aliases():
    assert data_window_seconds("1-day") == 24 * 3600
    assert data_window_seconds("one_day") == 24 * 3600
    assert data_window_seconds("7d") == 7 * 24 * 3600
    assert data_window_seconds("7 days") == 7 * 24 * 3600
    assert data_window_seconds("all") is None


def test_partition_window_dir_name_aliases():
    assert partition_window_dir_name("1-day") == "one-day"
    assert partition_window_dir_name("five_days") == "five-days"
    assert partition_window_dir_name("7d") == "a-week"


def test_resolve_house_ids_presets():
    assert resolve_house_ids(
        "one-house",
        house_ids=[9],
        one_house_id=3,
        five_house_ids=[0, 1, 2, 3, 4],
    ) == [3]
    assert resolve_house_ids(
        "five-house",
        house_ids=[9],
        one_house_id=3,
        five_house_ids=[0, 1, 2, 3, 4],
    ) == [0, 1, 2, 3, 4]
    assert resolve_house_ids(
        "all-house",
        house_ids=[9],
        one_house_id=3,
        five_house_ids=[0, 1, 2, 3, 4],
    ) is None


def test_resolve_data_files_custom(sample_csv):
    files = resolve_data_files(
        str(sample_csv.parent),
        dataset_preset="custom",
        data_window="all",
        data_file=sample_csv.name,
        data_glob="",
        one_house_id=1,
        five_house_ids=[0, 1, 2, 3, 4],
    )
    assert files == [sample_csv]


def test_resolve_five_houses_prefers_per_house_partitions(tmp_path):
    partition_dir = tmp_path / "partitioned" / "one-day"
    partition_dir.mkdir(parents=True)
    expected = []
    for house_id in [1, 2, 3, 4, 5]:
        path = partition_dir / f"house-{house_id}.csv"
        path.write_text(f"1,1000,1.0,1,0,0,{house_id}\n")
        expected.append(path)
    combined = tmp_path / "all-house" / "one-day.csv"
    combined.parent.mkdir()
    combined.write_text("1,1000,1.0,1,0,0,1\n")

    files = resolve_data_files(
        str(tmp_path),
        dataset_preset="five_houses",
        data_window="one_day",
        data_file="",
        data_glob="",
        one_house_id=1,
        five_house_ids=[1, 2, 3, 4, 5],
    )

    assert files == expected


def test_resolve_five_houses_rejects_incomplete_file_selection(tmp_path):
    (tmp_path / "house-1.csv").write_text("1,1000,1.0,1,0,0,1\n")

    with pytest.raises(FileNotFoundError, match="house-2.csv"):
        resolve_data_files(
            str(tmp_path),
            dataset_preset="five_houses",
            data_window="all",
            data_file="",
            data_glob="",
            one_house_id=1,
            five_house_ids=[1, 2],
        )


def test_resolve_all_houses_requires_partitions_for_combined_data(tmp_path):
    combined = tmp_path / "all-house" / "one-day.csv"
    combined.parent.mkdir()
    combined.write_text("1,1000,1.0,1,0,0,1\n")

    with pytest.raises(FileNotFoundError, match="partition_debs_by_house"):
        resolve_data_files(
            str(tmp_path),
            dataset_preset="all_house",
            data_window="one_day",
            data_file="",
            data_glob="",
            one_house_id=1,
            five_house_ids=[1, 2, 3, 4, 5],
        )


# ─── iter_timestep_batches tests ─────────────────────────────────────────────
def test_iter_batches_count(sample_csv):
    df = load_debs(sample_csv, house_ids=[1])
    batches = list(iter_timestep_batches(df))
    assert len(batches) == 3   # ts 1000, 1001, 1002


def test_iter_batches_ordering(sample_csv):
    df = load_debs(sample_csv, house_ids=[1])
    batches = list(iter_timestep_batches(df))
    ts = [b.timestamp for b in batches]
    assert ts == sorted(ts)


def test_iter_batches_plug_count(sample_csv):
    df = load_debs(sample_csv, house_ids=[1])
    batches = list(iter_timestep_batches(df))
    ts1000 = next(b for b in batches if b.timestamp == 1000)
    ts1002 = next(b for b in batches if b.timestamp == 1002)
    assert len(ts1000.readings) == 2
    assert len(ts1002.readings) == 1


def test_iter_batches_multi_house(sample_csv):
    df = load_debs(sample_csv, house_ids=[1, 2])
    batches = list(iter_timestep_batches(df))
    ts1002 = [b for b in batches if b.timestamp == 1002]
    assert len(ts1002) == 2
    assert {b.house_id for b in ts1002} == {1, 2}


def test_iter_batches_multi_house_global_timestamp_order(sample_csv):
    df = load_debs(sample_csv, house_ids=[1, 2])
    batches = list(iter_timestep_batches(df))
    keys = [(b.timestamp, b.house_id) for b in batches]
    assert keys == sorted(keys)


def test_iter_batches_reading_fields(sample_csv):
    df = load_debs(sample_csv, house_ids=[1])
    batch = next(iter_timestep_batches(df))
    reading = batch.readings[0]
    assert isinstance(reading, PlugReading)
    assert isinstance(reading.plug_uid, int)
    assert isinstance(reading.household_id, int)
    assert isinstance(reading.plug_id, int)
    assert isinstance(reading.value, float)


def test_iter_batches_from_files_merges_partitions_in_timestamp_order(tmp_path):
    house_1 = tmp_path / "house-1.csv"
    house_2 = tmp_path / "house-2.csv"
    house_1.write_text(
        "1,1000,50.0,1,0,0,1\n"
        "2,1001,51.0,1,0,0,1\n"
    )
    house_2.write_text(
        "3,1000,70.0,1,0,0,2\n"
        "4,1002,71.0,1,0,0,2\n"
    )

    batches = list(
        iter_timestep_batches_from_files(
            [house_1, house_2],
            house_ids=None,
            read_chunk_size=1,
        )
    )

    assert [(batch.timestamp, batch.house_id) for batch in batches] == [
        (1000, 1),
        (1000, 2),
        (1001, 1),
        (1002, 2),
    ]


def test_iter_batches_from_files_coalesces_same_house_snapshot(tmp_path):
    left = tmp_path / "left.csv"
    right = tmp_path / "right.csv"
    left.write_text("1,1000,50.0,1,0,0,1\n")
    right.write_text("2,1000,60.0,1,1,0,1\n")

    batches = list(iter_timestep_batches_from_files([left, right], house_ids=[1]))

    assert len(batches) == 1
    assert len(batches[0].readings) == 2


def test_discover_stream_house_ids_reads_one_partition_per_house(tmp_path):
    files = []
    for house_id in [5, 8]:
        path = tmp_path / f"house-{house_id}.csv"
        path.write_text(f"1,1000,1.0,1,0,0,{house_id}\n")
        files.append(path)

    assert discover_stream_house_ids(files, house_ids=None) == [5, 8]


# real data test
def test_with_real_house_1_data():
    """Test với file thật của bạn (house-1.csv)."""
    real_path = Path("data/house-1.csv") 
    if real_path.exists():
        df = load_debs(real_path, house_ids=[1])
        assert len(df) > 1000
        assert "plug_uid" in df.columns
        print(f"✅ Real data test passed: {len(df):,} records, "
              f"{df['plug_uid'].nunique()} unique plugs, "
              f"house_ids: {sorted(df['house_id'].unique())}")
    else:
        pytest.skip(f"data/house-1.csv not found - skipping real data test")


if __name__ == "__main__":
    pytest.main(["-v", "--tb=short"])
