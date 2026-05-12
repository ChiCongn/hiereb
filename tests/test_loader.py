"""
Unit tests for src/simulator/loader.py
Run: pytest tests/test_loader.py -v
"""
from __future__ import annotations

from pathlib import Path
import textwrap

import pandas as pd
import pytest

from src.simulator.loader import (
    PlugReading,
    TimestepBatch,
    data_window_seconds,
    decode_plug_uid,
    iter_timestep_batches,
    load_debs,
    make_plug_uid,
    resolve_data_files,
    resolve_house_ids,
)


# ─── Fixtures ────────────────────────────────────────────────────────────────
_SAMPLE_CSV = textwrap.dedent("""\
1,1000,50.0,0,0,0,1
2,1000,30.0,0,1,0,1
3,1000,20.0,1,2,0,1
4,1001,55.0,0,0,0,1
5,1001,32.0,0,1,0,1
6,1002,60.0,0,0,0,1
7,1002,90.0,0,0,0,2
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
        "1,1000,50.0,0,0,0,1,2013-01-01 00:00:00+00:00\n"
        "2,1001,20.0,1,0,0,1,2013-01-01 00:00:01+00:00\n"
        "3,1002,70.0,0,0,0,2,2013-01-01 00:00:02+00:00\n"
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
    assert "property" not in df.columns
    assert "plug_uid" in df.columns


def test_load_debs_filters_property(sample_csv):
    df = load_debs(sample_csv, house_ids=[1], property_filter=0)
    assert len(df) == 5


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
    assert pd.api.types.is_integer_dtype(df["timestamp"])
    assert pd.api.types.is_float_dtype(df["value"])
    assert pd.api.types.is_integer_dtype(df["plug_uid"])


def test_load_debs_accepts_header_and_extra_datetime_column(sample_csv_with_header):
    df = load_debs(sample_csv_with_header, house_ids=[1], property_filter=0)
    assert len(df) == 1
    assert df.iloc[0]["timestamp"] == 1000
    assert "datetime" not in df.columns


def test_load_debs_limits_duration(sample_csv):
    df = load_debs(sample_csv, house_ids=[1], max_duration_seconds=2)
    assert df["timestamp"].max() < df["timestamp"].min() + 2
    assert set(df["timestamp"].unique()) == {1000, 1001}


def test_data_window_seconds_aliases():
    assert data_window_seconds("1-day") == 24 * 3600
    assert data_window_seconds("one_day") == 24 * 3600
    assert data_window_seconds("7d") == 7 * 24 * 3600
    assert data_window_seconds("7 days") == 7 * 24 * 3600
    assert data_window_seconds("all") is None


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
