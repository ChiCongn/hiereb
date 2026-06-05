"""Unit tests for scripts/partition_debs_by_house.py."""
from __future__ import annotations

from pathlib import Path

import pytest

from scripts.partition_debs_by_house import partition_file
from src.simulator.loader import load_debs


def test_partition_file_writes_selected_property_per_house(tmp_path: Path):
    source = tmp_path / "combined.csv"
    source.write_text(
        "id,timestamp,value,property,plug_id,household_id,house_id\n"
        "1,1000,10.0,0,0,0,1\n"
        "2,1000,11.0,1,0,0,1\n"
        "3,1000,20.0,1,0,0,2\n"
    )

    output = tmp_path / "partitioned" / "one-day"
    manifest = partition_file(
        source,
        output,
        property_filter=1,
        house_ids=None,
        overwrite=False,
    )

    assert manifest["house_count"] == 2
    assert manifest["rows_written"] == 2
    assert load_debs(output / "house-1.csv", house_ids=[1]).shape[0] == 1
    assert load_debs(output / "house-2.csv", house_ids=[2]).shape[0] == 1
    assert (output / "manifest.json").exists()


def test_partition_file_does_not_overwrite_existing_partitions(tmp_path: Path):
    source = tmp_path / "combined.csv"
    source.write_text("1,1000,10.0,1,0,0,1\n")
    output = tmp_path / "partitioned"

    partition_file(source, output, property_filter=1, house_ids=None, overwrite=False)

    with pytest.raises(FileExistsError, match="--overwrite"):
        partition_file(source, output, property_filter=1, house_ids=None, overwrite=False)


def test_partition_file_accepts_multiple_inputs_and_limits_each_house_window(tmp_path: Path):
    first = tmp_path / "house-1.csv"
    second = tmp_path / "house-2.csv"
    first.write_text(
        "1,1000,10.0,1,0,0,1\n"
        "2,1002,12.0,1,0,0,1\n"
    )
    second.write_text(
        "3,2000,20.0,1,0,0,2\n"
        "4,2002,22.0,1,0,0,2\n"
    )

    output = tmp_path / "partitioned"
    manifest = partition_file(
        [first, second],
        output,
        property_filter=1,
        house_ids=None,
        overwrite=False,
        max_duration_seconds=2,
    )

    assert manifest["houses"] == [1, 2]
    assert manifest["rows_written"] == 2


def test_partition_file_can_skip_reported_malformed_rows(tmp_path: Path):
    source = tmp_path / "dirty.csv"
    source.write_text(
        "1,1000,10.0,1,0,0,1\n"
        "broken,row\n"
        "2,1001,11.0,1,0,0,1\n"
    )

    manifest = partition_file(
        source,
        tmp_path / "partitioned",
        property_filter=1,
        house_ids=None,
        overwrite=False,
        skip_malformed=True,
    )

    assert manifest["rows_written"] == 2
    assert manifest["malformed_rows_skipped"] == 1
