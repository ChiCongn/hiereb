#!/usr/bin/env python3
"""Split a DEBS CSV stream into one CSV file per house.

The script streams the source file row by row and therefore does not
materialize a multi-gigabyte all-house dataset in memory.

Example:
  python scripts/partition_debs_by_house.py \
    --input data/all-house/one-day.csv \
    --output-dir data/partitioned/one-day \
    --property 1

  python scripts/partition_debs_by_house.py \
    --input data/house-*.csv \
    --output-dir data/partitioned/one-day \
    --property 1 --max-duration-seconds 86400 --overwrite
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import IO, Iterable


COLUMNS = [
    "id",
    "timestamp",
    "value",
    "property",
    "plug_id",
    "household_id",
    "house_id",
]


def _parse_house_ids(raw: str) -> set[int] | None:
    if not raw.strip():
        return None
    return {int(item.strip()) for item in raw.split(",") if item.strip()}


def _iter_canonical_rows(input_path: Path) -> Iterable[tuple[int, list[str]]]:
    with input_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        try:
            first = next(reader)
        except StopIteration:
            return

        has_header = not first[0].strip().lstrip("-").isdigit()
        if has_header:
            header = {name.strip().lower(): idx for idx, name in enumerate(first)}
            missing = [name for name in COLUMNS if name not in header]
            if missing:
                raise ValueError(f"Input CSV is missing columns: {missing}")

            for line_number, row in enumerate(reader, start=2):
                try:
                    canonical = [row[header[name]] for name in COLUMNS]
                except IndexError:
                    canonical = row
                yield line_number, canonical
            return

        yield 1, first[: len(COLUMNS)]
        for line_number, row in enumerate(reader, start=2):
            yield line_number, row[: len(COLUMNS)]


def partition_file(
    input_path: Path | list[Path],
    output_dir: Path,
    *,
    property_filter: int | None,
    house_ids: set[int] | None,
    overwrite: bool,
    max_duration_seconds: int | None = None,
    skip_malformed: bool = False,
) -> dict[str, object]:
    input_paths = [input_path] if isinstance(input_path, Path) else input_path
    if not input_paths:
        raise ValueError("At least one input CSV is required")
    missing_inputs = [path for path in input_paths if not path.is_file()]
    if missing_inputs:
        raise FileNotFoundError(f"Input file does not exist: {missing_inputs[0]}")

    output_dir.mkdir(parents=True, exist_ok=True)
    existing = sorted(output_dir.glob("house-*.csv"))
    if existing and not overwrite:
        raise FileExistsError(
            f"{output_dir} already contains partition CSV files. "
            "Use --overwrite to replace them."
        )
    if overwrite:
        for path in existing:
            path.unlink()

    handles: dict[int, IO[str]] = {}
    writers: dict[int, csv.writer] = {}
    row_counts: dict[int, int] = {}
    first_timestamp_by_house: dict[int, int] = {}
    skipped_rows = 0
    malformed_rows = 0

    try:
        for source in input_paths:
            for line_number, row in _iter_canonical_rows(source):
                if len(row) < len(COLUMNS):
                    if skip_malformed:
                        malformed_rows += 1
                        continue
                    raise ValueError(
                        f"Invalid DEBS row at {source}:{line_number}: fewer than seven columns"
                    )
                try:
                    timestamp = int(row[1])
                    property_value = int(row[3])
                    house_id = int(row[6])
                except ValueError as exc:
                    if skip_malformed:
                        malformed_rows += 1
                        continue
                    raise ValueError(
                        f"Invalid DEBS row at {source}:{line_number}: {row}"
                    ) from exc

                if property_filter is not None and property_value != property_filter:
                    skipped_rows += 1
                    continue
                if house_ids is not None and house_id not in house_ids:
                    skipped_rows += 1
                    continue

                first_timestamp = first_timestamp_by_house.setdefault(house_id, timestamp)
                if (
                    max_duration_seconds is not None
                    and timestamp >= first_timestamp + max_duration_seconds
                ):
                    skipped_rows += 1
                    continue

                if house_id not in writers:
                    target = output_dir / f"house-{house_id}.csv"
                    handle = target.open("w", encoding="utf-8", newline="")
                    writer = csv.writer(handle)
                    writer.writerow(COLUMNS)
                    handles[house_id] = handle
                    writers[house_id] = writer
                    row_counts[house_id] = 0

                writers[house_id].writerow(row)
                row_counts[house_id] += 1
    finally:
        for handle in handles.values():
            handle.close()

    manifest: dict[str, object] = {
        "inputs": [str(path) for path in input_paths],
        "property_filter": property_filter,
        "max_duration_seconds": max_duration_seconds,
        "houses": sorted(row_counts),
        "house_count": len(row_counts),
        "rows_by_house": {str(key): row_counts[key] for key in sorted(row_counts)},
        "rows_written": sum(row_counts.values()),
        "rows_skipped": skipped_rows,
        "malformed_rows_skipped": malformed_rows,
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        nargs="+",
        help="One combined input CSV or multiple per-house CSV files.",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="Output folder, for example data/partitioned/one-day.",
    )
    parser.add_argument(
        "--property",
        type=int,
        default=1,
        dest="property_filter",
        help="Only write this DEBS property value; DEBS property 1 is load in Watts.",
    )
    parser.add_argument(
        "--house-ids",
        default="",
        help="Optional comma-separated subset of house IDs to write.",
    )
    parser.add_argument(
        "--max-duration-seconds",
        type=int,
        default=None,
        help="Keep at most this duration from the first timestamp of each house.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing house-*.csv files in the output directory.",
    )
    parser.add_argument(
        "--skip-malformed",
        action="store_true",
        help="Skip malformed source rows and record their count in manifest.json.",
    )
    args = parser.parse_args()

    manifest = partition_file(
        args.input,
        args.output_dir,
        property_filter=args.property_filter,
        house_ids=_parse_house_ids(args.house_ids),
        overwrite=args.overwrite,
        max_duration_seconds=args.max_duration_seconds,
        skip_malformed=args.skip_malformed,
    )
    print(
        f"Created {manifest['house_count']} house partitions with "
        f"{manifest['rows_written']} rows in {args.output_dir}; "
        f"skipped malformed rows: {manifest['malformed_rows_skipped']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
