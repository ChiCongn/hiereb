#!/usr/bin/env python3
"""Export the six mandatory HierEB report CSV files.

Input is a compact JSON artifact:
{
  "experiment": {...},
  "event_decisions": [{...}],
  "threshold_trace": [{...}]
}
"""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.aggregator.csv_exporter import export_required_csvs_from_artifact, load_run_artifact


def main() -> int:
    parser = argparse.ArgumentParser(description="Export required HierEB report CSVs.")
    parser.add_argument("--input-json", required=True, help="Run artifact JSON file")
    parser.add_argument("--output-dir", required=True, help="Directory for generated CSV files")
    args = parser.parse_args()

    artifact = load_run_artifact(Path(args.input_json))
    outputs = export_required_csvs_from_artifact(artifact, Path(args.output_dir))
    for output in outputs:
        print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
