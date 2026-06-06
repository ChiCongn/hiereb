"""Tests for report SVG generation."""
from __future__ import annotations

import csv
import subprocess
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _write_required_export_fixture(input_dir: Path) -> None:
    run_rows = [
        {"run_id": "sweep01_full_tx_house1", "mode": "full_tx", "sweep_id": "sweep01", "epsilon_ratio": ""},
        {"run_id": "sweep01_uniform_house1_eps002", "mode": "uniform", "sweep_id": "sweep01", "epsilon_ratio": "0.02"},
        {"run_id": "sweep01_hiereb_house1_eps002", "mode": "hiereb", "sweep_id": "sweep01", "epsilon_ratio": "0.02"},
        {"run_id": "sweep01_uniform_house1_eps005", "mode": "uniform", "sweep_id": "sweep01", "epsilon_ratio": "0.05"},
        {"run_id": "sweep01_hiereb_house1_eps005", "mode": "hiereb", "sweep_id": "sweep01", "epsilon_ratio": "0.05"},
    ]
    _write_csv(input_dir / "experiment_runs.csv", run_rows)

    _write_csv(
        input_dir / "house_summary.csv",
        [
            {
                "run_id": row["run_id"],
                "mode": row["mode"],
                "house_id": 1,
                "avg_transmission_rate": tr,
                "rmse_house_error": rmse,
                "p95_abs_house_error": p95,
                "Delta_H": 12.0,
                "epsilon_ratio": row["epsilon_ratio"],
            }
            for row, tr, rmse, p95 in [
                (run_rows[0], 1.00, 0.0, 0.0),
                (run_rows[1], 0.42, 8.0, 13.0),
                (run_rows[2], 0.39, 7.0, 10.0),
                (run_rows[3], 0.61, 3.5, 5.5),
                (run_rows[4], 0.58, 3.0, 4.5),
            ]
        ],
    )

    timeseries_rows: list[dict[str, object]] = []
    for run_id, mode, base in [
        ("sweep01_uniform_house1_eps002", "uniform", 9999.0),
        ("sweep01_hiereb_house1_eps002", "hiereb", 9999.0),
        ("sweep01_uniform_house1_eps005", "uniform", 100.0),
        ("sweep01_hiereb_house1_eps005", "hiereb", 110.0),
    ]:
        for idx in range(3):
            actual = base + idx
            reconstructed = actual - (idx + 1)
            timeseries_rows.append(
                {
                    "run_id": run_id,
                    "mode": mode,
                    "timestamp": 1000 + idx,
                    "source_timestamp": 1000 + idx,
                    "house_id": 1,
                    "actual_load": actual,
                    "reconstructed_load": reconstructed,
                    "house_error": actual - reconstructed,
                    "transmission_rate": 0.5,
                }
            )
    _write_csv(input_dir / "house_timeseries.csv", timeseries_rows)

    threshold_rows: list[dict[str, object]] = []
    for run_id in ["sweep01_hiereb_house1_eps002", "sweep01_hiereb_house1_eps005"]:
        for version in range(3):
            for plug_id in [1, 2]:
                threshold_rows.append(
                    {
                        "run_id": run_id,
                        "mode": "hiereb",
                        "allocation_time": 1000 + version * 300,
                        "effective_after_time": 1000 + version * 300,
                        "threshold_version": version,
                        "trace_granularity": "plug",
                        "house_id": 1,
                        "household_id": 0,
                        "plug_id": plug_id,
                        "plug_uid": 100000 + plug_id,
                        "threshold": 2.0 + version + plug_id,
                    }
                )
    _write_csv(input_dir / "threshold_trace.csv", threshold_rows)


def test_required_export_generates_pareto_and_threshold_svgs(tmp_path: Path):
    input_dir = tmp_path / "export"
    output_dir = tmp_path / "charts"
    _write_required_export_fixture(input_dir)

    result = subprocess.run(
        [
            sys.executable,
            "scripts/generate_report_charts.py",
            "--input-dir",
            str(input_dir),
            "--output-dir",
            str(output_dir),
        ],
        cwd=ROOT_DIR,
        text=True,
        capture_output=True,
        check=True,
    )

    assert "Generated SVG files" in result.stdout

    svg_files = {path.name for path in output_dir.glob("*.svg")}
    assert "sweep01_house1_pareto_tr_rmse.svg" in svg_files
    assert "sweep01_house1_pareto_tr_p95.svg" in svg_files
    assert "sweep01_house1_uniform_actual_vs_reconstructed.svg" in svg_files
    assert "sweep01_house1_hiereb_actual_vs_reconstructed.svg" in svg_files
    assert "sweep01_house1_uniform_house_error.svg" in svg_files
    assert "sweep01_house1_hiereb_house_error.svg" in svg_files
    assert "sweep01_house1_hiereb_threshold_trace.svg" in svg_files
    assert "sweep01_house1_hiereb_threshold_distribution.svg" in svg_files

    pareto_rmse = (output_dir / "sweep01_house1_pareto_tr_rmse.svg").read_text(encoding="utf-8")
    assert "eps=0.02" in pareto_rmse
    assert "eps=0.05" in pareto_rmse

    detail_chart = (output_dir / "sweep01_house1_hiereb_actual_vs_reconstructed.svg").read_text(encoding="utf-8")
    assert "eps=0.05" in detail_chart
    assert "eps=0.02" not in detail_chart

    house_error = (output_dir / "sweep01_house1_hiereb_house_error.svg").read_text(encoding="utf-8")
    assert "+Delta_H" in house_error
    assert "-Delta_H" in house_error
