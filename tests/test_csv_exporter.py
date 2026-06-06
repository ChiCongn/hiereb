"""Schema tests for mandatory HierEB CSV exports."""
from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

import pytest

from src.aggregator.csv_exporter import CSV_SCHEMAS, export_required_csvs_from_artifact


def _fixture_artifact() -> dict:
    experiment = {
        "run_id": "test-run-1",
        "mode": "hiereb",
        "started_at": "2026-06-06T00:00:00Z",
        "completed_at": "2026-06-06T00:01:00Z",
        "house_ids": [1],
        "property_filter": 1,
        "warmup_start": 100,
        "warmup_end": 999,
        "eval_start": 1000,
        "eval_end": 4600,
        "Delta_H": 30.0,
        "epsilon_ratio": 0.05,
        "tau": 300,
        "active_window_seconds": 3600,
        "predictor_bin_seconds": 300,
        "uniform_delta_initial": None,
        "sigma_floor_used": 1.5,
        "is_sweep": False,
        "sweep_id": None,
        "sweep_param_name": None,
        "sweep_param_value": None,
        "notes": "fixture",
    }
    events = [
        {
            "timestamp": 1000,
            "source_timestamp": 1000,
            "house_id": 1,
            "household_id": 0,
            "plug_id": 1,
            "plug_uid": 100001,
            "actual_load": 10.0,
            "predicted_load": 9.0,
            "reconstructed_load": 10.0,
            "transmitted": True,
            "decision": "transmit",
            "reason": "normal",
            "plug_status": "active",
            "is_forced_transmit": False,
            "threshold": 5.0,
            "residual": 1.0,
            "abs_residual": 1.0,
        },
        {
            "timestamp": 1000,
            "source_timestamp": 1000,
            "house_id": 1,
            "household_id": 0,
            "plug_id": 2,
            "plug_uid": 100002,
            "actual_load": 20.0,
            "predicted_load": 18.0,
            "reconstructed_load": 18.0,
            "transmitted": False,
            "decision": "suppress",
            "reason": "normal",
            "plug_status": "active",
            "is_forced_transmit": False,
            "threshold": 5.0,
            "residual": 2.0,
            "abs_residual": 2.0,
        },
        {
            "timestamp": 2800,
            "source_timestamp": 2800,
            "house_id": 1,
            "household_id": 0,
            "plug_id": 1,
            "plug_uid": 100001,
            "actual_load": 12.0,
            "predicted_load": None,
            "reconstructed_load": 12.0,
            "transmitted": True,
            "decision": "transmit",
            "reason": "missing_prediction",
            "plug_status": "active",
            "is_forced_transmit": True,
            "threshold": 5.0,
            "residual": None,
            "abs_residual": None,
        },
        {
            "timestamp": 2800,
            "source_timestamp": 2800,
            "house_id": 1,
            "household_id": 0,
            "plug_id": 2,
            "plug_uid": 100002,
            "actual_load": 22.0,
            "predicted_load": 21.0,
            "reconstructed_load": 21.0,
            "transmitted": False,
            "decision": "suppress",
            "reason": "normal",
            "plug_status": "active",
            "is_forced_transmit": False,
            "threshold": 5.0,
            "residual": 1.0,
            "abs_residual": 1.0,
        },
        {
            "timestamp": 4600,
            "source_timestamp": 4600,
            "house_id": 1,
            "household_id": 0,
            "plug_id": 1,
            "plug_uid": 100001,
            "actual_load": 11.0,
            "predicted_load": 10.0,
            "reconstructed_load": 11.0,
            "transmitted": True,
            "decision": "transmit",
            "reason": "inactive_reactivation",
            "plug_status": "reactivated",
            "is_forced_transmit": True,
            "threshold": 0.0,
            "residual": 1.0,
            "abs_residual": 1.0,
        },
        {
            "timestamp": 4600,
            "source_timestamp": 4600,
            "house_id": 1,
            "household_id": 0,
            "plug_id": 2,
            "plug_uid": 100002,
            "actual_load": 19.0,
            "predicted_load": 19.0,
            "reconstructed_load": 19.0,
            "transmitted": False,
            "decision": "suppress",
            "reason": "normal",
            "plug_status": "active",
            "is_forced_transmit": False,
            "threshold": 5.0,
            "residual": 0.0,
            "abs_residual": 0.0,
        },
    ]
    thresholds = [
        {
            "allocation_time": 1000,
            "effective_after_time": 999,
            "threshold_version": 0,
            "trace_granularity": "plug",
            "house_id": 1,
            "household_id": 0,
            "plug_id": 1,
            "plug_uid": 100001,
            "threshold": 5.0,
            "sigma_floor_used": 1.5,
            "sigma_p": 2.0,
            "weight": 2.0,
            "delta_used_for_censored_update": None,
            "n_budget_active_plugs": 2,
            "plug_status": "active",
            "reason": "normal",
        },
        {
            "allocation_time": 4600,
            "effective_after_time": 4600,
            "threshold_version": 1,
            "trace_granularity": "plug",
            "house_id": 1,
            "household_id": 0,
            "plug_id": 2,
            "plug_uid": 100002,
            "threshold": 0.0,
            "sigma_floor_used": 1.5,
            "sigma_p": 0.0,
            "weight": 1.5,
            "delta_used_for_censored_update": 5.0,
            "n_budget_active_plugs": 1,
            "plug_status": "inactive",
            "reason": "normal",
        },
    ]
    return {
        "experiment": experiment,
        "event_decisions": events,
        "threshold_trace": thresholds,
    }


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_required_csv_export_writes_all_schemas(tmp_path: Path):
    export_required_csvs_from_artifact(_fixture_artifact(), tmp_path)

    for filename, columns in CSV_SCHEMAS.items():
        path = tmp_path / filename
        assert path.exists(), filename
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            assert next(reader) == columns
        assert "mode" in columns


def test_required_csv_export_keeps_null_fields_empty(tmp_path: Path):
    export_required_csvs_from_artifact(_fixture_artifact(), tmp_path)

    event_rows = _read_csv(tmp_path / "event_decisions.csv")
    missing_prediction = next(row for row in event_rows if row["reason"] == "missing_prediction")

    assert missing_prediction["predicted_load"] == ""
    assert missing_prediction["residual"] == ""
    assert missing_prediction["abs_residual"] == ""

    threshold_rows = _read_csv(tmp_path / "threshold_trace.csv")
    assert threshold_rows[0]["delta_used_for_censored_update"] == ""


def test_required_csv_export_contains_special_columns_and_metrics(tmp_path: Path):
    export_required_csvs_from_artifact(_fixture_artifact(), tmp_path)

    house_timeseries = _read_csv(tmp_path / "house_timeseries.csv")
    last_house_row = house_timeseries[-1]
    assert last_house_row["rolling_tr_1h"] != ""
    assert last_house_row["rolling_rmse_1h"] != ""
    assert last_house_row["rolling_insufficient_data"] == "false"

    plug_metrics = _read_csv(tmp_path / "plug_metrics.csv")
    plug_1 = next(row for row in plug_metrics if row["plug_uid"] == "100001")
    assert plug_1["rmse_reconstruction"] != ""
    assert plug_1["mae_reconstruction"] != ""
    assert plug_1["rmse_prediction"] != ""
    assert plug_1["mae_prediction"] != ""
    assert plug_1["inactive_reactivation_count"] == "1"
    assert plug_1["missing_prediction_count"] == "1"

    house_summary = _read_csv(tmp_path / "house_summary.csv")[0]
    assert house_summary["p95_abs_house_error"] != ""
    assert house_summary["max_abs_house_error"] != ""
    assert house_summary["Delta_H"] == "30.0"
    assert house_summary["epsilon_ratio"] == "0.05"

    experiment = _read_csv(tmp_path / "experiment_runs.csv")[0]
    assert experiment["uniform_delta_initial"] == ""
    assert experiment["sigma_floor_used"] == "1.5"
    assert experiment["sweep_id"] == ""


def test_required_csv_cli_exports_fixture(tmp_path: Path):
    artifact_path = tmp_path / "artifact.json"
    output_dir = tmp_path / "csv"
    artifact_path.write_text(json.dumps(_fixture_artifact()), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/export_required_csv.py",
            "--input-json",
            str(artifact_path),
            "--output-dir",
            str(output_dir),
        ],
        check=True,
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
    )

    assert "event_decisions.csv" in result.stdout
    assert sorted(path.name for path in output_dir.glob("*.csv")) == sorted(CSV_SCHEMAS)
