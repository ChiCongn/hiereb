from __future__ import annotations

import json
from pathlib import Path

import yaml
from typer.testing import CliRunner

from hiereb.cli import app

runner = CliRunner()


def test_validate_and_generate(tmp_path: Path) -> None:
    validate = runner.invoke(app, ["validate-config", "--config", "configs/house_0.example.yaml"])
    assert validate.exit_code == 0
    assert "valid:" in validate.stdout
    output = tmp_path / "synthetic.csv"
    generated = runner.invoke(app, ["generate-synthetic", "--output", str(output)])
    assert generated.exit_code == 0
    assert output.is_file()


def test_run_compare_equivalence_and_inspect(tmp_path: Path) -> None:
    data = tmp_path / "data.csv"
    assert runner.invoke(app, ["generate-synthetic", "--output", str(data)]).exit_code == 0
    raw = yaml.safe_load(Path("configs/house_0.example.yaml").read_text())
    raw["data"]["path"] = str(data)
    raw["experiment"]["output_dir"] = str(tmp_path / "outputs")
    raw["experiment"]["modes"] = ["flat_variance", "legacy_two_stage"]
    raw["data"]["streaming"] = {
        "enabled": True,
        "chunk_rows": 3,
        "staging_dir": str(tmp_path / "staging"),
        "reuse_staging": True,
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(raw))

    run = runner.invoke(app, ["run", "--config", str(config_path), "--mode", "uniform"])
    assert run.exit_code == 0, run.output
    assert "[hiereb +" in run.stderr
    assert "mode uniform: streaming replay 100%" in run.stderr
    assert json.loads(run.stdout)["mode"] == "uniform"
    compare = runner.invoke(app, ["compare", "--config", str(config_path)])
    assert compare.exit_code == 0, compare.output
    verify = runner.invoke(app, ["verify-legacy-equivalence", "--config", str(config_path)])
    assert verify.exit_code == 0, verify.output
    assert '"equivalent": true' in verify.output
    inspect = runner.invoke(
        app,
        [
            "inspect-outliers",
            "--run-dir",
            str(tmp_path / "outputs" / "flat_variance"),
            "--limit",
            "2",
        ],
    )
    assert inspect.exit_code == 0, inspect.output


def test_invalid_config_path() -> None:
    result = runner.invoke(app, ["validate-config", "--config", "missing.yaml"])
    assert result.exit_code != 0


def test_empty_house_fails_fast(tmp_path: Path) -> None:
    data = tmp_path / "data.csv"
    assert runner.invoke(app, ["generate-synthetic", "--output", str(data)]).exit_code == 0
    raw = yaml.safe_load(Path("configs/house_0.example.yaml").read_text())
    raw["data"]["path"] = str(data)
    raw["data"]["house_id"] = 999
    raw["experiment"]["output_dir"] = str(tmp_path / "outputs")
    config_path = tmp_path / "empty-house.yaml"
    config_path.write_text(yaml.safe_dump(raw))
    result = runner.invoke(app, ["run", "--config", str(config_path), "--mode", "uniform"])
    assert result.exit_code != 0
    assert "has no rows" in str(result.exception)


def test_direction3_compare_and_inspect_commands(tmp_path: Path) -> None:
    data = tmp_path / "direction3.csv"
    generated = runner.invoke(app, ["generate-direction3-synthetic", "--output", str(data)])
    assert generated.exit_code == 0, generated.output
    raw = yaml.safe_load(Path("configs/direction3_synthetic.yaml").read_text())
    raw["data"]["path"] = str(data)
    raw["data"]["streaming"]["staging_dir"] = str(tmp_path / "staging")
    raw["experiment"]["output_dir"] = str(tmp_path / "outputs")
    raw["experiment"]["modes"] = ["full_tx"]
    raw["predictor"]["modes"] = ["slot_median_frozen", "slot_median_ewma"]
    config_path = tmp_path / "direction3.yaml"
    config_path.write_text(yaml.safe_dump(raw))
    compared = runner.invoke(
        app,
        [
            "compare-predictors",
            "--config",
            str(config_path),
            "--allocator",
            "uniform",
        ],
    )
    assert compared.exit_code == 0, compared.output
    single = runner.invoke(
        app,
        [
            "run-predictor",
            "--config",
            str(config_path),
            "--allocator",
            "full_tx",
            "--predictor-mode",
            "slot_median_ewma",
        ],
    )
    assert single.exit_code == 0, single.output
    single_summary = json.loads(single.stdout)
    assert single_summary["base_metrics"]["transmission_ratio"] == 1.0
    matrix = runner.invoke(
        app,
        [
            "compare-predictors",
            "--config",
            str(config_path),
        ],
    )
    assert matrix.exit_code == 0, matrix.output
    assert (tmp_path / "outputs" / "predictor_matrix.csv").is_file()
    run_dir = (
        tmp_path / "outputs" / "predictors" / "uniform" / "slot_median_ewma_drift_periodic_sync"
    )
    drift = runner.invoke(app, ["inspect-drift", "--run-dir", str(run_dir), "--limit", "2"])
    assert drift.exit_code == 0, drift.output
    resync = runner.invoke(
        app,
        ["inspect-resynchronization", "--run-dir", str(run_dir), "--limit", "2"],
    )
    assert resync.exit_code == 0, resync.output
