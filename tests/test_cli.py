from __future__ import annotations

from pathlib import Path

import yaml
from typer.testing import CliRunner

from hiereb.cli import app

runner = CliRunner()


def test_validate_and_generate(tmp_path: Path) -> None:
    validate = runner.invoke(
        app, ["validate-config", "--config", "configs/house_0.example.yaml"]
    )
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
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(raw))

    run = runner.invoke(app, ["run", "--config", str(config_path), "--mode", "uniform"])
    assert run.exit_code == 0, run.output
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
    result = runner.invoke(
        app, ["run", "--config", str(config_path), "--mode", "uniform"]
    )
    assert result.exit_code != 0
    assert "has no rows" in str(result.exception)
