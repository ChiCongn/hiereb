from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from hiereb.config import AppConfig, load_config


def test_example_config_is_valid(config: AppConfig) -> None:
    assert config.experiment.seed == 42
    assert config.data.house_id == 0
    assert len(config.experiment.modes) == 6
    assert config.data.streaming.chunk_rows == 100_000


def test_missing_config_fails() -> None:
    with pytest.raises(FileNotFoundError):
        load_config(Path("does-not-exist.yaml"))


def test_rejects_overlapping_splits(config_path: Path) -> None:
    raw = yaml.safe_load(config_path.read_text())
    raw["splits"]["evaluation"]["start"] = "2013-09-01T05:00:00Z"
    with pytest.raises(ValidationError, match="overlap"):
        AppConfig.model_validate(raw)


def test_rejects_degenerate_lambda_sum_extension(config_path: Path) -> None:
    raw = yaml.safe_load(config_path.read_text())
    raw["true_hierarchy"]["household_score"]["lambda_sum"] = 1.0
    with pytest.raises(ValidationError, match="lambda_sum"):
        AppConfig.model_validate(raw)


def test_rejects_invalid_lambda_normalization(config_path: Path) -> None:
    raw = yaml.safe_load(config_path.read_text())
    raw["true_hierarchy"]["household_score"]["lambda_mean"] = 0.9
    with pytest.raises(ValidationError, match="sum to 1"):
        AppConfig.model_validate(raw)


def test_rejects_empty_or_duplicate_predictor_matrix_modes(config_path: Path) -> None:
    raw = yaml.safe_load(config_path.read_text())
    raw["predictor"]["modes"] = []
    with pytest.raises(ValidationError, match=r"predictor\.modes must be non-empty and unique"):
        AppConfig.model_validate(raw)
    raw["predictor"]["modes"] = ["slot_median_frozen", "slot_median_frozen"]
    with pytest.raises(ValidationError, match=r"predictor\.modes must be non-empty and unique"):
        AppConfig.model_validate(raw)


def test_data_path_override_and_environment(
    config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    environment_path = tmp_path / "from-environment.csv"
    monkeypatch.setenv("HIEREB_DATA_PATH", str(environment_path))
    assert load_config(config_path).data.path == environment_path
    cli_path = tmp_path / "from-cli.csv"
    assert load_config(config_path, cli_path).data.path == cli_path
