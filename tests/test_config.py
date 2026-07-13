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
