from __future__ import annotations

from pathlib import Path

import pytest

from hiereb.config import AppConfig, load_config


@pytest.fixture
def config_path() -> Path:
    return Path("configs/house_0.example.yaml")


@pytest.fixture
def config(config_path: Path) -> AppConfig:
    return load_config(config_path)
