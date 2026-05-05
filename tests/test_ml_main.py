"""
Unit tests for src/ml_hiereb/main.py helpers.

Run: pytest tests/test_ml_main.py -v
"""
from __future__ import annotations

import pandas as pd
import pytest

from src.ml_hiereb.main import resolve_error_budget_watts


def test_resolve_error_budget_ratio_uses_mean_house_load():
    df = pd.DataFrame(
        [
            {"house_id": 1, "timestamp": 1000, "value": 100.0},
            {"house_id": 1, "timestamp": 1000, "value": 50.0},
            {"house_id": 1, "timestamp": 1001, "value": 250.0},
        ]
    )

    # Per-timestep house loads are 150W and 250W; mean is 200W.
    assert resolve_error_budget_watts(df, 0.05) == pytest.approx(10.0)


def test_resolve_error_budget_absolute_watts_passthrough():
    df = pd.DataFrame([{"house_id": 1, "timestamp": 1000, "value": 100.0}])

    assert resolve_error_budget_watts(df, 25.0) == pytest.approx(25.0)
