"""
Unit tests for src/ml_hiereb/main.py helpers.

Run: pytest tests/test_ml_main.py -v
"""
from __future__ import annotations

import math
import json

import pandas as pd
import pytest

from src.ml_hiereb.allocator import HierEBAllocator
from src.ml_hiereb.main import (
    build_threshold_payload,
    collect_warmup_residuals_by_house,
    compute_sigma_floor_by_house,
    flatten_warmup_residuals_by_plug,
    merge_house_structure,
    resolve_active_plug_uids_by_house,
    resolve_error_budget_from_load_stats,
    resolve_error_budget_watts,
)


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


def test_resolve_error_budget_from_partition_load_stats():
    assert resolve_error_budget_from_load_stats(
        load_sum_by_house={1: 400.0, 2: 600.0},
        sample_count_by_house={1: 2, 2: 2},
        epsilon_h=0.05,
    ) == pytest.approx(12.5)


def test_merge_house_structure_keeps_plugs_from_multiple_partitions():
    target = {1: {0: [100001]}}

    merge_house_structure(target, {1: {0: [100002]}, 2: {0: [200001]}})

    assert target == {1: {0: [100001, 100002]}, 2: {0: [200001]}}


def test_compute_sigma_floor_by_house_uses_warmup_percentile_and_fallback():
    residuals_by_house = {
        1: {
            101: [0.0, math.sqrt(2.0)],
            102: [0.0, 2.0 * math.sqrt(2.0)],
            103: [0.0, 0.0],
        },
        2: {
            201: [0.0, 0.0],
        },
    }

    floors = compute_sigma_floor_by_house(residuals_by_house)

    assert floors[1] == pytest.approx(1.05)
    assert floors[2] == pytest.approx(1.0)


def test_collect_warmup_residuals_by_house_skips_missing_predictions():
    class FakePredictor:
        def predict_single(self, plug_uid: int, timestamp: int) -> float | None:
            if plug_uid == 101:
                return 8.0
            return None

    df = pd.DataFrame(
        [
            {"house_id": 1, "plug_uid": 101, "timestamp": 1000, "value": 10.0},
            {"house_id": 1, "plug_uid": 102, "timestamp": 1000, "value": 20.0},
        ]
    )

    residuals = collect_warmup_residuals_by_house(df, FakePredictor())

    assert residuals == {1: {101: [2.0]}}
    assert flatten_warmup_residuals_by_plug(residuals) == {101: [2.0]}


def test_build_threshold_payload_includes_trace_metadata():
    allocator = HierEBAllocator(
        epsilon_h=10.0,
        sigma_floor_by_house={1: 1.5},
        min_variance_samples=1,
    )
    for plug_uid in [101, 102]:
        allocator.update_from_transmitted(plug_uid, 1.0)
    deltas = allocator.reallocate(
        {1: {0: [101, 102]}},
        allocation_time=2000,
        effective_after_time=2000,
        threshold_version=3,
    )

    payload = build_threshold_payload(allocator, 1, [101, 102], deltas)

    data = json.loads(payload)
    assert data["allocation_time"] == 2000
    assert data["effective_after_time"] == 2000
    assert data["threshold_version"] == 3
    assert data["sigma_floor_used"] == pytest.approx(1.5)
    assert data["n_budget_active_plugs"] == 2
    assert sum(data["deltas"].values()) == pytest.approx(10.0)


def test_resolve_active_plug_uids_by_house_uses_event_time_window():
    house_structure = {1: {0: [101, 102], 1: [201]}}
    last_seen = {
        101: 5000,
        102: 1399,
        201: 1400,
    }

    active = resolve_active_plug_uids_by_house(
        house_structure,
        last_seen,
        allocation_time=5000,
        active_window_seconds=3600,
    )

    assert active == {1: {101, 201}}
