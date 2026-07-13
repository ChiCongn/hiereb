from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from hiereb.config import AppConfig
from hiereb.data.synthetic import generate_synthetic
from hiereb.domain.models import Event
from hiereb.experiment import execute_mode, prepare, verify_legacy_equivalence
from hiereb.predictor.slot_median import TimeSliceMedianPredictor
from hiereb.simulation.replay import replay


@pytest.fixture(scope="module")
def synthetic_config() -> AppConfig:
    from hiereb.config import load_config

    config = load_config(Path("configs/house_0.example.yaml"))
    generate_synthetic(config.data.path)
    return config


def test_full_tx_zero_error(synthetic_config: AppConfig) -> None:
    result, summary = execute_mode(synthetic_config, "full_tx", write=False)
    assert summary["transmission_ratio"] == 1.0
    assert summary["rmse"] == 0.0
    assert all(row["reconstruction_error"] == 0 for row in result.event_rows)


def test_strict_boundary_reactivation_and_bound(synthetic_config: AppConfig) -> None:
    prepared = prepare(synthetic_config)
    result, summary = execute_mode(synthetic_config, "uniform", prepared, write=False)
    boundary = result.event_rows[0]
    assert boundary["residual"] == boundary["threshold_used"] == 0.5
    assert boundary["suppressed"] is True
    assert result.forced_transmit["inactive_reactivation"] == 1
    assert summary["bound_violation_count"] == 0
    assert summary["max_bound_utilization"] <= 1.0 + synthetic_config.replay.float_tolerance


def test_batch_atomic_and_effective_after(synthetic_config: AppConfig) -> None:
    result, _ = execute_mode(synthetic_config, "flat_variance", write=False)
    allocation_time = datetime.fromisoformat("2013-09-01T06:05:00+00:00")
    initial = {
        (row["household_id"], row["plug_id"]): row["threshold"]
        for row in result.threshold_rows
        if row["allocation_timestamp"] < allocation_time
    }
    same_batch = [row for row in result.event_rows if row["timestamp"] == allocation_time]
    assert len(same_batch) == 4
    assert all(
        row["threshold_used"] == initial[(row["household_id"], row["plug_id"])]
        for row in same_batch
    )
    published = [
        row for row in result.threshold_rows if row["allocation_timestamp"] == allocation_time
    ]
    assert published and all(row["effective_after"] == allocation_time for row in published)


def test_hierarchy_differs_and_legacy_equivalence(synthetic_config: AppConfig) -> None:
    prepared = prepare(synthetic_config)
    flat, _ = execute_mode(synthetic_config, "flat_variance", prepared, write=False)
    hierarchy, _ = execute_mode(synthetic_config, "true_hierarchical", prepared, write=False)
    assert [row["threshold"] for row in flat.threshold_rows] != [
        row["threshold"] for row in hierarchy.threshold_rows
    ]
    report = verify_legacy_equivalence(synthetic_config)
    assert report["equivalent"] is True
    assert report["decisions_equal"] is True


def test_all_modes_have_zero_safety_violations(synthetic_config: AppConfig) -> None:
    prepared = prepare(synthetic_config)
    for mode in synthetic_config.experiment.modes:
        _, summary = execute_mode(synthetic_config, mode, prepared, write=False)
        assert summary["suppression_violation_count"] == 0
        assert summary["budget_violation_count"] == 0
        assert summary["bound_violation_count"] == 0


def test_all_required_artifacts(synthetic_config: AppConfig, tmp_path: Path) -> None:
    config = synthetic_config.model_copy(
        update={
            "experiment": synthetic_config.experiment.model_copy(
                update={"output_dir": tmp_path, "overwrite": True}
            )
        }
    )
    _, summary = execute_mode(config, "true_hierarchical_cap")
    run_dir = tmp_path / "true_hierarchical_cap"
    expected = {
        "resolved_config.yaml",
        "run_metadata.json",
        "summary.json",
        "summary.csv",
        "daily_metrics.csv",
        "household_metrics.csv",
        "plug_metrics.parquet",
        "threshold_trace.parquet",
        "top_outliers.parquet",
        "filter_counts.json",
    }
    assert expected <= {path.name for path in run_dir.iterdir()}
    assert {"threshold", "effective_after", "unused_budget"} <= set(
        pl.read_parquet(run_dir / "threshold_trace.parquet").columns
    )
    assert summary["bound_violation_count"] == 0
    assert any((run_dir / "plots").iterdir())


def test_missing_prediction_forces_transmit(synthetic_config: AppConfig) -> None:
    warmup_event = Event(
        timestamp=datetime.fromisoformat("2013-09-01T05:55:00+00:00"),
        value=10.0,
        property_value=1,
        plug_id=0,
        household_id=0,
        house_id=0,
        original_row_index=0,
        event_id="warmup",
    )
    unknown_event = Event(
        timestamp=datetime.fromisoformat("2013-09-01T06:00:00+00:00"),
        value=20.0,
        property_value=1,
        plug_id=99,
        household_id=9,
        house_id=0,
        original_row_index=1,
        event_id="unknown",
    )
    predictor = TimeSliceMedianPredictor(300)
    predictor.fit((warmup_event,))
    result = replay(
        "uniform",
        (unknown_event,),
        (warmup_event,),
        predictor,
        synthetic_config,
        1.0,
        1.0,
        {(0, 0): (0.0,)},
        {(0, 0): (0.0,)},
    )
    assert result.event_rows[0]["forced_reason"] == "missing_prediction"
    assert result.event_rows[0]["reconstructed"] == 20.0


def test_replay_is_deterministic(synthetic_config: AppConfig) -> None:
    prepared = prepare(synthetic_config)
    first, first_summary = execute_mode(
        synthetic_config, "true_hierarchical_cap", prepared, write=False
    )
    second, second_summary = execute_mode(
        synthetic_config, "true_hierarchical_cap", prepared, write=False
    )
    assert first_summary == second_summary
    assert first.event_rows == second.event_rows
    assert first.threshold_rows == second.threshold_rows


def test_uniform_proxy_runs_without_hidden_suppressed_residuals(
    synthetic_config: AppConfig,
) -> None:
    proxy_config = synthetic_config.model_copy(
        update={
            "residual": synthetic_config.residual.model_copy(update={"estimator": "uniform_proxy"})
        }
    )
    exact, _ = execute_mode(synthetic_config, "flat_variance", write=False)
    proxy, summary = execute_mode(proxy_config, "flat_variance", write=False)
    assert summary["suppression_violation_count"] == 0
    assert [row["threshold"] for row in exact.threshold_rows] != [
        row["threshold"] for row in proxy.threshold_rows
    ]
