from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from hiereb.config import AppConfig, DriftConfig, PredictorTraceConfig, load_config
from hiereb.data.streaming import StreamingPreparedExperiment, prepare_streaming
from hiereb.data.synthetic_direction3 import generate_direction3_synthetic
from hiereb.domain.models import Event
from hiereb.experiment import (
    compare_predictors,
    execute_predictor_mode,
    run_predictor_matrix,
    validate_predictor_hyperparameters,
)
from hiereb.predictor.adaptive import DualPredictorRuntime, ewma_bias_transition
from hiereb.predictor.drift import (
    DriftRuntimeState,
    DriftScaleModel,
    commit_drift,
    evaluate_drift,
)
from hiereb.simulation.active_set import ActiveSet
from hiereb.simulation.adaptive_replay import _decision_reason


class _ConstantPredictor:
    def predict(self, plug: tuple[int, int], timestamp: datetime) -> float | None:
        _ = plug, timestamp
        return 10.0


class _MissingPredictor:
    def predict(self, plug: tuple[int, int], timestamp: datetime) -> float | None:
        _ = plug, timestamp
        return None


@pytest.fixture(scope="module")
def direction3_config(tmp_path_factory: pytest.TempPathFactory) -> AppConfig:
    root = tmp_path_factory.mktemp("direction3")
    data_path = root / "synthetic.csv"
    generate_direction3_synthetic(data_path)
    config = load_config(Path("configs/direction3_synthetic.yaml"), data_path)
    streaming = config.data.streaming.model_copy(
        update={"chunk_rows": 17, "staging_dir": root / "staging", "reuse_staging": True}
    )
    artifacts = config.artifacts.model_copy(
        update={"predictor_trace": PredictorTraceConfig(sample_every_n_events=1)}
    )
    return config.model_copy(
        update={
            "data": config.data.model_copy(update={"streaming": streaming}),
            "artifacts": artifacts,
            "experiment": config.experiment.model_copy(
                update={"output_dir": root / "outputs", "overwrite": True}
            ),
        }
    )


@pytest.fixture(scope="module")
def direction3_prepared(direction3_config: AppConfig) -> StreamingPreparedExperiment:
    return prepare_streaming(direction3_config)


def test_ewma_transition_and_dual_update_semantics() -> None:
    timestamp = datetime(2020, 1, 1, tzinfo=UTC)
    plug = (0, 0)
    assert ewma_bias_transition(2.0, 4.0, 0.25) == 3.0
    assert ewma_bias_transition(2.0, 4.0, 0.0) == 2.0
    assert ewma_bias_transition(2.0, 4.0, 1.0) == 6.0
    runtime = DualPredictorRuntime(_ConstantPredictor(), "slot_median_ewma", 0.5)
    assert runtime.predict(plug, timestamp) == (10.0, 10.0)
    assert runtime.commit(plug, 4.0, transmitted=False, timestamp=timestamp) is False
    assert runtime.predict(plug, timestamp) == (10.0, 10.0)
    assert runtime.commit(plug, 4.0, transmitted=True, timestamp=timestamp) is True
    assert runtime.predict(plug, timestamp) == (12.0, 12.0)
    assert runtime.state(plug).predictor_update_count == 1
    assert runtime.divergence_count(1e-9) == 0


def test_local_oracle_is_explicitly_non_deployable() -> None:
    timestamp = datetime(2020, 1, 1, tzinfo=UTC)
    runtime = DualPredictorRuntime(_ConstantPredictor(), "slot_median_ewma_local_oracle", 0.5)
    runtime.commit((0, 0), 4.0, transmitted=False, timestamp=timestamp)
    assert runtime.predict((0, 0), timestamp) == (12.0, 10.0)
    assert runtime.deployable is False


def test_missing_prediction_does_not_create_or_update_state() -> None:
    timestamp = datetime(2020, 1, 1, tzinfo=UTC)
    runtime = DualPredictorRuntime(_MissingPredictor(), "slot_median_ewma", 0.5)
    assert runtime.predict((7, 9), timestamp) == (None, None)
    assert runtime.states == {}


def test_zscore_cusum_cooldown_and_scale_fallback(direction3_config: AppConfig) -> None:
    zscore = direction3_config.predictor.adaptive.drift.model_copy(
        update={
            "detector": "zscore_consecutive",
            "zscore": direction3_config.predictor.adaptive.drift.zscore.model_copy(
                update={"threshold": 2.0, "consecutive_count": 2}
            ),
            "cooldown_events": 2,
        }
    )
    state = DriftRuntimeState()
    first = evaluate_drift(state, 3.0, 1.0, zscore, enabled=True)
    assert first.trigger is False
    commit_drift(state, first)
    second = evaluate_drift(state, 3.0, 1.0, zscore, enabled=True)
    assert second.trigger is True
    commit_drift(state, second)
    cooldown = evaluate_drift(state, 3.0, 1.0, zscore, enabled=True)
    assert cooldown.trigger is False

    cusum = DriftConfig(
        detector="cusum",
        min_scale=1.0,
        cusum={"kappa": 0.0, "threshold_h": 2.0},
    )
    assert evaluate_drift(DriftRuntimeState(), 3.0, 1.0, cusum, enabled=True).trigger
    assert evaluate_drift(DriftRuntimeState(), -3.0, 1.0, cusum, enabled=True).trigger
    assert not evaluate_drift(DriftRuntimeState(), 0.1, 1.0, cusum, enabled=True).trigger

    scales = DriftScaleModel({(0, 0): 2.0}, {1: 3.0}, 4.0, 1.0, 0.5)
    assert scales.scale((0, 0)) == 2.0
    assert scales.scale((1, 9)) == 3.0
    assert scales.scale((9, 9)) == 4.0


def test_direction3_config_validation(direction3_config: AppConfig) -> None:
    raw = direction3_config.model_dump(mode="python")
    raw["predictor"]["adaptive"]["alpha"] = 1.5
    with pytest.raises(ValidationError):
        AppConfig.model_validate(raw)
    raw = direction3_config.model_dump(mode="python")
    raw["predictor"]["adaptive"]["resynchronization"]["max_consecutive_suppressions"] = 0
    assert (
        AppConfig.model_validate(raw)
        .predictor.adaptive.resynchronization.max_consecutive_suppressions
        is None
    )
    raw["predictor"]["adaptive"]["resynchronization"]["max_silence_seconds"] = -1
    with pytest.raises(ValidationError, match="null, 0, or > 0"):
        AppConfig.model_validate(raw)


def test_alpha_zero_matches_frozen(
    direction3_config: AppConfig,
    direction3_prepared: StreamingPreparedExperiment,
) -> None:
    adaptive = direction3_config.predictor.adaptive.model_copy(update={"alpha": 0.0})
    config = direction3_config.model_copy(
        update={"predictor": direction3_config.predictor.model_copy(update={"adaptive": adaptive})}
    )
    prepared = prepare_streaming(config)
    frozen, frozen_summary = execute_predictor_mode(
        config, "uniform", "slot_median_frozen", prepared, write=False
    )
    ewma, ewma_summary = execute_predictor_mode(
        config, "uniform", "slot_median_ewma", prepared, write=False
    )
    assert frozen.base.decision_sha256 == ewma.base.decision_sha256
    assert frozen_summary == ewma_summary
    assert ewma.predictor_summary["predictor_state_divergence_count"] == 0
    _ = direction3_prepared


def test_adaptive_disabled_reproduces_frozen(direction3_config: AppConfig) -> None:
    adaptive = direction3_config.predictor.adaptive.model_copy(update={"enabled": False})
    config = direction3_config.model_copy(
        update={"predictor": direction3_config.predictor.model_copy(update={"adaptive": adaptive})}
    )
    prepared = prepare_streaming(config)
    frozen, frozen_summary = execute_predictor_mode(
        config, "uniform", "slot_median_frozen", prepared, write=False
    )
    requested_adaptive, adaptive_summary = execute_predictor_mode(
        config, "uniform", "slot_median_ewma_drift_periodic_sync", prepared, write=False
    )
    assert requested_adaptive.predictor_mode == "slot_median_frozen"
    assert requested_adaptive.base.decision_sha256 == frozen.base.decision_sha256
    assert adaptive_summary == frozen_summary


def test_warmup_initialization_is_deterministic(
    direction3_config: AppConfig,
    direction3_prepared: StreamingPreparedExperiment,
) -> None:
    repeated = prepare_streaming(direction3_config)
    assert repeated.warmup_initial_biases == direction3_prepared.warmup_initial_biases
    assert any(abs(value) > 0.0 for value in repeated.warmup_initial_biases.values())


def test_adaptive_modes_safety_sync_and_qualitative_behavior(
    direction3_config: AppConfig,
    direction3_prepared: StreamingPreparedExperiment,
) -> None:
    frozen, frozen_base = execute_predictor_mode(
        direction3_config,
        "uniform",
        "slot_median_frozen",
        direction3_prepared,
        write=False,
    )
    adaptive, adaptive_base = execute_predictor_mode(
        direction3_config,
        "uniform",
        "slot_median_ewma_drift_periodic_sync",
        direction3_prepared,
        write=False,
    )
    assert (
        adaptive.predictor_summary["prediction_rmse"] < frozen.predictor_summary["prediction_rmse"]
    )
    assert adaptive.predictor_summary["drift_trigger_count"] > 0
    assert (
        adaptive.predictor_summary["periodic_sync_count_trigger_count"]
        + adaptive.predictor_summary["periodic_sync_time_trigger_count"]
        > 0
    )
    assert adaptive.predictor_summary["predictor_state_divergence_count"] == 0
    maximum = direction3_config.predictor.adaptive.resynchronization.max_consecutive_suppressions
    assert maximum is not None
    assert adaptive.predictor_summary["maximum_consecutive_suppressions_observed"] <= maximum
    for summary in (frozen_base, adaptive_base):
        assert summary["suppression_violation_count"] == 0
        assert summary["budget_violation_count"] == 0
        assert summary["bound_violation_count"] == 0


def test_duplicate_plug_events_use_pre_batch_prediction(
    direction3_config: AppConfig,
    direction3_prepared: StreamingPreparedExperiment,
) -> None:
    result, _ = execute_predictor_mode(
        direction3_config,
        "uniform",
        "slot_median_ewma_drift_periodic_sync",
        direction3_prepared,
        write=False,
    )
    duplicate_timestamp = datetime(2013, 9, 8, 15, 0, tzinfo=UTC)
    rows = [
        row
        for row in result.bias_trace_rows
        if row["timestamp"] == duplicate_timestamp
        and row["household_id"] == 0
        and row["plug_id"] == 0
    ]
    assert len(rows) == 2
    assert rows[0]["prediction"] == rows[1]["prediction"]


def test_reason_priority_prefers_drift_over_periodic(direction3_config: AppConfig) -> None:
    event = Event(
        timestamp=datetime(2013, 9, 8, tzinfo=UTC),
        value=20.0,
        property_value=1,
        plug_id=0,
        household_id=0,
        house_id=0,
        original_row_index=0,
        event_id=0,
    )
    active = ActiveSet((event,), 600)
    runtime = DualPredictorRuntime(_ConstantPredictor(), "slot_median_ewma", 0.1)
    state = runtime.state(event.plug_key)
    state.consecutive_suppressions = 10_000
    drift = evaluate_drift(
        DriftRuntimeState(),
        100.0,
        1.0,
        direction3_config.predictor.adaptive.drift,
        enabled=True,
    )
    assert drift.trigger
    reason = _decision_reason(
        event=event,
        prediction=10.0,
        timestamp=event.timestamp,
        evaluation_start=event.timestamp,
        active=active,
        predictor_state=state,
        drift=drift,
        normal_would_transmit=True,
        predictor_mode="slot_median_ewma_drift_periodic_sync",
        config=direction3_config,
    )
    assert reason == "drift_detected"


def test_periodic_count_time_and_disabled_semantics(direction3_config: AppConfig) -> None:
    evaluation_start = datetime(2013, 9, 8, tzinfo=UTC)
    timestamp = datetime(2013, 9, 8, 0, 30, tzinfo=UTC)
    event = Event(
        timestamp=timestamp,
        value=10.0,
        property_value=1,
        plug_id=0,
        household_id=0,
        house_id=0,
        original_row_index=0,
        event_id=0,
    )
    active = ActiveSet((event,), 600)
    runtime = DualPredictorRuntime(_ConstantPredictor(), "slot_median_ewma", 0.1)
    state = runtime.state(event.plug_key)
    state.consecutive_suppressions = 100
    assert (
        _decision_reason(
            event=event,
            prediction=10.0,
            timestamp=timestamp,
            evaluation_start=evaluation_start,
            active=active,
            predictor_state=state,
            drift=None,
            normal_would_transmit=False,
            predictor_mode="slot_median_ewma_periodic_sync",
            config=direction3_config,
        )
        == "periodic_sync_count"
    )
    state.consecutive_suppressions = 0
    assert (
        _decision_reason(
            event=event,
            prediction=10.0,
            timestamp=timestamp,
            evaluation_start=evaluation_start,
            active=active,
            predictor_state=state,
            drift=None,
            normal_would_transmit=False,
            predictor_mode="slot_median_ewma_periodic_sync",
            config=direction3_config,
        )
        == "periodic_sync_time"
    )
    disabled_resync = direction3_config.predictor.adaptive.resynchronization.model_copy(
        update={"max_consecutive_suppressions": None, "max_silence_seconds": None}
    )
    adaptive = direction3_config.predictor.adaptive.model_copy(
        update={"resynchronization": disabled_resync}
    )
    disabled = direction3_config.model_copy(
        update={
            "predictor": direction3_config.predictor.model_copy(update={"adaptive": adaptive})
        }
    )
    assert (
        _decision_reason(
            event=event,
            prediction=10.0,
            timestamp=timestamp,
            evaluation_start=evaluation_start,
            active=active,
            predictor_state=state,
            drift=None,
            normal_would_transmit=False,
            predictor_mode="slot_median_ewma_periodic_sync",
            config=disabled,
        )
        is None
    )


def test_periodic_count_has_no_duplicate_transmission_count(
    direction3_config: AppConfig,
) -> None:
    resync = direction3_config.predictor.adaptive.resynchronization.model_copy(
        update={"max_consecutive_suppressions": 2, "max_silence_seconds": None}
    )
    drift = direction3_config.predictor.adaptive.drift.model_copy(update={"detector": "none"})
    adaptive = direction3_config.predictor.adaptive.model_copy(
        update={"drift": drift, "resynchronization": resync}
    )
    config = direction3_config.model_copy(
        update={
            "predictor": direction3_config.predictor.model_copy(update={"adaptive": adaptive})
        }
    )
    prepared = prepare_streaming(config)
    result, _summary = execute_predictor_mode(
        config, "uniform", "slot_median_ewma_periodic_sync", prepared, write=False
    )
    forced = result.base.forced_transmit["periodic_sync_count"]
    assert forced > 0
    assert forced == len(result.resynchronization_rows)
    assert result.predictor_summary["maximum_consecutive_suppressions_observed"] <= 2


def test_adaptive_replay_supports_full_tx_and_legacy_allocator(
    direction3_config: AppConfig,
    direction3_prepared: StreamingPreparedExperiment,
) -> None:
    full_tx, full_tx_summary = execute_predictor_mode(
        direction3_config,
        "full_tx",
        "slot_median_ewma",
        direction3_prepared,
        write=False,
    )
    assert full_tx.base.transmitted == full_tx.base.valid_events
    assert full_tx_summary["transmission_ratio"] == 1.0
    assert full_tx_summary["rmse"] == 0.0
    assert full_tx_summary["max"] == 0.0

    flat, _ = execute_predictor_mode(
        direction3_config,
        "flat_variance",
        "slot_median_ewma_drift_periodic_sync",
        direction3_prepared,
        write=False,
    )
    legacy, _ = execute_predictor_mode(
        direction3_config,
        "legacy_two_stage",
        "slot_median_ewma_drift_periodic_sync",
        direction3_prepared,
        write=False,
    )
    assert legacy.base.decision_sha256 == flat.base.decision_sha256


def test_predictor_matrix_uses_only_configured_combinations(
    direction3_config: AppConfig,
) -> None:
    experiment = direction3_config.experiment.model_copy(
        update={"modes": ("full_tx", "legacy_two_stage")}
    )
    predictor = direction3_config.predictor.model_copy(
        update={"modes": ("slot_median_frozen", "slot_median_ewma")}
    )
    config = direction3_config.model_copy(
        update={"experiment": experiment, "predictor": predictor}
    )
    rows = run_predictor_matrix(config)
    assert [
        (row["allocator_mode"], row["predictor_mode"]) for row in rows
    ] == [
        ("full_tx", "slot_median_frozen"),
        ("full_tx", "slot_median_ewma"),
        ("legacy_two_stage", "slot_median_frozen"),
        ("legacy_two_stage", "slot_median_ewma"),
    ]
    assert (config.experiment.output_dir / "predictor_matrix.csv").is_file()
    assert (config.experiment.output_dir / "predictor_matrix.json").is_file()


def test_compare_predictors_writes_all_artifacts(
    direction3_config: AppConfig,
) -> None:
    rows = compare_predictors(direction3_config, "uniform", matched_tr=True)
    assert len(rows) == 5
    run_dir = (
        direction3_config.experiment.output_dir
        / "predictors"
        / "uniform"
        / "slot_median_ewma_drift_periodic_sync"
    )
    expected = {
        "predictor_summary.json",
        "predictor_metrics.csv",
        "predictor_plug_metrics.parquet",
        "predictor_update_trace.parquet",
        "drift_events.parquet",
        "resynchronization_events.parquet",
        "drift_recovery.parquet",
        "adaptive_bias_trace.parquet",
        "predictor_config_resolved.yaml",
        "top_prediction_outliers.parquet",
    }
    assert expected <= {path.name for path in run_dir.iterdir()}
    assert (run_dir.parent / "predictor_comparison.csv").is_file()
    assert (run_dir.parent / "predictor_selection.json").is_file()
    assert (run_dir.parent / "predictor_pareto_points.csv").is_file()
    assert (run_dir.parent / "predictor_matched_tr.csv").is_file()
    assert (run_dir.parent / "predictor_tradeoff_summary.json").is_file()
    assert (run_dir.parent / "predictor_tr_rmse.png").is_file()
    assert (run_dir.parent / "predictor_outlier_analysis.json").is_file()
    assert (run_dir.parent / "predictor_root_cause.csv").is_file()


def test_staged_validation_uses_validation_interval_only(
    direction3_config: AppConfig,
) -> None:
    validation = direction3_config.splits.validation.model_copy(
        update={
            "enabled": True,
            "start": datetime(2013, 9, 8, 0, 0, tzinfo=UTC),
            "end": datetime(2013, 9, 8, 11, 59, 59, tzinfo=UTC),
        }
    )
    evaluation = direction3_config.splits.evaluation.model_copy(
        update={
            "start": datetime(2013, 9, 8, 12, 0, tzinfo=UTC),
            "end": datetime(2013, 9, 8, 23, 55, tzinfo=UTC),
        }
    )
    config = direction3_config.model_copy(
        update={
            "splits": direction3_config.splits.model_copy(
                update={"validation": validation, "evaluation": evaluation}
            )
        }
    )
    selection = validate_predictor_hyperparameters(config, "uniform")
    assert selection["evaluation_interval_used_for_tuning"] is False
    assert selection["candidate_count"] == 24
    assert selection["selection_interval"]["end"] == "2013-09-08T11:59:59+00:00"
    output_dir = config.experiment.output_dir / "predictor-validation" / "uniform"
    assert (output_dir / "predictor_validation_candidates.csv").is_file()
    selected_path = output_dir / "selected_predictor_config.yaml"
    selected = load_config(selected_path)
    assert selected.splits.evaluation.start == evaluation.start
    assert selected.predictor.mode == "slot_median_ewma_drift_periodic_sync"
