"""End-to-end experiment orchestration."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import yaml

from hiereb.config import (
    AdaptivePredictorConfig,
    AppConfig,
    Mode,
    PredictorMode,
)
from hiereb.data.loader import LoadedData, load_events
from hiereb.data.splits import mean_event_aligned_house_load, split_events
from hiereb.data.streaming import (
    StreamingPreparedExperiment,
    initialize_adaptive_biases,
    iter_evaluation_batches,
    prepare_streaming,
)
from hiereb.domain.models import Event
from hiereb.evaluation.artifacts import (
    file_sha256,
    write_adaptive_artifacts,
    write_artifacts,
    write_predictor_tradeoff_artifacts,
    write_streaming_artifacts,
)
from hiereb.evaluation.metrics import evaluate, evaluate_streaming
from hiereb.predictor.adaptive import COMPARISON_PREDICTOR_MODES
from hiereb.predictor.drift import DriftScaleModel
from hiereb.predictor.slot_median import TimeSliceMedianPredictor
from hiereb.residual.state import fit_warmup_residuals
from hiereb.simulation.adaptive_replay import AdaptiveReplayResult, replay_adaptive_streaming
from hiereb.simulation.replay import ReplayResult, replay
from hiereb.simulation.streaming_replay import StreamingReplayResult, replay_streaming
from hiereb.utils.progress import ProgressCallback, notify_progress


@dataclass(frozen=True, slots=True)
class PreparedExperiment:
    loaded: LoadedData
    warmup: tuple[Event, ...]
    evaluation: tuple[Event, ...]
    validation: tuple[Event, ...]
    predictor: TimeSliceMedianPredictor
    house_budget: float
    sigma_floor: float
    squared_residuals: dict[tuple[int, int], tuple[float, ...]]
    absolute_residuals: dict[tuple[int, int], tuple[float, ...]]
    mean_warmup_house_load: float
    input_sha256: str
    cap_quantiles: dict[float, dict[tuple[int, int], float]]
    cap_sample_counts: dict[tuple[int, int], int]


def prepare(config: AppConfig, progress: ProgressCallback | None = None) -> PreparedExperiment:
    notify_progress(progress, "prepare: loading and filtering input")
    loaded = load_events(config.data, config.splits, progress)
    notify_progress(progress, "prepare: creating warm-up/validation/evaluation splits")
    splits = split_events(loaded.events, config.splits)
    notify_progress(
        progress,
        f"prepare: split sizes warmup={len(splits.warmup):,}, "
        f"validation={len(splits.validation):,}, evaluation={len(splits.evaluation):,}",
    )
    notify_progress(progress, "prepare: fitting time-slice median predictor")
    predictor = TimeSliceMedianPredictor(config.predictor.slot_seconds)
    predictor.fit(splits.warmup)
    notify_progress(progress, "prepare: fitting residual floor and warm-up cap statistics")
    sigma_floor, squared, absolute = fit_warmup_residuals(
        splits.warmup,
        predictor,
        config.residual.sigma_floor_percentile,
        config.residual.rolling_window_size,
    )
    mean_load = mean_event_aligned_house_load(splits.warmup)
    budget = config.budget.epsilon_ratio * mean_load
    quantiles = {0.90, 0.95, 0.99, config.cap.quantile}
    cap_quantiles = {
        quantile: {plug: float(np.quantile(values, quantile)) for plug, values in absolute.items()}
        for quantile in quantiles
    }
    cap_sample_counts = {plug: len(values) for plug, values in absolute.items()}
    compact_loaded = LoadedData((), loaded.filter_counts, loaded.input_path)
    notify_progress(progress, f"prepare: hashing input {config.data.path}")
    input_sha256 = file_sha256(config.data.path)
    notify_progress(
        progress,
        f"prepare: ready; house_budget={budget:.6f}, sigma_floor={sigma_floor:.6f}",
    )
    return PreparedExperiment(
        compact_loaded,
        splits.warmup,
        splits.evaluation,
        splits.validation,
        predictor,
        budget,
        sigma_floor,
        squared,
        {},
        mean_load,
        input_sha256,
        cap_quantiles,
        cap_sample_counts,
    )


def execute_mode(
    config: AppConfig,
    mode: Mode,
    prepared: PreparedExperiment | None = None,
    *,
    write: bool = True,
    progress: ProgressCallback | None = None,
) -> tuple[ReplayResult, dict[str, Any]]:
    prepared = prepared or prepare(config, progress)
    notify_progress(
        progress,
        f"mode {mode}: replay start ({len(prepared.evaluation):,} evaluation events)",
    )
    result = replay(
        mode,
        prepared.evaluation,
        prepared.warmup,
        prepared.predictor,
        config,
        prepared.house_budget,
        prepared.sigma_floor,
        prepared.squared_residuals,
        prepared.absolute_residuals,
        prepared.cap_quantiles.get(config.cap.quantile),
        prepared.cap_sample_counts,
        progress,
    )
    notify_progress(progress, f"mode {mode}: computing metrics")
    summary = evaluate(result, prepared.house_budget)
    if write:
        output_dir = config.experiment.output_dir / mode
        notify_progress(progress, f"mode {mode}: writing artifacts to {output_dir}")
        write_artifacts(
            output_dir,
            config,
            result,
            summary,
            prepared.loaded.filter_counts,
            prepared.input_sha256,
        )
    notify_progress(
        progress,
        f"mode {mode}: done; TR={summary['transmission_ratio']:.6f}, "
        f"RMSE={summary['rmse']:.6f}, Max={summary['max']:.6f}",
    )
    return result, summary


def execute_mode_streaming(
    config: AppConfig,
    mode: Mode,
    prepared: StreamingPreparedExperiment | None = None,
    *,
    write: bool = True,
    progress: ProgressCallback | None = None,
) -> tuple[StreamingReplayResult, dict[str, Any]]:
    """Execute one mode with bounded-memory input and replay state."""
    prepared = prepared or prepare_streaming(config, progress)
    notify_progress(
        progress,
        f"mode {mode}: bounded-memory replay start "
        f"({prepared.evaluation_event_count:,} evaluation events)",
    )
    result = replay_streaming(
        mode,
        iter_evaluation_batches(prepared, config),
        prepared.predictor,
        config,
        prepared.house_budget,
        prepared.sigma_floor,
        prepared.squared_residuals,
        prepared.initial_last_seen,
        prepared.evaluation_event_count,
        prepared.cap_quantiles.get(config.cap.quantile),
        prepared.cap_sample_counts,
        progress,
    )
    notify_progress(progress, f"mode {mode}: computing compact metrics")
    summary = evaluate_streaming(result, prepared.house_budget)
    if write:
        output_dir = config.experiment.output_dir / mode
        notify_progress(progress, f"mode {mode}: writing artifacts to {output_dir}")
        write_streaming_artifacts(
            output_dir,
            config,
            result,
            summary,
            prepared.filter_counts,
            prepared.input_sha256,
        )
    notify_progress(
        progress,
        f"mode {mode}: done; TR={summary['transmission_ratio']:.6f}, "
        f"RMSE={summary['rmse']:.6f}, Max={summary['max']:.6f}",
    )
    return result, summary


def execute_predictor_mode(
    config: AppConfig,
    allocator_mode: Mode,
    predictor_mode: PredictorMode,
    prepared: StreamingPreparedExperiment | None = None,
    *,
    write: bool = True,
    progress: ProgressCallback | None = None,
) -> tuple[AdaptiveReplayResult, dict[str, Any]]:
    """Run one Direction 3 predictor mode against one fixed allocator."""
    effective_predictor_mode: PredictorMode = (
        predictor_mode if config.predictor.adaptive.enabled else "slot_median_frozen"
    )
    prepared = prepared or prepare_streaming(config, progress)
    notify_progress(
        progress,
        f"predictor {effective_predictor_mode}: allocator={allocator_mode}, "
        f"events={prepared.evaluation_event_count:,}",
    )
    scale_model = DriftScaleModel(
        per_plug=prepared.drift_scales,
        per_household=prepared.household_drift_scales,
        house_scale=prepared.house_drift_scale,
        sigma_floor=prepared.sigma_floor,
        min_scale=config.predictor.adaptive.drift.min_scale,
    )
    result = replay_adaptive_streaming(
        allocator_mode,
        effective_predictor_mode,
        iter_evaluation_batches(prepared, config),
        prepared.predictor,
        config,
        prepared.house_budget,
        prepared.sigma_floor,
        prepared.squared_residuals,
        prepared.initial_last_seen,
        prepared.evaluation_event_count,
        scale_model,
        (
            prepared.warmup_initial_biases
            if effective_predictor_mode != "slot_median_frozen"
            else {}
        ),
        prepared.cap_quantiles.get(config.cap.quantile),
        prepared.cap_sample_counts,
        progress,
    )
    base_summary = evaluate_streaming(result.base, prepared.house_budget)
    if write:
        output_dir = (
            config.experiment.output_dir / "predictors" / allocator_mode / effective_predictor_mode
        )
        notify_progress(
            progress,
            f"predictor {effective_predictor_mode}: writing artifacts to {output_dir}",
        )
        write_adaptive_artifacts(
            output_dir,
            config,
            result,
            base_summary,
            prepared.filter_counts,
            prepared.input_sha256,
            allocator_mode,
        )
    return result, base_summary


def compare_predictors(
    config: AppConfig,
    allocator_mode: Mode,
    progress: ProgressCallback | None = None,
    *,
    matched_tr: bool = False,
) -> list[dict[str, Any]]:
    """Run the five required Direction 3 predictor modes with one allocator."""
    if not config.data.streaming.enabled:
        raise ValueError("compare-predictors requires data.streaming.enabled=true")
    if not config.predictor.adaptive.enabled:
        raise ValueError("compare-predictors requires predictor.adaptive.enabled=true")
    prepared = prepare_streaming(config, progress)
    runs: list[tuple[AdaptiveReplayResult, dict[str, Any]]] = []
    for index, predictor_mode in enumerate(COMPARISON_PREDICTOR_MODES, start=1):
        notify_progress(
            progress,
            f"compare-predictors: {index}/{len(COMPARISON_PREDICTOR_MODES)} {predictor_mode}",
        )
        runs.append(
            execute_predictor_mode(
                config,
                allocator_mode,
                predictor_mode,
                prepared,
                progress=progress,
            )
        )
    frozen_result, frozen_base = runs[0]
    rows: list[dict[str, Any]] = []
    for result, base in runs:
        additional_tr = float(base["transmission_ratio"]) - float(frozen_base["transmission_ratio"])
        predictor_metrics = result.predictor_summary
        predictor_metrics.update(
            {
                "additional_TR_vs_frozen": additional_tr,
                "RMSE_delta_vs_frozen": float(base["rmse"]) - float(frozen_base["rmse"]),
                "CVaR99_delta_vs_frozen": float(base["cvar99"]) - float(frozen_base["cvar99"]),
                "Max_delta_vs_frozen": float(base["max"]) - float(frozen_base["max"]),
                "tail_reduction_per_additional_transmission": (
                    (float(frozen_base["cvar99"]) - float(base["cvar99"])) / additional_tr
                    if additional_tr > 0
                    else 0.0
                ),
            }
        )
        predictor_summary_path = (
            config.experiment.output_dir
            / "predictors"
            / allocator_mode
            / result.predictor_mode
            / "predictor_summary.json"
        )
        if predictor_summary_path.is_file():
            artifact_summary = json.loads(predictor_summary_path.read_text(encoding="utf-8"))
            artifact_summary["predictor_metrics"] = predictor_metrics
            predictor_summary_path.write_text(
                json.dumps(artifact_summary, indent=2, sort_keys=True),
                encoding="utf-8",
            )
        safety_pass = (
            all(
                int(base[key]) == 0
                for key in (
                    "suppression_violation_count",
                    "budget_violation_count",
                    "bound_violation_count",
                )
            )
            and int(predictor_metrics["predictor_state_divergence_count"]) == 0
        )
        rejected = (
            additional_tr > config.predictor.adaptive.maximum_additional_tr or not safety_pass
        )
        rows.append(
            {
                "allocator_mode": allocator_mode,
                "predictor_mode": result.predictor_mode,
                "deployable": result.deployable,
                "transmission_ratio": base["transmission_ratio"],
                "rmse": base["rmse"],
                "cvar99": base["cvar99"],
                "max": base["max"],
                "prediction_rmse": predictor_metrics["prediction_rmse"],
                "prediction_cvar99": predictor_metrics["prediction_cvar99"],
                "drift_trigger_count": predictor_metrics["drift_trigger_count"],
                "periodic_sync_count_trigger_count": predictor_metrics[
                    "periodic_sync_count_trigger_count"
                ],
                "periodic_sync_time_trigger_count": predictor_metrics[
                    "periodic_sync_time_trigger_count"
                ],
                "predictor_state_divergence_count": predictor_metrics[
                    "predictor_state_divergence_count"
                ],
                "additional_TR_vs_frozen": additional_tr,
                "RMSE_delta_vs_frozen": predictor_metrics["RMSE_delta_vs_frozen"],
                "CVaR99_delta_vs_frozen": predictor_metrics["CVaR99_delta_vs_frozen"],
                "Max_delta_vs_frozen": predictor_metrics["Max_delta_vs_frozen"],
                "tail_reduction_per_additional_transmission": predictor_metrics[
                    "tail_reduction_per_additional_transmission"
                ],
                "safety_pass": safety_pass,
                "rejected": rejected,
                "rejection_reason": (
                    "additional_tr_limit"
                    if additional_tr > config.predictor.adaptive.maximum_additional_tr
                    else ("safety" if not safety_pass else None)
                ),
            }
        )
    comparison_dir = config.experiment.output_dir / "predictors" / allocator_mode
    comparison_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_csv(comparison_dir / "predictor_comparison.csv")
    eligible = [
        row
        for row in rows
        if not row["rejected"]
        and row["deployable"]
        and row["predictor_mode"] != "slot_median_frozen"
    ]
    selected = min(
        eligible,
        key=lambda row: (
            float(row["cvar99"]),
            float(row["rmse"]),
            float(row["max"]),
            float(row["transmission_ratio"]),
            int(row["drift_trigger_count"])
            + int(row["periodic_sync_count_trigger_count"])
            + int(row["periodic_sync_time_trigger_count"]),
        ),
        default=None,
    )
    (comparison_dir / "predictor_selection.json").write_text(
        json.dumps({"selected": selected, "candidates": rows}, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    selected_result = (
        next(
            result
            for result, _base in runs
            if selected is not None and result.predictor_mode == selected["predictor_mode"]
        )
        if selected is not None
        else frozen_result
    )
    _write_predictor_root_cause(comparison_dir, frozen_result, selected_result)
    if matched_tr and selected is not None:
        _run_predictor_matched_tr_sweep(
            config,
            allocator_mode,
            selected_result.predictor_mode,
            prepared,
            comparison_dir,
            progress,
        )
    return rows


def run_predictor_matrix(
    config: AppConfig,
    progress: ProgressCallback | None = None,
) -> list[dict[str, Any]]:
    """Run the allocator x predictor matrix declared entirely in the config."""
    if not config.data.streaming.enabled:
        raise ValueError("predictor matrix requires data.streaming.enabled=true")
    if not config.predictor.adaptive.enabled and any(
        mode != "slot_median_frozen" for mode in config.predictor.modes
    ):
        raise ValueError(
            "predictor.adaptive.enabled=false only supports slot_median_frozen in predictor.modes"
        )
    prepared = prepare_streaming(config, progress)
    total = len(config.experiment.modes) * len(config.predictor.modes)
    rows: list[dict[str, Any]] = []
    run_index = 0
    for allocator_mode in config.experiment.modes:
        for predictor_mode in config.predictor.modes:
            run_index += 1
            notify_progress(
                progress,
                f"predictor-matrix: {run_index}/{total} "
                f"allocator={allocator_mode} predictor={predictor_mode}",
            )
            result, base = execute_predictor_mode(
                config,
                allocator_mode,
                predictor_mode,
                prepared,
                progress=progress,
            )
            predictor = result.predictor_summary
            safety_pass = (
                all(
                    int(base[key]) == 0
                    for key in (
                        "suppression_violation_count",
                        "budget_violation_count",
                        "bound_violation_count",
                    )
                )
                and int(predictor["predictor_state_divergence_count"]) == 0
            )
            rows.append(
                {
                    "allocator_mode": allocator_mode,
                    "predictor_mode": result.predictor_mode,
                    "transmission_ratio": base["transmission_ratio"],
                    "rmse": base["rmse"],
                    "cvar99": base["cvar99"],
                    "max": base["max"],
                    "prediction_mae": predictor["prediction_mae"],
                    "prediction_rmse": predictor["prediction_rmse"],
                    "prediction_cvar99": predictor["prediction_cvar99"],
                    "predictor_update_count": predictor["predictor_update_count"],
                    "drift_trigger_count": predictor["drift_trigger_count"],
                    "periodic_sync_count_trigger_count": predictor[
                        "periodic_sync_count_trigger_count"
                    ],
                    "periodic_sync_time_trigger_count": predictor[
                        "periodic_sync_time_trigger_count"
                    ],
                    "predictor_state_divergence_count": predictor[
                        "predictor_state_divergence_count"
                    ],
                    "safety_pass": safety_pass,
                }
            )
    output_dir = config.experiment.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_csv(output_dir / "predictor_matrix.csv")
    (output_dir / "predictor_matrix.json").write_text(
        json.dumps(
            {
                "allocator_modes": list(config.experiment.modes),
                "predictor_modes": list(config.predictor.modes),
                "combination_count": total,
                "rows": rows,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return rows


def validate_predictor_hyperparameters(
    config: AppConfig,
    allocator_mode: Mode = "uniform",
    progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Run the staged Direction 3 search using validation data only."""
    if not config.data.streaming.enabled:
        raise ValueError("validate-predictor requires data.streaming.enabled=true")
    validation = config.splits.validation
    if not validation.enabled or validation.start is None or validation.end is None:
        raise ValueError("validate-predictor requires an enabled validation interval")
    if not config.predictor.adaptive.enabled:
        raise ValueError("validate-predictor requires predictor.adaptive.enabled=true")

    prepared = prepare_streaming(config, progress)
    validation_splits = config.splits.model_copy(
        update={
            "validation": validation.model_copy(
                update={"enabled": False, "start": None, "end": None}
            ),
            "evaluation": config.splits.evaluation.model_copy(
                update={"start": validation.start, "end": validation.end}
            ),
        }
    )
    validation_config = config.model_copy(update={"splits": validation_splits})
    validation_prepared = replace(
        prepared,
        evaluation_event_count=prepared.validation_event_count,
    )
    frozen_result, frozen_summary = execute_predictor_mode(
        validation_config,
        allocator_mode,
        "slot_median_frozen",
        validation_prepared,
        write=False,
        progress=progress,
    )
    _ = frozen_result
    candidate_rows: list[dict[str, Any]] = []
    candidate_configs: dict[str, tuple[PredictorMode, AppConfig]] = {}
    initial_bias_cache: dict[float, dict[tuple[int, int], float]] = {}

    def evaluate_candidate(
        stage: str,
        candidate_id: str,
        mode: PredictorMode,
        candidate_config: AppConfig,
    ) -> None:
        notify_progress(progress, f"validate-predictor: {candidate_id}")
        try:
            alpha = candidate_config.predictor.adaptive.alpha
            if alpha not in initial_bias_cache:
                initial_bias_cache[alpha] = initialize_adaptive_biases(
                    validation_prepared.stage_path,
                    validation_prepared.predictor,
                    candidate_config,
                )
            candidate_prepared = replace(
                validation_prepared,
                warmup_initial_biases=initial_bias_cache[alpha],
            )
            result, summary = execute_predictor_mode(
                candidate_config,
                allocator_mode,
                mode,
                candidate_prepared,
                write=False,
                progress=progress,
            )
            predictor_metrics = result.predictor_summary
            drift_count = int(predictor_metrics["drift_trigger_count"])
            unresolved = int(predictor_metrics["number_of_unresolved_drift_episodes"])
            unresolved_fraction = unresolved / drift_count if drift_count else 0.0
            additional_tr = float(summary["transmission_ratio"]) - float(
                frozen_summary["transmission_ratio"]
            )
            safety_pass = (
                all(
                    int(summary[key]) == 0
                    for key in (
                        "suppression_violation_count",
                        "budget_violation_count",
                        "bound_violation_count",
                    )
                )
                and int(predictor_metrics["predictor_state_divergence_count"]) == 0
            )
            rejection_reasons: list[str] = []
            adaptive = candidate_config.predictor.adaptive
            if additional_tr > adaptive.maximum_additional_tr:
                rejection_reasons.append("additional_tr_limit")
            if unresolved_fraction > adaptive.maximum_unresolved_drift_fraction:
                rejection_reasons.append("unresolved_drift_limit")
            if not safety_pass:
                rejection_reasons.append("safety")
            drift = adaptive.drift
            resync = adaptive.resynchronization
            row: dict[str, Any] = {
                "stage": stage,
                "candidate_id": candidate_id,
                "predictor_mode": mode,
                "alpha": adaptive.alpha,
                "detector": drift.detector,
                "cusum_kappa": drift.cusum.kappa,
                "cusum_threshold_h": drift.cusum.threshold_h,
                "zscore_threshold": drift.zscore.threshold,
                "zscore_consecutive_count": drift.zscore.consecutive_count,
                "max_consecutive_suppressions": resync.max_consecutive_suppressions,
                "max_silence_seconds": resync.max_silence_seconds,
                "transmission_ratio": summary["transmission_ratio"],
                "additional_TR_vs_frozen": additional_tr,
                "rmse": summary["rmse"],
                "cvar99": summary["cvar99"],
                "max": summary["max"],
                "drift_trigger_count": drift_count,
                "periodic_sync_trigger_count": int(
                    predictor_metrics["periodic_sync_count_trigger_count"]
                )
                + int(predictor_metrics["periodic_sync_time_trigger_count"]),
                "unresolved_drift_fraction": unresolved_fraction,
                "safety_pass": safety_pass,
                "rejected": bool(rejection_reasons),
                "rejection_reason": ",".join(rejection_reasons) or None,
                "runtime_error": None,
            }
        except (MemoryError, RuntimeError, ValueError) as error:
            row = {
                "stage": stage,
                "candidate_id": candidate_id,
                "predictor_mode": mode,
                "rejected": True,
                "rejection_reason": "runtime_error",
                "runtime_error": f"{type(error).__name__}: {error}",
            }
        candidate_rows.append(row)
        candidate_configs[candidate_id] = (mode, candidate_config)

    no_drift = validation_config.predictor.adaptive.drift.model_copy(update={"detector": "none"})
    no_resync = validation_config.predictor.adaptive.resynchronization.model_copy(
        update={"max_consecutive_suppressions": None, "max_silence_seconds": None}
    )
    for alpha in (0.01, 0.05, 0.10, 0.20, 0.40):
        adaptive = validation_config.predictor.adaptive.model_copy(
            update={"alpha": alpha, "drift": no_drift, "resynchronization": no_resync}
        )
        candidate = _with_adaptive(validation_config, adaptive, "slot_median_ewma")
        evaluate_candidate("A_alpha", f"A-alpha-{alpha:.2f}", "slot_median_ewma", candidate)
    stage_a = _select_validation_candidate(candidate_rows, "A_alpha")

    best_a_mode, best_a_config = candidate_configs[str(stage_a["candidate_id"])]
    _ = best_a_mode
    for kappa in (0.25, 0.50, 1.00):
        for threshold_h in (3.0, 5.0, 8.0):
            drift = best_a_config.predictor.adaptive.drift.model_copy(
                update={
                    "detector": "cusum",
                    "cusum": best_a_config.predictor.adaptive.drift.cusum.model_copy(
                        update={"kappa": kappa, "threshold_h": threshold_h}
                    ),
                }
            )
            adaptive = best_a_config.predictor.adaptive.model_copy(
                update={"drift": drift, "resynchronization": no_resync}
            )
            candidate = _with_adaptive(best_a_config, adaptive, "slot_median_ewma_drift")
            candidate_id = f"B-cusum-k{kappa:.2f}-h{threshold_h:.1f}"
            evaluate_candidate("B_cusum", candidate_id, "slot_median_ewma_drift", candidate)
    stage_b = _select_validation_candidate(candidate_rows, "B_cusum")

    _best_b_mode, best_b_config = candidate_configs[str(stage_b["candidate_id"])]
    for maximum in (20, 50, 100, 200, None):
        resync = best_b_config.predictor.adaptive.resynchronization.model_copy(
            update={"max_consecutive_suppressions": maximum, "max_silence_seconds": None}
        )
        adaptive = best_b_config.predictor.adaptive.model_copy(update={"resynchronization": resync})
        candidate = _with_adaptive(best_b_config, adaptive, "slot_median_ewma_drift_periodic_sync")
        label = "off" if maximum is None else str(maximum)
        evaluate_candidate(
            "C_count",
            f"C-count-{label}",
            "slot_median_ewma_drift_periodic_sync",
            candidate,
        )
    stage_c_count = _select_validation_candidate(candidate_rows, "C_count")

    _count_mode, count_config = candidate_configs[str(stage_c_count["candidate_id"])]
    for maximum_silence in (300, 900, 1800, 3600, None):
        resync = count_config.predictor.adaptive.resynchronization.model_copy(
            update={"max_silence_seconds": maximum_silence}
        )
        adaptive = count_config.predictor.adaptive.model_copy(update={"resynchronization": resync})
        candidate = _with_adaptive(count_config, adaptive, "slot_median_ewma_drift_periodic_sync")
        label = "off" if maximum_silence is None else str(maximum_silence)
        evaluate_candidate(
            "C_time",
            f"C-time-{label}",
            "slot_median_ewma_drift_periodic_sync",
            candidate,
        )
    stage_c_time = _select_validation_candidate(candidate_rows, "C_time")
    selected_id = str(stage_c_time["candidate_id"])
    selected_mode, selected_validation_config = candidate_configs[selected_id]
    selected_config = _with_adaptive(
        config,
        selected_validation_config.predictor.adaptive,
        selected_mode,
    )

    output_dir = config.experiment.output_dir / "predictor-validation" / allocator_mode
    output_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(candidate_rows).write_csv(output_dir / "predictor_validation_candidates.csv")
    selection = {
        "allocator_mode": allocator_mode,
        "selection_interval": {
            "start": validation.start.isoformat(),
            "end": validation.end.isoformat(),
        },
        "evaluation_interval_used_for_tuning": False,
        "frozen_validation_metrics": frozen_summary,
        "stage_a_selected": stage_a,
        "stage_b_selected": stage_b,
        "stage_c_count_selected": stage_c_count,
        "selected": stage_c_time,
        "candidate_count": len(candidate_rows),
    }
    (output_dir / "predictor_validation_selection.json").write_text(
        json.dumps(selection, indent=2, sort_keys=True), encoding="utf-8"
    )
    (output_dir / "selected_predictor_config.yaml").write_text(
        yaml.safe_dump(selected_config.model_dump(mode="json"), sort_keys=True),
        encoding="utf-8",
    )
    return selection


def _with_adaptive(
    config: AppConfig,
    adaptive: AdaptivePredictorConfig,
    mode: PredictorMode,
) -> AppConfig:
    predictor = config.predictor.model_copy(update={"mode": mode, "adaptive": adaptive})
    return config.model_copy(update={"predictor": predictor})


def _select_validation_candidate(rows: list[dict[str, Any]], stage: str) -> dict[str, Any]:
    eligible = [row for row in rows if row.get("stage") == stage and not row["rejected"]]
    if not eligible:
        raise ValueError(f"all predictor validation candidates rejected in stage {stage}")
    return min(
        eligible,
        key=lambda row: (
            float(row["cvar99"]),
            float(row["rmse"]),
            float(row["max"]),
            float(row["transmission_ratio"]),
            int(row["drift_trigger_count"]) + int(row["periodic_sync_trigger_count"]),
            str(row["candidate_id"]),
        ),
    )


def _run_predictor_matched_tr_sweep(
    config: AppConfig,
    allocator_mode: Mode,
    selected_mode: PredictorMode,
    prepared: StreamingPreparedExperiment,
    output_dir: Path,
    progress: ProgressCallback | None,
) -> None:
    epsilon_values = (0.005, 0.01, 0.02, 0.03, 0.05, 0.075, 0.10)
    points: list[dict[str, Any]] = []
    for epsilon in epsilon_values:
        notify_progress(progress, f"matched-TR: epsilon_ratio={epsilon:.3f}")
        epsilon_config = config.model_copy(
            update={"budget": config.budget.model_copy(update={"epsilon_ratio": epsilon})}
        )
        epsilon_prepared = replace(
            prepared,
            house_budget=epsilon * prepared.mean_warmup_house_load,
        )
        sweep_modes: tuple[PredictorMode, ...] = ("slot_median_frozen", selected_mode)
        for mode in sweep_modes:
            result, summary = execute_predictor_mode(
                epsilon_config,
                allocator_mode,
                mode,
                epsilon_prepared,
                write=False,
                progress=progress,
            )
            points.append(
                {
                    "allocator_mode": allocator_mode,
                    "predictor_mode": mode,
                    "epsilon_ratio": epsilon,
                    "transmission_ratio": summary["transmission_ratio"],
                    "rmse": summary["rmse"],
                    "cvar99": summary["cvar99"],
                    "max": summary["max"],
                    "prediction_rmse": result.predictor_summary["prediction_rmse"],
                    "safety_pass": all(
                        int(summary[key]) == 0
                        for key in (
                            "suppression_violation_count",
                            "budget_violation_count",
                            "bound_violation_count",
                        )
                    ),
                }
            )
    write_predictor_tradeoff_artifacts(output_dir, points)


def _write_predictor_root_cause(
    output_dir: Path,
    frozen: AdaptiveReplayResult,
    adaptive: AdaptiveReplayResult,
) -> None:
    frozen_rows = {str(row["event_id"]): row for row in frozen.top_prediction_outlier_rows}
    adaptive_rows = {str(row["event_id"]): row for row in adaptive.top_prediction_outlier_rows}
    frozen_ids = set(frozen_rows)
    adaptive_ids = set(adaptive_rows)
    overlap = frozen_ids & adaptive_ids
    removed = frozen_ids - adaptive_ids
    introduced = adaptive_ids - frozen_ids
    analysis = {
        "frozen_predictor_mode": frozen.predictor_mode,
        "adaptive_predictor_mode": adaptive.predictor_mode,
        "outlier_overlap_count": len(overlap),
        "outlier_removed_by_adaptive_count": len(removed),
        "new_outlier_introduced_count": len(introduced),
        "outlier_overlap_event_ids": sorted(overlap),
        "removed_event_ids": sorted(removed),
        "introduced_event_ids": sorted(introduced),
        "trigger_precision_proxy": (
            1.0
            - float(adaptive.predictor_summary["redundant_drift_trigger_count"])
            / float(adaptive.predictor_summary["drift_trigger_count"])
            if int(adaptive.predictor_summary["drift_trigger_count"]) > 0
            else 0.0
        ),
        "redundant_trigger_rate": (
            float(adaptive.predictor_summary["redundant_drift_trigger_count"])
            / float(adaptive.predictor_summary["drift_trigger_count"])
            if int(adaptive.predictor_summary["drift_trigger_count"]) > 0
            else 0.0
        ),
        "frozen_top_1_percent_squared_error_share": frozen.predictor_summary[
            "top_1_percent_squared_error_share"
        ],
        "frozen_top_5_percent_squared_error_share": frozen.predictor_summary[
            "top_5_percent_squared_error_share"
        ],
        "adaptive_top_1_percent_squared_error_share": adaptive.predictor_summary[
            "top_1_percent_squared_error_share"
        ],
        "adaptive_top_5_percent_squared_error_share": adaptive.predictor_summary[
            "top_5_percent_squared_error_share"
        ],
        "median_recovery_time_after_drift": adaptive.predictor_summary[
            "median_recovery_time_after_drift"
        ],
        "p95_recovery_time_after_drift": adaptive.predictor_summary[
            "p95_recovery_time_after_drift"
        ],
        "mean_error_reduction_after_resync": adaptive.predictor_summary[
            "mean_error_reduction_after_resync"
        ],
    }
    root_rows: list[dict[str, Any]] = []
    for event_id in sorted(frozen_ids | adaptive_ids):
        frozen_row = frozen_rows.get(event_id)
        adaptive_row = adaptive_rows.get(event_id)
        source = adaptive_row or frozen_row
        assert source is not None
        root_rows.append(
            {
                "event_id": event_id,
                "timestamp": source["timestamp"],
                "household_id": source["household_id"],
                "plug_id": source["plug_id"],
                "classification": _classify_prediction_outlier(source),
                "in_frozen_top": frozen_row is not None,
                "in_adaptive_top": adaptive_row is not None,
                "frozen_absolute_prediction_error": (
                    frozen_row["absolute_prediction_error"] if frozen_row else None
                ),
                "adaptive_absolute_prediction_error": (
                    adaptive_row["absolute_prediction_error"] if adaptive_row else None
                ),
                "error_reduction": (
                    float(frozen_row["absolute_prediction_error"])
                    - float(adaptive_row["absolute_prediction_error"])
                    if frozen_row and adaptive_row
                    else None
                ),
                "adaptive_reason": adaptive_row["reason"] if adaptive_row else None,
            }
        )
    classifications: dict[str, int] = {}
    for row in root_rows:
        classification = str(row["classification"])
        classifications[classification] = classifications.get(classification, 0) + 1
    analysis.update(
        {
            "root_cause_counts": classifications,
            "predictor_error_contribution": sum(
                float(row["absolute_prediction_error"]) for row in frozen_rows.values()
            ),
            "threshold_concentration_contribution": classifications.get(
                "threshold_concentration", 0
            ),
            "suppression_streak_contribution": classifications.get("long_suppression_streak", 0),
            "drift_trigger_timing": [
                row["timestamp"].isoformat() for row in adaptive.drift_event_rows
            ],
        }
    )
    (output_dir / "predictor_outlier_analysis.json").write_text(
        json.dumps(analysis, indent=2, sort_keys=True), encoding="utf-8"
    )
    pl.DataFrame(root_rows).sort(
        ["timestamp", "household_id", "plug_id", "event_id"], maintain_order=True
    ).write_csv(output_dir / "predictor_root_cause.csv")


def _classify_prediction_outlier(row: dict[str, Any]) -> str:
    if row.get("reason") == "inactive_reactivation":
        return "reactivation"
    if int(row.get("prior_consecutive_suppressions") or 0) >= 20:
        return "long_suppression_streak"
    standardized = float(row.get("standardized_innovation") or 0.0)
    if standardized >= 10.0:
        return "isolated_spike"
    if row.get("reason") == "drift_detected" and standardized >= 5.0:
        return "abrupt_regime_change"
    if row.get("reason") == "drift_detected":
        return "gradual_drift"
    if float(row.get("threshold_used") or 0.0) <= 0.0:
        return "threshold_concentration"
    return "persistent_predictor_bias"


def compare_modes(
    config: AppConfig, progress: ProgressCallback | None = None
) -> list[dict[str, Any]]:
    if config.data.streaming.enabled:
        return _compare_modes_streaming(config, progress)
    notify_progress(
        progress,
        f"run: preparing once for {len(config.experiment.modes)} configured modes",
    )
    prepared = prepare(config, progress)
    summaries: list[dict[str, Any]] = []
    for index, mode in enumerate(config.experiment.modes, start=1):
        notify_progress(
            progress,
            f"run: mode {index}/{len(config.experiment.modes)} -> {mode}",
        )
        summaries.append(execute_mode(config, mode, prepared, progress=progress)[1])
    config.experiment.output_dir.mkdir(parents=True, exist_ok=True)
    flat = [{k: v for k, v in row.items() if not isinstance(v, dict)} for row in summaries]
    pl.DataFrame(flat).write_csv(config.experiment.output_dir / "comparison.csv")
    notify_progress(
        progress,
        f"run: comparison written to {config.experiment.output_dir / 'comparison.csv'}",
    )
    return summaries


def _compare_modes_streaming(
    config: AppConfig, progress: ProgressCallback | None
) -> list[dict[str, Any]]:
    notify_progress(
        progress,
        f"run: streaming preparation once for {len(config.experiment.modes)} configured modes",
    )
    prepared = prepare_streaming(config, progress)
    summaries: list[dict[str, Any]] = []
    for index, mode in enumerate(config.experiment.modes, start=1):
        notify_progress(
            progress,
            f"run: streaming mode {index}/{len(config.experiment.modes)} -> {mode}",
        )
        summaries.append(execute_mode_streaming(config, mode, prepared, progress=progress)[1])
    config.experiment.output_dir.mkdir(parents=True, exist_ok=True)
    flat = [
        {key: value for key, value in row.items() if not isinstance(value, dict)}
        for row in summaries
    ]
    pl.DataFrame(flat).write_csv(config.experiment.output_dir / "comparison.csv")
    notify_progress(
        progress,
        f"run: comparison written to {config.experiment.output_dir / 'comparison.csv'}",
    )
    return summaries


def verify_legacy_equivalence(
    config: AppConfig, tolerance: float | None = None
) -> dict[str, float | int | bool]:
    if config.data.streaming.enabled:
        return _verify_legacy_equivalence_streaming(config, tolerance)
    prepared = prepare(config)
    flat, _ = execute_mode(config, "flat_variance", prepared, write=False)
    legacy, _ = execute_mode(config, "legacy_two_stage", prepared, write=False)
    tolerance = tolerance if tolerance is not None else config.replay.float_tolerance
    if len(flat.threshold_rows) != len(legacy.threshold_rows):
        return {
            "equivalent": False,
            "threshold_rows": len(flat.threshold_rows),
            "max_difference": float("inf"),
        }
    max_difference = max(
        (
            abs(float(left["threshold"]) - float(right["threshold"]))
            for left, right in zip(flat.threshold_rows, legacy.threshold_rows, strict=True)
        ),
        default=0.0,
    )
    decisions_equal = [row["transmitted"] for row in flat.event_rows] == [
        row["transmitted"] for row in legacy.event_rows
    ]
    return {
        "equivalent": max_difference <= tolerance and decisions_equal,
        "threshold_rows": len(flat.threshold_rows),
        "max_difference": max_difference,
        "decisions_equal": decisions_equal,
    }


def _verify_legacy_equivalence_streaming(
    config: AppConfig, tolerance: float | None
) -> dict[str, float | int | bool]:
    prepared = prepare_streaming(config)
    flat, _ = execute_mode_streaming(config, "flat_variance", prepared, write=False)
    legacy, _ = execute_mode_streaming(config, "legacy_two_stage", prepared, write=False)
    tolerance = tolerance if tolerance is not None else config.replay.float_tolerance
    if len(flat.threshold_rows) != len(legacy.threshold_rows):
        return {
            "equivalent": False,
            "threshold_rows": len(flat.threshold_rows),
            "max_difference": float("inf"),
            "decisions_equal": False,
        }
    max_difference = max(
        (
            abs(float(left["threshold"]) - float(right["threshold"]))
            for left, right in zip(flat.threshold_rows, legacy.threshold_rows, strict=True)
        ),
        default=0.0,
    )
    decisions_equal = (
        flat.valid_events == legacy.valid_events and flat.decision_sha256 == legacy.decision_sha256
    )
    return {
        "equivalent": max_difference <= tolerance and decisions_equal,
        "threshold_rows": len(flat.threshold_rows),
        "max_difference": max_difference,
        "decisions_equal": decisions_equal,
    }
