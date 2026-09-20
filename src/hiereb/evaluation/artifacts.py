"""Deterministic PRD artifact writer."""

from __future__ import annotations

import hashlib
import json
import os
import platform
from pathlib import Path
from typing import Any

os.environ.setdefault("MPLCONFIGDIR", "/tmp/hiereb-matplotlib")

import matplotlib
import numpy as np
import polars as pl
import yaml

from hiereb import __version__
from hiereb.config import AppConfig
from hiereb.evaluation.diagnostics import (
    daily_metrics,
    household_metrics,
    plug_metrics,
    top_outliers,
)
from hiereb.simulation.adaptive_replay import AdaptiveReplayResult
from hiereb.simulation.replay import ReplayResult
from hiereb.simulation.streaming_replay import StreamingReplayResult

matplotlib.use("Agg")
from matplotlib import pyplot as plt


def write_artifacts(
    output_dir: Path,
    config: AppConfig,
    result: ReplayResult,
    summary: dict[str, Any],
    filter_counts: dict[str, int],
    input_sha256: str | None = None,
) -> None:
    """Write every output required by the PRD for one mode."""
    if output_dir.exists() and any(output_dir.iterdir()) and not config.experiment.overwrite:
        raise FileExistsError(f"output exists and overwrite=false: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    plots_dir = output_dir / "plots"
    plots_dir.mkdir(exist_ok=True)

    resolved = config.model_dump(mode="json")
    (output_dir / "resolved_config.yaml").write_text(
        yaml.safe_dump(resolved, sort_keys=True), encoding="utf-8"
    )
    metadata = {
        "package_version": __version__,
        "python_version": platform.python_version(),
        "mode": result.mode,
        "seed": config.experiment.seed,
        "config_sha256": hashlib.sha256(
            json.dumps(resolved, sort_keys=True).encode("utf-8")
        ).hexdigest(),
        "input_sha256": input_sha256 or file_sha256(config.data.path),
    }
    _write_json(output_dir / "run_metadata.json", metadata)
    _write_json(output_dir / "summary.json", summary)
    flat_summary = {k: v for k, v in summary.items() if not isinstance(v, dict)}
    flat_summary.update(
        {f"forced_{key}": value for key, value in summary["forced_transmit"].items()}
    )
    pl.DataFrame([flat_summary]).write_csv(output_dir / "summary.csv")
    pl.DataFrame(daily_metrics(result)).write_csv(output_dir / "daily_metrics.csv")
    pl.DataFrame(household_metrics(result)).write_csv(output_dir / "household_metrics.csv")
    pl.DataFrame(plug_metrics(result)).write_parquet(output_dir / "plug_metrics.parquet")
    pl.DataFrame(result.threshold_rows).write_parquet(output_dir / "threshold_trace.parquet")
    pl.DataFrame(top_outliers(result, config.replay.top_k_outliers)).write_parquet(
        output_dir / "top_outliers.parquet"
    )
    _write_json(output_dir / "filter_counts.json", filter_counts)
    _write_plots(plots_dir, result)


def write_streaming_artifacts(
    output_dir: Path,
    config: AppConfig,
    result: StreamingReplayResult,
    summary: dict[str, Any],
    filter_counts: dict[str, int],
    input_sha256: str,
) -> None:
    """Write the PRD artifact set from compact streaming aggregates."""
    if output_dir.exists() and any(output_dir.iterdir()) and not config.experiment.overwrite:
        raise FileExistsError(f"output exists and overwrite=false: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    plots_dir = output_dir / "plots"
    plots_dir.mkdir(exist_ok=True)
    resolved = config.model_dump(mode="json")
    (output_dir / "resolved_config.yaml").write_text(
        yaml.safe_dump(resolved, sort_keys=True), encoding="utf-8"
    )
    metadata = {
        "package_version": __version__,
        "python_version": platform.python_version(),
        "mode": result.mode,
        "seed": config.experiment.seed,
        "config_sha256": hashlib.sha256(
            json.dumps(resolved, sort_keys=True).encode("utf-8")
        ).hexdigest(),
        "input_sha256": input_sha256,
        "streaming": True,
        "chunk_rows": config.data.streaming.chunk_rows,
    }
    _write_json(output_dir / "run_metadata.json", metadata)
    _write_json(output_dir / "summary.json", summary)
    flat_summary = {k: v for k, v in summary.items() if not isinstance(v, dict)}
    flat_summary.update(
        {f"forced_{key}": value for key, value in summary["forced_transmit"].items()}
    )
    pl.DataFrame([flat_summary]).write_csv(output_dir / "summary.csv")
    pl.DataFrame(result.daily_rows).write_csv(output_dir / "daily_metrics.csv")
    pl.DataFrame(result.household_rows).write_csv(output_dir / "household_metrics.csv")
    pl.DataFrame(result.plug_rows).write_parquet(output_dir / "plug_metrics.parquet")
    pl.DataFrame(result.threshold_rows).write_parquet(output_dir / "threshold_trace.parquet")
    pl.DataFrame(result.top_outlier_rows).write_parquet(output_dir / "top_outliers.parquet")
    _write_json(output_dir / "filter_counts.json", filter_counts)
    _write_streaming_plots(plots_dir, result)


def write_adaptive_artifacts(
    output_dir: Path,
    config: AppConfig,
    result: AdaptiveReplayResult,
    base_summary: dict[str, Any],
    filter_counts: dict[str, int],
    input_sha256: str,
    allocator_mode: str,
) -> None:
    """Write Direction 1 safety artifacts plus all Direction 3 predictor artifacts."""
    write_streaming_artifacts(
        output_dir,
        config,
        result.base,
        base_summary,
        filter_counts,
        input_sha256,
    )
    predictor_summary = {
        "allocator_mode": allocator_mode,
        "predictor_mode": result.predictor_mode,
        "deployable": result.deployable,
        "base_metrics": base_summary,
        "predictor_metrics": result.predictor_summary,
    }
    _write_json(output_dir / "predictor_summary.json", predictor_summary)
    flat_metrics = {
        key: value for key, value in result.predictor_summary.items() if not isinstance(value, dict)
    }
    flat_metrics.update(
        {
            "allocator_mode": allocator_mode,
            "predictor_mode": result.predictor_mode,
            "transmission_ratio": base_summary["transmission_ratio"],
            "house_rmse": base_summary["rmse"],
            "house_cvar99": base_summary["cvar99"],
            "house_max": base_summary["max"],
        }
    )
    pl.DataFrame([flat_metrics]).write_csv(output_dir / "predictor_metrics.csv")
    _write_parquet_rows(
        output_dir / "predictor_plug_metrics.parquet",
        result.predictor_plug_rows,
        {"household_id": pl.Int64, "plug_id": pl.Int64, "predictor_mode": pl.String},
    )
    _write_parquet_rows(
        output_dir / "predictor_update_trace.parquet",
        result.predictor_update_rows,
        {"timestamp": pl.Datetime("us", "UTC"), "event_id": pl.String},
    )
    _write_parquet_rows(
        output_dir / "drift_events.parquet",
        result.drift_event_rows,
        {"timestamp": pl.Datetime("us", "UTC"), "event_id": pl.String},
    )
    _write_parquet_rows(
        output_dir / "resynchronization_events.parquet",
        result.resynchronization_rows,
        {"timestamp": pl.Datetime("us", "UTC"), "event_id": pl.String},
    )
    _write_parquet_rows(
        output_dir / "drift_recovery.parquet",
        result.recovery_rows,
        {"trigger_timestamp": pl.Datetime("us", "UTC")},
    )
    _write_parquet_rows(
        output_dir / "adaptive_bias_trace.parquet",
        result.bias_trace_rows,
        {"timestamp": pl.Datetime("us", "UTC"), "event_id": pl.String},
    )
    _write_parquet_rows(
        output_dir / "top_prediction_outliers.parquet",
        result.top_prediction_outlier_rows,
        {"timestamp": pl.Datetime("us", "UTC"), "event_id": pl.String},
    )
    predictor_resolved = {
        "allocator": allocator_mode,
        "mode": result.predictor_mode,
        "deployable": result.deployable,
        "predictor": config.predictor.model_dump(mode="json"),
        "predictor_trace": config.artifacts.predictor_trace.model_dump(mode="json"),
    }
    (output_dir / "predictor_config_resolved.yaml").write_text(
        yaml.safe_dump(predictor_resolved, sort_keys=True), encoding="utf-8"
    )


def write_predictor_tradeoff_artifacts(
    output_dir: Path,
    points: list[dict[str, Any]],
) -> None:
    """Write matched-TR tables and Pareto plots for a selected adaptive mode."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for point in points:
        point["pareto_dominated"] = _is_predictor_point_dominated(point, points)
    pl.DataFrame(points).write_csv(output_dir / "predictor_pareto_points.csv")
    frozen = [point for point in points if point["predictor_mode"] == "slot_median_frozen"]
    adaptive = [point for point in points if point["predictor_mode"] != "slot_median_frozen"]
    matched: list[dict[str, Any]] = []
    for point in adaptive:
        reference = min(
            frozen,
            key=lambda candidate: abs(
                float(candidate["transmission_ratio"]) - float(point["transmission_ratio"])
            ),
        )
        cvar_reference = min(
            frozen,
            key=lambda candidate: abs(float(candidate["cvar99"]) - float(point["cvar99"])),
        )
        matched.append(
            {
                "predictor_mode": point["predictor_mode"],
                "adaptive_epsilon_ratio": point["epsilon_ratio"],
                "frozen_epsilon_ratio": reference["epsilon_ratio"],
                "adaptive_transmission_ratio": point["transmission_ratio"],
                "frozen_transmission_ratio": reference["transmission_ratio"],
                "transmission_ratio_gap": float(point["transmission_ratio"])
                - float(reference["transmission_ratio"]),
                "rmse_at_matched_tr": point["rmse"],
                "frozen_rmse_at_matched_tr": reference["rmse"],
                "cvar99_at_matched_tr": point["cvar99"],
                "frozen_cvar99_at_matched_tr": reference["cvar99"],
                "max_at_matched_tr": point["max"],
                "frozen_max_at_matched_tr": reference["max"],
                "adaptive_tr_at_matched_cvar99": point["transmission_ratio"],
                "frozen_tr_at_matched_cvar99": cvar_reference["transmission_ratio"],
                "adaptive_cvar99_target": point["cvar99"],
                "frozen_cvar99_at_matched_cvar99": cvar_reference["cvar99"],
            }
        )
    pl.DataFrame(matched).write_csv(output_dir / "predictor_matched_tr.csv")
    _write_json(
        output_dir / "predictor_tradeoff_summary.json",
        {
            "point_count": len(points),
            "matched_pair_count": len(matched),
            "pareto_dominated_point_count": sum(
                bool(point["pareto_dominated"]) for point in points
            ),
            "adaptive_modes": sorted(
                {str(point["predictor_mode"]) for point in adaptive}
            ),
        },
    )
    for metric, filename in (
        ("rmse", "predictor_tr_rmse.png"),
        ("cvar99", "predictor_tr_cvar99.png"),
        ("max", "predictor_tr_max.png"),
    ):
        fig, axis = plt.subplots(figsize=(5, 3))
        for mode in sorted({str(point["predictor_mode"]) for point in points}):
            mode_points = sorted(
                (point for point in points if point["predictor_mode"] == mode),
                key=lambda point: float(point["transmission_ratio"]),
            )
            axis.plot(
                [float(point["transmission_ratio"]) for point in mode_points],
                [float(point[metric]) for point in mode_points],
                marker="o",
                label=mode,
            )
        axis.set_xlabel("transmission ratio")
        axis.set_ylabel(metric)
        axis.legend(fontsize=6)
        fig.tight_layout()
        fig.savefig(output_dir / filename, dpi=120)
        plt.close(fig)


def _is_predictor_point_dominated(point: dict[str, Any], points: list[dict[str, Any]]) -> bool:
    objectives = ("transmission_ratio", "rmse", "cvar99", "max")
    for candidate in points:
        if candidate is point:
            continue
        no_worse = all(float(candidate[key]) <= float(point[key]) for key in objectives)
        strictly_better = any(float(candidate[key]) < float(point[key]) for key in objectives)
        if no_worse and strictly_better:
            return True
    return False


def _write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")


def _write_parquet_rows(
    path: Path,
    rows: tuple[dict[str, Any], ...],
    empty_schema: dict[str, Any],
) -> None:
    frame = pl.DataFrame(rows) if rows else pl.DataFrame(schema=empty_schema)
    if "timestamp" in frame.columns:
        sort_columns = [
            column
            for column in ("timestamp", "household_id", "plug_id", "event_id")
            if column in frame.columns
        ]
        frame = frame.sort(sort_columns, maintain_order=True)
    elif "trigger_timestamp" in frame.columns:
        frame = frame.sort("trigger_timestamp", maintain_order=True)
    frame.write_parquet(path)


def file_sha256(path: Path) -> str:
    """Hash an input once so multi-mode runners can reuse the digest."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_plots(output_dir: Path, result: ReplayResult) -> None:
    timestamps = [row["timestamp"] for row in result.batch_rows]
    errors = [row["house_error"] for row in result.batch_rows]
    fig, axis = plt.subplots(figsize=(8, 3))
    axis.plot(timestamps, errors, linewidth=0.8)
    axis.set_title(f"Event-aligned house error: {result.mode}")
    axis.set_ylabel("error")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(output_dir / "house_error.png", dpi=120)
    plt.close(fig)

    fig, axis = plt.subplots(figsize=(5, 3))
    axis.hist([abs(float(value)) for value in errors], bins=min(30, max(len(errors), 1)))
    axis.set_title("Absolute error distribution")
    axis.set_xlabel("absolute error")
    fig.tight_layout()
    fig.savefig(output_dir / "absolute_error_histogram.png", dpi=120)
    plt.close(fig)


def _write_streaming_plots(output_dir: Path, result: StreamingReplayResult) -> None:
    errors = np.frombuffer(result.errors, dtype=np.float64)
    fig, axis = plt.subplots(figsize=(8, 3))
    numeric_timestamps = [timestamp.timestamp() for timestamp in result.plot_timestamps]
    axis.plot(numeric_timestamps, list(result.plot_errors), linewidth=0.8)
    axis.set_xlabel("UTC epoch seconds")
    axis.set_title(f"Event-aligned house error: {result.mode}")
    axis.set_ylabel("error")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(output_dir / "house_error.png", dpi=120)
    plt.close(fig)

    fig, axis = plt.subplots(figsize=(5, 3))
    axis.hist(np.abs(errors), bins=min(30, max(errors.size, 1)))
    axis.set_title("Absolute error distribution")
    axis.set_xlabel("absolute error")
    fig.tight_layout()
    fig.savefig(output_dir / "absolute_error_histogram.png", dpi=120)
    plt.close(fig)
