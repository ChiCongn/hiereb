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


def _write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")


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
