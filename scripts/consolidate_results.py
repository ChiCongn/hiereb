"""Consolidate Direction 1 and Direction 3 run summaries into one CSV."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import polars as pl
import yaml

OUTPUT_COLUMNS = (
    "source_group",
    "result_type",
    "source_path",
    "experiment_name",
    "input_sha256",
    "house_id",
    "evaluation_start",
    "evaluation_end",
    "allocator_mode",
    "predictor_mode",
    "valid_events",
    "timestamp_batches",
    "transmitted",
    "suppressed",
    "transmission_ratio",
    "reduction",
    "mae",
    "rmse",
    "p95",
    "p99",
    "cvar95",
    "cvar99",
    "max",
    "house_budget",
    "bound_violation_count",
    "suppression_violation_count",
    "budget_violation_count",
    "maximum_consecutive_suppression",
    "prediction_events",
    "prediction_mae",
    "prediction_rmse",
    "prediction_p95",
    "prediction_p99",
    "prediction_cvar95",
    "prediction_cvar99",
    "prediction_bias",
    "predictor_update_count",
    "predictor_update_rate",
    "drift_trigger_count",
    "additional_drift_transmission_count",
    "periodic_sync_count_trigger_count",
    "periodic_sync_time_trigger_count",
    "additional_periodic_transmission_count",
    "maximum_consecutive_suppressions_observed",
    "predictor_state_divergence_count",
    "safety_pass",
)


def _one_row(path: Path) -> dict[str, Any]:
    frame = pl.read_csv(path)
    if frame.height != 1:
        raise ValueError(f"expected exactly one summary row: {path}")
    return dict(frame.row(0, named=True))


def _run_context(run_dir: Path) -> dict[str, Any]:
    config_path = run_dir / "resolved_config.yaml"
    metadata_path = run_dir / "run_metadata.json"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    evaluation = config["splits"]["evaluation"]
    return {
        "experiment_name": config["experiment"]["name"],
        "input_sha256": metadata["input_sha256"],
        "house_id": config["data"]["house_id"],
        "evaluation_start": str(evaluation["start"]),
        "evaluation_end": str(evaluation["end"]),
    }


def _safety_pass(summary: dict[str, Any], divergence_count: int = 0) -> bool:
    return (
        int(summary["bound_violation_count"]) == 0
        and int(summary["suppression_violation_count"]) == 0
        and int(summary["budget_violation_count"]) == 0
        and divergence_count == 0
    )


def _baseline_rows(root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for summary_path in sorted(root.glob("*/summary.csv")):
        run_dir = summary_path.parent
        summary = _one_row(summary_path)
        row = {
            "source_group": root.name,
            "result_type": "allocator_baseline",
            "source_path": str(summary_path),
            "allocator_mode": summary["mode"],
            "predictor_mode": "slot_median_frozen",
            **_run_context(run_dir),
            **summary,
            "safety_pass": _safety_pass(summary),
        }
        rows.append(row)
    return rows


def _adaptive_rows(root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for summary_path in sorted(root.glob("predictors/*/*/summary.csv")):
        run_dir = summary_path.parent
        metrics_path = run_dir / "predictor_metrics.csv"
        if not metrics_path.is_file():
            raise FileNotFoundError(f"missing predictor metrics: {metrics_path}")
        summary = _one_row(summary_path)
        metrics = _one_row(metrics_path)
        divergence = int(metrics["predictor_state_divergence_count"])
        row = {
            "source_group": root.name,
            "result_type": "adaptive_predictor",
            "source_path": str(summary_path),
            "allocator_mode": metrics["allocator_mode"],
            "predictor_mode": metrics["predictor_mode"],
            **_run_context(run_dir),
            **summary,
            "prediction_events": metrics["events"],
            **{
                key: metrics[key]
                for key in OUTPUT_COLUMNS
                if key.startswith("prediction_") and key in metrics
            },
            **{
                key: metrics[key]
                for key in (
                    "predictor_update_count",
                    "predictor_update_rate",
                    "drift_trigger_count",
                    "additional_drift_transmission_count",
                    "periodic_sync_count_trigger_count",
                    "periodic_sync_time_trigger_count",
                    "additional_periodic_transmission_count",
                    "maximum_consecutive_suppressions_observed",
                    "predictor_state_divergence_count",
                )
            },
            "safety_pass": _safety_pass(summary, divergence),
        }
        rows.append(row)
    return rows


def consolidate(adaptive_root: Path, baseline_root: Path, output: Path) -> pl.DataFrame:
    """Read canonical per-run summaries and write one normalized table."""
    rows = _baseline_rows(baseline_root) + _adaptive_rows(adaptive_root)
    if not rows:
        raise ValueError("no run summaries found")
    normalized = [{column: row.get(column) for column in OUTPUT_COLUMNS} for row in rows]
    frame = pl.DataFrame(normalized, infer_schema_length=None).sort(
        ["source_group", "allocator_mode", "predictor_mode"], maintain_order=True
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.write_csv(output)
    return frame


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--adaptive-root", type=Path, default=Path("outputs/all-modes"))
    parser.add_argument("--baseline-root", type=Path, default=Path("outputs/offline-house0"))
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("outputs/combined_all_modes_offline_house0.csv"),
    )
    args = parser.parse_args()
    frame = consolidate(args.adaptive_root, args.baseline_root, args.output)
    print(f"wrote {frame.height} rows to {args.output}")


if __name__ == "__main__":
    main()
