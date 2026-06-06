"""Report CSV exporter for required HierEB experiment artifacts.

The exporter consumes a run artifact with event decisions and threshold trace
records, then writes the six mandatory CSV files used by the report pipeline.
It is intentionally independent from Kafka and TimescaleDB so schema tests can
run with small in-memory fixtures.
"""
from __future__ import annotations

import csv
import json
import math
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Iterable


EXPERIMENT_RUNS_COLUMNS = [
    "run_id",
    "mode",
    "started_at",
    "completed_at",
    "house_ids",
    "property_filter",
    "warmup_start",
    "warmup_end",
    "eval_start",
    "eval_end",
    "Delta_H",
    "epsilon_ratio",
    "tau",
    "active_window_seconds",
    "predictor_bin_seconds",
    "uniform_delta_initial",
    "sigma_floor_used",
    "is_sweep",
    "sweep_id",
    "sweep_param_name",
    "sweep_param_value",
    "notes",
]

EVENT_DECISIONS_COLUMNS = [
    "run_id",
    "mode",
    "timestamp",
    "source_timestamp",
    "house_id",
    "household_id",
    "plug_id",
    "plug_uid",
    "actual_load",
    "predicted_load",
    "reconstructed_load",
    "transmitted",
    "decision",
    "reason",
    "plug_status",
    "is_forced_transmit",
    "threshold",
    "residual",
    "abs_residual",
]

HOUSE_TIMESERIES_COLUMNS = [
    "run_id",
    "mode",
    "timestamp",
    "source_timestamp",
    "house_id",
    "actual_load",
    "predicted_load",
    "reconstructed_load",
    "house_error",
    "abs_house_error",
    "transmission_rate",
    "plug_count",
    "transmitted_count",
    "suppressed_count",
    "forced_transmit_count",
    "missing_prediction_count",
    "inactive_reactivation_count",
    "rolling_tr_1h",
    "rolling_rmse_1h",
    "rolling_insufficient_data",
]

PLUG_METRICS_COLUMNS = [
    "run_id",
    "mode",
    "house_id",
    "household_id",
    "plug_id",
    "plug_uid",
    "event_count",
    "transmitted_count",
    "suppressed_count",
    "transmission_rate",
    "rmse_reconstruction",
    "mae_reconstruction",
    "rmse_prediction",
    "mae_prediction",
    "inactive_reactivation_count",
    "missing_prediction_count",
    "forced_transmit_count",
]

HOUSE_SUMMARY_COLUMNS = [
    "run_id",
    "mode",
    "house_id",
    "event_count",
    "timestamp_start",
    "timestamp_end",
    "avg_transmission_rate",
    "rmse_house_error",
    "mae_house_error",
    "p95_abs_house_error",
    "max_abs_house_error",
    "Delta_H",
    "epsilon_ratio",
    "total_plug_events",
    "transmitted_count",
    "suppressed_count",
    "missing_prediction_count",
    "inactive_reactivation_count",
]

THRESHOLD_TRACE_COLUMNS = [
    "run_id",
    "mode",
    "allocation_time",
    "effective_after_time",
    "threshold_version",
    "trace_granularity",
    "house_id",
    "household_id",
    "plug_id",
    "plug_uid",
    "threshold",
    "sigma_floor_used",
    "sigma_p",
    "weight",
    "delta_used_for_censored_update",
    "n_budget_active_plugs",
    "Delta_H",
    "epsilon_ratio",
    "plug_status",
    "reason",
]

CSV_SCHEMAS = {
    "experiment_runs.csv": EXPERIMENT_RUNS_COLUMNS,
    "event_decisions.csv": EVENT_DECISIONS_COLUMNS,
    "house_timeseries.csv": HOUSE_TIMESERIES_COLUMNS,
    "plug_metrics.csv": PLUG_METRICS_COLUMNS,
    "house_summary.csv": HOUSE_SUMMARY_COLUMNS,
    "threshold_trace.csv": THRESHOLD_TRACE_COLUMNS,
}


def load_run_artifact(path: Path) -> dict[str, Any]:
    """Load a report artifact JSON file."""
    return json.loads(path.read_text(encoding="utf-8"))


def export_required_csvs_from_artifact(artifact: dict[str, Any], output_dir: Path) -> list[Path]:
    """Write all required CSV files and return their paths."""
    output_dir.mkdir(parents=True, exist_ok=True)
    experiment = dict(artifact.get("experiment", {}))
    event_rows = _normalise_event_rows(artifact.get("event_decisions", []), experiment)
    threshold_rows = _normalise_threshold_rows(artifact.get("threshold_trace", []), experiment)

    outputs = [
        _write_csv(output_dir / "experiment_runs.csv", EXPERIMENT_RUNS_COLUMNS, [_experiment_row(experiment)]),
        _write_csv(output_dir / "event_decisions.csv", EVENT_DECISIONS_COLUMNS, event_rows),
        _write_csv(output_dir / "house_timeseries.csv", HOUSE_TIMESERIES_COLUMNS, _house_timeseries_rows(event_rows)),
        _write_csv(output_dir / "plug_metrics.csv", PLUG_METRICS_COLUMNS, _plug_metric_rows(event_rows)),
        _write_csv(output_dir / "house_summary.csv", HOUSE_SUMMARY_COLUMNS, _house_summary_rows(event_rows, experiment)),
        _write_csv(output_dir / "threshold_trace.csv", THRESHOLD_TRACE_COLUMNS, threshold_rows),
    ]
    return outputs


def _experiment_row(experiment: dict[str, Any]) -> dict[str, Any]:
    return {column: experiment.get(column) for column in EXPERIMENT_RUNS_COLUMNS}


def _normalise_event_rows(
    rows: Iterable[dict[str, Any]],
    experiment: dict[str, Any],
) -> list[dict[str, Any]]:
    run_id = experiment.get("run_id")
    mode = experiment.get("mode")
    normalised: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item.setdefault("run_id", run_id)
        item.setdefault("mode", mode)
        if item.get("predicted_load") is None and item.get("predicted") is not None:
            item["predicted_load"] = item.get("predicted")
        if item.get("threshold") is None and item.get("delta") is not None:
            item["threshold"] = item.get("delta")
        if item.get("source_timestamp") is None:
            item["source_timestamp"] = item.get("timestamp")
        normalised.append({column: item.get(column) for column in EVENT_DECISIONS_COLUMNS})
    normalised.sort(
        key=lambda row: (
            _float_or_zero(row.get("source_timestamp")),
            _int_or_zero(row.get("house_id")),
            _int_or_zero(row.get("household_id")),
            _int_or_zero(row.get("plug_id")),
            _int_or_zero(row.get("plug_uid")),
        )
    )
    return normalised


def _normalise_threshold_rows(
    rows: Iterable[dict[str, Any]],
    experiment: dict[str, Any],
) -> list[dict[str, Any]]:
    run_id = experiment.get("run_id")
    mode = experiment.get("mode")
    normalised: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item.setdefault("run_id", run_id)
        item.setdefault("mode", mode)
        item.setdefault("Delta_H", experiment.get("Delta_H"))
        item.setdefault("epsilon_ratio", experiment.get("epsilon_ratio"))
        if item.get("threshold") is None and item.get("delta_p") is not None:
            item["threshold"] = item.get("delta_p")
        if item.get("trace_granularity") is None:
            item["trace_granularity"] = "plug" if item.get("plug_uid") is not None else "house"
        normalised.append({column: item.get(column) for column in THRESHOLD_TRACE_COLUMNS})
    normalised.sort(
        key=lambda row: (
            _float_or_zero(row.get("allocation_time")),
            _int_or_zero(row.get("house_id")),
            _int_or_zero(row.get("household_id")),
            _int_or_zero(row.get("plug_id")),
            _int_or_zero(row.get("threshold_version")),
        )
    )
    return normalised


def _house_timeseries_rows(event_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, Any, Any, Any], list[dict[str, Any]]] = defaultdict(list)
    for row in event_rows:
        key = (row["run_id"], row["mode"], row["house_id"], row["source_timestamp"])
        grouped[key].append(row)

    rows: list[dict[str, Any]] = []
    for (run_id, mode, house_id, source_timestamp), items in grouped.items():
        actual_load = sum(_float_or_zero(row.get("actual_load")) for row in items)
        predicted_values = [_to_float(row.get("predicted_load")) for row in items]
        predicted_load = (
            sum(value for value in predicted_values if value is not None)
            if any(value is not None for value in predicted_values)
            else None
        )
        reconstructed_load = sum(_float_or_zero(row.get("reconstructed_load")) for row in items)
        transmitted_count = sum(1 for row in items if _is_true(row.get("transmitted")))
        plug_count = len(items)
        house_error = actual_load - reconstructed_load
        rows.append({
            "run_id": run_id,
            "mode": mode,
            "timestamp": items[0].get("timestamp"),
            "source_timestamp": source_timestamp,
            "house_id": house_id,
            "actual_load": actual_load,
            "predicted_load": predicted_load,
            "reconstructed_load": reconstructed_load,
            "house_error": house_error,
            "abs_house_error": abs(house_error),
            "transmission_rate": transmitted_count / plug_count if plug_count else None,
            "plug_count": plug_count,
            "transmitted_count": transmitted_count,
            "suppressed_count": plug_count - transmitted_count,
            "forced_transmit_count": sum(1 for row in items if _is_true(row.get("is_forced_transmit"))),
            "missing_prediction_count": sum(1 for row in items if row.get("reason") == "missing_prediction"),
            "inactive_reactivation_count": sum(1 for row in items if row.get("reason") == "inactive_reactivation"),
        })

    rows.sort(key=lambda row: (_as_sortable(row["run_id"]), _as_sortable(row["mode"]), _int_or_zero(row["house_id"]), _float_or_zero(row["source_timestamp"])))
    _add_rolling_house_metrics(rows)
    return [{column: row.get(column) for column in HOUSE_TIMESERIES_COLUMNS} for row in rows]


def _add_rolling_house_metrics(rows: list[dict[str, Any]]) -> None:
    by_house: dict[tuple[Any, Any, Any], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_house[(row["run_id"], row["mode"], row["house_id"])].append(row)

    for house_rows in by_house.values():
        window: deque[dict[str, Any]] = deque()
        for row in house_rows:
            current_ts = _float_or_zero(row["source_timestamp"])
            window.append(row)
            while window and current_ts - _float_or_zero(window[0]["source_timestamp"]) > 3600:
                window.popleft()
            tr_values = [_float_or_zero(item.get("transmission_rate")) for item in window]
            errors = [_float_or_zero(item.get("house_error")) for item in window]
            row["rolling_tr_1h"] = _mean(tr_values)
            row["rolling_rmse_1h"] = _rmse(errors)
            row["rolling_insufficient_data"] = (
                not window or current_ts - _float_or_zero(window[0]["source_timestamp"]) < 3600
            )


def _plug_metric_rows(event_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, Any, Any, Any, Any, Any], list[dict[str, Any]]] = defaultdict(list)
    for row in event_rows:
        key = (
            row["run_id"],
            row["mode"],
            row["house_id"],
            row["household_id"],
            row["plug_id"],
            row["plug_uid"],
        )
        grouped[key].append(row)

    output: list[dict[str, Any]] = []
    for (run_id, mode, house_id, household_id, plug_id, plug_uid), items in grouped.items():
        event_count = len(items)
        transmitted_count = sum(1 for row in items if _is_true(row.get("transmitted")))
        reconstruction_errors = [
            _float_or_zero(row.get("actual_load")) - _float_or_zero(row.get("reconstructed_load"))
            for row in items
        ]
        prediction_errors = [
            _float_or_zero(row.get("actual_load")) - predicted
            for row in items
            if (predicted := _to_float(row.get("predicted_load"))) is not None
        ]
        output.append({
            "run_id": run_id,
            "mode": mode,
            "house_id": house_id,
            "household_id": household_id,
            "plug_id": plug_id,
            "plug_uid": plug_uid,
            "event_count": event_count,
            "transmitted_count": transmitted_count,
            "suppressed_count": event_count - transmitted_count,
            "transmission_rate": transmitted_count / event_count if event_count else None,
            "rmse_reconstruction": _rmse(reconstruction_errors),
            "mae_reconstruction": _mae(reconstruction_errors),
            "rmse_prediction": _rmse(prediction_errors) if prediction_errors else None,
            "mae_prediction": _mae(prediction_errors) if prediction_errors else None,
            "inactive_reactivation_count": sum(1 for row in items if row.get("reason") == "inactive_reactivation"),
            "missing_prediction_count": sum(1 for row in items if row.get("reason") == "missing_prediction"),
            "forced_transmit_count": sum(1 for row in items if _is_true(row.get("is_forced_transmit"))),
        })

    output.sort(key=lambda row: (_as_sortable(row["run_id"]), _as_sortable(row["mode"]), _int_or_zero(row["house_id"]), _int_or_zero(row["household_id"]), _int_or_zero(row["plug_id"])))
    return [{column: row.get(column) for column in PLUG_METRICS_COLUMNS} for row in output]


def _house_summary_rows(
    event_rows: list[dict[str, Any]],
    experiment: dict[str, Any],
) -> list[dict[str, Any]]:
    timeseries = _house_timeseries_rows(event_rows)
    events_by_house: dict[tuple[Any, Any, Any], list[dict[str, Any]]] = defaultdict(list)
    for row in event_rows:
        events_by_house[(row["run_id"], row["mode"], row["house_id"])].append(row)

    ts_by_house: dict[tuple[Any, Any, Any], list[dict[str, Any]]] = defaultdict(list)
    for row in timeseries:
        ts_by_house[(row["run_id"], row["mode"], row["house_id"])].append(row)

    output: list[dict[str, Any]] = []
    for key, house_rows in ts_by_house.items():
        run_id, mode, house_id = key
        event_items = events_by_house[key]
        errors = [_float_or_zero(row.get("house_error")) for row in house_rows]
        abs_errors = [abs(value) for value in errors]
        tr_values = [_float_or_zero(row.get("transmission_rate")) for row in house_rows]
        output.append({
            "run_id": run_id,
            "mode": mode,
            "house_id": house_id,
            "event_count": len(house_rows),
            "timestamp_start": min(row["source_timestamp"] for row in house_rows),
            "timestamp_end": max(row["source_timestamp"] for row in house_rows),
            "avg_transmission_rate": _mean(tr_values),
            "rmse_house_error": _rmse(errors),
            "mae_house_error": _mae(errors),
            "p95_abs_house_error": _percentile(abs_errors, 95.0),
            "max_abs_house_error": max(abs_errors) if abs_errors else None,
            "Delta_H": experiment.get("Delta_H"),
            "epsilon_ratio": experiment.get("epsilon_ratio"),
            "total_plug_events": len(event_items),
            "transmitted_count": sum(1 for row in event_items if _is_true(row.get("transmitted"))),
            "suppressed_count": sum(1 for row in event_items if not _is_true(row.get("transmitted"))),
            "missing_prediction_count": sum(1 for row in event_items if row.get("reason") == "missing_prediction"),
            "inactive_reactivation_count": sum(1 for row in event_items if row.get("reason") == "inactive_reactivation"),
        })

    output.sort(key=lambda row: (_as_sortable(row["run_id"]), _as_sortable(row["mode"]), _int_or_zero(row["house_id"])))
    return [{column: row.get(column) for column in HOUSE_SUMMARY_COLUMNS} for row in output]


def _write_csv(path: Path, columns: list[str], rows: Iterable[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _csv_value(row.get(column)) for column in columns})
    return path


def _csv_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list):
        return ";".join(str(item) for item in value)
    return value


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _float_or_zero(value: Any) -> float:
    parsed = _to_float(value)
    return 0.0 if parsed is None else parsed


def _int_or_zero(value: Any) -> int:
    if value in (None, ""):
        return 0
    return int(value)


def _is_true(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _rmse(errors: list[float]) -> float | None:
    if not errors:
        return None
    return math.sqrt(sum(error * error for error in errors) / len(errors))


def _mae(errors: list[float]) -> float | None:
    if not errors:
        return None
    return sum(abs(error) for error in errors) / len(errors)


def _percentile(values: list[float], percentile: float) -> float | None:
    cleaned = sorted(values)
    if not cleaned:
        return None
    if len(cleaned) == 1:
        return cleaned[0]
    rank = (percentile / 100.0) * (len(cleaned) - 1)
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return cleaned[lower]
    fraction = rank - lower
    return cleaned[lower] + ((cleaned[upper] - cleaned[lower]) * fraction)


def _as_sortable(value: Any) -> str:
    return "" if value is None else str(value)
