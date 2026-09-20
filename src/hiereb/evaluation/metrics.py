"""Core, tail, utilization, and diagnostic metrics."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import numpy as np

from hiereb.simulation.replay import ReplayResult
from hiereb.simulation.streaming_replay import StreamingReplayResult


def _quantile(values: np.ndarray, q: float) -> float:
    return float(np.quantile(values, q)) if values.size else 0.0


def _cvar(values: np.ndarray, q: float) -> float:
    if not values.size:
        return 0.0
    var = np.quantile(values, q)
    return float(np.mean(values[values >= var]))


def evaluate(result: ReplayResult, house_budget: float) -> dict[str, Any]:
    """Compute PRD metrics from ground-truth replay rows."""
    errors = np.asarray([row["house_error"] for row in result.batch_rows], dtype=float)
    absolute = np.abs(errors)
    total_events = len(result.event_rows)
    transmitted = sum(bool(row["transmitted"]) for row in result.event_rows)
    allocated = np.asarray([row["allocated_budget"] for row in result.batch_rows], dtype=float)
    max_streak = _maximum_suppression_streak(result)
    allocation_snapshots = _allocation_snapshots(result)
    cap_hit_count = sum(int(row["cap_hit_count"]) for row in allocation_snapshots)
    unused_values = [float(row["unused_budget"]) for row in allocation_snapshots]
    return {
        "mode": result.mode,
        "valid_events": total_events,
        "timestamp_batches": len(result.batch_rows),
        "transmitted": transmitted,
        "suppressed": total_events - transmitted,
        "transmission_ratio": transmitted / total_events if total_events else 0.0,
        "reduction": 1.0 - transmitted / total_events if total_events else 0.0,
        "mae": float(np.mean(absolute)) if absolute.size else 0.0,
        "rmse": float(np.sqrt(np.mean(np.square(errors)))) if errors.size else 0.0,
        "p95": _quantile(absolute, 0.95),
        "p99": _quantile(absolute, 0.99),
        "max": float(np.max(absolute)) if absolute.size else 0.0,
        "cvar95": _cvar(absolute, 0.95),
        "cvar99": _cvar(absolute, 0.99),
        "bound_violation_count": sum(bool(row["bound_violation"]) for row in result.batch_rows),
        "suppression_violation_count": sum(
            bool(row["suppressed"])
            and row["residual"] is not None
            and abs(float(row["residual"])) > float(row["threshold_used"]) + 1e-9
            for row in result.event_rows
        ),
        "budget_violation_count": sum(
            float(row["used_budget"]) > house_budget + 1e-9 for row in allocation_snapshots
        ),
        "max_bound_utilization": (
            float(np.max(absolute / house_budget)) if absolute.size and house_budget > 0 else 0.0
        ),
        "mean_budget_utilization": (
            float(np.mean(allocated / house_budget)) if allocated.size and house_budget > 0 else 0.0
        ),
        "unused_budget": max(unused_values, default=0.0),
        "unused_budget_ratio": (
            max(unused_values, default=0.0) / house_budget if house_budget > 0 else 0.0
        ),
        "number_of_cycles_with_unused_budget": sum(value > 1e-9 for value in unused_values),
        "cap_hit_count": cap_hit_count,
        "cap_hit_rate": (
            cap_hit_count / len(result.threshold_rows) if result.threshold_rows else 0.0
        ),
        "redistribution_rounds": sum(
            int(row["redistribution_rounds"]) for row in allocation_snapshots
        ),
        "within_household_redistributed_budget": sum(
            float(row["within_household_redistributed_budget"]) for row in allocation_snapshots
        ),
        "cross_household_spill_budget": sum(
            float(row["cross_household_spill_budget"]) for row in allocation_snapshots
        ),
        "forced_transmit": result.forced_transmit,
        "maximum_consecutive_suppression": max_streak,
        "house_budget": house_budget,
    }


def evaluate_streaming(result: StreamingReplayResult, house_budget: float) -> dict[str, Any]:
    """Compute the same PRD metrics from compact bounded-memory replay state."""
    errors = np.frombuffer(result.errors, dtype=np.float64)
    absolute = np.abs(errors)
    allocated = np.frombuffer(result.allocated_budgets, dtype=np.float64)
    snapshots = _streaming_allocation_snapshots(result)
    cap_hit_count = sum(int(row["cap_hit_count"]) for row in snapshots)
    unused_values = [float(row["unused_budget"]) for row in snapshots]
    total_events = result.valid_events
    transmitted = result.transmitted
    return {
        "mode": result.mode,
        "valid_events": total_events,
        "timestamp_batches": result.timestamp_batches,
        "transmitted": transmitted,
        "suppressed": total_events - transmitted,
        "transmission_ratio": transmitted / total_events if total_events else 0.0,
        "reduction": 1.0 - transmitted / total_events if total_events else 0.0,
        "mae": float(np.mean(absolute)) if absolute.size else 0.0,
        "rmse": float(np.sqrt(np.mean(np.square(errors)))) if errors.size else 0.0,
        "p95": _quantile(absolute, 0.95),
        "p99": _quantile(absolute, 0.99),
        "max": float(np.max(absolute)) if absolute.size else 0.0,
        "cvar95": _cvar(absolute, 0.95),
        "cvar99": _cvar(absolute, 0.99),
        "bound_violation_count": result.bound_violation_count,
        "suppression_violation_count": result.suppression_violation_count,
        "budget_violation_count": sum(
            float(row["used_budget"]) > house_budget + 1e-9 for row in snapshots
        ),
        "max_bound_utilization": (
            float(np.max(absolute / house_budget)) if absolute.size and house_budget > 0 else 0.0
        ),
        "mean_budget_utilization": (
            float(np.mean(allocated / house_budget)) if allocated.size and house_budget > 0 else 0.0
        ),
        "unused_budget": max(unused_values, default=0.0),
        "unused_budget_ratio": (
            max(unused_values, default=0.0) / house_budget if house_budget > 0 else 0.0
        ),
        "number_of_cycles_with_unused_budget": sum(value > 1e-9 for value in unused_values),
        "cap_hit_count": cap_hit_count,
        "cap_hit_rate": (
            cap_hit_count / len(result.threshold_rows) if result.threshold_rows else 0.0
        ),
        "redistribution_rounds": sum(int(row["redistribution_rounds"]) for row in snapshots),
        "within_household_redistributed_budget": sum(
            float(row["within_household_redistributed_budget"]) for row in snapshots
        ),
        "cross_household_spill_budget": sum(
            float(row["cross_household_spill_budget"]) for row in snapshots
        ),
        "forced_transmit": result.forced_transmit,
        "maximum_consecutive_suppression": result.maximum_consecutive_suppression,
        "house_budget": house_budget,
    }


def _allocation_snapshots(result: ReplayResult) -> list[dict[str, Any]]:
    snapshots: dict[object, dict[str, Any]] = {}
    for row in result.threshold_rows:
        snapshots.setdefault(row["allocation_timestamp"], row)
    return list(snapshots.values())


def _streaming_allocation_snapshots(
    result: StreamingReplayResult,
) -> list[dict[str, Any]]:
    snapshots: dict[object, dict[str, Any]] = {}
    for row in result.threshold_rows:
        snapshots.setdefault(row["allocation_timestamp"], row)
    return list(snapshots.values())


def _maximum_suppression_streak(result: ReplayResult) -> int:
    streak: defaultdict[tuple[int, int], int] = defaultdict(int)
    maximum = 0
    for row in result.event_rows:
        plug = (int(row["household_id"]), int(row["plug_id"]))
        if row["suppressed"]:
            streak[plug] += 1
            maximum = max(maximum, streak[plug])
        else:
            streak[plug] = 0
    return maximum
