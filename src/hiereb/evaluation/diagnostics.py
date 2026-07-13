"""Tabular daily, household, plug, and outlier diagnostics."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import numpy as np

from hiereb.simulation.replay import ReplayResult


def daily_metrics(result: ReplayResult) -> list[dict[str, Any]]:
    groups: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in result.batch_rows:
        groups[row["timestamp"].date().isoformat()].append(row)
    output = []
    for day in sorted(groups):
        rows = groups[day]
        errors = np.asarray([row["house_error"] for row in rows], dtype=float)
        output.append(
            {
                "date": day,
                "mode": result.mode,
                "batches": len(rows),
                "mae": float(np.mean(np.abs(errors))),
                "rmse": float(np.sqrt(np.mean(np.square(errors)))),
                "max": float(np.max(np.abs(errors))),
                "bound_violation_count": sum(bool(row["bound_violation"]) for row in rows),
            }
        )
    return output


def household_metrics(result: ReplayResult) -> list[dict[str, Any]]:
    groups: defaultdict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in result.event_rows:
        groups[int(row["household_id"])].append(row)
    output = []
    for household in sorted(groups):
        rows = groups[household]
        errors = np.asarray([row["reconstruction_error"] for row in rows], dtype=float)
        transmitted = sum(bool(row["transmitted"]) for row in rows)
        budgets = [
            float(row["household_budget"])
            for row in result.threshold_rows
            if int(row["household_id"]) == household
        ]
        output.append(
            {
                "household_id": household,
                "mode": result.mode,
                "events": len(rows),
                "transmission_ratio": transmitted / len(rows),
                "mae": float(np.mean(np.abs(errors))),
                "rmse": float(np.sqrt(np.mean(np.square(errors)))),
                "mean_budget": float(np.mean(budgets)) if budgets else 0.0,
            }
        )
    return output


def plug_metrics(result: ReplayResult) -> list[dict[str, Any]]:
    groups: defaultdict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in result.event_rows:
        groups[(int(row["household_id"]), int(row["plug_id"]))].append(row)
    output = []
    for plug in sorted(groups):
        rows = groups[plug]
        errors = np.asarray([row["reconstruction_error"] for row in rows], dtype=float)
        thresholds = [float(row["threshold_used"]) for row in rows]
        transmitted = sum(bool(row["transmitted"]) for row in rows)
        output.append(
            {
                "household_id": plug[0],
                "plug_id": plug[1],
                "mode": result.mode,
                "events": len(rows),
                "transmission_ratio": transmitted / len(rows),
                "mae": float(np.mean(np.abs(errors))),
                "rmse": float(np.sqrt(np.mean(np.square(errors)))),
                "threshold_mean": float(np.mean(thresholds)),
                "threshold_max": max(thresholds),
            }
        )
    return output


def top_outliers(result: ReplayResult, top_k: int) -> list[dict[str, Any]]:
    rows = sorted(
        result.batch_rows,
        key=lambda row: (-float(row["abs_house_error"]), row["timestamp"]),
    )[:top_k]
    return [dict(row) for row in rows]
