"""Cross-run research diagnostics for HierEB Direction 1."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any

import numpy as np
from scipy.stats import spearmanr

from hiereb.simulation.replay import ReplayResult


def hierarchy_effect(
    flat: ReplayResult,
    hierarchical: ReplayResult,
    house_budget: float,
    tolerance: float,
) -> tuple[dict[str, float | int], list[dict[str, Any]]]:
    """Compare threshold versions only when at least two households are active."""
    flat_cycles = _trace_cycles(flat)
    hierarchy_cycles = _trace_cycles(hierarchical)
    cycle_rows: list[dict[str, Any]] = []
    for timestamp in sorted(set(flat_cycles) & set(hierarchy_cycles)):
        flat_rows = flat_cycles[timestamp]
        hierarchy_rows = hierarchy_cycles[timestamp]
        plugs = sorted(set(flat_rows) | set(hierarchy_rows))
        households = {plug[0] for plug in plugs}
        if len(households) < 2:
            continue
        threshold_l1 = sum(
            abs(
                float(hierarchy_rows.get(plug, {}).get("threshold", 0.0))
                - float(flat_rows.get(plug, {}).get("threshold", 0.0))
            )
            for plug in plugs
        )
        flat_budgets = _actual_household_budgets(flat_rows)
        hierarchy_budgets = _actual_household_budgets(hierarchy_rows)
        household_l1 = sum(
            abs(hierarchy_budgets.get(household, 0.0) - flat_budgets.get(household, 0.0))
            for household in households
        )
        normalized = threshold_l1 / house_budget if house_budget > 0 else 0.0
        cycle_rows.append(
            {
                "allocation_timestamp": timestamp,
                "active_households": len(households),
                "active_plugs": len(plugs),
                "threshold_l1_normalized": normalized,
                "household_budget_l1_normalized": (
                    household_l1 / house_budget if house_budget > 0 else 0.0
                ),
                "hierarchy_changed": normalized > tolerance,
            }
        )
    changed = sum(bool(row["hierarchy_changed"]) for row in cycle_rows)
    eligible = len(cycle_rows)
    summary: dict[str, float | int] = {
        "eligible_allocation_cycles": eligible,
        "hierarchy_changed_cycles": changed,
        "hierarchy_effect_rate": changed / eligible if eligible else 0.0,
        "mean_threshold_l1_distance_vs_flat": _mean(
            [float(row["threshold_l1_normalized"]) for row in cycle_rows]
        ),
        "max_threshold_l1_distance_vs_flat": max(
            (float(row["threshold_l1_normalized"]) for row in cycle_rows), default=0.0
        ),
        "mean_household_budget_l1_distance_vs_flat": _mean(
            [float(row["household_budget_l1_normalized"]) for row in cycle_rows]
        ),
    }
    return summary, cycle_rows


def exact_proxy_comparison(
    exact: ReplayResult,
    proxy: ReplayResult,
    exact_summary: dict[str, Any],
    proxy_summary: dict[str, Any],
    house_budget: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Compare aligned score, threshold, decision, and evaluation metrics."""
    exact_trace = _trace_lookup(exact)
    proxy_trace = _trace_lookup(proxy)
    aligned = sorted(set(exact_trace) & set(proxy_trace))
    rows: list[dict[str, Any]] = []
    for key in aligned:
        exact_score = float(exact_trace[key]["score"])
        proxy_score = float(proxy_trace[key]["score"])
        rows.append(
            {
                "allocation_timestamp": key[0],
                "household_id": key[1][0],
                "plug_id": key[1][1],
                "exact_score": exact_score,
                "proxy_score": proxy_score,
                "score_relative_error": abs(proxy_score - exact_score)
                / max(abs(exact_score), 1e-12),
                "exact_threshold": float(exact_trace[key]["threshold"]),
                "proxy_threshold": float(proxy_trace[key]["threshold"]),
            }
        )
    exact_scores = [float(row["exact_score"]) for row in rows]
    proxy_scores = [float(row["proxy_score"]) for row in rows]
    correlation = _spearman(exact_scores, proxy_scores)
    threshold_l1_by_cycle: defaultdict[object, float] = defaultdict(float)
    for row in rows:
        threshold_l1_by_cycle[row["allocation_timestamp"]] += abs(
            float(row["proxy_threshold"]) - float(row["exact_threshold"])
        )
    exact_decisions = {
        (row["event_id"], row["original_row_index"]): bool(row["transmitted"])
        for row in exact.event_rows
    }
    proxy_decisions = {
        (row["event_id"], row["original_row_index"]): bool(row["transmitted"])
        for row in proxy.event_rows
    }
    common_events = sorted(set(exact_decisions) & set(proxy_decisions))
    disagreement = sum(exact_decisions[key] != proxy_decisions[key] for key in common_events)
    metric_names = [
        "transmission_ratio",
        "mae",
        "rmse",
        "p95",
        "p99",
        "max",
        "cvar95",
        "cvar99",
    ]
    comparison = {
        "aligned_score_rows": len(rows),
        "mean_score_relative_error": _mean([float(row["score_relative_error"]) for row in rows]),
        "spearman_score_rank_correlation": correlation,
        "mean_threshold_l1_distance": _mean(
            [
                distance / house_budget if house_budget > 0 else 0.0
                for distance in threshold_l1_by_cycle.values()
            ]
        ),
        "decision_disagreement_rate": disagreement / len(common_events) if common_events else 0.0,
        "metric_deltas_proxy_minus_exact": {
            name: float(proxy_summary[name]) - float(exact_summary[name]) for name in metric_names
        },
    }
    return comparison, rows


def _trace_cycles(
    result: ReplayResult,
) -> dict[datetime, dict[tuple[int, int], dict[str, Any]]]:
    cycles: defaultdict[datetime, dict[tuple[int, int], dict[str, Any]]] = defaultdict(dict)
    for row in result.threshold_rows:
        plug = (int(row["household_id"]), int(row["plug_id"]))
        cycles[row["allocation_timestamp"]][plug] = row
    return dict(cycles)


def _trace_lookup(
    result: ReplayResult,
) -> dict[tuple[datetime, tuple[int, int]], dict[str, Any]]:
    return {
        (
            row["allocation_timestamp"],
            (int(row["household_id"]), int(row["plug_id"])),
        ): row
        for row in result.threshold_rows
    }


def _actual_household_budgets(
    rows: dict[tuple[int, int], dict[str, Any]],
) -> dict[int, float]:
    budgets: dict[int, float] = defaultdict(float)
    for (household, _plug), row in rows.items():
        budgets[household] += float(row["threshold"])
    return dict(budgets)


def _mean(values: list[float]) -> float:
    return float(np.mean(values)) if values else 0.0


def _spearman(left: list[float], right: list[float]) -> float:
    if not left:
        return 0.0
    if left == right and len(set(left)) == 1:
        return 1.0
    value = float(spearmanr(left, right).statistic)
    return value if np.isfinite(value) else 0.0
