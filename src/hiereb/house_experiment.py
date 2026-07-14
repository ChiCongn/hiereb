"""Reproducible one-house DEBS Direction 1 experiment workflow."""
# ruff: noqa: E501

from __future__ import annotations

import json
import resource
from dataclasses import replace
from pathlib import Path
from time import perf_counter
from typing import Any

import matplotlib
import polars as pl

from hiereb.config import AppConfig, IntervalConfig, Mode
from hiereb.evaluation.comparisons import exact_proxy_comparison, hierarchy_effect
from hiereb.experiment import PreparedExperiment, execute_mode, prepare
from hiereb.simulation.replay import ReplayResult

matplotlib.use("Agg")
from matplotlib import pyplot as plt

HOUSE_MODES: tuple[Mode, ...] = (
    "full_tx",
    "uniform",
    "flat_variance",
    "legacy_two_stage",
    "true_hierarchical",
    "true_hierarchical_cap",
)
PARETO_MODES: tuple[Mode, ...] = (
    "uniform",
    "flat_variance",
    "true_hierarchical",
    "true_hierarchical_cap",
)
EPSILON_VALUES = (0.0025, 0.005, 0.01, 0.015, 0.02, 0.03, 0.05, 0.075, 0.10)


def run_house_experiment(config: AppConfig) -> dict[str, Any]:
    """Run validation, six-mode evaluation, Pareto, and exact/proxy comparisons."""
    started = perf_counter()
    prepared = prepare(config)
    selected_config, validation_report = _validation_sensitivity(config, prepared)
    root = selected_config.experiment.output_dir
    root.mkdir(parents=True, exist_ok=True)
    _write_json(root / "validation_selection.json", validation_report)

    summaries: dict[Mode, dict[str, Any]] = {}
    exact_results: dict[Mode, ReplayResult] = {}
    flat_result: ReplayResult | None = None
    legacy_report: dict[str, Any] = {}
    hierarchy_summary: dict[str, float | int] = {}
    hierarchy_cycles: list[dict[str, Any]] = []
    for mode in HOUSE_MODES:
        result, summary = execute_mode(selected_config, mode, prepared)
        _assert_safe(summary)
        summaries[mode] = summary
        if mode == "flat_variance":
            flat_result = result
        elif mode == "legacy_two_stage":
            assert flat_result is not None
            legacy_report = _assert_legacy_equivalence(
                flat_result, result, selected_config.replay.float_tolerance
            )
        elif mode == "true_hierarchical":
            assert flat_result is not None
            hierarchy_summary, hierarchy_cycles = hierarchy_effect(
                flat_result,
                result,
                prepared.house_budget,
                selected_config.replay.float_tolerance,
            )
            flat_result = None
            exact_results[mode] = result
        elif mode == "true_hierarchical_cap":
            exact_results[mode] = result

    _write_comparison_tables(root, summaries)
    _write_json(root / "legacy_equivalence.json", legacy_report)
    _write_json(root / "hierarchy_effect_summary.json", hierarchy_summary)
    pl.DataFrame(hierarchy_cycles).write_parquet(root / "hierarchy_effect_cycles.parquet")
    cap_summary = {
        key: summaries["true_hierarchical_cap"][key]
        for key in (
            "cap_hit_count",
            "cap_hit_rate",
            "redistribution_rounds",
            "within_household_redistributed_budget",
            "cross_household_spill_budget",
            "unused_budget",
            "unused_budget_ratio",
            "number_of_cycles_with_unused_budget",
        )
    }
    _write_json(root / "cap_diagnostics.json", cap_summary)

    pareto_rows = _pareto_sweep(selected_config, prepared, root)
    proxy_report = _exact_proxy(selected_config, prepared, exact_results, summaries, root)
    runtime = {
        "elapsed_seconds": perf_counter() - started,
        "peak_rss_mib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0,
        "input_rows": prepared.loaded.filter_counts["input_rows"],
        "accepted_rows": prepared.loaded.filter_counts["accepted_rows"],
        "warmup_events": len(prepared.warmup),
        "validation_events": len(prepared.validation),
        "evaluation_events": len(prepared.evaluation),
    }
    _write_json(root / "runtime_memory.json", runtime)
    report = _write_report(
        root,
        selected_config,
        prepared,
        summaries,
        hierarchy_summary,
        cap_summary,
        proxy_report,
        pareto_rows,
        validation_report,
        runtime,
    )
    return {
        "output_dir": str(root),
        "report": str(report),
        "hierarchy_effect": hierarchy_summary,
        "runtime": runtime,
    }


def _validation_sensitivity(
    config: AppConfig, prepared: PreparedExperiment
) -> tuple[AppConfig, dict[str, Any]]:
    if not config.splits.validation.enabled or not prepared.validation:
        return config, {
            "status": "skipped",
            "reason": "validation split disabled or empty",
            "objective": "minimize CVaR99, then RMSE, then transmission ratio; safety is mandatory",
            "selected_config": config.model_dump(mode="json"),
            "candidates": [],
        }
    assert config.splits.validation.start is not None
    assert config.splits.validation.end is not None
    validation_interval = IntervalConfig(
        start=config.splits.validation.start, end=config.splits.validation.end
    )
    validation_splits = config.splits.model_copy(
        update={
            "validation": config.splits.validation.model_copy(
                update={"enabled": False, "start": None, "end": None}
            ),
            "evaluation": validation_interval,
        }
    )
    validation_prepared = replace(prepared, evaluation=prepared.validation)
    rows: list[dict[str, Any]] = []
    candidates: list[tuple[str, AppConfig]] = [("base", config), *_ofat_variants(config)]
    for name, candidate in candidates:
        run_config = candidate.model_copy(update={"splits": validation_splits})
        _, summary = execute_mode(
            run_config, "true_hierarchical_cap", validation_prepared, write=False
        )
        _assert_safe(summary)
        rows.append(
            {
                "candidate": name,
                "cvar99": summary["cvar99"],
                "rmse": summary["rmse"],
                "transmission_ratio": summary["transmission_ratio"],
                "config": candidate,
            }
        )
    selected = min(
        rows,
        key=lambda row: (
            float(row["cvar99"]),
            float(row["rmse"]),
            float(row["transmission_ratio"]),
            str(row["candidate"]),
        ),
    )
    selected_config = selected["config"]
    assert isinstance(selected_config, AppConfig)
    serializable = [{key: value for key, value in row.items() if key != "config"} for row in rows]
    return selected_config, {
        "status": "selected",
        "objective": "minimize CVaR99, then RMSE, then transmission ratio; safety is mandatory",
        "selected_candidate": selected["candidate"],
        "selected_config": selected_config.model_dump(mode="json"),
        "candidates": serializable,
    }


def _ofat_variants(config: AppConfig) -> list[tuple[str, AppConfig]]:
    variants: list[tuple[str, AppConfig]] = []
    hierarchy = config.true_hierarchy
    for value in (0.10, 0.20, 0.40):
        if value != hierarchy.household_fairness_alpha:
            variants.append(
                (
                    f"alpha={value}",
                    config.model_copy(
                        update={
                            "true_hierarchy": hierarchy.model_copy(
                                update={"household_fairness_alpha": value}
                            )
                        }
                    ),
                )
            )
    for value in (0.00, 0.10, 0.20):
        if value != hierarchy.plug_fairness_beta:
            variants.append(
                (
                    f"beta={value}",
                    config.model_copy(
                        update={
                            "true_hierarchy": hierarchy.model_copy(
                                update={"plug_fairness_beta": value}
                            )
                        }
                    ),
                )
            )
    cap_fields = {
        "quantile": (0.90, 0.95, 0.99),
        "multiplier": (0.5, 1.0, 1.5),
        "max_house_budget_fraction": (0.10, 0.25, 0.50),
    }
    for field, values in cap_fields.items():
        current = float(getattr(config.cap, field))
        for value in values:
            if value != current:
                variants.append(
                    (
                        f"cap.{field}={value}",
                        config.model_copy(
                            update={"cap": config.cap.model_copy(update={field: value})}
                        ),
                    )
                )
    return variants


def _pareto_sweep(
    config: AppConfig, prepared: PreparedExperiment, root: Path
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for epsilon in EPSILON_VALUES:
        epsilon_config = config.model_copy(
            update={"budget": config.budget.model_copy(update={"epsilon_ratio": epsilon})}
        )
        epsilon_prepared = replace(prepared, house_budget=prepared.mean_warmup_house_load * epsilon)
        for mode in PARETO_MODES:
            _, summary = execute_mode(epsilon_config, mode, epsilon_prepared, write=False)
            _assert_safe(summary)
            rows.append(
                {
                    "mode": mode,
                    "epsilon_ratio": epsilon,
                    "transmission_ratio": summary["transmission_ratio"],
                    "rmse": summary["rmse"],
                    "cvar99": summary["cvar99"],
                    "mae": summary["mae"],
                    "p99": summary["p99"],
                    "max": summary["max"],
                }
            )
    pl.DataFrame(rows).write_csv(root / "pareto_points.csv")
    _write_pareto_plot(root / "pareto_tr_rmse.png", rows, "rmse", "RMSE")
    _write_pareto_plot(root / "pareto_tr_cvar99.png", rows, "cvar99", "CVaR99")
    matched = _matched_transmission_rows(rows)
    pl.DataFrame(matched).write_csv(root / "matched_tr_comparison.csv")
    return rows


def _matched_transmission_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    uniform = [row for row in rows if row["mode"] == "uniform"]
    matched: list[dict[str, Any]] = []
    for row in rows:
        if row["mode"] == "uniform":
            continue
        baseline = min(
            uniform,
            key=lambda item: abs(
                float(item["transmission_ratio"]) - float(row["transmission_ratio"])
            ),
        )
        matched.append(
            {
                "mode": row["mode"],
                "mode_epsilon": row["epsilon_ratio"],
                "mode_tr": row["transmission_ratio"],
                "mode_rmse": row["rmse"],
                "mode_cvar99": row["cvar99"],
                "matched_uniform_epsilon": baseline["epsilon_ratio"],
                "matched_uniform_tr": baseline["transmission_ratio"],
                "matched_uniform_rmse": baseline["rmse"],
                "matched_uniform_cvar99": baseline["cvar99"],
                "absolute_tr_gap": abs(
                    float(baseline["transmission_ratio"]) - float(row["transmission_ratio"])
                ),
            }
        )
    return matched


def _exact_proxy(
    config: AppConfig,
    prepared: PreparedExperiment,
    exact_results: dict[Mode, ReplayResult],
    exact_summaries: dict[Mode, dict[str, Any]],
    root: Path,
) -> dict[str, Any]:
    proxy_config = config.model_copy(
        update={"residual": config.residual.model_copy(update={"estimator": "uniform_proxy"})}
    )
    report: dict[str, Any] = {}
    score_rows: list[dict[str, Any]] = []
    for mode in ("true_hierarchical", "true_hierarchical_cap"):
        proxy_result, proxy_summary = execute_mode(proxy_config, mode, prepared, write=False)
        _assert_safe(proxy_summary)
        comparison, rows = exact_proxy_comparison(
            exact_results[mode],
            proxy_result,
            exact_summaries[mode],
            proxy_summary,
            prepared.house_budget,
        )
        report[mode] = comparison
        score_rows.extend({"mode": mode, **row} for row in rows)
    _write_json(root / "exact_proxy_comparison.json", report)
    pl.DataFrame(score_rows).write_parquet(root / "exact_proxy_plug_scores.parquet")
    return report


def _write_comparison_tables(root: Path, summaries: dict[Mode, dict[str, Any]]) -> None:
    rows = [
        {key: value for key, value in summary.items() if not isinstance(value, dict)}
        for summary in summaries.values()
    ]
    pl.DataFrame(rows).write_csv(root / "comparison.csv")
    tail_fields = (
        "mode",
        "p95",
        "p99",
        "max",
        "cvar95",
        "cvar99",
        "rmse",
        "mae",
        "transmission_ratio",
        "max_bound_utilization",
        "mean_budget_utilization",
    )
    pl.DataFrame(
        [{key: summary[key] for key in tail_fields} for summary in summaries.values()]
    ).write_csv(root / "comparison_tail_metrics.csv")


def _assert_safe(summary: dict[str, Any]) -> None:
    violations = {
        name: int(summary[name])
        for name in (
            "suppression_violation_count",
            "budget_violation_count",
            "bound_violation_count",
        )
        if int(summary[name]) != 0
    }
    if violations:
        raise RuntimeError(f"safety invariant failure: {violations}")


def _assert_legacy_equivalence(
    flat: ReplayResult, legacy: ReplayResult, tolerance: float
) -> dict[str, Any]:
    if len(flat.threshold_rows) != len(legacy.threshold_rows):
        raise RuntimeError("legacy equivalence failed: threshold row counts differ")
    maximum = max(
        (
            abs(float(left["threshold"]) - float(right["threshold"]))
            for left, right in zip(flat.threshold_rows, legacy.threshold_rows, strict=True)
        ),
        default=0.0,
    )
    decisions_equal = [row["transmitted"] for row in flat.event_rows] == [
        row["transmitted"] for row in legacy.event_rows
    ]
    if maximum > tolerance or not decisions_equal:
        raise RuntimeError(
            f"legacy equivalence failed: max threshold difference={maximum}, "
            f"decisions_equal={decisions_equal}"
        )
    return {
        "equivalent": True,
        "max_threshold_difference": maximum,
        "decisions_equal": decisions_equal,
        "threshold_rows": len(flat.threshold_rows),
        "event_rows": len(flat.event_rows),
    }


def _write_pareto_plot(path: Path, rows: list[dict[str, Any]], metric: str, label: str) -> None:
    figure, axis = plt.subplots(figsize=(6, 4))
    for mode in PARETO_MODES:
        mode_rows = sorted(
            (row for row in rows if row["mode"] == mode),
            key=lambda row: float(row["transmission_ratio"]),
        )
        axis.plot(
            [row["transmission_ratio"] for row in mode_rows],
            [row[metric] for row in mode_rows],
            marker="o",
            label=mode,
        )
    axis.set_xlabel("Transmission rate")
    axis.set_ylabel(label)
    axis.legend(fontsize=7)
    figure.tight_layout()
    figure.savefig(path, dpi=140)
    plt.close(figure)


def _write_report(
    root: Path,
    config: AppConfig,
    prepared: PreparedExperiment,
    summaries: dict[Mode, dict[str, Any]],
    hierarchy: dict[str, float | int],
    cap: dict[str, Any],
    proxy: dict[str, Any],
    pareto: list[dict[str, Any]],
    validation: dict[str, Any],
    runtime: dict[str, Any],
) -> Path:
    evaluation_start = min(event.timestamp for event in prepared.evaluation)
    evaluation_end = max(event.timestamp for event in prepared.evaluation)
    cap_improved = float(summaries["true_hierarchical_cap"]["cvar99"]) < float(
        summaries["true_hierarchical"]["cvar99"]
    )
    hierarchy_improved = float(summaries["true_hierarchical"]["cvar99"]) < float(
        summaries["flat_variance"]["cvar99"]
    )
    cap_active = float(cap["cap_hit_rate"]) > 0.0
    effect_rate = float(hierarchy["hierarchy_effect_rate"])
    if effect_rate >= 0.5 and hierarchy_improved and cap_active and cap_improved:
        decision = "Mở rộng xác minh sang nhiều house trước khi đổi allocator."
    elif effect_rate >= 0.5 and not hierarchy_improved:
        decision = "Chuyển sang đánh giá empirical-CDF allocator cho kiểm soát tail error."
    else:
        decision = "Cải tiến predictor và phân tích outlier trước khi mở rộng allocator."
    text = f"""# House 0 Direction 1 Report

## 1. Dataset và split

- Input: `{config.data.path}`; house `{config.data.house_id}`; property `{config.data.load_property_value}`.
- Warm-up configured: `{config.splits.warmup.start.isoformat()}` đến `{config.splits.warmup.end.isoformat()}`.
- Validation configured: `{config.splits.validation.start}` đến `{config.splits.validation.end}`.
- Evaluation configured: `{config.splits.evaluation.start.isoformat()}` đến `{config.splits.evaluation.end.isoformat()}`.
- Evaluation coverage thực tế: `{evaluation_start.isoformat()}` đến `{evaluation_end.isoformat()}`.
- Event-aligned house error không phải synchronized full-house load.

## 2. Legacy equivalence

`flat_variance` và `legacy_two_stage` đã qua gate threshold/decision equivalence; runner sẽ fail nếu khác tolerance.

## 3. Hierarchy effect

```json
{json.dumps(hierarchy, indent=2, sort_keys=True)}
```

## 4. Cap effect

```json
{json.dumps(cap, indent=2, sort_keys=True)}
```

## 5. Exact versus proxy

```json
{json.dumps(proxy, indent=2, sort_keys=True)}
```

## 6. Pareto results

Có `{len(pareto)}` điểm. So sánh matched-TR nằm trong `matched_tr_comparison.csv`; không kết luận outperform chỉ từ cùng epsilon.

## 7. Top outlier root causes

Mỗi mode có `top_outliers.parquet` chứa batch error, plug/household, actual, prediction, residual, threshold, decision, score, cap, suppression streak, last-seen và forced reason.

## 8. Invariants

Tất cả run được chấp nhận chỉ khi suppression, budget và bound violation đều bằng 0. `full_tx` phải có reconstruction error bằng 0.

## 9. Runtime và memory

```json
{json.dumps(runtime, indent=2, sort_keys=True)}
```

Validation selection dùng riêng validation window với objective: `{validation["objective"]}`. Evaluation không tham gia tuning.

## 10. Decision gate

{decision}

Không suy diễn production scalability từ experiment offline một house này.
"""
    path = root / "HOUSE0_DIRECTION1_REPORT.md"
    path.write_text(text, encoding="utf-8")
    Path("HOUSE0_DIRECTION1_REPORT.md").write_text(text, encoding="utf-8")
    return path


def _write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
