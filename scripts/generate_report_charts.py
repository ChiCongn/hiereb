#!/usr/bin/env python3
"""Generate report-ready SVG charts from exported CSV artifacts.

Usage:
  python3 scripts/generate_report_charts.py --input-dir results/demo_*/export --output-dir results/demo_*/charts
"""

from __future__ import annotations

import argparse
import csv
import math
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple


COLORS = [
    "#1b9e77",
    "#d95f02",
    "#7570b3",
    "#e7298a",
    "#66a61e",
    "#e6ab02",
]
DETAIL_EPSILON_RATIO = 0.05


def to_safe_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value)


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def parse_float(value: str, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_optional_float(value: str | None) -> float | None:
    try:
        if value is None or value == "":
            return None
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed) or math.isinf(parsed):
        return None
    return parsed


def short_label(run_id: str) -> str:
    match = re.match(r"e2e_([^_]+)_(\d{14})$", run_id)
    if match:
        return f"{match.group(1)}"
    return run_id


def epsilon_label(epsilon_ratio: float | None) -> str:
    if epsilon_ratio is None:
        return "null"
    return f"{epsilon_ratio:.2f}".rstrip("0").rstrip(".")


def is_detail_epsilon(epsilon_ratio: float | None) -> bool:
    return epsilon_ratio is not None and math.isclose(
        epsilon_ratio,
        DETAIL_EPSILON_RATIO,
        rel_tol=0.0,
        abs_tol=1e-9,
    )


def parse_run_id_parts(run_id: str) -> dict[str, str | float | None]:
    match = re.match(
        r"^(?P<run_group>.+)_(?P<mode>full_tx|uniform|hiereb)_house(?P<house_id>\d+)(?:_eps(?P<eps>\d+))?$",
        run_id,
    )
    if not match:
        return {"run_group": None, "mode": None, "house_id": None, "epsilon_ratio": None}
    eps_token = match.group("eps")
    epsilon_ratio = None if eps_token is None else int(eps_token) / 100.0
    return {
        "run_group": match.group("run_group"),
        "mode": match.group("mode"),
        "house_id": match.group("house_id"),
        "epsilon_ratio": epsilon_ratio,
    }


def chart_path(run_group: str, house_id: str, chart_name: str, *, mode: str | None = None) -> Path:
    safe_group = to_safe_id(run_group or "report")
    safe_house = to_safe_id(str(house_id))
    safe_chart = to_safe_id(chart_name)
    if mode:
        return Path(f"{safe_group}_house{safe_house}_{to_safe_id(mode)}_{safe_chart}.svg")
    return Path(f"{safe_group}_house{safe_house}_{safe_chart}.svg")


def svg_escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def _nice_y_bounds(values: Iterable[float]) -> Tuple[float, float]:
    cleaned = [v for v in values if not math.isnan(v) and not math.isinf(v)]
    if not cleaned:
        return 0.0, 1.0
    v_min = min(cleaned)
    v_max = max(cleaned)
    if v_min == v_max:
        if v_min == 0:
            return 0.0, 1.0
        return 0.0, v_max * 1.2
    if v_min > 0:
        v_min = 0.0
    padding = (v_max - v_min) * 0.08
    return v_min - padding, v_max + padding


def draw_bar_chart(
    labels: Sequence[str],
    values: Sequence[float],
    title: str,
    y_label: str,
    out_path: Path,
    *,
    value_fmt: str = "{:.3f}",
) -> None:
    width, height = 1280, 720
    left, right, top, bottom = 110, 50, 80, 170
    plot_w = width - left - right
    plot_h = height - top - bottom
    y_min, y_max = _nice_y_bounds(values)
    if y_max <= y_min:
        y_max = y_min + 1.0

    def y_px(v: float) -> float:
        return top + (y_max - v) / (y_max - y_min) * plot_h

    bars = []
    n = max(1, len(values))
    slot_w = plot_w / n
    bar_w = slot_w * 0.58
    for idx, (label, value) in enumerate(zip(labels, values)):
        x = left + idx * slot_w + (slot_w - bar_w) / 2
        y = y_px(value)
        h = top + plot_h - y
        bars.append(
            f'<rect x="{x:.2f}" y="{y:.2f}" width="{bar_w:.2f}" height="{h:.2f}" fill="{COLORS[idx % len(COLORS)]}" rx="6" />'
        )
        bars.append(
            f'<text x="{x + bar_w / 2:.2f}" y="{y - 10:.2f}" text-anchor="middle" class="value">{svg_escape(value_fmt.format(value))}</text>'
        )
        bars.append(
            f'<text x="{x + bar_w / 2:.2f}" y="{height - 95:.2f}" text-anchor="middle" class="tick">{svg_escape(label)}</text>'
        )

    grid = []
    for i in range(6):
        ratio = i / 5
        y = top + plot_h * ratio
        v = y_max - (y_max - y_min) * ratio
        grid.append(
            f'<line x1="{left}" y1="{y:.2f}" x2="{left + plot_w}" y2="{y:.2f}" class="grid" />'
        )
        grid.append(
            f'<text x="{left - 10}" y="{y + 5:.2f}" text-anchor="end" class="tick">{svg_escape(f"{v:.2f}")}</text>'
        )

    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">
  <style>
    .title {{ font: 700 30px Arial, sans-serif; fill: #111827; }}
    .axis {{ stroke: #374151; stroke-width: 2; }}
    .grid {{ stroke: #d1d5db; stroke-width: 1; }}
    .tick {{ font: 18px Arial, sans-serif; fill: #374151; }}
    .value {{ font: 16px Arial, sans-serif; fill: #111827; }}
    .ylabel {{ font: 18px Arial, sans-serif; fill: #111827; }}
  </style>
  <rect width="100%" height="100%" fill="#ffffff" />
  <text x="{left}" y="42" class="title">{svg_escape(title)}</text>
  <line x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" y2="{top + plot_h}" class="axis" />
  <line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_h}" class="axis" />
  {"".join(grid)}
  {"".join(bars)}
  <text x="{left - 80}" y="{top - 20}" class="ylabel">{svg_escape(y_label)}</text>
</svg>
"""
    out_path.write_text(svg, encoding="utf-8")


def draw_scatter_chart(
    points: Sequence[Dict[str, object]],
    title: str,
    x_label: str,
    y_label: str,
    out_path: Path,
) -> None:
    width, height = 1280, 720
    left, right, top, bottom = 120, 260, 80, 120
    plot_w = width - left - right
    plot_h = height - top - bottom
    cleaned = [
        point
        for point in points
        if parse_optional_float(str(point.get("x", ""))) is not None
        and parse_optional_float(str(point.get("y", ""))) is not None
    ]
    if not cleaned:
        cleaned = [{"x": 0.0, "y": 0.0, "label": "no data", "mode": "none", "epsilon_ratio": None}]

    xs = [float(point["x"]) for point in cleaned]
    ys = [float(point["y"]) for point in cleaned]
    x_min, x_max = _nice_y_bounds(xs)
    y_min, y_max = _nice_y_bounds(ys)
    if x_max <= x_min:
        x_max = x_min + 1.0
    if y_max <= y_min:
        y_max = y_min + 1.0

    def x_px(v: float) -> float:
        return left + (v - x_min) / (x_max - x_min) * plot_w

    def y_px(v: float) -> float:
        return top + (y_max - v) / (y_max - y_min) * plot_h

    grid = []
    for i in range(6):
        ratio = i / 5
        x = left + plot_w * ratio
        y = top + plot_h * ratio
        x_value = x_min + (x_max - x_min) * ratio
        y_value = y_max - (y_max - y_min) * ratio
        grid.append(f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{top + plot_h}" class="grid" />')
        grid.append(f'<line x1="{left}" y1="{y:.2f}" x2="{left + plot_w}" y2="{y:.2f}" class="grid" />')
        grid.append(f'<text x="{x:.2f}" y="{top + plot_h + 32:.2f}" text-anchor="middle" class="tick">{x_value:.3f}</text>')
        grid.append(f'<text x="{left - 12}" y="{y + 5:.2f}" text-anchor="end" class="tick">{y_value:.2f}</text>')

    mode_order = {"full_tx": 0, "uniform": 1, "hiereb": 2}
    dots = []
    legends = []
    seen_modes: list[str] = []
    for idx, point in enumerate(sorted(cleaned, key=lambda item: (str(item.get("mode", "")), str(item.get("label", ""))))):
        mode = str(point.get("mode", "unknown"))
        color = COLORS[mode_order.get(mode, idx) % len(COLORS)]
        x = x_px(float(point["x"]))
        y = y_px(float(point["y"]))
        epsilon_ratio = parse_optional_float(str(point.get("epsilon_ratio", "")))
        eps_text = f"eps={epsilon_label(epsilon_ratio)}"
        label = str(point.get("label") or f"{mode} {eps_text}")
        dots.append(f'<circle cx="{x:.2f}" cy="{y:.2f}" r="8" fill="{color}" stroke="#111827" stroke-width="1.5"><title>{svg_escape(label)}</title></circle>')
        dots.append(f'<text x="{x + 12:.2f}" y="{y - 10:.2f}" class="point-label">{svg_escape(eps_text)}</text>')
        dots.append(f'<text x="{x + 12:.2f}" y="{y + 10:.2f}" class="point-mode">{svg_escape(mode)}</text>')
        if mode not in seen_modes:
            seen_modes.append(mode)
            ly = top + 20 + (len(seen_modes) - 1) * 30
            legends.append(f'<circle cx="{left + plot_w + 28}" cy="{ly - 6}" r="7" fill="{color}" stroke="#111827" stroke-width="1.2" />')
            legends.append(f'<text x="{left + plot_w + 48}" y="{ly}" class="tick">{svg_escape(mode)}</text>')

    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">
  <style>
    .title {{ font: 700 30px Arial, sans-serif; fill: #111827; }}
    .axis {{ stroke: #374151; stroke-width: 2; }}
    .grid {{ stroke: #e5e7eb; stroke-width: 1; }}
    .tick {{ font: 15px Arial, sans-serif; fill: #374151; }}
    .xlabel {{ font: 18px Arial, sans-serif; fill: #111827; }}
    .ylabel {{ font: 18px Arial, sans-serif; fill: #111827; }}
    .point-label {{ font: 15px Arial, sans-serif; fill: #111827; }}
    .point-mode {{ font: 13px Arial, sans-serif; fill: #4b5563; }}
  </style>
  <rect width="100%" height="100%" fill="#ffffff" />
  <text x="{left}" y="42" class="title">{svg_escape(title)}</text>
  <line x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" y2="{top + plot_h}" class="axis" />
  <line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_h}" class="axis" />
  {"".join(grid)}
  {"".join(dots)}
  {"".join(legends)}
  <text x="{left + plot_w / 2}" y="{height - 28}" text-anchor="middle" class="xlabel">{svg_escape(x_label)}</text>
  <text x="{left - 92}" y="{top - 20}" class="ylabel">{svg_escape(y_label)}</text>
</svg>
"""
    out_path.write_text(svg, encoding="utf-8")


def draw_histogram(
    values: Sequence[float],
    title: str,
    x_label: str,
    out_path: Path,
    *,
    bins: int = 12,
) -> None:
    cleaned = [value for value in values if not math.isnan(value) and not math.isinf(value)]
    if not cleaned:
        cleaned = [0.0]
    v_min = min(cleaned)
    v_max = max(cleaned)
    if v_min == v_max:
        v_min = 0.0
        v_max = max(1.0, v_max * 1.2)
    bin_count = max(1, min(bins, len(cleaned)))
    width = (v_max - v_min) / bin_count
    labels: list[str] = []
    counts = [0.0 for _ in range(bin_count)]
    for value in cleaned:
        idx = min(bin_count - 1, int((value - v_min) / width))
        counts[idx] += 1.0
    for idx in range(bin_count):
        start = v_min + idx * width
        end = start + width
        labels.append(f"{start:.1f}-{end:.1f}")
    draw_bar_chart(labels, counts, title, x_label, out_path, value_fmt="{:.0f}")


def draw_line_chart(
    series: Sequence[Tuple[str, Sequence[float]]],
    title: str,
    y_label: str,
    out_path: Path,
    *,
    reference_lines: Sequence[Tuple[str, float]] = (),
) -> None:
    width, height = 1280, 720
    left, right, top, bottom = 110, 220, 80, 120
    plot_w = width - left - right
    plot_h = height - top - bottom

    max_len = max((len(values) for _, values in series), default=0)
    if max_len < 2:
        max_len = 2

    all_values = [v for _, values in series for v in values]
    all_values.extend(value for _, value in reference_lines)
    y_min, y_max = _nice_y_bounds(all_values)
    if y_max <= y_min:
        y_max = y_min + 1.0

    def x_px(i: int, n: int) -> float:
        if n <= 1:
            return left
        return left + (i / (n - 1)) * plot_w

    def y_px(v: float) -> float:
        return top + (y_max - v) / (y_max - y_min) * plot_h

    grid = []
    for i in range(6):
        ratio = i / 5
        y = top + plot_h * ratio
        v = y_max - (y_max - y_min) * ratio
        grid.append(
            f'<line x1="{left}" y1="{y:.2f}" x2="{left + plot_w}" y2="{y:.2f}" class="grid" />'
        )
        grid.append(
            f'<text x="{left - 10}" y="{y + 5:.2f}" text-anchor="end" class="tick">{svg_escape(f"{v:.2f}")}</text>'
        )

    for i in range(6):
        ratio = i / 5
        x = left + plot_w * ratio
        idx = int(round((max_len - 1) * ratio))
        grid.append(
            f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{top + plot_h}" class="grid" />'
        )
        grid.append(
            f'<text x="{x:.2f}" y="{top + plot_h + 30:.2f}" text-anchor="middle" class="tick">{idx}</text>'
        )

    lines = []
    legends = []
    for idx, (name, values) in enumerate(series):
        if not values:
            continue
        color = COLORS[idx % len(COLORS)]
        points = " ".join(f"{x_px(i, len(values)):.2f},{y_px(v):.2f}" for i, v in enumerate(values))
        lines.append(
            f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="2.8" stroke-linejoin="round" stroke-linecap="round" />'
        )
        ly = top + 20 + idx * 28
        legends.append(
            f'<line x1="{left + plot_w + 20}" y1="{ly - 6}" x2="{left + plot_w + 48}" y2="{ly - 6}" stroke="{color}" stroke-width="4" />'
        )
        legends.append(
            f'<text x="{left + plot_w + 56}" y="{ly}" class="tick">{svg_escape(name)}</text>'
        )
    for ref_idx, (name, value) in enumerate(reference_lines):
        color = "#111827" if ref_idx == 0 else "#6b7280"
        y = y_px(value)
        lines.append(
            f'<line x1="{left}" y1="{y:.2f}" x2="{left + plot_w}" y2="{y:.2f}" stroke="{color}" stroke-width="2" stroke-dasharray="8 6" />'
        )
        ly = top + 20 + (len(legends) // 2 + ref_idx) * 28
        legends.append(
            f'<line x1="{left + plot_w + 20}" y1="{ly - 6}" x2="{left + plot_w + 48}" y2="{ly - 6}" stroke="{color}" stroke-width="3" stroke-dasharray="8 6" />'
        )
        legends.append(
            f'<text x="{left + plot_w + 56}" y="{ly}" class="tick">{svg_escape(name)}</text>'
        )

    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">
  <style>
    .title {{ font: 700 30px Arial, sans-serif; fill: #111827; }}
    .axis {{ stroke: #374151; stroke-width: 2; }}
    .grid {{ stroke: #e5e7eb; stroke-width: 1; }}
    .tick {{ font: 16px Arial, sans-serif; fill: #374151; }}
    .xlabel {{ font: 18px Arial, sans-serif; fill: #111827; }}
    .ylabel {{ font: 18px Arial, sans-serif; fill: #111827; }}
  </style>
  <rect width="100%" height="100%" fill="#ffffff" />
  <text x="{left}" y="42" class="title">{svg_escape(title)}</text>
  <line x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" y2="{top + plot_h}" class="axis" />
  <line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_h}" class="axis" />
  {"".join(grid)}
  {"".join(lines)}
  {"".join(legends)}
  <text x="{left + plot_w / 2}" y="{height - 30}" text-anchor="middle" class="xlabel">sample index</text>
  <text x="{left - 80}" y="{top - 20}" class="ylabel">{svg_escape(y_label)}</text>
</svg>
"""
    out_path.write_text(svg, encoding="utf-8")


def collect_series_from_csv(
    run_rows: Sequence[Dict[str, str]],
    by_safe_id: Dict[str, Path],
    value_key: str,
) -> List[Tuple[str, List[float]]]:
    output: List[Tuple[str, List[float]]] = []
    for row in run_rows:
        run_id = row["run_id"]
        safe_id = to_safe_id(run_id)
        file_path = by_safe_id.get(safe_id)
        if not file_path:
            continue
        values = [parse_float(item.get(value_key, "")) for item in read_csv_rows(file_path)]
        output.append((short_label(run_id), values))
    return output


def discover_files(input_dir: Path, prefix: str) -> Dict[str, Path]:
    found: Dict[str, Path] = {}
    pattern = re.compile(rf"^{re.escape(prefix)}house_\d+_(.+)\.csv$")
    for file_path in sorted(input_dir.glob(f"{prefix}house_*_*.csv")):
        match = pattern.match(file_path.name)
        if not match:
            continue
        safe_id = match.group(1)
        found[safe_id] = file_path
        # Backward compatibility with older export names ending in "_".
        found[safe_id.rstrip("_")] = file_path
    return found


def write_index_md(
    output_dir: Path,
    run_rows: Sequence[Dict[str, str]],
    *,
    chart_files: Sequence[Path] | None = None,
) -> None:
    if chart_files is None:
        chart_file_names = [
            "chart_avg_tr.svg",
            "chart_rmse.svg",
            "chart_tr_1m_comparison.svg",
            "chart_rmse_1m_comparison.svg",
            "chart_actual_vs_reconstructed_<run_id>.svg (one file per run)",
        ]
    else:
        chart_file_names = [path.name for path in chart_files]
    lines = [
        "# Generated Charts",
        "",
        "## Summary",
        "",
        "| run_id | mode | epsilon_ratio | avg_tr | rmse | p95 |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for row in run_rows:
        avg_tr = row.get("avg_tr") or row.get("avg_transmission_rate") or ""
        rmse = row.get("rmse") or row.get("rmse_house_error") or ""
        p95 = row.get("p95_abs_eh") or row.get("p95_abs_house_error") or ""
        lines.append(
            f"| {row.get('run_id', '')} | {row.get('mode', '')} | {row.get('epsilon_ratio', '')} | {avg_tr} | {rmse} | {p95} |"
        )
    lines.extend(["", "## Files", ""])
    lines.extend(f"- {name}" for name in chart_file_names)
    (output_dir / "charts_index.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def has_required_export(input_dir: Path) -> bool:
    return (input_dir / "house_summary.csv").exists() and (input_dir / "house_timeseries.csv").exists()


def generate_required_charts(input_dir: Path, output_dir: Path, max_points: int) -> list[Path]:
    experiment_rows = read_csv_rows(input_dir / "experiment_runs.csv") if (input_dir / "experiment_runs.csv").exists() else []
    house_summary_rows = read_csv_rows(input_dir / "house_summary.csv")
    house_timeseries_rows = read_csv_rows(input_dir / "house_timeseries.csv")
    threshold_rows = read_csv_rows(input_dir / "threshold_trace.csv") if (input_dir / "threshold_trace.csv").exists() else []

    experiments_by_run = {row.get("run_id", ""): row for row in experiment_rows}
    summaries = [_enrich_run_row(row, experiments_by_run.get(row.get("run_id", ""), {})) for row in house_summary_rows]
    groups: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in summaries:
        groups[(row["run_group"], row["house_id"])].append(row)

    generated: list[Path] = []
    for (run_group, house_id), rows in sorted(groups.items()):
        rows = sorted(rows, key=_run_sort_key)
        pareto_rmse = [_pareto_point(row, "rmse_house_error") for row in rows]
        pareto_p95 = [_pareto_point(row, "p95_abs_house_error") for row in rows]
        rmse_path = output_dir / chart_path(run_group, house_id, "pareto_tr_rmse")
        p95_path = output_dir / chart_path(run_group, house_id, "pareto_tr_p95")
        draw_scatter_chart(
            [point for point in pareto_rmse if point],
            f"Pareto TR vs RMSE ({run_group}, house {house_id})",
            "transmission rate",
            "RMSE_H",
            rmse_path,
        )
        draw_scatter_chart(
            [point for point in pareto_p95 if point],
            f"Pareto TR vs P95 ({run_group}, house {house_id})",
            "transmission rate",
            "P95 |E_H|",
            p95_path,
        )
        generated.extend([rmse_path, p95_path])

        detail_rows = [
            row
            for row in rows
            if row.get("mode") in {"uniform", "hiereb"} and is_detail_epsilon(parse_optional_float(row.get("epsilon_ratio")))
        ]
        for row in detail_rows:
            mode = row["mode"]
            run_id = row["run_id"]
            series_rows = _sample_rows(
                [
                    item
                    for item in house_timeseries_rows
                    if item.get("run_id") == run_id and str(item.get("house_id")) == str(house_id)
                ],
                max_points,
            )
            if not series_rows:
                continue
            actual_values = [parse_float(item.get("actual_load", "")) for item in series_rows]
            reconstructed_values = [parse_float(item.get("reconstructed_load", "")) for item in series_rows]
            error_values = [parse_float(item.get("house_error", "")) for item in series_rows]
            delta_h = parse_optional_float(row.get("Delta_H"))

            load_path = output_dir / chart_path(run_group, house_id, "actual_vs_reconstructed", mode=mode)
            draw_line_chart(
                [(f"{mode} actual", actual_values), (f"{mode} reconstructed", reconstructed_values)],
                f"Actual vs Reconstructed ({mode}, eps=0.05)",
                "watts",
                load_path,
            )
            generated.append(load_path)

            reference_lines: list[tuple[str, float]] = []
            if delta_h is not None:
                reference_lines = [("+Delta_H", delta_h), ("-Delta_H", -delta_h)]
            error_path = output_dir / chart_path(run_group, house_id, "house_error", mode=mode)
            draw_line_chart(
                [(f"{mode} E_H", error_values)],
                f"House Error with Delta_H ({mode}, eps=0.05)",
                "watts",
                error_path,
                reference_lines=reference_lines,
            )
            generated.append(error_path)

        hiereb_run_ids = {row["run_id"] for row in detail_rows if row.get("mode") == "hiereb"}
        if not hiereb_run_ids:
            hiereb_run_ids = {row["run_id"] for row in rows if row.get("mode") == "hiereb"}
        hiereb_threshold_rows = [
            row
            for row in threshold_rows
            if row.get("run_id") in hiereb_run_ids
            and row.get("mode") == "hiereb"
            and str(row.get("house_id")) == str(house_id)
        ]
        if hiereb_threshold_rows:
            threshold_values = [value for value in (parse_optional_float(row.get("threshold")) for row in hiereb_threshold_rows) if value is not None]
            distribution_path = output_dir / chart_path(run_group, house_id, "threshold_distribution", mode="hiereb")
            draw_histogram(
                threshold_values,
                f"HierEB Threshold Distribution ({run_group}, house {house_id})",
                "threshold count",
                distribution_path,
            )
            generated.append(distribution_path)

            trace_series: list[tuple[str, list[float]]] = []
            for plug_key, plug_rows in sorted(_group_threshold_rows_by_plug(hiereb_threshold_rows).items())[:6]:
                values = [
                    parse_float(row.get("threshold", ""))
                    for row in sorted(plug_rows, key=lambda item: parse_float(item.get("allocation_time", "")))
                ]
                trace_series.append((plug_key, values))
            trace_path = output_dir / chart_path(run_group, house_id, "threshold_trace", mode="hiereb")
            draw_line_chart(
                trace_series,
                f"HierEB Threshold Trace ({run_group}, house {house_id})",
                "delta_p",
                trace_path,
            )
            generated.append(trace_path)

    write_index_md(output_dir, summaries, chart_files=generated)
    return generated


def _enrich_run_row(row: Dict[str, str], experiment: Dict[str, str]) -> Dict[str, str]:
    item = dict(row)
    run_id = item.get("run_id", "")
    parsed = parse_run_id_parts(run_id)
    for key in ("mode", "epsilon_ratio"):
        if item.get(key) in (None, "") and experiment.get(key) not in (None, ""):
            item[key] = experiment[key]
    if item.get("mode") in (None, "") and parsed.get("mode"):
        item["mode"] = str(parsed["mode"])
    if item.get("epsilon_ratio") in (None, "") and parsed.get("epsilon_ratio") is not None:
        item["epsilon_ratio"] = str(parsed["epsilon_ratio"])
    if item.get("house_id") in (None, "") and parsed.get("house_id") is not None:
        item["house_id"] = str(parsed["house_id"])
    run_group = experiment.get("sweep_id") or str(parsed.get("run_group") or "report")
    item["run_group"] = run_group
    return item


def _run_sort_key(row: Dict[str, str]) -> tuple[int, float, str]:
    mode_order = {"full_tx": 0, "uniform": 1, "hiereb": 2}
    epsilon = parse_optional_float(row.get("epsilon_ratio"))
    return (mode_order.get(row.get("mode", ""), 99), -1.0 if epsilon is None else epsilon, row.get("run_id", ""))


def _pareto_point(row: Dict[str, str], y_key: str) -> Dict[str, object] | None:
    x_value = parse_optional_float(row.get("avg_transmission_rate"))
    y_value = parse_optional_float(row.get(y_key))
    if x_value is None or y_value is None:
        return None
    epsilon_ratio = parse_optional_float(row.get("epsilon_ratio"))
    mode = row.get("mode", "unknown")
    return {
        "x": x_value,
        "y": y_value,
        "mode": mode,
        "epsilon_ratio": epsilon_ratio,
        "label": f"{mode} eps={epsilon_label(epsilon_ratio)}",
    }


def _sample_rows(rows: Sequence[Dict[str, str]], max_points: int) -> list[Dict[str, str]]:
    sorted_rows = sorted(rows, key=lambda item: parse_float(item.get("timestamp", item.get("source_timestamp", ""))))
    if not sorted_rows:
        return []
    step = max(1, math.ceil(len(sorted_rows) / max(1, max_points)))
    return sorted_rows[::step]


def _group_threshold_rows_by_plug(rows: Sequence[Dict[str, str]]) -> dict[str, list[Dict[str, str]]]:
    grouped: dict[str, list[Dict[str, str]]] = defaultdict(list)
    for row in rows:
        label = f"plug {row.get('household_id', '')}/{row.get('plug_id', '')}"
        grouped[label].append(row)
    return grouped


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate SVG charts for report artifacts.")
    parser.add_argument("--input-dir", required=True, help="Folder created by export_report_data.sh")
    parser.add_argument("--output-dir", required=True, help="Destination folder for generated SVGs")
    parser.add_argument("--max-points", type=int, default=700, help="Max points for per-run load charts")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if has_required_export(input_dir):
        generated = generate_required_charts(input_dir, output_dir, args.max_points)
        print(f"Charts generated in: {output_dir}")
        print(f"Generated SVG files: {len(generated)}")
        return 0

    run_summary_path = input_dir / "run_summary.csv"
    if not run_summary_path.exists():
        raise FileNotFoundError(f"Missing file: {run_summary_path}")

    run_rows = read_csv_rows(run_summary_path)
    if not run_rows:
        raise ValueError("run_summary.csv is empty")

    labels = [short_label(row["run_id"]) for row in run_rows]
    avg_tr = [parse_float(row.get("avg_tr", "")) for row in run_rows]
    rmse = [parse_float(row.get("rmse", "")) for row in run_rows]

    draw_bar_chart(labels, avg_tr, "Average Transmission Rate by Run", "avg tr", output_dir / "chart_avg_tr.svg")
    draw_bar_chart(labels, rmse, "RMSE by Run", "rmse", output_dir / "chart_rmse.svg")

    minute_files = discover_files(input_dir, "minute_metrics_")
    tr_series = collect_series_from_csv(run_rows, minute_files, "avg_tr")
    rmse_series = collect_series_from_csv(run_rows, minute_files, "rmse_1m")
    if tr_series:
        draw_line_chart(
            tr_series,
            "Transmission Rate Trend (1-minute buckets)",
            "avg tr",
            output_dir / "chart_tr_1m_comparison.svg",
        )
    if rmse_series:
        draw_line_chart(
            rmse_series,
            "RMSE Trend (1-minute buckets)",
            "rmse 1m",
            output_dir / "chart_rmse_1m_comparison.svg",
        )

    timeseries_files = discover_files(input_dir, "timeseries_")
    for row in run_rows:
        run_id = row["run_id"]
        safe_id = to_safe_id(run_id)
        file_path = timeseries_files.get(safe_id)
        if not file_path:
            continue
        series_rows = read_csv_rows(file_path)
        if not series_rows:
            continue

        step = max(1, math.ceil(len(series_rows) / max(1, args.max_points)))
        sampled = series_rows[::step]
        actual_values = [parse_float(item.get("actual_load", "")) for item in sampled]
        reconstructed_values = [
            parse_float(item.get("reconstructed_load", item.get("pred_load", ""))) for item in sampled
        ]
        draw_line_chart(
            [(f"{short_label(run_id)} actual", actual_values), (f"{short_label(run_id)} reconstructed", reconstructed_values)],
            f"Actual vs Reconstructed Load ({short_label(run_id)})",
            "watts",
            output_dir / f"chart_actual_vs_reconstructed_{safe_id}.svg",
        )

    write_index_md(output_dir, run_rows)
    print(f"Charts generated in: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
