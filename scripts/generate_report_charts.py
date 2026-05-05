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


def short_label(run_id: str) -> str:
    match = re.match(r"e2e_([^_]+)_(\d{14})$", run_id)
    if match:
        return f"{match.group(1)}"
    return run_id


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


def draw_line_chart(
    series: Sequence[Tuple[str, Sequence[float]]],
    title: str,
    y_label: str,
    out_path: Path,
) -> None:
    width, height = 1280, 720
    left, right, top, bottom = 110, 220, 80, 120
    plot_w = width - left - right
    plot_h = height - top - bottom

    max_len = max((len(values) for _, values in series), default=0)
    if max_len < 2:
        max_len = 2

    all_values = [v for _, values in series for v in values]
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


def write_index_md(output_dir: Path, run_rows: Sequence[Dict[str, str]]) -> None:
    lines = [
        "# Generated Charts",
        "",
        "## Summary",
        "",
        "| run_id | avg_tr | rmse |",
        "|---|---:|---:|",
    ]
    for row in run_rows:
        lines.append(
            f"| {row['run_id']} | {row.get('avg_tr', '')} | {row.get('rmse', '')} |"
        )
    lines.extend(
        [
            "",
            "## Files",
            "",
            "- chart_avg_tr.svg",
            "- chart_rmse.svg",
            "- chart_tr_1m_comparison.svg",
            "- chart_rmse_1m_comparison.svg",
            "- chart_actual_vs_pred_<run_id>.svg (one file per run)",
        ]
    )
    (output_dir / "charts_index.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate SVG charts for report artifacts.")
    parser.add_argument("--input-dir", required=True, help="Folder created by export_report_data.sh")
    parser.add_argument("--output-dir", required=True, help="Destination folder for generated SVGs")
    parser.add_argument("--max-points", type=int, default=700, help="Max points for per-run load charts")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

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
        pred_values = [parse_float(item.get("pred_load", "")) for item in sampled]
        draw_line_chart(
            [(f"{short_label(run_id)} actual", actual_values), (f"{short_label(run_id)} pred", pred_values)],
            f"Actual vs Predicted Load ({short_label(run_id)})",
            "watts",
            output_dir / f"chart_actual_vs_pred_{safe_id}.svg",
        )

    write_index_md(output_dir, run_rows)
    print(f"Charts generated in: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
