#!/usr/bin/env python3
"""Build and optionally execute deterministic HierEB sweep plans."""
from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

from config.settings import settings


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_EPSILON_RATIO_VALUES = [0.01, 0.02, 0.05, 0.10, 0.20]
REDUCED_EPSILON_RATIO_VALUES = [0.02, 0.05, 0.10]
SWEEP_MODES = ("uniform", "hiereb")

PLAN_COLUMNS = [
    "sequence",
    "run_id",
    "mode",
    "house_id",
    "epsilon_ratio",
    "sweep_id",
    "is_sweep",
    "sweep_size",
    "reduced_sweep",
    "sweep_param_name",
    "sweep_param_value",
]


@dataclass(frozen=True)
class SweepRun:
    """One planned experiment invocation."""

    sequence: int
    run_id: str
    mode: str
    house_id: int
    epsilon_ratio: float | None
    sweep_id: str
    is_sweep: bool
    sweep_size: int
    reduced_sweep: bool
    sweep_param_name: str | None
    sweep_param_value: float | None


def epsilon_ratio_token(epsilon_ratio: float) -> str:
    """Return the eps token used in sweep run_id values."""
    ratio_x100 = int(round(epsilon_ratio * 100))
    return f"eps{ratio_x100:03d}"


def format_run_id(
    sweep_id: str,
    mode: str,
    house_id: int,
    epsilon_ratio: float | None,
) -> str:
    """Format deterministic run IDs for sweep runs."""
    prefix = f"{sweep_id}_{mode}_house{house_id}"
    if mode == "full_tx":
        return prefix
    if epsilon_ratio is None:
        raise ValueError(f"epsilon_ratio is required for mode={mode}")
    return f"{prefix}_{epsilon_ratio_token(epsilon_ratio)}"


def build_sweep_plan(
    *,
    sweep_id: str,
    house_id: int,
    epsilon_ratio_values: Iterable[float],
    reduced_sweep: bool = False,
) -> list[SweepRun]:
    """Build the full_tx anchor plus uniform/hiereb runs for each epsilon."""
    epsilon_values = [_normalise_epsilon(value) for value in epsilon_ratio_values]
    if not epsilon_values:
        raise ValueError("epsilon_ratio_values must not be empty")

    sweep_size = len(epsilon_values)
    runs = [
        SweepRun(
            sequence=1,
            run_id=format_run_id(sweep_id, "full_tx", house_id, None),
            mode="full_tx",
            house_id=house_id,
            epsilon_ratio=None,
            sweep_id=sweep_id,
            is_sweep=False,
            sweep_size=sweep_size,
            reduced_sweep=reduced_sweep,
            sweep_param_name=None,
            sweep_param_value=None,
        )
    ]

    sequence = 2
    for epsilon_ratio in epsilon_values:
        for mode in SWEEP_MODES:
            runs.append(
                SweepRun(
                    sequence=sequence,
                    run_id=format_run_id(sweep_id, mode, house_id, epsilon_ratio),
                    mode=mode,
                    house_id=house_id,
                    epsilon_ratio=epsilon_ratio,
                    sweep_id=sweep_id,
                    is_sweep=True,
                    sweep_size=sweep_size,
                    reduced_sweep=reduced_sweep,
                    sweep_param_name="epsilon_ratio",
                    sweep_param_value=epsilon_ratio,
                )
            )
            sequence += 1

    return runs


def plan_document(runs: list[SweepRun]) -> dict:
    """Return a JSON-friendly sweep plan document."""
    if not runs:
        raise ValueError("runs must not be empty")
    sweep_runs = [run for run in runs if run.is_sweep]
    epsilon_ratio_values = sorted(
        {run.epsilon_ratio for run in sweep_runs if run.epsilon_ratio is not None}
    )
    first = runs[0]
    return {
        "sweep_id": first.sweep_id,
        "house_id": first.house_id,
        "epsilon_ratio_values": epsilon_ratio_values,
        "sweep_size": first.sweep_size,
        "reduced_sweep": first.reduced_sweep,
        "runs": [asdict(run) for run in runs],
    }


def write_plan_csv(path: Path, runs: list[SweepRun]) -> Path:
    """Write the deterministic sweep plan to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PLAN_COLUMNS)
        writer.writeheader()
        for run in runs:
            writer.writerow(_csv_row(run))
    return path


def print_plan(runs: list[SweepRun], output_format: str) -> None:
    """Print a deterministic dry-run plan to stdout."""
    if output_format == "json":
        json.dump(plan_document(runs), sys.stdout, indent=2)
        sys.stdout.write("\n")
        return

    writer = csv.DictWriter(sys.stdout, fieldnames=PLAN_COLUMNS)
    writer.writeheader()
    for run in runs:
        writer.writerow(_csv_row(run))


def execute_sweep(
    runs: list[SweepRun],
    *,
    verify_script: Path,
    runtime_seconds: int | None,
    summary_file: Path,
) -> None:
    """Execute each planned run through the E2E verifier."""
    summary_file.parent.mkdir(parents=True, exist_ok=True)
    verify_script = verify_script if verify_script.is_absolute() else ROOT_DIR / verify_script
    for run in runs:
        env = os.environ.copy()
        env["RUN_ID_OVERRIDE"] = run.run_id
        env["HOUSE_IDS"] = json.dumps([run.house_id], separators=(",", ":"))
        env["ONE_HOUSE_ID"] = str(run.house_id)
        env["SUMMARY_FILE"] = str(summary_file)
        env["SWEEP_ID"] = run.sweep_id
        env["IS_SWEEP"] = _bool_env(run.is_sweep)
        env["SWEEP_SIZE"] = str(run.sweep_size)
        env["REDUCED_SWEEP"] = _bool_env(run.reduced_sweep)
        if runtime_seconds is not None:
            env["E2E_RUNTIME_SECONDS"] = str(runtime_seconds)
        if run.epsilon_ratio is not None:
            env["EPSILON_H"] = _float_env(run.epsilon_ratio)
            env["EPSILON_RATIO"] = _float_env(run.epsilon_ratio)

        subprocess.run(
            ["bash", str(verify_script), run.mode],
            cwd=ROOT_DIR,
            env=env,
            check=True,
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Only print/write the deterministic plan.")
    parser.add_argument("--execute", action="store_true", help="Run the planned experiments via verify_e2e.sh.")
    parser.add_argument("--format", choices=("json", "csv"), default="json", help="Dry-run stdout format.")
    parser.add_argument("--sweep-id", default=settings.SWEEP_ID)
    parser.add_argument("--house-id", type=int, default=settings.ONE_HOUSE_ID)
    parser.add_argument(
        "--epsilon-values",
        default=None,
        help="Comma-separated or JSON list of epsilon ratios. Defaults to the configured full sweep.",
    )
    parser.add_argument("--reduced", action="store_true", help="Use the reduced sweep grid.")
    parser.add_argument("--output", type=Path, default=None, help="Optional path for sweep_plan.csv.")
    parser.add_argument("--summary-file", type=Path, default=None, help="E2E summary CSV path for --execute.")
    parser.add_argument("--runtime-seconds", type=int, default=None, help="Override E2E_RUNTIME_SECONDS.")
    parser.add_argument("--verify-script", type=Path, default=Path("scripts/verify_e2e.sh"))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.dry_run and args.execute:
        raise SystemExit("--dry-run and --execute are mutually exclusive")

    epsilon_values, reduced_sweep = _resolve_epsilon_values(args)
    runs = build_sweep_plan(
        sweep_id=args.sweep_id,
        house_id=args.house_id,
        epsilon_ratio_values=epsilon_values,
        reduced_sweep=reduced_sweep,
    )

    if args.output is not None:
        write_plan_csv(args.output, runs)

    if args.execute:
        output_dir = ROOT_DIR / "results" / "sweeps" / args.sweep_id
        plan_path = args.output or output_dir / "sweep_plan.csv"
        summary_file = args.summary_file or output_dir / "e2e_summary.csv"
        write_plan_csv(plan_path, runs)
        execute_sweep(
            runs,
            verify_script=args.verify_script,
            runtime_seconds=args.runtime_seconds,
            summary_file=summary_file,
        )
        print(f"Sweep completed. Plan: {plan_path}")
        print(f"Summary: {summary_file}")
        return 0

    print_plan(runs, args.format)
    return 0


def _resolve_epsilon_values(args: argparse.Namespace) -> tuple[list[float], bool]:
    if args.epsilon_values:
        return parse_epsilon_values(args.epsilon_values), args.reduced
    if args.reduced:
        return list(settings.SWEEP_REDUCED_EPSILON_RATIO_VALUES), True
    return list(settings.SWEEP_EPSILON_RATIO_VALUES), False


def parse_epsilon_values(raw: str) -> list[float]:
    """Parse comma-separated or JSON list epsilon values."""
    stripped = raw.strip()
    if stripped.startswith("["):
        values = json.loads(stripped)
    else:
        values = [part.strip() for part in stripped.split(",") if part.strip()]
    return [_normalise_epsilon(float(value)) for value in values]


def _normalise_epsilon(value: float) -> float:
    if value <= 0:
        raise ValueError("epsilon ratios must be positive")
    return round(float(value), 10)


def _csv_row(run: SweepRun) -> dict[str, object]:
    row = asdict(run)
    return {column: _csv_value(row.get(column)) for column in PLAN_COLUMNS}


def _csv_value(value: object) -> object:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).lower()
    return value


def _bool_env(value: bool) -> str:
    return "true" if value else "false"


def _float_env(value: float) -> str:
    return f"{value:g}"


if __name__ == "__main__":
    raise SystemExit(main())
