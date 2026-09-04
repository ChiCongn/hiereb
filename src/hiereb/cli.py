"""Typer command-line interface for offline HierEB experiments."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Literal

import polars as pl
import typer

from hiereb.config import DeployablePredictorMode, Mode, load_config
from hiereb.data.synthetic import generate_synthetic
from hiereb.data.synthetic_direction3 import generate_direction3_synthetic
from hiereb.determinism import verify_determinism
from hiereb.experiment import (
    compare_modes,
    compare_predictors,
    execute_mode,
    execute_mode_streaming,
    execute_predictor_mode,
    run_predictor_matrix,
    validate_predictor_hyperparameters,
    verify_legacy_equivalence,
)
from hiereb.house_experiment import run_house_experiment
from hiereb.utils.progress import ProgressReporter

app = typer.Typer(no_args_is_help=True, help="Deterministic offline HierEB simulator")
ConfigOption = Annotated[Path, typer.Option("--config", exists=True, dir_okay=False)]
DataPathOption = Annotated[
    Path | None,
    typer.Option("--data-path", dir_okay=False, help="Override data.path or HIEREB_DATA_PATH"),
]
PredictorAllocator = Literal[
    "full_tx",
    "uniform",
    "flat_variance",
    "legacy_two_stage",
    "true_hierarchical",
    "true_hierarchical_cap",
]


def _progress_reporter() -> ProgressReporter:
    return ProgressReporter(lambda message: typer.echo(message, err=True))


@app.command("validate-config")
def validate_config(config: ConfigOption, data_path: DataPathOption = None) -> None:
    """Parse and validate a configuration without reading its dataset."""
    resolved = load_config(config, data_path)
    typer.echo(f"valid: {resolved.experiment.name} ({len(resolved.experiment.modes)} modes)")


@app.command("generate-synthetic")
def generate_synthetic_command(
    output: Annotated[Path, typer.Option("--output", dir_okay=False)],
    seed: Annotated[int, typer.Option("--seed")] = 42,
    house_id: Annotated[int, typer.Option("--house-id")] = 0,
) -> None:
    """Generate the deterministic 12-hour multi-household fixture."""
    rows = generate_synthetic(output, seed, house_id)
    typer.echo(f"wrote {rows} rows to {output}")


@app.command("generate-direction3-synthetic")
def generate_direction3_synthetic_command(
    output: Annotated[Path, typer.Option("--output", dir_okay=False)],
    seed: Annotated[int, typer.Option("--seed")] = 42,
    house_id: Annotated[int, typer.Option("--house-id")] = 0,
) -> None:
    """Generate the deterministic adaptive-predictor drift fixture."""
    rows = generate_direction3_synthetic(output, seed, house_id)
    typer.echo(f"wrote {rows} rows to {output}")


@app.command()
def run(
    config: ConfigOption,
    mode: Annotated[Mode | None, typer.Option("--mode")] = None,
    data_path: DataPathOption = None,
) -> None:
    """Run one configured mode, or all configured modes when mode is omitted."""
    progress = _progress_reporter()
    progress(f"run: loading config {config}")
    resolved = load_config(config, data_path)
    if mode is None:
        summaries = compare_modes(resolved, progress)
        typer.echo(json.dumps(summaries, indent=2, sort_keys=True))
    else:
        if resolved.data.streaming.enabled:
            _, summary = execute_mode_streaming(resolved, mode, progress=progress)
        else:
            _, summary = execute_mode(resolved, mode, progress=progress)
        typer.echo(json.dumps(summary, indent=2, sort_keys=True))


@app.command()
def compare(config: ConfigOption, data_path: DataPathOption = None) -> None:
    """Run every configured mode and write a comparison table."""
    progress = _progress_reporter()
    progress(f"compare: loading config {config}")
    summaries = compare_modes(load_config(config, data_path), progress)
    for summary in summaries:
        typer.echo(
            f"{summary['mode']}: TR={summary['transmission_ratio']:.6f} "
            f"RMSE={summary['rmse']:.6f} Max={summary['max']:.6f}"
        )


@app.command("compare-predictors")
def compare_predictors_command(
    config: ConfigOption,
    allocator: Annotated[PredictorAllocator | None, typer.Option("--allocator")] = None,
    matched_tr: Annotated[
        bool,
        typer.Option("--matched-tr", help="Run the seven-epsilon matched-TR sweep"),
    ] = False,
    data_path: DataPathOption = None,
) -> None:
    """Run the config matrix, or compare all predictors for one allocator."""
    progress = _progress_reporter()
    progress(f"compare-predictors: loading config {config}")
    resolved = load_config(config, data_path)
    if allocator is None:
        if matched_tr:
            raise typer.BadParameter("--matched-tr requires an explicit --allocator")
        rows = run_predictor_matrix(resolved, progress)
    else:
        rows = compare_predictors(resolved, allocator, progress, matched_tr=matched_tr)
    for row in rows:
        typer.echo(
            f"{row['predictor_mode']}: TR={float(row['transmission_ratio']):.6f} "
            f"RMSE={float(row['rmse']):.6f} CVaR99={float(row['cvar99']):.6f}"
            + (f" rejected={row['rejected']}" if "rejected" in row else "")
        )


@app.command("run-predictor")
def run_predictor_command(
    config: ConfigOption,
    allocator: Annotated[PredictorAllocator, typer.Option("--allocator")],
    predictor_mode: Annotated[DeployablePredictorMode, typer.Option("--predictor-mode")],
    data_path: DataPathOption = None,
) -> None:
    """Run exactly one Direction 3 predictor/allocator pair."""
    progress = _progress_reporter()
    progress(f"run-predictor: loading config {config}")
    resolved = load_config(config, data_path)
    if not resolved.data.streaming.enabled:
        raise typer.BadParameter("run-predictor requires data.streaming.enabled=true")
    result, summary = execute_predictor_mode(
        resolved,
        allocator,
        predictor_mode,
        progress=progress,
    )
    typer.echo(
        json.dumps(
            {
                "allocator_mode": allocator,
                "predictor_mode": result.predictor_mode,
                "base_metrics": summary,
                "predictor_metrics": result.predictor_summary,
            },
            indent=2,
            sort_keys=True,
        )
    )


@app.command("validate-predictor")
def validate_predictor_command(
    config: ConfigOption,
    allocator: Annotated[PredictorAllocator, typer.Option("--allocator")] = "uniform",
    data_path: DataPathOption = None,
) -> None:
    """Tune Direction 3 on validation only using the staged search."""
    progress = _progress_reporter()
    progress(f"validate-predictor: loading config {config}")
    selection = validate_predictor_hyperparameters(
        load_config(config, data_path), allocator, progress
    )
    typer.echo(json.dumps(selection, indent=2, sort_keys=True))


@app.command("verify-legacy-equivalence")
def verify_legacy_equivalence_command(
    config: ConfigOption, data_path: DataPathOption = None
) -> None:
    """Prove flat and legacy equivalence on thresholds and replay decisions."""
    report = verify_legacy_equivalence(load_config(config, data_path))
    typer.echo(json.dumps(report, indent=2, sort_keys=True))
    if not report["equivalent"]:
        raise typer.Exit(code=1)


@app.command("verify-determinism")
def verify_determinism_command(config: ConfigOption, data_path: DataPathOption = None) -> None:
    """Run a config twice and compare non-runtime artifact checksums."""
    report = verify_determinism(load_config(config, data_path))
    typer.echo(json.dumps(report, indent=2, sort_keys=True))
    if not report["deterministic"]:
        raise typer.Exit(code=1)


@app.command("experiment-house")
def experiment_house_command(config: ConfigOption, data_path: DataPathOption = None) -> None:
    """Run the complete one-house Direction 1 research workflow."""
    report = run_house_experiment(load_config(config, data_path))
    typer.echo(json.dumps(report, indent=2, sort_keys=True))


@app.command("inspect-outliers")
def inspect_outliers(
    run_dir: Annotated[Path, typer.Option("--run-dir", exists=True, file_okay=False)],
    limit: Annotated[int, typer.Option("--limit", min=1)] = 10,
) -> None:
    """Print the largest event-aligned house errors from an existing run."""
    path = run_dir / "top_outliers.parquet"
    if not path.is_file():
        raise typer.BadParameter(f"missing artifact: {path}")
    typer.echo(pl.read_parquet(path).head(limit))


@app.command("inspect-drift")
def inspect_drift(
    run_dir: Annotated[Path, typer.Option("--run-dir", exists=True, file_okay=False)],
    limit: Annotated[int, typer.Option("--limit", min=1)] = 20,
) -> None:
    """Print deterministic drift-trigger diagnostics from an adaptive run."""
    path = run_dir / "drift_events.parquet"
    if not path.is_file():
        raise typer.BadParameter(f"missing artifact: {path}")
    typer.echo(pl.read_parquet(path).head(limit))


@app.command("inspect-resynchronization")
def inspect_resynchronization(
    run_dir: Annotated[Path, typer.Option("--run-dir", exists=True, file_okay=False)],
    limit: Annotated[int, typer.Option("--limit", min=1)] = 20,
) -> None:
    """Print periodic synchronization diagnostics from an adaptive run."""
    path = run_dir / "resynchronization_events.parquet"
    if not path.is_file():
        raise typer.BadParameter(f"missing artifact: {path}")
    typer.echo(pl.read_parquet(path).head(limit))


if __name__ == "__main__":
    app()
