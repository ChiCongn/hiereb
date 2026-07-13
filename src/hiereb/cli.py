"""Typer command-line interface for offline HierEB experiments."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import polars as pl
import typer

from hiereb.config import Mode, load_config
from hiereb.data.synthetic import generate_synthetic
from hiereb.experiment import compare_modes, execute_mode, verify_legacy_equivalence

app = typer.Typer(no_args_is_help=True, help="Deterministic offline HierEB simulator")
ConfigOption = Annotated[Path, typer.Option("--config", exists=True, dir_okay=False)]


@app.command("validate-config")
def validate_config(config: ConfigOption) -> None:
    """Parse and validate a configuration without reading its dataset."""
    resolved = load_config(config)
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


@app.command()
def run(
    config: ConfigOption,
    mode: Annotated[Mode | None, typer.Option("--mode")] = None,
) -> None:
    """Run one configured mode, or all configured modes when mode is omitted."""
    resolved = load_config(config)
    if mode is None:
        summaries = compare_modes(resolved)
        typer.echo(json.dumps(summaries, indent=2, sort_keys=True))
    else:
        _, summary = execute_mode(resolved, mode)
        typer.echo(json.dumps(summary, indent=2, sort_keys=True))


@app.command()
def compare(config: ConfigOption) -> None:
    """Run every configured mode and write a comparison table."""
    summaries = compare_modes(load_config(config))
    for summary in summaries:
        typer.echo(
            f"{summary['mode']}: TR={summary['transmission_ratio']:.6f} "
            f"RMSE={summary['rmse']:.6f} Max={summary['max']:.6f}"
        )


@app.command("verify-legacy-equivalence")
def verify_legacy_equivalence_command(config: ConfigOption) -> None:
    """Prove flat and legacy equivalence on thresholds and replay decisions."""
    report = verify_legacy_equivalence(load_config(config))
    typer.echo(json.dumps(report, indent=2, sort_keys=True))
    if not report["equivalent"]:
        raise typer.Exit(code=1)


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


if __name__ == "__main__":
    app()

