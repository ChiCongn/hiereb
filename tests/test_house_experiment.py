from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from hiereb.config import load_config
from hiereb.data.synthetic import generate_synthetic
from hiereb.determinism import verify_determinism
from hiereb.house_experiment import run_house_experiment


def test_complete_house_workflow_on_synthetic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_path = (tmp_path / "synthetic.csv").resolve()
    generate_synthetic(data_path)
    config = load_config(Path("configs/house_0.example.yaml"), data_path)
    config = config.model_copy(
        update={
            "experiment": config.experiment.model_copy(
                update={"output_dir": (tmp_path / "output").resolve(), "overwrite": True}
            )
        }
    )
    monkeypatch.chdir(tmp_path)
    report = run_house_experiment(config)
    output = Path(str(report["output_dir"]))
    assert (tmp_path / "HOUSE0_DIRECTION1_REPORT.md").is_file()
    for name in (
        "comparison_tail_metrics.csv",
        "hierarchy_effect_summary.json",
        "hierarchy_effect_cycles.parquet",
        "cap_diagnostics.json",
        "validation_selection.json",
        "pareto_points.csv",
        "pareto_tr_rmse.png",
        "pareto_tr_cvar99.png",
        "matched_tr_comparison.csv",
        "exact_proxy_comparison.json",
        "exact_proxy_plug_scores.parquet",
        "runtime_memory.json",
        "HOUSE0_DIRECTION1_REPORT.md",
    ):
        assert (output / name).is_file(), name
    assert pl.read_csv(output / "pareto_points.csv").height == 36


def test_artifact_checksum_determinism(tmp_path: Path) -> None:
    data_path = tmp_path / "synthetic.csv"
    generate_synthetic(data_path)
    config = load_config(Path("configs/house_0.example.yaml"), data_path)
    config = config.model_copy(
        update={
            "experiment": config.experiment.model_copy(
                update={"modes": ("flat_variance", "true_hierarchical")}
            )
        }
    )
    report = verify_determinism(config)
    assert report["deterministic"] is True
    assert report["artifact_count"] > 0
