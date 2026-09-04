from __future__ import annotations

import json
from pathlib import Path

import pytest

from hiereb.config import AppConfig, Mode, load_config
from hiereb.data.streaming import iter_evaluation_batches, prepare_streaming
from hiereb.data.synthetic import generate_synthetic
from hiereb.experiment import execute_mode, execute_mode_streaming, prepare


def _streaming_config(tmp_path: Path, *, chunk_rows: int = 3) -> AppConfig:
    data_path = tmp_path / "synthetic.csv"
    generate_synthetic(data_path)
    config = load_config(Path("configs/house_0.example.yaml"), data_path)
    streaming = config.data.streaming.model_copy(
        update={
            "enabled": True,
            "chunk_rows": chunk_rows,
            "staging_dir": tmp_path / "staging",
            "reuse_staging": True,
        }
    )
    return config.model_copy(
        update={
            "data": config.data.model_copy(update={"streaming": streaming}),
            "experiment": config.experiment.model_copy(
                update={"output_dir": tmp_path / "outputs", "overwrite": True}
            ),
        }
    )


def test_streaming_batches_are_atomic_and_stably_sorted(tmp_path: Path) -> None:
    config = _streaming_config(tmp_path)
    prepared = prepare_streaming(config)
    batches = list(iter_evaluation_batches(prepared, config))
    assert sum(map(len, batches)) == prepared.evaluation_event_count == 270
    assert any(len(batch) > config.data.streaming.chunk_rows for batch in batches)
    for batch in batches:
        assert len({event.timestamp for event in batch}) == 1
        assert list(batch) == sorted(
            batch,
            key=lambda event: (
                event.timestamp,
                event.household_id,
                event.plug_id,
                event.original_row_index,
            ),
        )


@pytest.mark.parametrize(
    "mode",
    [
        "full_tx",
        "uniform",
        "flat_variance",
        "legacy_two_stage",
        "true_hierarchical",
        "true_hierarchical_cap",
    ],
)
def test_streaming_matches_in_memory(mode: Mode, tmp_path: Path) -> None:
    config = _streaming_config(tmp_path)
    memory_config = config.model_copy(
        update={
            "data": config.data.model_copy(
                update={"streaming": config.data.streaming.model_copy(update={"enabled": False})}
            )
        }
    )
    memory_prepared = prepare(memory_config)
    stream_prepared = prepare_streaming(config)
    memory_result, memory_summary = execute_mode(memory_config, mode, memory_prepared, write=False)
    stream_result, stream_summary = execute_mode_streaming(
        config, mode, stream_prepared, write=False
    )
    for key in (
        "valid_events",
        "timestamp_batches",
        "transmitted",
        "suppressed",
        "rmse",
        "p95",
        "p99",
        "max",
        "cap_hit_count",
        "unused_budget",
        "maximum_consecutive_suppression",
    ):
        assert stream_summary[key] == pytest.approx(memory_summary[key])
    assert [row["threshold"] for row in stream_result.threshold_rows] == pytest.approx(
        [row["threshold"] for row in memory_result.threshold_rows]
    )
    assert not hasattr(stream_result, "event_rows")


def test_streaming_writes_required_artifacts(tmp_path: Path) -> None:
    config = _streaming_config(tmp_path)
    _, summary = execute_mode_streaming(config, "true_hierarchical_cap")
    run_dir = config.experiment.output_dir / "true_hierarchical_cap"
    expected = {
        "resolved_config.yaml",
        "run_metadata.json",
        "summary.json",
        "summary.csv",
        "daily_metrics.csv",
        "household_metrics.csv",
        "plug_metrics.parquet",
        "threshold_trace.parquet",
        "top_outliers.parquet",
        "filter_counts.json",
    }
    assert expected <= {path.name for path in run_dir.iterdir()}
    metadata = json.loads((run_dir / "run_metadata.json").read_text())
    assert metadata["streaming"] is True
    assert metadata["chunk_rows"] == 3
    assert summary["bound_violation_count"] == 0
