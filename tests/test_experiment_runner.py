"""Tests for deterministic sweep runner plans."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from config.settings import Settings
from scripts.run_sweep import (
    DEFAULT_EPSILON_RATIO_VALUES,
    REDUCED_EPSILON_RATIO_VALUES,
    build_sweep_plan,
    format_run_id,
)


ROOT_DIR = Path(__file__).resolve().parents[1]


def test_settings_exposes_default_sweep_grids(monkeypatch):
    monkeypatch.delenv("SWEEP_EPSILON_RATIO_VALUES", raising=False)
    monkeypatch.delenv("SWEEP_REDUCED_EPSILON_RATIO_VALUES", raising=False)

    cfg = Settings(_env_file=None)

    assert cfg.SWEEP_EPSILON_RATIO_VALUES == [0.01, 0.02, 0.05, 0.10, 0.20]
    assert cfg.SWEEP_REDUCED_EPSILON_RATIO_VALUES == [0.02, 0.05, 0.10]


def test_run_id_format_matches_epsilon_ratio_x100_contract():
    assert format_run_id("sweep01", "hiereb", 0, 0.05) == "sweep01_hiereb_house0_eps005"
    assert format_run_id("sweep01", "uniform", 0, 0.10) == "sweep01_uniform_house0_eps010"
    assert format_run_id("sweep01", "full_tx", 0, None) == "sweep01_full_tx_house0"


def test_default_plan_runs_full_tx_once_and_sweeps_two_modes_per_epsilon():
    plan = build_sweep_plan(
        sweep_id="sweep01",
        house_id=0,
        epsilon_ratio_values=DEFAULT_EPSILON_RATIO_VALUES,
    )

    assert len(plan) == 1 + 2 * len(DEFAULT_EPSILON_RATIO_VALUES)
    assert [run.mode for run in plan].count("full_tx") == 1
    assert plan[0].mode == "full_tx"
    assert plan[0].epsilon_ratio is None
    assert plan[0].is_sweep is False
    assert plan[0].sweep_size == 5
    assert plan[0].reduced_sweep is False

    sweep_runs = plan[1:]
    assert {run.mode for run in sweep_runs} == {"uniform", "hiereb"}
    assert {run.epsilon_ratio for run in sweep_runs} == set(DEFAULT_EPSILON_RATIO_VALUES)
    assert all(run.is_sweep for run in sweep_runs)
    assert all(run.sweep_id == "sweep01" for run in plan)


def test_reduced_plan_marks_reduced_sweep_metadata():
    plan = build_sweep_plan(
        sweep_id="sweep_small",
        house_id=2,
        epsilon_ratio_values=REDUCED_EPSILON_RATIO_VALUES,
        reduced_sweep=True,
    )

    assert len(plan) == 1 + 2 * len(REDUCED_EPSILON_RATIO_VALUES)
    assert {run.epsilon_ratio for run in plan[1:]} == set(REDUCED_EPSILON_RATIO_VALUES)
    assert all(run.sweep_size == 3 for run in plan)
    assert all(run.reduced_sweep is True for run in plan)
    assert plan[0].is_sweep is False


def test_dry_run_json_plan_is_deterministic_without_docker():
    cmd = [
        sys.executable,
        "scripts/run_sweep.py",
        "--dry-run",
        "--format",
        "json",
        "--sweep-id",
        "sweep01",
        "--house-id",
        "0",
    ]
    first = subprocess.run(cmd, cwd=ROOT_DIR, text=True, capture_output=True, check=True)
    second = subprocess.run(cmd, cwd=ROOT_DIR, text=True, capture_output=True, check=True)

    assert first.stdout == second.stdout

    payload = json.loads(first.stdout)
    assert payload["epsilon_ratio_values"] == [0.01, 0.02, 0.05, 0.10, 0.20]
    assert payload["sweep_size"] == 5
    assert payload["reduced_sweep"] is False
    assert payload["runs"][0]["run_id"] == "sweep01_full_tx_house0"
    assert payload["runs"][0]["epsilon_ratio"] is None
    assert payload["runs"][0]["is_sweep"] is False
    assert "sweep01_hiereb_house0_eps005" in {run["run_id"] for run in payload["runs"]}
