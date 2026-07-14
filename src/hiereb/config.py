"""Strict YAML configuration models and validation."""

from __future__ import annotations

import os
from datetime import datetime
from itertools import pairwise
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

Mode = Literal[
    "full_tx",
    "uniform",
    "flat_variance",
    "legacy_two_stage",
    "true_hierarchical",
    "true_hierarchical_cap",
]


class StrictModel(BaseModel):
    """Base for fail-fast, typo-resistant configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class ExperimentConfig(StrictModel):
    name: str = Field(min_length=1)
    seed: int = 42
    output_dir: Path
    overwrite: bool = False
    modes: tuple[Mode, ...]

    @model_validator(mode="after")
    def unique_modes(self) -> ExperimentConfig:
        if not self.modes or len(set(self.modes)) != len(self.modes):
            raise ValueError("experiment.modes must be non-empty and unique")
        return self


class ColumnConfig(StrictModel):
    id: str = "id"
    timestamp: str = "timestamp"
    value: str = "value"
    property: str = "property"
    plug_id: str = "plug_id"
    household_id: str = "household_id"
    house_id: str = "house_id"


class StreamingConfig(StrictModel):
    """Bounded-memory input and replay settings."""

    enabled: bool = False
    chunk_rows: int = Field(default=100_000, ge=1)
    staging_dir: Path = Path("data/.hiereb-staging")
    reuse_staging: bool = True


class DataConfig(StrictModel):
    path: Path
    format: Literal["csv", "parquet"]
    house_id: int
    load_property_value: int = 1
    timestamp_unit: Literal["seconds", "milliseconds", "microseconds"] = "seconds"
    timezone: Literal["UTC"] = "UTC"
    columns: ColumnConfig
    streaming: StreamingConfig = Field(default_factory=StreamingConfig)


class IntervalConfig(StrictModel):
    start: datetime
    end: datetime

    @model_validator(mode="after")
    def ordered(self) -> IntervalConfig:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("split timestamps must include a timezone")
        if self.start > self.end:
            raise ValueError("split start must be <= end")
        return self


class ValidationIntervalConfig(StrictModel):
    enabled: bool = False
    start: datetime | None = None
    end: datetime | None = None

    @model_validator(mode="after")
    def coherent(self) -> ValidationIntervalConfig:
        if self.enabled and (self.start is None or self.end is None):
            raise ValueError("enabled validation requires start and end")
        if self.start is not None and self.start.tzinfo is None:
            raise ValueError("validation start must include a timezone")
        if self.end is not None and self.end.tzinfo is None:
            raise ValueError("validation end must include a timezone")
        if self.start is not None and self.end is not None and self.start > self.end:
            raise ValueError("validation start must be <= end")
        return self


class SplitsConfig(StrictModel):
    warmup: IntervalConfig
    validation: ValidationIntervalConfig
    evaluation: IntervalConfig

    @model_validator(mode="after")
    def disjoint(self) -> SplitsConfig:
        intervals = [("warmup", self.warmup.start, self.warmup.end)]
        if self.validation.enabled:
            assert self.validation.start is not None and self.validation.end is not None
            intervals.append(("validation", self.validation.start, self.validation.end))
        intervals.append(("evaluation", self.evaluation.start, self.evaluation.end))
        for (_, _start, end), (next_name, next_start, _end) in pairwise(intervals):
            if end >= next_start:
                raise ValueError(f"splits overlap or are unordered before {next_name}")
        return self


class PredictorConfig(StrictModel):
    type: Literal["time_slice_median"]
    slot_seconds: int = Field(gt=0, le=86400)


class BudgetConfig(StrictModel):
    epsilon_ratio: float = Field(ge=0.0)


class ReplayConfig(StrictModel):
    allocation_period_seconds: int = Field(gt=0)
    active_window_seconds: int = Field(gt=0)
    float_tolerance: float = Field(gt=0.0)
    top_k_outliers: int = Field(gt=0)


class ResidualConfig(StrictModel):
    estimator: Literal["exact", "uniform_proxy"]
    rolling_window_size: int = Field(gt=0)
    min_samples: int = Field(ge=1)
    sigma_floor_percentile: float = Field(ge=0.0, le=100.0)


class HouseholdScoreConfig(StrictModel):
    lambda_mean: float = Field(ge=0.0)
    lambda_max: float = Field(ge=0.0)
    lambda_count: float = Field(ge=0.0)

    @model_validator(mode="after")
    def normalized(self) -> HouseholdScoreConfig:
        total = self.lambda_mean + self.lambda_max + self.lambda_count
        if abs(total - 1.0) > 1e-9:
            raise ValueError("household score lambdas must sum to 1")
        return self


class TrueHierarchyConfig(StrictModel):
    household_fairness_alpha: float = Field(ge=0.0, le=1.0)
    plug_fairness_beta: float = Field(ge=0.0, le=1.0)
    household_score: HouseholdScoreConfig


class CapConfig(StrictModel):
    enabled: bool
    quantile: float = Field(ge=0.0, le=1.0)
    multiplier: float = Field(ge=0.0)
    max_house_budget_fraction: float = Field(ge=0.0, le=1.0)
    min_samples: int = Field(ge=1)
    redistribution_tolerance: float = Field(gt=0.0)
    max_iterations_extra: int = Field(ge=0)


class ArtifactsConfig(StrictModel):
    write_threshold_trace: bool = True
    write_plug_metrics: bool = True
    write_household_metrics: bool = True
    write_plots: bool = True


class AppConfig(StrictModel):
    experiment: ExperimentConfig
    data: DataConfig
    splits: SplitsConfig
    predictor: PredictorConfig
    budget: BudgetConfig
    replay: ReplayConfig
    residual: ResidualConfig
    true_hierarchy: TrueHierarchyConfig
    cap: CapConfig
    artifacts: ArtifactsConfig

    @model_validator(mode="after")
    def mode_consistency(self) -> AppConfig:
        if "true_hierarchical_cap" in self.experiment.modes and not self.cap.enabled:
            raise ValueError("true_hierarchical_cap requires cap.enabled=true")
        return self


def load_config(path: Path, data_path: Path | None = None) -> AppConfig:
    """Load a strict configuration from YAML."""
    if not path.is_file():
        raise FileNotFoundError(f"config does not exist: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("config root must be a mapping")
    config = AppConfig.model_validate(raw)
    override = data_path or (Path(value) if (value := os.getenv("HIEREB_DATA_PATH")) else None)
    if override is None:
        return config
    return config.model_copy(update={"data": config.data.model_copy(update={"path": override})})
