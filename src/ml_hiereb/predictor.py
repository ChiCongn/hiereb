"""
Time-Slice Median Predictor.

For each plug, predicts load based on the median value observed at the same
(day_of_week, time-of-day bin) in the training data.

Why time-slice median (not mean)?
  - Robust to outliers (AC spikes, anomalies)
  - Captures daily/weekly periodicity in DEBS data
  - Simple to compute, deterministic, reproducible

Training:
  - Call fit() once with 7 days of data
  - Each plug gets a lookup table: (hour, dow) → median value

Inference:
  - predict_batch(plug_uid, start_ts, n) returns n predictions
  - Fallback: if no training data for a (hour, dow) bin → global median

NOT responsible for:
  - Kafka publishing
  - Drift detection
  - Threshold allocation
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import NamedTuple

import numpy as np
import pandas as pd
import structlog

log = structlog.get_logger(__name__)

# Bin key: (hour_of_day [0-23], day_of_week [0=Mon, 6=Sun], bin within hour)
class SliceKey(NamedTuple):
    hour: int
    dow: int
    bin_index: int = 0


@dataclass
class PlugPredictor:
    """
    Prediction model for a single plug.

    Stores two lookup tables:
      - slice_medians: (hour, dow, bin_index) → median load (Watts)
      - global_median: fallback when slice has no training data
    """
    plug_uid: int
    bin_seconds: int = 3600
    slice_medians: dict[SliceKey, float] = field(default_factory=dict)
    global_median: float = 0.0
    n_training_samples: int = 0

    def predict(self, timestamp_unix: int) -> float:
        """
        Return predicted load (Watts) for this unix timestamp.

        Falls back to global_median if the (hour, dow) bin has no data.
        """
        dt = pd.Timestamp(timestamp_unix, unit="s", tz="UTC")
        bin_index = ((dt.minute * 60) + dt.second) // self.bin_seconds
        key = SliceKey(hour=dt.hour, dow=dt.dayofweek, bin_index=bin_index)
        return self.slice_medians.get(key, self.global_median)


class TimeSlicePredictor:
    """
    Fits and serves time-slice median predictions for all plugs.

    Usage:
        predictor = TimeSlicePredictor()
        predictor.fit(training_df)            # once at startup
        preds = predictor.predict_batch(uid, start_ts, n=300)
    """

    def __init__(self, bin_seconds: int = 3600) -> None:
        if bin_seconds <= 0 or bin_seconds > 3600:
            raise ValueError("bin_seconds must be in range 1..3600")
        self._bin_seconds = bin_seconds
        self._plug_models: dict[int, PlugPredictor] = {}

    def fit(self, df: pd.DataFrame) -> None:
        """
        Build per-plug time-slice lookup tables from training data.

        Args:
            df: DataFrame with columns [timestamp, value, plug_uid].
                Expected to contain only DEBS property=1 rows (load, Watts).
                Timestamps are unix integers.

        Raises:
            ValueError: if required columns are missing.
        """
        required = {"timestamp", "value", "plug_uid"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Training DataFrame missing columns: {missing}")

        if df.empty:
            raise ValueError("Training DataFrame is empty")

        df = df.copy()
        df["_dt"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df["_hour"] = df["_dt"].dt.hour
        df["_dow"] = df["_dt"].dt.dayofweek
        df["_bin"] = (
            (df["_dt"].dt.minute * 60 + df["_dt"].dt.second) // self._bin_seconds
        )

        plug_count = df["plug_uid"].nunique()
        log.info("predictor_fitting", plug_count=plug_count, training_rows=len(df))

        for plug_uid, plug_df in df.groupby("plug_uid"):
            # Global median for this plug (fallback)
            global_med = float(plug_df["value"].median())

            # Per-slice medians
            slice_stats = (
                plug_df.groupby(["_hour", "_dow", "_bin"])["value"]
                .median()
            )

            slice_medians: dict[SliceKey, float] = {
                SliceKey(hour=int(h), dow=int(d), bin_index=int(b)): float(v)
                for (h, d, b), v in slice_stats.items()
            }

            self._plug_models[int(plug_uid)] = PlugPredictor(
                plug_uid=int(plug_uid),
                bin_seconds=self._bin_seconds,
                slice_medians=slice_medians,
                global_median=global_med,
                n_training_samples=len(plug_df),
            )

        log.info(
            "predictor_fitted",
            plug_count=len(self._plug_models),
            bin_seconds=self._bin_seconds,
            slice_bins_total=sum(len(m.slice_medians) for m in self._plug_models.values()),
        )

    def predict_batch(
        self,
        plug_uid: int,
        start_ts: int,
        n: int,
    ) -> dict[int, float]:
        """
        Return predictions for n consecutive 1-second timestamps starting at start_ts.

        Args:
            plug_uid:  globally unique plug identifier
            start_ts:  unix timestamp of the first prediction
            n:         number of predictions (one per second)

        Returns:
            dict mapping timestamp → predicted load (Watts)
            Returns {ts: 0.0} for unknown plug (not seen in training).
        """
        if plug_uid not in self._plug_models:
            log.warning("predict_unknown_plug", plug_uid=plug_uid)
            return {start_ts + i: 0.0 for i in range(n)}

        model = self._plug_models[plug_uid]
        result: dict[int, float] = {}

        for i in range(n):
            ts = start_ts + i
            result[ts] = model.predict(ts)

        return result

    def predict_single(self, plug_uid: int, timestamp: int) -> float:
        """Convenience: predict one timestamp for one plug."""
        if plug_uid not in self._plug_models:
            return 0.0
        return self._plug_models[plug_uid].predict(timestamp)

    def known_plug_uids(self) -> list[int]:
        return list(self._plug_models.keys())

    def is_fitted(self) -> bool:
        return len(self._plug_models) > 0

    def plug_global_median(self, plug_uid: int) -> float:
        """Return global median for a plug. Used by allocator for sigma initialisation."""
        if plug_uid not in self._plug_models:
            return 0.0
        return self._plug_models[plug_uid].global_median
