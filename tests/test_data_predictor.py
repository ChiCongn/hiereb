from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from hiereb.config import AppConfig
from hiereb.data.loader import load_events
from hiereb.data.splits import mean_event_aligned_house_load, split_events
from hiereb.domain.models import Event
from hiereb.predictor.slot_median import TimeSliceMedianPredictor


def _event(ts: str, value: float, household: int = 0, plug: int = 0, idx: int = 0) -> Event:
    return Event(
        timestamp=datetime.fromisoformat(ts.replace("Z", "+00:00")),
        value=value,
        property_value=1,
        plug_id=plug,
        household_id=household,
        house_id=0,
        original_row_index=idx,
        event_id=str(idx),
    )


def test_loader_filters_and_stable_sorts(tmp_path: Path, config: AppConfig) -> None:
    path = tmp_path / "events.csv"
    pl.DataFrame(
        {
            "timestamp": [
                1378015200,
                1377993600,
                1377993600,
                1377993600,
                1377993600,
                1377993600,
            ],
            "value": [3.0, 2.0, 1.0, -1.0, 5.0, 6.0],
            "property": [1, 1, 1, 1, 2, 1],
            "plug_id": [2, 2, 1, 3, 4, 5],
            "household_id": [1, 1, 1, 1, 1, 1],
            "house_id": [0, 0, 0, 0, 0, 9],
        }
    ).write_csv(path)
    loaded = load_events(config.data.model_copy(update={"path": path}), config.splits)
    assert [(e.timestamp, e.plug_id) for e in loaded.events] == [
        (datetime(2013, 9, 1, tzinfo=UTC), 1),
        (datetime(2013, 9, 1, tzinfo=UTC), 2),
        (datetime(2013, 9, 1, 6, tzinfo=UTC), 2),
    ]
    assert loaded.filter_counts == {
        "input_rows": 6,
        "wrong_house": 1,
        "wrong_property": 1,
        "invalid_value": 1,
        "outside_experiment_range": 0,
        "accepted_rows": 3,
    }


def test_split_and_budget_are_event_aligned(config: AppConfig) -> None:
    events = (
        _event("2013-09-01T00:00:00Z", 1.0, plug=0),
        _event("2013-09-01T00:00:00Z", 2.0, plug=1),
        _event("2013-09-01T00:05:00Z", 5.0),
        _event("2013-09-01T06:00:00Z", 9.0),
    )
    split = split_events(events, config.splits)
    assert len(split.warmup) == 3
    assert len(split.evaluation) == 1
    assert mean_event_aligned_house_load(split.warmup) == 4.0


def test_predictor_slot_global_missing_and_no_leakage() -> None:
    warmup = (
        _event("2013-09-01T00:00:00Z", 1.0),
        _event("2013-09-01T00:01:00Z", 3.0),
        _event("2013-09-01T01:00:00Z", 9.0),
    )
    predictor = TimeSliceMedianPredictor(300)
    predictor.fit(warmup)
    matching = datetime.fromisoformat("2013-09-08T00:02:00+00:00")
    other_slot = datetime.fromisoformat("2013-09-08T02:00:00+00:00")
    assert predictor.predict((0, 0), matching) == 2.0
    assert predictor.predict((0, 0), other_slot) == 3.0
    assert predictor.predict((0, 99), matching) is None
    before = predictor.predict((0, 0), matching)
    _ = replace(warmup[0], value=1_000_000.0)
    assert predictor.predict((0, 0), matching) == before


def test_empty_warmup_fails(config: AppConfig) -> None:
    with pytest.raises(ValueError, match="warm-up"):
        split_events((_event("2013-09-01T06:00:00Z", 1.0),), config.splits)


def test_parquet_input(tmp_path: Path, config: AppConfig) -> None:
    path = tmp_path / "events.parquet"
    pl.DataFrame(
        {
            "timestamp": [1377993600, 1378015200],
            "value": [1.0, 2.0],
            "property": [1, 1],
            "plug_id": [0, 0],
            "household_id": [7, 7],
            "house_id": [3, 3],
        }
    ).write_parquet(path)
    data_config = config.data.model_copy(update={"path": path, "format": "parquet", "house_id": 3})
    loaded = load_events(data_config, config.splits)
    assert len(loaded.events) == 2
    assert all(event.house_id == 3 for event in loaded.events)
