"""
Unit tests for src/aggregator/main.py

Tests the aggregation logic (TimestepState.to_metric) in isolation.
No Kafka or DB required.

Run: pytest tests/test_aggregator.py -v
"""
from __future__ import annotations

import pytest

from src.aggregator.main import PlugSnapshot, TimestepState


def make_state(house_id: int = 1, timestamp: int = 1000) -> TimestepState:
    return TimestepState(house_id=house_id, timestamp=timestamp)


def make_snap(
    plug_uid: int = 1,
    value: float | None = 100.0,
    predicted: float = 95.0,
    transmitted: bool = True,
) -> PlugSnapshot:
    return PlugSnapshot(
        plug_uid=plug_uid,
        household_id=0,
        plug_id=plug_uid,
        value=value,
        predicted=predicted,
        transmitted=transmitted,
    )


# ─── full_tx mode ─────────────────────────────────────────────────────────────

def test_all_transmitted_tr_is_one():
    state = make_state()
    state.add_plug(make_snap(plug_uid=1, value=100.0, predicted=95.0, transmitted=True))
    state.add_plug(make_snap(plug_uid=2, value=200.0, predicted=190.0, transmitted=True))

    record = state.to_metric()

    assert record.tr == pytest.approx(1.0)
    assert record.transmitted_count == 2
    assert record.plug_count == 2


def test_all_transmitted_actual_load():
    state = make_state()
    state.add_plug(make_snap(plug_uid=1, value=100.0, predicted=95.0, transmitted=True))
    state.add_plug(make_snap(plug_uid=2, value=200.0, predicted=190.0, transmitted=True))

    record = state.to_metric()

    assert record.actual_load == pytest.approx(300.0)
    assert record.pred_load == pytest.approx(285.0)
    assert record.e_h == pytest.approx(15.0)


# ─── suppression mode ─────────────────────────────────────────────────────────

def test_suppressed_plug_uses_predicted_as_actual():
    """
    Invariant: suppressed plug contributes predicted to both actual_load and pred_load.
    → e_h contribution from suppressed plug = 0.
    """
    state = make_state()
    # Transmitted plug
    state.add_plug(make_snap(plug_uid=1, value=100.0, predicted=95.0, transmitted=True))
    # Suppressed plug: value=None, use predicted
    state.add_plug(make_snap(plug_uid=2, value=None, predicted=80.0, transmitted=False))

    record = state.to_metric()

    # actual_load = 100 (transmitted) + 80 (predicted for suppressed)
    assert record.actual_load == pytest.approx(180.0)
    # pred_load = 95 (transmitted prediction) + 80 (suppressed prediction)
    assert record.pred_load == pytest.approx(175.0)
    # e_h comes only from the transmitted plug
    assert record.e_h == pytest.approx(5.0)


def test_all_suppressed_e_h_is_zero():
    """If all plugs suppressed, e_h = 0 (we have no information about actual error)."""
    state = make_state()
    state.add_plug(make_snap(plug_uid=1, value=None, predicted=100.0, transmitted=False))
    state.add_plug(make_snap(plug_uid=2, value=None, predicted=200.0, transmitted=False))

    record = state.to_metric()

    assert record.e_h == pytest.approx(0.0)
    assert record.tr == pytest.approx(0.0)
    assert record.transmitted_count == 0


def test_mixed_tr_calculation():
    state = make_state()
    state.add_plug(make_snap(plug_uid=1, transmitted=True))
    state.add_plug(make_snap(plug_uid=2, transmitted=True))
    state.add_plug(make_snap(plug_uid=3, value=None, transmitted=False))
    state.add_plug(make_snap(plug_uid=4, value=None, transmitted=False))

    record = state.to_metric()

    assert record.tr == pytest.approx(0.5)
    assert record.transmitted_count == 2
    assert record.plug_count == 4


# ─── Edge cases ───────────────────────────────────────────────────────────────

def test_empty_state_tr_is_zero():
    state = make_state()
    record = state.to_metric()
    assert record.tr == pytest.approx(0.0)
    assert record.plug_count == 0
    assert record.actual_load == pytest.approx(0.0)


def test_single_plug_transmitted():
    state = make_state()
    state.add_plug(make_snap(plug_uid=1, value=42.5, predicted=40.0, transmitted=True))
    record = state.to_metric()
    assert record.actual_load == pytest.approx(42.5)
    assert record.e_h == pytest.approx(2.5)
    assert record.tr == pytest.approx(1.0)


def test_record_has_correct_house_id_and_timestamp():
    state = make_state(house_id=5, timestamp=99999)
    state.add_plug(make_snap())
    record = state.to_metric()
    assert record.house_id == 5
    assert record.timestamp_unix == 99999