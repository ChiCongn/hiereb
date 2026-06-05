"""
Unit tests for src/aggregator/main.py

Tests the aggregation logic (TimestepState.to_metric) in isolation.
No Kafka or DB required.

Run: pytest tests/test_aggregator.py -v
"""
from __future__ import annotations

import pytest

from src.aggregator.main import PlugSnapshot, TimestepState, _snapshot_from_message


def make_state(house_id: int = 1, timestamp: int = 1000) -> TimestepState:
    return TimestepState(house_id=house_id, timestamp=timestamp)


def make_snap(
    plug_uid: int = 1,
    actual_load: float = 100.0,
    predicted_load: float | None = 95.0,
    reconstructed_load: float | None = None,
    transmitted: bool = True,
    reason: str = "normal",
) -> PlugSnapshot:
    if reconstructed_load is None:
        reconstructed_load = actual_load if transmitted else predicted_load
    assert reconstructed_load is not None
    return PlugSnapshot(
        plug_uid=plug_uid,
        household_id=0,
        plug_id=plug_uid,
        actual_load=actual_load,
        predicted_load=predicted_load,
        reconstructed_load=reconstructed_load,
        transmitted=transmitted,
        decision="transmit" if transmitted else "suppress",
        reason=reason,
        plug_status="active",
        is_forced_transmit=reason != "normal",
    )


# ─── full_tx mode ─────────────────────────────────────────────────────────────

def test_all_transmitted_tr_is_one():
    state = make_state()
    state.add_plug(make_snap(plug_uid=1, actual_load=100.0, predicted_load=95.0, transmitted=True))
    state.add_plug(make_snap(plug_uid=2, actual_load=200.0, predicted_load=190.0, transmitted=True))

    record = state.to_metric()

    assert record.tr == pytest.approx(1.0)
    assert record.transmitted_count == 2
    assert record.plug_count == 2


def test_full_tx_reconstruction_error_is_zero():
    state = make_state()
    state.add_plug(make_snap(plug_uid=1, actual_load=100.0, predicted_load=95.0, transmitted=True))
    state.add_plug(make_snap(plug_uid=2, actual_load=200.0, predicted_load=190.0, transmitted=True))

    record = state.to_metric()

    assert record.actual_load == pytest.approx(300.0)
    assert record.pred_load == pytest.approx(285.0)
    assert record.reconstructed_load == pytest.approx(300.0)
    assert record.e_h == pytest.approx(0.0)


# ─── suppression mode ─────────────────────────────────────────────────────────

def test_suppressed_plug_uses_prediction_as_reconstruction_not_actual():
    state = make_state()
    state.add_plug(make_snap(plug_uid=1, actual_load=100.0, predicted_load=95.0, transmitted=True))
    state.add_plug(make_snap(plug_uid=2, actual_load=90.0, predicted_load=80.0, transmitted=False))

    record = state.to_metric()

    # actual_load keeps the true observed value for the suppressed event.
    assert record.actual_load == pytest.approx(190.0)
    # pred_load = 95 (transmitted prediction) + 80 (suppressed prediction)
    assert record.pred_load == pytest.approx(175.0)
    # reconstructed_load = actual for transmitted + prediction for suppressed.
    assert record.reconstructed_load == pytest.approx(180.0)
    assert record.e_h == pytest.approx(10.0)


def test_all_suppressed_actual_not_equal_prediction_has_nonzero_error():
    state = make_state()
    state.add_plug(make_snap(plug_uid=1, actual_load=110.0, predicted_load=100.0, transmitted=False))
    state.add_plug(make_snap(plug_uid=2, actual_load=180.0, predicted_load=200.0, transmitted=False))

    record = state.to_metric()

    assert record.actual_load == pytest.approx(290.0)
    assert record.reconstructed_load == pytest.approx(300.0)
    assert record.e_h == pytest.approx(-10.0)
    assert record.tr == pytest.approx(0.0)
    assert record.transmitted_count == 0


def test_mixed_transmit_suppress_house_error_is_sum_actual_minus_reconstructed():
    state = make_state()
    state.add_plug(make_snap(plug_uid=1, actual_load=100.0, predicted_load=90.0, transmitted=True))
    state.add_plug(make_snap(plug_uid=2, actual_load=50.0, predicted_load=45.0, transmitted=False))
    state.add_plug(make_snap(plug_uid=3, actual_load=20.0, predicted_load=None, transmitted=True))

    record = state.to_metric()

    assert record.actual_load == pytest.approx(170.0)
    assert record.reconstructed_load == pytest.approx(165.0)
    assert record.pred_load == pytest.approx(135.0)
    assert record.e_h == pytest.approx(5.0)


def test_mixed_tr_calculation():
    state = make_state()
    state.add_plug(make_snap(plug_uid=1, transmitted=True))
    state.add_plug(make_snap(plug_uid=2, transmitted=True))
    state.add_plug(make_snap(plug_uid=3, actual_load=100.0, predicted_load=95.0, transmitted=False))
    state.add_plug(make_snap(plug_uid=4, actual_load=100.0, predicted_load=95.0, transmitted=False))

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
    assert record.reconstructed_load == pytest.approx(0.0)


def test_single_plug_transmitted():
    state = make_state()
    state.add_plug(make_snap(plug_uid=1, actual_load=42.5, predicted_load=40.0, transmitted=True))
    record = state.to_metric()
    assert record.actual_load == pytest.approx(42.5)
    assert record.reconstructed_load == pytest.approx(42.5)
    assert record.e_h == pytest.approx(0.0)
    assert record.tr == pytest.approx(1.0)


def test_record_has_correct_house_id_and_timestamp():
    state = make_state(house_id=5, timestamp=99999)
    state.add_plug(make_snap())
    record = state.to_metric()
    assert record.house_id == 5
    assert record.timestamp_unix == 99999


def test_snapshot_from_message_keeps_event_decision_fields():
    snap = _snapshot_from_message({
        "plug_uid": 1,
        "household_id": 2,
        "plug_id": 3,
        "actual_load": 105.0,
        "predicted_load": 100.0,
        "reconstructed_load": 100.0,
        "transmitted": False,
        "decision": "suppress",
        "reason": "normal",
        "plug_status": "active",
        "is_forced_transmit": False,
    })

    assert snap.actual_load == pytest.approx(105.0)
    assert snap.predicted_load == pytest.approx(100.0)
    assert snap.reconstructed_load == pytest.approx(100.0)
    assert snap.decision == "suppress"
    assert snap.reason == "normal"
    assert snap.plug_status == "active"
    assert snap.is_forced_transmit is False


def test_snapshot_from_message_rejects_suppressed_legacy_without_actual():
    with pytest.raises(ValueError, match="actual_load is required"):
        _snapshot_from_message({
            "plug_uid": 1,
            "household_id": 2,
            "plug_id": 3,
            "value": None,
            "predicted": 100.0,
            "transmitted": False,
        })
