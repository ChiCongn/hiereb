"""
Unit tests for src/simulator/plug_state.py
Run: pytest tests/test_plug_state.py -v
"""
from __future__ import annotations

import math
import pytest

from src.simulator.plug_state import HouseState, PlugState


def make_plug(plug_uid: int = 1, house_id: int = 0, household_id: int = 0, plug_id: int = 1) -> PlugState:
    return PlugState(
        plug_uid=plug_uid,
        house_id=house_id,
        household_id=household_id,
        plug_id=plug_id,
    )


# ====================== PlugState Tests ======================

def test_default_delta_is_inf():
    plug = make_plug()
    assert plug.delta == math.inf


def test_full_tx_mode_always_transmits():
    """In Week 1 full_tx mode (delta=inf), should always transmit."""
    plug = make_plug()
    plug.update_predictions({1000: 100.0})
    assert plug.should_transmit(actual=100.0, timestamp=1000) is True
    assert plug.should_transmit(actual=9999.0, timestamp=1000) is True


def test_no_prediction_uses_zero_before_first_observation():
    plug = make_plug()
    plug.set_delta(10.0)
    assert plug.should_transmit(actual=5.0, timestamp=9999) is False   # |5-0| = 5 < 10
    assert plug.should_transmit(actual=15.0, timestamp=9999) is True   # |15-0| = 15 > 10


def test_missing_prediction_uses_last_observed_value():
    plug = make_plug()
    plug.observe(42.0, 1000)
    assert plug.get_prediction(1001) == pytest.approx(42.0)


def test_suppression_boundary_exact_delta_suppresses():
    """Critical: |diff| == delta must suppress (not transmit)"""
    plug = make_plug()
    plug.set_delta(10.0)
    plug.update_predictions({1000: 100.0})

    assert plug.should_transmit(110.0, 1000) is False   # exactly 10 → suppress
    assert plug.should_transmit(110.01, 1000) is True   # 10.01 > 10 → transmit
    assert plug.should_transmit(89.99, 1000) is True    # negative side


def test_update_predictions_merges_batches():
    plug = make_plug()
    plug.update_predictions({1000: 50.0, 1001: 55.0})
    plug.update_predictions({2000: 60.0})

    assert plug.get_prediction(1000) == 50.0
    assert plug.get_prediction(2000) == 60.0


def test_observe_prunes_predictions_far_from_current_timestamp():
    plug = make_plug()
    plug.prediction_cache_max_seconds = 10
    plug.update_predictions({1000: 50.0, 1010: 55.0, 2000: 60.0})
    plug.observe(42.0, 1005)

    assert plug.get_prediction(1000) == 50.0
    assert plug.get_prediction(1010) == 55.0
    assert 2000 not in plug.predictions


def test_set_delta_negative_raises():
    plug = make_plug()
    with pytest.raises(ValueError, match="non-negative"):
        plug.set_delta(-1.0)


def test_set_delta_zero_behavior():
    plug = make_plug()
    plug.set_delta(0.0)
    plug.update_predictions({1000: 50.0})
    assert plug.should_transmit(50.0, 1000) is False      # exactly equal → suppress
    assert plug.should_transmit(50.001, 1000) is True


# ====================== HouseState Tests ======================

def test_house_creates_and_stores_plug():
    house = HouseState(house_id=0)
    plug = house.get_or_create_plug(plug_uid=101, household_id=0, plug_id=1)
    assert plug.plug_uid == 101
    assert house.plug_count == 1


def test_house_returns_same_plug_instance():
    house = HouseState(house_id=0)
    p1 = house.get_or_create_plug(101, 0, 1)
    p2 = house.get_or_create_plug(101, 0, 1)
    assert p1 is p2


def test_house_update_deltas():
    house = HouseState(house_id=0)
    house.get_or_create_plug(101, 0, 1)
    house.get_or_create_plug(102, 0, 2)

    house.update_deltas({101: 15.0, 102: 5.0})

    assert house.plugs[101].delta == 15.0
    assert house.plugs[102].delta == 5.0


def test_house_update_deltas_ignores_unknown_plug():
    house = HouseState(house_id=0)
    house.get_or_create_plug(101, 0, 1)
    # Should not raise error
    house.update_deltas({101: 8.0, 9999: 20.0})
    assert house.plugs[101].delta == 8.0
