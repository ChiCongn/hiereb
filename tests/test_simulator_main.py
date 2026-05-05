"""
Unit tests for src/simulator/main.py message construction.

Run: pytest tests/test_simulator_main.py -v
"""
from __future__ import annotations

import json

import pytest

from src.simulator.loader import PlugReading, TimestepBatch, make_plug_uid
from src.simulator.main import build_kafka_message
from src.simulator.plug_state import HouseState
from src.simulator.stats import SimStats


def make_batch(value: float = 105.0) -> TimestepBatch:
    return TimestepBatch(
        house_id=1,
        timestamp=1000,
        readings=(
            PlugReading(
                plug_uid=make_plug_uid(1, 2, 3),
                household_id=2,
                plug_id=3,
                timestamp=1000,
                value=value,
            ),
        ),
    )


def test_build_kafka_message_full_tx_includes_plug_metadata():
    house_state = HouseState(house_id=1)
    stats = SimStats()

    payload, variance_updates = build_kafka_message(
        make_batch(),
        house_state,
        stats,
        mode="full_tx",
    )

    data = json.loads(payload)
    plug = data["plugs"][0]

    assert variance_updates == []
    assert plug["household_id"] == 2
    assert plug["plug_id"] == 3
    assert plug["value"] == pytest.approx(105.0)
    assert plug["transmitted"] is True
    assert stats.overall_tr == pytest.approx(1.0)


def test_build_kafka_message_uniform_suppresses_within_delta():
    house_state = HouseState(house_id=1)
    plug_uid = make_plug_uid(1, 2, 3)
    plug = house_state.get_or_create_plug(plug_uid, household_id=2, plug_id=3)
    plug.update_predictions({1000: 100.0})
    stats = SimStats()

    payload, variance_updates = build_kafka_message(
        make_batch(value=105.0),
        house_state,
        stats,
        mode="uniform",
        uniform_delta=10.0,
    )

    data = json.loads(payload)
    plug_msg = data["plugs"][0]

    assert plug_msg["value"] is None
    assert plug_msg["predicted"] == pytest.approx(100.0)
    assert plug_msg["transmitted"] is False
    assert variance_updates == [(plug_uid, False, None, 10.0)]
    assert stats.overall_tr == pytest.approx(0.0)
