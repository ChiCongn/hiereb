"""
Unit tests for src/simulator/main.py message construction.

Run: pytest tests/test_simulator_main.py -v
"""
from __future__ import annotations

import json

import pytest

from src.simulator.loader import PlugReading, TimestepBatch, make_plug_uid
from src.simulator.main import build_kafka_message, resolve_stream_timestamp
from src.simulator.plug_state import HouseState
from src.simulator.stats import SimStats


def make_batch(value: float = 105.0) -> TimestepBatch:
    return TimestepBatch(
        house_id=1,
        timestamp=1000,
        readings=(
            PlugReading(
                source_id=1,
                property=1,
                plug_uid=make_plug_uid(1, 2, 3),
                household_id=2,
                plug_id=3,
                timestamp=1000,
                value=value,
            ),
        ),
    )


def make_multi_batch(timestamp: int, values: dict[tuple[int, int], float]) -> TimestepBatch:
    return TimestepBatch(
        house_id=1,
        timestamp=timestamp,
        readings=tuple(
            PlugReading(
                source_id=index,
                property=1,
                plug_uid=make_plug_uid(1, household_id, plug_id),
                household_id=household_id,
                plug_id=plug_id,
                timestamp=timestamp,
                value=value,
            )
            for index, ((household_id, plug_id), value) in enumerate(
                values.items(),
                start=1,
            )
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
    assert data["timestamp"] == 1000
    assert data["source_timestamp"] == 1000
    assert plug["household_id"] == 2
    assert plug["plug_id"] == 3
    assert plug["value"] == pytest.approx(105.0)
    assert plug["actual_load"] == pytest.approx(105.0)
    assert plug["predicted_load"] is None
    assert plug["reconstructed_load"] == pytest.approx(105.0)
    assert plug["decision"] == "transmit"
    assert plug["reason"] == "normal"
    assert plug["plug_status"] == "active"
    assert plug["is_forced_transmit"] is False
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
    assert plug_msg["actual_load"] == pytest.approx(105.0)
    assert plug_msg["predicted"] == pytest.approx(100.0)
    assert plug_msg["predicted_load"] == pytest.approx(100.0)
    assert plug_msg["reconstructed_load"] == pytest.approx(100.0)
    assert plug_msg["residual"] == pytest.approx(5.0)
    assert plug_msg["decision"] == "suppress"
    assert plug_msg["reason"] == "normal"
    assert plug_msg["plug_status"] == "active"
    assert plug_msg["is_forced_transmit"] is False
    assert plug_msg["transmitted"] is False
    assert variance_updates == [(plug_uid, False, None, 10.0)]
    assert stats.overall_tr == pytest.approx(0.0)


def test_build_kafka_message_uniform_delta_uses_house_budget_active_count():
    house_state = HouseState(house_id=1)
    p1 = house_state.get_or_create_plug(make_plug_uid(1, 2, 1), 2, 1)
    p2 = house_state.get_or_create_plug(make_plug_uid(1, 2, 2), 2, 2)
    p1.update_predictions({1000: 100.0})
    p2.update_predictions({1000: 100.0})
    stats = SimStats()

    payload, variance_updates = build_kafka_message(
        make_multi_batch(1000, {(2, 1): 140.0, (2, 2): 130.0}),
        house_state,
        stats,
        mode="uniform",
        uniform_delta=100.0,
    )

    data = json.loads(payload)

    assert house_state.uniform_active_count == 2
    assert house_state.uniform_delta_per_active == pytest.approx(50.0)
    assert p1.delta == pytest.approx(50.0)
    assert p2.delta == pytest.approx(50.0)
    assert [plug["decision"] for plug in data["plugs"]] == ["suppress", "suppress"]
    assert variance_updates == [
        (p1.plug_uid, False, None, 50.0),
        (p2.plug_uid, False, None, 50.0),
    ]


def test_build_kafka_message_uniform_active_count_changes_at_next_allocation():
    house_state = HouseState(house_id=1)
    p1 = house_state.get_or_create_plug(make_plug_uid(1, 2, 1), 2, 1)
    p2 = house_state.get_or_create_plug(make_plug_uid(1, 2, 2), 2, 2)
    p1.update_predictions({1000: 100.0})
    p2.update_predictions({1000: 100.0})
    stats = SimStats()

    build_kafka_message(
        make_multi_batch(1000, {(2, 1): 140.0, (2, 2): 130.0}),
        house_state,
        stats,
        mode="uniform",
        uniform_delta=100.0,
        uniform_allocation_period_seconds=1,
    )
    p1.update_predictions({4000: 100.0, 4601: 100.0, 4602: 100.0})
    build_kafka_message(
        make_multi_batch(4000, {(2, 1): 140.0}),
        house_state,
        stats,
        mode="uniform",
        uniform_delta=100.0,
        uniform_allocation_period_seconds=1,
    )
    build_kafka_message(
        make_multi_batch(4601, {(2, 1): 140.0}),
        house_state,
        stats,
        mode="uniform",
        uniform_delta=100.0,
        uniform_allocation_period_seconds=1,
    )
    payload, variance_updates = build_kafka_message(
        make_multi_batch(4602, {(2, 1): 180.0}),
        house_state,
        stats,
        mode="uniform",
        uniform_delta=100.0,
        uniform_allocation_period_seconds=1,
    )

    data = json.loads(payload)
    plug_msg = data["plugs"][0]

    assert house_state.uniform_active_count == 1
    assert house_state.uniform_delta_per_active == pytest.approx(100.0)
    assert p1.delta == pytest.approx(100.0)
    assert p2.delta == pytest.approx(0.0)
    assert plug_msg["decision"] == "suppress"
    assert plug_msg["residual"] == pytest.approx(80.0)
    assert variance_updates == [(p1.plug_uid, False, None, 100.0)]


def test_build_kafka_message_missing_prediction_forces_transmit():
    house_state = HouseState(house_id=1)
    plug_uid = make_plug_uid(1, 2, 3)
    plug = house_state.get_or_create_plug(plug_uid, household_id=2, plug_id=3)
    plug.observe(105.0, 999)
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

    assert plug_msg["value"] == pytest.approx(105.0)
    assert plug_msg["actual_load"] == pytest.approx(105.0)
    assert plug_msg["predicted"] is None
    assert plug_msg["predicted_load"] is None
    assert plug_msg["reconstructed_load"] == pytest.approx(105.0)
    assert plug_msg["residual"] is None
    assert plug_msg["abs_residual"] is None
    assert plug_msg["decision"] == "transmit"
    assert plug_msg["reason"] == "missing_prediction"
    assert plug_msg["plug_status"] == "active"
    assert plug_msg["is_forced_transmit"] is True
    assert plug_msg["transmitted"] is True
    assert variance_updates == [(plug_uid, True, None, 10.0)]
    assert stats.overall_tr == pytest.approx(1.0)


def test_build_kafka_message_inactive_reactivation_forces_transmit():
    house_state = HouseState(house_id=1)
    plug_uid = make_plug_uid(1, 2, 3)
    plug = house_state.get_or_create_plug(plug_uid, household_id=2, plug_id=3)
    plug.update_predictions({1000: 100.0})
    stats = SimStats()

    build_kafka_message(
        make_batch(value=100.0),
        house_state,
        stats,
        mode="uniform",
        uniform_delta=100.0,
    )
    plug.update_predictions({4601: 100.0})
    payload, variance_updates = build_kafka_message(
        TimestepBatch(
            house_id=1,
            timestamp=4601,
            readings=(
                PlugReading(
                    source_id=2,
                    property=1,
                    plug_uid=plug_uid,
                    household_id=2,
                    plug_id=3,
                    timestamp=4601,
                    value=100.0,
                ),
            ),
        ),
        house_state,
        stats,
        mode="uniform",
        uniform_delta=100.0,
    )

    data = json.loads(payload)
    plug_msg = data["plugs"][0]

    assert plug_msg["value"] == pytest.approx(100.0)
    assert plug_msg["decision"] == "transmit"
    assert plug_msg["reason"] == "inactive_reactivation"
    assert plug_msg["plug_status"] == "reactivated"
    assert plug_msg["is_forced_transmit"] is True
    assert variance_updates == [(plug_uid, True, 0.0, 100.0)]


def test_build_kafka_message_missing_prediction_priority_over_reactivation():
    house_state = HouseState(house_id=1)
    plug_uid = make_plug_uid(1, 2, 3)
    plug = house_state.get_or_create_plug(plug_uid, household_id=2, plug_id=3)
    plug.update_predictions({1000: 100.0})
    stats = SimStats()

    build_kafka_message(
        make_batch(value=100.0),
        house_state,
        stats,
        mode="uniform",
        uniform_delta=100.0,
    )
    payload, variance_updates = build_kafka_message(
        TimestepBatch(
            house_id=1,
            timestamp=4601,
            readings=(
                PlugReading(
                    source_id=2,
                    property=1,
                    plug_uid=plug_uid,
                    household_id=2,
                    plug_id=3,
                    timestamp=4601,
                    value=100.0,
                ),
            ),
        ),
        house_state,
        stats,
        mode="uniform",
        uniform_delta=100.0,
    )

    data = json.loads(payload)
    plug_msg = data["plugs"][0]

    assert plug_msg["value"] == pytest.approx(100.0)
    assert plug_msg["predicted_load"] is None
    assert plug_msg["residual"] is None
    assert plug_msg["decision"] == "transmit"
    assert plug_msg["reason"] == "missing_prediction"
    assert plug_msg["plug_status"] == "reactivated"
    assert plug_msg["is_forced_transmit"] is True
    assert variance_updates == [(plug_uid, True, None, 100.0)]


def test_build_kafka_message_hiereb_threshold_applies_after_allocation_time():
    house_state = HouseState(house_id=1)
    plug_uid = make_plug_uid(1, 2, 3)
    plug = house_state.get_or_create_plug(plug_uid, household_id=2, plug_id=3)
    plug.set_delta(10.0)
    plug.update_predictions({1000: 100.0, 1001: 100.0})
    house_state.stage_hiereb_thresholds(
        {plug_uid: 100.0},
        allocation_time=1000,
        effective_after_time=1000,
        threshold_version=1,
    )
    stats = SimStats()

    payload_at_allocation, updates_at_allocation = build_kafka_message(
        make_batch(value=150.0),
        house_state,
        stats,
        mode="hiereb",
    )
    payload_after_allocation, updates_after_allocation = build_kafka_message(
        TimestepBatch(
            house_id=1,
            timestamp=1001,
            readings=(
                PlugReading(
                    source_id=2,
                    property=1,
                    plug_uid=plug_uid,
                    household_id=2,
                    plug_id=3,
                    timestamp=1001,
                    value=150.0,
                ),
            ),
        ),
        house_state,
        stats,
        mode="hiereb",
    )

    plug_at_allocation = json.loads(payload_at_allocation)["plugs"][0]
    plug_after_allocation = json.loads(payload_after_allocation)["plugs"][0]

    assert plug_at_allocation["decision"] == "transmit"
    assert updates_at_allocation == [(plug_uid, True, 50.0, 10.0)]
    assert plug_after_allocation["decision"] == "suppress"
    assert updates_after_allocation == [(plug_uid, False, None, 100.0)]


def test_build_kafka_message_can_use_wall_clock_timestamp():
    house_state = HouseState(house_id=1)
    stats = SimStats()

    payload, _ = build_kafka_message(
        make_batch(),
        house_state,
        stats,
        mode="full_tx",
        output_timestamp=1_700_000_000.5,
    )

    data = json.loads(payload)
    assert data["timestamp"] == pytest.approx(1_700_000_000.5)
    assert data["source_timestamp"] == 1000


def test_resolve_stream_timestamp_wall_clock_scales_replay_time():
    assert resolve_stream_timestamp(
        1060,
        source_start_timestamp=1000,
        wall_start_timestamp=1_700_000_000.0,
        replay_speed=60,
        stream_time_mode="wall_clock",
    ) == pytest.approx(1_700_000_001.0)


def test_resolve_stream_timestamp_source_mode_passthrough():
    assert resolve_stream_timestamp(
        1060,
        source_start_timestamp=1000,
        wall_start_timestamp=1_700_000_000.0,
        replay_speed=60,
        stream_time_mode="source",
    ) == pytest.approx(1060.0)
