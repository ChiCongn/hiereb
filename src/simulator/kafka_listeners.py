"""
Background Kafka consumers for the simulator.

The simulator needs to receive two types of messages from the ML service:
  1. hiereb.predictions → update plug prediction caches
  2. hiereb.thresholds  → update plug delta values

These run as asyncio background tasks alongside the main replay loop.

Also provides a variance publisher helper:
  publish_variance() → sends residuals/censored info to hiereb.variance
  so the ML service can update its sigma estimates.
"""
from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any

import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from config.settings import settings
from src.simulator.loader import decode_plug_uid

if TYPE_CHECKING:
    from src.simulator.plug_state import HouseState

log = structlog.get_logger(__name__)

TOPIC_PREDICTIONS = "hiereb.predictions"
TOPIC_THRESHOLDS = "hiereb.thresholds"
TOPIC_VARIANCE = "hiereb.variance"
CONSUMER_GROUP_PREDICTIONS = "simulator-predictions"
CONSUMER_GROUP_THRESHOLDS = "simulator-thresholds"


async def consume_predictions(
    house_states: dict[int, "HouseState"],
    shutdown: asyncio.Event,
) -> None:
    """
    Consumes hiereb.predictions and updates PlugState.predictions for matching plugs.

    Message format:
      {plug_uid, batch_start, n, predictions: {ts_str: value}, delta}
    """
    consumer = AIOKafkaConsumer(
        TOPIC_PREDICTIONS,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_PREDICTIONS,
        auto_offset_reset="latest",
    )
    await consumer.start()
    log.info("prediction_consumer_started")
    received = 0

    try:
        async for msg in consumer:
            if shutdown.is_set():
                break
            try:
                data: dict[str, Any] = json.loads(msg.value)
                plug_uid: int = data["plug_uid"]
                raw_preds: dict[str, float] = data["predictions"]
                delta: float = data.get("delta", float("inf"))

                # Parse timestamp keys (JSON keys are always strings)
                predictions = {int(ts): val for ts, val in raw_preds.items()}

                # Find which house this plug belongs to and update its state.
                # Predictions can arrive before the simulator has seen the plug
                # in replay, so create the PlugState from the encoded UID.
                for house_state in house_states.values():
                    if plug_uid in house_state.plugs:
                        plug = house_state.plugs[plug_uid]
                        plug.update_predictions(predictions)
                        plug.set_delta(delta)
                        break
                else:
                    house_id, household_id, plug_id = decode_plug_uid(plug_uid)
                    house_state = house_states.get(house_id)
                    if house_state is not None:
                        plug = house_state.get_or_create_plug(plug_uid, household_id, plug_id)
                        plug.update_predictions(predictions)
                        plug.set_delta(delta)
                    else:
                        log.debug("prediction_for_unconfigured_house", plug_uid=plug_uid, house_id=house_id)

                received += 1
                if received % 200 == 0:
                    log.info("predictions_received", count=received)

            except (KeyError, json.JSONDecodeError, ValueError) as exc:
                log.warning("prediction_message_error", error=str(exc))

    finally:
        await consumer.stop()
        log.info("prediction_consumer_stopped", total_received=received)


async def consume_thresholds(
    house_states: dict[int, "HouseState"],
    shutdown: asyncio.Event,
) -> None:
    """
    Consumes hiereb.thresholds and updates all plug deltas for the given house.

    Message format:
      {house_id, deltas: {plug_uid_str: delta_float}}
    """
    consumer = AIOKafkaConsumer(
        TOPIC_THRESHOLDS,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_THRESHOLDS,
        auto_offset_reset="latest",
    )
    await consumer.start()
    log.info("threshold_consumer_started")
    received = 0

    try:
        async for msg in consumer:
            if shutdown.is_set():
                break
            try:
                data: dict[str, Any] = json.loads(msg.value)
                house_id: int = data["house_id"]
                raw_deltas: dict[str, float] = data["deltas"]

                # Convert string keys back to int
                new_deltas = {int(uid): delta for uid, delta in raw_deltas.items()}

                if house_id in house_states:
                    house_states[house_id].update_deltas(new_deltas, create_missing=True)
                    log.debug(
                        "thresholds_applied",
                        house_id=house_id,
                        plug_count=len(new_deltas),
                    )

                received += 1

            except (KeyError, json.JSONDecodeError, ValueError) as exc:
                log.warning("threshold_message_error", error=str(exc))

    finally:
        await consumer.stop()
        log.info("threshold_consumer_stopped", total_received=received)


async def publish_variance_update(
    producer: AIOKafkaProducer,
    plug_uid: int,
    transmitted: bool,
    residual: float | None,
    delta: float,
) -> None:
    """
    Publish one variance observation to hiereb.variance.

    Called by simulator after each transmission decision.

    Args:
        producer:    shared AIOKafkaProducer
        plug_uid:    globally unique plug ID
        transmitted: True if plug transmitted this timestep
        residual:    actual - predicted, or None if suppressed/missing prediction
        delta:       current delta for this plug
    """
    payload = json.dumps({
        "plug_uid": plug_uid,
        "transmitted": transmitted,
        "residual": residual,    # None if suppressed or prediction is missing
        "delta": delta,
    }, separators=(",", ":")).encode()

    await producer.send(
        TOPIC_VARIANCE,
        key=str(plug_uid).encode(),
        value=payload,
    )
