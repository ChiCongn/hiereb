"""
IoT Simulator – entry point (Day 5 - Week 1)
Replay DEBS data at REPLAY_SPEED× into Kafka with full_tx mode.
"""
from __future__ import annotations

import asyncio
import json
import signal
from pathlib import Path
from typing import Any

import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from config.settings import settings
from src.simulator.loader import load_debs, iter_timestep_batches, TimestepBatch
from src.simulator.plug_state import HouseState
from src.simulator.stats import SimStats

log = structlog.get_logger(__name__)


def build_kafka_message(
    batch: TimestepBatch,
    house_state: HouseState,
    stats: SimStats,
) -> bytes:
    """Build one message per house per timestamp - full_tx mode (Week 1)."""
    plug_messages = []

    for reading in batch.readings:
        plug = house_state.get_or_create_plug(
            plug_uid=reading.plug_uid,
            household_id=getattr(reading, 'household_id', 0),
            plug_id=getattr(reading, 'plug_id', 0),
        )

        # Full_tx mode: always transmit
        actual = reading.value
        predicted = plug.get_prediction(reading.timestamp)

        plug_messages.append({
            "plug_uid": reading.plug_uid,
            "plug_id": getattr(reading, 'plug_id', 0),
            "household_id": getattr(reading, 'household_id', 0),
            "value": float(actual),
            "predicted": float(predicted),
            "transmitted": True,
        })

        # Update stats
        error = actual - predicted
        stats.record_transmit(reading.plug_uid, error)

    payload = {
        "house_id": batch.house_id,
        "timestamp": batch.timestamp,
        "plugs": plug_messages,
    }

    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


async def run_simulator() -> None:
    log.info("simulator_starting",
             mode=settings.SUPPRESSION_MODE,
             replay_speed=settings.REPLAY_SPEED,
             house_ids=settings.HOUSE_IDS,
             data_file=settings.DATA_FILE)

    # Load data
    data_file = Path("data") / settings.DATA_FILE
    if not data_file.exists():
        data_file = Path("data/house-1.csv")   # fallback

    df = load_debs(data_file, house_ids=settings.HOUSE_IDS)

    # Per-house state
    house_states = {hid: HouseState(house_id=hid) for hid in settings.HOUSE_IDS}
    stats = SimStats(house_id=0)   # global stats for Week 1

    # Kafka producer
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        linger_ms=5,
        compression_type="gzip",
        acks="all",
    )

    try:
        await producer.start()
        log.info("kafka_producer_started", topic="hiereb.sensor_data")
    except KafkaConnectionError as e:
        log.error("kafka_connection_failed", error=str(e))
        raise

    try:
        messages_sent = 0
        interval = 1.0 / settings.REPLAY_SPEED

        for batch in iter_timestep_batches(df):
            if batch.house_id not in house_states:
                continue

            payload = build_kafka_message(
                batch, house_states[batch.house_id], stats
            )

            await producer.send(
                topic="hiereb.sensor_data",
                key=str(batch.house_id).encode("utf-8"),
                value=payload,
            )

            messages_sent += 1

            if messages_sent % 500 == 0:
                log.info("simulator_progress",
                         messages=messages_sent,
                         current_ts=batch.timestamp,
                         tr=round(stats.transmission_rate(), 4),
                         house_id=batch.house_id)

            await asyncio.sleep(interval)

        log.info("simulation_completed", total_messages=messages_sent, final_tr=round(stats.transmission_rate(), 4))

    finally:
        await producer.flush()
        await producer.stop()
        log.info("kafka_producer_stopped", total_messages=messages_sent)


def main() -> None:
    """Entry point with graceful shutdown."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, loop.stop)

    try:
        loop.run_until_complete(run_simulator())
    except Exception as e:  # noqa: BLE001
        log.error("simulator_crashed", error=str(e))
    finally:
        loop.close()


if __name__ == "__main__":
    main()