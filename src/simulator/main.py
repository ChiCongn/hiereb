"""
IoT Simulator – entry point.

Reads DEBS CSV, replays at REPLAY_SPEED×, publishes to hiereb.sensor_data.

Message format (1 message per house per timestamp):
{
    "house_id":  int,
    "timestamp": int,                    # event-time (unix seconds from DEBS)
    "plugs": [
        {
            "plug_uid":     int,
            "plug_id":      int,
            "household_id": int,
            "value":        float | null, # null when suppressed
            "predicted":    float,
            "transmitted":  bool
        },
        ...
    ]
}

Week 1: SUPPRESSION_MODE=full_tx → value is always present, transmitted=True.
Week 2+: suppression activates, value=null for suppressed plugs.
"""
from __future__ import annotations

import asyncio
import json
import signal
import time
from pathlib import Path
from typing import Any

import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from config.logging_config import configure_logging
from config.settings import settings
from src.simulator.kafka_listeners import (
    consume_predictions,
    consume_thresholds,
    publish_variance_update,
)
from src.simulator.loader import TimestepBatch, iter_timestep_batches, load_debs
from src.simulator.plug_state import HouseState
from src.simulator.stats import SimStats

log = structlog.get_logger(__name__)

TOPIC_SENSOR_DATA = "hiereb.sensor_data"


# ─── Message builder ──────────────────────────────────────────────────────────

def build_kafka_message(
    batch: TimestepBatch,
    house_state: HouseState,
    stats: SimStats,
    mode: str,
    uniform_delta: float | None = None,
) -> tuple[bytes, list[tuple[int, bool, float | None, float]]]:
    """
    Serialize one timestep batch to JSON bytes for Kafka.

    Returns:
        (payload_bytes, variance_updates)
        variance_updates: list of (plug_uid, transmitted, residual, delta)
        to be published to hiereb.variance by the caller.
    """
    plug_messages: list[dict[str, Any]] = []
    variance_updates: list[tuple[int, bool, float | None, float]] = []

    for reading in batch.readings:
        plug = house_state.get_or_create_plug(
            plug_uid=reading.plug_uid,
            household_id=reading.household_id,
            plug_id=reading.plug_id,
        )

        predicted = plug.get_prediction(reading.timestamp)

        if mode == "full_tx":
            transmitted = True
        elif mode == "uniform":
            if plug.delta == float("inf"):
                plug.set_delta(settings.UNIFORM_DELTA if uniform_delta is None else uniform_delta)
            transmitted = plug.should_transmit(reading.value, reading.timestamp)
        else:
            transmitted = plug.should_transmit(reading.value, reading.timestamp)

        residual = reading.value - predicted if transmitted else None

        if transmitted:
            stats.record_transmit(reading.plug_uid, residual if residual is not None else 0.0)
        else:
            stats.record_suppress(reading.plug_uid)

        # Queue variance update for hiereb mode
        if mode != "full_tx":
            variance_updates.append(
                (reading.plug_uid, transmitted, residual, plug.delta)
            )

        plug_messages.append({
            "plug_uid": reading.plug_uid,
            "plug_id": reading.plug_id,
            "household_id": reading.household_id,
            "value": reading.value if transmitted else None,
            "predicted": predicted,
            "transmitted": transmitted,
        })

    payload = {
        "house_id": batch.house_id,
        "timestamp": batch.timestamp,
        "plugs": plug_messages,
    }

    return (
        json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        variance_updates,
    )


# ─── Main simulation coroutine ────────────────────────────────────────────────

async def run_simulator() -> None:
    configure_logging(settings.LOG_LEVEL)

    log.info(
        "simulator_starting",
        mode=settings.SUPPRESSION_MODE,
        replay_speed=settings.REPLAY_SPEED,
        house_ids=settings.HOUSE_IDS,
        data_file=settings.DATA_FILE,
    )

    # ── Load DEBS data (one-time, at startup) ─────────────────────────────
    data_file = Path(settings.DATA_PATH) / settings.DATA_FILE
    df = load_debs(
        data_file,
        house_ids=settings.HOUSE_IDS,
        property_filter=settings.PROPERTY_FILTER,
    )

    # ── Initialise per-house state ─────────────────────────────────────────
    house_states: dict[int, HouseState] = {
        h_id: HouseState(house_id=h_id) for h_id in settings.HOUSE_IDS
    }
    stats = SimStats()

    # ── Kafka producer ─────────────────────────────────────────────────────
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        max_batch_size=1_048_576,  # 1 MB
        linger_ms=5,               # small batch window for throughput
        compression_type="gzip",
        acks="all",
    )

    try:
        await producer.start()
    except KafkaConnectionError as exc:
        log.error("kafka_connection_failed", error=str(exc), bootstrap=settings.KAFKA_BOOTSTRAP_SERVERS)
        raise

    log.info("kafka_producer_started", topic=TOPIC_SENSOR_DATA)

    # ── Graceful shutdown ──────────────────────────────────────────────────
    shutdown_event = asyncio.Event()

    def _handle_signal(*_: Any) -> None:
        log.info("shutdown_signal_received")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal)

    # ── Background listeners (hiereb/uniform mode only) ────────────────────
    background_tasks: list[asyncio.Task] = []
    if settings.SUPPRESSION_MODE != "full_tx":
        background_tasks = [
            asyncio.create_task(consume_predictions(house_states, shutdown_event)),
            asyncio.create_task(consume_thresholds(house_states, shutdown_event)),
        ]
        log.info("background_listeners_started", mode=settings.SUPPRESSION_MODE)

    # ── Replay loop ────────────────────────────────────────────────────────
    # Sleep 1/REPLAY_SPEED seconds between consecutive timestamps
    interval_seconds: float = 1.0 / settings.REPLAY_SPEED

    messages_sent = 0
    last_log_time = time.monotonic()
    last_log_count = 0
    prev_ts: int | None = None

    try:
        for batch in iter_timestep_batches(df):
            if shutdown_event.is_set():
                log.info("simulator_shutdown_requested", messages_sent=messages_sent)
                break

            # Throttle: sleep once per unique timestamp
            if prev_ts is not None and batch.timestamp != prev_ts:
                await asyncio.sleep(interval_seconds)

            prev_ts = batch.timestamp

            # Build and send message
            house_state = house_states[batch.house_id]
            payload, variance_updates = build_kafka_message(
                batch, house_state, stats, settings.SUPPRESSION_MODE
            )

            await producer.send(
                TOPIC_SENSOR_DATA,
                key=str(batch.house_id).encode(),
                value=payload,
            )

            # Publish variance updates in hiereb/uniform modes
            if variance_updates:
                for plug_uid, transmitted, residual, delta in variance_updates:
                    await publish_variance_update(
                        producer, plug_uid, transmitted, residual, delta
                    )

            messages_sent += 1

            # Progress log every 500 messages
            if messages_sent % 500 == 0:
                now = time.monotonic()
                elapsed = now - last_log_time
                rate = (messages_sent - last_log_count) / elapsed if elapsed > 0 else 0.0
                log.info(
                    "simulator_progress",
                    messages_sent=messages_sent,
                    current_ts=batch.timestamp,
                    overall_tr=round(stats.overall_tr, 4),
                    msg_per_sec=round(rate, 1),
                )
                last_log_time = now
                last_log_count = messages_sent

    finally:
        # Cancel background listeners
        for task in background_tasks:
            task.cancel()
        if background_tasks:
            await asyncio.gather(*background_tasks, return_exceptions=True)

        log.info("simulator_shutting_down", **stats.summary())
        await producer.flush()
        await producer.stop()
        log.info("simulator_stopped", messages_sent=messages_sent)


def main() -> None:
    asyncio.run(run_simulator())


if __name__ == "__main__":
    main()
