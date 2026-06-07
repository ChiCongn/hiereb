"""
IoT Simulator – entry point.

Reads DEBS CSV, replays at REPLAY_SPEED×, publishes to hiereb.sensor_data.

Message format (1 message per house per timestamp):
{
    "house_id":  int,
    "timestamp": float,                  # stream-time unix seconds
    "source_timestamp": int,             # original DEBS event-time
    "plugs": [
        {
            "plug_uid":     int,
            "plug_id":      int,
            "household_id": int,
            "value":        float | null, # null when suppressed
            "actual_load":  float,
            "predicted":    float | null,
            "predicted_load": float | null,
            "reconstructed_load": float,
            "residual":     float | null,
            "decision":     str,
            "reason":       str,
            "plug_status":  str,
            "is_forced_transmit": bool,
            "transmitted":  bool
        },
        ...
    ]
}

full_tx: value is always present, transmitted=True.
uniform/hiereb: suppression activates, value=null for suppressed plugs.
"""
from __future__ import annotations

import asyncio
from itertools import chain
import json
import signal
import time
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
from src.simulator.loader import (
    TimestepBatch,
    discover_stream_house_ids,
    iter_timestep_batches_from_files,
    resolve_data_files,
    resolve_house_ids,
    resolve_time_windows,
)
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
    active_window_seconds: int | None = None,
    uniform_allocation_period_seconds: int | None = None,
    output_timestamp: float | None = None,
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
    uniform_delta_h = settings.EPSILON_H if uniform_delta is None else uniform_delta
    uniform_active_window = (
        settings.ACTIVE_WINDOW_SECONDS
        if active_window_seconds is None
        else active_window_seconds
    )
    uniform_allocation_period = (
        settings.TAU
        if uniform_allocation_period_seconds is None
        else uniform_allocation_period_seconds
    )

    if mode == "uniform":
        for reading in batch.readings:
            house_state.get_or_create_plug(
                plug_uid=reading.plug_uid,
                household_id=reading.household_id,
                plug_id=reading.plug_id,
            )
        house_state.activate_due_uniform_allocation(batch.timestamp)
        house_state.ensure_initial_uniform_allocation(
            timestamp=batch.timestamp,
            delta_h=uniform_delta_h,
            active_window_seconds=uniform_active_window,
        )
    elif mode == "hiereb":
        house_state.activate_due_hiereb_thresholds(batch.timestamp)

    for reading in batch.readings:
        plug = house_state.get_or_create_plug(
            plug_uid=reading.plug_uid,
            household_id=reading.household_id,
            plug_id=reading.plug_id,
        )

        if mode == "full_tx":
            decision = plug.decide_transmission(
                reading.value,
                reading.timestamp,
                bypass_suppression=True,
            )
        elif mode == "uniform":
            plug_status = house_state.plug_status_for_event(
                plug,
                reading.timestamp,
                uniform_active_window,
            )
            decision = plug.decide_transmission(
                reading.value,
                reading.timestamp,
                plug_status=plug_status,
            )
        else:
            decision = plug.decide_transmission(reading.value, reading.timestamp)

        if decision.transmitted:
            stats.record_transmit(reading.plug_uid, decision.residual)
        else:
            stats.record_suppress(reading.plug_uid)

        plug.observe(reading.value, reading.timestamp)

        # Queue variance update for suppression modes.
        if mode != "full_tx":
            variance_residual = decision.residual if decision.transmitted else None
            variance_updates.append(
                (reading.plug_uid, decision.transmitted, variance_residual, plug.delta)
            )

        reconstructed_load = (
            reading.value if decision.transmitted else decision.predicted
        )
        if reconstructed_load is None:
            # Missing prediction is always forced transmit, so this should only
            # be reachable if future decision logic violates that contract.
            reconstructed_load = reading.value

        plug_messages.append({
            "plug_uid": reading.plug_uid,
            "plug_id": reading.plug_id,
            "household_id": reading.household_id,
            "value": reading.value if decision.transmitted else None,
            "actual_load": reading.value,
            "predicted": decision.predicted,
            "predicted_load": decision.predicted,
            "reconstructed_load": reconstructed_load,
            "residual": decision.residual,
            "abs_residual": decision.abs_residual,
            "decision": "transmit" if decision.transmitted else "suppress",
            "reason": decision.reason,
            "plug_status": decision.plug_status,
            "is_forced_transmit": decision.is_forced_transmit,
            "transmitted": decision.transmitted,
        })

    if mode == "uniform":
        house_state.stage_uniform_allocation_if_due(
            timestamp=batch.timestamp,
            delta_h=uniform_delta_h,
            active_window_seconds=uniform_active_window,
            allocation_period_seconds=uniform_allocation_period,
        )

    payload = {
        "house_id": batch.house_id,
        "timestamp": batch.timestamp if output_timestamp is None else output_timestamp,
        "source_timestamp": batch.timestamp,
        "plugs": plug_messages,
    }

    return (
        json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        variance_updates,
    )


def resolve_stream_timestamp(
    source_timestamp: int,
    *,
    source_start_timestamp: int,
    wall_start_timestamp: float,
    replay_speed: int,
    stream_time_mode: str,
) -> float:
    """Map source CSV/DEBS event-time to the timestamp published downstream.

    The simulator keeps all suppression/prediction logic on source event-time.
    This function only rewrites the externally visible stream timestamp, so
    TimescaleDB/Grafana can look like a live stream while source_timestamp keeps
    the original CSV timestamp for audit and deterministic replay.
    """
    mode = stream_time_mode.strip().lower().replace("-", "_")
    if mode == "source":
        return float(source_timestamp)
    if mode in {"wall_clock", "live"}:
        speed = max(replay_speed, 1)
        return wall_start_timestamp + ((source_timestamp - source_start_timestamp) / speed)
    if mode in {"stream_epoch", "rebased", "rebase"}:
        return float(int(wall_start_timestamp) + (source_timestamp - source_start_timestamp))
    raise ValueError(
        "STREAM_TIME_MODE must be one of: source, wall_clock/live, stream_epoch/rebased"
    )


# ─── Main simulation coroutine ────────────────────────────────────────────────

async def run_simulator() -> None:
    configure_logging(settings.LOG_LEVEL)

    log.info(
        "simulator_starting",
        mode=settings.SUPPRESSION_MODE,
        replay_speed=settings.REPLAY_SPEED,
        dataset_preset=settings.DATASET_PRESET,
        data_window=settings.DATA_WINDOW,
        eval_start=settings.EVAL_START,
        eval_end=settings.EVAL_END,
        stream_time_mode=settings.STREAM_TIME_MODE,
    )

    # ── Resolve partitioned data; rows are streamed during replay ─────────
    data_files = resolve_data_files(
        settings.DATA_PATH,
        dataset_preset=settings.DATASET_PRESET,
        data_window=settings.DATA_WINDOW,
        data_file=settings.DATA_FILE,
        data_glob=settings.DATA_GLOB,
        one_house_id=settings.ONE_HOUSE_ID,
        five_house_ids=settings.FIVE_HOUSE_IDS,
    )
    house_ids = resolve_house_ids(
        settings.DATASET_PRESET,
        house_ids=settings.HOUSE_IDS,
        one_house_id=settings.ONE_HOUSE_ID,
        five_house_ids=settings.FIVE_HOUSE_IDS,
        auto_detect_house_ids=settings.AUTO_DETECT_HOUSE_IDS,
    )
    warmup_start, warmup_end, eval_start, eval_end = resolve_time_windows(
        data_files,
        house_ids=house_ids,
        property_filter=settings.PROPERTY_FILTER,
        warmup_start=settings.WARMUP_START,
        warmup_end=settings.WARMUP_END,
        eval_start=settings.EVAL_START,
        eval_end=settings.EVAL_END,
        auto_detect_time_window=settings.AUTO_DETECT_TIME_WINDOW,
        read_chunk_size=settings.STREAM_READ_CHUNK_SIZE,
    )
    effective_house_ids = discover_stream_house_ids(
        data_files,
        house_ids=house_ids,
        property_filter=settings.PROPERTY_FILTER,
        timestamp_start=eval_start,
        timestamp_end=eval_end,
    )
    log.info(
        "stream_data_resolved",
        files=[str(path) for path in data_files],
        configured_house_ids=house_ids if house_ids is not None else "auto",
        effective_house_ids=effective_house_ids,
        warmup_start=warmup_start,
        warmup_end=warmup_end,
        eval_start=eval_start,
        eval_end=eval_end,
        auto_detect_house_ids=settings.AUTO_DETECT_HOUSE_IDS,
        auto_detect_time_window=settings.AUTO_DETECT_TIME_WINDOW,
    )
    batches = iter_timestep_batches_from_files(
        data_files,
        house_ids=house_ids,
        property_filter=settings.PROPERTY_FILTER,
        max_duration_seconds=None,
        read_chunk_size=settings.STREAM_READ_CHUNK_SIZE,
        timestamp_start=eval_start,
        timestamp_end=eval_end,
    )
    try:
        first_batch = next(batches)
    except StopIteration as exc:
        raise ValueError("Configured stream contains no replay batches") from exc

    # ── Initialise per-house state ─────────────────────────────────────────
    house_states: dict[int, HouseState] = {
        h_id: HouseState(house_id=h_id) for h_id in effective_house_ids
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
    source_start_timestamp = first_batch.timestamp
    wall_start_timestamp = time.time()

    try:
        for batch in chain((first_batch,), batches):
            if shutdown_event.is_set():
                log.info("simulator_shutdown_requested", messages_sent=messages_sent)
                break

            # Throttle: sleep once per unique timestamp
            if prev_ts is not None and batch.timestamp != prev_ts:
                await asyncio.sleep(interval_seconds)

            prev_ts = batch.timestamp

            # Build and send message
            house_state = house_states.setdefault(
                batch.house_id,
                HouseState(house_id=batch.house_id),
            )
            output_timestamp = resolve_stream_timestamp(
                batch.timestamp,
                source_start_timestamp=source_start_timestamp,
                wall_start_timestamp=wall_start_timestamp,
                replay_speed=settings.REPLAY_SPEED,
                stream_time_mode=settings.STREAM_TIME_MODE,
            )
            payload, variance_updates = build_kafka_message(
                batch,
                house_state,
                stats,
                settings.SUPPRESSION_MODE,
                output_timestamp=output_timestamp,
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
                        producer,
                        plug_uid,
                        transmitted,
                        residual,
                        delta,
                        timestamp=batch.timestamp,
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
                    source_ts=batch.timestamp,
                    stream_ts=round(output_timestamp, 3),
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
