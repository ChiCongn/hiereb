"""
Aggregator – replaces Apache Flink.

Consumes hiereb.sensor_data, computes house-level metrics per timestep,
writes to TimescaleDB.

Architectural invariants (DO NOT VIOLATE):
  - Aggregator is STATELESS regarding prediction logic.
    Predictions arrive embedded in each Kafka message from the simulator.
  - Aggregator distinguishes TRANSMITTED (value present) vs SUPPRESSED (value=null).
    For suppressed plugs, predicted value is used as the actual estimate.
  - All timestamps are EVENT-TIME (from DEBS data), not processing-time.
"""
from __future__ import annotations

import asyncio
import json
import signal
from dataclasses import dataclass, field
from typing import Any

import structlog
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError

from config.logging_config import configure_logging
from config.settings import settings
from src.aggregator.db_writer import HouseMetricRecord, TimescaleWriter

log = structlog.get_logger(__name__)

TOPIC_SENSOR_DATA = "hiereb.sensor_data"
CONSUMER_GROUP = "hiereb-aggregator"


# ─── Data structures ──────────────────────────────────────────────────────────

@dataclass
class PlugSnapshot:
    """Parsed plug entry from one Kafka message."""
    plug_uid: int
    household_id: int
    plug_id: int
    value: float | None    # None → suppressed, use predicted
    predicted: float
    transmitted: bool


@dataclass
class TimestepState:
    """
    Accumulates plug snapshots for (house_id, timestamp).

    In full_tx mode, one Kafka message = one complete timestep.
    In hiereb mode, same assumption holds (simulator batches per house per ts).
    """
    house_id: int
    timestamp: int           # event-time (unix seconds)
    snapshots: list[PlugSnapshot] = field(default_factory=list)

    def add_plug(self, snap: PlugSnapshot) -> None:
        self.snapshots.append(snap)

    def to_metric(self) -> HouseMetricRecord:
        """
        Compute house-level metrics from all plug snapshots.

        For suppressed plugs: use predicted as estimate of actual load.
        This matches the paper's definition of Ê_h(t).
        """
        actual_load = 0.0
        pred_load = 0.0
        transmitted_count = 0

        for snap in self.snapshots:
            if snap.transmitted and snap.value is not None:
                actual_load += snap.value
                pred_load += snap.predicted
                transmitted_count += 1
            else:
                # Suppressed plug: receiver uses prediction as its best estimate
                actual_load += snap.predicted
                pred_load += snap.predicted
                # e_h contribution from this plug = 0 (can't know actual)

        plug_count = len(self.snapshots)
        tr = transmitted_count / plug_count if plug_count > 0 else 0.0
        e_h = actual_load - pred_load  # house-level error

        return HouseMetricRecord(
            timestamp_unix=self.timestamp,
            house_id=self.house_id,
            actual_load=round(actual_load, 4),
            pred_load=round(pred_load, 4),
            e_h=round(e_h, 4),
            tr=round(tr, 6),
            plug_count=plug_count,
            transmitted_count=transmitted_count,
        )


# ─── Aggregator ───────────────────────────────────────────────────────────────

class HouseAggregator:
    """
    Consumes sensor_data messages, aggregates per house per timestep,
    writes metrics to TimescaleDB.
    """

    def __init__(self, writer: TimescaleWriter) -> None:
        self._writer = writer
        # Keyed by (house_id, timestamp) – one entry per in-flight timestep
        self._pending: dict[tuple[int, int], TimestepState] = {}
        self._metrics_written = 0
        self._messages_consumed = 0

    async def run(self) -> None:
        consumer = AIOKafkaConsumer(
            TOPIC_SENSOR_DATA,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            fetch_max_bytes=10 * 1024 * 1024,  # 10MB fetch
        )

        try:
            await consumer.start()
        except KafkaConnectionError as exc:
            log.error(
                "kafka_consumer_connection_failed",
                error=str(exc),
                bootstrap=settings.KAFKA_BOOTSTRAP_SERVERS,
            )
            raise

        log.info("aggregator_consumer_started", topic=TOPIC_SENSOR_DATA, group=CONSUMER_GROUP)

        # ── Shutdown ───────────────────────────────────────────────────────
        shutdown_event = asyncio.Event()

        def _handle_signal(*_: Any) -> None:
            log.info("shutdown_signal_received")
            shutdown_event.set()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, _handle_signal)

        # ── Periodic flush task ────────────────────────────────────────────
        flush_task = asyncio.create_task(self._periodic_flush())

        try:
            async for msg in consumer:
                if shutdown_event.is_set():
                    break

                await self._process_message(msg.value)

        finally:
            flush_task.cancel()
            try:
                await flush_task
            except asyncio.CancelledError:
                pass

            await consumer.stop()
            log.info(
                "aggregator_stopped",
                messages_consumed=self._messages_consumed,
                metrics_written=self._metrics_written,
            )

    async def _process_message(self, raw: bytes) -> None:
        """Parse one Kafka message and aggregate into TimestepState."""
        try:
            data: dict[str, Any] = json.loads(raw)
        except json.JSONDecodeError as exc:
            log.warning("invalid_json_skipped", error=str(exc), raw_bytes=len(raw))
            return

        self._messages_consumed += 1

        house_id: int = data["house_id"]
        timestamp: int = data["timestamp"]
        key = (house_id, timestamp)

        if key not in self._pending:
            self._pending[key] = TimestepState(house_id=house_id, timestamp=timestamp)

        state = self._pending[key]

        for plug_data in data.get("plugs", []):
            state.add_plug(PlugSnapshot(
                plug_uid=plug_data["plug_uid"],
                household_id=plug_data["household_id"],
                plug_id=plug_data["plug_id"],
                value=plug_data.get("value"),        # None if suppressed
                predicted=plug_data.get("predicted", 0.0),
                transmitted=plug_data["transmitted"],
            ))

        # One message = one complete timestep for a house (simulator guarantee).
        # Flush immediately.
        record = state.to_metric()
        await self._writer.enqueue(record)
        del self._pending[key]

        self._metrics_written += 1

        if self._metrics_written % 1000 == 0:
            log.info(
                "aggregator_progress",
                metrics_written=self._metrics_written,
                messages_consumed=self._messages_consumed,
                pending_states=len(self._pending),
            )

    async def _periodic_flush(self) -> None:
        """Flush the DB write buffer every 5 seconds regardless of batch fill level."""
        while True:
            await asyncio.sleep(5.0)
            await self._writer.flush()


# ─── Entry point ──────────────────────────────────────────────────────────────

async def _main() -> None:
    configure_logging(settings.LOG_LEVEL)
    log.info("aggregator_starting", kafka=settings.KAFKA_BOOTSTRAP_SERVERS, db=settings.DB_HOST)

    writer = await TimescaleWriter.create()
    aggregator = HouseAggregator(writer)

    try:
        await aggregator.run()
    finally:
        await writer.close()


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()