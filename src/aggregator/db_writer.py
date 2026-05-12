"""
TimescaleDB writer.

Collects HouseMetricRecord objects in a buffer and batch-inserts them.
Flush is triggered either when the buffer reaches DB_WRITE_BATCH_SIZE
or every 5 seconds (periodic flush task).

NOT responsible for:
- Computing metrics (that is HouseAggregator's job)
- Kafka interaction
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone

import asyncpg
import structlog

from config.settings import settings

log = structlog.get_logger(__name__)


@dataclass
class HouseMetricRecord:
    """One row to insert into house_metrics."""
    timestamp_unix: float     # stream-time unix seconds
    house_id: int
    actual_load: float        # Watts – transmitted + predicted-for-suppressed
    pred_load: float          # Watts – sum of predicted values
    e_h: float                # house-level error: actual_load - pred_load
    tr: float                 # transmission rate [0.0, 1.0]
    plug_count: int           # total plugs observed this timestep
    transmitted_count: int    # plugs that transmitted


_INSERT_SQL = """
    INSERT INTO house_metrics
        (run_id, time, house_id, actual_load, pred_load, e_h, tr, plug_count, transmitted_count)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    ON CONFLICT DO NOTHING
"""


class TimescaleWriter:
    """Async batch writer. Thread-safe within a single asyncio event loop."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self._buffer: list[HouseMetricRecord] = []
        self._lock = asyncio.Lock()
        self._rows_written = 0

    @classmethod
    async def create(cls) -> "TimescaleWriter":
        """Factory: create connection pool and return writer."""
        dsn = (
            f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}"
            f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
        )
        try:
            pool = await asyncpg.create_pool(
                dsn=dsn,
                min_size=2,
                max_size=settings.DB_POOL_SIZE,
                command_timeout=30,
            )
        except Exception as exc:
            log.error("db_pool_creation_failed", host=settings.DB_HOST, error=str(exc))
            raise

        log.info("db_pool_created", host=settings.DB_HOST, db=settings.DB_NAME)
        return cls(pool)

    async def enqueue(self, record: HouseMetricRecord) -> None:
        """Add record to buffer; flush if buffer is full."""
        async with self._lock:
            self._buffer.append(record)
            if len(self._buffer) >= settings.DB_WRITE_BATCH_SIZE:
                await self._flush_locked()

    async def flush(self) -> None:
        """Force flush all buffered records."""
        async with self._lock:
            await self._flush_locked()

    async def _flush_locked(self) -> None:
        """Must be called while holding self._lock."""
        if not self._buffer:
            return

        records = self._buffer[:]
        self._buffer.clear()

        rows = [
            (
                settings.RUN_ID,
                datetime.fromtimestamp(r.timestamp_unix, tz=timezone.utc),
                r.house_id,
                r.actual_load,
                r.pred_load,
                r.e_h,
                r.tr,
                r.plug_count,
                r.transmitted_count,
            )
            for r in records
        ]

        try:
            async with self._pool.acquire() as conn:
                await conn.executemany(_INSERT_SQL, rows)
            self._rows_written += len(records)
            log.debug("db_batch_written", rows=len(records), total=self._rows_written)
        except asyncpg.PostgresError as exc:
            log.error("db_write_failed", rows=len(records), error=str(exc))
            # Re-buffer on failure so data isn't lost silently
            self._buffer = records + self._buffer
            raise

    async def close(self) -> None:
        """Flush remaining records and close pool."""
        await self.flush()
        await self._pool.close()
        log.info("db_writer_closed", total_rows_written=self._rows_written)
