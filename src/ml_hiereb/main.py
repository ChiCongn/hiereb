"""
ML + HierEB Service – entry point.

Runs three concurrent asyncio tasks:
  1. prediction_loop   – publishes prediction batches every batch interval
  2. variance_consumer – consumes hiereb.variance, feeds allocator
  3. reallocation_loop – water-filling every τ seconds, publishes new deltas

Shares state (predictor, allocator) within one asyncio event loop.
No locks needed: single-threaded asyncio.

Kafka topics consumed:
  - hiereb.variance   ← variance updates from simulator

Kafka topics published:
  - hiereb.predictions  ← prediction batches per plug
  - hiereb.thresholds   ← new delta assignments per house

Message schemas:
  hiereb.predictions:
    {
      "plug_uid": int,
      "batch_start": int,        # unix timestamp
      "n": int,                  # number of predictions
      "predictions": {str: float},  # {"ts": value, ...}
      "delta": float
    }

  hiereb.thresholds:
    {
      "house_id": int,
      "deltas": {"plug_uid_str": delta_float, ...}
    }

  hiereb.variance (consumed):
    {
      "plug_uid": int,
      "residual": float | null,    # null if suppressed
      "delta": float,
      "transmitted": bool
    }
"""
from __future__ import annotations

import asyncio
import json
import signal
from typing import Any

import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from config.logging_config import configure_logging
from config.settings import settings
from src.ml_hiereb.allocator import HierEBAllocator
from src.ml_hiereb.predictor import TimeSlicePredictor
from src.simulator.loader import (
    load_debs,
    resolve_data_files,
    resolve_house_ids,
)

log = structlog.get_logger(__name__)

TOPIC_PREDICTIONS = "hiereb.predictions"
TOPIC_THRESHOLDS = "hiereb.thresholds"
TOPIC_VARIANCE = "hiereb.variance"
CONSUMER_GROUP = "hiereb-ml"

# ─── House structure helper ───────────────────────────────────────────────────

def _scaled_sleep_seconds(data_time_seconds: int) -> float:
    """Convert replay data-time seconds to wall-clock seconds."""
    replay_speed = max(settings.REPLAY_SPEED, 1)
    return data_time_seconds / replay_speed


def build_house_structure(df) -> dict[int, dict[int, list[int]]]:
    """
    Build {house_id: {household_id: [plug_uid, ...]}} from loaded DataFrame.
    Used to drive two-stage water-filling.
    """
    structure: dict[int, dict[int, list[int]]] = {}
    for _, row in df[["house_id", "household_id", "plug_uid"]].drop_duplicates().iterrows():
        h, hh, p = int(row["house_id"]), int(row["household_id"]), int(row["plug_uid"])
        structure.setdefault(h, {}).setdefault(hh, [])
        if p not in structure[h][hh]:
            structure[h][hh].append(p)
    return structure


def merge_house_structure(
    target: dict[int, dict[int, list[int]]],
    incoming: dict[int, dict[int, list[int]]],
) -> None:
    """Merge plug hierarchy from one data partition into a shared structure."""
    for house_id, households in incoming.items():
        target_households = target.setdefault(house_id, {})
        for household_id, plug_uids in households.items():
            target_plugs = target_households.setdefault(household_id, [])
            for plug_uid in plug_uids:
                if plug_uid not in target_plugs:
                    target_plugs.append(plug_uid)


def resolve_error_budget_watts(df, epsilon_h: float) -> float:
    """
    Resolve EPSILON_H into an absolute Watt budget.

    Convention:
      - 0 < EPSILON_H < 1 means ratio of mean house load, e.g. 0.05 = 5%.
      - EPSILON_H >= 1 means an explicit Watt budget.
    """
    if epsilon_h >= 1.0:
        return epsilon_h

    house_load = df.groupby(["house_id", "timestamp"])["value"].sum()
    mean_house_load = float(house_load.groupby("house_id").mean().mean())
    return epsilon_h * mean_house_load


def resolve_error_budget_from_load_stats(
    load_sum_by_house: dict[int, float],
    sample_count_by_house: dict[int, int],
    epsilon_h: float,
) -> float:
    """Resolve the Watt budget from training partitions without concatenating them."""
    if epsilon_h >= 1.0:
        return epsilon_h
    means = [
        load_sum_by_house[house_id] / sample_count_by_house[house_id]
        for house_id in load_sum_by_house
        if sample_count_by_house.get(house_id, 0) > 0
    ]
    if not means:
        raise ValueError("No training house loads available to resolve EPSILON_H")
    return epsilon_h * (sum(means) / len(means))


# ─── Three concurrent tasks ───────────────────────────────────────────────────

async def prediction_loop(
    predictor: TimeSlicePredictor,
    allocator: HierEBAllocator,
    producer: AIOKafkaProducer,
    shutdown: asyncio.Event,
    initial_batch_start: int,
) -> None:
    """
    Publishes prediction batches for all known plugs every BATCH_INTERVAL_SECONDS.

    Each message carries predictions for the next BATCH_INTERVAL_SECONDS timestamps,
    so the simulator always has predictions ready without round-trips.
    """
    interval = settings.BATCH_INTERVAL_SECONDS
    sleep_seconds = _scaled_sleep_seconds(interval)
    batch_start = initial_batch_start

    while not shutdown.is_set():
        if not predictor.is_fitted():
            log.warning("predictor_not_fitted_yet")
            await asyncio.sleep(sleep_seconds)
            continue

        plug_uids = predictor.known_plug_uids()
        published = 0

        for plug_uid in plug_uids:
            predictions = predictor.predict_batch(plug_uid, batch_start, n=interval)
            delta = allocator.get_delta(plug_uid)

            payload = json.dumps({
                "plug_uid": plug_uid,
                "batch_start": batch_start,
                "n": interval,
                "predictions": {str(ts): val for ts, val in predictions.items()},
                "delta": delta,
            }, separators=(",", ":")).encode()

            await producer.send(
                TOPIC_PREDICTIONS,
                key=str(plug_uid).encode(),
                value=payload,
            )
            published += 1

        log.info(
            "predictions_published",
            batch_start=batch_start,
            plug_count=published,
            interval=interval,
            sleep_seconds=round(sleep_seconds, 3),
        )
        batch_start += interval

        try:
            await asyncio.wait_for(shutdown.wait(), timeout=sleep_seconds)
        except asyncio.TimeoutError:
            pass


async def variance_consumer_loop(
    allocator: HierEBAllocator,
    shutdown: asyncio.Event,
) -> None:
    """
    Consumes hiereb.variance messages and feeds residuals into the allocator.

    Message format:
      {plug_uid, residual (null if suppressed), delta, transmitted}
    """
    consumer = AIOKafkaConsumer(
        TOPIC_VARIANCE,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="latest",
    )
    await consumer.start()
    log.info("variance_consumer_started")

    messages_processed = 0
    try:
        async for msg in consumer:
            if shutdown.is_set():
                break

            try:
                data: dict[str, Any] = json.loads(msg.value)
            except json.JSONDecodeError as exc:
                log.warning("variance_invalid_json", error=str(exc))
                continue

            plug_uid: int = data["plug_uid"]
            transmitted: bool = data["transmitted"]
            delta: float = data.get("delta", 0.0)

            if transmitted:
                residual = data.get("residual")
                if residual is not None:
                    allocator.update_from_transmitted(plug_uid, float(residual))
            else:
                allocator.update_from_suppressed(plug_uid, delta)

            messages_processed += 1
            if messages_processed % 5000 == 0:
                log.info("variance_consumer_progress", messages=messages_processed)

    finally:
        await consumer.stop()
        log.info("variance_consumer_stopped", total_messages=messages_processed)


async def reallocation_loop(
    allocator: HierEBAllocator,
    house_structure: dict[int, dict[int, list[int]]],
    producer: AIOKafkaProducer,
    shutdown: asyncio.Event,
) -> None:
    """
    Every TAU seconds: run water-filling, publish new deltas to hiereb.thresholds.
    """
    tau = settings.TAU
    sleep_seconds = _scaled_sleep_seconds(tau)

    while not shutdown.is_set():
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=sleep_seconds)
        except asyncio.TimeoutError:
            pass
        if shutdown.is_set():
            break

        new_deltas = allocator.reallocate(house_structure)

        # Publish per house (one message per house)
        for house_id, hh_struct in house_structure.items():
            all_plugs = [p for hh in hh_struct.values() for p in hh]
            house_deltas = {
                str(p): new_deltas.get(p, allocator.get_delta(p))
                for p in all_plugs
            }

            payload = json.dumps({
                "house_id": house_id,
                "deltas": house_deltas,
            }, separators=(",", ":")).encode()

            await producer.send(
                TOPIC_THRESHOLDS,
                key=str(house_id).encode(),
                value=payload,
            )

        log.info(
            "thresholds_published",
            house_count=len(house_structure),
            plug_count=len(new_deltas),
            tau=tau,
            sleep_seconds=round(sleep_seconds, 3),
        )


# ─── Entry point ──────────────────────────────────────────────────────────────

async def _main() -> None:
    configure_logging(settings.LOG_LEVEL)
    log.info(
        "ml_hiereb_starting",
        epsilon_h=settings.EPSILON_H,
        tau=settings.TAU,
        dataset_preset=settings.DATASET_PRESET,
        data_window=settings.DATA_WINDOW,
        warmup_start=settings.WARMUP_START,
        warmup_end=settings.WARMUP_END,
        eval_start=settings.EVAL_START,
        eval_end=settings.EVAL_END,
    )

    # ── Resolve partition files and fit one partition at a time ────────────
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
    )
    predictor = TimeSlicePredictor(bin_seconds=settings.PREDICTOR_BIN_SECONDS)
    house_structure: dict[int, dict[int, list[int]]] = {}
    load_sum_by_house: dict[int, float] = {}
    sample_count_by_house: dict[int, int] = {}
    initial_batch_start: int | None = settings.EVAL_START
    total_rows = 0
    total_training_rows = 0

    for data_file in data_files:
        df_full = load_debs(
            data_file,
            house_ids=house_ids,
            property_filter=settings.PROPERTY_FILTER,
            timestamp_start=settings.WARMUP_START,
            timestamp_end=settings.EVAL_END,
        )
        df_train = df_full[
            (df_full["timestamp"] >= settings.WARMUP_START)
            & (df_full["timestamp"] <= settings.WARMUP_END)
        ]
        if df_train.empty:
            continue

        predictor.fit(df_train)
        merge_house_structure(house_structure, build_house_structure(df_full))

        house_load = df_train.groupby(["house_id", "timestamp"])["value"].sum()
        per_house_stats = house_load.groupby("house_id").agg(["sum", "count"])
        for house_id, row in per_house_stats.iterrows():
            key = int(house_id)
            load_sum_by_house[key] = load_sum_by_house.get(key, 0.0) + float(row["sum"])
            sample_count_by_house[key] = sample_count_by_house.get(key, 0) + int(row["count"])

        total_rows += len(df_full)
        total_training_rows += len(df_train)
        del df_full, df_train

    if initial_batch_start is None or not predictor.is_fitted():
        raise ValueError("No training data loaded from configured data partitions")

    log.info(
        "training_split",
        files=len(data_files),
        total_rows=total_rows,
        training_rows=total_training_rows,
        warmup_start=settings.WARMUP_START,
        warmup_end=settings.WARMUP_END,
        eval_start=settings.EVAL_START,
        eval_end=settings.EVAL_END,
    )

    log.info(
        "house_structure_built",
        houses=len(house_structure),
        households=sum(len(hh) for hh in house_structure.values()),
        plugs=sum(len(p) for hh in house_structure.values() for p in hh.values()),
    )

    # ── Init allocator ─────────────────────────────────────────────────────
    epsilon_h_watts = resolve_error_budget_from_load_stats(
        load_sum_by_house,
        sample_count_by_house,
        settings.EPSILON_H,
    )
    log.info(
        "error_budget_resolved",
        epsilon_h_config=settings.EPSILON_H,
        epsilon_h_watts=round(epsilon_h_watts, 4),
    )
    allocator = HierEBAllocator(epsilon_h=epsilon_h_watts)

    # ── Kafka producer ─────────────────────────────────────────────────────
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        compression_type="gzip",
        acks="all",
    )
    await producer.start()

    # ── Shutdown coordination ──────────────────────────────────────────────
    shutdown = asyncio.Event()

    def _handle_signal(*_: Any) -> None:
        log.info("shutdown_signal_received")
        shutdown.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal)

    # ── Run all three loops concurrently ───────────────────────────────────
    try:
        await asyncio.gather(
            prediction_loop(predictor, allocator, producer, shutdown, initial_batch_start),
            variance_consumer_loop(allocator, shutdown),
            reallocation_loop(allocator, house_structure, producer, shutdown),
        )
    finally:
        await producer.flush()
        await producer.stop()
        log.info(
            "ml_hiereb_stopped",
            total_drift_events=0,  # drift detector added in Week 3
            realloc_count=allocator._realloc_count,
        )


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
