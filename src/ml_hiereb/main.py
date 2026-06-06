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
from collections import deque
import json
import math
import signal
from typing import Any

import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from config.logging_config import configure_logging
from config.settings import settings
from src.ml_hiereb.allocator import (
    DEFAULT_ROLLING_WINDOW_SIZE,
    SIGMA_FLOOR_FALLBACK,
    HierEBAllocator,
)
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
WarmupResidualsByHouse = dict[int, dict[int, list[float]]]

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


def collect_warmup_residuals_by_house(
    df,
    predictor: TimeSlicePredictor,
    *,
    rolling_window_size: int = DEFAULT_ROLLING_WINDOW_SIZE,
) -> WarmupResidualsByHouse:
    """Collect signed warm-up residuals per house/plug using fitted predictions."""
    required = {"house_id", "plug_uid", "timestamp", "value"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Warm-up DataFrame missing columns: {missing}")

    buffers: dict[int, dict[int, deque[float]]] = {}
    for row in df[["house_id", "plug_uid", "timestamp", "value"]].itertuples(index=False):
        house_id = int(row.house_id)
        plug_uid = int(row.plug_uid)
        prediction = predictor.predict_single(plug_uid, int(row.timestamp))
        if prediction is None:
            continue
        house_buffers = buffers.setdefault(house_id, {})
        plug_buffer = house_buffers.setdefault(
            plug_uid,
            deque(maxlen=rolling_window_size),
        )
        plug_buffer.append(float(row.value) - float(prediction))

    return {
        house_id: {plug_uid: list(residuals) for plug_uid, residuals in plugs.items()}
        for house_id, plugs in buffers.items()
    }


def merge_warmup_residuals_by_house(
    target: dict[int, dict[int, deque[float]]],
    incoming: WarmupResidualsByHouse,
    *,
    rolling_window_size: int = DEFAULT_ROLLING_WINDOW_SIZE,
) -> None:
    """Merge residual windows from one partition, keeping only the last N samples."""
    for house_id, plug_residuals in incoming.items():
        target_house = target.setdefault(house_id, {})
        for plug_uid, residuals in plug_residuals.items():
            target_buffer = target_house.setdefault(
                plug_uid,
                deque(maxlen=rolling_window_size),
            )
            target_buffer.extend(residuals)


def freeze_warmup_residuals_by_house(
    residual_buffers: dict[int, dict[int, deque[float]]],
) -> WarmupResidualsByHouse:
    """Convert mutable warm-up residual buffers into plain lists."""
    return {
        house_id: {plug_uid: list(residuals) for plug_uid, residuals in plugs.items()}
        for house_id, plugs in residual_buffers.items()
    }


def flatten_warmup_residuals_by_plug(
    residuals_by_house: WarmupResidualsByHouse,
) -> dict[int, list[float]]:
    """Return {plug_uid: residuals} for allocator seeding."""
    return {
        plug_uid: residuals
        for plug_residuals in residuals_by_house.values()
        for plug_uid, residuals in plug_residuals.items()
    }


def _sample_sigma(residuals: list[float]) -> float:
    if len(residuals) < 2:
        return 0.0
    mean = sum(residuals) / len(residuals)
    variance = sum((value - mean) ** 2 for value in residuals) / (len(residuals) - 1)
    return math.sqrt(max(variance, 0.0))


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("percentile requires at least one value")
    if len(ordered) == 1:
        return ordered[0]
    rank = (percentile / 100.0) * (len(ordered) - 1)
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    fraction = rank - lower
    return ordered[lower] + ((ordered[upper] - ordered[lower]) * fraction)


def compute_sigma_floor_by_house(
    residuals_by_house: WarmupResidualsByHouse,
) -> dict[int, float]:
    """
    Compute fixed sigma_floor_H from warm-up residual sigmas.

    sigma_floor_H = percentile_5({sigma_p > 0 in house}); fallback is 1W.
    """
    result: dict[int, float] = {}
    for house_id, plug_residuals in residuals_by_house.items():
        positive_sigmas = [
            sigma
            for residuals in plug_residuals.values()
            if (sigma := _sample_sigma(residuals)) > 0
        ]
        result[house_id] = (
            _percentile(positive_sigmas, 5.0)
            if positive_sigmas
            else SIGMA_FLOOR_FALLBACK
        )
    return result


def build_threshold_payload(
    allocator: HierEBAllocator,
    house_id: int,
    plug_uids: list[int],
    deltas: dict[int, float],
) -> bytes:
    """Build one hiereb.thresholds message with trace metadata."""
    trace = allocator.latest_trace_by_house[house_id]
    house_deltas = {
        str(plug_uid): deltas.get(plug_uid, allocator.get_delta(plug_uid))
        for plug_uid in plug_uids
    }
    payload = {
        "house_id": house_id,
        "deltas": house_deltas,
        "allocation_time": trace.allocation_time,
        "effective_after_time": trace.effective_after_time,
        "threshold_version": trace.threshold_version,
        "sigma_floor_used": trace.sigma_floor,
        "n_budget_active_plugs": trace.n_budget_active_plugs,
    }
    return json.dumps(payload, separators=(",", ":")).encode()


def resolve_active_plug_uids_by_house(
    house_structure: dict[int, dict[int, list[int]]],
    last_event_timestamp_by_plug: dict[int, int],
    *,
    allocation_time: int,
    active_window_seconds: int,
) -> dict[int, set[int]]:
    """Return budget-active plug sets for the allocator at allocation_time."""
    if active_window_seconds < 0:
        raise ValueError("active_window_seconds must be non-negative")
    active_by_house: dict[int, set[int]] = {}
    for house_id, households in house_structure.items():
        active_set: set[int] = set()
        for plug_uids in households.values():
            for plug_uid in plug_uids:
                last_seen = last_event_timestamp_by_plug.get(plug_uid)
                if last_seen is None:
                    continue
                if allocation_time - last_seen <= active_window_seconds:
                    active_set.add(plug_uid)
        active_by_house[house_id] = active_set
    return active_by_house


def due_event_time_allocations(
    latest_event_timestamp: int | None,
    next_allocation_time: int,
    allocation_period_seconds: int,
) -> list[int]:
    """Return event-time allocation boundaries ready to process."""
    if allocation_period_seconds <= 0:
        raise ValueError("allocation_period_seconds must be positive")
    if latest_event_timestamp is None:
        return []

    due_times: list[int] = []
    allocation_time = next_allocation_time
    while latest_event_timestamp >= allocation_time:
        due_times.append(allocation_time)
        allocation_time += allocation_period_seconds
    return due_times


async def publish_thresholds(
    producer: AIOKafkaProducer,
    allocator: HierEBAllocator,
    house_structure: dict[int, dict[int, list[int]]],
    deltas: dict[int, float],
) -> None:
    """Publish threshold messages for all houses."""
    for house_id, hh_struct in house_structure.items():
        all_plugs = [p for hh in hh_struct.values() for p in hh]
        payload = build_threshold_payload(allocator, house_id, all_plugs, deltas)
        await producer.send(
            TOPIC_THRESHOLDS,
            key=str(house_id).encode(),
            value=payload,
        )


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
    last_event_timestamp_by_plug: dict[int, int] | None = None,
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
            event_timestamp = data.get("timestamp")
            if event_timestamp is not None and last_event_timestamp_by_plug is not None:
                last_event_timestamp_by_plug[plug_uid] = max(
                    int(event_timestamp),
                    last_event_timestamp_by_plug.get(plug_uid, int(event_timestamp)),
                )

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
    initial_allocation_time: int,
    last_event_timestamp_by_plug: dict[int, int],
) -> None:
    """
    Every TAU event-time seconds: run water-filling and publish new deltas.
    """
    tau = settings.TAU
    sleep_seconds = _scaled_sleep_seconds(tau)
    next_allocation_time = initial_allocation_time + tau

    while not shutdown.is_set():
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=sleep_seconds)
        except asyncio.TimeoutError:
            pass
        if shutdown.is_set():
            break

        latest_event_timestamp = (
            max(last_event_timestamp_by_plug.values())
            if last_event_timestamp_by_plug
            else None
        )
        due_allocations = due_event_time_allocations(
            latest_event_timestamp,
            next_allocation_time,
            tau,
        )
        if not due_allocations:
            continue

        for allocation_time in due_allocations:
            active_plug_uids_by_house = resolve_active_plug_uids_by_house(
                house_structure,
                last_event_timestamp_by_plug,
                allocation_time=allocation_time,
                active_window_seconds=settings.ACTIVE_WINDOW_SECONDS,
            )
            new_deltas = allocator.reallocate(
                house_structure,
                active_plug_uids_by_house=active_plug_uids_by_house,
                allocation_time=allocation_time,
                effective_after_time=allocation_time,
            )

            await publish_thresholds(producer, allocator, house_structure, new_deltas)

            log.info(
                "thresholds_published",
                house_count=len(house_structure),
                plug_count=len(new_deltas),
                tau=tau,
                allocation_time=allocation_time,
                active_plug_count=sum(len(active) for active in active_plug_uids_by_house.values()),
                latest_event_timestamp=latest_event_timestamp,
                sleep_seconds=round(sleep_seconds, 3),
            )

        next_allocation_time = due_allocations[-1] + tau


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
    warmup_residual_buffers_by_house: dict[int, dict[int, deque[float]]] = {}
    last_event_timestamp_by_plug: dict[int, int] = {}
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
        partition_residuals = collect_warmup_residuals_by_house(
            df_train,
            predictor,
            rolling_window_size=DEFAULT_ROLLING_WINDOW_SIZE,
        )
        merge_warmup_residuals_by_house(
            warmup_residual_buffers_by_house,
            partition_residuals,
            rolling_window_size=DEFAULT_ROLLING_WINDOW_SIZE,
        )

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
    warmup_residuals_by_house = freeze_warmup_residuals_by_house(
        warmup_residual_buffers_by_house
    )
    sigma_floor_by_house = compute_sigma_floor_by_house(warmup_residuals_by_house)
    epsilon_h_watts = resolve_error_budget_from_load_stats(
        load_sum_by_house,
        sample_count_by_house,
        settings.EPSILON_H,
    )
    log.info(
        "error_budget_resolved",
        epsilon_h_config=settings.EPSILON_H,
        epsilon_h_watts=round(epsilon_h_watts, 4),
        sigma_floor_by_house={
            house_id: round(floor, 4)
            for house_id, floor in sigma_floor_by_house.items()
        },
    )
    allocator = HierEBAllocator(
        epsilon_h=epsilon_h_watts,
        sigma_floor_by_house=sigma_floor_by_house,
    )
    allocator.seed_warmup_residuals(
        flatten_warmup_residuals_by_plug(warmup_residuals_by_house)
    )
    initial_deltas = allocator.reallocate(
        house_structure,
        allocation_time=settings.EVAL_START,
        effective_after_time=settings.EVAL_START - 1,
        threshold_version=0,
    )

    # ── Kafka producer ─────────────────────────────────────────────────────
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        compression_type="gzip",
        acks="all",
    )
    await producer.start()
    await publish_thresholds(producer, allocator, house_structure, initial_deltas)
    log.info(
        "initial_thresholds_published",
        house_count=len(house_structure),
        plug_count=len(initial_deltas),
        allocation_time=settings.EVAL_START,
        effective_after_time=settings.EVAL_START - 1,
    )

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
            variance_consumer_loop(allocator, shutdown, last_event_timestamp_by_plug),
            reallocation_loop(
                allocator,
                house_structure,
                producer,
                shutdown,
                initial_allocation_time=settings.EVAL_START,
                last_event_timestamp_by_plug=last_event_timestamp_by_plug,
            ),
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
