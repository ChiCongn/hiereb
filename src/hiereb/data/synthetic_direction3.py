"""Deterministic Direction 3 fixture with bias, drift, step, spike, and reactivation."""

from __future__ import annotations

import csv
import math
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path


def generate_direction3_synthetic(
    output: Path,
    seed: int = 42,
    house_id: int = 0,
) -> int:
    """Write an eight-day, two-household adaptive-predictor fixture."""
    rng = random.Random(seed)
    output.parent.mkdir(parents=True, exist_ok=True)
    start = datetime(2013, 9, 1, tzinfo=UTC)
    evaluation_start = start + timedelta(days=7)
    rows: list[dict[str, int | float]] = []
    event_id = 0
    for step in range(8 * 24 * 12):
        timestamp = start + timedelta(minutes=5 * step)
        phase = 2.0 * math.pi * ((timestamp.hour * 60 + timestamp.minute) / 1440.0)
        evaluation_minutes = (timestamp - evaluation_start).total_seconds() / 60.0
        evaluation = timestamp >= evaluation_start
        values = {
            (0, 0): 40.0 + 4.0 * math.sin(phase) + rng.uniform(-0.15, 0.15),
            (0, 1): 70.0 + 6.0 * math.cos(phase) + rng.uniform(-0.4, 0.4),
            (1, 0): 100.0 + 8.0 * math.sin(phase + 0.5) + rng.uniform(-0.6, 0.6),
            (1, 1): 130.0 + 5.0 * math.cos(phase - 0.2) + rng.uniform(-0.3, 0.3),
        }
        if evaluation:
            values[(0, 1)] += 8.0
            values[(1, 0)] += max(evaluation_minutes, 0.0) / (24.0 * 60.0) * 12.0
            if evaluation_minutes >= 12 * 60:
                values[(1, 1)] += 15.0
            if evaluation_minutes == 6 * 60:
                values[(0, 0)] += 60.0
        for household_id, plug_id in sorted(values):
            if (
                evaluation
                and (household_id, plug_id) == (1, 1)
                and 8 * 60 <= evaluation_minutes < 12 * 60
            ):
                continue
            rows.append(
                {
                    "id": event_id,
                    "timestamp": int(timestamp.timestamp()),
                    "value": max(values[(household_id, plug_id)], 0.0),
                    "property": 1,
                    "plug_id": plug_id,
                    "household_id": household_id,
                    "house_id": house_id,
                }
            )
            event_id += 1
            if evaluation and evaluation_minutes == 15 * 60 and (household_id, plug_id) == (0, 0):
                duplicate = dict(rows[-1])
                duplicate["id"] = event_id
                duplicate["value"] = float(duplicate["value"]) + 0.1
                rows.append(duplicate)
                event_id += 1
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)
