"""Deterministic research fixture with hierarchy, inactivity, boundary, and outlier cases."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl


def generate_synthetic(path: Path, seed: int = 42, house_id: int = 0) -> int:
    """Generate the 12-hour house fixture used by the example configuration."""
    # The formula is deterministic; seed gives a reproducible phase-shifted variant.
    phase = (seed % 360) * math.pi / 180.0
    start = datetime(2013, 9, 1, tzinfo=UTC)
    bases = {(0, 0): 110.0, (0, 1): 60.0, (1, 0): 20.0, (1, 1): 10.0}
    rows: list[dict[str, object]] = []
    row_id = 0
    for step in range(12 * 12):
        timestamp = start + timedelta(minutes=5 * step)
        for (household, plug), base in sorted(bases.items()):
            if (household, plug) == (1, 1) and datetime(
                2013, 9, 1, 6, 30, tzinfo=UTC
            ) <= timestamp < datetime(2013, 9, 1, 8, 0, tzinfo=UTC):
                continue
            value = base
            if timestamp >= datetime(2013, 9, 1, 6, tzinfo=UTC):
                if (household, plug) == (0, 1):
                    value += 2.0 * math.sin(step * 0.7 + phase)
                if (household, plug) == (1, 0):
                    value += 0.2 * ((step % 5) - 2)
            # Warm-up batch mean is 200, hence uniform delta is exactly 200*.01/4=.5.
            if timestamp == datetime(2013, 9, 1, 6, tzinfo=UTC) and (household, plug) == (0, 0):
                value = base + 0.5
            if timestamp == datetime(2013, 9, 1, 9, tzinfo=UTC) and (household, plug) == (1, 0):
                value += 50.0
            rows.append(
                {
                    "id": f"synthetic-{row_id}",
                    "timestamp": int(timestamp.timestamp()),
                    "value": value,
                    "property": 1,
                    "plug_id": plug,
                    "household_id": household,
                    "house_id": house_id,
                }
            )
            row_id += 1
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_csv(path)
    return len(rows)
