"""
Unit tests for src/simulator/stats.py
Run: pytest tests/test_stats.py -v
"""
from __future__ import annotations

import pytest

from src.simulator.stats import SimStats


def test_stats_all_transmitted():
    stats = SimStats(house_id=0)
    for _ in range(10):
        stats.record_transmit(plug_uid=1, error=2.0)
    assert stats.transmission_rate() == pytest.approx(1.0)


def test_stats_all_suppressed():
    stats = SimStats(house_id=0)
    for _ in range(10):
        stats.record_suppress(plug_uid=1)
    assert stats.transmission_rate() == pytest.approx(0.0)


def test_stats_mixed_transmission():
    stats = SimStats(house_id=0)
    stats.record_transmit(1, error=1.0)
    stats.record_suppress(1)
    stats.record_transmit(2, error=2.0)
    stats.record_suppress(2)
    assert stats.transmission_rate() == pytest.approx(0.5)


def test_stats_welford_variance():
    stats = SimStats(house_id=0)
    for x in [10.0, 20.0, 30.0]:
        stats.record_transmit(plug_uid=1, error=x)

    var = stats.variance_states[1].variance()
    assert var > 0.0


def test_stats_empty_state():
    stats = SimStats(house_id=0)
    assert stats.transmission_rate() == 0.0
    assert stats.transmitted == 0
    assert stats.suppressed == 0
    assert stats.total_timesteps == 0


def test_stats_summary():
    stats = SimStats(house_id=0)
    stats.record_transmit(1, 5.0)
    stats.record_suppress(2)
    stats.record_transmit(3, 2.0)

    s = stats.summary()
    assert s["house_id"] == 0
    assert s["tr"] == pytest.approx(2/3, abs=1e-4)
    assert s["transmitted"] == 2
    assert s["suppressed"] == 1


# def test_stats_record_suppress_does_not_update_variance_yet():
#     """Censored correction is done in allocator (Week 2)."""
#     stats = SimStats(house_id=0)
#     stats.record_suppress(plug_uid=999)
#     assert 999 in stats.variance_states   # just created, variance still 0
#     assert stats.variance_states[999].count == 0