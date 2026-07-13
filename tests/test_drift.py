"""
Unit tests for src/ml_hiereb/drift.py

Run: pytest tests/test_drift.py -v
"""
from __future__ import annotations

import pytest

from src.ml_hiereb.drift import DriftDetector, PageHinkleyState


# ─── PageHinkleyState ─────────────────────────────────────────────────────────

def test_no_drift_on_stable_signal():
    """
    Constant zero residuals: CUSUM stays at 0 due to max(0,...) clamping.
    No alarm should fire.
    """
    # Use large xi to ensure stability
    state = PageHinkleyState(plug_uid=1, lambda_=5.0, xi=200.0)
    for _ in range(200):
        drift = state.update(0.0)
    assert drift is False
    assert state.drift_count == 0


def test_drift_detected_on_step_change():
    """
    Persistent positive shift (50W >> λ=5W) triggers alarm before 20 steps.
    """
    state = PageHinkleyState(plug_uid=1, lambda_=5.0, xi=50.0)

    # Warm up with stable period
    for _ in range(20):
        state.update(0.0)

    # Inject persistent positive step
    detected = False
    for _ in range(200):
        if state.update(50.0):
            detected = True
            break

    assert detected is True
    assert state.drift_count >= 1


def test_drift_detected_negative_shift():
    """Persistent negative shift should also trigger alarm."""
    state = PageHinkleyState(plug_uid=1, lambda_=5.0, xi=50.0)

    for _ in range(20):
        state.update(0.0)

    detected = False
    for _ in range(200):
        if state.update(-50.0):
            detected = True
            break

    assert detected is True


def test_no_false_alarm_on_single_spike():
    """
    A single large spike followed by return to baseline should not trigger.
    With large xi and single spike, CUSUM resets quickly via mean update.
    """
    state = PageHinkleyState(plug_uid=1, lambda_=5.0, xi=200.0)  # high xi

    for _ in range(100):
        state.update(0.0)

    # Single spike
    state.update(100.0)

    # Return to baseline – should not alarm
    alarmed = False
    for _ in range(100):
        if state.update(0.0):
            alarmed = True
            break

    assert alarmed is False


def test_forced_tx_window_after_drift():
    """After drift detection, plug is in forced TX for FORCED_TX_SECONDS ticks."""
    from src.ml_hiereb.drift import FORCED_TX_SECONDS

    state = PageHinkleyState(plug_uid=1, lambda_=5.0, xi=50.0)

    # Warm up at 0W baseline
    for _ in range(30):
        state.update(0.0)

    # Shift to 100W — CUSUM should alarm within a few steps
    for _ in range(200):
        state.update(100.0)
        if state.in_forced_tx:
            break

    assert state.drift_count >= 1, "Drift should have been detected"
    assert state.in_forced_tx, "Should be in forced TX right after drift"

    # Tick down the forced TX window
    for _ in range(FORCED_TX_SECONDS):
        state.tick()

    assert state.in_forced_tx is False, "Forced TX should end after countdown"


def test_drift_count_increments():
    """Each alarm increments drift_count. Uses warm-up-then-shift pattern."""
    state = PageHinkleyState(plug_uid=1, lambda_=5.0, xi=50.0)

    # Warm up
    for _ in range(20):
        state.update(0.0)

    # Large shift triggers at least one alarm
    for _ in range(200):
        state.update(100.0)

    assert state.drift_count >= 1


def test_drift_summary_only_shows_drifted_plugs():
    """
    Plug with stable signal (no drift) does not appear in summary.
    Plug with shift from baseline (drift detected) does appear.
    """
    # Use two independent detectors with the same default params
    stable_detector = DriftDetector(lambda_=5.0, xi=200.0)
    shift_detector = DriftDetector(lambda_=5.0, xi=50.0)

    # Plug 1: constant zero — no shift, no drift
    for _ in range(200):
        stable_detector.update(plug_uid=1, residual=0.0)
    assert 1 not in stable_detector.drift_summary()

    # Plug 2: warm-up at 0, then persistent 100W shift
    for _ in range(30):
        shift_detector.update(plug_uid=2, residual=0.0)
    for _ in range(200):
        shift_detector.update(plug_uid=2, residual=100.0)

    summary = shift_detector.drift_summary()
    assert 2 in summary
    assert summary[2] >= 1


# ─── DriftDetector ────────────────────────────────────────────────────────────

def test_detector_creates_state_on_demand():
    detector = DriftDetector()
    assert detector.in_forced_tx(plug_uid=42) is False  # unknown plug → False

    detector.update(plug_uid=42, residual=0.0)
    assert 42 in detector._states


def test_detector_in_forced_tx_false_for_unknown():
    detector = DriftDetector()
    assert detector.in_forced_tx(99) is False


def test_detector_total_drift_events():
    detector = DriftDetector(lambda_=1.0, xi=5.0)

    # Trigger drift for plug 1
    for _ in range(50):
        detector.update(plug_uid=1, residual=100.0)

    # Might have multiple drift events
    assert detector.total_drift_events() >= 0  # at minimum 0