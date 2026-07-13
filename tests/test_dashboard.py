"""Dashboard contract tests."""
from __future__ import annotations

import json
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]


def test_dashboard_uses_corrected_metrics_and_required_panels():
    dashboard = json.loads((ROOT_DIR / "grafana/dashboards/hiereb_main.json").read_text(encoding="utf-8"))
    titles = {panel.get("title") for panel in dashboard["panels"]}
    raw_dashboard = json.dumps(dashboard)

    assert "Actual vs Reconstructed Load" in titles
    assert "House Error with Delta_H" in titles
    assert "Transmission Rate Trend" in titles
    assert "TR vs RMSE" in titles
    assert "TR vs P95" in titles
    assert "Threshold Trace" in titles
    assert "Threshold Distribution" in titles

    assert "reconstructed_load" in raw_dashboard
    assert "percentile_cont(0.95)" in raw_dashboard
    assert "Delta_H" in raw_dashboard
    assert "50W" not in raw_dashboard
    assert "abs(e_h) > 50" not in raw_dashboard
    assert "P90" not in raw_dashboard
    assert "percentile_cont(0.9)" not in raw_dashboard
