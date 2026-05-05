#!/usr/bin/env bash
# One-command demo flow:
# 1) Run 3 modes (full_tx, uniform, hiereb)
# 2) Keep stack running for Grafana live demo
# 3) Export report CSV artifacts
#
# Usage:
#   bash scripts/demo_report.sh
#
# Optional env vars:
#   E2E_RUNTIME_SECONDS=45
#   HOUSE_ID=1
#   KEEP_STACK=1   # default 1 (leave Grafana/Kafka/DB up)

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

E2E_RUNTIME_SECONDS="${E2E_RUNTIME_SECONDS:-45}"
HOUSE_ID="${HOUSE_ID:-1}"
KEEP_STACK="${KEEP_STACK:-1}"
DEMO_DIR="results/demo_$(date +%Y%m%d_%H%M%S)"
SUMMARY_FILE="$DEMO_DIR/e2e_summary.csv"

mkdir -p "$DEMO_DIR"

SUMMARY_FILE="$SUMMARY_FILE" KEEP_STACK="$KEEP_STACK" E2E_RUNTIME_SECONDS="$E2E_RUNTIME_SECONDS" \
  bash scripts/verify_e2e.sh all

readarray -t RUN_IDS < <(tail -n +2 "$SUMMARY_FILE" | cut -d',' -f1)

if [[ "${#RUN_IDS[@]}" -eq 0 ]]; then
  echo "No run IDs captured from verify_e2e summary." >&2
  exit 1
fi

OUTPUT_DIR="$DEMO_DIR/export" HOUSE_ID="$HOUSE_ID" bash scripts/export_report_data.sh "${RUN_IDS[@]}"
python3 scripts/generate_report_charts.py --input-dir "$DEMO_DIR/export" --output-dir "$DEMO_DIR/charts"

echo ""
echo "Demo completed."
echo "Summary: $SUMMARY_FILE"
echo "Artifacts: $DEMO_DIR/export"
echo "Charts: $DEMO_DIR/charts"
echo "Grafana: http://localhost:3000  (admin / hiereb_pass)"
echo "Dashboard: HierEB Report Dashboard"
