#!/usr/bin/env bash
# Run all HierEB inspection scripts in a practical order.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export LOG_LINES="${LOG_LINES:-80}"

run_script() {
  local script="$1"
  printf "\n\n######## %s ########\n" "$script"
  "$SCRIPT_DIR/$script" || true
}

run_script inspect_system.sh
run_script inspect_kafka.sh
run_script inspect_timescaledb.sh
run_script inspect_grafana.sh
run_script inspect_simulator.sh
run_script inspect_aggregator.sh
run_script inspect_ml_hiereb.sh
run_script inspect_demo_artifacts.sh
