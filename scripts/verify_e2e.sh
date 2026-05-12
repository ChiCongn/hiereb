#!/usr/bin/env bash
# Verify the HierEB streaming stack end-to-end without deleting existing DB data.
#
# Usage:
#   bash scripts/verify_e2e.sh            # runs full_tx, uniform, hiereb
#   bash scripts/verify_e2e.sh full_tx    # runs one mode
#
# Useful overrides:
#   E2E_RUNTIME_SECONDS=45 bash scripts/verify_e2e.sh hiereb
#   KEEP_STACK=1 bash scripts/verify_e2e.sh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MODE_ARG="${1:-all}"
E2E_RUNTIME_SECONDS="${E2E_RUNTIME_SECONDS:-35}"
REPLAY_SPEED="${REPLAY_SPEED:-60}"
HOUSE_IDS_JSON="${HOUSE_IDS:-[1]}"
DATA_FILE="${DATA_FILE:-house-1.csv}"
DATA_GLOB="${DATA_GLOB:-}"
DATA_WINDOW="${DATA_WINDOW:-all}"
EPSILON_H="${EPSILON_H:-0.05}"
TAU="${TAU:-300}"
BATCH_INTERVAL_SECONDS="${BATCH_INTERVAL_SECONDS:-300}"
UNIFORM_DELTA="${UNIFORM_DELTA:-10.0}"
DB_WRITE_BATCH_SIZE="${DB_WRITE_BATCH_SIZE:-1}"
KEEP_STACK="${KEEP_STACK:-0}"
SUMMARY_FILE="${SUMMARY_FILE:-}"

OVERRIDE_FILE="$(mktemp /tmp/hiereb-e2e-override.XXXXXX.yml)"
RUN_IDS=()

COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.override.yml -f "$OVERRIDE_FILE")

cleanup() {
  set +e
  "${COMPOSE[@]}" stop simulator aggregator ml-hiereb >/dev/null 2>&1
  rm -f "$OVERRIDE_FILE"
  if [[ "$KEEP_STACK" != "1" ]]; then
    docker compose -f docker-compose.yml -f docker-compose.override.yml stop simulator aggregator ml-hiereb >/dev/null 2>&1
  fi
}
trap cleanup EXIT

if [[ -n "$SUMMARY_FILE" ]]; then
  mkdir -p "$(dirname "$SUMMARY_FILE")"
  if [[ ! -f "$SUMMARY_FILE" ]]; then
    echo "run_id,mode,rows,avg_tr,rmse" > "$SUMMARY_FILE"
  fi
fi

write_override() {
  local mode="$1"
  local run_id="$2"

  cat > "$OVERRIDE_FILE" <<YAML
services:
  simulator:
    environment:
      SUPPRESSION_MODE: "$mode"
      RUN_ID: "$run_id"
      DATASET_PRESET: "custom"
      DATA_WINDOW: "$DATA_WINDOW"
      REPLAY_SPEED: "$REPLAY_SPEED"
      HOUSE_IDS: '$HOUSE_IDS_JSON'
      DATA_FILE: "$DATA_FILE"
      DATA_GLOB: "$DATA_GLOB"
      EPSILON_H: "$EPSILON_H"
      TAU: "$TAU"
      BATCH_INTERVAL_SECONDS: "$BATCH_INTERVAL_SECONDS"
      UNIFORM_DELTA: "$UNIFORM_DELTA"
      LOG_LEVEL: INFO

  aggregator:
    environment:
      RUN_ID: "$run_id"
      DB_WRITE_BATCH_SIZE: "$DB_WRITE_BATCH_SIZE"
      LOG_LEVEL: INFO

  ml-hiereb:
    environment:
      SUPPRESSION_MODE: "$mode"
      RUN_ID: "$run_id"
      DATASET_PRESET: "custom"
      DATA_WINDOW: "$DATA_WINDOW"
      REPLAY_SPEED: "$REPLAY_SPEED"
      HOUSE_IDS: '$HOUSE_IDS_JSON'
      DATA_FILE: "$DATA_FILE"
      DATA_GLOB: "$DATA_GLOB"
      EPSILON_H: "$EPSILON_H"
      TAU: "$TAU"
      BATCH_INTERVAL_SECONDS: "$BATCH_INTERVAL_SECONDS"
      LOG_LEVEL: INFO
YAML
}

query_db() {
  local sql="$1"
  "${COMPOSE[@]}" exec -T timescaledb psql -U hiereb -d hiereb_db -At -F '|' -c "$sql"
}

assert_float_lt() {
  local value="$1"
  local max="$2"
  local message="$3"
  awk -v value="$value" -v max="$max" -v message="$message" 'BEGIN {
    if (!(value < max)) {
      printf("ASSERTION FAILED: %s (value=%s, expected < %s)\n", message, value, max) > "/dev/stderr";
      exit 1;
    }
  }'
}

assert_float_ge() {
  local value="$1"
  local min="$2"
  local message="$3"
  awk -v value="$value" -v min="$min" -v message="$message" 'BEGIN {
    if (!(value >= min)) {
      printf("ASSERTION FAILED: %s (value=%s, expected >= %s)\n", message, value, min) > "/dev/stderr";
      exit 1;
    }
  }'
}

start_infra() {
  write_override "full_tx" "e2e_bootstrap"
  "${COMPOSE[@]}" rm -f kafka-init >/dev/null 2>&1 || true
  "${COMPOSE[@]}" up -d kafka timescaledb grafana kafka-init
  "${COMPOSE[@]}" exec -T timescaledb psql -U hiereb -d hiereb_db < scripts/init_db.sql >/dev/null
}

stop_python_services() {
  "${COMPOSE[@]}" stop simulator aggregator ml-hiereb >/dev/null 2>&1 || true
  "${COMPOSE[@]}" rm -f simulator aggregator ml-hiereb >/dev/null 2>&1 || true
}

run_mode() {
  local mode="$1"
  local run_id="e2e_${mode}_$(date +%Y%m%d%H%M%S)"

  echo ""
  echo "=== Running E2E mode: $mode (RUN_ID=$run_id) ==="
  write_override "$mode" "$run_id"
  stop_python_services

  "${COMPOSE[@]}" up -d --build aggregator

  if [[ "$mode" == "hiereb" ]]; then
    "${COMPOSE[@]}" up -d --build ml-hiereb
    sleep 3
  fi

  "${COMPOSE[@]}" up -d --build simulator
  sleep "$E2E_RUNTIME_SECONDS"

  local stats
  stats="$(query_db "SELECT COUNT(*), COALESCE(AVG(tr), 0), COALESCE(SQRT(AVG(e_h * e_h)), 0) FROM house_metrics WHERE run_id = '$run_id';")"
  local row_count avg_tr rmse
  IFS='|' read -r row_count avg_tr rmse <<< "$stats"

  echo "Rows: $row_count"
  echo "Average TR: $avg_tr"
  echo "RMSE: $rmse"

  if [[ "${row_count:-0}" -le 0 ]]; then
    echo "ASSERTION FAILED: no rows written for RUN_ID=$run_id" >&2
    "${COMPOSE[@]}" logs --no-color --tail=120 aggregator simulator ml-hiereb >&2 || true
    exit 1
  fi

  case "$mode" in
    full_tx)
      assert_float_ge "$avg_tr" "0.999" "full_tx should transmit every plug"
      ;;
    uniform)
      assert_float_lt "$avg_tr" "0.999" "uniform mode should suppress at least some readings"
      ;;
    hiereb)
      "${COMPOSE[@]}" logs --no-color --tail=500 ml-hiereb | grep -q "predictions_published"
      "${COMPOSE[@]}" logs --no-color --tail=500 ml-hiereb | grep -q "thresholds_published"
      assert_float_lt "$avg_tr" "0.999" "hiereb mode should suppress after thresholds arrive"
      ;;
    *)
      echo "Unknown mode: $mode" >&2
      exit 1
      ;;
  esac

  stop_python_services
  RUN_IDS+=("$run_id")
  if [[ -n "$SUMMARY_FILE" ]]; then
    printf "%s,%s,%s,%s,%s\n" "$run_id" "$mode" "$row_count" "$avg_tr" "$rmse" >> "$SUMMARY_FILE"
  fi
  echo "Mode $mode passed."
}

case "$MODE_ARG" in
  all)
    MODES=(full_tx uniform hiereb)
    ;;
  full_tx|uniform|hiereb)
    MODES=("$MODE_ARG")
    ;;
  *)
    echo "Usage: bash scripts/verify_e2e.sh [all|full_tx|uniform|hiereb]" >&2
    exit 2
    ;;
esac

start_infra

for mode in "${MODES[@]}"; do
  run_mode "$mode"
done

echo ""
echo "All requested E2E checks passed."
echo "Run IDs: ${RUN_IDS[*]}"
