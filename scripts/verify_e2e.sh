#!/usr/bin/env bash
# Verify the HierEB streaming stack end-to-end without deleting existing DB data.
#
# Usage:
#   bash scripts/verify_e2e.sh            # runs full_tx, uniform, hiereb
#   bash scripts/verify_e2e.sh full_tx    # runs one mode
#
# Useful overrides:
#   E2E_RUNTIME_SECONDS=45 bash scripts/verify_e2e.sh hiereb
#   RUN_ID_OVERRIDE=sweep01_hiereb_house0_eps005 bash scripts/verify_e2e.sh hiereb
#   DATASET_PRESET=five_houses DATA_WINDOW=one_day FIVE_HOUSE_IDS='[0,1,2,10,11]' bash scripts/verify_e2e.sh hiereb
#   KEEP_STACK=1 bash scripts/verify_e2e.sh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

dotenv_or_default() {
  local key="$1"
  local fallback="$2"
  local value=""
  if [[ -f .env ]]; then
    value="$(sed -n "s/^${key}=//p" .env | tail -n 1)"
  fi
  printf "%s" "${value:-$fallback}"
}

MODE_ARG="${1:-all}"
E2E_RUNTIME_SECONDS="${E2E_RUNTIME_SECONDS:-35}"
REPLAY_SPEED="${REPLAY_SPEED:-$(dotenv_or_default REPLAY_SPEED 60)}"
DATASET_PRESET="${DATASET_PRESET:-$(dotenv_or_default DATASET_PRESET custom)}"
HOUSE_IDS_JSON="${HOUSE_IDS:-$(dotenv_or_default HOUSE_IDS '[1]')}"
ONE_HOUSE_ID="${ONE_HOUSE_ID:-$(dotenv_or_default ONE_HOUSE_ID 1)}"
FIVE_HOUSE_IDS_JSON="${FIVE_HOUSE_IDS:-$(dotenv_or_default FIVE_HOUSE_IDS '[0,1,2,3,4]')}"
DATA_FILE="${DATA_FILE:-$(dotenv_or_default DATA_FILE house-1.csv)}"
DATA_GLOB="${DATA_GLOB:-$(dotenv_or_default DATA_GLOB '')}"
DATA_WINDOW="${DATA_WINDOW:-$(dotenv_or_default DATA_WINDOW all)}"
STREAM_READ_CHUNK_SIZE="${STREAM_READ_CHUNK_SIZE:-$(dotenv_or_default STREAM_READ_CHUNK_SIZE 10000)}"
PROPERTY_FILTER="${PROPERTY_FILTER:-$(dotenv_or_default PROPERTY_FILTER 1)}"
EPSILON_H="${EPSILON_H:-$(dotenv_or_default EPSILON_H 0.05)}"
TAU="${TAU:-$(dotenv_or_default TAU 300)}"
BATCH_INTERVAL_SECONDS="${BATCH_INTERVAL_SECONDS:-$(dotenv_or_default BATCH_INTERVAL_SECONDS 300)}"
UNIFORM_DELTA="${UNIFORM_DELTA:-$(dotenv_or_default UNIFORM_DELTA 10.0)}"
SWEEP_ID="${SWEEP_ID:-$(dotenv_or_default SWEEP_ID sweep01)}"
IS_SWEEP="${IS_SWEEP:-$(dotenv_or_default IS_SWEEP false)}"
SWEEP_SIZE="${SWEEP_SIZE:-$(dotenv_or_default SWEEP_SIZE 0)}"
REDUCED_SWEEP="${REDUCED_SWEEP:-$(dotenv_or_default REDUCED_SWEEP false)}"
ML_READY_TIMEOUT_SECONDS="${ML_READY_TIMEOUT_SECONDS:-$(dotenv_or_default ML_READY_TIMEOUT_SECONDS 180)}"
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
      DATASET_PRESET: "$DATASET_PRESET"
      DATA_WINDOW: "$DATA_WINDOW"
      REPLAY_SPEED: "$REPLAY_SPEED"
      HOUSE_IDS: '$HOUSE_IDS_JSON'
      ONE_HOUSE_ID: "$ONE_HOUSE_ID"
      FIVE_HOUSE_IDS: '$FIVE_HOUSE_IDS_JSON'
      DATA_FILE: "$DATA_FILE"
      DATA_GLOB: "$DATA_GLOB"
      STREAM_READ_CHUNK_SIZE: "$STREAM_READ_CHUNK_SIZE"
      PROPERTY_FILTER: "$PROPERTY_FILTER"
      EPSILON_H: "$EPSILON_H"
      SWEEP_ID: "$SWEEP_ID"
      IS_SWEEP: "$IS_SWEEP"
      SWEEP_SIZE: "$SWEEP_SIZE"
      REDUCED_SWEEP: "$REDUCED_SWEEP"
      TAU: "$TAU"
      BATCH_INTERVAL_SECONDS: "$BATCH_INTERVAL_SECONDS"
      UNIFORM_DELTA: "$UNIFORM_DELTA"
      LOG_LEVEL: INFO

  aggregator:
    environment:
      RUN_ID: "$run_id"
      SWEEP_ID: "$SWEEP_ID"
      IS_SWEEP: "$IS_SWEEP"
      SWEEP_SIZE: "$SWEEP_SIZE"
      REDUCED_SWEEP: "$REDUCED_SWEEP"
      DB_WRITE_BATCH_SIZE: "$DB_WRITE_BATCH_SIZE"
      LOG_LEVEL: INFO

  ml-hiereb:
    environment:
      SUPPRESSION_MODE: "$mode"
      RUN_ID: "$run_id"
      DATASET_PRESET: "$DATASET_PRESET"
      DATA_WINDOW: "$DATA_WINDOW"
      REPLAY_SPEED: "$REPLAY_SPEED"
      HOUSE_IDS: '$HOUSE_IDS_JSON'
      ONE_HOUSE_ID: "$ONE_HOUSE_ID"
      FIVE_HOUSE_IDS: '$FIVE_HOUSE_IDS_JSON'
      DATA_FILE: "$DATA_FILE"
      DATA_GLOB: "$DATA_GLOB"
      PROPERTY_FILTER: "$PROPERTY_FILTER"
      EPSILON_H: "$EPSILON_H"
      SWEEP_ID: "$SWEEP_ID"
      IS_SWEEP: "$IS_SWEEP"
      SWEEP_SIZE: "$SWEEP_SIZE"
      REDUCED_SWEEP: "$REDUCED_SWEEP"
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

wait_for_ml_ready() {
  local deadline=$((SECONDS + ML_READY_TIMEOUT_SECONDS))
  local ml_logs=""
  until {
    ml_logs="$("${COMPOSE[@]}" logs --no-color --tail=500 ml-hiereb 2>/dev/null || true)"
    grep -q "predictions_published" <<< "$ml_logs"
  }; do
    if (( SECONDS >= deadline )); then
      echo "ASSERTION FAILED: ml-hiereb did not publish predictions within ${ML_READY_TIMEOUT_SECONDS}s" >&2
      "${COMPOSE[@]}" logs --no-color --tail=200 ml-hiereb >&2 || true
      exit 1
    fi
    sleep 2
  done
}

run_mode() {
  local mode="$1"
  local run_id="${RUN_ID_OVERRIDE:-}"
  if [[ -z "$run_id" ]]; then
    run_id="${RUN_ID:-}"
  fi
  if [[ -z "$run_id" ]]; then
    run_id="e2e_${mode}_$(date +%Y%m%d%H%M%S)"
  fi

  echo ""
  echo "=== Running E2E mode: $mode (RUN_ID=$run_id) ==="
  write_override "$mode" "$run_id"
  stop_python_services

  "${COMPOSE[@]}" up -d --build aggregator

  if [[ "$mode" != "full_tx" ]]; then
    "${COMPOSE[@]}" up -d --build ml-hiereb
    wait_for_ml_ready
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
      ml_logs="$("${COMPOSE[@]}" logs --no-color --tail=500 ml-hiereb 2>/dev/null || true)"
      grep -q "predictions_published" <<< "$ml_logs"
      grep -q "thresholds_published" <<< "$ml_logs"
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
