#!/usr/bin/env bash
# Shared helpers for HierEB inspection scripts.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -f "$ROOT_DIR/docker-compose.yml" -f "$ROOT_DIR/docker-compose.override.yml")
LOG_LINES="${LOG_LINES:-120}"

cd "$ROOT_DIR"

section() {
  printf "\n==== %s ====\n" "$*"
}

print_cmd() {
  printf "\n$"
  printf " %q" "$@"
  printf "\n"
}

run_cmd() {
  print_cmd "$@"
  "$@"
}

try_cmd() {
  print_cmd "$@"
  "$@" || true
}

compose_cmd() {
  "${COMPOSE[@]}" "$@"
}

service_running() {
  local service="$1"
  compose_cmd ps --status running --services "$service" 2>/dev/null | grep -Fxq "$service"
}

compose_exec() {
  local service="$1"
  shift

  if service_running "$service"; then
    run_cmd "${COMPOSE[@]}" exec -T "$service" "$@"
  else
    echo "SKIP: service '$service' is not running."
  fi
}

try_compose_exec() {
  compose_exec "$@" || true
}

service_logs() {
  local service="$1"
  try_cmd "${COMPOSE[@]}" logs --no-color --tail="$LOG_LINES" "$service"
}

db_query() {
  local sql="$1"
  try_compose_exec timescaledb psql -U hiereb -d hiereb_db -P pager=off -c "$sql"
}

kafka_tool() {
  local tool="$1"
  shift
  try_compose_exec kafka "/opt/kafka/bin/${tool}" --bootstrap-server localhost:9092 "$@"
}

kafka_offsets() {
  local topic="$1"
  try_compose_exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 \
    --topic "$topic"
}

service_env() {
  local service="$1"
  shift

  if service_running "$service"; then
    print_cmd "${COMPOSE[@]}" exec -T "$service" env
    "${COMPOSE[@]}" exec -T "$service" env | sort | grep -E "$*" || true
  else
    echo "SKIP: service '$service' is not running."
  fi
}
