#!/usr/bin/env bash
# Inspect the whole HierEB stack: services, health, Kafka topics, DB summary, artifacts.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/inspect_common.sh"

section "Compose Services"
try_cmd "${COMPOSE[@]}" config --services

section "Container Status"
try_cmd "${COMPOSE[@]}" ps

section "Resource Snapshot"
try_cmd "${COMPOSE[@]}" stats --no-stream

section "Kafka Topics"
kafka_tool kafka-topics.sh --list
for topic in hiereb.sensor_data hiereb.predictions hiereb.thresholds hiereb.variance; do
  kafka_tool kafka-topics.sh --describe --topic "$topic"
done

section "Database Run Summary"
db_query "
SELECT
  run_id,
  MIN(time) AS start_time,
  MAX(time) AS end_time,
  COUNT(*) AS rows,
  ROUND(AVG(tr)::numeric, 4) AS avg_tr,
  ROUND(SQRT(AVG(e_h * e_h))::numeric, 4) AS rmse
FROM house_metrics
WHERE run_id <> 'default'
GROUP BY run_id
ORDER BY MAX(time) DESC
LIMIT 10;
"

section "Latest Metrics By House"
db_query "
SELECT
  run_id,
  house_id,
  COUNT(*) AS rows,
  ROUND(AVG(tr)::numeric, 4) AS avg_tr,
  ROUND(SQRT(AVG(e_h * e_h))::numeric, 4) AS rmse,
  ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY ABS(e_h))::numeric, 4) AS p90_abs_eh
FROM house_metrics
WHERE run_id <> 'default'
GROUP BY run_id, house_id
ORDER BY MAX(time) DESC, house_id
LIMIT 20;
"

section "Grafana Endpoint"
try_compose_exec grafana wget -q -O- http://localhost:3000/api/health

section "Latest Demo Artifacts"
if compgen -G "results/demo_*" >/dev/null; then
  latest_demo="$(ls -dt results/demo_* | head -n 1)"
  echo "Latest demo directory: $latest_demo"
  find "$latest_demo" -maxdepth 2 -type f | sort
else
  echo "No results/demo_* directory found."
fi

section "Useful URLs"
echo "Grafana: http://localhost:3000"
echo "Kafka external listener: localhost:9092"
echo "TimescaleDB external port: localhost:5433"
