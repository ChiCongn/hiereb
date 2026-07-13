#!/usr/bin/env bash
# Inspect TimescaleDB health, schema, recent runs, and report metrics.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/inspect_common.sh"

section "TimescaleDB Status"
try_cmd "${COMPOSE[@]}" ps timescaledb
try_compose_exec timescaledb pg_isready -U hiereb -d hiereb_db

section "Database Version"
db_query "SELECT version();"

section "Tables"
db_query "
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;
"

section "Hypertables"
db_query "
SELECT hypertable_schema, hypertable_name, num_dimensions, num_chunks
FROM timescaledb_information.hypertables
ORDER BY hypertable_name;
"

section "Run Summary"
db_query "
SELECT
  run_id,
  MIN(time) AS start_time,
  MAX(time) AS end_time,
  COUNT(*) AS rows,
  ROUND(AVG(tr)::numeric, 4) AS avg_tr,
  ROUND(SQRT(AVG(e_h * e_h))::numeric, 4) AS rmse,
  ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY ABS(e_h))::numeric, 4) AS p90_abs_eh,
  ROUND(MAX(ABS(e_h))::numeric, 4) AS max_abs_eh
FROM house_metrics
WHERE run_id <> 'default'
GROUP BY run_id
ORDER BY MAX(time) DESC
LIMIT 15;
"

section "Latest House Metrics"
db_query "
SELECT *
FROM house_metrics
WHERE run_id <> 'default'
ORDER BY time DESC
LIMIT 10;
"

section "Threshold Log Summary"
db_query "
SELECT
  COUNT(*) AS rows,
  MIN(time) AS first_time,
  MAX(time) AS last_time,
  COUNT(DISTINCT plug_uid) AS plug_count
FROM threshold_log;
"

section "TimescaleDB Logs"
service_logs timescaledb
