#!/usr/bin/env bash
# Inspect the aggregator service: consumer lag, DB writes, env, and logs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/inspect_common.sh"

section "Aggregator Status"
try_cmd "${COMPOSE[@]}" ps aggregator

section "Aggregator Runtime Environment"
service_env aggregator '^(RUN_ID|KAFKA_BOOTSTRAP_SERVERS|DB_HOST|DB_PORT|DB_NAME|DB_USER|DB_WRITE_BATCH_SIZE|LOG_LEVEL)='

section "Aggregator Consumer Group"
kafka_tool kafka-consumer-groups.sh --describe --group hiereb-aggregator

section "Recent Metrics Written"
db_query "
SELECT
  run_id,
  house_id,
  time,
  actual_load,
  pred_load,
  e_h,
  tr,
  plug_count,
  transmitted_count
FROM house_metrics
WHERE run_id <> 'default'
ORDER BY time DESC
LIMIT 20;
"

section "Run-Level Aggregates"
db_query "
SELECT
  run_id,
  house_id,
  COUNT(*) AS rows,
  ROUND(AVG(tr)::numeric, 4) AS avg_tr,
  ROUND(SQRT(AVG(e_h * e_h))::numeric, 4) AS rmse
FROM house_metrics
WHERE run_id <> 'default'
GROUP BY run_id, house_id
ORDER BY MAX(time) DESC, house_id
LIMIT 20;
"

section "Aggregator Logs"
service_logs aggregator
