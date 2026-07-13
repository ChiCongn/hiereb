#!/usr/bin/env bash
# Inspect the simulator service: runtime env, Kafka output offsets, and recent logs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/inspect_common.sh"

section "Simulator Status"
try_cmd "${COMPOSE[@]}" ps simulator

section "Simulator Runtime Environment"
service_env simulator '^(SUPPRESSION_MODE|RUN_ID|REPLAY_SPEED|STREAM_TIME_MODE|DATASET_PRESET|DATA_WINDOW|ONE_HOUSE_ID|FIVE_HOUSE_IDS|HOUSE_IDS|AUTO_DETECT_HOUSE_IDS|AUTO_DETECT_TIME_WINDOW|DATA_FILE|DATA_GLOB|DATA_PATH|STREAM_READ_CHUNK_SIZE|PROPERTY_FILTER|WARMUP_START|WARMUP_END|EVAL_START|EVAL_END|EPSILON_H|EPSILON_RATIO|TAU|BATCH_INTERVAL_SECONDS|UNIFORM_DELTA|KAFKA_BOOTSTRAP_SERVERS|LOG_LEVEL)='

section "Sensor Data Topic Offset"
kafka_offsets hiereb.sensor_data

section "Variance Topic Offset"
kafka_offsets hiereb.variance

section "Latest Run Rows Written To DB"
db_query "
SELECT
  run_id,
  COUNT(*) AS rows,
  ROUND(AVG(tr)::numeric, 4) AS avg_tr,
  MIN(time) AS first_event_time,
  MAX(time) AS last_event_time
FROM house_metrics
WHERE run_id <> 'default'
GROUP BY run_id
ORDER BY MAX(time) DESC
LIMIT 10;
"

section "Simulator Logs"
service_logs simulator
