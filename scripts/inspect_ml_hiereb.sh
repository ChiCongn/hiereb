#!/usr/bin/env bash
# Inspect the ML + HierEB service: predictions, thresholds, variance input, env, and logs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/inspect_common.sh"

section "ML-HierEB Status"
try_cmd "${COMPOSE[@]}" ps ml-hiereb

section "ML-HierEB Runtime Environment"
service_env ml-hiereb '^(SUPPRESSION_MODE|RUN_ID|REPLAY_SPEED|STREAM_TIME_MODE|DATASET_PRESET|DATA_WINDOW|ONE_HOUSE_ID|FIVE_HOUSE_IDS|HOUSE_IDS|AUTO_DETECT_HOUSE_IDS|AUTO_DETECT_TIME_WINDOW|DATA_FILE|DATA_GLOB|DATA_PATH|PROPERTY_FILTER|WARMUP_START|WARMUP_END|EVAL_START|EVAL_END|EPSILON_H|EPSILON_RATIO|TAU|BATCH_INTERVAL_SECONDS|KAFKA_BOOTSTRAP_SERVERS|LOG_LEVEL)='

section "ML Consumer Group"
kafka_tool kafka-consumer-groups.sh --describe --group hiereb-ml

section "Prediction Topic Offset"
kafka_offsets hiereb.predictions

section "Threshold Topic Offset"
kafka_offsets hiereb.thresholds

section "Variance Topic Offset"
kafka_offsets hiereb.variance

section "Threshold Log Summary"
db_query "
SELECT
  COUNT(*) AS rows,
  MIN(time) AS first_time,
  MAX(time) AS last_time,
  COUNT(DISTINCT plug_uid) AS plug_count,
  ROUND(AVG(delta_p)::numeric, 4) AS avg_delta_p
FROM threshold_log;
"

section "Recent ML Logs: Predictions"
if service_running ml-hiereb; then
  print_cmd "${COMPOSE[@]}" logs --no-color --tail="$LOG_LINES" ml-hiereb
  "${COMPOSE[@]}" logs --no-color --tail="$LOG_LINES" ml-hiereb | grep -E 'predictions_published|thresholds_published|error_budget_resolved|training_split' || true
else
  echo "SKIP: service 'ml-hiereb' is not running."
fi

section "ML-HierEB Logs"
service_logs ml-hiereb
