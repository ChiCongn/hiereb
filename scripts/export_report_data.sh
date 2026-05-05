#!/usr/bin/env bash
# Export report-ready CSV artifacts from TimescaleDB for one or more run_ids.
#
# Usage:
#   bash scripts/export_report_data.sh <run_id_1> [run_id_2 ...]
#   bash scripts/export_report_data.sh                  # latest N runs
#
# Optional env vars:
#   LATEST_N=3
#   HOUSE_ID=1
#   OUTPUT_DIR=results/report_YYYYmmdd_HHMMSS

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

LATEST_N="${LATEST_N:-3}"
HOUSE_ID="${HOUSE_ID:-1}"
OUTPUT_DIR="${OUTPUT_DIR:-results/report_$(date +%Y%m%d_%H%M%S)}"

COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.override.yml)

psql_exec() {
  "${COMPOSE[@]}" exec -T timescaledb psql -U hiereb -d hiereb_db "$@"
}

sql_escape_literal() {
  local value="$1"
  value="${value//\'/\'\'}"
  printf "%s" "$value"
}

copy_query_to_file() {
  local query="$1"
  local out_file="$2"
  psql_exec -At -c "COPY ($query) TO STDOUT WITH CSV HEADER" > "$out_file"
}

to_safe_id() {
  local value="$1"
  # Use printf to avoid converting trailing newline into underscore.
  printf "%s" "$value" | tr -cs 'A-Za-z0-9._-' '_'
}

readarray -t RUN_IDS < <(
  if [[ "$#" -gt 0 ]]; then
    printf "%s\n" "$@"
  else
    psql_exec -At -c "SELECT run_id FROM house_metrics WHERE run_id <> 'default' GROUP BY run_id ORDER BY MAX(time) DESC LIMIT ${LATEST_N};"
  fi
)

if [[ "${#RUN_IDS[@]}" -eq 0 ]]; then
  echo "No run_id found in house_metrics. Run experiments first." >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"

quoted_values=()
for run_id in "${RUN_IDS[@]}"; do
  escaped="$(sql_escape_literal "$run_id")"
  quoted_values+=("'$escaped'")
done
run_ids_sql="ARRAY[$(IFS=,; echo "${quoted_values[*]}")]::text[]"

summary_query="SELECT run_id, MIN(time) AS start_time, MAX(time) AS end_time, COUNT(*) AS rows, ROUND(AVG(tr)::numeric, 4) AS avg_tr, ROUND(SQRT(AVG(e_h * e_h))::numeric, 4) AS rmse, ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY ABS(e_h))::numeric, 4) AS p90_abs_eh, ROUND(MAX(ABS(e_h))::numeric, 4) AS max_abs_eh FROM house_metrics WHERE run_id = ANY($run_ids_sql) GROUP BY run_id ORDER BY run_id"
copy_query_to_file "$summary_query" "$OUTPUT_DIR/run_summary.csv"

for run_id in "${RUN_IDS[@]}"; do
  escaped="$(sql_escape_literal "$run_id")"
  safe_run_id="$(to_safe_id "$run_id")"

  ts_query="SELECT time, house_id, actual_load, pred_load, e_h, tr, plug_count, transmitted_count FROM house_metrics WHERE run_id = '$escaped' AND house_id = ${HOUSE_ID} ORDER BY time"
  copy_query_to_file "$ts_query" "$OUTPUT_DIR/timeseries_house_${HOUSE_ID}_${safe_run_id}.csv"

  minute_query="SELECT time_bucket('1 minute', time) AS time, house_id, AVG(tr) AS avg_tr, SQRT(AVG(e_h * e_h)) AS rmse_1m, PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY ABS(e_h)) AS p90_abs_eh_1m FROM house_metrics WHERE run_id = '$escaped' AND house_id = ${HOUSE_ID} GROUP BY 1, 2 ORDER BY 1"
  copy_query_to_file "$minute_query" "$OUTPUT_DIR/minute_metrics_house_${HOUSE_ID}_${safe_run_id}.csv"
done

{
  echo "# Report Artifacts"
  echo ""
  echo "Generated at: $(date -Iseconds)"
  echo "House ID: ${HOUSE_ID}"
  echo "Run IDs:"
  for run_id in "${RUN_IDS[@]}"; do
    echo "- ${run_id}"
  done
  echo ""
  echo "Files:"
  echo "- run_summary.csv"
  for run_id in "${RUN_IDS[@]}"; do
    safe_run_id="$(to_safe_id "$run_id")"
    echo "- timeseries_house_${HOUSE_ID}_${safe_run_id}.csv"
    echo "- minute_metrics_house_${HOUSE_ID}_${safe_run_id}.csv"
  done
} > "$OUTPUT_DIR/report_notes.md"

echo "Export complete: $OUTPUT_DIR"
