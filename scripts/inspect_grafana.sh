#!/usr/bin/env bash
# Inspect Grafana health, provisioning files, dashboard files, and recent logs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/inspect_common.sh"

section "Grafana Status"
try_cmd "${COMPOSE[@]}" ps grafana

section "Grafana Health API"
try_compose_exec grafana wget -q -O- http://localhost:3000/api/health

section "Provisioned Datasources"
find grafana/provisioning/datasources -maxdepth 1 -type f -print -exec sed -n '1,180p' {} \; 2>/dev/null || true

section "Provisioned Dashboard Providers"
find grafana/provisioning/dashboards -maxdepth 1 -type f -print -exec sed -n '1,180p' {} \; 2>/dev/null || true

section "Dashboard Files"
find grafana/dashboards -maxdepth 1 -type f -print 2>/dev/null || true

section "Dashboard Titles"
if compgen -G "grafana/dashboards/*.json" >/dev/null; then
  grep -H '"title"' grafana/dashboards/*.json || true
else
  echo "No dashboard JSON files found."
fi

section "Grafana Logs"
service_logs grafana
