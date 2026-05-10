#!/usr/bin/env bash
# Inspect generated demo/report artifacts. No Docker required.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DEMO_DIR="${1:-}"

section() {
  printf "\n==== %s ====\n" "$*"
}

if [[ -z "$DEMO_DIR" ]]; then
  if compgen -G "results/demo_*" >/dev/null; then
    DEMO_DIR="$(ls -dt results/demo_* | head -n 1)"
  else
    echo "No results/demo_* directory found. Run: bash scripts/demo_report.sh"
    exit 0
  fi
fi

if [[ ! -d "$DEMO_DIR" ]]; then
  echo "Demo directory not found: $DEMO_DIR" >&2
  exit 1
fi

section "Demo Directory"
echo "$DEMO_DIR"

section "Artifact Files"
find "$DEMO_DIR" -maxdepth 2 -type f -printf '%p\t%k KB\n' | sort

section "E2E Summary"
if [[ -f "$DEMO_DIR/e2e_summary.csv" ]]; then
  sed -n '1,40p' "$DEMO_DIR/e2e_summary.csv"
else
  echo "Missing: $DEMO_DIR/e2e_summary.csv"
fi

section "Export Run Summary"
if [[ -f "$DEMO_DIR/export/run_summary.csv" ]]; then
  sed -n '1,40p' "$DEMO_DIR/export/run_summary.csv"
else
  echo "Missing: $DEMO_DIR/export/run_summary.csv"
fi

section "Chart Index"
if [[ -f "$DEMO_DIR/charts/charts_index.md" ]]; then
  sed -n '1,120p' "$DEMO_DIR/charts/charts_index.md"
else
  echo "Missing: $DEMO_DIR/charts/charts_index.md"
fi

section "Report Notes"
if [[ -f "$DEMO_DIR/export/report_notes.md" ]]; then
  sed -n '1,120p' "$DEMO_DIR/export/report_notes.md"
else
  echo "Missing: $DEMO_DIR/export/report_notes.md"
fi
