#!/usr/bin/env bash
# init_db_manual.sh
#
# Manually applies the DB schema to TimescaleDB.
# Run this when init_db.sql is NOT auto-mounted in docker-compose
# (i.e., the volume mount line is commented out).
#
# Usage:
#   bash scripts/init_db_manual.sh
#
# Safe to run multiple times (uses CREATE IF NOT EXISTS).

set -euo pipefail

echo "Applying HierEB schema to TimescaleDB..."

docker exec -i timescaledb psql -U hiereb -d hiereb_db \
  < scripts/init_db.sql

echo ""
echo "Verifying tables:"
docker exec timescaledb psql -U hiereb -d hiereb_db -c \
  "\dt"

echo ""
echo "Verifying hypertable:"
docker exec timescaledb psql -U hiereb -d hiereb_db -c \
  "SELECT hypertable_name, num_chunks FROM timescaledb_information.hypertables;"

echo ""
echo "✓ Schema applied successfully."