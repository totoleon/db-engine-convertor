#!/bin/bash
# Convenience script to convert SQLite database to PostgreSQL

set -e

# Configuration
SQLITE_DB="${1:-/home/hailongli/bird_data/dev_20240627/dev_databases/california_schools/california_schools.sqlite}"
PG_HOST="${2:-136.119.143.89}"
PG_PORT="${3:-5432}"
PG_USER="${4:-postgres}"
PG_PASSWORD="${5:-Admin@1234}"
PG_DATABASE="${6:-california_schools}"
MAX_ATTEMPTS="${7:-10}"

# Derived paths
WORK_DIR="/home/hailongli/db-engine-convertor"
EXPORT_DIR="${WORK_DIR}/sqlite_export"
SCHEMA_FILE="${EXPORT_DIR}/schema.sql"
CSV_DIR="${EXPORT_DIR}"

echo "=================================="
echo "SQLite to PostgreSQL Converter"
echo "=================================="
echo "SQLite DB: ${SQLITE_DB}"
echo "PostgreSQL: ${PG_USER}@${PG_HOST}:${PG_PORT}/${PG_DATABASE}"
echo "Work dir: ${WORK_DIR}"
echo "=================================="

cd "${WORK_DIR}"

# Step 1: Export SQLite schema and data
echo ""
echo "Step 1: Exporting SQLite database..."
python3 sqlite_export.py "${SQLITE_DB}" -o "${EXPORT_DIR}"

# Step 2: Run orchestrator to convert and migrate
echo ""
echo "Step 2: Running AI-powered conversion orchestrator..."
python3 orchestrator.py \
    --sqlite-schema "${SCHEMA_FILE}" \
    --source-csv "${CSV_DIR}" \
    --pg-host "${PG_HOST}" \
    --pg-port "${PG_PORT}" \
    --pg-user "${PG_USER}" \
    --pg-password "${PG_PASSWORD}" \
    --pg-database "${PG_DATABASE}" \
    --max-attempts "${MAX_ATTEMPTS}"

echo ""
echo "=================================="
echo "Conversion complete!"
echo "=================================="

