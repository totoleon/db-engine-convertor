#!/bin/bash
# Convenience wrapper for database conversion using the new CLI

set -e

# Configuration
SQLITE_DB="${1:-/home/hailongli/bird_data/dev_20240627/dev_databases/california_schools/california_schools.sqlite}"
PG_HOST="${2:-136.119.143.89}"
PG_PORT="${3:-5432}"
PG_USER="${4:-postgres}"
PG_PASSWORD="${5:-Admin@1234}"
PG_DATABASE="${6:-california_schools}"
MAX_ATTEMPTS="${7:-10}"

# Paths
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WORK_DIR="$(dirname "$SCRIPT_DIR")"
CLI_SCRIPT="${SCRIPT_DIR}/convert_database.py"

echo "=================================="
echo "SQLite to PostgreSQL Converter"
echo "=================================="
echo "SQLite DB: ${SQLITE_DB}"
echo "PostgreSQL: ${PG_USER}@${PG_HOST}:${PG_PORT}/${PG_DATABASE}"
echo "Work dir: ${WORK_DIR}"
echo "=================================="

# Run full conversion (export + convert integrated)
echo ""
echo "Running AI-powered conversion..."
echo "Flow: Source Export → AI Generation → Pipeline Execution"
echo ""

python3 "${CLI_SCRIPT}" convert \
    --source-type sqlite \
    --target-type postgresql \
    --source-connection "${SQLITE_DB}" \
    --target-host "${PG_HOST}" \
    --target-port "${PG_PORT}" \
    --target-user "${PG_USER}" \
    --target-password "${PG_PASSWORD}" \
    --target-database "${PG_DATABASE}" \
    --work-dir "${WORK_DIR}" \
    --max-attempts "${MAX_ATTEMPTS}"

echo ""
echo "=================================="
echo "Conversion complete!"
echo "=================================="

