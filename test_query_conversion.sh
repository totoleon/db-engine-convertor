#!/bin/bash
# Test script for query conversion feature

set -e

# Paths
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SOURCE_DB="/home/hailongli/bird_data/dev_20240627/dev_databases/california_schools/california_schools.sqlite"
MIGRATION_DIR="${SCRIPT_DIR}/migrations/sqlite_to_postgresql_california_schools_20251218_231420"
SOURCE_SCHEMA="${MIGRATION_DIR}/source/schema.sql"
TARGET_SCHEMA="${MIGRATION_DIR}/artifacts/postgresql_schema.sql"
QUERIES_FILE="${SCRIPT_DIR}/test_queries_california_schools.sql"

# PostgreSQL connection
PG_HOST="136.119.143.89"
PG_PORT="5432"
PG_USER="postgres"
PG_PASSWORD="Admin@1234"
PG_DATABASE="california_schools"

echo "=================================="
echo "Query Conversion Test"
echo "=================================="
echo "Source DB: ${SOURCE_DB}"
echo "Source Schema: ${SOURCE_SCHEMA}"
echo "Target Schema: ${TARGET_SCHEMA}"
echo "Queries: ${QUERIES_FILE}"
echo "Target: ${PG_USER}@${PG_HOST}:${PG_PORT}/${PG_DATABASE}"
echo "=================================="
echo ""

# Run conversion
python3 "${SCRIPT_DIR}/scripts/convert_queries.py" \
    --source-type sqlite \
    --target-type postgresql \
    --source-connection "${SOURCE_DB}" \
    --source-schema "${SOURCE_SCHEMA}" \
    --target-schema "${TARGET_SCHEMA}" \
    --queries-file "${QUERIES_FILE}" \
    --target-host "${PG_HOST}" \
    --target-port "${PG_PORT}" \
    --target-user "${PG_USER}" \
    --target-password "${PG_PASSWORD}" \
    --target-database "${PG_DATABASE}" \
    --max-attempts 3 \
    --output query_conversion_results.json

echo ""
echo "=================================="
echo "Test complete!"
echo "Results saved to: query_conversion_results.json"
echo "=================================="

