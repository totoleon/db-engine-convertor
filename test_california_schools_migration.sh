#!/bin/bash
# Complete test: Data migration + Query conversion for California Schools

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SOURCE_DB="/home/hailongli/bird_data/dev_20240627/dev_databases/california_schools/california_schools.sqlite"
BIRD_JSON="/home/hailongli/bird_data/dev_20240627/dev.json"
QUERIES_CSV="${SCRIPT_DIR}/california_schools_queries.csv"

# PostgreSQL connection
PG_HOST="136.119.143.89"
PG_PORT="5432"
PG_USER="postgres"
PG_PASSWORD="Admin@1234"
PG_DATABASE="california_schools"

echo "============================================="
echo "California Schools: Data + Query Migration"
echo "============================================="
echo ""

# Step 1: Extract queries if not already done
if [ ! -f "${QUERIES_CSV}" ]; then
    echo "Step 1: Extracting queries from BIRD dataset..."
    python3 "${SCRIPT_DIR}/scripts/extract_queries_from_bird.py" \
        "${BIRD_JSON}" \
        --db-id california_schools \
        --output "${QUERIES_CSV}"
    echo ""
fi

# Step 2: Run data migration
echo "Step 2: Running data migration..."
"${SCRIPT_DIR}/scripts/convert_db.sh" \
    --source-connection "${SOURCE_DB}" \
    --dest-host "${PG_HOST}" \
    --dest-user "${PG_USER}" \
    --dest-password "${PG_PASSWORD}" \
    --dest-database "${PG_DATABASE}" \
    --max-attempts 5

# Find the latest migration directory
MIGRATION_DIR=$(ls -td "${SCRIPT_DIR}"/migrations/sqlite_to_postgresql_california_schools_* 2>/dev/null | head -1)

if [ -z "${MIGRATION_DIR}" ]; then
    echo "Error: No migration directory found"
    exit 1
fi

echo ""
echo "Step 3: Running query conversion..."
echo "Migration directory: ${MIGRATION_DIR}"
echo ""

# Step 3: Run query conversion
python3 "${SCRIPT_DIR}/scripts/convert_queries.py" \
    --source-type sqlite \
    --target-type postgresql \
    --source-connection "${SOURCE_DB}" \
    --source-schema "${MIGRATION_DIR}/source/schema.sql" \
    --target-schema "${MIGRATION_DIR}/artifacts/postgresql_schema.sql" \
    --queries-file "${QUERIES_CSV}" \
    --target-host "${PG_HOST}" \
    --target-port "${PG_PORT}" \
    --target-user "${PG_USER}" \
    --target-password "${PG_PASSWORD}" \
    --target-database "${PG_DATABASE}" \
    --max-attempts 3 \
    --output "${MIGRATION_DIR}/query_conversion_results.csv"

echo ""
echo "============================================="
echo "Migration Complete!"
echo "============================================="
echo "Migration directory: ${MIGRATION_DIR}"
echo "Data conversion: ${MIGRATION_DIR}/artifacts/"
echo "Query conversion: ${MIGRATION_DIR}/query_conversion_results.csv"
echo ""
echo "Review results:"
echo "  cat ${MIGRATION_DIR}/query_conversion_results.csv | head -20"
echo "============================================="

