# Conversion Path Comparison Example

This example demonstrates comparing two different conversion paths:
- **Path 1 (Multi-hop)**: SQLite → PostgreSQL → MySQL
- **Path 2 (Direct)**: SQLite → MySQL

**See also:** `CONVERSION_COMPARISON_RESULTS.md` for detailed analysis and key findings.

## Test Dataset
- **Database**: california_schools (from BIRD benchmark)
- **Queries**: 89 SQL queries
- **Tables**: schools, frpm, satscores

## Testing Steps

### Step 1: SQLite → PostgreSQL Query Conversion
```bash
python3 scripts/convert_queries.py \
  --source-type sqlite \
  --source-connection ~/bird_data/dev_20240627/dev_databases/california_schools/california_schools.sqlite \
  --source-schema migrations/sqlite_to_postgresql_california_schools_*/source/schema.sql \
  --target-type postgresql \
  --target-schema migrations/sqlite_to_postgresql_california_schools_*/artifacts/postgresql_schema.sql \
  --target-host <pg-host> --target-port 5432 \
  --target-user postgres --target-password <password> \
  --target-database california_schools \
  --queries-file examples/conversion_path_comparison/california_schools_queries.csv \
  --output step1_sqlite_to_pg_queries.csv \
  --max-attempts 10 --num-workers 10
```

**Result**: 75/89 queries matched (84.3%)

### Step 2: PostgreSQL → MySQL Query Conversion
Extract successful PG queries from Step 1, then:
```bash
python3 scripts/convert_queries.py \
  --source-type postgresql \
  --source-host <pg-host> --source-port 5432 \
  --source-user postgres --source-password <password> \
  --source-database california_schools \
  --source-schema migrations/postgresql_to_mysql_*/source/schema.sql \
  --target-type mysql \
  --target-schema migrations/postgresql_to_mysql_*/artifacts/mysql_schema.sql \
  --target-host <mysql-host> --target-port 3306 \
  --target-user <user> --target-password <password> \
  --target-database california_schools_from_pg \
  --queries-file step2_pg_queries.csv \
  --output step2_pg_to_mysql_queries.csv \
  --max-attempts 10 --num-workers 10
```

**Result**: 71/73 queries matched (97.3%)

### Step 3: SQLite → MySQL Direct Conversion
```bash
python3 scripts/convert_queries.py \
  --source-type sqlite \
  --source-connection ~/bird_data/dev_20240627/dev_databases/california_schools/california_schools.sqlite \
  --source-schema migrations/sqlite_to_mysql_*/source/schema.sql \
  --target-type mysql \
  --target-schema migrations/sqlite_to_mysql_*/artifacts/mysql_schema.sql \
  --target-host <mysql-host> --target-port 3306 \
  --target-user <user> --target-password <password> \
  --target-database california_schools \
  --queries-file examples/conversion_path_comparison/california_schools_queries.csv \
  --output step3_sqlite_to_mysql_queries.csv \
  --max-attempts 10 --num-workers 10
```

**Result**: 82/89 queries matched (92.1%)

## Files in this Directory

### Input Files
- `california_schools_queries.csv` - Original 89 SQLite queries (reusable for future tests)

### Result Files
- `step1_sqlite_to_pg_queries.csv` - SQLite → PostgreSQL conversion results
- `step2_pg_to_mysql_queries.csv` - PostgreSQL → MySQL conversion results
- `step3_sqlite_to_mysql_queries.csv` - Direct SQLite → MySQL conversion results
- `mysql_queries_side_by_side.csv` - **Final comparison** with both paths side-by-side

### Analysis
- `CONVERSION_COMPARISON_RESULTS.md` - Detailed analysis and key findings
- `README.md` - This file (test methodology and replication steps)

## Key Findings

### Success Rates
- SQLite → PostgreSQL: 84.3%
- PostgreSQL → MySQL: 97.3%
- SQLite → MySQL (direct): 92.1%

### Head-to-Head Comparison (73 queries)
- Both paths succeeded: 71/73 (97.3%)
- Direct path only: 2/73 (2.7%)
- Multi-hop only: 0/73 (0%)

### Query Differences
- Only 9/71 queries were **identical** between paths
- 62/71 queries **differ cosmetically** (backtick usage, formatting) but produce same results
- **Path 1 (via PG)**: Consistent backticks everywhere
- **Path 2 (direct)**: Selective backticks, only for complex identifiers

## Conclusion
Both conversion paths are viable with high success rates. Multi-hop conversion demonstrates the system's composability, while direct conversion has slightly higher overall success due to avoiding intermediate steps.

## Reusing This Example

To run similar comparisons with other databases:

1. Replace `california_schools_queries.csv` with your queries
2. Update connection parameters in commands above
3. Ensure migrations exist for your database
4. Run the three steps sequentially
5. Generate side-by-side comparison (see repo scripts)

