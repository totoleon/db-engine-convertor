# Examples

This directory contains example queries and test scripts for the database conversion toolkit.

## Files

- **`california_schools_queries.csv`**: 89 example queries extracted from the BIRD dataset for the California Schools database
- **`test_california_schools_migration.sh`**: Test script for full data migration of California Schools database
- **`test_query_conversion.sh`**: Test script for query conversion only
- **`test_queries_california_schools.sql`**: Sample SQLite queries for manual testing

## Query CSV Format

The query conversion system expects a CSV file with the following columns:

```csv
question_id,source_query
0,"SELECT COUNT(*) FROM schools WHERE City = 'Fresno'"
1,"SELECT AVG(score) FROM satscores WHERE cds LIKE '01%'"
...
```

## Running Examples

### Full Migration + Query Conversion
```bash
./examples/test_california_schools_migration.sh
```

### Query Conversion Only
```bash
./examples/test_query_conversion.sh
```

## Extracting Queries from BIRD Dataset

If you have the BIRD dataset, you can extract queries using:

```bash
python3 scripts/extract_queries_from_bird.py \
  --bird-json ~/bird_data/dev/dev.json \
  --database-name california_schools \
  --output-csv california_schools_queries.csv
```

See `../README.md` for full documentation.

