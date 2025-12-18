# SQLite to PostgreSQL Database Converter

An AI-powered database migration tool that automatically converts SQLite databases to PostgreSQL using Gemini AI.

## Overview

This system uses an iterative AI agent approach to convert SQLite databases to PostgreSQL:

1. **Export**: Extracts schema and data from SQLite database
2. **Agent**: Uses Gemini AI to generate PostgreSQL schema and data conversion script
3. **Pipeline**: Tests the conversion by creating schema and loading data
4. **Iterate**: If errors occur, feeds them back to the agent for correction
5. **Success**: Continues until migration succeeds or max attempts reached

## Architecture

```
┌─────────────────┐
│  SQLite DB      │
└────────┬────────┘
         │
         v
┌─────────────────┐
│ sqlite_export.py│  ← Exports schema.sql and CSV files
└────────┬────────┘
         │
         v
┌─────────────────┐
│  orchestrator.py│  ← Main coordinator
└────────┬────────┘
         │
         ├──→ ┌─────────────┐
         │    │  agent.py   │  ← Calls Gemini AI to generate:
         │    └─────────────┘     • pg_schema.sql
         │                        • csv_convertor.py
         │
         └──→ ┌─────────────┐
              │ pipeline.py │  ← Tests conversion:
              └─────────────┘     1. Wipe DB
                                  2. Create schema
                                  3. Convert CSV
                                  4. Load data
                   │
                   v
              ┌─────────────┐
              │ PostgreSQL  │
              └─────────────┘
```

## Components

### 1. `sqlite_export.py`
Exports SQLite database to:
- `schema.sql`: All CREATE statements
- `*.csv`: Table data files

### 2. `agent.py`
AI agent that generates:
- `pg_schema.sql`: PostgreSQL-compatible schema
- `csv_convertor.py`: Script to convert CSV data for PostgreSQL

The agent sees:
- SQLite schema
- CSV data summaries (first/last 5 rows, columns, row counts)
- Previous PostgreSQL schema (if any)
- Previous convertor script (if any)
- Pipeline errors (if any)

### 3. `pipeline.py`
Executes the conversion:
1. **Wipe**: Drop and recreate public schema
2. **Schema**: Create tables from `pg_schema.sql`
3. **Convert**: Run `csv_convertor.py` to transform data
4. **Load**: Use PostgreSQL COPY to load converted CSV files

### 4. `orchestrator.py`
Main coordinator that:
- Calls agent to generate conversion artifacts
- Runs pipeline to test conversion
- If pipeline fails, calls agent again with error feedback
- Repeats until success or max attempts

### 5. `convert_db.sh`
Convenience script that runs the full process.

## Usage

### Quick Start

```bash
cd /home/hailongli/db-engine-convertor

# Use defaults (California schools database)
./convert_db.sh

# Or specify custom parameters
./convert_db.sh \
    /path/to/database.sqlite \
    pg_host \
    pg_port \
    pg_user \
    pg_password \
    pg_database \
    max_attempts
```

### Manual Usage

#### Step 1: Export SQLite Database

```bash
python3 sqlite_export.py /path/to/database.sqlite -o sqlite_export
```

#### Step 2: Run Orchestrator

```bash
python3 orchestrator.py \
    --sqlite-schema sqlite_export/schema.sql \
    --source-csv sqlite_export \
    --pg-host 136.119.143.89 \
    --pg-port 5432 \
    --pg-user postgres \
    --pg-password 'Admin@1234' \
    --pg-database california_schools \
    --max-attempts 10
```

## Configuration

### PostgreSQL Connection

Default connection used in `convert_db.sh`:
```bash
Host: 136.119.143.89
Port: 5432
User: postgres
Password: Admin@1234
Database: california_schools
```

### Environment Variables

The system uses `GEMINI_API_KEY` from environment or falls back to Vertex AI.

```bash
export GEMINI_API_KEY="your-api-key-here"
```

## How It Works

### Agent Prompt Structure

The agent receives:
1. **SQLite Schema**: Original table definitions
2. **CSV Summaries**: Data samples and statistics
3. **Previous Artifacts**: Prior pg_schema.sql and csv_convertor.py (with line numbers)
4. **Pipeline Errors**: Detailed error messages from failed attempts

The agent outputs JSON:
```json
{
  "pg_schema": "CREATE TABLE ...",
  "csv_convertor": "#!/usr/bin/env python3\n..."
}
```

### Conversion Rules

The agent handles:
- Type conversions (TEXT→VARCHAR, INTEGER→BIGINT, REAL→NUMERIC, etc.)
- Foreign key syntax
- Quoted identifiers
- NULL handling
- Date/boolean conversions
- Data precision adjustments

### Error Feedback Loop

When pipeline fails:
1. Error is captured (with full traceback)
2. Error is saved to `pipeline_error.txt`
3. Agent is called again with error context
4. Agent generates corrected artifacts
5. Pipeline runs again
6. Repeat until success or max attempts

## Output Files

After successful conversion:
- `pg_schema.sql`: Final PostgreSQL schema
- `csv_convertor.py`: Final conversion script
- `converted_csv/`: Directory with converted CSV files
- `pipeline_error.txt`: Last error (if any)

## Example

```bash
cd /home/hailongli/db-engine-convertor

# Convert California schools database
./convert_db.sh \
    /home/hailongli/bird_data/dev_20240627/dev_databases/california_schools/california_schools.sqlite \
    136.119.143.89 \
    5432 \
    postgres \
    'Admin@1234' \
    california_schools \
    10
```

Output:
```
==================================
SQLite to PostgreSQL Converter
==================================
SQLite DB: /home/hailongli/bird_data/.../california_schools.sqlite
PostgreSQL: postgres@136.119.143.89:5432/california_schools
Work dir: /home/hailongli/db-engine-convertor
==================================

Step 1: Exporting SQLite database...
✓ Exported 3 tables with 29941 total rows

Step 2: Running AI-powered conversion orchestrator...
ATTEMPT 1/10
🤖 Running agent to generate conversion artifacts...
✓ Saved pg_schema.sql (5234 chars)
✓ Saved csv_convertor.py (3456 chars)

🚀 Running pipeline...
STEP 1: Wiping destination database
✓ Database wiped successfully

STEP 2: Creating schema
✓ Schema created successfully

STEP 3: Converting CSV files
✓ CSV files converted successfully

STEP 4: Uploading CSV files to PostgreSQL
  ✓ Uploaded 9986 rows (frpm)
  ✓ Uploaded 2269 rows (satscores)
  ✓ Uploaded 17686 rows (schools)

✓ PIPELINE COMPLETED SUCCESSFULLY!
🎉 SUCCESS! Migration completed successfully!
==================================
```

## Requirements

- Python 3.7+
- PostgreSQL client tools (`psql`)
- Google Gemini API access
- Required Python packages:
  - `google-genai`

## Troubleshooting

### Agent Fails
- Check `GEMINI_API_KEY` environment variable
- Verify Vertex AI credentials if using Vertex AI
- Check network connectivity

### Pipeline Fails
- Check PostgreSQL connection: `PGPASSWORD=Admin@1234 psql -h 136.119.143.89 -p 5432 -U postgres -d california_schools -c "SELECT 1"`
- Review `pipeline_error.txt` for details
- Check PostgreSQL logs

### Max Attempts Reached
- Review the evolution of errors across attempts
- May need to manually adjust `pg_schema.sql` or `csv_convertor.py`
- Increase `--max-attempts` parameter

## License

MIT

