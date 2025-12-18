#!/usr/bin/env python3
"""
SQLite to PostgreSQL Conversion Agent

This agent uses Gemini AI to convert SQLite schemas and data to PostgreSQL.
"""

import json
import os
import sqlite3
import csv
from pathlib import Path
from utils import gemini_inference


def get_csv_summary(csv_path, num_lines=5):
    """Get summary of CSV file: first N and last N lines, column names, row count."""
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        all_rows = list(reader)
    
    if not all_rows:
        return {
            'columns': [],
            'total_rows': 0,
            'first_lines': [],
            'last_lines': []
        }
    
    header = all_rows[0]
    data_rows = all_rows[1:]
    
    first_lines = data_rows[:num_lines]
    last_lines = data_rows[-num_lines:] if len(data_rows) > num_lines else []
    
    return {
        'columns': header,
        'total_rows': len(data_rows),
        'first_lines': first_lines,
        'last_lines': last_lines
    }


def format_csv_summary(csv_summaries):
    """Format CSV summaries for the prompt."""
    output = []
    for table_name, summary in csv_summaries.items():
        output.append(f"\n=== Table: {table_name} ===")
        output.append(f"Columns: {', '.join(summary['columns'])}")
        output.append(f"Total rows: {summary['total_rows']}")
        
        if summary['first_lines']:
            output.append("\nFirst 5 rows:")
            for i, row in enumerate(summary['first_lines'], 1):
                output.append(f"  {i}: {row}")
        
        if summary['last_lines'] and len(summary['last_lines']) > 0:
            output.append("\nLast 5 rows:")
            for i, row in enumerate(summary['last_lines'], 1):
                output.append(f"  {i}: {row}")
    
    return '\n'.join(output)


def read_file_with_line_numbers(file_path):
    """Read file with line numbers (nl -a format)."""
    if not os.path.exists(file_path):
        return None
    
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    numbered_lines = []
    for i, line in enumerate(lines, 1):
        numbered_lines.append(f"{i:6d}\t{line.rstrip()}")
    
    return '\n'.join(numbered_lines)


def build_agent_prompt(sqlite_schema, csv_summaries, prev_pg_schema=None, 
                       prev_convertor_script=None, pipeline_error=None):
    """Build the prompt for the agent."""
    
    prompt = """You are an expert database migration agent. Your task is to convert a SQLite database to PostgreSQL.

You will receive:
1. SQLite table creation statements
2. Summary of CSV data files (first 5 and last 5 rows, column names, row counts)
3. Previously generated PostgreSQL schema (if any)
4. Previously generated csv_convertor.py script (if any)
5. Pipeline execution error (if any)

Your job is to output TWO files:
1. **pg_schema.sql**: PostgreSQL table creation statements
2. **csv_convertor.py**: Python script to convert SQLite CSV data to PostgreSQL-compatible CSV

IMPORTANT CONVERSION RULES FOR pg_schema.sql:
- Convert SQLite types to appropriate PostgreSQL types:
  * TEXT -> VARCHAR or TEXT
  * INTEGER -> INTEGER or BIGINT
  * REAL -> DOUBLE PRECISION or NUMERIC
  * BLOB -> BYTEA
  * DATE -> DATE
- Handle NULL values properly
- Convert foreign key syntax to PostgreSQL format
- Ensure column names are properly quoted if they contain special characters

CRITICAL REQUIREMENTS FOR csv_convertor.py:
- MUST accept TWO command-line arguments: <source_dir> <output_dir>
  Example: python3 csv_convertor.py ./source_csvs ./converted_csvs
- MUST read CSV files from the source_dir argument (sys.argv[1])
- MUST write converted CSV files to the output_dir argument (sys.argv[2])
- MUST use the exact same filenames (table_name.csv) for input and output
- Handle any data type conversions needed (dates, booleans, empty strings to NULL, etc.)
- Convert SQLite integer booleans (0/1) to PostgreSQL boolean format (f/t) if needed
- Make sure data precision matches PostgreSQL requirements
- The converted CSVs must be compatible with PostgreSQL's COPY command with CSV format

OUTPUT FORMAT:
You must output a JSON object with this exact structure:
{
  "pg_schema": "-- Full PostgreSQL schema here\\nCREATE TABLE ...",
  "csv_convertor": "#!/usr/bin/env python3\\n# Full Python script here"
}

=== SQLITE SCHEMA ===
"""
    
    prompt += sqlite_schema
    
    prompt += "\n\n=== CSV DATA SUMMARIES ==="
    prompt += format_csv_summary(csv_summaries)
    
    if prev_pg_schema:
        prompt += "\n\n=== PREVIOUS POSTGRESQL SCHEMA (with line numbers) ==="
        prompt += "\n" + read_file_with_line_numbers(prev_pg_schema)
    
    if prev_convertor_script:
        prompt += "\n\n=== PREVIOUS CSV CONVERTOR SCRIPT (with line numbers) ==="
        prompt += "\n" + read_file_with_line_numbers(prev_convertor_script)
    
    if pipeline_error:
        prompt += "\n\n=== PIPELINE EXECUTION ERROR ==="
        prompt += "\n" + pipeline_error
        prompt += "\n\nPlease fix the errors in the schema or convertor script."
    
    prompt += """

Now generate the corrected or improved pg_schema.sql and csv_convertor.py.
Remember to output valid JSON with "pg_schema" and "csv_convertor" keys.
"""
    
    return prompt


def run_agent(sqlite_schema_path, csv_dir, prev_pg_schema_path=None, 
              prev_convertor_path=None, pipeline_error=None):
    """Run the agent to generate PG schema and convertor script."""
    
    # Read SQLite schema
    with open(sqlite_schema_path, 'r', encoding='utf-8') as f:
        sqlite_schema = f.read()
    
    # Get CSV summaries
    csv_summaries = {}
    csv_files = sorted(Path(csv_dir).glob('*.csv'))
    for csv_file in csv_files:
        table_name = csv_file.stem
        csv_summaries[table_name] = get_csv_summary(csv_file)
    
    # Build prompt
    prompt = build_agent_prompt(
        sqlite_schema, 
        csv_summaries,
        prev_pg_schema_path,
        prev_convertor_path,
        pipeline_error
    )
    
    print("=" * 80)
    print("Calling Gemini AI agent...")
    print("=" * 80)
    
    # Call Gemini
    response = gemini_inference(prompt, temperature=0.3, enforce_json=True)
    
    # Parse response
    try:
        result = json.loads(response)
        return result['pg_schema'], result['csv_convertor']
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error parsing agent response: {e}")
        print("Raw response:")
        print(response)
        raise


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python agent.py <sqlite_schema.sql> <csv_dir> [prev_pg_schema.sql] [prev_convertor.py] [error.txt]")
        sys.exit(1)
    
    sqlite_schema_path = sys.argv[1]
    csv_dir = sys.argv[2]
    prev_pg_schema = sys.argv[3] if len(sys.argv) > 3 else None
    prev_convertor = sys.argv[4] if len(sys.argv) > 4 else None
    error_file = sys.argv[5] if len(sys.argv) > 5 else None
    
    pipeline_error = None
    if error_file and os.path.exists(error_file):
        with open(error_file, 'r') as f:
            pipeline_error = f.read()
    
    pg_schema, csv_convertor = run_agent(
        sqlite_schema_path, 
        csv_dir,
        prev_pg_schema,
        prev_convertor,
        pipeline_error
    )
    
    # Write outputs
    with open('pg_schema.sql', 'w', encoding='utf-8') as f:
        f.write(pg_schema)
    print("✓ Wrote pg_schema.sql")
    
    with open('csv_convertor.py', 'w', encoding='utf-8') as f:
        f.write(csv_convertor)
    os.chmod('csv_convertor.py', 0o755)
    print("✓ Wrote csv_convertor.py")

