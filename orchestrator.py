#!/usr/bin/env python3
"""
SQLite to PostgreSQL Migration Orchestrator

This script orchestrates the conversion process:
1. Calls the agent to generate PG schema and CSV convertor
2. Runs the pipeline
3. If pipeline fails, calls agent again with error feedback
4. Repeats until success or max attempts
"""

import os
import sys
import argparse
from pathlib import Path
from agent import run_agent
from pipeline import run_pipeline


def save_error_to_file(error, error_file='pipeline_error.txt'):
    """Save pipeline error to a file."""
    with open(error_file, 'w', encoding='utf-8') as f:
        f.write(error)
    print(f"Error saved to {error_file}")


def orchestrate_conversion(sqlite_schema_path, source_csv_dir,
                           pg_host, pg_port, pg_user, pg_password, pg_database,
                           max_attempts=10):
    """
    Orchestrate the conversion process.
    
    Args:
        sqlite_schema_path: Path to SQLite schema.sql
        source_csv_dir: Directory containing source CSV files
        pg_host, pg_port, pg_user, pg_password, pg_database: PostgreSQL connection info
        max_attempts: Maximum number of attempts
    
    Returns:
        True if successful, False otherwise
    """
    
    # Set up paths
    work_dir = Path.cwd()
    pg_schema_path = work_dir / 'pg_schema.sql'
    convertor_path = work_dir / 'csv_convertor.py'
    converted_csv_dir = work_dir / 'converted_csv'
    error_file = work_dir / 'pipeline_error.txt'
    
    print("=" * 80)
    print("SQLite to PostgreSQL Migration Orchestrator")
    print("=" * 80)
    print(f"SQLite schema: {sqlite_schema_path}")
    print(f"Source CSV dir: {source_csv_dir}")
    print(f"PostgreSQL: {pg_user}@{pg_host}:{pg_port}/{pg_database}")
    print(f"Max attempts: {max_attempts}")
    print("=" * 80)
    
    pipeline_error = None
    prev_pg_schema = None
    prev_convertor = None
    
    for attempt in range(1, max_attempts + 1):
        print(f"\n{'=' * 80}")
        print(f"ATTEMPT {attempt}/{max_attempts}")
        print(f"{'=' * 80}\n")
        
        # Run agent to generate PG schema and convertor
        try:
            print("🤖 Running agent to generate conversion artifacts...")
            pg_schema, csv_convertor = run_agent(
                sqlite_schema_path,
                source_csv_dir,
                prev_pg_schema,
                prev_convertor,
                pipeline_error
            )
            
            # Save outputs
            with open(pg_schema_path, 'w', encoding='utf-8') as f:
                f.write(pg_schema)
            print(f"✓ Saved pg_schema.sql ({len(pg_schema)} chars)")
            
            with open(convertor_path, 'w', encoding='utf-8') as f:
                f.write(csv_convertor)
            os.chmod(convertor_path, 0o755)
            print(f"✓ Saved csv_convertor.py ({len(csv_convertor)} chars)")
            
            # Update previous paths for next iteration
            prev_pg_schema = str(pg_schema_path)
            prev_convertor = str(convertor_path)
            
        except Exception as e:
            print(f"✗ Agent failed: {e}")
            import traceback
            traceback.print_exc()
            continue
        
        # Run pipeline
        print("\n🚀 Running pipeline...")
        error = run_pipeline(
            pg_host,
            pg_port,
            pg_user,
            pg_password,
            pg_database,
            str(pg_schema_path),
            str(convertor_path),
            source_csv_dir,
            str(converted_csv_dir)
        )
        
        if error is None:
            print("\n" + "=" * 80)
            print("🎉 SUCCESS! Migration completed successfully!")
            print("=" * 80)
            print(f"\nFinal artifacts:")
            print(f"  - PostgreSQL schema: {pg_schema_path}")
            print(f"  - CSV convertor: {convertor_path}")
            print(f"  - Converted CSVs: {converted_csv_dir}")
            return True
        else:
            print(f"\n✗ Pipeline failed on attempt {attempt}")
            pipeline_error = error
            save_error_to_file(error, error_file)
            
            if attempt < max_attempts:
                print(f"\n🔄 Will retry with error feedback...")
            else:
                print(f"\n✗ Max attempts ({max_attempts}) reached. Giving up.")
    
    return False


def main():
    parser = argparse.ArgumentParser(
        description='Orchestrate SQLite to PostgreSQL migration using AI agent'
    )
    
    # SQLite source
    parser.add_argument('--sqlite-schema', required=True,
                       help='Path to SQLite schema.sql file')
    parser.add_argument('--source-csv', required=True,
                       help='Directory containing source CSV files')
    
    # PostgreSQL destination
    parser.add_argument('--pg-host', required=True,
                       help='PostgreSQL host')
    parser.add_argument('--pg-port', default='5432',
                       help='PostgreSQL port (default: 5432)')
    parser.add_argument('--pg-user', required=True,
                       help='PostgreSQL user')
    parser.add_argument('--pg-password', required=True,
                       help='PostgreSQL password')
    parser.add_argument('--pg-database', required=True,
                       help='PostgreSQL database name')
    
    # Options
    parser.add_argument('--max-attempts', type=int, default=10,
                       help='Maximum number of attempts (default: 10)')
    
    args = parser.parse_args()
    
    # Set PGPASSWORD environment variable
    os.environ['PGPASSWORD'] = args.pg_password
    
    success = orchestrate_conversion(
        args.sqlite_schema,
        args.source_csv,
        args.pg_host,
        args.pg_port,
        args.pg_user,
        args.pg_password,
        args.pg_database,
        args.max_attempts
    )
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()

