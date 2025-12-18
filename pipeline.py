#!/usr/bin/env python3
"""
PostgreSQL Migration Pipeline

This script:
1. Wipes the destination database
2. Creates schema from pg_schema.sql
3. Runs csv_convertor.py to convert CSV files
4. Uploads converted CSV to PostgreSQL
"""

import subprocess
import sys
import os
from pathlib import Path


class PipelineError(Exception):
    """Custom exception for pipeline errors."""
    pass


def run_command(cmd, capture_output=True, check=True):
    """Run a shell command and return the result."""
    print(f"Running: {cmd}")
    result = subprocess.run(
        cmd,
        shell=True,
        capture_output=capture_output,
        text=True
    )
    
    if check and result.returncode != 0:
        error_msg = f"Command failed with exit code {result.returncode}\n"
        error_msg += f"Command: {cmd}\n"
        if result.stdout:
            error_msg += f"STDOUT:\n{result.stdout}\n"
        if result.stderr:
            error_msg += f"STDERR:\n{result.stderr}\n"
        raise PipelineError(error_msg)
    
    return result


def wipe_database(pg_host, pg_port, pg_user, pg_password, pg_database):
    """Wipe all tables from the PostgreSQL database."""
    print("\n" + "=" * 80)
    print("STEP 1: Wiping destination database")
    print("=" * 80)
    
    env = os.environ.copy()
    env['PGPASSWORD'] = pg_password
    
    # Get all tables
    cmd = f"psql -h {pg_host} -p {pg_port} -U {pg_user} -d {pg_database} -t -c \"SELECT tablename FROM pg_tables WHERE schemaname='public'\""
    result = run_command(cmd, capture_output=True, check=True)
    
    tables = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
    
    if tables:
        print(f"Found {len(tables)} tables to drop: {', '.join(tables)}")
        
        # Drop tables with CASCADE
        drop_cmd = f"psql -h {pg_host} -p {pg_port} -U {pg_user} -d {pg_database} -c \"DROP SCHEMA public CASCADE; CREATE SCHEMA public;\""
        run_command(drop_cmd, capture_output=True, check=True)
        print("✓ Database wiped successfully")
    else:
        print("✓ Database is already empty")


def create_schema(pg_host, pg_port, pg_user, pg_password, pg_database, schema_file):
    """Create schema in PostgreSQL database."""
    print("\n" + "=" * 80)
    print("STEP 2: Creating schema")
    print("=" * 80)
    
    env = os.environ.copy()
    env['PGPASSWORD'] = pg_password
    
    cmd = f"psql -h {pg_host} -p {pg_port} -U {pg_user} -d {pg_database} -f {schema_file}"
    result = run_command(cmd, capture_output=True, check=True)
    
    print("✓ Schema created successfully")
    if result.stdout:
        print(result.stdout)


def run_csv_convertor(convertor_script, source_csv_dir, output_csv_dir):
    """Run the CSV convertor script."""
    print("\n" + "=" * 80)
    print("STEP 3: Converting CSV files")
    print("=" * 80)
    
    # Make sure the script is executable
    os.chmod(convertor_script, 0o755)
    
    cmd = f"python3 {convertor_script} {source_csv_dir} {output_csv_dir}"
    result = run_command(cmd, capture_output=True, check=True)
    
    print("✓ CSV files converted successfully")
    if result.stdout:
        print(result.stdout)


def get_table_dependencies(pg_host, pg_port, pg_user, pg_password, pg_database):
    """Get table dependency order from foreign keys."""
    cmd = f"""psql -h {pg_host} -p {pg_port} -U {pg_user} -d {pg_database} -t -c "
        SELECT DISTINCT tablename 
        FROM pg_tables 
        WHERE schemaname='public' 
        ORDER BY tablename
    \""""
    result = run_command(cmd, capture_output=True, check=True)
    all_tables = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
    
    # Get foreign key relationships
    cmd = f"""psql -h {pg_host} -p {pg_port} -U {pg_user} -d {pg_database} -t -c "
        SELECT 
            tc.table_name,
            ccu.table_name AS foreign_table_name
        FROM 
            information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema='public'
    \""""
    result = run_command(cmd, capture_output=True, check=True)
    
    dependencies = {}
    for line in result.stdout.strip().split('\n'):
        if '|' in line:
            parts = [p.strip() for p in line.split('|')]
            if len(parts) == 2:
                table, ref_table = parts
                if table not in dependencies:
                    dependencies[table] = []
                dependencies[table].append(ref_table)
    
    # Topological sort
    ordered = []
    visited = set()
    
    def visit(table):
        if table in visited:
            return
        visited.add(table)
        if table in dependencies:
            for dep in dependencies[table]:
                visit(dep)
        if table in all_tables and table not in ordered:
            ordered.append(table)
    
    for table in all_tables:
        visit(table)
    
    return ordered


def upload_csv_to_postgres(pg_host, pg_port, pg_user, pg_password, pg_database, csv_dir):
    """Upload CSV files to PostgreSQL in dependency order."""
    print("\n" + "=" * 80)
    print("STEP 4: Uploading CSV files to PostgreSQL")
    print("=" * 80)
    
    env = os.environ.copy()
    env['PGPASSWORD'] = pg_password
    
    csv_files = {f.stem: f for f in Path(csv_dir).glob('*.csv')}
    
    if not csv_files:
        raise PipelineError(f"No CSV files found in {csv_dir}. CSV convertor failed to create output files.")
    
    # Get table dependency order
    print("  Determining table load order based on foreign keys...")
    table_order = get_table_dependencies(pg_host, pg_port, pg_user, pg_password, pg_database)
    print(f"  Load order: {' -> '.join(table_order)}")
    
    for table_name in table_order:
        if table_name not in csv_files:
            print(f"  Warning: No CSV file for table {table_name}, skipping")
            continue
        
        csv_file = csv_files[table_name]
        print(f"  Uploading {csv_file.name} to table {table_name}...")
        
        # Use COPY command to load CSV
        copy_cmd = f"\\COPY {table_name} FROM '{csv_file.absolute()}' WITH (FORMAT csv, HEADER true, ENCODING 'UTF-8')"
        cmd = f"psql -h {pg_host} -p {pg_port} -U {pg_user} -d {pg_database} -c \"{copy_cmd}\""
        
        try:
            result = run_command(cmd, capture_output=True, check=True)
            
            # Count rows
            count_cmd = f"psql -h {pg_host} -p {pg_port} -U {pg_user} -d {pg_database} -t -c \"SELECT COUNT(*) FROM {table_name}\""
            count_result = run_command(count_cmd, capture_output=True, check=True)
            row_count = count_result.stdout.strip()
            
            print(f"    ✓ Uploaded {row_count} rows")
        except PipelineError as e:
            print(f"    ✗ Failed to upload {csv_file.name}")
            raise


def run_pipeline(pg_host, pg_port, pg_user, pg_password, pg_database,
                 schema_file, convertor_script, source_csv_dir, converted_csv_dir):
    """Run the full pipeline."""
    
    try:
        # Create output directory
        os.makedirs(converted_csv_dir, exist_ok=True)
        
        # Step 1: Wipe database
        wipe_database(pg_host, pg_port, pg_user, pg_password, pg_database)
        
        # Step 2: Create schema
        create_schema(pg_host, pg_port, pg_user, pg_password, pg_database, schema_file)
        
        # Step 3: Convert CSV files
        run_csv_convertor(convertor_script, source_csv_dir, converted_csv_dir)
        
        # Step 4: Upload CSV files
        upload_csv_to_postgres(pg_host, pg_port, pg_user, pg_password, pg_database, converted_csv_dir)
        
        print("\n" + "=" * 80)
        print("✓ PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        return None
        
    except PipelineError as e:
        print("\n" + "=" * 80)
        print("✗ PIPELINE FAILED!")
        print("=" * 80)
        print(str(e))
        return str(e)
    except Exception as e:
        print("\n" + "=" * 80)
        print("✗ PIPELINE FAILED WITH UNEXPECTED ERROR!")
        print("=" * 80)
        import traceback
        error_msg = traceback.format_exc()
        print(error_msg)
        return error_msg


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Run PostgreSQL migration pipeline')
    parser.add_argument('--pg-host', required=True, help='PostgreSQL host')
    parser.add_argument('--pg-port', default='5432', help='PostgreSQL port')
    parser.add_argument('--pg-user', required=True, help='PostgreSQL user')
    parser.add_argument('--pg-password', required=True, help='PostgreSQL password')
    parser.add_argument('--pg-database', required=True, help='PostgreSQL database')
    parser.add_argument('--schema', required=True, help='Path to pg_schema.sql')
    parser.add_argument('--convertor', required=True, help='Path to csv_convertor.py')
    parser.add_argument('--source-csv', required=True, help='Source CSV directory')
    parser.add_argument('--output-csv', required=True, help='Output CSV directory')
    
    args = parser.parse_args()
    
    # Set PGPASSWORD environment variable
    os.environ['PGPASSWORD'] = args.pg_password
    
    error = run_pipeline(
        args.pg_host,
        args.pg_port,
        args.pg_user,
        args.pg_password,
        args.pg_database,
        args.schema,
        args.convertor,
        args.source_csv,
        args.output_csv
    )
    
    if error:
        sys.exit(1)
    else:
        sys.exit(0)

