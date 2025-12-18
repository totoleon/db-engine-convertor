#!/usr/bin/env python3
"""
SQLite Database Export Tool

This script exports:
1. CREATE statements for all tables, views, triggers, and indexes
2. Table data to CSV files
"""

import sqlite3
import csv
import os
import sys
import argparse
from pathlib import Path


def get_schema(conn):
    """Extract all CREATE statements from the database."""
    cursor = conn.cursor()
    
    # Get all schema objects (tables, views, triggers, indexes)
    cursor.execute("""
        SELECT sql 
        FROM sqlite_master 
        WHERE sql IS NOT NULL
        ORDER BY 
            CASE type
                WHEN 'table' THEN 1
                WHEN 'index' THEN 2
                WHEN 'view' THEN 3
                WHEN 'trigger' THEN 4
                ELSE 5
            END,
            name
    """)
    
    schema_statements = []
    for row in cursor.fetchall():
        if row[0]:
            schema_statements.append(row[0] + ';')
    
    return schema_statements


def get_tables(conn):
    """Get list of all tables in the database."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name 
        FROM sqlite_master 
        WHERE type='table' 
        AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """)
    return [row[0] for row in cursor.fetchall()]


def export_table_to_csv(conn, table_name, output_dir):
    """Export a single table to a CSV file."""
    cursor = conn.cursor()
    
    # Get all data from the table
    cursor.execute(f'SELECT * FROM "{table_name}"')
    
    # Get column names
    column_names = [description[0] for description in cursor.description]
    
    # Create CSV file
    csv_path = os.path.join(output_dir, f"{table_name}.csv")
    
    with open(csv_path, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        
        # Write header
        writer.writerow(column_names)
        
        # Write data
        row_count = 0
        for row in cursor:
            writer.writerow(row)
            row_count += 1
    
    return row_count, csv_path


def main():
    parser = argparse.ArgumentParser(
        description='Export SQLite database schema and data to SQL and CSV files'
    )
    parser.add_argument(
        'database',
        help='Path to the SQLite database file'
    )
    parser.add_argument(
        '-o', '--output-dir',
        default='.',
        help='Output directory for exported files (default: current directory)'
    )
    parser.add_argument(
        '-s', '--schema-file',
        default='schema.sql',
        help='Output filename for schema (default: schema.sql)'
    )
    
    args = parser.parse_args()
    
    # Check if database exists
    if not os.path.exists(args.database):
        print(f"Error: Database file '{args.database}' not found", file=sys.stderr)
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Connecting to database: {args.database}")
    
    try:
        # Connect to database
        conn = sqlite3.connect(args.database)
        
        # Export schema
        print("\nExporting schema...")
        schema_statements = get_schema(conn)
        schema_path = output_dir / args.schema_file
        
        with open(schema_path, 'w', encoding='utf-8') as f:
            f.write("-- SQLite Database Schema Export\n")
            f.write(f"-- Source: {args.database}\n\n")
            for statement in schema_statements:
                f.write(statement + '\n\n')
        
        print(f"Schema exported to: {schema_path}")
        print(f"Found {len(schema_statements)} schema objects")
        
        # Export tables
        print("\nExporting tables to CSV...")
        tables = get_tables(conn)
        
        if not tables:
            print("No tables found in database")
        else:
            total_rows = 0
            for table in tables:
                row_count, csv_path = export_table_to_csv(conn, table, output_dir)
                print(f"  {table}: {row_count} rows -> {csv_path}")
                total_rows += row_count
            
            print(f"\nExported {len(tables)} tables with {total_rows} total rows")
        
        conn.close()
        print("\nExport completed successfully!")
        
    except sqlite3.Error as e:
        print(f"SQLite error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
