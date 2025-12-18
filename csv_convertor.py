#!/usr/bin/env python3
import sys
import os
import csv

def convert_csv(source_path, output_path):
    """Converts a single CSV file from SQLite format to PostgreSQL format."""
    table_name = os.path.splitext(os.path.basename(source_path))[0]

    # Define columns that should be treated as booleans for each table
    boolean_columns = {
        'frpm': {
            'Charter School (Y/N)',
            'IRC',
            '2013-14 CALPADS Fall 1 Certification Status'
        },
        'schools': {
            'Charter',
            'Magnet'
        },
        'satscores': set()
    }

    try:
        with open(source_path, 'r', encoding='utf-8') as infile, \
             open(output_path, 'w', encoding='utf-8', newline='') as outfile:

            reader = csv.reader(infile)
            writer = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL)

            header = next(reader)
            writer.writerow(header)

            # Get the indices of boolean columns for the current table
            table_booleans = boolean_columns.get(table_name, set())
            bool_indices = {i for i, col_name in enumerate(header) if col_name in table_booleans}

            # Get the index of the 'cds' column for the 'satscores' table to fix foreign key mismatch
            cds_index = -1
            if table_name == 'satscores':
                try:
                    cds_index = header.index('cds')
                except ValueError:
                    print(f"Warning: 'cds' column not found in {source_path}", file=sys.stderr)

            for row in reader:
                processed_row = []
                for i, value in enumerate(row):
                    # Handle satscores.cds padding to match the 'schools' table's 'CDSCode' format
                    if i == cds_index:
                        if len(value) == 13 and value.isdigit():
                            processed_row.append('0' + value)
                        else:
                            processed_row.append(value)
                    # Handle boolean conversion (0/1 to f/t)
                    elif i in bool_indices:
                        if value == '1':
                            processed_row.append('t')
                        elif value == '0':
                            processed_row.append('f')
                        else:
                            # For boolean columns, any value other than '1' or '0' is treated as NULL.
                            # An empty, unquoted string represents NULL for PostgreSQL's COPY command.
                            processed_row.append('')
                    # Handle all other columns
                    else:
                        # An empty string in the source CSV will be written as an empty field,
                        # which COPY correctly interprets as NULL for various data types.
                        processed_row.append(value)
                writer.writerow(processed_row)

    except FileNotFoundError:
        print(f"Error: Source file not found at {source_path}", file=sys.stderr)
    except Exception as e:
        print(f"An error occurred while processing {source_path}: {e}", file=sys.stderr)

def main(source_dir, output_dir):
    """Processes all CSV files in the source directory and writes them to the output directory."""
    os.makedirs(output_dir, exist_ok=True)

    csv_files = ['frpm.csv', 'satscores.csv', 'schools.csv']

    for filename in csv_files:
        source_path = os.path.join(source_dir, filename)
        output_path = os.path.join(output_dir, filename)

        if os.path.exists(source_path):
            print(f"Converting {filename}...")
            convert_csv(source_path, output_path)
        else:
            print(f"Warning: {filename} not found in source directory, skipping.", file=sys.stderr)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 csv_convertor.py <source_dir> <output_dir>")
        sys.exit(1)

    source_directory = sys.argv[1]
    output_directory = sys.argv[2]

    main(source_directory, output_directory)
