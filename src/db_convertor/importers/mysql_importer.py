"""MySQL database importer."""

import csv
import mysql.connector
from pathlib import Path
from typing import Optional, List, Dict
from .base import DatabaseImporter


class MySQLImporter(DatabaseImporter):
    """Import data into MySQL database."""
    
    def __init__(
        self,
        host: str,
        port: str,
        user: str,
        password: str,
        database: str
    ):
        """Initialize MySQL importer.
        
        Args:
            host: MySQL host
            port: MySQL port
            user: MySQL user
            password: MySQL password
            database: Database name
        """
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.database = database
    
    def _get_connection(self, database=True):
        """Get a MySQL database connection.
        
        Args:
            database: If True, connect to the specific database. If False, connect without database.
        """
        config = {
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'password': self.password
        }
        if database:
            config['database'] = self.database
        return mysql.connector.connect(**config)
    
    def _ensure_database_exists(self):
        """Ensure the target database exists, create if it doesn't."""
        conn = self._get_connection(database=False)
        cursor = conn.cursor()
        try:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{self.database}`")
            conn.commit()
        finally:
            cursor.close()
            conn.close()
    
    def wipe_database(self):
        """Wipe all tables from the database."""
        print("\n" + "=" * 80)
        print("STEP: Wiping destination database")
        print("=" * 80)
        
        # Ensure database exists
        self._ensure_database_exists()
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Get list of tables
            cursor.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
                (self.database,)
            )
            tables = [row[0] for row in cursor.fetchall()]
            
            if tables:
                print(f"Found {len(tables)} tables to drop: {', '.join(tables)}")
                
                # Disable foreign key checks
                cursor.execute("SET FOREIGN_KEY_CHECKS=0")
                
                # Drop all tables
                for table in tables:
                    cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
                
                # Re-enable foreign key checks
                cursor.execute("SET FOREIGN_KEY_CHECKS=1")
                conn.commit()
                
                print("✓ Database wiped successfully")
            else:
                print("✓ Database is already empty")
        finally:
            cursor.close()
            conn.close()
    
    def create_schema(self, schema_file: Path):
        """Create database schema from SQL file.
        
        Args:
            schema_file: Path to schema SQL file
        """
        print("\n" + "=" * 80)
        print("STEP: Creating schema")
        print("=" * 80)
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Read schema file
            with open(schema_file, 'r') as f:
                schema_sql = f.read()
            
            # Split into individual statements and execute
            # MySQL connector requires executing statements one at a time
            statements = []
            current_statement = []
            
            for line in schema_sql.split('\n'):
                line = line.strip()
                # Skip comments and empty lines
                if not line or line.startswith('--'):
                    continue
                current_statement.append(line)
                if line.endswith(';'):
                    statements.append(' '.join(current_statement))
                    current_statement = []
            
            # Execute each statement
            for statement in statements:
                if statement.strip():
                    cursor.execute(statement)
            
            conn.commit()
            print("✓ Schema created successfully")
        finally:
            cursor.close()
            conn.close()
    
    def load_data(self, table: str, csv_file: Path):
        """Load data from CSV file into table.
        
        Args:
            table: Table name
            csv_file: Path to CSV file
        
        Raises:
            Exception: If data loading fails
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)  # Skip header row
                
                # Prepare INSERT statement
                placeholders = ', '.join(['%s'] * len(headers))
                columns = ', '.join([f'`{col}`' for col in headers])
                insert_sql = f"INSERT INTO `{table}` ({columns}) VALUES ({placeholders})"
                
                # Batch insert for better performance
                batch_size = 1000
                batch = []
                rows_inserted = 0
                
                for row_num, row in enumerate(reader, start=1):
                    # Convert empty strings to None for NULL
                    processed_row = [val if val != '' else None for val in row]
                    batch.append(processed_row)
                    
                    if len(batch) >= batch_size:
                        try:
                            cursor.executemany(insert_sql, batch)
                            conn.commit()
                            rows_inserted += len(batch)
                            batch = []
                        except Exception as e:
                            # Rollback on error
                            conn.rollback()
                            raise Exception(f"Failed to load batch ending at row {row_num} in table {table}: {e}")
                
                # Insert remaining rows
                if batch:
                    try:
                        cursor.executemany(insert_sql, batch)
                        conn.commit()
                        rows_inserted += len(batch)
                    except Exception as e:
                        # Rollback on error
                        conn.rollback()
                        raise Exception(f"Failed to load final batch in table {table}: {e}")
                
        except Exception as e:
            raise Exception(f"Error loading data into {table}: {e}")
        finally:
            cursor.close()
            conn.close()
    
    def get_table_dependencies(self) -> List[str]:
        """Get tables in dependency order based on foreign keys.
        
        Returns:
            List of table names in load order
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Get all tables
            cursor.execute("SHOW TABLES")
            all_tables = [row[0] for row in cursor.fetchall()]
            
            # Get foreign key relationships
            dependencies = {}  # table -> list of tables it depends on
            
            for table in all_tables:
                cursor.execute("""
                    SELECT REFERENCED_TABLE_NAME 
                    FROM information_schema.KEY_COLUMN_USAGE 
                    WHERE TABLE_SCHEMA = %s 
                    AND TABLE_NAME = %s 
                    AND REFERENCED_TABLE_NAME IS NOT NULL
                """, (self.database, table))
                
                refs = [row[0] for row in cursor.fetchall()]
                if refs:
                    dependencies[table] = list(set(refs))
            
            # Topological sort
            visited = set()
            ordered = []
            
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
        finally:
            cursor.close()
            conn.close()
    
    def load_csv_data(self, csv_dir: Path, tables: List[str] = None):
        """Load CSV data into MySQL tables.
        
        Args:
            csv_dir: Directory containing CSV files
            tables: List of tables in dependency order (if None, will determine automatically)
        """
        print("\n" + "=" * 80)
        print("STEP: Uploading CSV files to MySQL")
        print("=" * 80)
        
        csv_files = {f.stem: f for f in Path(csv_dir).glob('*.csv')}
        
        if not csv_files:
            raise Exception(f"No CSV files found in {csv_dir}")
        
        # Get table load order if not provided
        if tables is None:
            tables = self.get_table_dependencies()
        
        print(f"Loading {len(csv_files)} tables in dependency order: {', '.join(tables)}")
        
        # Load each table
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            for table_name in tables:
                if table_name not in csv_files:
                    print(f"  ⚠ Skipping {table_name} (no CSV file)")
                    continue
                
                csv_file = csv_files[table_name]
                print(f"  Loading {table_name}...")
                
                self.load_data(table_name, csv_file)
                
                # Count rows - need fresh query to see committed data from other connection
                # Close and reopen connection to avoid transaction isolation issues
                cursor.close()
                conn.close()
                conn = self._get_connection()
                cursor = conn.cursor()
                
                cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                row_count = cursor.fetchone()[0]
                print(f"    ✓ Uploaded {row_count} rows")
        finally:
            cursor.close()
            conn.close()
    
    def verify_row_counts(self, expected_counts: Dict[str, int]) -> bool:
        """Verify row counts match expected values.
        
        Args:
            expected_counts: Dict mapping table names to expected row counts
            
        Returns:
            True if all counts match, False otherwise
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            all_match = True
            for table_name, expected in expected_counts.items():
                cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                actual = cursor.fetchone()[0]
                
                if actual != expected:
                    print(f"✗ Row count mismatch for {table_name}: expected {expected}, got {actual}")
                    all_match = False
                else:
                    print(f"✓ Row count match for {table_name}: {actual}")
            
            return all_match
        finally:
            cursor.close()
            conn.close()

