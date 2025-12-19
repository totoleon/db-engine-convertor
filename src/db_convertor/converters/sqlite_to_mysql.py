"""SQLite to MySQL converter."""

from typing import Dict, Optional
from .base import DatabaseConverter, ConversionConfig
from ..exporters.sqlite_exporter import SQLiteExporter
from ..importers.mysql_importer import MySQLImporter


class SQLiteToMySQLConverter(DatabaseConverter):
    """Convert from SQLite to MySQL."""
    
    def __init__(self, config: ConversionConfig):
        """Initialize SQLite to MySQL converter."""
        super().__init__(config)
        if config.source_type != 'sqlite':
            raise ValueError(f"Source type must be 'sqlite', got '{config.source_type}'")
        if config.target_type != 'mysql':
            raise ValueError(f"Target type must be 'mysql', got '{config.target_type}'")
    
    def get_exporter(self):
        """Get SQLite exporter instance."""
        return SQLiteExporter(self.config.source_connection)
    
    def get_importer(self):
        """Get MySQL importer instance."""
        return MySQLImporter(
            host=self.config.target_connection['host'],
            port=self.config.target_connection.get('port', '3306'),
            user=self.config.target_connection['user'],
            password=self.config.target_connection['password'],
            database=self.config.target_connection['database']
        )
    
    def get_schema_conversion_prompt(
        self,
        source_schema: str,
        csv_summaries: Dict,
        prev_schema: Optional[str] = None,
        prev_convertor: Optional[str] = None,
        error: Optional[str] = None
    ) -> str:
        """Build prompt for AI agent to convert SQLite schema to MySQL.
        
        Args:
            source_schema: SQLite schema (CREATE statements)
            csv_summaries: Dictionary of table names to CSV summaries
            prev_schema: Previous MySQL schema (if any)
            prev_convertor: Previous data convertor script (if any)
            error: Error from previous attempt (if any)
            
        Returns:
            Prompt string for AI agent
        """
        # Format CSV summaries
        csv_summary_text = "\n\n".join([
            f"Table: {table}\n{summary}"
            for table, summary in csv_summaries.items()
        ])
        
        prompt = f"""You are a database conversion expert. Convert this SQLite database to MySQL.

===== SOURCE SQLITE SCHEMA =====
{source_schema}

===== CSV DATA SUMMARY =====
{csv_summary_text}
"""
        
        if error:
            prompt += f"""
===== PREVIOUS ATTEMPT ERROR =====
{error}
"""
        
        if prev_schema:
            prompt += f"""
===== PREVIOUS MYSQL SCHEMA =====
{prev_schema}
"""
        
        if prev_convertor:
            prompt += f"""
===== PREVIOUS DATA CONVERTOR =====
{prev_convertor}
"""
        
        prompt += """
===== YOUR TASK =====

Generate TWO outputs to convert this SQLite database to MySQL:

1. **mysql_schema**: Full MySQL table creation statements
2. **data_convertor**: Python script to convert CSV files for MySQL

**KEY MYSQL CONVERSION RULES:**

**Data Types:**
- INTEGER → INT or BIGINT (use BIGINT for large IDs)
- REAL → DOUBLE
- TEXT → VARCHAR(n) or TEXT (choose appropriate length)
- BLOB → BLOB
- BOOLEAN → TINYINT(1) or BOOLEAN
- DATE → DATE
- DATETIME → DATETIME

**Syntax Differences:**
- Use backticks for identifiers: `table_name`, `column_name`
- MySQL doesn't have AUTOINCREMENT, use AUTO_INCREMENT
- PRIMARY KEY columns should be NOT NULL
- Use ENGINE=InnoDB for tables with foreign keys
- NULL representation: Use NULL keyword (not \\N)
- Boolean: Use 1/0 for TINYINT(1), or TRUE/FALSE for BOOLEAN type

**data_convertor Requirements:**
- Accept TWO positional command-line arguments: source_dir and output_dir
  Usage: python3 data_convertor.py <source_dir> <output_dir>
- Read all CSV files from source directory
- Convert data for MySQL compatibility:
  * Handle NULL values (empty strings → NULL for numeric/date types)
  * Convert boolean values (empty/0 → 0, 1/true → 1 for TINYINT)
  * Ensure date formats are YYYY-MM-DD
  * Ensure datetime formats are YYYY-MM-DD HH:MM:SS
  * Validate VARCHAR lengths match schema
- Write converted CSVs to output directory
- Print progress and any warnings

**IMPORTANT: Python Syntax**
- When writing Python code that mentions backslash-N, use raw strings r"\\N" or escape it properly
- Do NOT write \\N directly in docstrings - Python will raise SyntaxError
- Example: Use cell.replace(r'\\N', '') or cell.replace('\\\\N', '')

**CRITICAL PATTERNS (LEARNED FROM SUCCESSFUL MIGRATIONS):**

1. **NULL HANDLING FOR DATE COLUMNS** (⚠️ Most common failure!):
   SQLite exports may contain '\N' or '\\N' which MySQL rejects for DATE columns.
   MUST do this in data_convertor.py:
   ```python
   # Clean each cell
   cell = cell.strip()
   # Replace both '\N' and '\\N' with empty string
   cell = cell.replace('\\N', '').replace('\N', '')
   if cell == '':
       processed_row.append('')  # Empty string = NULL in MySQL
   ```

2. **VARCHAR LENGTH** (⚠️ Second most common failure!):
   Look at CSV summaries "Max length" - don't guess!
   - If max data is 14 chars, don't use VARCHAR(2)
   - Common: 'Adult' (5), 'Post Secondary' (14), grade values
   - Add 50-100% buffer: if max is 14, use VARCHAR(30)

3. **SCHEMA VALIDATION**:
   - Check CSV summaries for actual VARCHAR max lengths before setting schema
   - Foreign key columns must match exactly (length and type)

**OUTPUT FORMAT (JSON only, no markdown):**
{
  "mysql_schema": "-- Full MySQL schema here...",
  "data_convertor": "#!/usr/bin/env python3\\n# Full script here..."
}

Output your conversion now:
"""
        
        return prompt

