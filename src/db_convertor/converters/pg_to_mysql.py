"""PostgreSQL to MySQL converter."""

from typing import Dict, Optional, List
from .base import DatabaseConverter, ConversionConfig
from ..exporters.pg_exporter import PostgreSQLExporter
from ..importers.mysql_importer import MySQLImporter


class PGToMySQLConverter(DatabaseConverter):
    """Convert from PostgreSQL to MySQL."""
    
    def __init__(self, config: ConversionConfig):
        """Initialize PostgreSQL to MySQL converter."""
        super().__init__(config)
        if config.source_type != 'postgresql':
            raise ValueError(f"Source type must be 'postgresql', got '{config.source_type}'")
        if config.target_type != 'mysql':
            raise ValueError(f"Target type must be 'mysql', got '{config.target_type}'")
    
    def get_exporter(self):
        """Get PostgreSQL exporter instance."""
        return PostgreSQLExporter(self.config.source_connection)
    
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
        error: Optional[str] = None,
        history: Optional[List[Dict]] = None
    ) -> str:
        """Build prompt for AI agent to convert PostgreSQL schema to MySQL.
        
        Args:
            source_schema: PostgreSQL schema (CREATE statements)
            csv_summaries: Dictionary of table names to CSV summaries
            prev_schema: Previous MySQL schema (if any)
            prev_convertor: Previous data convertor script (if any)
            error: Error from previous attempt (if any)
            history: List of previous attempts with errors
            
        Returns:
            Prompt string for AI agent
        """
        # Format CSV summaries with max column lengths prominently displayed
        csv_summary_parts = []
        for table, summary in csv_summaries.items():
            part = f"Table: {table}\n"
            part += f"Columns: {', '.join(summary.get('columns', []))}\n"
            part += f"Total rows: {summary.get('total_rows', 0)}\n"
            
            # Show max lengths prominently
            if 'max_lengths' in summary:
                part += "\n⚠️ MAX COLUMN LENGTHS (USE THESE FOR VARCHAR SIZING!):\n"
                max_lens = summary['max_lengths']
                for col, length in sorted(max_lens.items(), key=lambda x: -x[1])[:20]:  # Top 20 longest
                    if length > 0:
                        part += f"  {col}: {length} chars\n"
            
            # Show sample data
            if summary.get('first_lines'):
                part += f"\nFirst {len(summary['first_lines'])} rows:\n"
                for row in summary['first_lines']:
                    part += f"  {row}\n"
            
            csv_summary_parts.append(part)
        
        csv_summary_text = "\n\n".join(csv_summary_parts)
        
        prompt = f"""You are a database conversion expert. Convert this PostgreSQL database to MySQL.

===== SOURCE POSTGRESQL SCHEMA =====
{source_schema}

===== CSV DATA SUMMARY =====
{csv_summary_text}

===== CONVERSION RULES =====

**Data Types:**
- SERIAL → INT AUTO_INCREMENT
- BIGSERIAL → BIGINT AUTO_INCREMENT
- TEXT → Use max_lengths from CSV summary to decide:
  * If max_length < 100 → VARCHAR(max * 2) with minimum VARCHAR(50)
  * If max_length < 255 → VARCHAR(max * 1.5) or VARCHAR(255)
  * If max_length > 255 → TEXT
  * If all NULL/empty → VARCHAR(255) default
- CHARACTER VARYING(n) → VARCHAR(n)
- INTEGER → INT
- BIGINT → BIGINT
- SMALLINT → SMALLINT
- DOUBLE PRECISION → DOUBLE
- NUMERIC/DECIMAL → DECIMAL
- BOOLEAN → TINYINT(1)
- DATE → DATE
- TIMESTAMP → DATETIME
- BYTEA → BLOB

**NULL Representation:**
- PostgreSQL CSV exports may contain empty strings for NULL
- MySQL uses empty string as NULL for nullable columns
- No special conversion needed

**Boolean Conversion:**
- PostgreSQL: t/f or true/false
- MySQL: 1/0 for TINYINT(1)
- Convert in data_convertor.py

**Identifier Quoting:**
- PostgreSQL uses double quotes: "column_name"
- MySQL uses backticks: `column_name`
- Convert all identifiers

**Foreign Keys:**
- Maintain all foreign key relationships
- Convert table/column names to MySQL style

**CRITICAL PATTERNS (LEARNED FROM SUCCESSFUL MIGRATIONS):**

1. **NULL HANDLING** (⚠️ Critical for DATE, DATETIME, INT columns!):
   CSV files may contain various NULL representations that MySQL rejects.
   MUST clean all cells in data_convertor.py:
   ```python
   # Clean each cell - handle all NULL representations
   cell = str(cell).strip()
   # Replace common NULL representations
   if cell in ('', 'NULL', 'null', '\\N', 'N/A', 'NA'):
       cell = ''
   # For backslash-N (common in PostgreSQL exports)
   cell = cell.replace('\\\\N', '').replace('\\N', '')
   
   # Write empty string for NULL (MySQL interprets as NULL)
   processed_row.append('' if cell == '' else cell)
   ```

2. **VARCHAR LENGTH** (⚠️ Most common failure!):
   The CSV summaries show MAX COLUMN LENGTHS for every column.
   
   **MANDATORY RULE**: 
   - For VARCHAR columns, look at "MAX COLUMN LENGTHS" section in CSV summary
   - Add 50-100% buffer to max length for safety
   - Examples:
     * Max length = 16 chars → Use VARCHAR(30) or VARCHAR(50)
     * Max length = 89 chars → Use VARCHAR(150) or VARCHAR(200)
     * Max length = 5 chars → Use VARCHAR(10) minimum
   
   **Default safe sizes** (if no data or all NULL):
   - Short text fields → VARCHAR(50)
   - Names, codes → VARCHAR(100)
   - Descriptions, URLs → VARCHAR(255) or TEXT
   
   ⚠️ NEVER use VARCHAR(2) or VARCHAR(10) without checking actual max length!

3. **BOOLEAN CONVERSION**:
   ```python
   # In data_convertor.py, for BOOLEAN/TINYINT(1) columns:
   if cell.lower() in ('t', 'true', '1', 'yes'):
       cell = '1'
   elif cell.lower() in ('f', 'false', '0', 'no', ''):
       cell = '0'
   ```

**data_convertor Requirements:**
- Accept TWO positional command-line arguments: source_dir and output_dir
  Usage: python3 data_convertor.py <source_dir> <output_dir>
- Read all CSV files from source directory
- Convert data for MySQL compatibility:
  * Handle NULL values (empty strings → NULL for numeric/date types)
  * Convert boolean values (t/true → 1, f/false → 0 for TINYINT)
  * Ensure date formats are YYYY-MM-DD
  * Ensure datetime formats are YYYY-MM-DD HH:MM:SS
  * Validate VARCHAR lengths match schema
- Write converted CSVs to output directory
- Print progress and any warnings

**IMPORTANT: Python Syntax**
- When writing Python code that mentions backslash-N, use raw strings r"\\N" or escape it properly
- Do NOT write backslash-N directly in docstrings - Python will raise SyntaxError
- Example: Use cell.replace(r'\\N', '') or cell.replace('\\\\N', '')
"""
        
        # Add previous attempts history if available
        if history:
            prompt += "\n===== PREVIOUS ATTEMPTS HISTORY ====="
            for i, attempt_data in enumerate(history):
                prompt += f"\n--- Attempt {i+1} ---"
                if 'error' in attempt_data and attempt_data['error']:
                    prompt += f"\nPrevious Error:\n{attempt_data['error']}"
                if 'mysql_schema' in attempt_data and attempt_data['mysql_schema']:
                    prompt += f"\nPrevious MySQL Schema:\n{attempt_data['mysql_schema'][:500]}..."
                if 'data_convertor' in attempt_data and attempt_data['data_convertor']:
                    prompt += f"\nPrevious Data Convertor:\n{attempt_data['data_convertor'][:500]}..."
            prompt += "\n\n"
        
        # Add current error if any
        if error:
            prompt += f"""
===== PIPELINE ERROR FROM PREVIOUS ATTEMPT =====
{error}

Please analyze the error and fix it in this attempt.
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
Generate TWO files in JSON format:

{
  "mysql_schema": "Full MySQL CREATE TABLE statements with all columns, data types, primary keys, and foreign keys",
  "data_convertor": "Complete Python script that takes source_dir and output_dir as positional arguments"
}

**CRITICAL**: Use the MAX COLUMN LENGTHS from CSV summaries to set VARCHAR sizes. Do NOT guess!
**CRITICAL**: Handle all NULL representations in data_convertor.py
**CRITICAL**: Convert PostgreSQL booleans (t/f) to MySQL (1/0)
"""
        
        return prompt

