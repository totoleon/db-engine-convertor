"""AI agent for database conversion."""

import json
import csv
from pathlib import Path
from typing import Dict, Optional, Tuple
from ..utils.llm import gemini_inference


def get_csv_summary(csv_path: Path, num_lines=5) -> Dict:
    """Get summary of CSV file: first N and last N lines, column names, row count.
    
    Args:
        csv_path: Path to CSV file
        num_lines: Number of lines to include from start and end
        
    Returns:
        Dict with columns, total_rows, first_lines, last_lines
    """
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


class ConversionAgent:
    """AI agent for generating database conversion artifacts."""
    
    def __init__(self, converter):
        """Initialize agent.
        
        Args:
            converter: DatabaseConverter instance
        """
        self.converter = converter
    
    def get_csv_summaries(self, csv_dir: Path) -> Dict[str, Dict]:
        """Get summaries for all CSV files in directory.
        
        Args:
            csv_dir: Directory containing CSV files
            
        Returns:
            Dict mapping table names to CSV summaries
        """
        csv_summaries = {}
        csv_files = sorted(Path(csv_dir).glob('*.csv'))
        for csv_file in csv_files:
            table_name = csv_file.stem
            csv_summaries[table_name] = get_csv_summary(csv_file)
        return csv_summaries
    
    def generate_conversion_artifacts(
        self,
        source_schema_path: Path,
        source_csv_dir: Path,
        prev_schema_path: Optional[Path] = None,
        prev_convertor_path: Optional[Path] = None,
        pipeline_error: Optional[str] = None
    ) -> Tuple[str, str]:
        """Generate schema and data convertor using AI.
        
        Args:
            source_schema_path: Path to source database schema file
            source_csv_dir: Directory containing source CSV files
            prev_schema_path: Path to previous target schema (if any)
            prev_convertor_path: Path to previous convertor script (if any)
            pipeline_error: Error message from previous pipeline run (if any)
            
        Returns:
            Tuple of (target_schema, data_convertor_script)
        """
        # Read source schema
        with open(source_schema_path, 'r', encoding='utf-8') as f:
            source_schema = f.read()
        
        # Get CSV summaries
        csv_summaries = self.get_csv_summaries(source_csv_dir)
        
        # Build prompt using converter-specific logic
        prompt = self.converter.get_schema_conversion_prompt(
            source_schema,
            csv_summaries,
            str(prev_schema_path) if prev_schema_path else None,
            str(prev_convertor_path) if prev_convertor_path else None,
            pipeline_error
        )
        
        print("=" * 80)
        print("Calling AI agent...")
        print("=" * 80)
        
        # Call AI
        response = gemini_inference(prompt, temperature=0.3, enforce_json=True)
        
        # Parse response
        try:
            result = json.loads(response)
            
            # Get keys based on target database type
            schema_key = f"{self.converter.target_type}_schema"
            convertor_key = "data_convertor"
            
            # Try alternate key names for backwards compatibility
            if schema_key not in result:
                # Try without database type prefix
                schema_key = "pg_schema" if "pg_schema" in result else "target_schema"
            
            if convertor_key not in result:
                convertor_key = "csv_convertor" if "csv_convertor" in result else "data_convertor"
            
            return result[schema_key], result[convertor_key]
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Error parsing agent response: {e}")
            print("Raw response:")
            print(response)
            raise

