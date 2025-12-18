"""Orchestrator for managing the conversion process."""

import os
import shutil
import json
from pathlib import Path
from datetime import datetime
from typing import Optional
from .agent import ConversionAgent
from .pipeline import ConversionPipeline


class ConversionOrchestrator:
    """Orchestrates the database conversion process with iterative refinement."""
    
    def __init__(self, converter):
        """Initialize orchestrator.
        
        Args:
            converter: DatabaseConverter instance
        """
        self.converter = converter
        self.config = converter.config
        self.agent = ConversionAgent(converter)
        self.importer = converter.get_importer()
        self.pipeline = ConversionPipeline(converter, self.importer)
    
    def run_conversion(
        self,
        export_source: bool = True,
        migration_dir: Optional[Path] = None
    ) -> bool:
        """Run the full conversion process with iterative refinement.
        
        Full flow:
        1. Export source database (if export_source=True)
        2. Generate conversion artifacts using AI
        3. Execute pipeline (create schema + convert data)
        4. Iterate until success or max attempts
        
        Args:
            export_source: Whether to export source database (default: True)
            migration_dir: Directory to store migration artifacts (auto-generated if None)
            
        Returns:
            True if successful, False otherwise
        """
        # Set up migration directory
        if migration_dir is None:
            migration_name = self.config.get_migration_name()
            migration_dir = self.config.work_dir / 'migrations' / migration_name
        
        migration_dir = Path(migration_dir)
        migration_dir.mkdir(parents=True, exist_ok=True)
        
        # Set up subdirectories
        artifacts_dir = migration_dir / 'artifacts'
        artifacts_dir.mkdir(exist_ok=True)
        
        converted_csv_dir = migration_dir / 'converted'
        converted_csv_dir.mkdir(exist_ok=True)
        
        logs_dir = migration_dir / 'logs'
        logs_dir.mkdir(exist_ok=True)
        
        source_dir = migration_dir / 'source'
        source_dir.mkdir(exist_ok=True)
        
        # STEP 1: Export source database
        if export_source:
            print("\n" + "=" * 80)
            print("STEP 1: EXPORTING SOURCE DATABASE")
            print("=" * 80)
            print(f"Database: {self.config.source_connection}")
            print(f"Type: {self.config.source_type}")
            print(f"Output: {source_dir}")
            
            try:
                exporter = self.converter.get_exporter()
                result = exporter.export_all(source_dir)
                
                print(f"\n✓ Export completed!")
                print(f"  Schema: {result['schema_path']}")
                print(f"  Tables: {result['table_count']}")
                print(f"  Total rows: {result['total_rows']}")
                
                if hasattr(exporter, 'close'):
                    exporter.close()
                    
            except Exception as e:
                print(f"\n✗ Export failed: {e}")
                import traceback
                traceback.print_exc()
                return False
        else:
            print("\n" + "=" * 80)
            print("STEP 1: USING EXISTING SOURCE DATA")
            print("=" * 80)
            print(f"Source: {source_dir}")
            
            # Verify source files exist
            if not (source_dir / 'schema.sql').exists():
                print(f"✗ Error: schema.sql not found in {source_dir}")
                return False
        
        # Save configuration
        config_path = migration_dir / 'config.json'
        with open(config_path, 'w') as f:
            json.dump({
                'source_type': self.config.source_type,
                'target_type': self.config.target_type,
                'database_name': self.config.database_name,
                'source_connection': self.config.source_connection,
                'timestamp': datetime.now().isoformat(),
                'max_attempts': self.config.max_attempts,
            }, f, indent=2)
        
        print("\n" + "=" * 80)
        print(f"{self.config.source_type.upper()} → {self.config.target_type.upper()} Migration")
        print("=" * 80)
        print(f"Database: {self.config.database_name}")
        print(f"Source: {self.config.source_connection}")
        print(f"Migration dir: {migration_dir}")
        print(f"Max attempts: {self.config.max_attempts}")
        print("=" * 80)
        
        # Orchestration loop
        pipeline_error = None
        prev_schema = None
        prev_convertor = None
        error_file = logs_dir / 'pipeline_error.txt'
        
        for attempt in range(1, self.config.max_attempts + 1):
            print(f"\n{'=' * 80}")
            print(f"ATTEMPT {attempt}/{self.config.max_attempts}")
            print(f"{'=' * 80}\n")
            
            # STEP 2: Run agent to generate artifacts
            try:
                print("STEP 2: AI AGENT - GENERATING CONVERSION ARTIFACTS")
                print("=" * 80)
                target_schema, data_convertor = self.agent.generate_conversion_artifacts(
                    source_dir / 'schema.sql',
                    source_dir,
                    prev_schema,
                    prev_convertor,
                    pipeline_error
                )
                
                # Save outputs to artifacts directory
                schema_filename = self.converter.get_schema_filename()
                convertor_filename = self.converter.get_convertor_filename()
                
                schema_path = artifacts_dir / schema_filename
                convertor_path = artifacts_dir / convertor_filename
                
                with open(schema_path, 'w', encoding='utf-8') as f:
                    f.write(target_schema)
                print(f"✓ Saved {schema_filename} ({len(target_schema)} chars)")
                
                with open(convertor_path, 'w', encoding='utf-8') as f:
                    f.write(data_convertor)
                os.chmod(convertor_path, 0o755)
                print(f"✓ Saved {convertor_filename} ({len(data_convertor)} chars)")
                
                # Update previous paths for next iteration
                prev_schema = schema_path
                prev_convertor = convertor_path
                
                # Save attempt log
                attempt_log = logs_dir / f'attempt_{attempt}.log'
                with open(attempt_log, 'w') as f:
                    f.write(f"Attempt {attempt}\n")
                    f.write(f"Schema size: {len(target_schema)}\n")
                    f.write(f"Convertor size: {len(data_convertor)}\n")
                
            except Exception as e:
                print(f"✗ Agent failed: {e}")
                import traceback
                traceback.print_exc()
                continue
            
            # STEP 3: Run pipeline
            print("\nSTEP 3: PIPELINE - EXECUTING CONVERSION")
            print("=" * 80)
            error = self.pipeline.run(
                schema_path,
                convertor_path,
                source_dir,
                converted_csv_dir
            )
            
            if error is None:
                print("\n" + "=" * 80)
                print("🎉 SUCCESS! Migration completed successfully!")
                print("=" * 80)
                print(f"\nMigration artifacts saved to:")
                print(f"  {migration_dir}")
                print(f"\nFinal artifacts:")
                print(f"  - Target schema: {schema_path}")
                print(f"  - Data convertor: {convertor_path}")
                print(f"  - Converted CSVs: {converted_csv_dir}")
                print(f"  - Configuration: {config_path}")
                
                # Save success marker
                success_file = migration_dir / 'SUCCESS'
                with open(success_file, 'w') as f:
                    f.write(f"Migration completed successfully on attempt {attempt}\n")
                    f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                
                return True
            else:
                print(f"\n✗ Pipeline failed on attempt {attempt}")
                pipeline_error = error
                
                # Save error to file
                with open(error_file, 'w', encoding='utf-8') as f:
                    f.write(error)
                print(f"Error saved to {error_file}")
                
                if attempt < self.config.max_attempts:
                    print(f"\n🔄 Will retry with error feedback...")
                else:
                    print(f"\n✗ Max attempts ({self.config.max_attempts}) reached. Giving up.")
                    
                    # Save failure marker
                    failure_file = migration_dir / 'FAILED'
                    with open(failure_file, 'w') as f:
                        f.write(f"Migration failed after {self.config.max_attempts} attempts\n")
                        f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                        f.write(f"Last error:\n{error}\n")
        
        return False

