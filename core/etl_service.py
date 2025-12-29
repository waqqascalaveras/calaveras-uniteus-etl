"""
ETL Orchestration Service

ETL (Extract, Transform, Load) orchestration service managing complete data
pipeline execution with status tracking, progress reporting, and job management.
Coordinates file discovery, data cleaning, validation, and database loading.

Author: Waqqas Hanafi
Copyright: © 2025 Calaveras County Health and Human Services Agency
"""

import threading
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import logging
import hashlib
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import config
from .database import get_database_manager
from .audit_logger import get_audit_logger, AuditCategory, AuditAction
from .schema_validator import get_schema_validator, SchemaError
from .siem_logger import get_siem_logger, SIEMEventType, SIEMSeverity


class ETLJobStatus(Enum):
    """ETL job execution status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class FileProcessingStatus(Enum):
    """Individual file processing status"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class FileProcessingTask:
    """Individual file processing task"""
    file_path: Path
    table_name: str
    file_date: str
    file_hash: str
    status: FileProcessingStatus = FileProcessingStatus.PENDING
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    records_processed: int = 0
    records_loaded: int = 0
    records_inserted: int = 0  # New records added
    records_updated: int = 0   # Existing records updated
    records_skipped: int = 0   # Duplicate/unchanged records
    issues_found: int = 0
    error_message: Optional[str] = None
    metadata_id: Optional[int] = None
    
    @property
    def processing_time_seconds(self) -> float:
        """Calculate processing time in seconds"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'file_path': str(self.file_path),
            'file_name': self.file_path.name,
            'table_name': self.table_name,
            'file_date': self.file_date,
            'status': self.status.value,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'records_processed': self.records_processed,
            'records_loaded': self.records_loaded,
            'records_inserted': self.records_inserted,
            'records_updated': self.records_updated,
            'records_skipped': self.records_skipped,
            'issues_found': self.issues_found,
            'processing_time_seconds': self.processing_time_seconds,
            'error_message': self.error_message
        }


@dataclass
class ETLJobProgress:
    """ETL job progress tracking"""
    job_id: str = ""
    status: ETLJobStatus = ETLJobStatus.PENDING
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    total_files: int = 0
    completed_files: int = 0
    failed_files: int = 0
    skipped_files: int = 0
    total_records_processed: int = 0
    total_records_loaded: int = 0
    total_issues_found: int = 0
    current_file: Optional[str] = None
    error_messages: List[str] = field(default_factory=list)
    file_results: List['FileProcessingTask'] = field(default_factory=list)  # Track individual file results
    trigger_type: str = "manual"  # 'manual' or 'automatic'
    triggered_by: Optional[str] = None  # Username who triggered the job
    
    @property
    def completion_percentage(self) -> float:
        """Calculate completion percentage"""
        if self.total_files == 0:
            return 0.0
        processed = self.completed_files + self.failed_files + self.skipped_files
        return (processed / self.total_files) * 100
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate"""
        processed = self.completed_files + self.failed_files
        if processed == 0:
            return 0.0
        return (self.completed_files / processed) * 100
    
    @property
    def is_running(self) -> bool:
        """Check if job is currently running"""
        return self.status == ETLJobStatus.RUNNING
    
    @property
    def is_completed(self) -> bool:
        """Check if job is completed (success or failure)"""
        return self.status in [ETLJobStatus.COMPLETED, ETLJobStatus.FAILED, ETLJobStatus.CANCELLED]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses"""
        duration_seconds = 0.0
        if self.start_time and self.end_time:
            duration_seconds = (self.end_time - self.start_time).total_seconds()
        
        # Calculate total files processed (completed + failed + skipped)
        files_processed = self.completed_files + self.failed_files + self.skipped_files
        
        return {
            'job_id': self.job_id,
            'status': self.status.value,
            'is_running': self.is_running,
            'is_completed': self.is_completed,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': duration_seconds,
            'total_files': self.total_files,
            'files_processed': files_processed,
            'completed_files': self.completed_files,
            'failed_files': self.failed_files,
            'skipped_files': self.skipped_files,
            'completion_percentage': self.completion_percentage,
            'success_rate': self.success_rate,
            'total_records': self.total_records_loaded,  # Alias for UI compatibility
            'total_records_processed': self.total_records_processed,
            'total_records_loaded': self.total_records_loaded,
            'total_issues_found': self.total_issues_found,
            'current_file': self.current_file,
            'error_messages': self.error_messages,
            'files': [result.to_dict() for result in self.file_results],  # Include file-level details
            'trigger_type': self.trigger_type,
            'triggered_by': self.triggered_by
        }


class FileDiscoveryService:
    """Service for discovering and analyzing data files"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def discover_files(self, directory: Path = None, force_reprocess: bool = False, selected_files: List[str] = None) -> List[FileProcessingTask]:
        """Discover data files and create processing tasks"""
        if not directory:
            directory = config.directories.input_dir
        
        self.logger.info(f"Discovering files in {directory}")
        
        if not directory.exists():
            self.logger.warning(f"Directory does not exist: {directory}")
            return []
        
        # Find data files using configured patterns
        file_patterns = config.etl.file_patterns
        data_files = []
        
        for pattern in file_patterns:
            data_files.extend(directory.glob(pattern))
        
        # Filter by selected files if provided
        if selected_files:
            selected_set = set(selected_files)
            data_files = [f for f in data_files if f.name in selected_set]
            self.logger.info(f"Filtering to {len(data_files)} selected files")
        
        if not data_files:
            self.logger.info("No data files found")
            return []
        
        # Get already processed files if not forcing reprocess
        processed_files = set()
        if not force_reprocess:
            db_manager = get_database_manager()
            processed_files = set(db_manager.etl_metadata.get_processed_files())
        
        # Create processing tasks
        tasks = []
        for file_path in data_files:
            try:
                task = self._create_processing_task(file_path)
                
                # Check if already processed (only if not forcing reprocess)
                if not force_reprocess:
                    file_key = (task.file_path.name, task.file_hash)
                    if file_key in processed_files:
                        task.status = FileProcessingStatus.SKIPPED
                        self.logger.debug(f"Skipping already processed file: {file_path.name}")
                
                tasks.append(task)
                
            except Exception as e:
                self.logger.error(
                    f"Error analyzing file: {file_path.name}\n"
                    f"  File Path: {file_path}\n"
                    f"  Exception Type: {type(e).__name__}\n"
                    f"  Message: {str(e)}",
                    exc_info=True
                )
        
        self.logger.info(f"Discovered {len(tasks)} files ({len([t for t in tasks if t.status != FileProcessingStatus.SKIPPED])} new)")
        
        return tasks
    
    def _create_processing_task(self, file_path: Path) -> FileProcessingTask:
        """Create a processing task for a file"""
        # Extract table name from filename
        table_name = self._extract_table_name(file_path.name)
        
        # Extract file date (will use file modified date if not in filename)
        file_date = self._extract_file_date(file_path)
        
        # Calculate file hash
        file_hash = self._calculate_file_hash(file_path)
        
        return FileProcessingTask(
            file_path=file_path,
            table_name=table_name,
            file_date=file_date,
            file_hash=file_hash
        )
    
    def _extract_table_name(self, filename: str) -> str:
        """Extract table name from filename
        
        First checks configurable file-to-table mappings, then falls back to
        pattern-based extraction.
        
        Handles various filename patterns by removing known prefixes and date suffixes:
        - SAMPLE_chhsca_people_20250828.txt → chhsca_people
        - chhsca_people_20250828.txt → chhsca_people
        - people_20250828.txt → people
        - people.txt → people
        
        Configurable prefixes to ignore are defined in config.etl.ignored_filename_prefixes
        """
        # Check configurable mappings first
        try:
            import sqlite3
            from pathlib import Path
            internal_db = config.directories.database_dir / "internal.db"
            if internal_db.exists():
                with sqlite3.connect(internal_db) as conn:
                    conn.row_factory = sqlite3.Row
                    # Try exact filename match first
                    cursor = conn.execute("""
                        SELECT table_name FROM file_table_mappings
                        WHERE file_pattern = ? AND is_active = 1
                        LIMIT 1
                    """, (filename,))
                    row = cursor.fetchone()
                    if row:
                        return row['table_name']
                    
                    # Try pattern matching (simple wildcard support)
                    cursor = conn.execute("""
                        SELECT table_name, file_pattern FROM file_table_mappings
                        WHERE is_active = 1
                    """)
                    for row in cursor.fetchall():
                        pattern = row['file_pattern']
                        # Simple wildcard matching
                        if '*' in pattern:
                            import fnmatch
                            if fnmatch.fnmatch(filename, pattern):
                                return row['table_name']
                        elif pattern in filename:
                            return row['table_name']
        except Exception as e:
            self.logger.debug(f"Error checking file mappings: {e}")
        
        # Fall back to pattern-based extraction
        # Remove extension using configured extensions
        name = filename
        for ext in config.etl.recognized_extensions:
            name = name.replace(ext, '')
        
        name_parts = name.split('_')
        
        # Get ignored prefixes from config (convert to uppercase for case-insensitive comparison)
        ignored_prefixes = [p.upper() for p in config.etl.ignored_filename_prefixes]
        
        # Find table name (skip configured prefixes and date suffix)
        table_parts = []
        for part in name_parts:
            # Skip configured prefixes (case-insensitive)
            if part.upper() in ignored_prefixes:
                continue
            # Stop at date pattern YYYYMMDD
            if part.isdigit() and len(part) == 8:
                break
            table_parts.append(part)
        
        return '_'.join(table_parts) if table_parts else 'unknown_table'
    
    def _extract_file_date(self, file_path: Path) -> str:
        """Extract date from filename, fallback to file modified date
        
        Looks for YYYYMMDD pattern in filename. If not found, uses file's
        last modified timestamp to generate a date string.
        """
        filename = file_path.name
        
        # Look for YYYYMMDD pattern in filename
        parts = filename.split('_')
        for part in parts:
            if part.isdigit() and len(part) == 8:
                # Validate it's a real date
                try:
                    year = int(part[0:4])
                    month = int(part[4:6])
                    day = int(part[6:8])
                    # Try to create date to validate
                    datetime(year, month, day)
                    return part
                except (ValueError, TypeError):
                    continue
        
        # No valid date in filename, use file modified time
        modified_time = datetime.fromtimestamp(file_path.stat().st_mtime)
        return modified_time.strftime('%Y%m%d')
    
    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate file hash for duplicate detection"""
        hasher = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()


class DataProcessingService:
    """Service for processing individual data files"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def process_file(self, task: FileProcessingTask, username: str = "system", trigger_type: str = "manual") -> FileProcessingTask:
        """Process a single file
        
        Args:
            task: File processing task
            username: User who triggered the processing
            trigger_type: Type of trigger - 'manual' or 'automatic'
        """
        if task.status == FileProcessingStatus.SKIPPED:
            # Log skipped file
            audit_logger = get_audit_logger()
            audit_logger.log(
                username=username,
                action=AuditAction.FILE_SKIPPED,
                category=AuditCategory.ETL,
                success=True,
                details="File already processed",
                target_resource=task.file_path.name
            )
            return task
        
        task.status = FileProcessingStatus.PROCESSING
        task.start_time = datetime.now()
        
        self.logger.info(f"Processing file: {task.file_path.name} -> {task.table_name} (trigger: {trigger_type})")
        
        audit_logger = get_audit_logger()
        
        try:
            db_manager = get_database_manager()
            
            # Log processing start with trigger information
            task.metadata_id = db_manager.etl_metadata.log_processing_start(
                task.file_path.name,
                task.table_name,
                task.file_date,
                task.file_hash,
                trigger_type,
                username
            )
            
            # Read and clean data
            df_raw = self._read_file(task.file_path)
            if df_raw.empty:
                task.status = FileProcessingStatus.SKIPPED
                task.error_message = "Empty file"
                
                # Log empty file
                audit_logger.log(
                    username=username,
                    action=AuditAction.FILE_SKIPPED,
                    category=AuditCategory.ETL,
                    success=True,
                    details="File is empty",
                    target_resource=task.file_path.name
                )
                return task
            
            task.records_processed = len(df_raw)
            
            # Validate schema before processing
            schema_validator = get_schema_validator()
            file_columns = list(df_raw.columns)
            schema_errors = schema_validator.validate_schema(
                task.table_name,
                file_columns,
                task.file_path.name
            )
            
            if schema_errors:
                # Log schema errors as critical events
                error_messages = []
                for error in schema_errors:
                    if error.severity == 'critical':
                        # Log to schema_errors table
                        schema_validator.log_schema_error(error, username)
                        
                        # Log to audit logger
                        audit_logger.log(
                            username=username,
                            action=AuditAction.FILE_PROCESSED,
                            category=AuditCategory.ETL,
                            success=False,
                            details=f"Schema mismatch: {error.error_message}",
                            target_resource=task.file_path.name,
                            error_message=error.error_message
                        )
                        
                        # Log to SIEM if enabled
                        try:
                            siem_logger = get_siem_logger()
                            if siem_logger:
                                siem_logger.log_event(
                                    event_type=SIEMEventType.ERROR,
                                    message=f"Schema validation error during file import: {error.error_message}",
                                    severity=SIEMSeverity.CRITICAL,
                                    username=username,
                                    resource=task.file_path.name,
                                    success=False,
                                    additional_data={
                                        'error_type': error.error_type,
                                        'table_name': error.table_name,
                                        'file_name': error.file_name,
                                        'error_details': error.error_details
                                    }
                                )
                        except Exception as siem_err:
                            self.logger.warning(f"Failed to log to SIEM: {siem_err}")
                        
                        error_messages.append(error.error_message)
                
                # Fail the import if there are critical errors
                critical_errors = [e for e in schema_errors if e.severity == 'critical']
                if critical_errors:
                    task.status = FileProcessingStatus.FAILED
                    task.error_message = (
                        f"Schema validation failed:\n" +
                        "\n".join(f"  - {e.error_message}" for e in critical_errors) +
                        f"\n\nPlease check the Schema section in AdminCP Database settings for SQL commands to fix these issues."
                    )
                    
                    # Log failure
                    if task.metadata_id:
                        db_manager.etl_metadata.log_processing_complete(
                            task.metadata_id,
                            0,
                            'failed',
                            task.error_message
                        )
                    
                    self.logger.error(
                        f"Schema validation failed for {task.file_path.name} -> {task.table_name}:\n" +
                        "\n".join(f"  - {e.error_message}" for e in critical_errors)
                    )
                    return task
            
            # Clean data (simplified for now - can be expanded with data cleaning logic)
            df_clean, issues = self._clean_data(df_raw, task.table_name, task.file_path.name)
            task.issues_found = len(issues)
            
            # Load into database using upsert to handle duplicates
            table_repo = db_manager.get_repository(task.table_name)
            
            # Get primary key for this table
            primary_key = config.data_quality.primary_keys.get(task.table_name)
            
            if primary_key and primary_key in df_clean.columns:
                # Use upsert to prevent duplicates
                self.logger.info(f"Using upsert for {task.table_name} with primary key {primary_key} ({len(df_clean)} records)")
                load_result = table_repo.upsert_dataframe(df_clean, primary_key)
            else:
                # Fall back to append if no primary key defined or not in data
                if primary_key:
                    self.logger.warning(f"Primary key '{primary_key}' not found in {task.table_name} columns: {list(df_clean.columns)[:5]}... Using append mode")
                else:
                    self.logger.warning(f"No primary key configured for {task.table_name}, using append mode")
                load_result = table_repo.insert_dataframe(df_clean)
            
            if load_result.success:
                task.records_loaded = load_result.row_count
                task.records_inserted = load_result.inserted_count
                task.records_updated = load_result.updated_count
                task.records_skipped = load_result.skipped_count
                task.status = FileProcessingStatus.COMPLETED
                
                # Log successful completion
                db_manager.etl_metadata.log_processing_complete(
                    task.metadata_id,
                    task.records_loaded,
                    'success',
                    None,
                    task.records_inserted,
                    task.records_updated
                )
                
                # Log data quality issues if any
                if issues:
                    db_manager.data_quality.log_issues(issues)
                
                self.logger.info(
                    f"Successfully processed {task.file_path.name}: "
                    f"{task.records_processed} records → "
                    f"{task.records_inserted} inserted, {task.records_updated} updated, {task.records_skipped} skipped"
                )
                
                # Audit log successful file processing
                audit_logger.log(
                    username=username,
                    action=AuditAction.FILE_PROCESSED,
                    category=AuditCategory.ETL,
                    success=True,
                    details=f"{task.records_inserted} inserted, {task.records_updated} updated, {task.records_skipped} skipped into {task.table_name}",
                    target_resource=task.file_path.name,
                    duration_ms=int(task.processing_time_seconds * 1000) if task.end_time else None,
                    record_count=task.records_loaded,
                    file_size=task.file_path.stat().st_size
                )
                
            else:
                task.status = FileProcessingStatus.FAILED
                task.error_message = load_result.error_message
                
                # Log failure
                db_manager.etl_metadata.log_processing_complete(
                    task.metadata_id,
                    0,
                    'failed',
                    task.error_message
                )
                
                # Audit log failed file processing
                audit_logger.log(
                    username=username,
                    action=AuditAction.FILE_PROCESSED,
                    category=AuditCategory.ETL,
                    success=False,
                    details=f"Failed to load into {task.table_name}",
                    target_resource=task.file_path.name,
                    error_message=task.error_message,
                    file_size=task.file_path.stat().st_size
                )
                
        except Exception as e:
            task.status = FileProcessingStatus.FAILED
            task.error_message = str(e)
            self.logger.exception(f"Error processing {task.file_path.name}")
            
            # Log failure if metadata_id exists
            if task.metadata_id:
                db_manager = get_database_manager()
                db_manager.etl_metadata.log_processing_complete(
                    task.metadata_id,
                    0,
                    'failed',
                    task.error_message
                )
            
            # Audit log file processing error
            audit_logger.log(
                username=username,
                action=AuditAction.FILE_PROCESSED,
                category=AuditCategory.ETL,
                success=False,
                details=f"Exception processing {task.table_name}",
                target_resource=task.file_path.name,
                error_message=str(e)
            )
        
        finally:
            task.end_time = datetime.now()
            # Explicit memory cleanup
            import gc
            gc.collect()
        
        return task
    
    def _read_file(self, file_path: Path) -> pd.DataFrame:
        """Read file with proper encoding handling and memory optimization"""
        encodings = ['utf-8', 'latin-1', 'cp1252']
        
        for encoding in encodings:
            try:
                df = pd.read_csv(
                    file_path,
                    sep='|',
                    quotechar='"',
                    encoding=encoding,
                    dtype=str,  # Read all as string initially 
                    na_values=['', 'NULL', 'null', 'None'],
                    low_memory=False  # Optimize memory usage
                )
                # Optimize memory by downcasting if possible
                return df
            except (UnicodeDecodeError, UnicodeError):
                if encoding == encodings[-1]:
                    raise
                continue
        
        raise ValueError(f"Could not read file {file_path} with any supported encoding")
    
    def _clean_data(self, df: pd.DataFrame, table_name: str, file_name: str) -> tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Clean data and track issues with detailed logging"""
        issues = []
        df_clean = df.copy()
        
        initial_count = len(df_clean)
        self.logger.info(f"🧹 Data Cleaning started for {table_name} ({file_name}): {initial_count} rows")
        
        # 1. Remove completely empty rows
        df_clean = df_clean.dropna(how='all')
        empty_rows_removed = initial_count - len(df_clean)
        
        if empty_rows_removed > 0:
            self.logger.info(f"  ├─ Removed {empty_rows_removed} completely empty rows")
            issues.append({
                'table_name': table_name,
                'issue_type': 'empty_rows',
                'issue_description': f'Removed {empty_rows_removed} empty rows',
                'file_name': file_name,
                'detected_at': datetime.now()
            })
        
        # 2. Handle null values by column
        null_stats = df_clean.isnull().sum()
        columns_with_nulls = null_stats[null_stats > 0]
        if len(columns_with_nulls) > 0:
            self.logger.info(f"  ├─ Null values detected in {len(columns_with_nulls)} column(s):")
            for col, count in columns_with_nulls.items():
                pct = (count / len(df_clean)) * 100
                self.logger.info(f"  │  ├─ {col}: {count} nulls ({pct:.1f}%)")
        
        # 3. Trim whitespace from text columns
        text_columns = df_clean.select_dtypes(include=['object']).columns
        trimmed_count = 0
        for column in text_columns:
            before = df_clean[column].astype(str)
            after = before.str.strip()
            changed = (before != after).sum()
            if changed > 0:
                df_clean[column] = after
                trimmed_count += changed
        
        if trimmed_count > 0:
            self.logger.info(f"  ├─ Trimmed whitespace from {trimmed_count} text values across {len(text_columns)} column(s)")
        
        # 4. Fix encoding issues in text fields
        encoding_fixes = 0
        for column in text_columns:
            original = df_clean[column].astype(str)
            df_clean[column] = original.replace({
                'â€™': "'",  # Smart apostrophe
                'â€œ': '"',  # Smart quote left
                'â€': '"',   # Smart quote right
                'nan': None
            })
            changes = (original != df_clean[column].astype(str)).sum()
            encoding_fixes += changes
        
        if encoding_fixes > 0:
            self.logger.info(f"  ├─ Fixed {encoding_fixes} encoding issues (smart quotes, etc.)")
        
        # 5. Apply PHI hashing if enabled
        if config.security.enable_phi_hashing and config.security.hash_on_import:
            hashed_fields = []
            for column in df_clean.columns:
                if config.security.should_hash_field(table_name, column):
                    # Hash the column values
                    df_clean[column] = df_clean[column].apply(
                        lambda x: config.security.hash_value(str(x)) if pd.notna(x) else x
                    )
                    hashed_fields.append(column)
            
            if hashed_fields:
                self.logger.info(f"  ├─ 🔒 Hashed {len(hashed_fields)} PHI field(s): {', '.join(hashed_fields)}")
                issues.append({
                    'table_name': table_name,
                    'issue_type': 'phi_hashing',
                    'issue_description': f'Hashed {len(hashed_fields)} PHI fields: {", ".join(hashed_fields)}',
                    'file_name': file_name,
                    'detected_at': datetime.now()
                })
        
        # 6. Detect and report data type conversions (informational)
        type_info = []
        for col in df_clean.columns:
            dtype = str(df_clean[col].dtype)
            if dtype == 'object':
                # Check if it's actually numeric
                try:
                    pd.to_numeric(df_clean[col], errors='coerce')
                    type_info.append(f"{col}=text")
                except:
                    type_info.append(f"{col}=text")
            else:
                type_info.append(f"{col}={dtype}")
        
        if type_info:
            self.logger.info(f"  ├─ Data types: {', '.join(type_info[:5])}{'...' if len(type_info) > 5 else ''}")
        
        # 7. Log final row count
        final_count = len(df_clean)
        self.logger.info(f"  └─ Cleaning complete: {final_count} rows ({initial_count - final_count} removed)")
        
        return df_clean, issues


class ETLOrchestrationService:
    """Main ETL orchestration service"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.discovery_service = FileDiscoveryService()
        self.processing_service = DataProcessingService()
        
        # Job management - REFACTORED to support multiple concurrent jobs
        self._active_jobs: Dict[str, ETLJobProgress] = {}  # Currently running jobs
        self._job_history: Dict[str, ETLJobProgress] = {}   # All jobs (active + completed)
        self._cancel_events: Dict[str, threading.Event] = {}  # Cancel event per job
        self._job_lock = threading.Lock()  # Thread safety for job management
        
        # Progress callbacks
        self._progress_callbacks: List[Callable[[ETLJobProgress], None]] = []
        
        # Database persistence - use config for internal database path
        self._internal_db_path = config.directories.database_dir / "internal.db"
        
        # Load job history from database on startup
        self._load_job_history_from_db()
    
    def _get_internal_db_connection(self) -> sqlite3.Connection:
        """Get connection to internal database"""
        self._internal_db_path.parent.mkdir(parents=True, exist_ok=True)
        return sqlite3.connect(str(self._internal_db_path))
    
    def _save_job_to_db(self, job: 'ETLJobProgress', username: str = "system"):
        """Save or update job in database"""
        try:
            with self._get_internal_db_connection() as conn:
                cursor = conn.cursor()
                
                # Calculate aggregated stats from file_results
                records_inserted = sum(f.records_inserted for f in job.file_results)
                records_updated = sum(f.records_updated for f in job.file_results)
                records_skipped = sum(f.records_skipped for f in job.file_results)
                total_records = records_inserted + records_updated + records_skipped
                
                # Use error_messages if available, otherwise None
                error_message = "; ".join(job.error_messages[:3]) if job.error_messages else None
                
                # Upsert job record
                cursor.execute("""
                    INSERT OR REPLACE INTO sys_etl_jobs (
                        job_id, status, start_time, end_time, total_files,
                        files_completed, files_failed, files_skipped,
                        total_records, records_inserted, records_updated, records_skipped,
                        error_message, username, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    job.job_id,
                    job.status.value,
                    job.start_time.isoformat() if job.start_time else None,
                    job.end_time.isoformat() if job.end_time else None,
                    job.total_files,
                    job.completed_files,
                    job.failed_files,
                    job.skipped_files,
                    total_records,
                    records_inserted,
                    records_updated,
                    records_skipped,
                    error_message,
                    username,
                    datetime.now().isoformat()
                ))
                
                # Save file results
                if job.file_results:
                    # Delete existing file results for this job
                    cursor.execute("DELETE FROM etl_job_files WHERE job_id = ?", (job.job_id,))
                    
                    # Insert file results
                    for file_result in job.file_results:
                        cursor.execute("""
                            INSERT INTO etl_job_files (
                                job_id, filename, table_name, status, record_count,
                                inserted, updated, skipped, error_message, processing_time_seconds
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            job.job_id,
                            file_result.file_path.name,  # Just the filename
                            file_result.table_name,
                            file_result.status.value,
                            file_result.records_loaded,
                            file_result.records_inserted,
                            file_result.records_updated,
                            file_result.records_skipped,
                            file_result.error_message,
                            file_result.processing_time_seconds
                        ))
                
                conn.commit()
                
        except Exception as e:
            self.logger.error(f"Error saving job {job.job_id} to database: {e}", exc_info=True)
    
    def _load_job_history_from_db(self, limit: int = 100):
        """Load job history from database on startup"""
        try:
            with self._get_internal_db_connection() as conn:
                cursor = conn.cursor()
                
                # Load jobs ordered by start time (most recent first)
                cursor.execute("""
                    SELECT job_id, status, start_time, end_time, total_files,
                           files_completed, files_failed, files_skipped,
                           total_records, records_inserted, records_updated, records_skipped,
                           error_message
                    FROM sys_etl_jobs
                    ORDER BY start_time DESC
                    LIMIT ?
                """, (limit,))
                
                jobs_loaded = 0
                for row in cursor.fetchall():
                    job_id = row[0]
                    
                    # Create job progress object
                    job = ETLJobProgress(job_id=job_id)
                    job.status = ETLJobStatus(row[1])
                    job.start_time = datetime.fromisoformat(row[2]) if row[2] else None
                    job.end_time = datetime.fromisoformat(row[3]) if row[3] else None
                    job.total_files = row[4] or 0
                    job.completed_files = row[5] or 0
                    job.failed_files = row[6] or 0
                    job.skipped_files = row[7] or 0
                    job.total_records_loaded = row[8] or 0
                    # Note: records_inserted/updated/skipped are calculated from file_results, not stored in job directly
                    if row[12]:  # error_message
                        job.error_messages = [row[12]]
                    
                    # Load file results
                    file_cursor = conn.cursor()
                    file_cursor.execute("""
                        SELECT filename, table_name, status, record_count,
                               inserted, updated, skipped, error_message, processing_time_seconds
                        FROM etl_job_files
                        WHERE job_id = ?
                        ORDER BY id
                    """, (job_id,))
                    
                    for file_row in file_cursor.fetchall():
                        # Reconstruct file path (just use filename since path might change)
                        file_result = FileProcessingTask(
                            file_path=Path(file_row[0]),  # filename
                            table_name=file_row[1],
                            file_date="",  # Not stored, not critical for history
                            file_hash="",  # Not stored, not critical for history
                            status=FileProcessingStatus(file_row[2]),
                            records_loaded=file_row[3] or 0,
                            records_inserted=file_row[4] or 0,
                            records_updated=file_row[5] or 0,
                            records_skipped=file_row[6] or 0,
                            error_message=file_row[7]
                        )
                        # Set processing time if available
                        if file_row[8]:
                            # Calculate synthetic start/end times based on processing time
                            file_result.start_time = job.start_time
                            if job.start_time:
                                from datetime import timedelta
                                file_result.end_time = job.start_time + timedelta(seconds=file_row[8])
                        
                        job.file_results.append(file_result)
                    
                    # Add to history (skip if running status - those are stale from crashed server)
                    if job.status != ETLJobStatus.RUNNING:
                        self._job_history[job_id] = job
                        jobs_loaded += 1
                    else:
                        # Mark stale running jobs as failed
                        job.status = ETLJobStatus.FAILED
                        job.error_message = "Server restarted during job execution"
                        if not job.end_time:
                            job.end_time = job.start_time
                        self._job_history[job_id] = job
                        self._save_job_to_db(job)  # Update the stale job
                        jobs_loaded += 1
                
                if jobs_loaded > 0:
                    self.logger.info(f"Loaded {jobs_loaded} job(s) from database history")
                
        except sqlite3.OperationalError as e:
            # Table might not exist yet on first run
            if "no such table" in str(e).lower():
                self.logger.info("ETL job history tables not yet created - will be initialized on first schema update")
            else:
                self.logger.error(f"Error loading job history from database: {e}", exc_info=True)
        except Exception as e:
            self.logger.error(f"Error loading job history from database: {e}", exc_info=True)
    
    def add_progress_callback(self, callback: Callable[[ETLJobProgress], None]):
        """Add a progress callback"""
        self._progress_callbacks.append(callback)
    
    def remove_progress_callback(self, callback: Callable[[ETLJobProgress], None]):
        """Remove a progress callback"""
        if callback in self._progress_callbacks:
            self._progress_callbacks.remove(callback)
    
    def _notify_progress(self, job_id: str):
        """Notify all progress callbacks for a specific job"""
        with self._job_lock:
            job = self._active_jobs.get(job_id) or self._job_history.get(job_id)
            
        if job:
            for callback in self._progress_callbacks:
                try:
                    callback(job)
                except Exception as e:
                    self.logger.error(
                        f"Error in progress callback for job {job_id}:\n"
                        f"  Exception Type: {type(e).__name__}\n"
                        f"  Message: {str(e)}",
                        exc_info=True
                    )
    
    def start_etl_job(self, force_reprocess: bool = False, latest_only: bool = False,
                     max_workers: int = None, selected_files: List[str] = None, username: str = "system",
                     trigger_type: str = "manual") -> str:
        """Start a new ETL job - supports multiple concurrent jobs
        
        Args:
            force_reprocess: Whether to reprocess files that have already been processed
            latest_only: Only process the latest files
            max_workers: Maximum number of worker threads
            selected_files: List of specific files to process
            username: User who triggered the job
            trigger_type: Type of trigger - 'manual' or 'automatic'
        """
        # Generate unique job ID with milliseconds for uniqueness
        job_id = f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:21]}"
        
        # Create job progress tracker
        job = ETLJobProgress(job_id=job_id)
        job.status = ETLJobStatus.RUNNING
        job.start_time = datetime.now()
        job.trigger_type = trigger_type
        job.triggered_by = username
        
        # Add to active jobs and history
        with self._job_lock:
            self._active_jobs[job_id] = job
            self._job_history[job_id] = job
            self._cancel_events[job_id] = threading.Event()
        
        self.logger.info(f"Starting ETL job {job_id} (active jobs: {len(self._active_jobs)}, trigger: {trigger_type})")
        
        # Log audit event - job started
        audit_logger = get_audit_logger()
        audit_details = f"Force reprocess: {force_reprocess}, Latest only: {latest_only}, Trigger: {trigger_type}"
        if selected_files:
            audit_details += f", Files: {len(selected_files)}"
        audit_logger.log(
            username=username,
            action=AuditAction.ETL_JOB_STARTED,
            category=AuditCategory.ETL,
            success=True,
            details=audit_details,
            target_resource=job_id
        )
        
        # Start job in background thread
        job_thread = threading.Thread(
            target=self._execute_etl_job,
            args=(job_id, force_reprocess, latest_only, max_workers or config.etl.max_workers, selected_files, username, trigger_type),
            daemon=True
        )
        job_thread.start()
        
        return job_id
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a specific job by ID"""
        with self._job_lock:
            if job_id not in self._active_jobs:
                return False
                
            job = self._active_jobs[job_id]
            if not job.is_running:
                return False
        
        self.logger.info(f"Cancelling ETL job {job_id}")
        
        # Signal cancellation
        if job_id in self._cancel_events:
            self._cancel_events[job_id].set()
        
        # Update job status
        with self._job_lock:
            job.status = ETLJobStatus.CANCELLED
            job.end_time = datetime.now()
        self._notify_progress(job_id)
        
        return True
    
    def cancel_current_job(self):
        """Cancel the first running job (for backward compatibility)"""
        with self._job_lock:
            active_jobs = list(self._active_jobs.keys())
        
        if active_jobs:
            return self.cancel_job(active_jobs[0])
        return False
    
    def get_job_status(self, job_id: str) -> Optional[ETLJobProgress]:
        """Get status of a specific job"""
        with self._job_lock:
            return self._active_jobs.get(job_id) or self._job_history.get(job_id)
    
    def get_current_job_status(self) -> Optional[ETLJobProgress]:
        """Get first active job status (for backward compatibility)"""
        with self._job_lock:
            if self._active_jobs:
                return list(self._active_jobs.values())[0]
        return None
    
    def get_active_jobs(self) -> List[ETLJobProgress]:
        """Get all currently active/running jobs"""
        with self._job_lock:
            return list(self._active_jobs.values())
    
    def get_job_history(self, limit: int = 10) -> List[ETLJobProgress]:
        """Get job execution history"""
        # Limit job history size to prevent memory buildup
        MAX_HISTORY_SIZE = 100
        
        # Clean old jobs if history is too large
        if len(self._job_history) > MAX_HISTORY_SIZE:
            jobs = list(self._job_history.items())
            jobs.sort(key=lambda x: x[1].start_time or datetime.min, reverse=True)
            # Keep only the most recent MAX_HISTORY_SIZE jobs
            self._job_history = dict(jobs[:MAX_HISTORY_SIZE])
            self.logger.info(f"Cleaned job history, kept {MAX_HISTORY_SIZE} most recent jobs")
        
        jobs = list(self._job_history.values())
        jobs.sort(key=lambda j: j.start_time or datetime.min, reverse=True)
        return jobs[:limit]
    
    def _execute_etl_job(self, job_id: str, force_reprocess: bool, latest_only: bool, max_workers: int, selected_files: List[str] = None, username: str = "system", trigger_type: str = "manual"):
        """Execute ETL job in background
        
        Args:
            job_id: Unique job identifier
            force_reprocess: Whether to reprocess already-processed files
            latest_only: Only process the latest files
            max_workers: Maximum number of worker threads
            selected_files: List of specific files to process
            username: User who triggered the job
            trigger_type: Type of trigger - 'manual' or 'automatic'
        """
        audit_logger = get_audit_logger()
        job_start_time = datetime.now()
        
        # Get the job object
        with self._job_lock:
            job = self._active_jobs.get(job_id)
        
        if not job:
            self.logger.error(f"Job {job_id} not found in active jobs")
            return
        
        # Get cancel event for this job
        cancel_event = self._cancel_events.get(job_id, threading.Event())
        
        try:
            # SFTP Download (if enabled and auto_download is on)
            if config.sftp.enabled and config.sftp.auto_download:
                try:
                    job.current_file = "Downloading files from SFTP..."
                    self._notify_progress(job_id)
                    
                    from .sftp_service import get_sftp_service
                    sftp_service = get_sftp_service()
                    
                    download_result = sftp_service.download_and_process(username=username)
                    
                    if download_result['successful_downloads'] > 0:
                        self.logger.info(
                            f"SFTP: Downloaded {download_result['successful_downloads']} "
                            f"of {download_result['total_files']} files before ETL"
                        )
                    
                    if download_result['failed_downloads'] > 0:
                        self.logger.warning(
                            f"SFTP: {download_result['failed_downloads']} files failed to download"
                        )
                
                except Exception as e:
                    self.logger.error(f"SFTP download error (continuing with ETL): {e}")
                    # Continue with ETL even if SFTP download fails
            
            # Discover files
            job.current_file = "Discovering files..."
            self._notify_progress(job_id)
            
            tasks = self.discovery_service.discover_files(
                config.directories.input_dir,
                force_reprocess,
                selected_files  # Pass selected files
            )
            
            # Filter for latest only if requested
            if latest_only:
                tasks = self._filter_latest_files(tasks)
            
            # Update job progress
            job.total_files = len(tasks)
            job.current_file = "Processing files..."
            self._notify_progress(job_id)
            
            if not tasks:
                job.status = ETLJobStatus.COMPLETED
                job.current_file = "No files to process"
                job.end_time = datetime.now()
                self._notify_progress(job_id)
                
                # Remove from active jobs
                with self._job_lock:
                    self._active_jobs.pop(job_id, None)
                
                # Log completion with no files
                audit_logger.log(
                    username=username,
                    action=AuditAction.ETL_JOB_COMPLETED,
                    category=AuditCategory.ETL,
                    success=True,
                    details="No files to process",
                    target_resource=job_id,
                    duration_ms=int((datetime.now() - job_start_time).total_seconds() * 1000)
                )
                return
            
            # Process files
            completed_tasks = self._process_files_parallel(job_id, tasks, max_workers, cancel_event, username, trigger_type)
            
            # Update final statistics
            self._update_job_statistics(job, completed_tasks)
            
            # Determine final status
            if cancel_event.is_set():
                job.status = ETLJobStatus.CANCELLED
            elif job.failed_files > 0:
                job.status = ETLJobStatus.FAILED
            else:
                job.status = ETLJobStatus.COMPLETED
            
            job.current_file = f"Completed ({job.status.value})"
            job.end_time = datetime.now()
            
            self.logger.info(f"ETL job {job_id} completed with status: {job.status.value}")
            
            # Log job completion
            duration_ms = int((job.end_time - job_start_time).total_seconds() * 1000)
            audit_details = (
                f"Processed {job.completed_files}/{job.total_files} files, "
                f"{job.total_records_loaded} records loaded, "
                f"{job.failed_files} failed, {job.skipped_files} skipped"
            )
            
            if job.status == ETLJobStatus.COMPLETED:
                audit_logger.log(
                    username=username,
                    action=AuditAction.ETL_JOB_COMPLETED,
                    category=AuditCategory.ETL,
                    success=True,
                    details=audit_details,
                    target_resource=job_id,
                    duration_ms=duration_ms,
                    record_count=job.total_records_loaded
                )
            elif job.status == ETLJobStatus.CANCELLED:
                audit_logger.log(
                    username=username,
                    action=AuditAction.ETL_JOB_CANCELLED,
                    category=AuditCategory.ETL,
                    success=False,
                    details=audit_details,
                    target_resource=job_id,
                    duration_ms=duration_ms
                )
            else:  # FAILED
                audit_logger.log(
                    username=username,
                    action=AuditAction.ETL_JOB_FAILED,
                    category=AuditCategory.ETL,
                    success=False,
                    details=audit_details,
                    target_resource=job_id,
                    error_message="; ".join(job.error_messages[:3]),  # First 3 errors
                    duration_ms=duration_ms
                )
            
        except Exception as e:
            self.logger.exception(f"ETL job {job_id} failed")
            job.status = ETLJobStatus.FAILED
            job.error_messages.append(str(e))
            job.end_time = datetime.now()
            
            # Log job failure
            duration_ms = int((job.end_time - job_start_time).total_seconds() * 1000)
            audit_logger.log(
                username=username,
                action=AuditAction.ETL_JOB_FAILED,
                category=AuditCategory.ETL,
                success=False,
                details=f"Job failed with exception",
                target_resource=job_id,
                error_message=str(e),
                duration_ms=duration_ms
            )
        
        finally:
            # Save job to database for persistence
            self._save_job_to_db(job, username)
            
            # Remove from active jobs
            with self._job_lock:
                self._active_jobs.pop(job_id, None)
                self._cancel_events.pop(job_id, None)
            
            self._notify_progress(job_id)
            
            # Job remains in _job_history for historical tracking
    
    def _filter_latest_files(self, tasks: List[FileProcessingTask]) -> List[FileProcessingTask]:
        """Keep only the latest file for each table"""
        table_latest = {}
        
        for task in tasks:
            if task.status == FileProcessingStatus.SKIPPED:
                continue
                
            if task.table_name not in table_latest:
                table_latest[task.table_name] = task
            elif task.file_date > table_latest[task.table_name].file_date:
                table_latest[task.table_name] = task
        
        return list(table_latest.values())
    
    def _process_files_parallel(self, job_id: str, tasks: List[FileProcessingTask], max_workers: int, cancel_event: threading.Event, username: str = "system", trigger_type: str = "manual") -> List[FileProcessingTask]:
        """Process files in parallel
        
        Args:
            job_id: Job identifier
            tasks: List of file processing tasks
            max_workers: Maximum number of worker threads
            cancel_event: Event to signal cancellation
            username: User who triggered the job
            trigger_type: Type of trigger - 'manual' or 'automatic'
        """
        completed_tasks = []
        
        # Get job object
        with self._job_lock:
            job = self._active_jobs.get(job_id)
        
        if not job:
            return tasks
        
        # Separate skipped tasks
        processable_tasks = [t for t in tasks if t.status != FileProcessingStatus.SKIPPED]
        skipped_tasks = [t for t in tasks if t.status == FileProcessingStatus.SKIPPED]
        
        # Update skipped count and add skipped files to results
        job.skipped_files = len(skipped_tasks)
        job.file_results.extend(skipped_tasks)  # Include skipped files in results
        self._notify_progress(job_id)
        
        if not processable_tasks:
            return tasks
        
        # Process files using thread pool
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks with trigger_type
            future_to_task = {
                executor.submit(self.processing_service.process_file, task, username, trigger_type): task
                for task in processable_tasks
            }
            
            # Process completed tasks
            for future in as_completed(future_to_task):
                if cancel_event.is_set():
                    # Cancel remaining futures
                    for f in future_to_task:
                        f.cancel()
                    break
                
                try:
                    completed_task = future.result()
                    completed_tasks.append(completed_task)
                    
                    # Store file result in job for API responses
                    job.file_results.append(completed_task)
                    
                    # Update progress
                    if completed_task.status == FileProcessingStatus.COMPLETED:
                        job.completed_files += 1
                        job.total_records_processed += completed_task.records_processed
                        job.total_records_loaded += completed_task.records_loaded
                        job.total_issues_found += completed_task.issues_found
                    elif completed_task.status == FileProcessingStatus.FAILED:
                        job.failed_files += 1
                        if completed_task.error_message:
                            job.error_messages.append(
                                f"{completed_task.file_path.name}: {completed_task.error_message}"
                            )
                    
                    job.current_file = f"Processed {completed_task.file_path.name}"
                    job.files_processed = job.completed_files + job.failed_files
                    self._notify_progress(job_id)
                    
                except Exception as e:
                    self.logger.error(
                        f"Error processing file in job {job_id}:\n"
                        f"  Exception Type: {type(e).__name__}\n"
                        f"  Message: {str(e)}",
                        exc_info=True
                    )
                    job.failed_files += 1
        
        # Add skipped tasks back
        completed_tasks.extend(skipped_tasks)
        
        return completed_tasks
    
    def _update_job_statistics(self, job: ETLJobProgress, tasks: List[FileProcessingTask]):
        """Update job statistics from completed tasks - mainly for final tallies"""
        # Most statistics are already updated in _process_files_parallel
        # This is just for any final adjustments
        pass


# Global service instance
etl_service = ETLOrchestrationService()


def get_etl_service() -> ETLOrchestrationService:
    """Get the global ETL orchestration service"""
    return etl_service