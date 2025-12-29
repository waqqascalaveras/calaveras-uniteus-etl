"""
Schema Validation Service

Validates database schema against imported file structure and detects
mismatches. Logs critical errors and provides SQL commands to fix issues.

Author: Waqqas Hanafi
Copyright: © 2025 Calaveras County Health and Human Services Agency
"""

import logging
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

from .config import config
from .database import get_database_manager
from .database_schema import get_schema_sql
from .database_adapter import get_database_adapter
import re


@dataclass
class SchemaError:
    """Schema validation error"""
    error_type: str  # 'missing_table', 'missing_column', 'column_type_mismatch', 'extra_column'
    table_name: str
    file_name: str
    error_message: str
    error_details: Dict[str, Any]
    suggested_sql: Optional[str] = None
    severity: str = 'critical'


class SchemaValidator:
    """Validates database schema against file structure"""
    
    def __init__(self, internal_db_path: Optional[Path] = None):
        self.logger = logging.getLogger(self.__class__.__name__)
        # Use config for internal database path, with fallback for backward compatibility
        if internal_db_path:
            self._internal_db_path = internal_db_path
        else:
            self._internal_db_path = config.directories.database_dir / "internal.db"
        self._table_schemas_cache: Dict[str, List[str]] = {}
    
    def validate_table_exists(self, table_name: str) -> Tuple[bool, Optional[str]]:
        """Check if table exists in database"""
        try:
            adapter = get_database_adapter()
            with adapter.get_connection() as conn:
                # Check based on database type
                if config.database.db_type == 'sqlite':
                    cursor = conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                        (table_name,)
                    )
                    exists = cursor.fetchone() is not None
                elif config.database.db_type in ['mssql', 'azuresql']:
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?",
                        (table_name,)
                    )
                    exists = cursor.fetchone()[0] > 0
                elif config.database.db_type == 'postgresql':
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = %s",
                        (table_name,)
                    )
                    exists = cursor.fetchone()[0] > 0
                elif config.database.db_type == 'mysql':
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = %s",
                        (table_name,)
                    )
                    exists = cursor.fetchone()[0] > 0
                else:
                    return False, "Unknown database type"
                
                if not exists:
                    return False, f"Table '{table_name}' does not exist in database"
                return True, None
            
        except Exception as e:
            self.logger.error(f"Error checking table existence: {e}", exc_info=True)
            return False, str(e)
    
    def get_table_columns(self, table_name: str) -> List[str]:
        """Get list of columns for a table"""
        if table_name in self._table_schemas_cache:
            return self._table_schemas_cache[table_name]
        
        try:
            adapter = get_database_adapter()
            columns = []
            
            with adapter.get_connection() as conn:
                if config.database.db_type == 'sqlite':
                    cursor = conn.execute(f"PRAGMA table_info({table_name})")
                    columns = [row[1] for row in cursor.fetchall()]
                elif config.database.db_type in ['mssql', 'azuresql']:
                    cursor = conn.cursor()
                    cursor.execute(f"""
                        SELECT COLUMN_NAME 
                        FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_NAME = '{table_name}'
                        ORDER BY ORDINAL_POSITION
                    """)
                    columns = [row[0] for row in cursor.fetchall()]
                elif config.database.db_type == 'postgresql':
                    cursor = conn.cursor()
                    cursor.execute(f"""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = '{table_name}'
                        ORDER BY ordinal_position
                    """)
                    columns = [row[0] for row in cursor.fetchall()]
                elif config.database.db_type == 'mysql':
                    cursor = conn.cursor()
                    cursor.execute(f"""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = '{table_name}'
                        ORDER BY ordinal_position
                    """)
                    columns = [row[0] for row in cursor.fetchall()]
            
            self._table_schemas_cache[table_name] = columns
            return columns
            
        except Exception as e:
            self.logger.error(f"Error getting table columns: {e}", exc_info=True)
            return []
    
    def validate_schema(self, table_name: str, file_columns: List[str], 
                       file_name: str) -> List[SchemaError]:
        """Validate schema and return list of errors"""
        errors = []
        
        # Check if table exists
        table_exists, error_msg = self.validate_table_exists(table_name)
        if not table_exists:
            errors.append(SchemaError(
                error_type='missing_table',
                table_name=table_name,
                file_name=file_name,
                error_message=error_msg or f"Table '{table_name}' does not exist",
                error_details={
                    'file_columns': file_columns,
                    'expected_table': table_name
                },
                suggested_sql=self._generate_create_table_sql(table_name, file_columns),
                severity='critical'
            ))
            return errors  # Can't validate columns if table doesn't exist
        
        # Get expected columns from config
        expected_columns = set(config.data_quality.expected_tables.get(table_name, []))
        
        # Get actual table columns
        actual_columns = set(self.get_table_columns(table_name))
        
        # Check for missing columns in table
        missing_in_table = set(file_columns) - actual_columns
        if missing_in_table:
            errors.append(SchemaError(
                error_type='missing_column',
                table_name=table_name,
                file_name=file_name,
                error_message=f"Table '{table_name}' is missing {len(missing_in_table)} column(s): {', '.join(sorted(missing_in_table))}",
                error_details={
                    'missing_columns': sorted(missing_in_table),
                    'file_columns': file_columns,
                    'table_columns': sorted(actual_columns)
                },
                suggested_sql=self._generate_alter_table_sql(table_name, sorted(missing_in_table)),
                severity='critical'
            ))
        
        # Check for extra columns in file (informational, not critical)
        extra_in_file = set(file_columns) - expected_columns - actual_columns
        if extra_in_file and expected_columns:  # Only warn if we have expected columns defined
            errors.append(SchemaError(
                error_type='extra_column',
                table_name=table_name,
                file_name=file_name,
                error_message=f"File contains {len(extra_in_file)} unexpected column(s): {', '.join(sorted(extra_in_file))}",
                error_details={
                    'extra_columns': sorted(extra_in_file),
                    'file_columns': file_columns,
                    'expected_columns': sorted(expected_columns)
                },
                severity='warning'
            ))
        
        return errors
    
    def _generate_create_table_sql(self, table_name: str, columns: List[str]) -> str:
        """Generate CREATE TABLE SQL based on file columns"""
        # Get base schema for this table if available
        base_schema = get_schema_sql()
        
        # Try to extract table definition from base schema
        table_match = re.search(
            rf'CREATE TABLE IF NOT EXISTS {table_name}\s*\((.*?)\);',
            base_schema,
            re.IGNORECASE | re.DOTALL
        )
        
        if table_match:
            # Use the actual schema definition
            table_def = table_match.group(1)
            # Normalize for current database type
            if config.database.db_type in ['mssql', 'azuresql']:
                table_def = table_def.replace('TEXT', 'NVARCHAR(MAX)')
                table_def = table_def.replace('INTEGER', 'INT')
                table_def = table_def.replace('REAL', 'FLOAT')
                table_def = table_def.replace('TIMESTAMP', 'DATETIME2')
                return f"CREATE TABLE {table_name} (\n{table_def}\n);"
            elif config.database.db_type == 'postgresql':
                table_def = table_def.replace('TEXT', 'VARCHAR')
                table_def = table_def.replace('INTEGER', 'INT')
                table_def = table_def.replace('REAL', 'DOUBLE PRECISION')
                return f"CREATE TABLE {table_name} (\n{table_def}\n);"
            elif config.database.db_type == 'mysql':
                table_def = table_def.replace('TEXT', 'TEXT')
                table_def = table_def.replace('INTEGER', 'INT')
                table_def = table_def.replace('REAL', 'DOUBLE')
                table_def = table_def.replace('TIMESTAMP', 'DATETIME')
                return f"CREATE TABLE {table_name} (\n{table_def}\n);"
            else:  # SQLite
                return f"CREATE TABLE IF NOT EXISTS {table_name} (\n{table_def}\n);"
        
        # Fallback: generate basic CREATE TABLE
        col_defs = []
        for col in columns:
            # Try to infer type from column name
            if 'id' in col.lower() and col.lower().endswith('_id'):
                col_type = 'TEXT PRIMARY KEY' if col == config.data_quality.primary_keys.get(table_name, '') else 'TEXT'
            elif 'date' in col.lower() or 'timestamp' in col.lower() or 'created_at' in col.lower() or 'updated_at' in col.lower():
                col_type = 'TIMESTAMP'
            elif 'count' in col.lower() or 'size' in col.lower() or 'number' in col.lower():
                col_type = 'INTEGER'
            elif 'amount' in col.lower() or 'price' in col.lower() or 'income' in col.lower():
                col_type = 'REAL'
            else:
                col_type = 'TEXT'
            
            col_defs.append(f"    {col} {col_type}")
        
        if config.database.db_type in ['mssql', 'azuresql']:
            sql = f"CREATE TABLE {table_name} (\n" + ",\n".join(col_defs) + "\n);"
            sql = sql.replace('TEXT', 'NVARCHAR(MAX)')
            sql = sql.replace('INTEGER', 'INT')
            sql = sql.replace('REAL', 'FLOAT')
            sql = sql.replace('TIMESTAMP', 'DATETIME2')
        elif config.database.db_type == 'postgresql':
            sql = f"CREATE TABLE {table_name} (\n" + ",\n".join(col_defs) + "\n);"
            sql = sql.replace('TEXT', 'VARCHAR')
            sql = sql.replace('INTEGER', 'INT')
            sql = sql.replace('REAL', 'DOUBLE PRECISION')
        elif config.database.db_type == 'mysql':
            sql = f"CREATE TABLE {table_name} (\n" + ",\n".join(col_defs) + "\n);"
            sql = sql.replace('TEXT', 'TEXT')
            sql = sql.replace('INTEGER', 'INT')
            sql = sql.replace('REAL', 'DOUBLE')
            sql = sql.replace('TIMESTAMP', 'DATETIME')
        else:  # SQLite
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n" + ",\n".join(col_defs) + "\n);"
        
        return sql
    
    def _generate_alter_table_sql(self, table_name: str, missing_columns: List[str]) -> str:
        """Generate ALTER TABLE SQL for missing columns"""
        alter_statements = []
        
        for col in missing_columns:
            # Infer type from column name
            if 'date' in col.lower() or 'timestamp' in col.lower() or 'created_at' in col.lower() or 'updated_at' in col.lower():
                col_type = 'TIMESTAMP'
            elif 'count' in col.lower() or 'size' in col.lower() or 'number' in col.lower():
                col_type = 'INTEGER'
            elif 'amount' in col.lower() or 'price' in col.lower() or 'income' in col.lower():
                col_type = 'REAL'
            else:
                col_type = 'TEXT'
            
            if config.database.db_type in ['mssql', 'azuresql']:
                col_type = col_type.replace('TEXT', 'NVARCHAR(MAX)')
                col_type = col_type.replace('INTEGER', 'INT')
                col_type = col_type.replace('REAL', 'FLOAT')
                col_type = col_type.replace('TIMESTAMP', 'DATETIME2')
                alter_statements.append(f"ALTER TABLE {table_name} ADD {col} {col_type};")
            elif config.database.db_type == 'postgresql':
                col_type = col_type.replace('TEXT', 'VARCHAR')
                col_type = col_type.replace('INTEGER', 'INT')
                col_type = col_type.replace('REAL', 'DOUBLE PRECISION')
                alter_statements.append(f"ALTER TABLE {table_name} ADD COLUMN {col} {col_type};")
            elif config.database.db_type == 'mysql':
                col_type = col_type.replace('TEXT', 'TEXT')
                col_type = col_type.replace('INTEGER', 'INT')
                col_type = col_type.replace('REAL', 'DOUBLE')
                col_type = col_type.replace('TIMESTAMP', 'DATETIME')
                alter_statements.append(f"ALTER TABLE {table_name} ADD COLUMN {col} {col_type};")
            else:  # SQLite
                alter_statements.append(f"ALTER TABLE {table_name} ADD COLUMN {col} {col_type};")
        
        return "\n".join(alter_statements)
    
    def log_schema_error(self, error: SchemaError, username: str = "system") -> int:
        """Log schema error to database"""
        try:
            with sqlite3.connect(self._internal_db_path) as conn:
                cursor = conn.execute("""
                    INSERT INTO schema_errors 
                    (error_type, table_name, file_name, error_message, detected_at, 
                     error_details, suggested_sql, severity)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    error.error_type,
                    error.table_name,
                    error.file_name,
                    error.error_message,
                    datetime.now().isoformat(),
                    str(error.error_details),
                    error.suggested_sql,
                    error.severity
                ))
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            self.logger.error(f"Error logging schema error: {e}", exc_info=True)
            return 0
    
    def get_recent_errors(self, limit: int = 50, resolved_only: bool = False) -> List[Dict[str, Any]]:
        """Get recent schema errors
        
        Args:
            limit: Maximum number of errors to return
            resolved_only: If True, return only resolved errors. If False, return only unresolved errors.
        """
        try:
            with sqlite3.connect(self._internal_db_path) as conn:
                conn.row_factory = sqlite3.Row
                query = """
                    SELECT * FROM schema_errors
                    WHERE resolved_at IS NULL
                    ORDER BY detected_at DESC
                    LIMIT ?
                """
                if resolved_only:
                    query = """
                        SELECT * FROM schema_errors
                        WHERE resolved_at IS NOT NULL
                        ORDER BY resolved_at DESC
                        LIMIT ?
                    """
                
                cursor = conn.execute(query, (limit,))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error getting schema errors: {e}", exc_info=True)
            return []
    
    def get_all_errors(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get all schema errors (both resolved and unresolved)"""
        try:
            with sqlite3.connect(self._internal_db_path) as conn:
                conn.row_factory = sqlite3.Row
                query = """
                    SELECT * FROM schema_errors
                    ORDER BY detected_at DESC
                    LIMIT ?
                """
                cursor = conn.execute(query, (limit,))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error getting all schema errors: {e}", exc_info=True)
            return []
    
    def mark_error_resolved(self, error_id: int, username: str):
        """Mark schema error as resolved"""
        try:
            with sqlite3.connect(self._internal_db_path) as conn:
                conn.execute("""
                    UPDATE schema_errors
                    SET resolved_at = ?, resolved_by = ?
                    WHERE id = ?
                """, (datetime.now().isoformat(), username, error_id))
                conn.commit()
                return True
        except Exception as e:
            self.logger.error(f"Error marking error as resolved: {e}", exc_info=True)
            return False


# Global validator instance
_validator: Optional[SchemaValidator] = None


def get_schema_validator() -> SchemaValidator:
    """Get global schema validator instance"""
    global _validator
    if _validator is None:
        _validator = SchemaValidator()
    return _validator

