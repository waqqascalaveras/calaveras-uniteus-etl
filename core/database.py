"""
Database Layer

Database layer implementing the Repository Pattern with connection pooling,
query builders, and proper separation of concerns. Manages schema creation,
data insertion, querying, and table management with thread-safe operations.

Author: Waqqas Hanafi
Copyright: © 2025 Calaveras County Health and Human Services Agency
"""

import sqlite3
import pandas as pd
import logging
import threading
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Tuple
from contextlib import contextmanager
from datetime import datetime
from dataclasses import dataclass, field
from abc import ABC, abstractmethod

from .config import config

# Register datetime adapter for Python 3.12+ compatibility
sqlite3.register_adapter(datetime, lambda dt: dt.isoformat())
sqlite3.register_converter("timestamp", lambda b: datetime.fromisoformat(b.decode()))

# Register pandas Timestamp adapter (pandas uses its own Timestamp type)
try:
    from pandas import Timestamp as pdTimestamp
    sqlite3.register_adapter(pdTimestamp, lambda ts: ts.isoformat())
except ImportError:
    pass  # pandas not installed


@dataclass
class QueryResult:
    """Standardized query result with metadata"""
    success: bool
    data: Optional[Union[List[Dict], pd.DataFrame]] = None
    row_count: int = 0
    columns: List[str] = field(default_factory=list)
    execution_time_ms: float = 0.0
    error_message: Optional[str] = None
    # Detailed stats for upsert operations
    inserted_count: int = 0
    updated_count: int = 0
    skipped_count: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses"""
        return {
            'success': self.success,
            'data': self.data,
            'row_count': self.row_count,
            'columns': self.columns,
            'execution_time_ms': self.execution_time_ms,
            'error_message': self.error_message,
            'inserted_count': self.inserted_count,
            'updated_count': self.updated_count,
            'skipped_count': self.skipped_count
        }


class DatabaseConnectionPool:
    """Thread-safe SQLite connection pool"""
    
    def __init__(self, db_path: Path, max_connections: int = 10, timeout: float = 30.0):
        self.db_path = db_path
        self.max_connections = max_connections
        self.timeout = timeout
        self._pool: List[sqlite3.Connection] = []
        self._pool_lock = threading.Lock()
        self._created_connections = 0
        self.logger = logging.getLogger(self.__class__.__name__)
        self._last_cleanup = datetime.now()
        
        # Ensure database directory exists
        db_path.parent.mkdir(parents=True, exist_ok=True)
    
    def _create_connection(self) -> sqlite3.Connection:
        """Create a new optimized database connection"""
        conn = sqlite3.connect(
            str(self.db_path),
            timeout=self.timeout,
            check_same_thread=False
        )
        
        # Configure SQLite for optimal performance
        conn.row_factory = sqlite3.Row  # Enable column access by name
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")  # Write-Ahead Logging
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA cache_size = 10000")
        conn.execute("PRAGMA temp_store = MEMORY")
        
        return conn
    
    @contextmanager
    def get_connection(self):
        """Get a connection from the pool"""
        conn = None
        try:
            with self._pool_lock:
                if self._pool:
                    conn = self._pool.pop()
                elif self._created_connections < self.max_connections:
                    conn = self._create_connection()
                    self._created_connections += 1
            
            if conn is None:
                # Pool exhausted, create temporary connection
                conn = self._create_connection()
                temp_connection = True
            else:
                temp_connection = False
            
            # Validate connection
            try:
                conn.execute("SELECT 1").fetchone()
            except sqlite3.Error:
                conn.close()
                conn = self._create_connection()
                temp_connection = True
            
            yield conn
            
        except Exception as e:
            self.logger.error(f"Database error: {e}")
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            raise
        finally:
            if conn:
                try:
                    if not temp_connection:
                        with self._pool_lock:
                            if len(self._pool) < self.max_connections:
                                self._pool.append(conn)
                            else:
                                conn.close()
                                self._created_connections -= 1
                    else:
                        conn.close()
                except Exception as e:
                    self.logger.error(f"Error managing connection: {e}")
    
    def close_all(self):
        """Close all connections in the pool"""
        with self._pool_lock:
            for conn in self._pool:
                try:
                    conn.close()
                except:
                    pass
            self._pool.clear()
            self._created_connections = 0
    
    def periodic_cleanup(self):
        """Periodic cleanup of idle connections to prevent memory buildup"""
        from datetime import timedelta
        
        # Run cleanup every 30 minutes
        if (datetime.now() - self._last_cleanup) > timedelta(minutes=30):
            with self._pool_lock:
                # Keep only half the max connections, close the rest
                target_size = self.max_connections // 2
                closed_count = 0
                while len(self._pool) > target_size:
                    conn = self._pool.pop()
                    try:
                        conn.close()
                        self._created_connections -= 1
                        closed_count += 1
                    except Exception as e:
                        self.logger.warning(f"Error closing connection during cleanup: {e}")
                
                if closed_count > 0:
                    self.logger.info(f"Periodic cleanup: closed {closed_count} idle connections, pool size: {len(self._pool)}/{self.max_connections}")
                self._last_cleanup = datetime.now()
    
    def get_pool_stats(self):
        """Get connection pool statistics for monitoring"""
        with self._pool_lock:
            return {
                'pool_size': len(self._pool),
                'created_connections': self._created_connections,
                'max_connections': self.max_connections,
                'available': len(self._pool),
                'in_use': self._created_connections - len(self._pool)
            }


class Repository(ABC):
    """Abstract repository base class"""
    
    def __init__(self, connection_pool: DatabaseConnectionPool, table_name: str):
        self.pool = connection_pool
        self.table_name = table_name
        self.logger = logging.getLogger(f"{self.__class__.__name__}[{table_name}]")
    
    @abstractmethod  
    def get_schema(self) -> List[str]:
        """Get expected column names for this table"""
        pass
    
    def execute_query(self, query: str, params: Optional[Tuple] = None, 
                     return_dataframe: bool = False) -> QueryResult:
        """Execute a query with standardized result handling"""
        import time
        
        start_time = time.time()
        
        try:
            with self.pool.get_connection() as conn:
                if return_dataframe:
                    df = pd.read_sql_query(query, conn, params=params)
                    execution_time = (time.time() - start_time) * 1000
                    
                    return QueryResult(
                        success=True,
                        data=df,
                        row_count=len(df),
                        columns=df.columns.tolist(),
                        execution_time_ms=execution_time
                    )
                else:
                    cursor = conn.cursor()
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)
                    
                    # Get column names
                    columns = [desc[0] for desc in cursor.description] if cursor.description else []
                    
                    # Fetch results
                    rows = cursor.fetchall()
                    data = [dict(zip(columns, row)) for row in rows] if rows else []
                    
                    conn.commit()
                    execution_time = (time.time() - start_time) * 1000
                    
                    return QueryResult(
                        success=True,
                        data=data,
                        row_count=len(data),
                        columns=columns,
                        execution_time_ms=execution_time
                    )
        
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            error_msg = f"Query failed: {str(e)}"
            self.logger.error(error_msg)
            
            return QueryResult(
                success=False,
                error_message=error_msg,
                execution_time_ms=execution_time
            )
    
    def count(self) -> int:
        """Get total record count"""
        result = self.execute_query(f"SELECT COUNT(*) as count FROM {self.table_name}")
        if result.success and result.data:
            return result.data[0]['count']
        return 0
    
    def exists(self) -> bool:
        """Check if table exists"""
        result = self.execute_query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (self.table_name,)
        )
        return result.success and len(result.data) > 0
    
    def get_all(self, limit: Optional[int] = None, offset: int = 0) -> QueryResult:
        """Get all records with optional pagination"""
        query = f"SELECT * FROM {self.table_name}"
        if limit:
            query += f" LIMIT {limit} OFFSET {offset}"
        
        return self.execute_query(query, return_dataframe=True)
    
    def get_by_id(self, record_id: Any, id_column: str = None) -> QueryResult:
        """Get a record by ID"""
        if not id_column:
            id_column = config.data_quality.primary_keys.get(self.table_name, 'id')
        
        query = f"SELECT * FROM {self.table_name} WHERE {id_column} = ?"
        return self.execute_query(query, (record_id,), return_dataframe=True)
    
    def search(self, search_term: str, columns: List[str] = None, limit: int = 100) -> QueryResult:
        """Search records by term in specified columns"""
        if not columns:
            columns = self.get_schema()
        
        # Build search conditions for text columns
        conditions = []
        params = []
        
        for column in columns:
            conditions.append(f"{column} LIKE ?")
            params.append(f"%{search_term}%")
        
        query = f"""
            SELECT * FROM {self.table_name} 
            WHERE {' OR '.join(conditions)}
            LIMIT {limit}
        """
        
        return self.execute_query(query, tuple(params), return_dataframe=True)
    
    def insert_dataframe(self, df: pd.DataFrame, if_exists: str = 'append') -> QueryResult:
        """Insert DataFrame with audit timestamps"""
        import time
        
        if df.empty:
            return QueryResult(
                success=True,
                row_count=0,
                execution_time_ms=0.0
            )
        
        start_time = time.time()
        
        try:
            # Add audit fields
            df = df.copy()
            df['etl_loaded_at'] = datetime.now()
            df['etl_updated_at'] = datetime.now()
            
            with self.pool.get_connection() as conn:
                df.to_sql(
                    self.table_name,
                    conn,
                    if_exists=if_exists,
                    index=False,
                    method='multi'  # Faster bulk insert
                )
                
                execution_time = (time.time() - start_time) * 1000
                
                return QueryResult(
                    success=True,
                    row_count=len(df),
                    execution_time_ms=execution_time
                )
        
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            error_msg = f"Insert failed: {str(e)}"
            self.logger.error(error_msg)
            
            return QueryResult(
                success=False,
                error_message=error_msg,
                execution_time_ms=execution_time
            )
    
    def upsert_dataframe(self, df: pd.DataFrame, primary_key: str) -> QueryResult:
        """Upsert DataFrame - insert new records or update existing ones based on primary key"""
        import time
        
        if df.empty:
            return QueryResult(
                success=True,
                row_count=0,
                execution_time_ms=0.0
            )
        
        start_time = time.time()
        
        try:
            # Add audit fields
            df = df.copy()
            now = datetime.now()
            
            # For new records
            if 'etl_loaded_at' not in df.columns:
                df['etl_loaded_at'] = now
            
            # Always update this field
            df['etl_updated_at'] = now
            
            with self.pool.get_connection() as conn:
                # Get existing primary keys
                existing_query = f"SELECT {primary_key} FROM {self.table_name}"
                try:
                    existing_keys = pd.read_sql(existing_query, conn)[primary_key].tolist()
                except:
                    existing_keys = []
                
                # Split into inserts and updates
                if existing_keys:
                    df_insert = df[~df[primary_key].isin(existing_keys)]
                    df_update = df[df[primary_key].isin(existing_keys)]
                else:
                    df_insert = df
                    df_update = pd.DataFrame()
                
                records_affected = 0
                
                # Insert new records
                if not df_insert.empty:
                    df_insert.to_sql(
                        self.table_name,
                        conn,
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                    records_affected += len(df_insert)
                    self.logger.info(f"Inserted {len(df_insert)} new records into {self.table_name}")
                
                # Update existing records
                if not df_update.empty:
                    for _, row in df_update.iterrows():
                        # Build UPDATE statement
                        set_clauses = []
                        params = []
                        for col in df_update.columns:
                            if col != primary_key:
                                set_clauses.append(f"{col} = ?")
                                params.append(row[col])
                        
                        params.append(row[primary_key])
                        
                        update_sql = f"""
                            UPDATE {self.table_name}
                            SET {', '.join(set_clauses)}
                            WHERE {primary_key} = ?
                        """
                        
                        conn.execute(update_sql, params)
                    
                    conn.commit()
                    records_affected += len(df_update)
                    self.logger.info(f"Updated {len(df_update)} existing records in {self.table_name}")
                
                execution_time = (time.time() - start_time) * 1000
                
                return QueryResult(
                    success=True,
                    row_count=records_affected,
                    inserted_count=len(df_insert) if not df_insert.empty else 0,
                    updated_count=len(df_update) if not df_update.empty else 0,
                    execution_time_ms=execution_time
                )
        
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            error_msg = f"Upsert failed: {str(e)}"
            self.logger.error(error_msg)
            
            return QueryResult(
                success=False,
                error_message=error_msg,
                execution_time_ms=execution_time
            )
    
    def delete_all(self) -> QueryResult:
        """Delete all records from table"""
        return self.execute_query(f"DELETE FROM {self.table_name}")


class TableRepository(Repository):
    """Generic repository for data tables"""
    
    def __init__(self, connection_pool: DatabaseConnectionPool, table_name: str):
        super().__init__(connection_pool, table_name)
        self._schema = config.data_quality.expected_tables.get(table_name, [])
    
    def get_schema(self) -> List[str]:
        """Get expected column names"""
        return self._schema


class ETLMetadataRepository(Repository):
    """Repository for ETL metadata tracking"""
    
    def __init__(self, connection_pool: DatabaseConnectionPool):
        super().__init__(connection_pool, 'etl_metadata')
    
    def get_schema(self) -> List[str]:
        return ['id', 'file_name', 'table_name', 'file_date', 'records_processed',
                'records_inserted', 'records_updated',
                'processing_started_at', 'processing_completed_at', 'status', 
                'error_message', 'file_hash', 'trigger_type', 'triggered_by']
    
    def log_processing_start(self, file_name: str, table_name: str, file_date: str, 
                           file_hash: str, trigger_type: str = 'manual', triggered_by: str = None) -> int:
        """Log the start of file processing"""
        result = self.execute_query("""
            INSERT OR REPLACE INTO etl_metadata 
            (file_name, table_name, file_date, records_processed, records_inserted, records_updated,
             processing_started_at, processing_completed_at, status, file_hash, trigger_type, triggered_by)
            VALUES (?, ?, ?, 0, 0, 0, ?, ?, 'processing', ?, ?, ?)
        """, (file_name, table_name, file_date, datetime.now(), datetime.now(), file_hash, trigger_type, triggered_by))
        
        if result.success:
            # Get the inserted ID
            id_result = self.execute_query("SELECT last_insert_rowid() as id")
            if id_result.success and id_result.data:
                return id_result.data[0]['id']
        
        return 0
    
    def log_processing_complete(self, metadata_id: int, records_processed: int,
                               status: str = 'success', error_message: str = None,
                               records_inserted: int = 0, records_updated: int = 0):
        """Log completion of file processing"""
        self.execute_query("""
            UPDATE etl_metadata
            SET records_processed = ?,
                records_inserted = ?,
                records_updated = ?,
                processing_completed_at = ?,
                status = ?,
                error_message = ?
            WHERE id = ?
        """, (records_processed, records_inserted, records_updated, datetime.now(), status, error_message, metadata_id))
    
    def get_processing_history(self, limit: int = 50) -> QueryResult:
        """Get recent processing history"""
        return self.execute_query("""
            SELECT * FROM etl_metadata 
            ORDER BY processing_started_at DESC 
            LIMIT ?
        """, (limit,), return_dataframe=True)
    
    def get_processed_files(self) -> List[Tuple[str, str]]:
        """Get list of already processed files"""
        result = self.execute_query("""
            SELECT file_name, file_hash FROM etl_metadata 
            WHERE status = 'success'
        """)
        
        if result.success:
            return [(row['file_name'], row['file_hash']) for row in result.data]
        return []


class DataQualityRepository(Repository):
    """Repository for data quality issues tracking"""
    
    def __init__(self, connection_pool: DatabaseConnectionPool):
        super().__init__(connection_pool, 'data_quality_issues')
    
    def get_schema(self) -> List[str]:
        return ['id', 'table_name', 'record_id', 'issue_type', 'issue_description',
                'field_name', 'original_value', 'corrected_value', 'detected_at', 'file_name']
    
    def log_issues(self, issues: List[Dict[str, Any]]):
        """Log data quality issues"""
        if not issues:
            return
        
        df = pd.DataFrame(issues)
        self.insert_dataframe(df)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get data quality summary"""
        # Get total issues
        total_result = self.execute_query("SELECT COUNT(*) as total FROM data_quality_issues")
        total_issues = total_result.data[0]['total'] if total_result.success else 0
        
        # Get issues by type
        type_result = self.execute_query("""
            SELECT issue_type, COUNT(*) as count 
            FROM data_quality_issues 
            GROUP BY issue_type 
            ORDER BY count DESC
        """)
        
        # Get issues by table
        table_result = self.execute_query("""
            SELECT table_name, COUNT(*) as count 
            FROM data_quality_issues 
            GROUP BY table_name 
            ORDER BY count DESC
        """)
        
        return {
            'total_issues': total_issues,
            'issues_by_type': {row['issue_type']: row['count'] for row in type_result.data} if type_result.success else {},
            'issues_by_table': {row['table_name']: row['count'] for row in table_result.data} if table_result.success else {},
            'last_updated': datetime.now().isoformat()
        }


class DatabaseManager:
    """
    Main database manager with repository pattern
    Provides high-level database operations and manages all repositories
    """
    
    def __init__(self, db_path: Path = None):
        self.db_path = db_path or config.database.path
        self.pool = DatabaseConnectionPool(
            self.db_path,
            max_connections=config.database.max_connections,
            timeout=config.database.connection_timeout
        )
        
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize repositories
        self.etl_metadata = ETLMetadataRepository(self.pool)
        self.data_quality = DataQualityRepository(self.pool)
        
        # Table repositories
        self._table_repos: Dict[str, TableRepository] = {}
        for table_name in config.data_quality.expected_tables.keys():
            self._table_repos[table_name] = TableRepository(self.pool, table_name)
        
        # Initialize database schema
        self.initialize_database()
        
        # Clean up any stuck processing records from previous runs
        self.cleanup_stuck_records()
    
    def cleanup_stuck_records(self):
        """Clean up any records stuck in 'processing' status from interrupted jobs"""
        try:
            with self.pool.get_connection() as conn:
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM etl_metadata WHERE status = 'processing'"
                )
                stuck_count = cursor.fetchone()[0]
                
                if stuck_count > 0:
                    self.logger.warning(f"Found {stuck_count} stuck processing records from interrupted jobs")
                    conn.execute("""
                        UPDATE etl_metadata 
                        SET status = 'failed', 
                            error_message = 'Processing interrupted - job was terminated before completion'
                        WHERE status = 'processing'
                    """)
                    conn.commit()
                    self.logger.info(f"Updated {stuck_count} stuck records to 'failed' status")
                    
        except Exception as e:
            self.logger.error(f"Failed to cleanup stuck records: {e}")
    
    def get_repository(self, table_name: str) -> TableRepository:
        """Get repository for a specific table"""
        if table_name not in self._table_repos:
            self._table_repos[table_name] = TableRepository(self.pool, table_name)
        return self._table_repos[table_name]
    
    def initialize_database(self):
        """Initialize database schema"""
        try:
            with self.pool.get_connection() as conn:
                # First, migrate existing databases: Add trigger_type and triggered_by columns if they don't exist
                # This must happen BEFORE running the schema SQL which includes indexes on these columns
                try:
                    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='etl_metadata'")
                    if cursor.fetchone():
                        # Table exists, check for columns
                        try:
                            conn.execute("SELECT trigger_type FROM etl_metadata LIMIT 1")
                        except sqlite3.OperationalError:
                            self.logger.info("Migrating etl_metadata: Adding trigger_type column")
                            conn.execute("ALTER TABLE etl_metadata ADD COLUMN trigger_type TEXT DEFAULT 'manual'")
                        
                        try:
                            conn.execute("SELECT triggered_by FROM etl_metadata LIMIT 1")
                        except sqlite3.OperationalError:
                            self.logger.info("Migrating etl_metadata: Adding triggered_by column")
                            conn.execute("ALTER TABLE etl_metadata ADD COLUMN triggered_by TEXT")
                        
                        conn.commit()
                except Exception as migrate_error:
                    self.logger.warning(f"Migration check failed (may be normal for new database): {migrate_error}")
                
                # Load and execute schema
                from .database_schema import get_schema_sql
                schema_sql = get_schema_sql()
                
                # Execute schema creation
                statement_count = 0
                for statement in schema_sql.split(';'):
                    statement = statement.strip()
                    if statement:
                        try:
                            conn.execute(statement)
                            statement_count += 1
                        except Exception as stmt_error:
                            # Ignore errors for indexes/tables that already exist
                            if "already exists" in str(stmt_error).lower():
                                self.logger.debug(f"Skipping existing object: {stmt_error}")
                                statement_count += 1
                            else:
                                self.logger.error(f"Error executing statement {statement_count + 1}: {stmt_error}")
                                self.logger.error(f"Statement: {statement[:200]}")
                                raise
                
                conn.commit()
                self.logger.info(f"Database schema initialized successfully ({statement_count} statements executed)")
                
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}", exc_info=True)
            raise
    
    def execute_query(self, query: str, params: Optional[Tuple] = None,
                     return_dataframe: bool = False) -> QueryResult:
        """Execute a raw SQL query"""
        # Use a temporary repository for query execution
        temp_repo = Repository.__new__(Repository)
        temp_repo.pool = self.pool
        temp_repo.table_name = "temp"
        temp_repo.logger = self.logger
        
        return temp_repo.execute_query(query, params, return_dataframe)
    
    def get_table_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get comprehensive statistics for all tables"""
        stats = {}
        
        # First, get all actual tables in the database
        try:
            with self.pool.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' 
                    AND name NOT LIKE 'sqlite_%'
                    AND name NOT IN ('etl_metadata', 'data_quality_issues')
                    ORDER BY name
                """)
                actual_tables = [row[0] for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Failed to get table list: {e}")
            actual_tables = []
        
        # Get stats for all actual tables
        for table_name in actual_tables:
            try:
                with self.pool.get_connection() as conn:
                    # Get record count
                    cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
                    record_count = cursor.fetchone()[0]
                    
                    # Get column info
                    cursor = conn.execute(f"PRAGMA table_info({table_name})")
                    columns = [row[1] for row in cursor.fetchall()]
                    
                    stats[table_name] = {
                        'record_count': record_count,
                        'exists': True,
                        'columns': columns,
                        'last_updated': datetime.now().isoformat()
                    }
                
            except Exception as e:
                self.logger.error(f"Error getting stats for {table_name}: {e}")
                stats[table_name] = {
                    'record_count': 0,
                    'exists': False,
                    'error': str(e)
                }
        
        return stats
    
    def get_database_info(self) -> Dict[str, Any]:
        """Get comprehensive database information"""
        try:
            file_size_mb = self.db_path.stat().st_size / (1024 * 1024) if self.db_path.exists() else 0
            
            # Get table stats
            table_stats = self.get_table_stats()
            
            # Calculate totals from table stats
            total_tables = len([t for t, s in table_stats.items() if s.get('exists', False)])
            total_records = sum(s.get('record_count', 0) for s in table_stats.values())
            
            # Get processing history summary
            history_result = self.etl_metadata.execute_query("""
                SELECT COUNT(*) as total_files, 
                       MAX(processing_completed_at) as last_processing
                FROM etl_metadata
                WHERE status = 'success'
            """)
            
            processing_info = {}
            if history_result.success and history_result.data:
                row = history_result.data[0]
                processing_info = {
                    'total_files_processed': row['total_files'],
                    'last_processing_date': row['last_processing']
                }
            
            return {
                'database_path': str(self.db_path),
                'database_size_mb': round(file_size_mb, 2),
                'total_tables': total_tables,
                'total_records': total_records,
                'table_stats': table_stats,
                **processing_info,
                'connection_pool_size': len(self.pool._pool),
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get database info: {e}")
            return {
                'database_path': str(self.db_path),
                'error': str(e)
            }
    
    def backup_database(self, backup_path: Path = None) -> Path:
        """Create database backup"""
        if not backup_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = config.directories.backup_dir / f"backup_{timestamp}.db"
        
        try:
            import shutil
            shutil.copy2(self.db_path, backup_path)
            self.logger.info(f"Database backed up to {backup_path}")
            return backup_path
            
        except Exception as e:
            self.logger.error(f"Backup failed: {e}")
            raise
    
    def migrate_automated_sync_for_other_databases(self, conn, db_type: str):
        """Create automated_sync_config table for non-SQLite databases"""
        try:
            if db_type in ['mssql', 'azuresql']:
                # MS SQL Server syntax
                conn.execute("""
                    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'automated_sync_config')
                    BEGIN
                        CREATE TABLE automated_sync_config (
                            id INT PRIMARY KEY CHECK (id = 1),
                            enabled INT DEFAULT 0,
                            interval_minutes INT DEFAULT 60,
                            last_run DATETIME2,
                            next_run DATETIME2,
                            updated_at DATETIME2 DEFAULT GETDATE(),
                            updated_by NVARCHAR(MAX)
                        )
                    END
                """)
                # Insert default config
                conn.execute("""
                    IF NOT EXISTS (SELECT * FROM automated_sync_config WHERE id = 1)
                    BEGIN
                        INSERT INTO automated_sync_config (id, enabled, interval_minutes)
                        VALUES (1, 0, 60)
                    END
                """)
            elif db_type == 'postgresql':
                # PostgreSQL syntax
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS automated_sync_config (
                        id INT PRIMARY KEY CHECK (id = 1),
                        enabled INT DEFAULT 0,
                        interval_minutes INT DEFAULT 60,
                        last_run TIMESTAMP,
                        next_run TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_by VARCHAR
                    )
                """)
                conn.execute("""
                    INSERT INTO automated_sync_config (id, enabled, interval_minutes)
                    VALUES (1, 0, 60)
                    ON CONFLICT (id) DO NOTHING
                """)
            elif db_type == 'mysql':
                # MySQL syntax
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS automated_sync_config (
                        id INT PRIMARY KEY CHECK (id = 1),
                        enabled INT DEFAULT 0,
                        interval_minutes INT DEFAULT 60,
                        last_run DATETIME,
                        next_run DATETIME,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_by TEXT
                    )
                """)
                conn.execute("""
                    INSERT IGNORE INTO automated_sync_config (id, enabled, interval_minutes)
                    VALUES (1, 0, 60)
                """)
            conn.commit()
            self.logger.info(f"Automated sync config table created for {db_type}")
        except Exception as e:
            self.logger.warning(f"Could not create automated sync config table for {db_type}: {e}")
    
    def close(self):
        """Close all database connections and cleanup resources"""
        try:
            self.pool.close_all()
            self.logger.info("Database manager closed - all connections released")
        except Exception as e:
            self.logger.error(f"Error closing database manager: {e}", exc_info=True)


# Global database manager instance
db_manager = DatabaseManager()

# Note: All table schemas are now centralized in database_schema.py following DRY principles
# This includes: automated_sync_config, sftp_cache, etl_metadata, and all data tables


def get_database_manager() -> DatabaseManager:
    """Get the global database manager instance"""
    return db_manager