"""
Database Adapter Layer

Database abstraction layer supporting SQLite, MS SQL Server, PostgreSQL, and
MySQL. Provides unified interface for database operations across different
database systems with automatic connection management.

Author: Waqqas Hanafi
Copyright: © 2025 Calaveras County Health and Human Services Agency
"""

import sqlite3
import logging
import threading
from pathlib import Path
from typing import Optional, Any, Dict, List
from contextlib import contextmanager
from abc import ABC, abstractmethod
from datetime import datetime

try:
    import pyodbc
    MSSQL_AVAILABLE = True
except ImportError:
    MSSQL_AVAILABLE = False

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

try:
    import pymysql
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

from .config import config


class DatabaseAdapter(ABC):
    """Abstract base class for database adapters"""
    
    @abstractmethod
    @contextmanager
    def get_connection(self):
        """Get a database connection"""
        pass
    
    @abstractmethod
    def execute(self, query: str, params: Optional[tuple] = None) -> Any:
        """Execute a query"""
        pass
    
    @abstractmethod
    def execute_many(self, query: str, params_list: List[tuple]) -> None:
        """Execute a query with multiple parameter sets"""
        pass
    
    @abstractmethod
    def fetchall(self, query: str, params: Optional[tuple] = None) -> List[Any]:
        """Fetch all results"""
        pass
    
    @abstractmethod
    def fetchone(self, query: str, params: Optional[tuple] = None) -> Any:
        """Fetch one result"""
        pass
    
    @abstractmethod
    def commit(self) -> None:
        """Commit transaction"""
        pass
    
    @abstractmethod
    def rollback(self) -> None:
        """Rollback transaction"""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close connection"""
        pass
    
    @abstractmethod
    def normalize_sql(self, sql: str) -> str:
        """Normalize SQL syntax for the database type"""
        pass


class SQLiteAdapter(DatabaseAdapter):
    """SQLite database adapter"""
    
    def __init__(self, db_path: Path, timeout: float = 30.0):
        self.db_path = db_path
        self.timeout = timeout
        self.logger = logging.getLogger(self.__class__.__name__)
        db_path.parent.mkdir(parents=True, exist_ok=True)
    
    def _create_connection(self) -> sqlite3.Connection:
        """Create a new SQLite connection"""
        conn = sqlite3.connect(
            str(self.db_path),
            timeout=self.timeout,
            check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(f"PRAGMA journal_mode = {config.database.journal_mode}")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA cache_size = 10000")
        conn.execute("PRAGMA temp_store = MEMORY")
        return conn
    
    @contextmanager
    def get_connection(self):
        """Get a SQLite connection"""
        conn = self._create_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def execute(self, query: str, params: Optional[tuple] = None) -> sqlite3.Cursor:
        """Execute a query"""
        conn = self._create_connection()
        try:
            cursor = conn.execute(query, params or ())
            conn.commit()
            return cursor
        finally:
            conn.close()
    
    def execute_many(self, query: str, params_list: List[tuple]) -> None:
        """Execute a query with multiple parameter sets"""
        conn = self._create_connection()
        try:
            conn.executemany(query, params_list)
            conn.commit()
        finally:
            conn.close()
    
    def fetchall(self, query: str, params: Optional[tuple] = None) -> List[sqlite3.Row]:
        """Fetch all results"""
        conn = self._create_connection()
        try:
            cursor = conn.execute(query, params or ())
            return cursor.fetchall()
        finally:
            conn.close()
    
    def fetchone(self, query: str, params: Optional[tuple] = None) -> Optional[sqlite3.Row]:
        """Fetch one result"""
        conn = self._create_connection()
        try:
            cursor = conn.execute(query, params or ())
            return cursor.fetchone()
        finally:
            conn.close()
    
    def commit(self) -> None:
        """Commit transaction (no-op for SQLite in this context)"""
        pass
    
    def rollback(self) -> None:
        """Rollback transaction (no-op for SQLite in this context)"""
        pass
    
    def close(self) -> None:
        """Close connection (no-op for SQLite in this context)"""
        pass
    
    def normalize_sql(self, sql: str) -> str:
        """Normalize SQL syntax for SQLite"""
        # SQLite uses AUTOINCREMENT, INTEGER PRIMARY KEY
        # Replace MS SQL Server specific syntax
        sql = sql.replace("IDENTITY(1,1)", "AUTOINCREMENT")
        sql = sql.replace("NVARCHAR(MAX)", "TEXT")
        sql = sql.replace("DATETIME2", "TIMESTAMP")
        sql = sql.replace("BIT", "INTEGER")
        # Remove IF NOT EXISTS if present (SQLite supports it natively)
        return sql


class MSSQLAdapter(DatabaseAdapter):
    """MS SQL Server database adapter"""
    
    def __init__(self, server: str, database: str, username: str = "", 
                 password: str = "", trusted_connection: bool = True,
                 port: int = 1433, driver: str = "ODBC Driver 17 for SQL Server",
                 timeout: int = 30):
        if not MSSQL_AVAILABLE:
            raise ImportError("pyodbc is required for MS SQL Server support. Install with: pip install pyodbc")
        
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.trusted_connection = trusted_connection
        self.port = port
        self.driver = driver
        self.timeout = timeout
        self.logger = logging.getLogger(self.__class__.__name__)
        self._connection_string = self._build_connection_string()
    
    def _build_connection_string(self) -> str:
        """Build ODBC connection string (supports Azure SQL and regular MS SQL)"""
        # Azure SQL requires encryption and certificate validation
        # Check if server looks like Azure SQL (contains .database.windows.net)
        is_azure = '.database.windows.net' in self.server.lower()
        
        if self.trusted_connection and not is_azure:
            # Windows Authentication for on-premises MS SQL
            conn_str = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server},{self.port};"
                f"DATABASE={self.database};"
                f"Trusted_Connection=yes;"
                f"Connection Timeout={self.timeout};"
            )
        else:
            # SQL Authentication (required for Azure SQL, optional for on-premises)
            conn_str = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server},{self.port};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                f"Connection Timeout={self.timeout};"
            )
            
            # Azure SQL specific settings
            if is_azure:
                conn_str += (
                    f"Encrypt=yes;"
                    f"TrustServerCertificate=no;"
                )
        
        return conn_str
    
    def _create_connection(self) -> pyodbc.Connection:
        """Create a new MS SQL connection"""
        try:
            conn = pyodbc.connect(self._connection_string, timeout=self.timeout)
            return conn
        except pyodbc.Error as e:
            self.logger.error(f"Failed to connect to MS SQL Server: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """Get a MS SQL connection"""
        conn = self._create_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def execute(self, query: str, params: Optional[tuple] = None) -> pyodbc.Cursor:
        """Execute a query"""
        conn = self._create_connection()
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            return cursor
        finally:
            conn.close()
    
    def execute_many(self, query: str, params_list: List[tuple]) -> None:
        """Execute a query with multiple parameter sets"""
        conn = self._create_connection()
        try:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            conn.commit()
        finally:
            conn.close()
    
    def fetchall(self, query: str, params: Optional[tuple] = None) -> List[pyodbc.Row]:
        """Fetch all results"""
        conn = self._create_connection()
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()
        finally:
            conn.close()
    
    def fetchone(self, query: str, params: Optional[tuple] = None) -> Optional[pyodbc.Row]:
        """Fetch one result"""
        conn = self._create_connection()
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchone()
        finally:
            conn.close()
    
    def commit(self) -> None:
        """Commit transaction (no-op in this context)"""
        pass
    
    def rollback(self) -> None:
        """Rollback transaction (no-op in this context)"""
        pass
    
    def close(self) -> None:
        """Close connection (no-op in this context)"""
        pass
    
    def normalize_sql(self, sql: str) -> str:
        """Normalize SQL syntax for MS SQL Server"""
        # MS SQL Server uses IDENTITY(1,1), NVARCHAR(MAX), DATETIME2
        # Replace SQLite specific syntax
        sql = sql.replace("AUTOINCREMENT", "IDENTITY(1,1)")
        sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "INT IDENTITY(1,1) PRIMARY KEY")
        sql = sql.replace("TEXT", "NVARCHAR(MAX)")
        sql = sql.replace("TIMESTAMP", "DATETIME2")
        sql = sql.replace("INTEGER", "INT")
        # MS SQL Server doesn't support IF NOT EXISTS in CREATE TABLE
        # We'll handle this in the schema creation logic
        return sql


class PostgreSQLAdapter(DatabaseAdapter):
    """PostgreSQL database adapter"""
    
    def __init__(self, host: str, database: str, username: str = "", 
                 password: str = "", port: int = 5432, timeout: int = 30):
        if not POSTGRES_AVAILABLE:
            raise ImportError(
                "PostgreSQL support requires psycopg2. "
                "Install with: pip install psycopg2-binary"
            )
        
        self.host = host
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.timeout = timeout
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _create_connection(self) -> psycopg2.extensions.connection:
        """Create a new PostgreSQL connection"""
        try:
            conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.username,
                password=self.password,
                port=self.port,
                connect_timeout=self.timeout
            )
            return conn
        except psycopg2.Error as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """Get a PostgreSQL connection"""
        conn = self._create_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def execute(self, query: str, params: Optional[tuple] = None) -> psycopg2.extensions.cursor:
        """Execute a query"""
        conn = self._create_connection()
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            return cursor
        finally:
            conn.close()
    
    def execute_many(self, query: str, params_list: List[tuple]) -> None:
        """Execute a query with multiple parameter sets"""
        conn = self._create_connection()
        try:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            conn.commit()
        finally:
            conn.close()
    
    def fetchall(self, query: str, params: Optional[tuple] = None) -> List[Dict]:
        """Fetch all results"""
        conn = self._create_connection()
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()
    
    def fetchone(self, query: str, params: Optional[tuple] = None) -> Optional[Dict]:
        """Fetch one result"""
        conn = self._create_connection()
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            row = cursor.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
    
    def commit(self) -> None:
        """Commit transaction (no-op in this context)"""
        pass
    
    def rollback(self) -> None:
        """Rollback transaction (no-op in this context)"""
        pass
    
    def close(self) -> None:
        """Close connection (no-op in this context)"""
        pass
    
    def normalize_sql(self, sql: str) -> str:
        """Normalize SQL syntax for PostgreSQL"""
        # PostgreSQL uses SERIAL, VARCHAR, TIMESTAMP
        sql = sql.replace("AUTOINCREMENT", "SERIAL")
        sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
        sql = sql.replace("INTEGER PRIMARY KEY", "INT PRIMARY KEY")
        sql = sql.replace("TEXT", "VARCHAR")
        sql = sql.replace("TIMESTAMP", "TIMESTAMP")
        sql = sql.replace("INTEGER", "INT")
        sql = sql.replace("REAL", "DOUBLE PRECISION")
        sql = sql.replace("BLOB", "BYTEA")
        # PostgreSQL uses IF NOT EXISTS natively
        return sql


class MySQLAdapter(DatabaseAdapter):
    """MySQL database adapter"""
    
    def __init__(self, host: str, database: str, username: str = "", 
                 password: str = "", port: int = 3306, timeout: int = 30,
                 charset: str = "utf8mb4"):
        if not MYSQL_AVAILABLE:
            raise ImportError(
                "MySQL support requires pymysql. "
                "Install with: pip install pymysql"
            )
        
        self.host = host
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.timeout = timeout
        self.charset = charset
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _create_connection(self) -> pymysql.Connection:
        """Create a new MySQL connection"""
        try:
            conn = pymysql.connect(
                host=self.host,
                database=self.database,
                user=self.username,
                password=self.password,
                port=self.port,
                connect_timeout=self.timeout,
                charset=self.charset,
                cursorclass=pymysql.cursors.DictCursor
            )
            return conn
        except pymysql.Error as e:
            self.logger.error(f"Failed to connect to MySQL: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """Get a MySQL connection"""
        conn = self._create_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def execute(self, query: str, params: Optional[tuple] = None) -> pymysql.cursors.DictCursor:
        """Execute a query"""
        conn = self._create_connection()
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            return cursor
        finally:
            conn.close()
    
    def execute_many(self, query: str, params_list: List[tuple]) -> None:
        """Execute a query with multiple parameter sets"""
        conn = self._create_connection()
        try:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            conn.commit()
        finally:
            conn.close()
    
    def fetchall(self, query: str, params: Optional[tuple] = None) -> List[Dict]:
        """Fetch all results"""
        conn = self._create_connection()
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()
        finally:
            conn.close()
    
    def fetchone(self, query: str, params: Optional[tuple] = None) -> Optional[Dict]:
        """Fetch one result"""
        conn = self._create_connection()
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchone()
        finally:
            conn.close()
    
    def commit(self) -> None:
        """Commit transaction (no-op in this context)"""
        pass
    
    def rollback(self) -> None:
        """Rollback transaction (no-op in this context)"""
        pass
    
    def close(self) -> None:
        """Close connection (no-op in this context)"""
        pass
    
    def normalize_sql(self, sql: str) -> str:
        """Normalize SQL syntax for MySQL"""
        # MySQL uses AUTO_INCREMENT, VARCHAR, DATETIME
        sql = sql.replace("AUTOINCREMENT", "AUTO_INCREMENT")
        sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "INT AUTO_INCREMENT PRIMARY KEY")
        sql = sql.replace("INTEGER PRIMARY KEY", "INT PRIMARY KEY")
        sql = sql.replace("TEXT", "TEXT")
        sql = sql.replace("TIMESTAMP", "DATETIME")
        sql = sql.replace("INTEGER", "INT")
        sql = sql.replace("REAL", "DOUBLE")
        sql = sql.replace("BLOB", "BLOB")
        # MySQL supports IF NOT EXISTS natively
        return sql


def get_database_adapter() -> DatabaseAdapter:
    """Get the appropriate database adapter based on configuration"""
    db_config = config.database
    
    if db_config.db_type == "mssql" or db_config.db_type == "azuresql":
        if not MSSQL_AVAILABLE:
            raise ImportError(
                "MS SQL Server/Azure SQL support requires pyodbc. "
                "Install with: pip install pyodbc"
            )
        # Azure SQL uses same adapter as MS SQL, but with different connection string
        return MSSQLAdapter(
            server=db_config.mssql_server,
            database=db_config.mssql_database,
            username=db_config.mssql_username,
            password=db_config.mssql_password,
            trusted_connection=db_config.mssql_trusted_connection,
            port=db_config.mssql_port,
            driver=db_config.mssql_driver,
            timeout=db_config.connection_timeout
        )
    elif db_config.db_type == "postgresql":
        if not POSTGRES_AVAILABLE:
            raise ImportError(
                "PostgreSQL support requires psycopg2. "
                "Install with: pip install psycopg2-binary"
            )
        return PostgreSQLAdapter(
            host=db_config.postgresql_host,
            database=db_config.postgresql_database,
            username=db_config.postgresql_username,
            password=db_config.postgresql_password,
            port=db_config.postgresql_port,
            timeout=db_config.connection_timeout
        )
    elif db_config.db_type == "mysql":
        if not MYSQL_AVAILABLE:
            raise ImportError(
                "MySQL support requires pymysql. "
                "Install with: pip install pymysql"
            )
        return MySQLAdapter(
            host=db_config.mysql_host,
            database=db_config.mysql_database,
            username=db_config.mysql_username,
            password=db_config.mysql_password,
            port=db_config.mysql_port,
            timeout=db_config.connection_timeout
        )
    else:  # Default to SQLite
        return SQLiteAdapter(
            db_path=db_config.path,
            timeout=db_config.connection_timeout
        )

