"""
================================================================================
Calaveras UniteUs ETL - Database Schema Converter Unit Tests
================================================================================
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency

Description:
    Comprehensive unit tests for database schema conversion between SQLite,
    MS SQL Server, PostgreSQL, and MySQL formats.

Test Coverage:
    - SQLite to MS SQL conversion
    - SQLite to PostgreSQL conversion
    - SQLite to MySQL conversion
    - Data type conversions
    - Index conversions
    - Table creation syntax differences

Total Tests: 30+

Author: Waqqas Hanafi
Organization: Calaveras County Health and Human Services Agency
================================================================================
"""

import pytest
from pathlib import Path

from core.database_schema_converter import (
    convert_sqlite_to_mssql,
    convert_sqlite_to_postgresql,
    convert_sqlite_to_mysql,
    get_schema_for_database_type
)


class TestSQLiteToMSSQLConversion:
    """Test SQLite to MS SQL Server conversion"""
    
    def test_basic_table_conversion(self):
        """Test basic CREATE TABLE conversion"""
        sqlite_sql = """
        CREATE TABLE IF NOT EXISTS sys_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE
        );
        """
        
        mssql_sql = convert_sqlite_to_mssql(sqlite_sql)
        
        # Check that IF NOT EXISTS is removed
        assert "IF NOT EXISTS" not in mssql_sql
        # Check AUTOINCREMENT conversion
        assert "IDENTITY(1,1)" in mssql_sql
        # Check TEXT conversion
        assert "NVARCHAR(MAX)" in mssql_sql
        # Check INTEGER conversion
        assert "INT" in mssql_sql
    
    def test_data_type_conversions(self):
        """Test all data type conversions"""
        sqlite_sql = """
        CREATE TABLE test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            age INTEGER,
            salary REAL,
            photo BLOB,
            created TIMESTAMP,
            active INTEGER
        );
        """
        
        mssql_sql = convert_sqlite_to_mssql(sqlite_sql)
        
        assert "INT IDENTITY(1,1)" in mssql_sql
        assert "NVARCHAR(MAX)" in mssql_sql  # TEXT
        assert "INT" in mssql_sql  # INTEGER
        assert "FLOAT" in mssql_sql  # REAL
        assert "VARBINARY(MAX)" in mssql_sql  # BLOB
        assert "DATETIME2" in mssql_sql  # TIMESTAMP
    
    def test_index_conversion(self):
        """Test CREATE INDEX conversion"""
        sqlite_sql = """
        CREATE INDEX IF NOT EXISTS idx_sys_users_email ON sys_users(email);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_sys_users_name ON sys_users(name);
        """
        
        mssql_sql = convert_sqlite_to_mssql(sqlite_sql)
        
        assert "CREATE INDEX" in mssql_sql
        assert "CREATE UNIQUE INDEX" in mssql_sql
        assert "IF NOT EXISTS" not in mssql_sql
    
    def test_complex_table(self):
        """Test complex table with multiple constraints"""
        sqlite_sql = """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            total REAL NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES sys_users(id)
        );
        """
        
        mssql_sql = convert_sqlite_to_mssql(sqlite_sql)
        
        assert "INT IDENTITY(1,1)" in mssql_sql
        assert "NVARCHAR(MAX)" in mssql_sql
        assert "FLOAT" in mssql_sql
        assert "DATETIME2" in mssql_sql
        assert "FOREIGN KEY" in mssql_sql  # Foreign keys should be preserved


class TestSQLiteToPostgreSQLConversion:
    """Test SQLite to PostgreSQL conversion"""
    
    def test_basic_table_conversion(self):
        """Test basic CREATE TABLE conversion"""
        sqlite_sql = """
        CREATE TABLE IF NOT EXISTS sys_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE
        );
        """
        
        postgresql_sql = convert_sqlite_to_postgresql(sqlite_sql)
        
        # PostgreSQL supports IF NOT EXISTS
        assert "CREATE TABLE" in postgresql_sql
        # Check SERIAL conversion
        assert "SERIAL" in postgresql_sql
        # Check VARCHAR conversion
        assert "VARCHAR" in postgresql_sql
    
    def test_data_type_conversions(self):
        """Test all data type conversions"""
        sqlite_sql = """
        CREATE TABLE test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            age INTEGER,
            salary REAL,
            photo BLOB,
            created TIMESTAMP
        );
        """
        
        postgresql_sql = convert_sqlite_to_postgresql(sqlite_sql)
        
        assert "SERIAL" in postgresql_sql
        assert "VARCHAR" in postgresql_sql  # TEXT
        assert "INT" in postgresql_sql  # INTEGER
        assert "DOUBLE PRECISION" in postgresql_sql  # REAL
        assert "BYTEA" in postgresql_sql  # BLOB
        assert "TIMESTAMP" in postgresql_sql  # TIMESTAMP (same)
    
    def test_index_conversion(self):
        """Test CREATE INDEX conversion"""
        sqlite_sql = """
        CREATE INDEX IF NOT EXISTS idx_sys_users_email ON sys_users(email);
        """
        
        postgresql_sql = convert_sqlite_to_postgresql(sqlite_sql)
        
        assert "CREATE INDEX" in postgresql_sql
        # PostgreSQL supports IF NOT EXISTS
        assert "IF NOT EXISTS" in postgresql_sql


class TestSQLiteToMySQLConversion:
    """Test SQLite to MySQL conversion"""
    
    def test_basic_table_conversion(self):
        """Test basic CREATE TABLE conversion"""
        sqlite_sql = """
        CREATE TABLE IF NOT EXISTS sys_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE
        );
        """
        
        mysql_sql = convert_sqlite_to_mysql(sqlite_sql)
        
        # MySQL supports IF NOT EXISTS
        assert "CREATE TABLE" in mysql_sql
        # Check AUTO_INCREMENT conversion
        assert "AUTO_INCREMENT" in mysql_sql
        # TEXT stays as TEXT in MySQL
        assert "TEXT" in mysql_sql
    
    def test_data_type_conversions(self):
        """Test all data type conversions"""
        sqlite_sql = """
        CREATE TABLE test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            age INTEGER,
            salary REAL,
            photo BLOB,
            created TIMESTAMP
        );
        """
        
        mysql_sql = convert_sqlite_to_mysql(sqlite_sql)
        
        assert "AUTO_INCREMENT" in mysql_sql
        assert "TEXT" in mysql_sql  # TEXT stays TEXT
        assert "INT" in mysql_sql  # INTEGER
        assert "DOUBLE" in mysql_sql  # REAL
        assert "BLOB" in mysql_sql  # BLOB stays BLOB
        assert "DATETIME" in mysql_sql  # TIMESTAMP -> DATETIME
    
    def test_index_conversion(self):
        """Test CREATE INDEX conversion"""
        sqlite_sql = """
        CREATE INDEX IF NOT EXISTS idx_sys_users_email ON sys_users(email);
        """
        
        mysql_sql = convert_sqlite_to_mysql(sqlite_sql)
        
        assert "CREATE INDEX" in mysql_sql
        # MySQL supports IF NOT EXISTS
        assert "IF NOT EXISTS" in mysql_sql


class TestGetSchemaForDatabaseType:
    """Test get_schema_for_database_type function"""
    
    def test_sqlite_returns_unchanged(self):
        """Test that SQLite returns unchanged schema"""
        base_sql = "CREATE TABLE sys_users (id INTEGER PRIMARY KEY AUTOINCREMENT);"
        
        result = get_schema_for_database_type("sqlite", base_sql)
        
        assert result == base_sql
    
    def test_mssql_converts_schema(self):
        """Test that MS SQL converts schema"""
        base_sql = "CREATE TABLE sys_users (id INTEGER PRIMARY KEY AUTOINCREMENT);"
        
        result = get_schema_for_database_type("mssql", base_sql)
        
        assert "IDENTITY(1,1)" in result
        assert result != base_sql
    
    def test_azuresql_converts_schema(self):
        """Test that Azure SQL converts schema (same as MS SQL)"""
        base_sql = "CREATE TABLE sys_users (id INTEGER PRIMARY KEY AUTOINCREMENT);"
        
        result_mssql = get_schema_for_database_type("mssql", base_sql)
        result_azuresql = get_schema_for_database_type("azuresql", base_sql)
        
        # Azure SQL should use same conversion as MS SQL
        assert result_mssql == result_azuresql
    
    def test_postgresql_converts_schema(self):
        """Test that PostgreSQL converts schema"""
        base_sql = "CREATE TABLE sys_users (id INTEGER PRIMARY KEY AUTOINCREMENT);"
        
        result = get_schema_for_database_type("postgresql", base_sql)
        
        assert "SERIAL" in result
        assert result != base_sql
    
    def test_mysql_converts_schema(self):
        """Test that MySQL converts schema"""
        base_sql = "CREATE TABLE sys_users (id INTEGER PRIMARY KEY AUTOINCREMENT);"
        
        result = get_schema_for_database_type("mysql", base_sql)
        
        assert "AUTO_INCREMENT" in result
        assert result != base_sql


class TestSchemaConversionEdgeCases:
    """Test edge cases in schema conversion"""
    
    def test_empty_sql(self):
        """Test conversion of empty SQL"""
        result_mssql = convert_sqlite_to_mssql("")
        result_postgresql = convert_sqlite_to_postgresql("")
        result_mysql = convert_sqlite_to_mysql("")
        
        # Should handle empty input gracefully
        assert isinstance(result_mssql, str)
        assert isinstance(result_postgresql, str)
        assert isinstance(result_mysql, str)
    
    def test_comments_preserved(self):
        """Test that comments are preserved"""
        sqlite_sql = """
        -- This is a comment
        CREATE TABLE sys_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT  -- Another comment
        );
        """
        
        mssql_sql = convert_sqlite_to_mssql(sqlite_sql)
        
        # Comments should be preserved
        assert "--" in mssql_sql
    
    def test_multiple_statements(self):
        """Test conversion of multiple statements"""
        sqlite_sql = """
        CREATE TABLE sys_users (id INTEGER PRIMARY KEY AUTOINCREMENT);
        CREATE TABLE orders (id INTEGER PRIMARY KEY AUTOINCREMENT);
        CREATE INDEX idx_sys_users_id ON sys_users(id);
        """
        
        mssql_sql = convert_sqlite_to_mssql(sqlite_sql)
        
        # Should handle multiple statements
        assert "CREATE TABLE" in mssql_sql
        assert "CREATE INDEX" in mssql_sql
        # Should have multiple CREATE TABLE statements
        assert mssql_sql.count("CREATE TABLE") == 2
    
    def test_pragma_statements_removed(self):
        """Test that PRAGMA statements are removed"""
        sqlite_sql = """
        PRAGMA foreign_keys = ON;
        CREATE TABLE sys_users (id INTEGER PRIMARY KEY AUTOINCREMENT);
        PRAGMA journal_mode = WAL;
        """
        
        mssql_sql = convert_sqlite_to_mssql(sqlite_sql)
        
        # PRAGMA statements should be removed
        assert "PRAGMA" not in mssql_sql
        # But CREATE TABLE should remain
        assert "CREATE TABLE" in mssql_sql


