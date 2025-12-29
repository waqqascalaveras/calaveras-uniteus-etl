"""
Database Schema Converter

Converts database schema between SQLite and MS SQL Server formats. Handles
data type mappings, constraint conversions, and SQL syntax differences
between database systems.

Author: Waqqas Hanafi
Copyright: © 2025 Calaveras County Health and Human Services Agency
"""

import re
from typing import Dict


def convert_sqlite_to_mssql(sql: str) -> str:
    """Convert SQLite schema SQL to MS SQL Server format"""
    
    # Split into individual statements
    statements = [s.strip() for s in sql.split(';') if s.strip()]
    converted_statements = []
    
    for statement in statements:
        if not statement:
            continue
        
        # Skip standalone comment lines (but not statements with leading comments)
        if statement.startswith('--') and 'CREATE' not in statement.upper():
            continue
        
        # Convert CREATE TABLE statements
        if 'CREATE TABLE' in statement.upper():
            converted = _convert_create_table(statement)
            converted_statements.append(converted)
        # Convert CREATE INDEX statements
        elif 'CREATE INDEX' in statement.upper():
            converted = _convert_create_index(statement)
            converted_statements.append(converted)
        # Convert CREATE VIEW statements (remove IF NOT EXISTS and fix SQLite syntax)
        elif 'CREATE VIEW' in statement.upper():
            converted = _convert_create_view(statement)
            converted_statements.append(converted)
        # Keep other statements as-is (PRAGMA, etc. are SQLite-specific)
        else:
            # Skip SQLite-specific statements
            if not any(keyword in statement.upper() for keyword in ['PRAGMA', 'BEGIN', 'COMMIT']):
                converted_statements.append(statement)
    
    return ';\n'.join(converted_statements) + ';'


def _convert_create_table(statement: str) -> str:
    """Convert CREATE TABLE statement from SQLite to MS SQL Server"""
    
    # Remove IF NOT EXISTS (MS SQL doesn't support it in CREATE TABLE)
    statement = re.sub(r'CREATE TABLE IF NOT EXISTS', 'CREATE TABLE', statement, flags=re.IGNORECASE)
    
    # Replace SQLite data types with MS SQL Server equivalents
    replacements = {
        r'\bINTEGER\b': 'INT',
        r'\bTEXT\b': 'NVARCHAR(MAX)',
        r'\bREAL\b': 'FLOAT',
        r'\bBLOB\b': 'VARBINARY(MAX)',
        r'\bTIMESTAMP\b': 'DATETIME2',
        r'\bAUTOINCREMENT\b': 'IDENTITY(1,1)',
    }
    
    for pattern, replacement in replacements.items():
        statement = re.sub(pattern, replacement, statement, flags=re.IGNORECASE)
    
    # Handle INTEGER PRIMARY KEY AUTOINCREMENT
    statement = re.sub(
        r'INTEGER PRIMARY KEY AUTOINCREMENT',
        'INT IDENTITY(1,1) PRIMARY KEY',
        statement,
        flags=re.IGNORECASE
    )
    
    # Handle INTEGER PRIMARY KEY (without AUTOINCREMENT)
    # Only replace if not already converted
    if 'IDENTITY' not in statement.upper():
        statement = re.sub(
            r'INTEGER PRIMARY KEY',
            'INT PRIMARY KEY',
            statement,
            flags=re.IGNORECASE
        )
    
    # Remove SQLite-specific constraints that MS SQL doesn't support
    # (Most constraints are compatible)
    
    return statement


def _convert_create_index(statement: str) -> str:
    """Convert CREATE INDEX statement from SQLite to MS SQL Server"""
    
    # Remove IF NOT EXISTS
    statement = re.sub(r'CREATE (UNIQUE )?INDEX IF NOT EXISTS', r'CREATE \1INDEX', statement, flags=re.IGNORECASE)
    
    # MS SQL Server index syntax is mostly compatible
    # Just ensure proper formatting
    
    return statement


def _convert_create_view(statement: str) -> str:
    """Convert CREATE VIEW statement from SQLite to MS SQL Server"""
    
    # Remove IF NOT EXISTS (MS SQL doesn't support it in CREATE VIEW)
    statement = re.sub(r'CREATE VIEW IF NOT EXISTS', 'CREATE VIEW', statement, flags=re.IGNORECASE)
    
    # Replace SQLite concatenation operator || with MS SQL +
    # Handle patterns like: first_name || ' ' || last_name
    statement = re.sub(r"(\w+)\s*\|\|\s*('[^']*')\s*\|\|\s*(\w+)", r"\1 + \2 + \3", statement)
    statement = re.sub(r"(\w+)\s*\|\|\s*('[^']*')", r"\1 + \2", statement)
    statement = re.sub(r"('[^']*')\s*\|\|\s*(\w+)", r"\1 + \2", statement)
    
    # Replace SQLite date functions with MS SQL equivalents
    # date('now') -> CAST(GETDATE() AS DATE)
    statement = re.sub(r"date\('now'\)", "CAST(GETDATE() AS DATE)", statement, flags=re.IGNORECASE)
    
    # date('now', '-30 days') -> DATEADD(day, -30, CAST(GETDATE() AS DATE))
    statement = re.sub(
        r"date\('now',\s*'([+-]\d+)\s+days'\)",
        r"DATEADD(day, \1, CAST(GETDATE() AS DATE))",
        statement,
        flags=re.IGNORECASE
    )
    
    # julianday('now') - julianday(column) -> DATEDIFF(day, column, GETDATE())
    statement = re.sub(
        r"julianday\('now'\)\s*-\s*julianday\((\w+)\)",
        r"DATEDIFF(day, \1, GETDATE())",
        statement,
        flags=re.IGNORECASE
    )
    
    # Replace DATETIME2 back to column references (in case we over-converted)
    # statement = re.sub(r'DATETIME2', 'DATETIME2', statement)
    
    return statement


def convert_sqlite_to_postgresql(sql: str) -> str:
    """Convert SQLite schema SQL to PostgreSQL format"""
    statements = [s.strip() for s in sql.split(';') if s.strip()]
    converted_statements = []
    
    for statement in statements:
        if not statement:
            continue
        
        # Skip standalone comment lines (but not statements with leading comments)
        if statement.startswith('--') and 'CREATE' not in statement.upper():
            continue
        
        if 'CREATE TABLE' in statement.upper():
            converted = _convert_create_table_postgresql(statement)
            converted_statements.append(converted)
        elif 'CREATE INDEX' in statement.upper():
            converted = _convert_create_index(statement)
            converted_statements.append(converted)
        else:
            if not any(keyword in statement.upper() for keyword in ['PRAGMA', 'BEGIN', 'COMMIT']):
                converted_statements.append(statement)
    
    return ';\n'.join(converted_statements) + ';'


def convert_sqlite_to_mysql(sql: str) -> str:
    """Convert SQLite schema SQL to MySQL format"""
    statements = [s.strip() for s in sql.split(';') if s.strip()]
    converted_statements = []
    
    for statement in statements:
        if not statement:
            continue
        
        # Skip standalone comment lines (but not statements with leading comments)
        if statement.startswith('--') and 'CREATE' not in statement.upper():
            continue
        
        if 'CREATE TABLE' in statement.upper():
            converted = _convert_create_table_mysql(statement)
            converted_statements.append(converted)
        elif 'CREATE INDEX' in statement.upper():
            converted = _convert_create_index(statement)
            converted_statements.append(converted)
        else:
            if not any(keyword in statement.upper() for keyword in ['PRAGMA', 'BEGIN', 'COMMIT']):
                converted_statements.append(statement)
    
    return ';\n'.join(converted_statements) + ';'


def _convert_create_table_postgresql(statement: str) -> str:
    """Convert CREATE TABLE statement from SQLite to PostgreSQL"""
    statement = re.sub(r'CREATE TABLE IF NOT EXISTS', 'CREATE TABLE IF NOT EXISTS', statement, flags=re.IGNORECASE)
    
    replacements = {
        r'\bINTEGER\b': 'INT',
        r'\bTEXT\b': 'VARCHAR',
        r'\bREAL\b': 'DOUBLE PRECISION',
        r'\bBLOB\b': 'BYTEA',
        r'\bTIMESTAMP\b': 'TIMESTAMP',
        r'\bAUTOINCREMENT\b': 'SERIAL',
    }
    
    for pattern, replacement in replacements.items():
        statement = re.sub(pattern, replacement, statement, flags=re.IGNORECASE)
    
    statement = re.sub(
        r'INTEGER PRIMARY KEY AUTOINCREMENT',
        'SERIAL PRIMARY KEY',
        statement,
        flags=re.IGNORECASE
    )
    
    if 'SERIAL' not in statement.upper():
        statement = re.sub(
            r'INTEGER PRIMARY KEY',
            'INT PRIMARY KEY',
            statement,
            flags=re.IGNORECASE
        )
    
    return statement


def _convert_create_table_mysql(statement: str) -> str:
    """Convert CREATE TABLE statement from SQLite to MySQL"""
    statement = re.sub(r'CREATE TABLE IF NOT EXISTS', 'CREATE TABLE IF NOT EXISTS', statement, flags=re.IGNORECASE)
    
    replacements = {
        r'\bINTEGER\b': 'INT',
        r'\bTEXT\b': 'TEXT',
        r'\bREAL\b': 'DOUBLE',
        r'\bBLOB\b': 'BLOB',
        r'\bTIMESTAMP\b': 'DATETIME',
        r'\bAUTOINCREMENT\b': 'AUTO_INCREMENT',
    }
    
    for pattern, replacement in replacements.items():
        statement = re.sub(pattern, replacement, statement, flags=re.IGNORECASE)
    
    statement = re.sub(
        r'INTEGER PRIMARY KEY AUTOINCREMENT',
        'INT AUTO_INCREMENT PRIMARY KEY',
        statement,
        flags=re.IGNORECASE
    )
    
    if 'AUTO_INCREMENT' not in statement.upper():
        statement = re.sub(
            r'INTEGER PRIMARY KEY',
            'INT PRIMARY KEY',
            statement,
            flags=re.IGNORECASE
        )
    
    return statement


def get_schema_for_database_type(db_type: str, base_sql: str) -> str:
    """Get schema SQL for the specified database type"""
    if db_type == "mssql" or db_type == "azuresql":
        return convert_sqlite_to_mssql(base_sql)
    elif db_type == "postgresql":
        return convert_sqlite_to_postgresql(base_sql)
    elif db_type == "mysql":
        return convert_sqlite_to_mysql(base_sql)
    else:  # SQLite
        return base_sql

