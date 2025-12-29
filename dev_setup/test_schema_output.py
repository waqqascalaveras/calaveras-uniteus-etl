"""
Diagnostic script to test schema converter output
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.database_schema import get_schema_sql
from core.database_schema_converter import convert_sqlite_to_mssql

# Get base schema
base_schema = get_schema_sql()
print("=" * 80)
print("BASE SQLITE SCHEMA (first 1000 chars):")
print("=" * 80)
print(base_schema[:1000])
print("\n")

# Convert to MS SQL
mssql_schema = convert_sqlite_to_mssql(base_schema)

print("=" * 80)
print("CONVERTED MS SQL SCHEMA (first 1000 chars):")
print("=" * 80)
print(mssql_schema[:1000])
print("\n")

# Parse statements
statements = [s.strip() for s in mssql_schema.split(';') if s.strip()]

print("=" * 80)
print(f"TOTAL STATEMENTS: {len(statements)}")
print("=" * 80)

# Count statement types
create_tables = [s for s in statements if 'CREATE TABLE' in s.upper()]
create_indexes = [s for s in statements if 'CREATE INDEX' in s.upper()]
create_views = [s for s in statements if 'CREATE VIEW' in s.upper()]

print(f"CREATE TABLE statements: {len(create_tables)}")
print(f"CREATE INDEX statements: {len(create_indexes)}")
print(f"CREATE VIEW statements: {len(create_views)}")
print()

# Show first few CREATE TABLE statements
print("=" * 80)
print("FIRST 3 CREATE TABLE STATEMENTS:")
print("=" * 80)
for i, stmt in enumerate(create_tables[:3], 1):
    print(f"\n--- Table {i} ---")
    print(stmt[:300] if len(stmt) > 300 else stmt)

# Check for problematic patterns
print("\n" + "=" * 80)
print("CHECKING FOR SQLITE-SPECIFIC SYNTAX:")
print("=" * 80)

issues = []
for i, stmt in enumerate(statements, 1):
    if '||' in stmt and 'CREATE VIEW' in stmt.upper():
        issues.append(f"Statement {i}: Contains '||' (SQLite concatenation)")
    if 'julianday' in stmt.lower():
        issues.append(f"Statement {i}: Contains 'julianday' (SQLite function)")
    if 'IF NOT EXISTS' in stmt.upper() and 'CREATE VIEW' in stmt.upper():
        issues.append(f"Statement {i}: CREATE VIEW with IF NOT EXISTS (not supported in MS SQL)")

if issues:
    print("⚠️  ISSUES FOUND:")
    for issue in issues:
        print(f"  - {issue}")
else:
    print("✅ No obvious SQLite-specific syntax found")

# Show all statement types in order
print("\n" + "=" * 80)
print("STATEMENT TYPES IN ORDER (first 20):")
print("=" * 80)
for i, stmt in enumerate(statements[:20], 1):
    stmt_type = "UNKNOWN"
    if 'CREATE TABLE' in stmt.upper():
        # Extract table name
        import re
        match = re.search(r'CREATE TABLE\s+(?:IF NOT EXISTS\s+)?(\w+)', stmt, re.IGNORECASE)
        table_name = match.group(1) if match else "???"
        stmt_type = f"CREATE TABLE {table_name}"
    elif 'CREATE INDEX' in stmt.upper():
        stmt_type = "CREATE INDEX"
    elif 'CREATE VIEW' in stmt.upper():
        match = re.search(r'CREATE VIEW\s+(?:IF NOT EXISTS\s+)?(\w+)', stmt, re.IGNORECASE)
        view_name = match.group(1) if match else "???"
        stmt_type = f"CREATE VIEW {view_name}"
    
    print(f"{i:2}. {stmt_type}")
