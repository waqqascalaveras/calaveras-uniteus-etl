"""
Test view conversion specifically
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.database_schema import get_schema_sql
from core.database_schema_converter import convert_sqlite_to_mssql

# Get base schema
base_schema = get_schema_sql()

# Convert to MS SQL
mssql_schema = convert_sqlite_to_mssql(base_schema)

# Parse statements
statements = [s.strip() for s in mssql_schema.split(';') if s.strip()]

# Find and display all views
views = [s for s in statements if 'CREATE VIEW' in s.upper()]

print("=" * 80)
print(f"FOUND {len(views)} VIEWS - SHOWING FULL CONTENT:")
print("=" * 80)

for i, view in enumerate(views, 1):
    print(f"\n--- VIEW {i} ---")
    print(view)
    print()
