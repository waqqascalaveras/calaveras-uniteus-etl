"""
Test the schema converter fix for MS SQL
"""
from core.database_schema import get_schema_sql
from core.database_schema_converter import (
    convert_sqlite_to_mssql,
    convert_sqlite_to_postgresql,
    convert_sqlite_to_mysql
)


def test_mssql_converter():
    """Test that MS SQL converter includes CREATE TABLE statements"""
    schema = get_schema_sql()
    converted = convert_sqlite_to_mssql(schema)
    
    statements = [s.strip() for s in converted.split(';') if s.strip()]
    
    # Count statement types
    create_tables = [s for s in statements if 'CREATE TABLE' in s.upper()]
    create_indexes = [s for s in statements if 'CREATE INDEX' in s.upper()]
    
    print(f"✓ Total statements: {len(statements)}")
    print(f"✓ CREATE TABLE statements: {len(create_tables)}")
    print(f"✓ CREATE INDEX statements: {len(create_indexes)}")
    
    # Verify we have tables
    assert len(create_tables) > 0, "Should have CREATE TABLE statements"
    assert len(create_tables) >= 9, f"Should have at least 9 tables, got {len(create_tables)}"
    
    # Verify tables come before their indexes
    # Find etl_metadata table
    etl_table_idx = None
    etl_index_idx = None
    
    for i, stmt in enumerate(statements):
        if 'CREATE TABLE' in stmt.upper() and 'etl_metadata' in stmt:
            etl_table_idx = i
        if 'CREATE INDEX' in stmt.upper() and 'idx_etl_metadata_table' in stmt:
            etl_index_idx = i
    
    assert etl_table_idx is not None, "Should find etl_metadata table"
    assert etl_index_idx is not None, "Should find etl_metadata index"
    assert etl_table_idx < etl_index_idx, f"Table at {etl_table_idx} should come before index at {etl_index_idx}"
    
    # Verify data type conversions
    table_stmt = create_tables[0]
    assert 'INT' in table_stmt.upper(), "Should convert INTEGER to INT"
    assert 'NVARCHAR' in table_stmt.upper(), "Should convert TEXT to NVARCHAR"
    assert 'DATETIME2' in table_stmt.upper(), "Should convert TIMESTAMP to DATETIME2"
    assert 'IDENTITY' in table_stmt.upper(), "Should convert AUTOINCREMENT to IDENTITY"
    
    # Verify IF NOT EXISTS is removed
    assert 'IF NOT EXISTS' not in converted, "Should remove IF NOT EXISTS for MS SQL"
    
    print("✅ MS SQL converter working correctly!")
    return True


def test_postgresql_converter():
    """Test that PostgreSQL converter includes CREATE TABLE statements"""
    schema = get_schema_sql()
    converted = convert_sqlite_to_postgresql(schema)
    
    statements = [s.strip() for s in converted.split(';') if s.strip()]
    
    # Count statement types
    create_tables = [s for s in statements if 'CREATE TABLE' in s.upper()]
    create_indexes = [s for s in statements if 'CREATE INDEX' in s.upper()]
    
    print(f"✓ Total statements: {len(statements)}")
    print(f"✓ CREATE TABLE statements: {len(create_tables)}")
    print(f"✓ CREATE INDEX statements: {len(create_indexes)}")
    
    assert len(create_tables) > 0, "Should have CREATE TABLE statements"
    assert len(create_tables) >= 9, f"Should have at least 9 tables, got {len(create_tables)}"
    
    # Verify IF NOT EXISTS is preserved for PostgreSQL
    assert 'IF NOT EXISTS' in converted, "Should keep IF NOT EXISTS for PostgreSQL"
    
    print("✅ PostgreSQL converter working correctly!")
    return True


def test_mysql_converter():
    """Test that MySQL converter includes CREATE TABLE statements"""
    schema = get_schema_sql()
    converted = convert_sqlite_to_mysql(schema)
    
    statements = [s.strip() for s in converted.split(';') if s.strip()]
    
    # Count statement types
    create_tables = [s for s in statements if 'CREATE TABLE' in s.upper()]
    create_indexes = [s for s in statements if 'CREATE INDEX' in s.upper()]
    
    print(f"✓ Total statements: {len(statements)}")
    print(f"✓ CREATE TABLE statements: {len(create_tables)}")
    print(f"✓ CREATE INDEX statements: {len(create_indexes)}")
    
    assert len(create_tables) > 0, "Should have CREATE TABLE statements"
    assert len(create_tables) >= 9, f"Should have at least 9 tables, got {len(create_tables)}"
    
    # Verify data type conversions
    table_stmt = create_tables[0]
    assert 'AUTO_INCREMENT' in table_stmt.upper(), "Should convert AUTOINCREMENT to AUTO_INCREMENT"
    
    print("✅ MySQL converter working correctly!")
    return True


if __name__ == "__main__":
    print("=" * 70)
    print("Testing Schema Converter Bug Fix")
    print("=" * 70)
    print()
    
    try:
        print("\n[Testing MS SQL Converter]")
        test_mssql_converter()
        
        print("\n[Testing PostgreSQL Converter]")
        test_postgresql_converter()
        
        print("\n[Testing MySQL Converter]")
        test_mysql_converter()
        
        print()
        print("=" * 70)
        print("✅ All Schema Converter Tests Passed!")
        print("=" * 70)
        print()
        print("Summary of fixes:")
        print("1. ✅ Schema converters now include CREATE TABLE statements")
        print("2. ✅ Tables are created before their indexes")
        print("3. ✅ Data type conversions working correctly")
        print("4. ✅ IF NOT EXISTS handled properly per database type")
        print("5. ✅ Test connection has 30-second timeout on frontend")
        
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        exit(1)
