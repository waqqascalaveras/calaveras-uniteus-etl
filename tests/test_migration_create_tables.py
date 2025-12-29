"""
Test the migration with create_tables functionality
Simple integration tests to verify the migration workflow
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_database_adapter_imports():
    """Test that database adapters can be imported"""
    try:
        from core.database_adapter import (
            SQLiteAdapter, MSSQLAdapter, 
            PostgreSQLAdapter, MySQLAdapter
        )
        print("✅ All database adapters imported successfully")
        return True
    except ImportError as e:
        print(f"❌ Failed to import adapters: {e}")
        return False


def test_schema_converter_imports():
    """Test that schema converters can be imported"""
    try:
        from core.database_schema_converter import (
            convert_sqlite_to_mssql,
            convert_sqlite_to_postgresql,
            convert_sqlite_to_mysql
        )
        print("✅ All schema converters imported successfully")
        return True
    except ImportError as e:
        print(f"❌ Failed to import converters: {e}")
        return False


def test_schema_function_import():
    """Test that schema function can be imported"""
    try:
        from core.database_schema import get_schema_sql
        schema = get_schema_sql()
        assert len(schema) > 0, "Schema should not be empty"
        assert 'CREATE TABLE' in schema, "Schema should contain CREATE TABLE"
        assert 'people' in schema, "Schema should contain people table"
        print(f"✅ Schema function works ({len(schema)} chars)")
        return True
    except Exception as e:
        print(f"❌ Failed to get schema: {e}")
        return False


def test_migration_endpoint_parameters():
    """Test that migration endpoint has create_tables parameter"""
    try:
        with open('core/app.py', 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Check for create_tables parameter
        assert 'create_tables: bool = Form(True)' in content, \
            "Migration endpoint should have create_tables parameter"
        
        # Check for table creation logic
        assert 'if create_tables:' in content, \
            "Migration endpoint should check create_tables"
        
        # Check for DatabaseSchemaConverter import
        assert 'DatabaseSchemaConverter' in content, \
            "Should import DatabaseSchemaConverter"
        
        print("✅ Migration endpoint has create_tables functionality")
        return True
    except Exception as e:
        print(f"❌ Migration endpoint check failed: {e}")
        return False


def test_frontend_checkbox():
    """Test that frontend has create_tables checkbox"""
    try:
        with open('core/web/templates/admincp_database.html', 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check for checkbox
        assert 'migration_create_tables' in content, \
            "Should have migration_create_tables checkbox"
        
        # Check checkbox is checked by default
        assert 'id="migration_create_tables" checked' in content, \
            "Checkbox should be checked by default"
        
        # Check label
        assert 'Create Tables' in content, \
            "Should have Create Tables label"
        
        # Check JavaScript sends parameter
        assert "formData.append('create_tables', createTables)" in content or \
               "formData.append('create_tables'" in content, \
            "JavaScript should send create_tables parameter"
        
        print("✅ Frontend has create_tables checkbox with correct setup")
        return True
    except Exception as e:
        print(f"❌ Frontend check failed: {e}")
        return False


def test_subprocess_no_window():
    """Test that launch.pyw has CREATE_NO_WINDOW flag"""
    try:
        with open('launch.pyw', 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check for CREATE_NO_WINDOW constant
        assert 'CREATE_NO_WINDOW = 0x08000000' in content, \
            "Should define CREATE_NO_WINDOW constant"
        
        # Check it's used in subprocess calls
        assert 'creationflags=CREATE_NO_WINDOW' in content, \
            "Should use CREATE_NO_WINDOW in subprocess calls"
        
        print("✅ launch.pyw properly hides console windows")
        return True
    except Exception as e:
        print(f"❌ launch.pyw check failed: {e}")
        return False


def test_form_change_tracking():
    """Test that form change tracking marks forms as saved"""
    try:
        with open('core/web/templates/admincp_database.html', 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check for markSaved call after save
        assert "formChangeTracker.markSaved('databaseSettingsForm')" in content, \
            "Should call markSaved after successful save"
        
        print("✅ Form change tracking properly resets after save")
        return True
    except Exception as e:
        print(f"❌ Form tracking check failed: {e}")
        return False


if __name__ == "__main__":
    print("=" * 70)
    print("Testing Recent Changes - Integration Tests")
    print("=" * 70)
    print()
    
    tests = [
        ("Database Adapters", test_database_adapter_imports),
        ("Schema Converters", test_schema_converter_imports),
        ("Schema Function", test_schema_function_import),
        ("Migration Endpoint", test_migration_endpoint_parameters),
        ("Frontend Checkbox", test_frontend_checkbox),
        ("Console Window Hiding", test_subprocess_no_window),
        ("Form Change Tracking", test_form_change_tracking),
    ]
    
    results = []
    for name, test_func in tests:
        print(f"\n[Testing: {name}]")
        result = test_func()
        results.append((name, result))
    
    print()
    print("=" * 70)
    print("Test Summary")
    print("=" * 70)
    passed = sum(1 for _, r in results if r)
    total = len(results)
    
    for name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{status}: {name}")
    
    print()
    print(f"Result: {passed}/{total} tests passed")
    print("=" * 70)
    
    sys.exit(0 if passed == total else 1)

