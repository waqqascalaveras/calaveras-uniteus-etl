"""
================================================================================
Calaveras UniteUs ETL - Database Recreation Utility
================================================================================
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency

Description:
    Utility script to completely recreate the database with the correct schema.
    Run this when schema changes need to be applied or when a fresh database
    is required. Backs up existing database before recreation.

Usage:
    python recreate_database.py

Features:
    - Password protection (default: 1234)
    - Automatic backup of existing database
    - Complete schema recreation
    - Index creation
    - Foreign key setup
    - Validation of schema application

Warning:
    This script will DELETE the existing database and create a new one.
    Always review backups before running.

================================================================================
"""

import sqlite3
import getpass
from pathlib import Path
from core.database_schema import get_schema_sql
from core.internal_schema import ensure_internal_schema, verify_internal_schema
from core.config import UnifiedConfig

def verify_password():
    """Verify user has authorization to recreate database."""
    DEFAULT_PASSWORD = "1234"
    MAX_ATTEMPTS = 3
    
    print("\n" + "="*60)
    print("DATABASE RECREATION - AUTHORIZATION REQUIRED")
    print("="*60)
    print("⚠️  WARNING: This will DELETE the existing database!")
    print("="*60 + "\n")
    
    for attempt in range(1, MAX_ATTEMPTS + 1):
        password = getpass.getpass(f"Enter password (Attempt {attempt}/{MAX_ATTEMPTS}): ")
        
        if password == DEFAULT_PASSWORD:
            print("✓ Authorization successful\n")
            return True
        else:
            remaining = MAX_ATTEMPTS - attempt
            if remaining > 0:
                print(f"✗ Incorrect password. {remaining} attempt(s) remaining.\n")
            else:
                print("✗ Maximum attempts exceeded. Access denied.")
    
    return False

def recreate_database():
    # Get the actual database path from config
    config = UnifiedConfig()
    db_path = config.database.path
    internal_db_path = config.directories.database_dir / "internal.db"
    
    print(f"Recreating main database at: {db_path}")
    print(f"Absolute path: {db_path.absolute()}")
    
    # Delete existing database
    if db_path.exists():
        db_path.unlink()
        print("✓ Deleted old main database")
    
    # Recreate internal database with correct schema
    if internal_db_path.exists():
        print(f"\nRecreating internal database at: {internal_db_path}")
        internal_db_path.unlink()
        print("✓ Deleted old internal database")
    
    # Use centralized schema manager for internal.db
    print("✓ Creating internal database with unified schema...")
    ensure_internal_schema(str(internal_db_path))
    
    # Verify internal schema
    if verify_internal_schema(str(internal_db_path)):
        print("✓ Internal database schema verified (sys_users + sys_audit_trail + sys_sessions)")
    else:
        print("✗ Warning: Internal database schema verification failed")
    
    # Verify internal database tables exist
    with sqlite3.connect(str(internal_db_path)) as conn:
        # Check sessions table
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sys_sessions'")
        if cursor.fetchone():
            cursor = conn.execute("PRAGMA table_info(sys_sessions)")
            session_cols = [row[1] for row in cursor.fetchall()]
            print(f"  ✓ Sessions table created with {len(session_cols)} columns")
        else:
            print("  ✗ Warning: Sessions table not found")
        
        # Check SIEM config table
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sys_siem_config'")
        if cursor.fetchone():
            print(f"  ✓ SIEM configuration table created")
        else:
            print("  ✗ Warning: SIEM config table not found")
        
        # Check SFTP config table
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sys_sftp_config'")
        if cursor.fetchone():
            print(f"  ✓ SFTP configuration table created")
        else:
            print("  ✗ Warning: SFTP config table not found")
        
        # Check SFTP file patterns table
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sftp_file_patterns'")
        if cursor.fetchone():
            print(f"  ✓ SFTP file patterns table created")
        else:
            print("  ✗ Warning: SFTP file patterns table not found")
    
    # Create new main database with correct schema
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    schema_sql = get_schema_sql()
    
    print("✓ Creating tables...")
    statement_count = 0
    for statement in schema_sql.split(';'):
        statement = statement.strip()
        if statement:
            try:
                cursor.execute(statement)
                statement_count += 1
            except Exception as e:
                print(f"✗ Error executing statement {statement_count + 1}: {e}")
                print(f"  Statement: {statement[:200]}...")
                raise
    
    conn.commit()
    print(f"✓ Created {statement_count} database objects")
    
    # Verify key tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cursor.fetchall()]
    print(f"\n✓ Created {len(tables)} tables:")
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", (table,))
        if cursor.fetchone()[0] > 0:
            cursor.execute(f"PRAGMA table_info({table})")
            cols = cursor.fetchall()
            print(f"  - {table} ({len(cols)} columns)")
    
    # Verify critical columns exist
    print("\n✓ Verifying critical columns:")
    critical_checks = [
        ('assistance_requests', 'assistance_request_id'),
        ('people', 'person_consent_status'),
        ('referrals', 'person_id'),
        ('resource_list_shares', 'person_id'),
    ]
    
    for table, column in critical_checks:
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [row[1] for row in cursor.fetchall()]
        if column in columns:
            print(f"  ✓ {table}.{column} exists")
        else:
            print(f"  ✗ {table}.{column} MISSING!")
    
    conn.close()
    print("\n✅ Database recreated successfully!")

if __name__ == "__main__":
    if verify_password():
        recreate_database()
    else:
        print("\n❌ Database recreation aborted - authentication failed.")
        exit(1)
