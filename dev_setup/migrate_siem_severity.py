"""
Migration script to add severity filtering columns to siem_config table

This script adds the following columns to existing siem_config tables:
- json_log_min_severity (default: WARNING)
- windows_event_log_min_severity (default: ERROR)
- syslog_min_severity (default: ERROR)

Author: Waqqas Hanafi
Date: 2025-12-16
"""

import sqlite3
from pathlib import Path

def migrate_database():
    """Add severity columns to siem_config table"""
    
    db_path = Path("data/database/internal.db")
    
    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        print("   Run the application first to create the database.")
        return False
    
    try:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            
            # Check if columns already exist
            cursor.execute("PRAGMA table_info(siem_config)")
            columns = [row[1] for row in cursor.fetchall()]
            
            columns_to_add = []
            
            if 'json_log_min_severity' not in columns:
                columns_to_add.append(('json_log_min_severity', 'WARNING'))
            
            if 'windows_event_log_min_severity' not in columns:
                columns_to_add.append(('windows_event_log_min_severity', 'ERROR'))
            
            if 'syslog_min_severity' not in columns:
                columns_to_add.append(('syslog_min_severity', 'ERROR'))
            
            if not columns_to_add:
                print("✅ Database already up to date - no migration needed")
                return True
            
            # Add missing columns
            for column_name, default_value in columns_to_add:
                try:
                    cursor.execute(f"ALTER TABLE siem_config ADD COLUMN {column_name} TEXT DEFAULT '{default_value}'")
                    print(f"✅ Added column: {column_name} (default: {default_value})")
                except sqlite3.OperationalError as e:
                    if "duplicate column name" in str(e).lower():
                        print(f"⚠️  Column already exists: {column_name}")
                    else:
                        raise
            
            # Update existing row to have the default values
            cursor.execute("""
                UPDATE siem_config 
                SET json_log_min_severity = COALESCE(json_log_min_severity, 'WARNING'),
                    windows_event_log_min_severity = COALESCE(windows_event_log_min_severity, 'ERROR'),
                    syslog_min_severity = COALESCE(syslog_min_severity, 'ERROR')
                WHERE id = 1
            """)
            
            conn.commit()
            
            print("\n✅ Migration completed successfully!")
            print("\nSeverity levels configured:")
            print("  - JSON Logging: WARNING and above")
            print("  - Windows Event Log: ERROR and above")
            print("  - Syslog/SIEM: ERROR and above")
            print("\nYou can adjust these in the Admin Control Panel > Logging pages.")
            
            return True
            
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("=" * 70)
    print("SIEM Severity Filtering Migration")
    print("=" * 70)
    print()
    
    success = migrate_database()
    
    print()
    print("=" * 70)
    
    if success:
        print("Migration completed - restart the application to use the new features.")
    else:
        print("Migration failed - please check the errors above.")
    
    print("=" * 70)
