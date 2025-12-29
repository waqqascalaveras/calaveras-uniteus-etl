"""
================================================================================
Calaveras UniteUs ETL - Schema Verification Script
================================================================================
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency

Description:
    One-time test script to verify recreate_database.py creates the correct schema
    with all the new fields added for visualizations. Validates database schema
    completeness and field presence.

Features:
    - Schema validation
    - Field presence checking
    - Visualization field verification
================================================================================
"""

import sqlite3
from pathlib import Path

def verify_schema():
    """Verify the database has all required fields for new visualizations"""
    
    db_path = Path("data/database/chhsca_data.db")
    
    if not db_path.exists():
        print("❌ Database does not exist. Run recreate_database.py first.")
        return False
    
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("Verifying Database Schema for New Visualizations")
    print("="*60)
    
    # Check people table has all visualization fields
    cursor.execute("PRAGMA table_info(people)")
    people_columns = {row[1] for row in cursor.fetchall()}
    
    required_people_fields = [
        'household_size',
        'adults_in_household',
        'children_in_household',
        'gross_monthly_income',
        'medicaid_id',
        'medicaid_state',
        'medicare_id',
        'insurance_created_at',
        'insurance_updated_at',
        'preferred_communication_method',
        'preferred_communication_time_of_day',
        'marital_status',
        'languages',
    ]
    
    print("\n✓ Checking people table columns:")
    all_present = True
    for field in required_people_fields:
        if field in people_columns:
            print(f"  ✓ {field}")
        else:
            print(f"  ✗ {field} MISSING!")
            all_present = False
    
    # Check that etl_loaded_at exists (needed for undo functionality)
    cursor.execute("PRAGMA table_info(people)")
    cols = {row[1] for row in cursor.fetchall()}
    if 'etl_loaded_at' in cols:
        print(f"\n✓ etl_loaded_at column exists (needed for undo)")
    else:
        print(f"\n✗ etl_loaded_at column MISSING (needed for undo)!")
        all_present = False
    
    # Check other tables have etl_loaded_at too
    tables_to_check = ['employees', 'cases', 'referrals', 'assistance_requests']
    print(f"\n✓ Checking etl_loaded_at in other tables:")
    for table in tables_to_check:
        cursor.execute(f"PRAGMA table_info({table})")
        cols = {row[1] for row in cursor.fetchall()}
        if 'etl_loaded_at' in cols:
            print(f"  ✓ {table}.etl_loaded_at")
        else:
            print(f"  ✗ {table}.etl_loaded_at MISSING!")
            all_present = False
    
    conn.close()
    
    print("\n" + "="*60)
    if all_present:
        print("✅ Schema is CORRECT - all fields present!")
        print("="*60)
        return True
    else:
        print("❌ Schema is INCOMPLETE - missing fields detected!")
        print("="*60)
        return False

if __name__ == "__main__":
    verify_schema()
