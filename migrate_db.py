"""
Quick database migration script to add trigger_type and triggered_by columns
"""
import sqlite3
from pathlib import Path

db_path = Path("data/database/uniteus.db")

if db_path.exists():
    print(f"Found database at: {db_path}")
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    # Check if etl_metadata table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='etl_metadata'")
    if cursor.fetchone():
        print("etl_metadata table exists")
        
        # Try to add trigger_type column
        try:
            cursor.execute("SELECT trigger_type FROM etl_metadata LIMIT 1")
            print("trigger_type column already exists")
        except sqlite3.OperationalError:
            print("Adding trigger_type column...")
            cursor.execute("ALTER TABLE etl_metadata ADD COLUMN trigger_type TEXT DEFAULT 'manual'")
            print("✓ Added trigger_type column")
        
        # Try to add triggered_by column
        try:
            cursor.execute("SELECT triggered_by FROM etl_metadata LIMIT 1")
            print("triggered_by column already exists")
        except sqlite3.OperationalError:
            print("Adding triggered_by column...")
            cursor.execute("ALTER TABLE etl_metadata ADD COLUMN triggered_by TEXT")
            print("✓ Added triggered_by column")
        
        # Create index if it doesn't exist
        try:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_etl_metadata_trigger ON etl_metadata(trigger_type)")
            print("✓ Created index on trigger_type")
        except Exception as e:
            print(f"Index creation: {e}")
        
        conn.commit()
        print("\n✓ Migration completed successfully!")
    else:
        print("etl_metadata table does not exist yet")
    
    conn.close()
else:
    print(f"Database not found at: {db_path}")
    print("This is normal for a fresh installation.")
