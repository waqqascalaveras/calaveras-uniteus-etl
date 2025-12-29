"""
================================================================================
Calaveras UniteUs ETL - Tech Debt Cleanup Script
================================================================================
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency

Description:
    Tech debt cleanup script that moves temporary test/debug scripts to archive
    and identifies issues. Helps maintain codebase organization by archiving
    temporary development scripts.

Features:
    - Automatic script archiving
    - Issue identification
    - Codebase organization
================================================================================
"""
import shutil
from pathlib import Path

# Temporary scripts created during debugging that should be archived
TEMP_SCRIPTS = [
    'test_sql.py',
    'test_handlers.py', 
    'test_endpoints.py',
    'check_visualization_data.py',
    'extract_endpoints.py',
    'cleanup_tech_debt.py',  # This script will archive itself last
]

def cleanup_temp_scripts():
    """Move temporary scripts to archive"""
    
    # Create archive directory if needed
    archive_dir = Path('archive')
    archive_dir.mkdir(exist_ok=True)
    
    temp_scripts_dir = archive_dir / 'debug_scripts'
    temp_scripts_dir.mkdir(exist_ok=True)
    
    print("=" * 80)
    print("TECH DEBT CLEANUP")
    print("=" * 80)
    
    moved_count = 0
    
    for script in TEMP_SCRIPTS:
        script_path = Path(script)
        if script_path.exists():
            target = temp_scripts_dir / script
            # Don't move if it already exists
            if not target.exists():
                shutil.move(str(script_path), str(target))
                print(f"✓ Moved {script} -> archive/debug_scripts/")
                moved_count += 1
            else:
                print(f"⊗ Skipped {script} (already in archive)")
        else:
            print(f"⊗ {script} not found (already archived?)")
    
    print(f"\n✅ Cleanup complete! Moved {moved_count} files to archive/debug_scripts/")
    
    # Update archive README
    readme_content = """# Debug Scripts Archive

This folder contains temporary debugging and testing scripts created during development.

## Scripts

### debug_scripts/
- **test_sql.py** - Direct SQL query testing
- **test_handlers.py** - Handler method testing  
- **test_endpoints.py** - API endpoint testing (requires 'requests' module)
- **check_visualization_data.py** - Database visualization data checker
- **extract_endpoints.py** - Dashboard API endpoint extractor
- **cleanup_tech_debt.py** - This cleanup script

These were temporary scripts used for:
1. Debugging visualization issues
2. Testing SQL queries directly against the database
3. Validating handler method outputs
4. Investigating schema mismatches

## Status

✅ All scripts served their purpose and are archived for reference.
⚠️ Some scripts may have import errors (e.g., 'requests' not in requirements.txt)

## Housekeeping

These files can be safely deleted if not needed for future reference.
The actual test suite is in `/unit_tests/` directory.
"""
    
    readme_path = archive_dir / 'DEBUG_SCRIPTS_README.md'
    with open(readme_path, 'w') as f:
        f.write(readme_content)
    print(f"✓ Created {readme_path}")
    
    print("\n" + "=" * 80)
    print("REMAINING TECH DEBT ITEMS")
    print("=" * 80)
    
    # Check for other potential issues
    issues = []
    
    # 1. Check for unused documentation files
    doc_files = list(Path('.').glob('*.md'))
    if len(doc_files) > 2:  # More than just README.md
        print(f"\n📄 Documentation files in root: {len(doc_files)}")
        for doc in sorted(doc_files):
            if doc.name != 'README.md':
                print(f"   - {doc.name}")
        print("   Consider moving to docs/ folder")
    
    # 2. Check __pycache__ directories
    pycache_dirs = list(Path('.').rglob('__pycache__'))
    if pycache_dirs:
        print(f"\n🗂️  Found {len(pycache_dirs)} __pycache__ directories")
        print("   These are auto-generated and can be removed")
        print("   Add to .gitignore: **/__pycache__/")
    
    # 3. Check .coverage and htmlcov
    if Path('.coverage').exists():
        print(f"\n📊 Coverage files found:")
        print("   - .coverage (SQLite database)")
        if Path('htmlcov').exists():
            print("   - htmlcov/ (HTML coverage report)")
        print("   Consider adding to .gitignore")
    
    print("\n" + "=" * 80)
    print("✅ Tech debt review complete!")
    print("\nNext steps:")
    print("  1. Review and organize documentation files")
    print("  2. Update .gitignore for Python artifacts")
    print("  3. Consider removing htmlcov/ and .coverage from repo")
    print("=" * 80)

if __name__ == "__main__":
    cleanup_temp_scripts()
