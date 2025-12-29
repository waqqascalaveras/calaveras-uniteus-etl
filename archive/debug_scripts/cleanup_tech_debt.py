"""
Technical Debt Cleanup Script
Organizes one-time-use scripts into an archive folder for reference
"""

import shutil
from pathlib import Path

# Scripts that were created for one-time tasks but should be kept for reference
ONE_TIME_SCRIPTS = [
    'fix_nov3_samples.py',
    'fix_nov3_remaining.py',
    'enrich_november_samples.py',
    'analyze_enrichment.py',
    'verify_schema.py',
]

# Documentation files that should be archived
ONE_TIME_DOCS = [
    'dashboard_analysis.md',
    'NEW_DASHBOARD_FEATURES.md',
    'test_results.txt',
]

def cleanup_tech_debt():
    """Move one-time scripts to archive folder"""
    
    # Create archive directory if it doesn't exist
    archive_dir = Path('archive')
    archive_dir.mkdir(exist_ok=True)
    
    scripts_dir = archive_dir / 'one_time_scripts'
    scripts_dir.mkdir(exist_ok=True)
    
    docs_dir = archive_dir / 'documentation'
    docs_dir.mkdir(exist_ok=True)
    
    print("Moving one-time scripts to archive...")
    moved_count = 0
    
    # Move scripts
    for script in ONE_TIME_SCRIPTS:
        script_path = Path(script)
        if script_path.exists():
            target = scripts_dir / script
            shutil.move(str(script_path), str(target))
            print(f"  ✓ Moved {script} → archive/one_time_scripts/")
            moved_count += 1
    
    # Move documentation
    for doc in ONE_TIME_DOCS:
        doc_path = Path(doc)
        if doc_path.exists():
            target = docs_dir / doc
            shutil.move(str(doc_path), str(target))
            print(f"  ✓ Moved {doc} → archive/documentation/")
            moved_count += 1
    
    print(f"\n✅ Cleanup complete! Moved {moved_count} files to archive/")
    print("\nNote: These files are still available in the archive folder for reference.")
    print("They can be safely deleted or kept as historical documentation.")
    
    # Create README in archive
    readme_content = """# Archive

This folder contains one-time scripts and documentation from the development process.

## one_time_scripts/
Scripts that were created to perform specific one-time tasks:
- **fix_nov3_samples.py** - Fixed November 3 sample files to match August 28 format
- **fix_nov3_remaining.py** - Fixed remaining November 3 sample files  
- **enrich_november_samples.py** - Added diverse demographic data to November samples
- **analyze_enrichment.py** - Analyzed the enriched data distribution
- **verify_schema.py** - Verified database schema has all required fields

These scripts have served their purpose and are kept for reference only.

## documentation/
Development documentation and analysis:
- **dashboard_analysis.md** - Analysis of dashboard visualizations (current vs potential)
- **NEW_DASHBOARD_FEATURES.md** - Summary of new dashboard features implemented
- **test_results.txt** - Historical test results

## Status
These files are archived because:
1. They were created for one-time migration/setup tasks that are now complete
2. The functionality they provide is no longer needed in regular operations
3. They are kept for historical reference and documentation purposes

You can safely delete this entire archive folder if you want to clean up the project.
"""
    
    readme_path = archive_dir / 'README.md'
    with open(readme_path, 'w') as f:
        f.write(readme_content)
    print(f"  ✓ Created archive/README.md")

if __name__ == "__main__":
    cleanup_tech_debt()
