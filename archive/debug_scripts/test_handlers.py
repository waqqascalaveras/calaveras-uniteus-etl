"""
================================================================================
Calaveras UniteUs ETL - Report Handlers Testing Script
================================================================================
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency

Description:
    Debug script for testing report handlers directly to identify issues.
    Used for development and troubleshooting of report generation logic.

Features:
    - Direct handler testing
    - Service layer validation
    - Issue identification
================================================================================
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from core.database import get_database_manager
from core.reports.service import ReportService
from core.reports.handlers import DemographicsReports

# Initialize
db = get_database_manager()
service = ReportService(db)
handler = DemographicsReports(service)

print("=" * 80)
print("TESTING DEMOGRAPHIC VISUALIZATION HANDLERS")
print("=" * 80)

# Test each handler
tests = [
    ("Income Distribution", lambda: handler.get_income_distribution(None, None)),
    ("Insurance Coverage", lambda: handler.get_insurance_coverage(None, None)),
    ("Communication Preferences", lambda: handler.get_communication_preferences(None, None)),
    ("Language Preferences", lambda: handler.get_language_preferences(None, None)),
    ("Household Composition", lambda: handler.get_household_composition(None, None)),
    ("Marital Status", lambda: handler.get_marital_status(None, None)),
]

for name, func in tests:
    print(f"\n{name}:")
    try:
        result = func()
        print(f"  Labels: {result.get('labels', [])}")
        print(f"  Values: {result.get('values', [])}")
        if not result.get('labels'):
            print("  ⚠️  EMPTY DATA!")
    except Exception as e:
        print(f"  ❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

print("\n" + "=" * 80)
