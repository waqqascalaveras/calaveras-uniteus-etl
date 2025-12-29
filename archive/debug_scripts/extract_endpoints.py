"""
================================================================================
Calaveras UniteUs ETL - API Endpoint Extraction Script
================================================================================
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency

Description:
    Utility script to extract all API endpoints from the dashboard HTML template.
    Used for documentation and endpoint discovery.

Features:
    - Endpoint pattern matching
    - API endpoint discovery
    - Documentation generation
================================================================================
"""
import re

with open('core/web/templates/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Find all fetch('/api/reports/...')  patterns
pattern = r"fetch\('/api/reports/([^']+)'"
matches = re.findall(pattern, content)

print("API Endpoints called from dashboard:")
print("=" * 80)
for endpoint in sorted(set(matches)):
    print(f"  /api/reports/{endpoint}")
