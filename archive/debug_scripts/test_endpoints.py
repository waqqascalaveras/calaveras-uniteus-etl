"""
================================================================================
Calaveras UniteUs ETL - Visualization Endpoints Testing Script
================================================================================
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency

Description:
    Debug script for testing visualization endpoints to identify failures.
    Used for development and troubleshooting of API endpoint responses.

Features:
    - Endpoint testing
    - Response validation
    - Error identification
================================================================================
"""
import requests
import json

BASE_URL = "http://localhost:8000"

endpoints_to_test = [
    "/api/reports/demographics/income-distribution",
    "/api/reports/demographics/insurance-coverage",
    "/api/reports/demographics/communication-preferences",
    "/api/reports/demographics/language-preferences",
    "/api/reports/demographics/household-composition",
]

print("Testing visualization endpoints...")
print("=" * 80)

for endpoint in endpoints_to_test:
    print(f"\nTesting: {endpoint}")
    try:
        response = requests.get(f"{BASE_URL}{endpoint}", timeout=5)
        print(f"  Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"  Data: {json.dumps(data, indent=2)[:200]}...")
        else:
            print(f"  Error: {response.text[:200]}")
    except requests.exceptions.ConnectionRefusedError:
        print("  ERROR: Server not running! Start it with: python core/app.py")
        break
    except Exception as e:
        print(f"  ERROR: {e}")

print("\n" + "=" * 80)
print("If server is not running, start it and run this script again")
