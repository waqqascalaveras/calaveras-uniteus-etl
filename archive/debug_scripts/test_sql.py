"""
================================================================================
Calaveras UniteUs ETL - SQL Query Testing Script
================================================================================
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency

Description:
    Debug script for testing SQL queries directly against the database.
    Used for development and troubleshooting of database queries.

Features:
    - Direct SQL query execution
    - Query result validation
    - Database connection testing
================================================================================
"""
import sqlite3
from pathlib import Path

db_path = Path("data/database/chhsca_data.db")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("=" * 80)
print("TESTING SQL QUERIES DIRECTLY")
print("=" * 80)

# Test 1: Income Distribution
print("\n1. INCOME DISTRIBUTION QUERY:")
query1 = """
SELECT 
    CASE 
        WHEN CAST(COALESCE(gross_monthly_income, 0) AS INTEGER) = 0 THEN 'No Income'
        WHEN CAST(gross_monthly_income AS INTEGER) < 1000 THEN 'Under $1,000'
        WHEN CAST(gross_monthly_income AS INTEGER) < 2000 THEN '$1,000-$1,999'
        WHEN CAST(gross_monthly_income AS INTEGER) < 3000 THEN '$2,000-$2,999'
        WHEN CAST(gross_monthly_income AS INTEGER) < 5000 THEN '$3,000-$4,999'
        ELSE '$5,000+'
    END as income_bracket,
    COUNT(*) as count
FROM people
GROUP BY income_bracket
ORDER BY 
    CASE income_bracket
        WHEN 'No Income' THEN 0
        WHEN 'Under $1,000' THEN 1
        WHEN '$1,000-$1,999' THEN 2
        WHEN '$2,000-$2,999' THEN 3
        WHEN '$3,000-$4,999' THEN 4
        ELSE 5
    END
"""
cursor.execute(query1)
results = cursor.fetchall()
print(f"Results: {results}")

# Test 2: Insurance Coverage
print("\n2. INSURANCE COVERAGE QUERY:")
query2 = """
SELECT 
    CASE 
        WHEN medicaid_id IS NOT NULL AND medicaid_id != '' AND medicare_id IS NOT NULL AND medicare_id != '' THEN 'Both'
        WHEN medicaid_id IS NOT NULL AND medicaid_id != '' THEN 'Medicaid'
        WHEN medicare_id IS NOT NULL AND medicare_id != '' THEN 'Medicare'
        ELSE 'None'
    END as coverage,
    COUNT(*) as count
FROM people
GROUP BY coverage
ORDER BY count DESC
"""
cursor.execute(query2)
results = cursor.fetchall()
print(f"Results: {results}")

# Test 3: Communication Preferences
print("\n3. COMMUNICATION PREFERENCES QUERY:")
query3 = """
SELECT 
    CASE 
        WHEN preferred_communication_method IS NULL OR preferred_communication_method = '' THEN 'Not Specified'
        ELSE preferred_communication_method
    END as method,
    COUNT(*) as count
FROM people
GROUP BY method
ORDER BY count DESC
LIMIT 10
"""
cursor.execute(query3)
results = cursor.fetchall()
print(f"Results: {results}")

# Test 4: Language Preferences
print("\n4. LANGUAGE PREFERENCES QUERY:")
query4 = """
SELECT 
    CASE 
        WHEN languages IS NULL OR languages = '' THEN 'Not Specified'
        ELSE languages
    END as language,
    COUNT(*) as count
FROM people
GROUP BY language
ORDER BY count DESC
LIMIT 15
"""
cursor.execute(query4)
results = cursor.fetchall()
print(f"Results: {results}")

# Test 5: Household Size Distribution
print("\n5. HOUSEHOLD SIZE DISTRIBUTION QUERY:")
query5 = """
SELECT 
    CASE 
        WHEN household_size IS NULL OR household_size = '' THEN 'Not Specified'
        ELSE CAST(household_size AS TEXT)
    END as size,
    COUNT(*) as count,
    ROUND(AVG(CAST(COALESCE(adults_in_household, 0) AS REAL)), 1) as avg_adults,
    ROUND(AVG(CAST(COALESCE(children_in_household, 0) AS REAL)), 1) as avg_children
FROM people
GROUP BY size
ORDER BY 
    CASE WHEN size = 'Not Specified' THEN 999
    ELSE CAST(size AS INTEGER)
    END
"""
cursor.execute(query5)
results = cursor.fetchall()
print(f"Results: {results}")

conn.close()
print("\n" + "=" * 80)
