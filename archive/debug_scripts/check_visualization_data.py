"""
================================================================================
Calaveras UniteUs ETL - Visualization Data Check Script
================================================================================
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency

Description:
    Debug script to check what data exists for empty visualizations.
    Used for troubleshooting visualization rendering issues.

Features:
    - Data availability checking
    - Visualization data validation
    - Database query verification
================================================================================
"""
import sqlite3
from pathlib import Path

db_path = Path("data/database/chhsca_data.db")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("=" * 80)
print("CHECKING VISUALIZATION DATA")
print("=" * 80)

# Check people table demographic data
print("\n1. PEOPLE TABLE DEMOGRAPHIC FIELDS:")
cursor.execute("SELECT COUNT(*) FROM people")
total_people = cursor.fetchone()[0]
print(f"   Total people records: {total_people}")

if total_people > 0:
    # Check gross_monthly_income
    cursor.execute("SELECT COUNT(*) FROM people WHERE gross_monthly_income IS NOT NULL AND gross_monthly_income != ''")
    income_count = cursor.fetchone()[0]
    print(f"   Records with income data: {income_count}")
    
    cursor.execute("SELECT DISTINCT gross_monthly_income FROM people WHERE gross_monthly_income IS NOT NULL AND gross_monthly_income != '' LIMIT 10")
    incomes = cursor.fetchall()
    print(f"   Sample incomes: {[i[0] for i in incomes]}")
    
    # Check household_size
    cursor.execute("SELECT COUNT(*) FROM people WHERE household_size IS NOT NULL AND household_size != ''")
    household_count = cursor.fetchone()[0]
    print(f"   Records with household size: {household_count}")
    
    # Check insurance
    cursor.execute("SELECT COUNT(*) FROM people WHERE medicaid_id IS NOT NULL OR medicare_id IS NOT NULL")
    insurance_count = cursor.fetchone()[0]
    print(f"   Records with insurance: {insurance_count}")
    
    # Check communication preferences
    cursor.execute("SELECT COUNT(*) FROM people WHERE preferred_communication_method IS NOT NULL AND preferred_communication_method != ''")
    comm_count = cursor.fetchone()[0]
    print(f"   Records with communication preference: {comm_count}")
    
    cursor.execute("SELECT DISTINCT preferred_communication_method FROM people WHERE preferred_communication_method IS NOT NULL AND preferred_communication_method != '' LIMIT 10")
    comm_methods = cursor.fetchall()
    print(f"   Sample methods: {[c[0] for c in comm_methods]}")
    
    # Check languages
    cursor.execute("SELECT COUNT(*) FROM people WHERE languages IS NOT NULL AND languages != ''")
    lang_count = cursor.fetchone()[0]
    print(f"   Records with languages: {lang_count}")
    
    cursor.execute("SELECT DISTINCT languages FROM people WHERE languages IS NOT NULL AND languages != '' LIMIT 10")
    languages = cursor.fetchall()
    print(f"   Sample languages: {[l[0] for l in languages]}")
    
    # Check marital status
    cursor.execute("SELECT COUNT(*) FROM people WHERE marital_status IS NOT NULL AND marital_status != ''")
    marital_count = cursor.fetchone()[0]
    print(f"   Records with marital status: {marital_count}")

# Check referrals table
print("\n2. REFERRALS TABLE:")
cursor.execute("SELECT COUNT(*) FROM referrals")
total_referrals = cursor.fetchone()[0]
print(f"   Total referral records: {total_referrals}")

if total_referrals > 0:
    cursor.execute("SELECT COUNT(*) FROM referrals WHERE status IS NOT NULL")
    status_count = cursor.fetchone()[0]
    print(f"   Records with status: {status_count}")
    
    cursor.execute("SELECT DISTINCT status FROM referrals WHERE status IS NOT NULL LIMIT 10")
    statuses = cursor.fetchall()
    print(f"   Sample statuses: {[s[0] for s in statuses]}")
    
    cursor.execute("SELECT COUNT(*) FROM referrals WHERE service_type IS NOT NULL")
    service_count = cursor.fetchone()[0]
    print(f"   Records with service_type: {service_count}")
    
    cursor.execute("SELECT DISTINCT service_type FROM referrals WHERE service_type IS NOT NULL LIMIT 10")
    services = cursor.fetchall()
    print(f"   Sample service types: {[s[0] for s in services]}")

# Check cases table
print("\n3. CASES TABLE:")
cursor.execute("SELECT COUNT(*) FROM cases")
total_cases = cursor.fetchone()[0]
print(f"   Total case records: {total_cases}")

if total_cases > 0:
    cursor.execute("SELECT COUNT(*) FROM cases WHERE housing_status IS NOT NULL AND housing_status != ''")
    housing_count = cursor.fetchone()[0]
    print(f"   Records with housing_status: {housing_count}")
    
    cursor.execute("SELECT DISTINCT housing_status FROM cases WHERE housing_status IS NOT NULL AND housing_status != '' LIMIT 10")
    housing = cursor.fetchall()
    print(f"   Sample housing statuses: {[h[0] for h in housing]}")

# Sample actual data
print("\n4. SAMPLE PEOPLE RECORDS:")
cursor.execute("""
    SELECT id, gross_monthly_income, household_size, medicaid_id, medicare_id, 
           preferred_communication_method, languages, marital_status 
    FROM people 
    LIMIT 5
""")
for row in cursor.fetchall():
    print(f"   {row}")

print("\n5. SAMPLE REFERRAL RECORDS:")
cursor.execute("SELECT id, status, service_type, created_at FROM referrals LIMIT 5")
for row in cursor.fetchall():
    print(f"   {row}")

conn.close()
print("\n" + "=" * 80)
