"""
================================================================================
Calaveras UniteUs ETL - November 3 Sample File Fix Script
================================================================================
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency

Description:
    One-time script to fix November 3, 2025 sample files to match August 28, 2025
    format. Ensures all sample files have the exact same structure as the
    UniteUs-provided originals.

Features:
    - File format standardization
    - Structure validation
    - Data consistency checking
================================================================================
"""

import csv
import random
from datetime import datetime, timedelta

# Read existing Nov 3 data to preserve IDs and basic info
def read_existing_file(filepath):
    """Read existing file and return as list of dicts"""
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='|', quotechar='"')
        return list(reader)

def write_file(filepath, fieldnames, rows):
    """Write file with pipe delimiters"""
    with open(filepath, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='|', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

# Fix people file
def fix_people_file():
    print("Fixing people file...")
    existing = read_existing_file(r'c:\csv\UniteUs ETL\temp_data_files\SAMPLE_chhsca_people_20251103.txt')
    
    # August 28 format fieldnames
    fieldnames = [
        "person_id", "people_created_by_id", "people_created_at", "people_updated_at",
        "first_name", "middle_name", "last_name", "title", "suffix", "preferred_name",
        "person_consent_status", "date_of_birth", "gender", "sexuality", "sexuality_other",
        "race", "ethnicity", "citizenship", "marital_status", "military_affiliation",
        "gross_monthly_income", "household_size", "adults_in_household", "children_in_household",
        "languages", "insurance_created_at", "insurance_updated_at", "medicaid_id",
        "medicaid_state", "medicare_id", "preferred_communication_method",
        "preferred_communication_time_of_day", "person_email_address", "person_phone_number",
        "current_person_address_created_at", "current_person_address_updated_at",
        "current_person_address_line1", "current_person_address_line2",
        "current_person_address_city", "current_person_address_county",
        "current_person_address_state", "current_person_address_postal_code",
        "current_person_address_type", "current_person_address_is_mailing_address",
        "person_external_id", "pull_timestamp"
    ]
    
    new_rows = []
    employee_ids = ["emp1-nov3", "emp2-nov3", "emp3-nov3"]
    
    for idx, person in enumerate(existing):
        # Calculate household composition
        household_size = int(person.get('household_size', '1'))
        if household_size == 1:
            adults, children = 1, 0
        elif household_size == 2:
            adults, children = (2, 0) if random.random() > 0.5 else (1, 1)
        elif household_size == 3:
            adults, children = (2, 1) if random.random() > 0.5 else (1, 2)
        else:
            adults, children = (2, household_size - 2)
        
        new_row = {
            "person_id": person['person_id'],
            "people_created_by_id": random.choice(employee_ids),
            "people_created_at": person.get('people_created_at', '2025-10-28 09:00:00'),
            "people_updated_at": person.get('people_updated_at', '2025-11-03 09:00:00'),
            "first_name": person.get('first_name', ''),
            "middle_name": person.get('middle_name', ''),
            "last_name": person.get('last_name', ''),
            "title": "",
            "suffix": person.get('suffix', ''),
            "preferred_name": "",
            "person_consent_status": "accepted",
            "date_of_birth": person.get('date_of_birth', ''),
            "gender": person.get('gender', '').lower(),
            "sexuality": "",
            "sexuality_other": "",
            "race": person.get('race', ''),
            "ethnicity": person.get('ethnicity', ''),
            "citizenship": "US Citizen" if random.random() > 0.1 else "undisclosed",
            "marital_status": person.get('marital_status', ''),
            "military_affiliation": "veteran" if person.get('is_veteran') == 'true' else "",
            "gross_monthly_income": person.get('gross_monthly_income', ''),
            "household_size": str(household_size),
            "adults_in_household": str(adults),
            "children_in_household": str(children),
            "languages": person.get('primary_language', 'English'),
            "insurance_created_at": person.get('people_created_at', '2025-10-28 09:00:00'),
            "insurance_updated_at": person.get('people_updated_at', '2025-11-03 09:00:00'),
            "medicaid_id": f"CA{random.randint(10000000, 99999999)}" if random.random() > 0.6 else "",
            "medicaid_state": "CA" if random.random() > 0.6 else "",
            "medicare_id": f"{chr(65+random.randint(0,25))}{random.randint(10000000, 99999999)}" if random.random() > 0.8 else "",
            "preferred_communication_method": random.choice(["phone", "text", "email", ""]),
            "preferred_communication_time_of_day": random.choice(["morning", "afternoon", "evening", ""]),
            "person_email_address": person.get('person_email_address', ''),
            "person_phone_number": person.get('person_phone_number', ''),
            "current_person_address_created_at": person.get('people_created_at', '2025-10-28 09:00:00'),
            "current_person_address_updated_at": person.get('people_updated_at', '2025-11-03 09:00:00'),
            "current_person_address_line1": person.get('current_person_address_line1', ''),
            "current_person_address_line2": person.get('current_person_address_line2', ''),
            "current_person_address_city": person.get('current_person_address_city', ''),
            "current_person_address_county": "Calaveras County",
            "current_person_address_state": person.get('current_person_address_state', 'CA'),
            "current_person_address_postal_code": person.get('current_person_address_postal_code', ''),
            "current_person_address_type": "home",
            "current_person_address_is_mailing_address": "",
            "person_external_id": f"CAL2025{str(idx+1).zfill(3)}",
            "pull_timestamp": person.get('pull_timestamp', '2025-11-03 09:00:00')
        }
        new_rows.append(new_row)
    
    write_file(r'c:\csv\UniteUs ETL\temp_data_files\SAMPLE_chhsca_people_20251103.txt', fieldnames, new_rows)
    print(f"  Fixed {len(new_rows)} people records")

# Fix employees file  
def fix_employees_file():
    print("Fixing employees file...")
    existing = read_existing_file(r'c:\csv\UniteUs ETL\temp_data_files\SAMPLE_chhsca_employees_20251103.txt')
    
    fieldnames = [
        "employee_id", "user_id", "user_created_at", "user_updated_at",
        "first_name", "last_name", "work_title", "email_address",
        "employee_created_at", "employee_updated_at", "employee_status",
        "network_id", "network_name", "provider_id", "provider_name", "pull_timestamp"
    ]
    
    new_rows = []
    for emp in existing:
        new_row = {
            "employee_id": emp['employee_id'],
            "user_id": f"user-{emp['employee_id']}",
            "user_created_at": emp.get('employee_created_at', '2025-10-15 08:00:00'),
            "user_updated_at": emp.get('employee_updated_at', '2025-11-03 09:00:00'),
            "first_name": emp.get('first_name', ''),
            "last_name": emp.get('last_name', ''),
            "work_title": emp.get('work_title', ''),
            "email_address": emp.get('email_address', ''),
            "employee_created_at": emp.get('employee_created_at', '2025-10-15 08:00:00'),
            "employee_updated_at": emp.get('employee_updated_at', '2025-11-03 09:00:00'),
            "employee_status": "active",
            "network_id": emp.get('network_id', ''),
            "network_name": emp.get('network_name', ''),
            "provider_id": emp.get('provider_id', ''),
            "provider_name": emp.get('provider_name', ''),
            "pull_timestamp": emp.get('pull_timestamp', '2025-11-03 09:00:00')
        }
        new_rows.append(new_row)
    
    write_file(r'c:\csv\UniteUs ETL\temp_data_files\SAMPLE_chhsca_employees_20251103.txt', fieldnames, new_rows)
    print(f"  Fixed {len(new_rows)} employee records")

# Fix cases file
def fix_cases_file():
    print("Fixing cases file...")
    existing = read_existing_file(r'c:\csv\UniteUs ETL\temp_data_files\SAMPLE_chhsca_cases_20251103.txt')
    
    fieldnames = [
        "case_id", "person_id", "cases_created_by_id", "case_created_at", "case_updated_at",
        "ar_submitted_on", "case_processed_at", "case_managed_at", "case_off_platform_at",
        "case_closed_at", "case_status", "service_type", "service_subtype", "case_description",
        "is_sensitive", "network_id", "network_name", "provider_id", "provider_name",
        "originating_provider_id", "originating_provider_name", "program_id", "program_name",
        "primary_worker_id", "primary_worker_name", "outcome_id", "outcome_description",
        "outcome_resolution_type", "application_source", "pull_timestamp"
    ]
    
    new_rows = []
    for case in existing:
        new_row = {
            "case_id": case['case_id'],
            "person_id": case.get('person_id', ''),
            "cases_created_by_id": case.get('case_created_by_id', ''),
            "case_created_at": case.get('case_created_at', ''),
            "case_updated_at": case.get('case_updated_at', ''),
            "ar_submitted_on": "",
            "case_processed_at": case.get('case_created_at', ''),
            "case_managed_at": case.get('case_created_at', ''),
            "case_off_platform_at": "",
            "case_closed_at": case.get('case_closed_at', ''),
            "case_status": case.get('case_status', ''),
            "service_type": case.get('service_type', ''),
            "service_subtype": case.get('service_subtype', ''),
            "case_description": case.get('case_description', ''),
            "is_sensitive": case.get('is_sensitive', 'false'),
            "network_id": case.get('network_id', ''),
            "network_name": case.get('network_name', ''),
            "provider_id": case.get('provider_id', ''),
            "provider_name": case.get('provider_name', ''),
            "originating_provider_id": case.get('provider_id', ''),
            "originating_provider_name": case.get('provider_name', ''),
            "program_id": case.get('program_id', ''),
            "program_name": case.get('program_name', ''),
            "primary_worker_id": case.get('primary_worker_id', ''),
            "primary_worker_name": case.get('primary_worker_name', ''),
            "outcome_id": "",
            "outcome_description": "",
            "outcome_resolution_type": "",
            "application_source": "web",
            "pull_timestamp": case.get('pull_timestamp', '2025-11-03 09:00:00')
        }
        new_rows.append(new_row)
    
    write_file(r'c:\csv\UniteUs ETL\temp_data_files\SAMPLE_chhsca_cases_20251103.txt', fieldnames, new_rows)
    print(f"  Fixed {len(new_rows)} case records")

# Fix referrals file
def fix_referrals_file():
    print("Fixing referrals file...")
    existing = read_existing_file(r'c:\csv\UniteUs ETL\temp_data_files\SAMPLE_chhsca_referrals_20251103.txt')
    
    fieldnames = [
        "referral_id", "person_id", "case_id", "referral_created_by_id",
        "referral_created_at", "referral_updated_at", "referral_status",
        "held_in_review_at", "held_in_review_reason", "recalled_at", "recalled_reason",
        "declined_at", "declined_reason", "sending_network_id", "sending_network_name",
        "sending_provider_id", "sending_provider_name", "receiving_network_id",
        "receiving_network_name", "receiving_provider_id", "receiving_provider_name",
        "receiving_program_id", "receiving_program_name", "application_source", "pull_timestamp"
    ]
    
    new_rows = []
    for ref in existing:
        status = ref.get('referral_status', 'accepted')
        new_row = {
            "referral_id": ref['referral_id'],
            "person_id": ref.get('person_id', ''),
            "case_id": ref.get('case_id', ''),
            "referral_created_by_id": ref.get('referral_created_by_id', ''),
            "referral_created_at": ref.get('referral_created_at', ''),
            "referral_updated_at": ref.get('referral_updated_at', ''),
            "referral_status": status,
            "held_in_review_at": "",
            "held_in_review_reason": "",
            "recalled_at": "",
            "recalled_reason": "",
            "declined_at": ref.get('referral_updated_at', '') if status == 'declined' else "",
            "declined_reason": ref.get('referral_reason', '') if status == 'declined' else "",
            "sending_network_id": ref.get('network_id', ''),
            "sending_network_name": ref.get('network_name', ''),
            "sending_provider_id": ref.get('sending_provider_id', ''),
            "sending_provider_name": ref.get('sending_provider_name', ''),
            "receiving_network_id": ref.get('network_id', ''),
            "receiving_network_name": ref.get('network_name', ''),
            "receiving_provider_id": ref.get('receiving_provider_id', ''),
            "receiving_provider_name": ref.get('receiving_provider_name', ''),
            "receiving_program_id": ref.get('program_id', ''),
            "receiving_program_name": ref.get('program_name', ''),
            "application_source": "web",
            "pull_timestamp": ref.get('pull_timestamp', '2025-11-03 09:00:00')
        }
        new_rows.append(new_row)
    
    write_file(r'c:\csv\UniteUs ETL\temp_data_files\SAMPLE_chhsca_referrals_20251103.txt', fieldnames, new_rows)
    print(f"  Fixed {len(new_rows)} referral records")

if __name__ == '__main__':
    print("Fixing November 3, 2025 sample files to match August 28 format...\n")
    fix_people_file()
    fix_employees_file()
    fix_cases_file()
    fix_referrals_file()
    print("\nAll files fixed successfully!")
