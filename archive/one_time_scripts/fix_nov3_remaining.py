"""
================================================================================
Calaveras UniteUs ETL - November 3 Remaining Files Fix Script
================================================================================
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency

Description:
    One-time script to complete fixes for all remaining November 3, 2025 sample files.
    Ensures consistency and proper formatting across all data files.

Features:
    - Batch file processing
    - Format standardization
    - Data validation
================================================================================
"""

import csv
import random

def read_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='|', quotechar='"')
        return list(reader)

def write_file(filepath, fieldnames, rows):
    with open(filepath, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='|', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

# Fix assistance_requests to match August 28 format
def fix_assistance_requests():
    print("Fixing assistance_requests file...")
    existing = read_file(r'c:\csv\UniteUs ETL\temp_data_files\SAMPLE_chhsca_assistance_requests_20251103.txt')
    people = read_file(r'c:\csv\UniteUs ETL\temp_data_files\SAMPLE_chhsca_people_20251103.txt')
    cases = read_file(r'c:\csv\UniteUs ETL\temp_data_files\SAMPLE_chhsca_cases_20251103.txt')
    
    # Create lookup dictionaries
    people_dict = {p['person_id']: p for p in people}
    case_dict = {c['case_id']: c for c in cases}
    
    fieldnames = [
        "assistance_request_id", "description", "service_id", "provider_id",
        "person_id", "case_id", "person_first_name", "person_last_name",
        "person_date_of_birth", "person_middle_name", "person_preferred_name",
        "person_gender", "person_sexuality", "person_sexuality_other", "person_citizenship",
        "person_ethnicity", "person_marital_status", "person_race", "person_title",
        "person_suffix", "person_gross_monthly_income", "person_email_address",
        "person_phone_number", "mil_affiliation", "mil_current_status",
        "mil_currently_transitioning", "mil_one_day_active_duty", "mil_branch",
        "mil_service_era", "mil_entry_date", "mil_exit_date", "mil_deployed",
        "mil_deployment_starts_at", "mil_deployment_ends_at", "mil_discharge_type",
        "mil_discharged_due_to_disability", "mil_service_connected_disability",
        "mil_service_connected_disability_rating", "mil_proof_of_veteran_status",
        "mil_proof_type", "address_type", "address_line_1", "address_line_2",
        "address_city", "address_state", "address_country", "address_postal_code",
        "address_county", "created_at", "updated_at", "pull_timestamp"
    ]
    
    new_rows = []
    for ar in existing:
        person = people_dict.get(ar.get('person_id', ''), {})
        case_id = ""
        # Find matching case for this person
        for c in cases:
            if c.get('person_id') == ar.get('person_id'):
                case_id = c.get('case_id', '')
                break
        
        new_row = {
            "assistance_request_id": ar['assistance_request_id'],
            "description": f"Client requesting assistance with {ar.get('program_name', 'services')}",
            "service_id": f"svc-{ar['assistance_request_id']}",
            "provider_id": ar.get('provider_id', ''),
            "person_id": ar.get('person_id', ''),
            "case_id": case_id,
            "person_first_name": person.get('first_name', ''),
            "person_last_name": person.get('last_name', ''),
            "person_date_of_birth": person.get('date_of_birth', ''),
            "person_middle_name": person.get('middle_name', ''),
            "person_preferred_name": person.get('preferred_name', ''),
            "person_gender": person.get('gender', ''),
            "person_sexuality": person.get('sexuality', ''),
            "person_sexuality_other": person.get('sexuality_other', ''),
            "person_citizenship": person.get('citizenship', ''),
            "person_ethnicity": person.get('ethnicity', ''),
            "person_marital_status": person.get('marital_status', ''),
            "person_race": person.get('race', ''),
            "person_title": person.get('title', ''),
            "person_suffix": "",
            "person_gross_monthly_income": person.get('gross_monthly_income', ''),
            "person_email_address": person.get('person_email_address', ''),
            "person_phone_number": person.get('person_phone_number', ''),
            "mil_affiliation": person.get('military_affiliation', ''),
            "mil_current_status": "",
            "mil_currently_transitioning": "",
            "mil_one_day_active_duty": "",
            "mil_branch": "",
            "mil_service_era": "",
            "mil_entry_date": "",
            "mil_exit_date": "",
            "mil_deployed": "",
            "mil_deployment_starts_at": "",
            "mil_deployment_ends_at": "",
            "mil_discharge_type": "",
            "mil_discharged_due_to_disability": "",
            "mil_service_connected_disability": "",
            "mil_service_connected_disability_rating": "",
            "mil_proof_of_veteran_status": "",
            "mil_proof_type": "",
            "address_type": person.get('current_person_address_type', 'home'),
            "address_line_1": person.get('current_person_address_line1', ''),
            "address_line_2": person.get('current_person_address_line2', ''),
            "address_city": person.get('current_person_address_city', ''),
            "address_state": person.get('current_person_address_state', 'CA'),
            "address_country": "US",
            "address_postal_code": person.get('current_person_address_postal_code', ''),
            "address_county": person.get('current_person_address_county', 'Calaveras County'),
            "created_at": ar.get('created_at', ''),
            "updated_at": ar.get('updated_at', ''),
            "pull_timestamp": ar.get('pull_timestamp', '2025-11-03 09:00:00')
        }
        new_rows.append(new_row)
    
    write_file(r'c:\csv\UniteUs ETL\temp_data_files\SAMPLE_chhsca_assistance_requests_20251103.txt', fieldnames, new_rows)
    print(f"  Fixed {len(new_rows)} assistance request records")

# Fix assistance_requests_supplemental_responses
def fix_supplemental_responses():
    print("Fixing assistance_requests_supplemental_responses file...")
    existing = read_file(r'c:\csv\UniteUs ETL\temp_data_files\SAMPLE_chhsca_assistance_requests_supplemental_responses_20251103.txt')
    
    fieldnames = [
        "ar_supplemental_response_id", "assistance_request_id", "provider_id",
        "form_id", "form_name", "submission_id", "question_id",
        "question_at_time_of_response", "form_responses", "created_at",
        "updated_at", "pull_timestamp"
    ]
    
    new_rows = []
    for resp in existing:
        new_row = {
            "ar_supplemental_response_id": resp.get('ar_supplemental_response_id', ''),
            "assistance_request_id": resp.get('assistance_request_id', ''),
            "provider_id": resp.get('provider_id', ''),
            "form_id": f"form-{random.randint(1000, 9999)}",
            "form_name": resp.get('form_name', 'Additional Information'),
            "submission_id": f"sub-{resp.get('ar_supplemental_response_id', '')[:8]}",
            "question_id": "",
            "question_at_time_of_response": resp.get('question', ''),
            "form_responses": resp.get('response', ''),
            "created_at": resp.get('created_at', ''),
            "updated_at": resp.get('updated_at', ''),
            "pull_timestamp": resp.get('pull_timestamp', '2025-11-03 09:00:00')
        }
        new_rows.append(new_row)
    
    write_file(r'c:\csv\UniteUs ETL\temp_data_files\SAMPLE_chhsca_assistance_requests_supplemental_responses_20251103.txt', fieldnames, new_rows)
    print(f"  Fixed {len(new_rows)} supplemental response records")

# Fix resource_lists
def fix_resource_lists():
    print("Fixing resource_lists file...")
    existing = read_file(r'c:\csv\UniteUs ETL\temp_data_files\SAMPLE_chhsca_resource_lists_20251103.txt')
    
    fieldnames = [
        "id", "resource_list_id", "resource_list_created_at", "resource_list_updated_at",
        "created_via_type", "created_via_id", "application_source", "provider_id",
        "provider_name", "program_id", "program_name", "service_type_name",
        "parent_service_type_name", "created_by_employee_id", "pull_timestamp"
    ]
    
    new_rows = []
    for rl in existing:
        new_row = {
            "id": rl.get('id', ''),
            "resource_list_id": rl.get('resource_list_id', ''),
            "resource_list_created_at": rl.get('resource_list_created_at', ''),
            "resource_list_updated_at": rl.get('resource_list_updated_at', ''),
            "created_via_type": "referral",
            "created_via_id": "",
            "application_source": "app-client",
            "provider_id": rl.get('provider_id', ''),
            "provider_name": rl.get('provider_name', ''),
            "program_id": f"prog-{rl.get('resource_list_id', '')[:8]}",
            "program_name": rl.get('resource_list_name', ''),
            "service_type_name": "Resource Directory",
            "parent_service_type_name": "Information & Referral",
            "created_by_employee_id": rl.get('created_by_employee_id', ''),
            "pull_timestamp": rl.get('pull_timestamp', '2025-11-03 09:00:00')
        }
        new_rows.append(new_row)
    
    write_file(r'c:\csv\UniteUs ETL\temp_data_files\SAMPLE_chhsca_resource_lists_20251103.txt', fieldnames, new_rows)
    print(f"  Fixed {len(new_rows)} resource list records")

# Fix resource_list_shares
def fix_resource_list_shares():
    print("Fixing resource_list_shares file...")
    existing = read_file(r'c:\csv\UniteUs ETL\temp_data_files\SAMPLE_chhsca_resource_list_shares_20251103.txt')
    
    fieldnames = [
        "id", "share_event_origin", "application_source", "resource_list_id",
        "shared_by_employee_id", "person_id", "share_event_id",
        "share_event_created_at", "shared_method_value", "shared_method_type",
        "shared_via_type", "shared_via_id", "shared_language", "provider_id",
        "provider_name", "program_id", "program_name", "pull_timestamp"
    ]
    
    new_rows = []
    for rls in existing:
        new_row = {
            "id": rls.get('id', ''),
            "share_event_origin": "Resource List Shares",
            "application_source": "app-client",
            "resource_list_id": rls.get('resource_list_id', ''),
            "shared_by_employee_id": rls.get('shared_by_employee_id', ''),
            "person_id": rls.get('person_id', ''),
            "share_event_id": f"evt-{rls.get('id', '')[:8]}",
            "share_event_created_at": rls.get('resource_list_share_created_at', ''),
            "shared_method_value": "",
            "shared_method_type": "email",
            "shared_via_type": rls.get('shared_via', 'referral'),
            "shared_via_id": "",
            "shared_language": "en",
            "provider_id": rls.get('provider_id', ''),
            "provider_name": rls.get('provider_name', ''),
            "program_id": f"prog-{rls.get('resource_list_id', '')[:8]}",
            "program_name": "Resource Sharing",
            "pull_timestamp": rls.get('pull_timestamp', '2025-11-03 09:00:00')
        }
        new_rows.append(new_row)
    
    write_file(r'c:\csv\UniteUs ETL\temp_data_files\SAMPLE_chhsca_resource_list_shares_20251103.txt', fieldnames, new_rows)
    print(f"  Fixed {len(new_rows)} resource list share records")

if __name__ == '__main__':
    print("Fixing remaining November 3, 2025 sample files...\n")
    fix_assistance_requests()
    fix_supplemental_responses()
    fix_resource_lists()
    fix_resource_list_shares()
    print("\nAll remaining files fixed!")
