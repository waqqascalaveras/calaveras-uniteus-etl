"""
Database Schema Definition

Consolidated database schema definition with proper indexing, relationships,
and constraints. Defines the structure for all data tables including people,
referrals, cases, assistance requests, employees, and resource management.

Note: The automated_sync_config table is created separately via 
create_automated_sync_table() and is not part of this schema definition.
This is because it's a system configuration table that needs special handling
for different database types (SQLite, MSSQL, PostgreSQL, MySQL).

Author: Waqqas Hanafi
Copyright: © 2025 Calaveras County Health and Human Services Agency
"""

from typing import Dict

def get_schema_sql() -> str:
    """Get the complete database schema SQL"""
    return """
CREATE TABLE IF NOT EXISTS etl_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_name TEXT NOT NULL UNIQUE,
    table_name TEXT NOT NULL,
    file_date TEXT NOT NULL,
    records_processed INTEGER NOT NULL,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    processing_started_at TIMESTAMP NOT NULL,
    processing_completed_at TIMESTAMP NOT NULL,
    status TEXT NOT NULL,
    error_message TEXT,
    file_hash TEXT NOT NULL,
    trigger_type TEXT DEFAULT 'manual',
    triggered_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_etl_metadata_table ON etl_metadata(table_name);
CREATE INDEX IF NOT EXISTS idx_etl_metadata_date ON etl_metadata(file_date);
CREATE INDEX IF NOT EXISTS idx_etl_metadata_status ON etl_metadata(status);
CREATE INDEX IF NOT EXISTS idx_etl_metadata_trigger ON etl_metadata(trigger_type);

CREATE TABLE IF NOT EXISTS people (
    person_id TEXT PRIMARY KEY,
    people_created_by_id TEXT,
    people_created_at TIMESTAMP,
    people_updated_at TIMESTAMP,
    first_name TEXT,
    middle_name TEXT,
    last_name TEXT,
    title TEXT,
    suffix TEXT,
    preferred_name TEXT,
    person_consent_status TEXT,
    date_of_birth DATE,
    gender TEXT,
    sexuality TEXT,
    sexuality_other TEXT,
    race TEXT,
    ethnicity TEXT,
    citizenship TEXT,
    marital_status TEXT,
    military_affiliation TEXT,
    gross_monthly_income REAL,
    household_size INTEGER,
    adults_in_household INTEGER,
    children_in_household INTEGER,
    languages TEXT,
    insurance_created_at TIMESTAMP,
    insurance_updated_at TIMESTAMP,
    medicaid_id TEXT,
    medicaid_state TEXT,
    medicare_id TEXT,
    preferred_communication_method TEXT,
    preferred_communication_time_of_day TEXT,
    person_email_address TEXT,
    person_phone_number TEXT,
    current_person_address_created_at TIMESTAMP,
    current_person_address_updated_at TIMESTAMP,
    current_person_address_line1 TEXT,
    current_person_address_line2 TEXT,
    current_person_address_city TEXT,
    current_person_address_county TEXT,
    current_person_address_state TEXT,
    current_person_address_postal_code TEXT,
    current_person_address_type TEXT,
    current_person_address_is_mailing_address TEXT,
    person_external_id TEXT,
    pull_timestamp TIMESTAMP,
    etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_people_name ON people(last_name, first_name);
CREATE INDEX IF NOT EXISTS idx_people_external_id ON people(person_external_id);
CREATE INDEX IF NOT EXISTS idx_people_created_at ON people(people_created_at);

CREATE TABLE IF NOT EXISTS employees (
    employee_id TEXT PRIMARY KEY,
    user_id TEXT,
    user_created_at TIMESTAMP,
    user_updated_at TIMESTAMP,
    first_name TEXT,
    last_name TEXT,
    work_title TEXT,
    email_address TEXT,
    employee_created_at TIMESTAMP,
    employee_updated_at TIMESTAMP,
    employee_status TEXT,
    network_id TEXT,
    network_name TEXT,
    provider_id TEXT,
    provider_name TEXT,
    pull_timestamp TIMESTAMP,
    etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_employees_user_id ON employees(user_id);
CREATE INDEX IF NOT EXISTS idx_employees_provider ON employees(provider_id);
CREATE INDEX IF NOT EXISTS idx_employees_status ON employees(employee_status);
CREATE INDEX IF NOT EXISTS idx_employees_name ON employees(last_name, first_name);

CREATE TABLE IF NOT EXISTS cases (
    case_id TEXT PRIMARY KEY,
    person_id TEXT,
    cases_created_by_id TEXT,
    case_created_at TIMESTAMP,
    case_updated_at TIMESTAMP,
    ar_submitted_on TIMESTAMP,
    case_processed_at TIMESTAMP,
    case_managed_at TIMESTAMP,
    case_off_platform_at TIMESTAMP,
    case_closed_at TIMESTAMP,
    case_status TEXT,
    service_type TEXT,
    service_subtype TEXT,
    case_description TEXT,
    is_sensitive BOOLEAN,
    network_id TEXT,
    network_name TEXT,
    provider_id TEXT,
    provider_name TEXT,
    originating_provider_id TEXT,
    originating_provider_name TEXT,
    program_id TEXT,
    program_name TEXT,
    primary_worker_id TEXT,
    primary_worker_name TEXT,
    outcome_id TEXT,
    outcome_description TEXT,
    outcome_resolution_type TEXT,
    application_source TEXT,
    pull_timestamp TIMESTAMP,
    etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_cases_person ON cases(person_id);
CREATE INDEX IF NOT EXISTS idx_cases_status ON cases(case_status);
CREATE INDEX IF NOT EXISTS idx_cases_service_type ON cases(service_type);
CREATE INDEX IF NOT EXISTS idx_cases_created_at ON cases(case_created_at);
CREATE INDEX IF NOT EXISTS idx_cases_worker ON cases(primary_worker_id);

CREATE TABLE IF NOT EXISTS referrals (
    referral_id TEXT PRIMARY KEY,
    person_id TEXT,
    case_id TEXT,
    referral_created_by_id TEXT,
    referral_created_at TIMESTAMP,
    referral_updated_at TIMESTAMP,
    referral_sent_at TIMESTAMP,
    referral_status TEXT,
    held_in_review_at TIMESTAMP,
    held_in_review_reason TEXT,
    recalled_at TIMESTAMP,
    recalled_reason TEXT,
    declined_at TIMESTAMP,
    declined_reason TEXT,
    sending_network_id TEXT,
    sending_network_name TEXT,
    sending_provider_id TEXT,
    sending_provider_name TEXT,
    receiving_network_id TEXT,
    receiving_network_name TEXT,
    receiving_provider_id TEXT,
    receiving_provider_name TEXT,
    receiving_program_id TEXT,
    receiving_program_name TEXT,
    referring_provider_id TEXT,
    referring_provider_name TEXT,
    referring_employee_id TEXT,
    receiving_employee_id TEXT,
    service_type TEXT,
    service_subtype TEXT,
    referral_reason TEXT,
    referral_notes TEXT,
    followup_date DATE,
    application_source TEXT,
    pull_timestamp TIMESTAMP,
    etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_referrals_case ON referrals(case_id);
CREATE INDEX IF NOT EXISTS idx_referrals_status ON referrals(referral_status);
CREATE INDEX IF NOT EXISTS idx_referrals_created_at ON referrals(referral_created_at);
CREATE INDEX IF NOT EXISTS idx_referrals_service_type ON referrals(service_type);

CREATE TABLE IF NOT EXISTS assistance_requests (
    assistance_request_id TEXT PRIMARY KEY,
    person_id TEXT,
    case_id TEXT,
    description TEXT,
    service_id TEXT,
    provider_id TEXT,
    person_first_name TEXT,
    person_last_name TEXT,
    person_date_of_birth DATE,
    person_middle_name TEXT,
    person_preferred_name TEXT,
    person_gender TEXT,
    person_sexuality TEXT,
    person_sexuality_other TEXT,
    person_citizenship TEXT,
    person_ethnicity TEXT,
    person_marital_status TEXT,
    person_race TEXT,
    person_title TEXT,
    person_suffix TEXT,
    person_gross_monthly_income REAL,
    person_email_address TEXT,
    person_phone_number TEXT,
    mil_affiliation TEXT,
    mil_current_status TEXT,
    mil_currently_transitioning BOOLEAN,
    mil_one_day_active_duty BOOLEAN,
    mil_branch TEXT,
    mil_service_era TEXT,
    mil_entry_date DATE,
    mil_exit_date DATE,
    mil_deployed BOOLEAN,
    mil_deployment_starts_at DATE,
    mil_deployment_ends_at DATE,
    mil_discharge_type TEXT,
    mil_discharged_due_to_disability BOOLEAN,
    mil_service_connected_disability BOOLEAN,
    mil_service_connected_disability_rating TEXT,
    mil_proof_of_veteran_status TEXT,
    mil_proof_type TEXT,
    address_type TEXT,
    address_line_1 TEXT,
    address_line_2 TEXT,
    address_city TEXT,
    address_state TEXT,
    address_country TEXT,
    address_postal_code TEXT,
    address_county TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    status TEXT,
    request_type TEXT,
    request_description TEXT,
    urgency_level TEXT,
    demographics_age INTEGER,
    demographics_gender_identity TEXT,
    demographics_race TEXT,
    demographics_ethnicity TEXT,
    demographics_primary_language TEXT,
    demographics_household_size INTEGER,
    demographics_income_range TEXT,
    housing_current_status TEXT,
    housing_type TEXT,
    employment_status TEXT,
    education_level TEXT,
    health_insurance_status TEXT,
    transportation_access TEXT,
    legal_status TEXT,
    mil_veteran_status TEXT,
    pull_timestamp TIMESTAMP,
    etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_assistance_requests_person ON assistance_requests(person_id);
CREATE INDEX IF NOT EXISTS idx_assistance_requests_case ON assistance_requests(case_id);
CREATE INDEX IF NOT EXISTS idx_assistance_requests_created_at ON assistance_requests(created_at);

CREATE TABLE IF NOT EXISTS assistance_requests_supplemental_responses (
    ar_supplemental_response_id TEXT PRIMARY KEY,
    assistance_request_id TEXT,
    provider_id TEXT,
    form_id TEXT,
    form_name TEXT,
    submission_id TEXT,
    question_id TEXT,
    question_at_time_of_response TEXT,
    form_responses TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    pull_timestamp TIMESTAMP,
    etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ar_supplemental_assistance_request ON assistance_requests_supplemental_responses(assistance_request_id);
CREATE INDEX IF NOT EXISTS idx_ar_supplemental_created_at ON assistance_requests_supplemental_responses(created_at);

CREATE TABLE IF NOT EXISTS resource_lists (
    id TEXT PRIMARY KEY,
    resource_list_id TEXT,
    resource_list_created_at TIMESTAMP,
    resource_list_updated_at TIMESTAMP,
    created_via_type TEXT,
    created_via_id TEXT,
    application_source TEXT,
    provider_id TEXT,
    provider_name TEXT,
    program_id TEXT,
    program_name TEXT,
    service_type_name TEXT,
    parent_service_type_name TEXT,
    created_by_employee_id TEXT,
    pull_timestamp TIMESTAMP,
    etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_resource_lists_resource_list_id ON resource_lists(resource_list_id);
CREATE INDEX IF NOT EXISTS idx_resource_lists_provider ON resource_lists(provider_id);
CREATE INDEX IF NOT EXISTS idx_resource_lists_service_type ON resource_lists(service_type_name);

CREATE TABLE IF NOT EXISTS resource_list_shares (
    id TEXT PRIMARY KEY,
    share_event_origin TEXT,
    application_source TEXT,
    resource_list_id TEXT,
    shared_by_employee_id TEXT,
    shared_to_employee_id TEXT,
    person_id TEXT,
    shared_to_person_id TEXT,
    share_event_id TEXT,
    share_event_created_at TIMESTAMP,
    shared_at TIMESTAMP,
    shared_method_value TEXT,
    shared_method_type TEXT,
    shared_via_type TEXT,
    shared_via_id TEXT,
    shared_language TEXT,
    sharing_method TEXT,
    sharing_notes TEXT,
    provider_id TEXT,
    provider_name TEXT,
    program_id TEXT,
    program_name TEXT,
    pull_timestamp TIMESTAMP,
    etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_resource_list_shares_resource_list ON resource_list_shares(resource_list_id);
CREATE INDEX IF NOT EXISTS idx_resource_list_shares_shared_by ON resource_list_shares(shared_by_employee_id);
CREATE INDEX IF NOT EXISTS idx_resource_list_shares_shared_at ON resource_list_shares(shared_at);

CREATE TABLE IF NOT EXISTS data_quality_issues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    record_id TEXT,
    issue_type TEXT NOT NULL,
    issue_description TEXT NOT NULL,
    field_name TEXT,
    original_value TEXT,
    corrected_value TEXT,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_name TEXT
);

CREATE INDEX IF NOT EXISTS idx_dq_table ON data_quality_issues(table_name);
CREATE INDEX IF NOT EXISTS idx_dq_type ON data_quality_issues(issue_type);
CREATE INDEX IF NOT EXISTS idx_dq_detected_at ON data_quality_issues(detected_at);

CREATE TABLE IF NOT EXISTS sftp_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sync_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_list TEXT NOT NULL,
    file_count INTEGER DEFAULT 0,
    synced_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_sftp_cache_sync_time ON sftp_cache(sync_time);

CREATE VIEW IF NOT EXISTS v_active_cases AS
SELECT 
    c.case_id,
    c.case_status,
    c.service_type,
    c.service_subtype,
    c.case_created_at,
    c.case_updated_at,
    p.person_id,
    p.first_name || ' ' || p.last_name AS client_name,
    p.person_email_address,
    p.person_phone_number,
    e.employee_id AS worker_id,
    e.first_name || ' ' || e.last_name AS worker_name,
    e.email_address AS worker_email,
    c.provider_name,
    c.program_name
FROM cases c
LEFT JOIN people p ON c.person_id = p.person_id
LEFT JOIN employees e ON c.primary_worker_id = e.employee_id
WHERE c.case_status IN ('active', 'open', 'in_progress');

CREATE VIEW IF NOT EXISTS v_referral_flow AS
SELECT 
    r.referral_id,
    r.referral_status,
    r.service_type,
    r.referral_created_at,
    r.referring_provider_name,
    r.receiving_provider_name,
    c.case_id,
    p.person_id,
    p.first_name || ' ' || p.last_name AS client_name
FROM referrals r
LEFT JOIN cases c ON r.case_id = c.case_id
LEFT JOIN people p ON c.person_id = p.person_id
WHERE r.referral_created_at >= date('now', '-30 days');

CREATE VIEW IF NOT EXISTS v_employee_workload AS
SELECT 
    e.employee_id,
    e.first_name || ' ' || e.last_name AS employee_name,
    e.provider_name,
    COUNT(CASE WHEN c.case_status IN ('active', 'open', 'in_progress') THEN 1 END) AS active_cases,
    COUNT(c.case_id) AS total_cases,
    MAX(c.case_created_at) AS last_case_date
FROM employees e
LEFT JOIN cases c ON e.employee_id = c.primary_worker_id
GROUP BY e.employee_id, e.first_name, e.last_name, e.provider_name;

CREATE VIEW IF NOT EXISTS v_service_demand AS
SELECT 
    service_type,
    service_subtype,
    COUNT(*) AS request_count,
    COUNT(CASE WHEN case_status IN ('active', 'open', 'in_progress') THEN 1 END) AS active_count,
    AVG(julianday('now') - julianday(case_created_at)) AS avg_age_days
FROM cases
WHERE case_created_at >= date('now', '-90 days')
GROUP BY service_type, service_subtype
ORDER BY request_count DESC;

CREATE VIEW IF NOT EXISTS v_client_journey AS
SELECT 
    p.person_id,
    p.first_name || ' ' || p.last_name AS client_name,
    c.case_id,
    c.case_status,
    c.service_type,
    c.case_created_at,
    COUNT(r.referral_id) AS referral_count,
    COUNT(ar.id) AS assistance_request_count,
    MAX(r.referral_created_at) AS last_referral_date
FROM people p
LEFT JOIN cases c ON p.person_id = c.person_id
LEFT JOIN referrals r ON c.case_id = r.case_id
LEFT JOIN assistance_requests ar ON p.person_id = ar.person_id
GROUP BY p.person_id, p.first_name, p.last_name, c.case_id, 
         c.case_status, c.service_type, c.case_created_at;
"""


def get_view_definitions() -> Dict[str, str]:
    """Get analytical view definitions for documentation"""
    return {
        'v_active_cases': 'Dashboard of current active cases with client and worker information',
        'v_referral_flow': 'Recent referral network analysis showing inter-agency connections',
        'v_employee_workload': 'Staff performance metrics and caseload statistics',
        'v_service_demand': 'Service request trends and demand analysis',
        'v_client_journey': 'Complete client interaction history and touchpoints'
    }


def get_table_descriptions() -> Dict[str, str]:
    """Get human-readable descriptions of all tables"""
    return {
        'automated_sync_config': 'System configuration for automated SFTP sync and ETL job scheduling (single-row table)',
        'etl_metadata': 'ETL processing history and job metadata tracking',
        'people': 'Individual client demographic and contact information',
        'employees': 'Staff member profiles and organizational assignments',
        'cases': 'Service cases with status tracking and outcomes',
        'referrals': 'Inter-agency referral network and coordination data',
        'assistance_requests': 'Client assistance requests with detailed intake information',
        'assistance_requests_supplemental_responses': 'Supplemental form responses linked to assistance requests',
        'resource_lists': 'Resource lists created for clients with service recommendations',
        'resource_list_shares': 'Resource list distribution tracking and sharing methods',
        'data_quality_issues': 'Data validation errors and quality concerns detected during ETL',
        'sftp_cache': 'SFTP remote file listing cache for performance optimization'
    }