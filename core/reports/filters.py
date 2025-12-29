"""
Report Filters

Filtering utilities for reports providing reusable functions for building SQL
WHERE clauses and filters. Implements safe parameterized queries to prevent
SQL injection while enabling flexible date and demographic filtering.

Author: Waqqas Hanafi
Copyright: © 2025 Calaveras County Health and Human Services Agency
"""

from typing import Optional, Tuple, List, Dict, Any


# Map table names to their primary date columns
DATE_COLUMN_MAP = {
    'referrals': 'referral_updated_at',
    'cases': 'case_updated_at',
    'assistance_requests': 'updated_at',
    'people': 'case_updated_at',  # Via JOIN with cases
}


def build_date_filter(
    table: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Tuple[str, List[Any]]:
    """
    Build WHERE clause fragment and params for date filtering.
    
    Args:
        table: Table name to determine the date column
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        
    Returns:
        Tuple of (where_clause_fragment, params_list)
        Example: (" AND created_at >= ? AND created_at <= ?", ["2024-01-01", "2024-12-31"])
    """
    where_conditions = []
    params = []
    
    date_col = DATE_COLUMN_MAP.get(table, 'created_at')
    
    if start_date:
        where_conditions.append(f"{date_col} >= ?")
        params.append(start_date)
    if end_date:
        where_conditions.append(f"{date_col} <= ?")
        params.append(end_date)
    
    where_clause = f" AND {' AND '.join(where_conditions)}" if where_conditions else ""
    return where_clause, params


def build_report_where_clause(
    table: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    status: Optional[str] = None,
    service_type: Optional[str] = None,
    provider: Optional[str] = None,
    program: Optional[str] = None,
    gender: Optional[str] = None,
    race: Optional[str] = None,
    base_conditions: Optional[List[str]] = None
) -> Tuple[str, List[Any]]:
    """
    Build complete WHERE clause for report queries with all filter types.
    
    Args:
        table: Table name for date column determination
        start_date: Start date filter
        end_date: End date filter
        status: Status filter (case_status or referral_status depending on table)
        service_type: Service type filter
        provider: Provider filter
        program: Program filter
        gender: Gender filter (requires JOIN with people table)
        race: Race filter (requires JOIN with people table)
        base_conditions: List of base WHERE conditions (e.g., ["column IS NOT NULL"])
        
    Returns:
        Tuple of (complete_where_clause, params_list)
    """
    conditions = base_conditions or []
    params = []
    
    # Date filtering
    date_filter, date_params = build_date_filter(table, start_date, end_date)
    if date_filter:
        # Remove leading " AND " from date_filter
        date_conditions = date_filter.replace(" AND ", "", 1).split(" AND ")
        conditions.extend(date_conditions)
        params.extend(date_params)
    
    # Status filtering
    if status:
        if table == 'referrals':
            conditions.append("referral_status = ?")
        elif table == 'cases':
            conditions.append("case_status = ?")
        params.append(status)
    
    # Service type filtering
    if service_type:
        conditions.append("service_type = ?")
        params.append(service_type)
    
    # Provider filtering
    if provider:
        if table == 'referrals':
            conditions.append("(sending_provider_name = ? OR receiving_provider_name = ?)")
            params.extend([provider, provider])
        elif table == 'cases':
            conditions.append("provider_name = ?")
            params.append(provider)
    
    # Program filtering
    if program:
        if table == 'referrals':
            conditions.append("receiving_program_name = ?")
        elif table == 'cases':
            conditions.append("program_name = ?")
        params.append(program)
    
    # Note: Gender and race filters require JOINs and are handled in specific endpoints
    
    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    return where_clause, params


def apply_demographics_filter(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Tuple[bool, str, List[Any]]:
    """
    Determine if demographics queries need JOIN with cases for date filtering.
    
    Returns:
        Tuple of (needs_join, where_clause, params)
    """
    if not (start_date or end_date):
        return False, "", []
    
    conditions = []
    params = []
    
    if start_date:
        conditions.append("c.case_updated_at >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("c.case_updated_at <= ?")
        params.append(end_date)
    
    where_clause = " AND ".join(conditions)
    return True, where_clause, params


def build_query_with_filters(
    base_query: str,
    table: str,
    filters: Dict[str, Optional[str]],
    base_conditions: Optional[List[str]] = None
) -> Tuple[str, List[Any]]:
    """
    High-level function to inject filters into a base query.
    
    Args:
        base_query: SQL query with {where_clause} placeholder
        table: Table name for filter building
        filters: Dictionary of filter values
        base_conditions: Base WHERE conditions
        
    Returns:
        Tuple of (complete_query, params)
    """
    where_clause, params = build_report_where_clause(
        table=table,
        start_date=filters.get('start_date'),
        end_date=filters.get('end_date'),
        status=filters.get('status'),
        service_type=filters.get('service_type'),
        provider=filters.get('provider'),
        program=filters.get('program'),
        gender=filters.get('gender'),
        race=filters.get('race'),
        base_conditions=base_conditions
    )
    
    query = base_query.format(where_clause=where_clause)
    return query, params
