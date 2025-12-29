"""
Report Handlers (Business Logic Layer)

Report endpoint handlers implementing the business logic layer for analytics
and reporting. Each handler class focuses on a specific report category with
minimal code duplication, following DRY principles and separation of concerns.

Author: Waqqas Hanafi
Copyright: © 2025 Calaveras County Health and Human Services Agency
"""

from typing import Optional
from fastapi import HTTPException

from .service import ReportService
from .filters import build_date_filter, apply_demographics_filter


class OverviewReports:
    """Handlers for overview/summary reports"""
    
    def __init__(self, service: ReportService):
        self.service = service
    
    def get_summary(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> dict:
        """Get summary statistics"""
        try:
            # Build date filters for referrals
            ref_date_filter, ref_params = build_date_filter('referrals', start_date, end_date)
            
            # Total referrals
            query = f"SELECT COUNT(*) FROM referrals WHERE 1=1{ref_date_filter}"
            result = self.service.execute_single(query, ref_params)
            total_referrals = result[0] if result else 0
            
            # Build date filters for cases
            case_date_filter, case_params = build_date_filter('cases', start_date, end_date)
            
            # Total cases
            query = f"SELECT COUNT(*) FROM cases WHERE 1=1{case_date_filter}"
            result = self.service.execute_single(query, case_params)
            total_cases = result[0] if result else 0
            
            # Total unique people
            query = f"SELECT COUNT(DISTINCT person_id) FROM cases WHERE 1=1{case_date_filter}"
            result = self.service.execute_single(query, case_params)
            total_people = result[0] if result else 0
            
            # Build date filters for assistance requests
            ar_date_filter, ar_params = build_date_filter('assistance_requests', start_date, end_date)
            
            # Total assistance requests
            query = f"SELECT COUNT(*) FROM assistance_requests WHERE 1=1{ar_date_filter}"
            result = self.service.execute_single(query, ar_params)
            total_assistance_requests = result[0] if result else 0
            
            return {
                "total_referrals": total_referrals or 0,
                "total_cases": total_cases or 0,
                "total_people": total_people or 0,
                "total_assistance_requests": total_assistance_requests or 0
            }
        except Exception as e:
            # Return zeros on error rather than failing
            return {
                "total_referrals": 0,
                "total_cases": 0,
                "total_people": 0,
                "total_assistance_requests": 0
            }
    
    def get_referral_status(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> dict:
        """Get referral status distribution"""
        try:
            date_filter, params = build_date_filter('referrals', start_date, end_date)
            query = f"""
                SELECT referral_status, COUNT(*) as count 
                FROM referrals 
                WHERE referral_status IS NOT NULL{date_filter}
                GROUP BY referral_status
                ORDER BY count DESC
            """
            results = self.service.execute_query(query, params)
            
            # Handle empty results
            if not results:
                return {"labels": [], "values": []}
            
            return self.service.format_chart_data(results)
        except Exception as e:
            # Return empty data on error
            return {"labels": [], "values": []}
    
    def get_case_status(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> dict:
        """Get case status distribution"""
        try:
            date_filter, params = build_date_filter('cases', start_date, end_date)
            query = f"""
                SELECT case_status, COUNT(*) as count 
                FROM cases 
                WHERE case_status IS NOT NULL{date_filter}
                GROUP BY case_status
                ORDER BY count DESC
            """
            results = self.service.execute_query(query, params)
            
            # Handle empty results
            if not results:
                return {"labels": [], "values": []}
            
            return self.service.format_chart_data(results)
        except Exception as e:
            return {"labels": [], "values": []}
    
    def get_service_types(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> dict:
        """Get service type distribution"""
        try:
            date_filter, params = build_date_filter('cases', start_date, end_date)
            query = f"""
                SELECT service_type, COUNT(*) as count 
                FROM cases 
                WHERE service_type IS NOT NULL{date_filter}
                GROUP BY service_type
                ORDER BY count DESC
                LIMIT 10
            """
            results = self.service.execute_query(query, params)
            
            # Handle empty results
            if not results:
                return {"labels": [], "values": []}
            
            return self.service.format_chart_data(results)
        except Exception as e:
            return {"labels": [], "values": []}


class ProviderReports:
    """Handlers for provider-related reports"""
    
    def __init__(self, service: ReportService):
        self.service = service
    
    def get_top_providers(
        self,
        provider_type: str,  # 'sending' or 'receiving'
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 10
    ) -> dict:
        """Get top sending or receiving providers"""
        try:
            column = f"{provider_type}_provider_name"
            date_filter, params = build_date_filter('referrals', start_date, end_date)
            params.append(limit)
            
            query = f"""
                SELECT {column}, COUNT(*) as count 
                FROM referrals 
                WHERE {column} IS NOT NULL{date_filter}
                GROUP BY {column}
                ORDER BY count DESC
                LIMIT ?
            """
            results = self.service.execute_query(query, params)
            
            # Handle empty results
            if not results:
                return {"labels": [], "values": []}
            
            return self.service.format_chart_data(results)
        except Exception as e:
            return {"labels": [], "values": []}
    
    def get_provider_collaboration(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> dict:
        """Get provider collaboration network"""
        try:
            date_filter, params = build_date_filter('referrals', start_date, end_date)
            query = f"""
                SELECT 
                    sending_provider_name,
                    receiving_provider_name,
                    COUNT(*) as referral_count
                FROM referrals
                WHERE sending_provider_name IS NOT NULL 
                    AND receiving_provider_name IS NOT NULL
                    AND sending_provider_name != receiving_provider_name{date_filter}
                GROUP BY sending_provider_name, receiving_provider_name
                HAVING referral_count >= 3
                ORDER BY referral_count DESC
                LIMIT 20
            """
            results = self.service.execute_query(query, params)
            
            # Handle empty results
            if not results:
                return {"collaborations": []}
            
            return {
                "collaborations": [
                    {"from": row[0], "to": row[1], "count": row[2]}
                    for row in results
                ]
            }
        except Exception as e:
            return {"collaborations": []}


class DemographicsReports:
    """Handlers for demographic reports"""
    
    def __init__(self, service: ReportService):
        self.service = service
    
    def get_age_distribution(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> dict:
        """Get age distribution of clients"""
        try:
            needs_join, where_clause, params = apply_demographics_filter(start_date, end_date)
            
            age_case = """
                CASE 
                    WHEN CAST((julianday('now') - julianday(date_of_birth)) / 365.25 AS INTEGER) < 18 THEN '0-17'
                    WHEN CAST((julianday('now') - julianday(date_of_birth)) / 365.25 AS INTEGER) BETWEEN 18 AND 24 THEN '18-24'
                    WHEN CAST((julianday('now') - julianday(date_of_birth)) / 365.25 AS INTEGER) BETWEEN 25 AND 34 THEN '25-34'
                    WHEN CAST((julianday('now') - julianday(date_of_birth)) / 365.25 AS INTEGER) BETWEEN 35 AND 44 THEN '35-44'
                    WHEN CAST((julianday('now') - julianday(date_of_birth)) / 365.25 AS INTEGER) BETWEEN 45 AND 54 THEN '45-54'
                    WHEN CAST((julianday('now') - julianday(date_of_birth)) / 365.25 AS INTEGER) BETWEEN 55 AND 64 THEN '55-64'
                    WHEN CAST((julianday('now') - julianday(date_of_birth)) / 365.25 AS INTEGER) >= 65 THEN '65+'
                    ELSE 'Unknown'
                END
            """
            
            age_sort = """
                CASE age_group
                    WHEN '0-17' THEN 1
                    WHEN '18-24' THEN 2
                    WHEN '25-34' THEN 3
                    WHEN '35-44' THEN 4
                    WHEN '45-54' THEN 5
                    WHEN '55-64' THEN 6
                    WHEN '65+' THEN 7
                    ELSE 8
                END
            """
            
            if needs_join:
                query = f"""
                    SELECT 
                        {age_case} as age_group,
                        COUNT(DISTINCT p.person_id) as count
                    FROM people p
                    INNER JOIN cases c ON p.person_id = c.person_id
                    WHERE p.date_of_birth IS NOT NULL AND {where_clause}
                    GROUP BY age_group
                    ORDER BY {age_sort}
                """
            else:
                query = f"""
                    SELECT 
                        {age_case} as age_group,
                        COUNT(*) as count
                    FROM people
                    WHERE date_of_birth IS NOT NULL
                    GROUP BY age_group
                    ORDER BY {age_sort}
                """
            
            results = self.service.execute_query(query, params)
            
            # Handle empty results
            if not results:
                return {"labels": [], "values": []}
            
            return self.service.format_chart_data(results)
        except Exception as e:
            return {"labels": [], "values": []}
    
    def get_gender_distribution(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> dict:
        """Get gender distribution"""
        try:
            needs_join, where_clause, params = apply_demographics_filter(start_date, end_date)
            
            if needs_join:
                query = f"""
                    SELECT 
                        COALESCE(p.gender, 'Not Specified') as gender,
                        COUNT(DISTINCT p.person_id) as count
                    FROM people p
                    INNER JOIN cases c ON p.person_id = c.person_id
                    WHERE {where_clause}
                    GROUP BY gender
                    ORDER BY count DESC
                """
            else:
                query = """
                    SELECT 
                        COALESCE(gender, 'Not Specified') as gender,
                        COUNT(*) as count
                    FROM people
                    GROUP BY gender
                    ORDER BY count DESC
                """
            
            results = self.service.execute_query(query, params)
            
            # Handle empty results
            if not results:
                return {"labels": [], "values": []}
            
            return self.service.format_chart_data(results)
        except Exception as e:
            return {"labels": [], "values": []}
    
    def get_race_ethnicity(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> dict:
        """Get race/ethnicity distribution"""
        try:
            needs_join, where_clause, params = apply_demographics_filter(start_date, end_date)
            
            if needs_join:
                query = f"""
                    SELECT 
                        COALESCE(p.race, 'Not Specified') as race,
                        COUNT(DISTINCT p.person_id) as count
                    FROM people p
                    INNER JOIN cases c ON p.person_id = c.person_id
                    WHERE p.race NOT IN ('undisclosed', '') AND {where_clause}
                    GROUP BY race
                    ORDER BY count DESC
                    LIMIT 10
                """
            else:
                query = """
                    SELECT 
                        COALESCE(race, 'Not Specified') as race,
                        COUNT(*) as count
                    FROM people
                    WHERE race NOT IN ('undisclosed', '')
                    GROUP BY race
                    ORDER BY count DESC
                    LIMIT 10
                """
            
            results = self.service.execute_query(query, params)
            
            # Handle empty results
            if not results:
                return {"labels": [], "values": []}
            
            return self.service.format_chart_data(results)
        except Exception as e:
            return {"labels": [], "values": []}

    def get_household_composition(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> dict:
        """Get household composition distribution (size, adults, children)"""
        try:
            needs_join, where_clause, params = apply_demographics_filter(start_date, end_date)
            
            if needs_join:
                query = f"""
                    SELECT 
                        CAST(COALESCE(p.household_size, 0) AS INTEGER) as household_size,
                        CAST(COALESCE(p.adults_in_household, 0) AS INTEGER) as adults,
                        CAST(COALESCE(p.children_in_household, 0) AS INTEGER) as children,
                        COUNT(DISTINCT p.person_id) as count
                    FROM people p
                    INNER JOIN cases c ON p.person_id = c.person_id
                    WHERE {where_clause}
                    GROUP BY household_size, adults, children
                    ORDER BY count DESC
                    LIMIT 10
                """
            else:
                query = """
                    SELECT 
                        CAST(COALESCE(household_size, 0) AS INTEGER) as household_size,
                        CAST(COALESCE(adults_in_household, 0) AS INTEGER) as adults,
                        CAST(COALESCE(children_in_household, 0) AS INTEGER) as children,
                        COUNT(*) as count
                    FROM people
                    GROUP BY household_size, adults, children
                    ORDER BY count DESC
                    LIMIT 10
                """
            
            results = self.service.execute_query(query, params)
            
            if not results:
                return {"labels": [], "values": []}
            
            # Format: "Size X (Y adults, Z children)" 
            labels = [f"Size {r['household_size']} ({r['adults']} adults, {r['children']} children)" for r in results]
            values = [r['count'] for r in results]
            
            return {"labels": labels, "values": values}
        except Exception as e:
            import logging
            logging.error(f"Error in get_household_composition: {e}")
            return {"labels": [], "values": []}

    def get_household_adults_children(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> dict:
        """Get scatterplot data of adults vs children per household"""
        try:
            needs_join, where_clause, params = apply_demographics_filter(start_date, end_date)
            
            if needs_join:
                query = f"""
                    SELECT 
                        CAST(COALESCE(p.adults_in_household, 0) AS INTEGER) as x,
                        CAST(COALESCE(p.children_in_household, 0) AS INTEGER) as y,
                        COUNT(DISTINCT p.person_id) as count
                    FROM people p
                    INNER JOIN cases c ON p.person_id = c.person_id
                    WHERE {where_clause}
                        AND (p.adults_in_household IS NOT NULL OR p.children_in_household IS NOT NULL)
                    GROUP BY x, y
                    ORDER BY count DESC
                """
            else:
                query = """
                    SELECT 
                        CAST(COALESCE(adults_in_household, 0) AS INTEGER) as x,
                        CAST(COALESCE(children_in_household, 0) AS INTEGER) as y,
                        COUNT(*) as count
                    FROM people
                    WHERE adults_in_household IS NOT NULL OR children_in_household IS NOT NULL
                    GROUP BY x, y
                    ORDER BY count DESC
                """
            
            results = self.service.execute_query(query, params)
            
            if not results:
                return {"data": []}
            
            # Format for Chart.js scatter: array of {x, y, count} objects
            data_points = [
                {
                    "x": r['x'],
                    "y": r['y'],
                    "count": r['count']
                }
                for r in results
            ]
            
            return {"data": data_points}
        except Exception as e:
            import logging
            logging.error(f"Error in get_household_adults_children: {e}")
            return {"data": []}

    def get_income_distribution(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> dict:
        """Get monthly income distribution in brackets"""
        try:
            needs_join, where_clause, params = apply_demographics_filter(start_date, end_date)
            
            if needs_join:
                query = f"""
                    SELECT 
                        CASE 
                            WHEN CAST(COALESCE(p.gross_monthly_income, 0) AS INTEGER) = 0 THEN 'No Income'
                            WHEN CAST(p.gross_monthly_income AS INTEGER) < 1000 THEN 'Under $1,000'
                            WHEN CAST(p.gross_monthly_income AS INTEGER) < 2000 THEN '$1,000-$1,999'
                            WHEN CAST(p.gross_monthly_income AS INTEGER) < 3000 THEN '$2,000-$2,999'
                            WHEN CAST(p.gross_monthly_income AS INTEGER) < 5000 THEN '$3,000-$4,999'
                            ELSE '$5,000+'
                        END as income_bracket,
                        COUNT(DISTINCT p.person_id) as count
                    FROM people p
                    INNER JOIN cases c ON p.person_id = c.person_id
                    WHERE {where_clause}
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
            else:
                query = """
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
            
            results = self.service.execute_query(query, params)
            
            if not results:
                return {"labels": [], "values": []}
            
            # Results are tuples: (income_bracket, count)
            return self.service.format_chart_data(results, label_index=0, value_index=1)
        except Exception as e:
            import logging
            logging.error(f"Error in get_income_distribution: {e}", exc_info=True)
            return {"labels": [], "values": []}

    def get_insurance_coverage(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> dict:
        """Get insurance coverage statistics (Medicaid, Medicare, None)"""
        try:
            needs_join, where_clause, params = apply_demographics_filter(start_date, end_date)
            
            if needs_join:
                query = f"""
                    SELECT 
                        CASE 
                            WHEN p.medicaid_id IS NOT NULL AND p.medicaid_id != '' AND p.medicare_id IS NOT NULL AND p.medicare_id != '' 
                                THEN 'Both Medicaid & Medicare'
                            WHEN p.medicaid_id IS NOT NULL AND p.medicaid_id != '' THEN 'Medicaid Only'
                            WHEN p.medicare_id IS NOT NULL AND p.medicare_id != '' THEN 'Medicare Only'
                            ELSE 'No Insurance Recorded'
                        END as coverage_type,
                        COUNT(DISTINCT p.person_id) as count
                    FROM people p
                    INNER JOIN cases c ON p.person_id = c.person_id
                    WHERE {where_clause}
                    GROUP BY coverage_type
                    ORDER BY count DESC
                """
            else:
                query = """
                    SELECT 
                        CASE 
                            WHEN medicaid_id IS NOT NULL AND medicaid_id != '' AND medicare_id IS NOT NULL AND medicare_id != '' 
                                THEN 'Both Medicaid & Medicare'
                            WHEN medicaid_id IS NOT NULL AND medicaid_id != '' THEN 'Medicaid Only'
                            WHEN medicare_id IS NOT NULL AND medicare_id != '' THEN 'Medicare Only'
                            ELSE 'No Insurance Recorded'
                        END as coverage_type,
                        COUNT(*) as count
                    FROM people
                    GROUP BY coverage_type
                    ORDER BY count DESC
                """
            
            results = self.service.execute_query(query, params)
            
            if not results:
                return {"labels": [], "values": []}
            
            # Results are tuples: (coverage_type, count)
            return self.service.format_chart_data(results, label_index=0, value_index=1)
        except Exception as e:
            import logging
            logging.error(f"Error in get_insurance_coverage: {e}", exc_info=True)
            return {"labels": [], "values": []}

    def get_communication_preferences(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> dict:
        """Get preferred communication methods"""
        try:
            needs_join, where_clause, params = apply_demographics_filter(start_date, end_date)
            
            if needs_join:
                query = f"""
                    SELECT 
                        CASE 
                            WHEN p.preferred_communication_method IS NULL OR p.preferred_communication_method = '' 
                            THEN 'Not Specified'
                            ELSE p.preferred_communication_method
                        END as method,
                        COUNT(DISTINCT p.person_id) as count
                    FROM people p
                    INNER JOIN cases c ON p.person_id = c.person_id
                    WHERE {where_clause}
                    GROUP BY method
                    ORDER BY count DESC
                """
            else:
                query = """
                    SELECT 
                        CASE 
                            WHEN preferred_communication_method IS NULL OR preferred_communication_method = '' 
                            THEN 'Not Specified'
                            ELSE preferred_communication_method
                        END as method,
                        COUNT(*) as count
                    FROM people
                    GROUP BY method
                    ORDER BY count DESC
                """
            
            results = self.service.execute_query(query, params)
            
            if not results:
                return {"labels": [], "values": []}
            
            # Results are tuples: (method, count)
            return self.service.format_chart_data(results, label_index=0, value_index=1)
        except Exception as e:
            import logging
            logging.error(f"Error in get_communication_preferences: {e}", exc_info=True)
            return {"labels": [], "values": []}

    def get_marital_status(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> dict:
        """Get marital status distribution"""
        try:
            needs_join, where_clause, params = apply_demographics_filter(start_date, end_date)
            
            if needs_join:
                query = f"""
                    SELECT 
                        CASE 
                            WHEN p.marital_status IS NULL OR p.marital_status = '' 
                            THEN 'Not Specified'
                            ELSE p.marital_status
                        END as status,
                        COUNT(DISTINCT p.person_id) as count
                    FROM people p
                    INNER JOIN cases c ON p.person_id = c.person_id
                    WHERE {where_clause}
                    GROUP BY status
                    ORDER BY count DESC
                """
            else:
                query = """
                    SELECT 
                        CASE 
                            WHEN marital_status IS NULL OR marital_status = '' 
                            THEN 'Not Specified'
                            ELSE marital_status
                        END as status,
                        COUNT(*) as count
                    FROM people
                    GROUP BY status
                    ORDER BY count DESC
                """
            
            results = self.service.execute_query(query, params)
            
            if not results:
                return {"labels": [], "values": []}
            
            # Results are tuples: (status, count)
            return self.service.format_chart_data(results, label_index=0, value_index=1)
        except Exception as e:
            import logging
            logging.error(f"Error in get_marital_status: {e}", exc_info=True)
            return {"labels": [], "values": []}

    def get_language_preferences(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> dict:
        """Get language preferences distribution"""
        try:
            needs_join, where_clause, params = apply_demographics_filter(start_date, end_date)
            
            if needs_join:
                query = f"""
                    SELECT 
                        CASE 
                            WHEN p.languages IS NULL OR p.languages = '' 
                            THEN 'Not Specified'
                            ELSE p.languages
                        END as language,
                        COUNT(DISTINCT p.person_id) as count
                    FROM people p
                    INNER JOIN cases c ON p.person_id = c.person_id
                    WHERE {where_clause}
                    GROUP BY language
                    ORDER BY count DESC
                    LIMIT 15
                """
            else:
                query = """
                    SELECT 
                        CASE 
                            WHEN languages IS NULL OR languages = '' 
                            THEN 'Not Specified'
                            ELSE languages
                        END as language,
                        COUNT(*) as count
                    FROM people
                    GROUP BY language
                    ORDER BY count DESC
                    LIMIT 15
                """
            
            results = self.service.execute_query(query, params)
            
            if not results:
                return {"labels": [], "values": []}
            
            # Results are tuples: (language, count)
            return self.service.format_chart_data(results, label_index=0, value_index=1)
        except Exception as e:
            import logging
            logging.error(f"Error in get_language_preferences: {e}", exc_info=True)
            return {"labels": [], "values": []}


class TimelineReports:
    """Handlers for timeline and trend reports"""
    
    def __init__(self, service: ReportService):
        self.service = service
    
    def get_referrals_timeline(
        self,
        grouping: str = "week",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> dict:
        """Get referrals over time"""
        try:
            date_formats = {"day": "%Y-%m-%d", "week": "%Y-W%W", "month": "%Y-%m"}
            date_format = date_formats.get(grouping, "%Y-W%W")
            
            # Build date filter
            conditions = ["referral_created_at IS NOT NULL"]
            params = []
            
            if start_date:
                conditions.append("referral_created_at >= ?")
                params.append(start_date)
            if end_date:
                conditions.append("referral_created_at <= ?")
                params.append(end_date)
            
            where_clause = " AND ".join(conditions)
            
            query = f"""
                SELECT strftime('{date_format}', referral_created_at) as period, COUNT(*) as count 
                FROM referrals 
                WHERE {where_clause}
                GROUP BY period
                ORDER BY period
                LIMIT 100
            """
            results = self.service.execute_query(query, params)
            
            # Handle empty results
            if not results:
                return {"labels": [], "values": []}
            
            return self.service.format_chart_data(results)
        except Exception as e:
            return {"labels": [], "values": []}
    
    def get_cases_over_time(
        self,
        grouping: str = "month",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> dict:
        """Get cases over time by status"""
        try:
            date_formats = {"day": "%Y-%m-%d", "week": "%Y-W%W", "month": "%Y-%m"}
            date_format = date_formats.get(grouping, "%Y-%m")
            
            conditions = ["case_created_at IS NOT NULL"]
            params = []
            
            if start_date:
                conditions.append("case_updated_at >= ?")
                params.append(start_date)
            if end_date:
                conditions.append("case_updated_at <= ?")
                params.append(end_date)
            
            where_clause = " AND ".join(conditions)
            
            query = f"""
                SELECT 
                    strftime('{date_format}', case_created_at) as period,
                    case_status,
                    COUNT(*) as count
                FROM cases
                WHERE {where_clause}
                GROUP BY period, case_status
                ORDER BY period
            """
            results = self.service.execute_query(query, params)
            
            # Handle empty results
            if not results:
                return {"labels": [], "datasets": []}
            
            # Organize data by status
            periods = {}
            statuses = set()
            
            for row in results:
                period, status, count = row
                if period not in periods:
                    periods[period] = {}
                periods[period][status] = count
                statuses.add(status)
            
            # Convert to chart format
            labels = sorted(periods.keys())
            datasets = [
                {
                    "label": status,
                    "data": [periods[period].get(status, 0) for period in labels]
                }
                for status in sorted(statuses)
            ]
            
            return {"labels": labels, "datasets": datasets}
        except Exception as e:
            return {"labels": [], "datasets": []}
