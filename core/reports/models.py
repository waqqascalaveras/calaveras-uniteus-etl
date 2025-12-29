"""
Report Models

Pydantic models for report API request/response validation and documentation.
Defines data structures for report filters, parameters, and response schemas.

Author: Waqqas Hanafi
Copyright: © 2025 Calaveras County Health and Human Services Agency
"""

from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class ReportFilters(BaseModel):
    """Base model for report filters"""
    start_date: Optional[str] = Field(None, description="Start date for filtering (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="End date for filtering (YYYY-MM-DD)")
    status: Optional[str] = Field(None, description="Status filter")
    service_type: Optional[str] = Field(None, description="Service type filter")
    provider: Optional[str] = Field(None, description="Provider filter")
    program: Optional[str] = Field(None, description="Program filter")
    gender: Optional[str] = Field(None, description="Gender filter")
    race: Optional[str] = Field(None, description="Race/ethnicity filter")


class ReportResponse(BaseModel):
    """Base response model for reports"""
    data: Any
    filters_applied: Optional[Dict[str, Any]] = None


class FilterOptions(BaseModel):
    """Available filter options"""
    date_range: Dict[str, Optional[str]]
    case_statuses: List[str]
    service_types: List[str]
    service_subtypes: List[str]
    providers: List[str]
    programs: List[str]
    genders: List[str]
    races: List[str]
    referral_statuses: List[str]


class ChartData(BaseModel):
    """Chart data response model"""
    labels: List[str]
    values: List[int]


class TimeSeriesData(BaseModel):
    """Time series data response model"""
    labels: List[str]
    datasets: List[Dict[str, Any]]
