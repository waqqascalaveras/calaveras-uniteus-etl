"""
Reports Module

Reports module providing comprehensive analytics and reporting endpoints
for the UniteUs ETL application. Implements a modular, layered architecture
with separation of concerns.

Author: Waqqas Hanafi
Copyright: © 2025 Calaveras County Health and Human Services Agency
"""

from .router import router as reports_router
from .filters import build_date_filter, build_report_where_clause
from .models import ReportFilters, ReportResponse, FilterOptions

__all__ = [
    "reports_router",
    "ReportFilters",
    "build_date_filter",
    "build_report_where_clause",
    "ReportResponse",
    "FilterOptions"
]
