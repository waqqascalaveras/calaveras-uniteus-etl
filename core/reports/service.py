"""
Report Service (Data Access Layer)

Data access layer for report queries providing reusable query execution and
data formatting methods. Implements the repository pattern with connection
pooling and safe database operations.

Author: Waqqas Hanafi
Copyright: © 2025 Calaveras County Health and Human Services Agency
"""

from typing import List, Dict, Any, Optional, Tuple
from contextlib import contextmanager


class ReportService:
    """Base service for executing report queries"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    @contextmanager
    def get_connection(self):
        """Get database connection from pool"""
        with self.db_manager.pool.get_connection() as conn:
            yield conn
    
    def execute_query(self, query: str, params: List[Any] = None) -> List[Tuple]:
        """
        Execute a query and return results.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            List of result tuples
        """
        params = params or []
        with self.get_connection() as conn:
            cursor = conn.execute(query, params)
            return cursor.fetchall()
    
    def execute_single(self, query: str, params: List[Any] = None) -> Optional[Tuple]:
        """Execute query and return single result"""
        params = params or []
        with self.get_connection() as conn:
            cursor = conn.execute(query, params)
            return cursor.fetchone()
    
    def format_chart_data(self, results: List[Tuple], label_index: int = 0, value_index: int = 1) -> Dict[str, List]:
        """
        Format query results as chart data.
        
        Args:
            results: Query results
            label_index: Index of label column
            value_index: Index of value column
            
        Returns:
            Dictionary with 'labels' and 'values' keys
        """
        return {
            "labels": [row[label_index] or 'Unknown' for row in results],
            "values": [row[value_index] for row in results]
        }
    
    def format_table_data(self, results: List[Tuple], columns: List[str]) -> List[Dict[str, Any]]:
        """
        Format query results as list of dictionaries.
        
        Args:
            results: Query results
            columns: Column names
            
        Returns:
            List of dictionaries with column names as keys
        """
        return [
            {col: row[i] for i, col in enumerate(columns)}
            for row in results
        ]
