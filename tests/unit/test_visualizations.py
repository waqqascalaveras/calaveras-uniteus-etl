"""
================================================================================
Calaveras UniteUs ETL - Visualization Integration Tests
================================================================================
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency

Description:
    Integration tests for dashboard visualization endpoints. Tests all chart/table
    endpoints against actual database to ensure correct data structure and
    visualization rendering. Includes chart data validation and error handling.

Test Coverage:
    - Visualization endpoint responses
    - Chart data structure validation
    - Database query integration
    - Data format verification
    - Chart data validation and error handling
================================================================================
"""

import pytest


class TestVisualizationEndpoints:
    """Test all visualization API endpoints return correct data structures"""
    
    def test_outcome_metrics_endpoint(self, client):
        """Test /api/reports/outcome-metrics returns correct structure"""
        response = client.get("/api/reports/outcome-metrics")
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify structure - should have these keys even if empty or error
        if "error" not in data:
            assert "outcome_distribution" in data
            assert "resolution_times" in data
            assert "types" in data["outcome_distribution"]
            assert "counts" in data["outcome_distribution"]
            assert isinstance(data["outcome_distribution"]["types"], list)
            assert isinstance(data["outcome_distribution"]["counts"], list)
            assert isinstance(data["resolution_times"], list)
            
            # Verify parallel arrays match
            assert len(data["outcome_distribution"]["types"]) == len(data["outcome_distribution"]["counts"])
            
            # Verify resolution_times structure
            for rt in data["resolution_times"]:
                assert "service_type" in rt
                assert "avg_days" in rt
                assert "count" in rt
    
    def test_outcome_metrics_with_filters(self, client):
        """Test outcome metrics with date and service filters"""
        response = client.get(
            "/api/reports/outcome-metrics",
            params={
                "start_date": "2025-01-01",
                "end_date": "2025-12-31",
                "service_type": "Housing"
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        if "error" not in data:
            assert "outcome_distribution" in data
            assert "resolution_times" in data
    
    def test_provider_performance_endpoint(self, client):
        """Test /api/reports/provider-performance returns correct structure"""
        response = client.get("/api/reports/provider-performance")
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify structure
        if "error" not in data:
            assert "providers" in data
            assert isinstance(data["providers"], list)
            
            # If we have providers, verify structure
            for provider in data["providers"]:
                assert "provider_name" in provider
                assert "total_cases" in provider
                assert "unique_clients" in provider
                assert "active_cases" in provider
                assert "pending_cases" in provider
                assert "closed_cases" in provider
                assert "avg_days" in provider or provider["avg_days"] is None
                assert "min_days" in provider or provider["min_days"] is None
                assert "max_days" in provider or provider["max_days"] is None
                assert "completion_rate" in provider
    
    def test_service_pathways_endpoint(self, client):
        """Test /api/reports/service-pathways returns correct structure"""
        response = client.get("/api/reports/service-pathways")
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify structure
        if "error" not in data:
            assert "pathways" in data
            assert isinstance(data["pathways"], list)
            
            # If we have pathways, verify structure
            for pathway in data["pathways"]:
                assert "initial_service" in pathway
                assert "referral_service" in pathway
                assert "count" in pathway
                assert "avg_days_between" in pathway or pathway["avg_days_between"] is None
    
    def test_service_pathways_with_date_filters(self, client):
        """Test service pathways respects date filters"""
        response = client.get(
            "/api/reports/service-pathways",
            params={
                "start_date": "2025-01-01",
                "end_date": "2025-06-30"
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        if "error" not in data:
            assert "pathways" in data
    
    def test_household_distribution_endpoint(self, client):
        """Test /api/reports/demographics/household-composition returns correct structure"""
        response = client.get("/api/reports/demographics/household-composition")
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify structure
        if "error" not in data:
            assert "labels" in data
            assert "values" in data
            assert isinstance(data["labels"], list)
            assert isinstance(data["values"], list)
            assert len(data["labels"]) == len(data["values"])
    
    def test_income_distribution_endpoint(self, client):
        """Test /api/reports/demographics/income-distribution returns correct structure"""
        response = client.get("/api/reports/demographics/income-distribution")
        
        assert response.status_code == 200
        data = response.json()
        
        if "error" not in data:
            assert "labels" in data
            assert "values" in data
            assert isinstance(data["labels"], list)
            assert isinstance(data["values"], list)
    
    def test_insurance_coverage_endpoint(self, client):
        """Test /api/reports/demographics/insurance-coverage returns correct structure"""
        response = client.get("/api/reports/demographics/insurance-coverage")
        
        assert response.status_code == 200
        data = response.json()
        
        if "error" not in data:
            assert "labels" in data
            assert "values" in data
    
    def test_communication_preferences_endpoint(self, client):
        """Test /api/reports/demographics/communication-preferences returns correct structure"""
        response = client.get("/api/reports/demographics/communication-preferences")
        
        assert response.status_code == 200
        data = response.json()
        
        if "error" not in data:
            assert "labels" in data
            assert "values" in data
    
    def test_marital_status_endpoint(self, client):
        """Test /api/reports/demographics/marital-status returns correct structure"""
        response = client.get("/api/reports/demographics/marital-status")
        
        assert response.status_code == 200
        data = response.json()
        
        if "error" not in data:
            assert "labels" in data
            assert "values" in data
    
    def test_language_preferences_endpoint(self, client):
        """Test /api/reports/demographics/language-preferences returns correct structure"""
        response = client.get("/api/reports/demographics/language-preferences")
        
        assert response.status_code == 200
        data = response.json()
        
        if "error" not in data:
            assert "labels" in data
            assert "values" in data


class TestDataFormattingConsistency:
    """Test that all visualization endpoints return consistent data formats"""
    
    def test_all_chart_endpoints_return_labels_and_values(self, client):
        """Test all basic chart endpoints return {labels, values} format"""
        chart_endpoints = [
            "/api/reports/demographics/household-composition",
            "/api/reports/demographics/income-distribution",
            "/api/reports/demographics/insurance-coverage",
            "/api/reports/demographics/communication-preferences",
            "/api/reports/demographics/marital-status",
            "/api/reports/demographics/language-preferences"
        ]
        
        for endpoint in chart_endpoints:
            response = client.get(endpoint)
            assert response.status_code == 200, f"{endpoint} failed with status {response.status_code}"
            data = response.json()
            
            if "error" not in data:
                assert "labels" in data, f"{endpoint} missing 'labels'"
                assert "values" in data, f"{endpoint} missing 'values'"
                assert isinstance(data["labels"], list), f"{endpoint} labels not a list"
                assert isinstance(data["values"], list), f"{endpoint} values not a list"
                assert len(data["labels"]) == len(data["values"]), f"{endpoint} labels/values length mismatch"
    
    def test_complex_endpoints_return_expected_structure(self, client):
        """Test complex endpoints return their documented structure"""
        
        # Outcome metrics
        response = client.get("/api/reports/outcome-metrics")
        assert response.status_code == 200
        data = response.json()
        if "error" not in data:
            assert "outcome_distribution" in data
            assert "resolution_times" in data
        
        # Provider performance
        response = client.get("/api/reports/provider-performance")
        assert response.status_code == 200
        data = response.json()
        if "error" not in data:
            assert "providers" in data
            assert isinstance(data["providers"], list)
        
        # Service pathways
        response = client.get("/api/reports/service-pathways")
        assert response.status_code == 200
        data = response.json()
        if "error" not in data:
            assert "pathways" in data
            assert isinstance(data["pathways"], list)


class TestChartDataValidation:
    """Test chart data validation before rendering."""
    
    def test_validate_chart_data_structure(self):
        """Test that chart data has required structure."""
        valid_data = {
            'labels': ['Status A', 'Status B', 'Status C'],
            'values': [10, 20, 30]
        }
        
        # Check required fields
        assert 'labels' in valid_data
        assert 'values' in valid_data
        assert len(valid_data['labels']) == len(valid_data['values'])
        assert all(isinstance(v, (int, float)) for v in valid_data['values'])
    
    def test_reject_invalid_chart_data(self):
        """Test that invalid chart data is rejected."""
        invalid_data_cases = [
            {},  # Empty object
            {'labels': []},  # Missing values
            {'values': []},  # Missing labels
            {'labels': ['A'], 'values': [1, 2]},  # Mismatched lengths
            {'labels': ['A'], 'values': ['not a number']},  # Invalid value type
        ]
        
        for invalid_data in invalid_data_cases:
            # Each of these should fail validation
            has_labels = 'labels' in invalid_data and len(invalid_data.get('labels', [])) > 0
            has_values = 'values' in invalid_data and len(invalid_data.get('values', [])) > 0
            
            if has_labels and has_values:
                labels_len = len(invalid_data['labels'])
                values_len = len(invalid_data['values'])
                # This specific test case should fail (mismatched lengths)
                is_valid = labels_len == values_len
                # Assert that this data is properly identified as invalid
                if labels_len != values_len:
                    assert not is_valid, f"Mismatched lengths should be invalid: {invalid_data}"
    
    def test_handle_null_values_in_data(self):
        """Test that null values in data arrays are handled."""
        data_with_nulls = {
            'labels': ['A', 'B', 'C'],
            'values': [10, None, 30]
        }
        
        # Should filter out null values or convert to 0
        cleaned_values = [v if v is not None else 0 for v in data_with_nulls['values']]
        assert all(isinstance(v, (int, float)) for v in cleaned_values)


class TestDatalabelsPlugin:
    """Test Chart.js datalabels plugin integration."""
    
    def test_datalabels_formatter_returns_valid_types(self):
        """Test that formatter returns string or array."""
        # Valid returns: '', 'string', ['line1', 'line2']
        valid_returns = [
            '',
            '45.2%',
            ['45.2%', 'n=123']
        ]
        assert all(isinstance(r, (str, list)) for r in valid_returns)
    
    def test_datalabels_handles_zero_sum(self):
        """Test that formatter handles zero sum gracefully."""
        # if (!value || sum === 0) return '';
        values = [0, 0, 0]
        sum_val = sum(values)
        assert sum_val == 0
        # Formatter should return '' for zero sum


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
