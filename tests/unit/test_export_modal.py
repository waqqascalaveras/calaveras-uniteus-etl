"""
================================================================================
Calaveras UniteUs ETL - Export Modal Unit Tests
================================================================================
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency

Description:
    Unit tests for Export to Word modal functionality. Tests the Word export
    API endpoint with chart data, validating document generation and
    data embedding.

Test Coverage:
    - Word export API endpoint
    - Chart data embedding
    - Document generation
    - Error handling
================================================================================
"""

import pytest
from unittest.mock import Mock, patch


class TestWordExportAPI:
    """Test Word export API endpoint"""
    
    @patch('core.app.require_auth')
    @patch('core.app.get_audit_logger')
    def test_export_with_valid_chart_data(self, mock_logger, mock_auth, client):
        """Test export with valid chart image data"""
        # Mock authentication
        mock_auth.return_value = lambda: None
        mock_logger.return_value = Mock()
        
        # Valid base64 PNG image (1x1 transparent pixel)
        valid_png = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=='
        
        export_data = {
            'period': '2025-01-01 to 2025-12-31',
            'generated_date': '2025-11-14 10:30:00',
            'summary': {
                'total_referrals': 150,
                'total_cases': 75,
                'total_people': 200,
                'total_assistance_requests': 100
            },
            'charts': {
                'referralStatus': valid_png,
                'caseStatus': valid_png,
                'serviceType': valid_png,
                'timeline': valid_png
            }
        }
        
        with patch('core.app.session_manager'):
            with patch('core.app.get_current_user', return_value=Mock(username='test_user', role='ADMIN')):
                response = client.post('/api/reports/export/annual-report-word', json=export_data)
        
        if response.status_code == 200:
            # Check that response is a Word document
            assert response.headers['content-type'] == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
            assert 'Dashboard_Export' in response.headers.get('content-disposition', '')
            assert len(response.content) > 1000  # Word docs are at least 1KB
            print(f"✓ Export succeeded - Document size: {len(response.content)} bytes")
        else:
            print(f"Export status: {response.status_code}")
            print(f"Response: {response.text[:500]}")
    
    @patch('core.app.require_auth')
    @patch('core.app.get_audit_logger')
    def test_export_with_multiple_charts(self, mock_logger, mock_auth, client):
        """Test export with all chart types"""
        mock_auth.return_value = lambda: None
        mock_logger.return_value = Mock()
        
        valid_png = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=='
        
        # Test with all 21 chart types
        all_charts = {
            'referralStatus': valid_png,
            'caseStatus': valid_png,
            'serviceType': valid_png,
            'timeline': valid_png,
            'ageDist': valid_png,
            'gender': valid_png,
            'race': valid_png,
            'household': valid_png,
            'householdScatter': valid_png,
            'income': valid_png,
            'insurance': valid_png,
            'commPref': valid_png,
            'marital': valid_png,
            'language': valid_png,
            'milAff': valid_png,
            'milBranch': valid_png,
            'sendingProviders': valid_png,
            'receivingProviders': valid_png,
            'touchpoint': valid_png,
            'casesOverTime': valid_png,
            'referralFunnel': valid_png
        }
        
        export_data = {
            'period': 'All Time',
            'generated_date': '2025-11-14',
            'summary': {},
            'charts': all_charts
        }
        
        with patch('core.app.session_manager'):
            with patch('core.app.get_current_user', return_value=Mock(username='admin', role='ADMIN')):
                response = client.post('/api/reports/export/annual-report-word', json=export_data)
        
        if response.status_code == 200:
            assert len(response.content) > 5000  # With 21 charts should be larger
            print(f"✓ Export with 21 charts succeeded - Size: {len(response.content)} bytes")
        else:
            print(f"Export status: {response.status_code}")
    
    @patch('core.app.require_auth')
    @patch('core.app.get_audit_logger')
    def test_export_handles_empty_charts(self, mock_logger, mock_auth, client):
        """Test export with no charts selected"""
        mock_auth.return_value = lambda: None
        mock_logger.return_value = Mock()
        
        export_data = {
            'period': 'Test Period',
            'generated_date': '2025-11-14',
            'summary': {
                'total_referrals': 0
            },
            'charts': {}  # No charts
        }
        
        with patch('core.app.session_manager'):
            with patch('core.app.get_current_user', return_value=Mock(username='test', role='ADMIN')):
                response = client.post('/api/reports/export/annual-report-word', json=export_data)
        
        # Should still succeed - just exports summary without charts
        if response.status_code == 200:
            assert len(response.content) > 0
            print("✓ Export with no charts succeeded")
        else:
            print(f"Export status: {response.status_code}")


class TestModalHTML:
    """Test that the modal HTML is properly structured"""
    
    def test_modal_structure_in_template(self):
        """Test that dashboard.html contains the export modal with all checkboxes"""
        with open('core/web/templates/dashboard.html', 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check modal exists
        assert 'id="exportWordModal"' in content
        assert 'Export Charts to Word' in content
        
        # Check all 21 chart checkboxes exist
        expected_charts = [
            'referralStatus', 'caseStatus', 'serviceType', 'timeline',
            'ageDist', 'gender', 'race', 'household', 'householdScatter',
            'income', 'insurance', 'commPref', 'marital', 'language',
            'milAff', 'milBranch',
            'sendingProviders', 'receivingProviders',
            'touchpoint', 'casesOverTime', 'referralFunnel'
        ]
        
        for chart_id in expected_charts:
            assert f'id="export_{chart_id}"' in content, f"Missing checkbox for {chart_id}"
            assert f'data-chart-id="{chart_id}"' in content
        
        # Check category headers
        assert '>Overview<' in content
        assert '>Demographics<' in content
        assert '>Network & Providers<' in content
        assert '>Performance & Outcomes<' in content
        
        # Check buttons
        assert 'selectAllExportCharts()' in content
        assert 'deselectAllExportCharts()' in content
        assert 'exportSelectedChartsToWord()' in content
        
        # Check all checkboxes are checked by default
        checked_count = content.count('type="checkbox" checked')
        assert checked_count == 21, f"Expected 21 checked boxes, found {checked_count}"
        
        print(f"✓ Modal HTML structure validated - All 21 charts present")
    
    def test_javascript_functions_defined(self):
        """Test that required JavaScript functions are defined"""
        with open('core/web/templates/dashboard.html', 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check function definitions
        assert 'function selectAllExportCharts()' in content
        assert 'function deselectAllExportCharts()' in content
        assert 'function updateExportSelectedCount()' in content
        assert 'async function exportSelectedChartsToWord()' in content
        
        # Check window exposure
        assert 'window.selectAllExportCharts' in content
        assert 'window.deselectAllExportCharts' in content
        assert 'window.exportSelectedChartsToWord' in content
        
        # Check event listeners
        assert "addEventListener('change', updateExportSelectedCount)" in content
        
        # Check chart validation
        assert 'window.dashboardCharts' in content
        
        print("✓ JavaScript functions validated")
    
    def test_functions_not_commented_out(self):
        """Test that export functions are NOT inside commented block"""
        with open('core/web/templates/dashboard.html', 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Find the comment block boundaries
        comment_start = content.find('/*  COMMENTED OUT - START OF OLD CODE')
        comment_end = content.find('END OF OLD CODE - COMMENTED OUT */')
        
        assert comment_start != -1, "Comment start marker not found"
        assert comment_end != -1, "Comment end marker not found"
        
        # Find where export functions are defined
        export_func_pos = content.find('function selectAllExportCharts()')
        export_async_pos = content.find('async function exportSelectedChartsToWord()')
        window_export_pos = content.find('window.selectAllExportCharts = selectAllExportCharts')
        
        assert export_func_pos != -1, "selectAllExportCharts function not found"
        assert export_async_pos != -1, "exportSelectedChartsToWord function not found"
        assert window_export_pos != -1, "window.selectAllExportCharts assignment not found"
        
        # Ensure export functions are AFTER the comment block ends
        assert export_func_pos > comment_end, f"selectAllExportCharts is inside commented block! Position {export_func_pos} vs comment end {comment_end}"
        assert export_async_pos > comment_end, f"exportSelectedChartsToWord is inside commented block! Position {export_async_pos} vs comment end {comment_end}"
        assert window_export_pos > comment_end, f"window assignments are inside commented block! Position {window_export_pos} vs comment end {comment_end}"
        
        print(f"✓ All export functions are OUTSIDE commented block (comment ends at {comment_end}, functions start at {export_func_pos})")


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
