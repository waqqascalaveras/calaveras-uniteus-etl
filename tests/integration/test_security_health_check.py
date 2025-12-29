"""
================================================================================
Calaveras UniteUs ETL - Security Health Check Integration Tests
================================================================================
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency

Description:
    Integration tests for Security Health Check module, testing security
    configuration validation and compliance checking. Validates authentication
    settings, PHI hashing status, and security best practices.

Test Coverage:
    - Security configuration validation
    - Authentication settings verification
    - PHI hashing status checks
    - Compliance validation
    - Security best practices assessment
================================================================================
"""

from unittest.mock import Mock, patch, MagicMock
import pytest

from core.security_health_check import SecurityHealthChecker


class TestSecurityHealthChecker:
    """Test SecurityHealthChecker class"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.checker = SecurityHealthChecker()
    
    def test_check_https_disabled(self):
        """Test HTTPS check when disabled"""
        with patch('core.security_health_check.config') as mock_config:
            mock_config.web.use_https = False
            
            result = self.checker.check_https()
            
            assert result['status'] == 'fail'
            assert 'not enabled' in result['message'].lower()
    
    def test_check_https_enabled(self):
        """Test HTTPS check when enabled"""
        with patch('core.security_health_check.config') as mock_config:
            with patch('core.security_health_check.Path') as mock_path:
                mock_config.web.use_https = True
                mock_config.web.ssl_certfile = "cert.pem"
                mock_config.web.ssl_keyfile = "key.pem"
                
                mock_path_instance = MagicMock()
                mock_path_instance.exists.return_value = True
                mock_path.return_value = mock_path_instance
                
                result = self.checker.check_https()
                
                assert result['status'] == 'pass'
    
    def test_check_default_password_changed(self):
        """Test default password check when changed"""
        with patch.object(self.checker.auth_service.local_db, 'authenticate') as mock_auth:
            mock_auth.return_value = None  # Password doesn't work
            
            result = self.checker.check_default_password()
            
            assert result['status'] == 'pass'
    
    def test_check_default_password_unchanged(self):
        """Test default password check when still default"""
        with patch.object(self.checker.auth_service.local_db, 'authenticate') as mock_auth:
            mock_auth.return_value = {'username': 'admin'}  # Password works
            
            result = self.checker.check_default_password()
            
            assert result['status'] == 'fail'
            assert 'default' in result['message'].lower()
    
    def test_check_phi_hash_salt_valid(self):
        """Test PHI hash salt check with valid 64-char hex"""
        with patch('core.security_health_check.config') as mock_config:
            mock_config.security.phi_hash_salt = "a" * 64  # Valid 64-char hex
            
            result = self.checker.check_phi_hash_salt()
            
            assert result['status'] == 'pass'
    
    def test_check_phi_hash_salt_invalid_length(self):
        """Test PHI hash salt check with invalid length"""
        with patch('core.security_health_check.config') as mock_config:
            mock_config.security.phi_hash_salt = "a" * 32  # Wrong length
            
            result = self.checker.check_phi_hash_salt()
            
            assert result['status'] == 'fail'
            assert 'length' in result['message'].lower()
    
    def test_check_phi_hash_salt_not_hex(self):
        """Test PHI hash salt check with non-hex characters"""
        with patch('core.security_health_check.config') as mock_config:
            mock_config.security.phi_hash_salt = "z" * 64  # Not valid hex
            
            result = self.checker.check_phi_hash_salt()
            
            assert result['status'] == 'fail'
            assert 'hexadecimal' in result['message'].lower()
    
    def test_check_csrf_protection(self):
        """Test CSRF protection check"""
        result = self.checker.check_csrf_protection()
        
        # Currently should fail as CSRF isn't implemented
        assert result['status'] == 'fail'
    
    def test_check_session_security(self):
        """Test session security check"""
        result = self.checker.check_session_security()
        
        assert 'status' in result
        assert 'message' in result
    
    def test_calculate_security_score(self):
        """Test security score calculation"""
        checks = {
            'https_enabled': {'status': 'pass'},
            'default_password': {'status': 'pass'},
            'phi_hash_salt': {'status': 'pass'},
            'csrf_protection': {'status': 'fail'},
            'session_security': {'status': 'pass'},
            'audit_logging': {'status': 'pass'},
            'password_policy': {'status': 'warning'},
            'ip_restrictions': {'status': 'pass'}
        }
        
        score_data = self.checker.calculate_security_score(checks)
        
        assert 'score' in score_data
        assert 'rating' in score_data
        assert 'passed' in score_data
        assert 'failed' in score_data
        assert score_data['score'] >= 0
        assert score_data['score'] <= 100
    
    def test_calculate_security_score_all_pass(self):
        """Test security score when all checks pass"""
        checks = {key: {'status': 'pass'} for key in [
            'https_enabled', 'default_password', 'phi_hash_salt',
            'csrf_protection', 'session_security', 'audit_logging',
            'password_policy', 'ip_restrictions'
        ]}
        
        score_data = self.checker.calculate_security_score(checks)
        
        assert score_data['score'] == 100
        assert score_data['failed'] == 0
        assert score_data['passed'] == 8
    
    def test_check_hipaa_compliance(self):
        """Test HIPAA compliance checking"""
        checks = {
            'https_enabled': {'status': 'pass'},
            'default_password': {'status': 'pass'},
            'csrf_protection': {'status': 'pass'},
            'audit_logging': {'status': 'pass'},
            'password_policy': {'status': 'pass'}
        }
        
        compliance = self.checker.check_hipaa_compliance(checks)
        
        assert isinstance(compliance, list)
        assert len(compliance) > 0
        
        for item in compliance:
            assert 'requirement' in item
            assert 'description' in item
            assert 'compliant' in item
            assert isinstance(item['compliant'], bool)
    
    def test_generate_recommendations_critical(self):
        """Test generating recommendations for critical issues"""
        checks = {
            'https_enabled': {'status': 'fail'},
            'default_password': {'status': 'fail'},
            'phi_hash_salt': {'status': 'pass'},
            'csrf_protection': {'status': 'pass'},
            'session_security': {'status': 'pass'},
            'audit_logging': {'status': 'pass'},
            'password_policy': {'status': 'pass'},
            'ip_restrictions': {'status': 'pass'}
        }
        
        recommendations = self.checker.generate_recommendations(checks)
        
        assert isinstance(recommendations, list)
        # Should have recommendations for failed checks
        assert len(recommendations) > 0
        
        # Check for critical priority recommendations
        critical_recs = [r for r in recommendations if r['priority'] == 'critical']
        assert len(critical_recs) > 0
    
    def test_generate_recommendations_all_pass(self):
        """Test generating recommendations when all checks pass"""
        checks = {key: {'status': 'pass'} for key in [
            'https_enabled', 'default_password', 'phi_hash_salt',
            'csrf_protection', 'session_security', 'audit_logging',
            'password_policy', 'ip_restrictions'
        ]}
        
        recommendations = self.checker.generate_recommendations(checks)
        
        # Should have no recommendations when everything passes
        assert len(recommendations) == 0
    
    def test_run_all_checks(self):
        """Test running all security checks"""
        result = self.checker.run_all_checks()
        
        assert result['success']
        assert 'checks' in result
        assert 'score' in result
        assert 'hipaa_compliance' in result
        assert 'recommendations' in result
        assert 'last_checked' in result
        
        # Verify all required checks are present
        required_checks = [
            'https_enabled', 'default_password', 'phi_hash_salt',
            'csrf_protection', 'session_security', 'audit_logging',
            'password_policy', 'ip_restrictions'
        ]
        
        for check in required_checks:
            assert check in result['checks']


class TestSecurityScoreRatings:
    """Test security score rating system"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.checker = SecurityHealthChecker()
    
    def test_rating_excellent(self):
        """Test rating for 90%+ score"""
        checks = {key: {'status': 'pass'} for key in [
            'https_enabled', 'default_password', 'phi_hash_salt',
            'csrf_protection', 'session_security', 'audit_logging',
            'password_policy', 'ip_restrictions'
        ]}
        
        score_data = self.checker.calculate_security_score(checks)
        
        assert score_data['rating'] == "Excellent"
    
    def test_rating_poor(self):
        """Test rating for <50% score"""
        checks = {key: {'status': 'fail'} for key in [
            'https_enabled', 'default_password', 'phi_hash_salt',
            'csrf_protection', 'session_security', 'audit_logging',
            'password_policy', 'ip_restrictions'
        ]}
        
        score_data = self.checker.calculate_security_score(checks)
        
        assert score_data['score'] == 0
        assert "Poor" in score_data['rating']


