"""
Security Health Check Module

Provides security configuration validation and compliance checking. Performs
comprehensive security audits of the system configuration, authentication
settings, and data protection measures.

Author: Waqqas Hanafi
Copyright: © 2025 Calaveras County Health and Human Services Agency
"""

import os
import logging
import sqlite3
import hashlib
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime

from .config import config
from .auth import get_auth_service, DEFAULT_ADMIN_PASSWORD

logger = logging.getLogger(__name__)


class SecurityHealthChecker:
    """Performs comprehensive security health checks"""
    
    def __init__(self):
        self.auth_service = get_auth_service()
    
    def run_all_checks(self) -> Dict[str, Any]:
        """Run all security checks and return comprehensive report"""
        checks = {}
        
        # Run individual checks
        checks['https_enabled'] = self.check_https()
        checks['default_password'] = self.check_default_password()
        checks['phi_hash_salt'] = self.check_phi_hash_salt()
        checks['csrf_protection'] = self.check_csrf_protection()
        checks['session_security'] = self.check_session_security()
        checks['audit_logging'] = self.check_audit_logging()
        checks['password_policy'] = self.check_password_policy()
        checks['ip_restrictions'] = self.check_ip_restrictions()
        
        # Calculate score
        score_data = self.calculate_security_score(checks)
        
        # Get HIPAA compliance status
        hipaa_compliance = self.check_hipaa_compliance(checks)
        
        # Generate recommendations
        recommendations = self.generate_recommendations(checks)
        
        return {
            'success': True,
            'checks': checks,
            'score': score_data,
            'hipaa_compliance': hipaa_compliance,
            'recommendations': recommendations,
            'last_checked': datetime.now().isoformat()
        }
    
    def check_https(self) -> Dict[str, str]:
        """Check if HTTPS is properly configured"""
        use_https = config.web.use_https if hasattr(config.web, 'use_https') else False
        
        if use_https:
            # Check if certificate files exist
            cert_file = getattr(config.web, 'ssl_certfile', None)
            key_file = getattr(config.web, 'ssl_keyfile', None)
            
            if cert_file and key_file and Path(cert_file).exists() and Path(key_file).exists():
                return {
                    'status': 'pass',
                    'message': 'HTTPS enabled with valid certificate'
                }
            else:
                return {
                    'status': 'warning',
                    'message': 'HTTPS enabled but certificate files not found'
                }
        else:
            return {
                'status': 'fail',
                'message': 'HTTPS not enabled - sessions transmitted in cleartext',
                'action': 'window.location.href="/admincp/config"'
            }
    
    def check_default_password(self) -> Dict[str, str]:
        """Check if default admin password has been changed"""
        try:
            # Try to authenticate with default credentials
            user_info = self.auth_service.local_db.authenticate('admin', DEFAULT_ADMIN_PASSWORD)
            
            if user_info:
                return {
                    'status': 'fail',
                    'message': '⚠️ CRITICAL: Default admin password still active!',
                    'action': 'window.location.href="/settings"'
                }
            else:
                return {
                    'status': 'pass',
                    'message': 'Default admin password has been changed'
                }
        except Exception as e:
            logger.error(f"Error checking default password: {e}")
            return {
                'status': 'warning',
                'message': f'Unable to verify: {str(e)}'
            }
    
    def check_phi_hash_salt(self) -> Dict[str, str]:
        """Check if PHI hash salt is properly configured"""
        salt = config.security.phi_hash_salt
        
        if not salt:
            return {
                'status': 'fail',
                'message': 'PHI hash salt not configured',
                'action': 'alert("Set PHI_HASH_SALT environment variable")'
            }
        
        if len(salt) != 64:
            return {
                'status': 'fail',
                'message': f'PHI hash salt invalid length ({len(salt)} chars, need 64)'
            }
        
        # Check if it's the default generated one (first 8 chars different each time)
        # We can't reliably detect if it's persistent, so just check it exists
        try:
            # Validate it's hexadecimal
            int(salt, 16)
            return {
                'status': 'pass',
                'message': 'PHI hash salt properly configured (64-char hex)'
            }
        except ValueError:
            return {
                'status': 'fail',
                'message': 'PHI hash salt is not valid hexadecimal'
            }
    
    def check_csrf_protection(self) -> Dict[str, str]:
        """Check if CSRF protection is enabled"""
        # For now, this is a manual check since CSRF isn't implemented yet
        # TODO: Update this when CSRF tokens are implemented
        return {
            'status': 'fail',
            'message': 'CSRF protection not implemented',
            'action': 'alert("Implement CSRF tokens per SECURITY_FIX_CHECKLIST.md")'
        }
    
    def check_session_security(self) -> Dict[str, str]:
        """Check session security configuration"""
        issues = []
        
        # Check session timeout
        timeout = self.auth_service.session_timeout_minutes
        if timeout > 120:
            issues.append(f'Session timeout too long ({timeout} min)')
        
        # Check if secure cookies (if HTTPS enabled)
        use_https = getattr(config.web, 'use_https', False)
        if not use_https:
            issues.append('Secure cookie flag not enabled (HTTP only)')
        
        if issues:
            return {
                'status': 'warning',
                'message': '; '.join(issues)
            }
        else:
            return {
                'status': 'pass',
                'message': f'Session timeout: {timeout} min, secure cookies enabled'
            }
    
    def check_audit_logging(self) -> Dict[str, str]:
        """Check if audit logging is properly configured"""
        try:
            # Check if audit logs are being written
            with sqlite3.connect(self.auth_service.local_db.db_path) as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM sys_audit_trail")
                count = cursor.fetchone()[0]
                
                if count == 0:
                    return {
                        'status': 'warning',
                        'message': 'No audit logs found - logging may not be working'
                    }
                
                # Check recent logs (last 24 hours)
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM sys_audit_trail 
                    WHERE datetime(timestamp) > datetime('now', '-1 day')
                """)
                recent_count = cursor.fetchone()[0]
                
                return {
                    'status': 'pass',
                    'message': f'Audit logging active ({count} total logs, {recent_count} today)'
                }
        except Exception as e:
            logger.error(f"Error checking audit logging: {e}")
            return {
                'status': 'fail',
                'message': f'Audit log check failed: {str(e)}'
            }
    
    def check_password_policy(self) -> Dict[str, str]:
        """Check password policy configuration"""
        # TODO: Update when password complexity requirements are implemented
        return {
            'status': 'warning',
            'message': 'Weak password policy (minimum 8 chars only)',
            'action': 'alert("Implement stronger password requirements per SECURITY_FIX_CHECKLIST.md")'
        }
    
    def check_ip_restrictions(self) -> Dict[str, str]:
        """Check IP address restrictions"""
        allowed_ips = self.auth_service.allowed_ip_ranges
        
        if not allowed_ips or allowed_ips == ["*"]:
            return {
                'status': 'warning',
                'message': 'No IP restrictions configured - accessible from any IP'
            }
        
        return {
            'status': 'pass',
            'message': f'IP restrictions active ({len(allowed_ips)} ranges)'
        }
    
    def calculate_security_score(self, checks: Dict[str, Dict]) -> Dict[str, Any]:
        """Calculate overall security score"""
        # Weight critical checks higher
        weights = {
            'https_enabled': 20,        # Critical
            'default_password': 20,     # Critical
            'phi_hash_salt': 20,        # Critical
            'csrf_protection': 15,      # High
            'session_security': 10,     # Medium
            'audit_logging': 5,         # Medium
            'password_policy': 5,       # Medium
            'ip_restrictions': 5        # Low
        }
        
        total_weight = sum(weights.values())
        earned_points = 0
        
        passed = 0
        warnings = 0
        failed = 0
        
        for check_name, check_result in checks.items():
            status = check_result.get('status', 'unknown')
            weight = weights.get(check_name, 0)
            
            if status == 'pass':
                earned_points += weight
                passed += 1
            elif status == 'warning':
                earned_points += weight * 0.5  # Half credit for warnings
                warnings += 1
            else:  # fail or unknown
                failed += 1
        
        score = int((earned_points / total_weight) * 100)
        
        # Determine rating
        if score >= 90:
            rating = "Excellent"
        elif score >= 70:
            rating = "Good"
        elif score >= 50:
            rating = "Fair"
        else:
            rating = "Poor - Immediate Action Required"
        
        return {
            'score': score,
            'rating': rating,
            'passed': passed,
            'warnings': warnings,
            'failed': failed,
            'total': len(checks)
        }
    
    def check_hipaa_compliance(self, checks: Dict[str, Dict]) -> List[Dict[str, Any]]:
        """Check HIPAA Security Rule compliance"""
        compliance_items = []
        
        # 164.312(a)(1) - Access Control
        compliance_items.append({
            'requirement': '164.312(a)(1)',
            'description': 'Access Control - Unique User Identification',
            'compliant': checks['default_password']['status'] == 'pass' and 
                        checks['password_policy']['status'] != 'fail'
        })
        
        # 164.312(e)(1) - Transmission Security
        compliance_items.append({
            'requirement': '164.312(e)(1)',
            'description': 'Transmission Security - Encryption in Transit',
            'compliant': checks['https_enabled']['status'] == 'pass'
        })
        
        # 164.308(a)(5) - Security Awareness Training
        compliance_items.append({
            'requirement': '164.308(a)(5)',
            'description': 'Security Awareness - Protection from Malicious Software',
            'compliant': checks['csrf_protection']['status'] == 'pass'
        })
        
        # 164.312(c)(1) - Integrity
        compliance_items.append({
            'requirement': '164.312(c)(1)',
            'description': 'Integrity - Protect ePHI from Alteration/Destruction',
            'compliant': checks['audit_logging']['status'] == 'pass'
        })
        
        # 164.312(b) - Audit Controls
        compliance_items.append({
            'requirement': '164.312(b)',
            'description': 'Audit Controls - Record and Examine Activity',
            'compliant': checks['audit_logging']['status'] == 'pass'
        })
        
        return compliance_items
    
    def generate_recommendations(self, checks: Dict[str, Dict]) -> List[Dict[str, str]]:
        """Generate prioritized security recommendations"""
        recommendations = []
        
        # Critical issues
        if checks['default_password']['status'] == 'fail':
            recommendations.append({
                'priority': 'critical',
                'title': 'Change Default Admin Password',
                'description': 'The default admin password (admin/admin123) is still active. This is a critical security risk.',
                'action': 'Go to Settings and change the admin password immediately',
                'impact': 'Unauthorized access, data breach, HIPAA violation'
            })
        
        if checks['https_enabled']['status'] == 'fail':
            recommendations.append({
                'priority': 'critical',
                'title': 'Enable HTTPS',
                'description': 'Application is running over HTTP. Session tokens and PHI data transmitted in cleartext.',
                'action': 'Configure SSL/TLS certificate and enable HTTPS (see docs/INTERNAL_NETWORK_SECURITY.md)',
                'impact': 'Session hijacking, man-in-the-middle attacks, HIPAA violation'
            })
        
        if checks['phi_hash_salt']['status'] == 'fail':
            recommendations.append({
                'priority': 'critical',
                'title': 'Configure PHI Hash Salt',
                'description': 'PHI hash salt is not properly configured. PHI de-identification may not be reliable.',
                'action': 'Set PHI_HASH_SALT environment variable with 64-character hex string',
                'impact': 'Inconsistent PHI hashing, data integrity issues'
            })
        
        # High priority issues
        if checks['csrf_protection']['status'] == 'fail':
            recommendations.append({
                'priority': 'high',
                'title': 'Implement CSRF Protection',
                'description': 'No CSRF token validation. Application vulnerable to cross-site request forgery.',
                'action': 'Implement CSRF tokens per SECURITY_FIX_CHECKLIST.md',
                'impact': 'Unauthorized actions, data modification'
            })
        
        if checks['password_policy']['status'] != 'pass':
            recommendations.append({
                'priority': 'high',
                'title': 'Strengthen Password Policy',
                'description': 'Current password policy is weak (8 chars minimum only).',
                'action': 'Implement password complexity requirements (12+ chars, uppercase, lowercase, numbers, symbols)',
                'impact': 'Brute force attacks, credential compromise'
            })
        
        # Medium priority issues
        if checks['session_security']['status'] == 'warning':
            recommendations.append({
                'priority': 'medium',
                'title': 'Review Session Security',
                'description': checks['session_security']['message'],
                'action': 'Configure secure session settings',
                'impact': 'Session hijacking risk'
            })
        
        if checks['ip_restrictions']['status'] == 'warning':
            recommendations.append({
                'priority': 'medium',
                'title': 'Configure IP Restrictions',
                'description': 'No IP address restrictions configured.',
                'action': 'Limit access to authorized IP ranges',
                'impact': 'Unauthorized network access'
            })
        
        return recommendations


# Global instance
_health_checker = None

def get_health_checker() -> SecurityHealthChecker:
    """Get global health checker instance"""
    global _health_checker
    if _health_checker is None:
        _health_checker = SecurityHealthChecker()
    return _health_checker

