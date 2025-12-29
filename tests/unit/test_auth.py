"""
================================================================================
Calaveras UniteUs ETL - Authentication System Unit Tests
================================================================================
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency

Description:
    Comprehensive test suite for the authentication module covering user
    database operations, password hashing, authentication flows, session
    management, role-based access control, and security features.

Test Coverage:
    - User database operations
    - Password hashing and verification
    - Authentication flows (local and AD)
    - Session management
    - Role-based access control
    - Account lockout mechanisms
    - Security features
================================================================================
"""

import pytest
import tempfile
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock

# Add parent directory to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.auth import (
    LocalUserDatabase,
    AuthenticationService,
    AuthMode,
    UserRole,
    UserSession,
    PASSWORD_HASH_ITERATIONS,
    MAX_FAILED_LOGIN_ATTEMPTS,
    ACCOUNT_LOCKOUT_MINUTES,
    DEFAULT_ADMIN_USERNAME,
    DEFAULT_ADMIN_PASSWORD
)


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def temp_db_path():
    """Create a temporary database file"""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.db') as f:
        db_path = Path(f.name)
    yield db_path
    # Cleanup
    try:
        if db_path.exists():
            db_path.unlink()
    except PermissionError:
        pass  # File may still be in use on Windows


@pytest.fixture
def user_db(temp_db_path):
    """Create a LocalUserDatabase instance with temp database"""
    db = LocalUserDatabase(db_path=temp_db_path)
    yield db
    # Close any connections
    if hasattr(db, 'conn') and db.conn:
        db.conn.close()


@pytest.fixture
def auth_service_local(temp_db_path):
    """Create an AuthenticationService in LOCAL_DATABASE mode"""
    service = AuthenticationService(mode=AuthMode.LOCAL_DATABASE)
    service.local_db = LocalUserDatabase(db_path=temp_db_path)
    yield service
    # Close connections
    if hasattr(service.local_db, 'conn') and service.local_db.conn:
        service.local_db.conn.close()


# ============================================================================
# LocalUserDatabase Tests
# ============================================================================

class TestLocalUserDatabase:
    """Test suite for LocalUserDatabase"""
    
    def test_database_initialization(self, user_db, temp_db_path):
        """Test database and tables are created"""
        assert temp_db_path.exists()
        
        # Check tables exist
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        assert 'sys_users' in tables
        assert 'sys_audit_trail' in tables
    
    def test_default_admin_created(self, user_db):
        """Test default admin user is created on initialization"""
        users = user_db.list_users()
        
        assert len(users) == 1
        assert users[0]['username'] == DEFAULT_ADMIN_USERNAME
        assert users[0]['role'] == UserRole.ADMIN.value
        assert users[0]['is_active'] == 1
    
    def test_password_hashing(self, user_db):
        """Test password hashing is secure"""
        password = "TestPassword123!"
        hash1 = user_db._hash_password(password)
        hash2 = user_db._hash_password(password)
        
        # Same password should produce different hashes (due to salt)
        assert hash1 != hash2
        
        # Both should verify correctly
        assert user_db._verify_password(password, hash1)
        assert user_db._verify_password(password, hash2)
        
        # Wrong password should not verify
        assert not user_db._verify_password("WrongPassword", hash1)
    
    def test_password_hash_format(self, user_db):
        """Test password hash format is correct"""
        password = "TestPassword123!"
        hash_value = user_db._hash_password(password)
        
        # Should be in format "salt:hash"
        assert ':' in hash_value
        parts = hash_value.split(':')
        assert len(parts) == 2
        
        # Salt should be 32 hex characters
        assert len(parts[0]) == 32
        
        # Hash should be 64 hex characters (SHA256)
        assert len(parts[1]) == 64
    
    def test_create_user(self, user_db):
        """Test creating a new user"""
        success = user_db.create_user(
            username='testuser',
            password='Password123!',
            display_name='Test User',
            email='test@example.com',
            role=UserRole.OPERATOR,
            created_by='admin'
        )
        
        assert success
        
        # Verify user exists
        users = user_db.list_users()
        assert len(users) == 2  # admin + testuser
        
        test_user = next(u for u in users if u['username'] == 'testuser')
        assert test_user['display_name'] == 'Test User'
        assert test_user['email'] == 'test@example.com'
        assert test_user['role'] == UserRole.OPERATOR.value
    
    def test_create_duplicate_user(self, user_db):
        """Test creating duplicate user fails"""
        # First creation should succeed
        success1 = user_db.create_user(
            username='testuser',
            password='Password123!',
            display_name='Test User',
            email='test@example.com',
            role=UserRole.VIEWER,
            created_by='admin'
        )
        assert success1
        
        # Second creation should fail
        success2 = user_db.create_user(
            username='testuser',
            password='Password456!',
            display_name='Test User 2',
            email='test2@example.com',
            role=UserRole.VIEWER,
            created_by='admin'
        )
        assert not success2
    
    def test_authenticate_success(self, user_db):
        """Test successful authentication"""
        # Authenticate with default admin
        user_info = user_db.authenticate(DEFAULT_ADMIN_USERNAME, DEFAULT_ADMIN_PASSWORD)
        
        assert user_info is not None
        assert user_info['username'] == DEFAULT_ADMIN_USERNAME
        assert user_info['role'] == UserRole.ADMIN
        assert 'password_hash' not in user_info  # Should not return password hash
    
    def test_authenticate_wrong_password(self, user_db):
        """Test authentication with wrong password"""
        user_info = user_db.authenticate(DEFAULT_ADMIN_USERNAME, 'WrongPassword')
        
        assert user_info is None
    
    def test_authenticate_nonexistent_user(self, user_db):
        """Test authentication with non-existent user"""
        user_info = user_db.authenticate('nonexistent', 'password')
        
        assert user_info is None
    
    def test_authenticate_case_insensitive_username(self, user_db):
        """Test username is case-insensitive"""
        # Should work with different case
        user_info1 = user_db.authenticate(DEFAULT_ADMIN_USERNAME.upper(), DEFAULT_ADMIN_PASSWORD)
        user_info2 = user_db.authenticate(DEFAULT_ADMIN_USERNAME.lower(), DEFAULT_ADMIN_PASSWORD)
        
        assert user_info1 is not None
        assert user_info2 is not None
        assert user_info1['username'] == user_info2['username']
    
    def test_account_lockout(self, user_db):
        """Test account locks after max failed attempts"""
        # Make multiple failed login attempts
        for i in range(MAX_FAILED_LOGIN_ATTEMPTS):
            result = user_db.authenticate(DEFAULT_ADMIN_USERNAME, 'WrongPassword')
            assert result is None
        
        # Next attempt should still be locked even with correct password
        result = user_db.authenticate(DEFAULT_ADMIN_USERNAME, DEFAULT_ADMIN_PASSWORD)
        assert result is None
    
    def test_account_lockout_expiration(self, user_db, temp_db_path):
        """Test locked account can be unlocked after timeout"""
        # Lock the account
        for i in range(MAX_FAILED_LOGIN_ATTEMPTS):
            user_db.authenticate(DEFAULT_ADMIN_USERNAME, 'WrongPassword')
        
        # Manually expire the lockout (simulate time passing)
        conn = sqlite3.connect(temp_db_path)
        past_time = (datetime.now() - timedelta(minutes=ACCOUNT_LOCKOUT_MINUTES + 1)).isoformat()
        conn.execute("UPDATE sys_users SET locked_until = ? WHERE username = ?", 
                    (past_time, DEFAULT_ADMIN_USERNAME))
        conn.commit()
        conn.close()
        
        # Should be able to login now
        result = user_db.authenticate(DEFAULT_ADMIN_USERNAME, DEFAULT_ADMIN_PASSWORD)
        assert result is not None
    
    def test_failed_attempts_reset_on_success(self, user_db, temp_db_path):
        """Test failed attempt counter resets after successful login"""
        # Make some failed attempts
        for i in range(2):
            user_db.authenticate(DEFAULT_ADMIN_USERNAME, 'WrongPassword')
        
        # Check failed attempts were recorded
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT failed_login_attempts FROM sys_users WHERE username = ?", 
                      (DEFAULT_ADMIN_USERNAME,))
        failed_attempts = cursor.fetchone()[0]
        conn.close()
        assert failed_attempts == 2
        
        # Successful login
        result = user_db.authenticate(DEFAULT_ADMIN_USERNAME, DEFAULT_ADMIN_PASSWORD)
        assert result is not None
        
        # Failed attempts should be reset
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT failed_login_attempts FROM sys_users WHERE username = ?", 
                      (DEFAULT_ADMIN_USERNAME,))
        failed_attempts = cursor.fetchone()[0]
        conn.close()
        assert failed_attempts == 0
    
    def test_change_password(self, user_db):
        """Test password change functionality"""
        new_password = 'NewPassword123!'
        
        success = user_db.change_password(
            DEFAULT_ADMIN_USERNAME,
            DEFAULT_ADMIN_PASSWORD,
            new_password
        )
        
        assert success
        
        # Old password should not work
        result = user_db.authenticate(DEFAULT_ADMIN_USERNAME, DEFAULT_ADMIN_PASSWORD)
        assert result is None
        
        # New password should work
        result = user_db.authenticate(DEFAULT_ADMIN_USERNAME, new_password)
        assert result is not None
    
    def test_change_password_wrong_old_password(self, user_db):
        """Test password change fails with wrong old password"""
        success = user_db.change_password(
            DEFAULT_ADMIN_USERNAME,
            'WrongOldPassword',
            'NewPassword123!'
        )
        
        assert not success
        
        # Original password should still work
        result = user_db.authenticate(DEFAULT_ADMIN_USERNAME, DEFAULT_ADMIN_PASSWORD)
        assert result is not None
    
    def test_deactivate_user(self, user_db):
        """Test deactivating a user"""
        # Create a test user
        user_db.create_user(
            username='testuser',
            password='Password123!',
            display_name='Test User',
            email='test@example.com',
            role=UserRole.VIEWER,
            created_by='admin'
        )
        
        # Deactivate user
        success = user_db.deactivate_user('testuser')
        assert success
        
        # User should not be able to authenticate
        result = user_db.authenticate('testuser', 'Password123!')
        assert result is None
    
    def test_list_users(self, user_db):
        """Test listing all users"""
        # Create additional users
        user_db.create_user('user1', 'Pass1!', 'User One', 'user1@example.com', UserRole.VIEWER, 'admin')
        user_db.create_user('user2', 'Pass2!', 'User Two', 'user2@example.com', UserRole.OPERATOR, 'admin')
        
        users = user_db.list_users()
        
        assert len(users) == 3  # admin + 2 test users
        usernames = [u['username'] for u in users]
        assert DEFAULT_ADMIN_USERNAME in usernames
        assert 'user1' in usernames
        assert 'user2' in usernames


# ============================================================================
# UserSession Tests
# ============================================================================

class TestUserSession:
    """Test suite for UserSession"""
    
    def test_session_creation(self):
        """Test creating a user session"""
        session = UserSession(
            session_id='test_session_123',
            username='testuser',
            display_name='Test User',
            email='test@example.com',
            role=UserRole.OPERATOR,
            login_time=datetime.now(),
            last_activity=datetime.now(),
            ip_address='192.168.1.100',
            user_agent='Mozilla/5.0',
            auth_method='local'
        )
        
        assert session.session_id == 'test_session_123'
        assert session.username == 'testuser'
        assert session.role == UserRole.OPERATOR
    
    def test_session_expiration(self):
        """Test session expiration check"""
        # Create session with old last_activity
        session = UserSession(
            session_id='test_session',
            username='testuser',
            display_name='Test User',
            email=None,
            role=UserRole.VIEWER,
            login_time=datetime.now() - timedelta(hours=2),
            last_activity=datetime.now() - timedelta(hours=2),
            ip_address='192.168.1.100',
            user_agent='Mozilla/5.0',
            auth_method='local'
        )
        
        # Should be expired with 60 minute timeout
        assert session.is_expired(timeout_minutes=60)
        
        # Should not be expired with 3 hour timeout
        assert not session.is_expired(timeout_minutes=180)
    
    def test_update_activity(self):
        """Test updating session activity"""
        session = UserSession(
            session_id='test_session',
            username='testuser',
            display_name='Test User',
            email=None,
            role=UserRole.VIEWER,
            login_time=datetime.now() - timedelta(minutes=30),
            last_activity=datetime.now() - timedelta(minutes=30),
            ip_address='192.168.1.100',
            user_agent='Mozilla/5.0',
            auth_method='local'
        )
        
        old_activity = session.last_activity
        session.update_activity()
        
        assert session.last_activity > old_activity
    
    def test_has_permission(self):
        """Test role-based permission checking"""
        admin_session = UserSession(
            session_id='admin_session',
            username='admin',
            display_name='Admin',
            email=None,
            role=UserRole.ADMIN,
            login_time=datetime.now(),
            last_activity=datetime.now(),
            ip_address='192.168.1.100',
            user_agent='Mozilla/5.0',
            auth_method='local'
        )
        
        operator_session = UserSession(
            session_id='operator_session',
            username='operator',
            display_name='Operator',
            email=None,
            role=UserRole.OPERATOR,
            login_time=datetime.now(),
            last_activity=datetime.now(),
            ip_address='192.168.1.100',
            user_agent='Mozilla/5.0',
            auth_method='local'
        )
        
        viewer_session = UserSession(
            session_id='viewer_session',
            username='viewer',
            display_name='Viewer',
            email=None,
            role=UserRole.VIEWER,
            login_time=datetime.now(),
            last_activity=datetime.now(),
            ip_address='192.168.1.100',
            user_agent='Mozilla/5.0',
            auth_method='local'
        )
        
        # Admin has all permissions
        assert admin_session.has_permission(UserRole.ADMIN)
        assert admin_session.has_permission(UserRole.OPERATOR)
        assert admin_session.has_permission(UserRole.VIEWER)
        
        # Operator has operator and viewer permissions
        assert not operator_session.has_permission(UserRole.ADMIN)
        assert operator_session.has_permission(UserRole.OPERATOR)
        assert operator_session.has_permission(UserRole.VIEWER)
        
        # Viewer has only viewer permission
        assert not viewer_session.has_permission(UserRole.ADMIN)
        assert not viewer_session.has_permission(UserRole.OPERATOR)
        assert viewer_session.has_permission(UserRole.VIEWER)
    
    def test_to_dict(self):
        """Test session serialization to dictionary"""
        session = UserSession(
            session_id='test_session',
            username='testuser',
            display_name='Test User',
            email='test@example.com',
            role=UserRole.OPERATOR,
            login_time=datetime.now(),
            last_activity=datetime.now(),
            ip_address='192.168.1.100',
            user_agent='Mozilla/5.0',
            auth_method='local'
        )
        
        session_dict = session.to_dict()
        
        assert session_dict['session_id'] == 'test_session'
        assert session_dict['username'] == 'testuser'
        assert session_dict['role'] == 'operator'
        assert session_dict['auth_method'] == 'local'
        assert 'login_time' in session_dict
        assert 'last_activity' in session_dict


# ============================================================================
# AuthenticationService Tests
# ============================================================================

class TestAuthenticationService:
    """Test suite for AuthenticationService"""
    
    def test_service_initialization(self, auth_service_local):
        """Test authentication service initializes correctly"""
        assert auth_service_local.mode == AuthMode.LOCAL_DATABASE
        assert auth_service_local.local_db is not None
        assert len(auth_service_local.sessions) == 0
    
    def test_create_session(self, auth_service_local):
        """Test creating a user session"""
        user_info = {
            'username': 'testuser',
            'display_name': 'Test User',
            'email': 'test@example.com',
            'role': UserRole.OPERATOR,
            'auth_method': 'local'
        }
        
        session_id = auth_service_local.create_session(
            user_info,
            '192.168.1.100',
            'Mozilla/5.0'
        )
        
        assert session_id is not None
        assert len(session_id) > 20  # Should be a secure token
        assert session_id in auth_service_local.sessions
        
        session = auth_service_local.sessions[session_id]
        assert session.username == 'testuser'
        assert session.role == UserRole.OPERATOR
    
    def test_get_session(self, auth_service_local):
        """Test retrieving a session"""
        user_info = {
            'username': 'testuser',
            'display_name': 'Test User',
            'email': None,
            'role': UserRole.VIEWER,
            'auth_method': 'local'
        }
        
        session_id = auth_service_local.create_session(user_info, '192.168.1.100', 'Mozilla/5.0')
        
        # Get session
        session = auth_service_local.get_session(session_id)
        assert session is not None
        assert session.username == 'testuser'
    
    def test_get_invalid_session(self, auth_service_local):
        """Test retrieving non-existent session returns None"""
        session = auth_service_local.get_session('invalid_session_id')
        assert session is None
    
    def test_destroy_session(self, auth_service_local):
        """Test destroying a session"""
        user_info = {
            'username': 'testuser',
            'display_name': 'Test User',
            'email': None,
            'role': UserRole.VIEWER,
            'auth_method': 'local'
        }
        
        session_id = auth_service_local.create_session(user_info, '192.168.1.100', 'Mozilla/5.0')
        assert session_id in auth_service_local.sessions
        
        # Destroy session
        auth_service_local.destroy_session(session_id)
        assert session_id not in auth_service_local.sessions
        
        # Getting destroyed session should return None
        session = auth_service_local.get_session(session_id)
        assert session is None
    
    def test_validate_ip(self, auth_service_local):
        """Test IP validation"""
        # These should be allowed (default LAN ranges)
        assert auth_service_local.validate_ip('192.168.1.100')
        assert auth_service_local.validate_ip('10.0.0.1')
        assert auth_service_local.validate_ip('127.0.0.1')
        
        # These should be blocked
        assert not auth_service_local.validate_ip('8.8.8.8')
        assert not auth_service_local.validate_ip('1.1.1.1')
    
    def test_get_active_sessions(self, auth_service_local):
        """Test getting list of active sessions"""
        # Create multiple sessions
        for i in range(3):
            user_info = {
                'username': f'user{i}',
                'display_name': f'User {i}',
                'email': None,
                'role': UserRole.VIEWER,
                'auth_method': 'local'
            }
            auth_service_local.create_session(user_info, f'192.168.1.{100+i}', 'Mozilla/5.0')
        
        active_sessions = auth_service_local.get_active_sessions()
        assert len(active_sessions) == 3


# ============================================================================
# Integration Tests
# ============================================================================

class TestAuthenticationIntegration:
    """Integration tests for complete authentication flows"""
    
    def test_complete_login_flow(self, auth_service_local):
        """Test complete login flow from authentication to session"""
        # Authenticate
        user_info = auth_service_local.authenticate(DEFAULT_ADMIN_USERNAME, DEFAULT_ADMIN_PASSWORD)
        assert user_info is not None
        
        # Create session
        session_id = auth_service_local.create_session(user_info, '192.168.1.100', 'Mozilla/5.0')
        assert session_id is not None
        
        # Verify session
        session = auth_service_local.get_session(session_id)
        assert session is not None
        assert session.username == DEFAULT_ADMIN_USERNAME
        assert session.role == UserRole.ADMIN
        
        # Logout (destroy session)
        auth_service_local.destroy_session(session_id)
        session = auth_service_local.get_session(session_id)
        assert session is None
    
    def test_failed_login_flow(self, auth_service_local):
        """Test failed login does not create session"""
        # Attempt authentication with wrong password
        user_info = auth_service_local.authenticate(DEFAULT_ADMIN_USERNAME, 'WrongPassword')
        assert user_info is None
        
        # Should have no sessions
        assert len(auth_service_local.sessions) == 0


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
