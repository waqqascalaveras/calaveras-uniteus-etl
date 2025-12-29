"""
Authentication & Authorization

Authentication system supporting Active Directory and local database
authentication with session management, role-based access control, and
comprehensive audit logging.

Author: Waqqas Hanafi
Copyright: © 2025 Calaveras County Health and Human Services Agency
"""

import secrets
import hashlib
import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from functools import wraps

from fastapi import Request, HTTPException, status, Depends

# Import unified audit logger
from .audit_logger import get_audit_logger
from fastapi.responses import RedirectResponse

from .config import config

logger = logging.getLogger(__name__)

# Try to import LDAP (optional for AD authentication)
try:
    from ldap3 import Server, Connection, ALL, NTLM, SIMPLE
    LDAP_AVAILABLE = True
except ImportError:
    LDAP_AVAILABLE = False
    logger.warning("ldap3 not available - Active Directory authentication disabled")


# ============================================================================
# Configuration Constants - Loaded from unified .config.json
# ============================================================================

def _load_auth_config():
    """Load authentication configuration from unified config"""
    try:
        import json
        from pathlib import Path
        
        config_file = Path(".config.json")
        if config_file.exists():
            with open(config_file, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            
            security_config = config_data.get('security', {})
            auth_config = security_config.get('authentication', {})
            
            # Password hashing
            pw_hash_config = auth_config.get('password_hashing', {})
            iterations = pw_hash_config.get('iterations', 100000)
            algorithm = pw_hash_config.get('algorithm', 'sha256')
            
            # Account security
            account_config = auth_config.get('account_security', {})
            max_failed = account_config.get('max_failed_login_attempts', 5)
            lockout_minutes = account_config.get('account_lockout_minutes', 30)
            session_timeout = account_config.get('default_session_timeout_minutes', 60)
            
            # Default admin
            admin_config = auth_config.get('default_admin', {})
            admin_username = admin_config.get('username', 'admin')
            admin_password = admin_config.get('password', 'admin123')
            admin_email = admin_config.get('email', 'admin@calaveras.local')
            
            return {
                'PASSWORD_HASH_ITERATIONS': iterations,
                'PASSWORD_HASH_ALGORITHM': algorithm,
                'MAX_FAILED_LOGIN_ATTEMPTS': max_failed,
                'ACCOUNT_LOCKOUT_MINUTES': lockout_minutes,
                'DEFAULT_SESSION_TIMEOUT_MINUTES': session_timeout,
                'DEFAULT_ADMIN_USERNAME': admin_username,
                'DEFAULT_ADMIN_PASSWORD': admin_password,
                'DEFAULT_ADMIN_EMAIL': admin_email
            }
    except Exception as e:
        logger.warning(f"Could not load auth config from .config.json: {e}. Using defaults.")
    
    # Return defaults
    return {
        'PASSWORD_HASH_ITERATIONS': 100000,
        'PASSWORD_HASH_ALGORITHM': 'sha256',
        'MAX_FAILED_LOGIN_ATTEMPTS': 5,
        'ACCOUNT_LOCKOUT_MINUTES': 30,
        'DEFAULT_SESSION_TIMEOUT_MINUTES': 60,
        'DEFAULT_ADMIN_USERNAME': 'admin',
        'DEFAULT_ADMIN_PASSWORD': 'admin123',
        'DEFAULT_ADMIN_EMAIL': 'admin@calaveras.local'
    }

# Load configuration
_auth_config = _load_auth_config()

# Password hashing configuration
PASSWORD_HASH_ITERATIONS = _auth_config['PASSWORD_HASH_ITERATIONS']
PASSWORD_HASH_ALGORITHM = _auth_config['PASSWORD_HASH_ALGORITHM']

# Account security
MAX_FAILED_LOGIN_ATTEMPTS = _auth_config['MAX_FAILED_LOGIN_ATTEMPTS']
ACCOUNT_LOCKOUT_MINUTES = _auth_config['ACCOUNT_LOCKOUT_MINUTES']
DEFAULT_SESSION_TIMEOUT_MINUTES = _auth_config['DEFAULT_SESSION_TIMEOUT_MINUTES']

# Default admin credentials (should be changed immediately)
DEFAULT_ADMIN_USERNAME = _auth_config['DEFAULT_ADMIN_USERNAME']
DEFAULT_ADMIN_PASSWORD = _auth_config['DEFAULT_ADMIN_PASSWORD']
DEFAULT_ADMIN_EMAIL = _auth_config['DEFAULT_ADMIN_EMAIL']


class AuthMode(Enum):
    """Authentication mode"""
    ACTIVE_DIRECTORY = "active_directory"
    LOCAL_DATABASE = "local_database"
    HYBRID = "hybrid"


class UserRole(Enum):
    """User roles for authorization"""
    NEW_USER = "new_user"    # No permissions - needs admin approval
    ADMIN = "admin"          # Full access - can manage users, run ETL, view everything
    OPERATOR = "operator"    # Can run ETL jobs and view data
    VIEWER = "viewer"        # Can only access dashboard (read-only)


@dataclass
class UserSession:
    """User session data"""
    session_id: str
    username: str
    display_name: str
    email: Optional[str]
    role: UserRole
    login_time: datetime
    last_activity: datetime
    ip_address: str
    user_agent: str
    auth_method: str  # 'ad' or 'local'
    
    def is_expired(self, timeout_minutes: int = 60) -> bool:
        """Check if session has expired due to inactivity"""
        return (datetime.now() - self.last_activity) > timedelta(minutes=timeout_minutes)
    
    def update_activity(self):
        """Update last activity timestamp"""
        self.last_activity = datetime.now()
    
    def has_permission(self, required_role: UserRole) -> bool:
        """Check if user has required permission level"""
        role_hierarchy = {
            UserRole.NEW_USER: 0,  # No permissions
            UserRole.VIEWER: 1,
            UserRole.OPERATOR: 2,
            UserRole.ADMIN: 3
        }
        return role_hierarchy[self.role] >= role_hierarchy[required_role]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses"""
        return {
            'session_id': self.session_id,
            'username': self.username,
            'display_name': self.display_name,
            'email': self.email,
            'role': self.role.value,
            'login_time': self.login_time.isoformat(),
            'last_activity': self.last_activity.isoformat(),
            'auth_method': self.auth_method
        }


class LocalUserDatabase:
    """Local user database for non-AD authentication"""
    
    def __init__(self, db_path: Path = None):
        if not db_path:
            db_path = config.directories.database_dir / "internal.db"
        
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self._init_database()
        logger.info(f"Internal database initialized at {db_path}")
    
    def _init_database(self):
        """Initialize user database schema using centralized schema manager"""
        from .internal_schema import ensure_internal_schema
        
        # Use centralized schema manager for consistency
        ensure_internal_schema(str(self.db_path))
        
        # Create default admin user if no users exist
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM sys_users")
            if cursor.fetchone()[0] == 0:
                self._create_default_admin(conn)
    
    def _create_default_admin(self, conn):
        """Create default admin user on first run"""
        password_hash = self._hash_password(DEFAULT_ADMIN_PASSWORD)
        
        conn.execute("""
            INSERT INTO sys_users (username, password_hash, display_name, email, role, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            DEFAULT_ADMIN_USERNAME,
            password_hash,
            "System Administrator",
            DEFAULT_ADMIN_EMAIL,
            UserRole.ADMIN.value,
            datetime.now().isoformat()
        ))
        
        logger.warning("⚠️  Default admin user created")
        logger.warning(f"⚠️  Username: '{DEFAULT_ADMIN_USERNAME}' | Password: '{DEFAULT_ADMIN_PASSWORD}'")
        logger.warning("⚠️  PLEASE CHANGE THE DEFAULT PASSWORD IMMEDIATELY!")
    
    def _hash_password(self, password: str) -> str:
        """Hash password with salt"""
        salt = secrets.token_hex(16)
        pw_hash = hashlib.pbkdf2_hmac(PASSWORD_HASH_ALGORITHM, password.encode(), salt.encode(), PASSWORD_HASH_ITERATIONS)
        return f"{salt}:{pw_hash.hex()}"
    
    def _verify_password(self, password: str, password_hash: str) -> bool:
        """Verify password against hash"""
        try:
            salt, stored_hash = password_hash.split(':')
            pw_hash = hashlib.pbkdf2_hmac(PASSWORD_HASH_ALGORITHM, password.encode(), salt.encode(), PASSWORD_HASH_ITERATIONS)
            return pw_hash.hex() == stored_hash
        except Exception as e:
            logger.error(f"Password verification error: {e}")
            return False
    
    def authenticate(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """Authenticate user against local database"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM sys_users WHERE LOWER(username) = LOWER(?) AND is_active = 1
            """, (username,))
            
            user = cursor.fetchone()
            
            if not user:
                logger.warning(f"Local auth failed: User '{username}' not found or inactive")
                return None
            
            # Check if account is locked
            if user['locked_until']:
                locked_until = datetime.fromisoformat(user['locked_until'])
                if datetime.now() < locked_until:
                    logger.warning(f"Local auth failed: Account '{username}' is locked until {locked_until}")
                    return None
                else:
                    # Unlock account
                    conn.execute("""
                        UPDATE sys_users SET locked_until = NULL, failed_login_attempts = 0 
                        WHERE username = ?
                    """, (username,))
            
            # Verify password
            if not self._verify_password(password, user['password_hash']):
                # Increment failed attempts
                failed_attempts = user['failed_login_attempts'] + 1
                
                # Lock account after max failed attempts
                if failed_attempts >= MAX_FAILED_LOGIN_ATTEMPTS:
                    lock_until = datetime.now() + timedelta(minutes=ACCOUNT_LOCKOUT_MINUTES)
                    conn.execute("""
                        UPDATE sys_users SET failed_login_attempts = ?, locked_until = ?
                        WHERE username = ?
                    """, (failed_attempts, lock_until.isoformat(), username))
                    logger.warning(f"Account '{username}' locked due to too many failed attempts")
                else:
                    conn.execute("""
                        UPDATE sys_users SET failed_login_attempts = ?
                        WHERE username = ?
                    """, (failed_attempts, username))
                
                logger.warning(f"Local auth failed: Invalid password for '{username}' (attempt {failed_attempts}/{MAX_FAILED_LOGIN_ATTEMPTS})")
                return None
            
            # Successful authentication - reset failed attempts and update last login
            conn.execute("""
                UPDATE sys_users SET failed_login_attempts = 0, last_login = ?
                WHERE username = ?
            """, (datetime.now().isoformat(), username))
            
            logger.info(f"Local authentication successful for user: {username}")
            
            return {
                'username': user['username'],
                'display_name': user['display_name'],
                'email': user['email'],
                'role': UserRole(user['role'])
            }
    
    def create_user(self, username: str, password: str, display_name: str, 
                   email: str, role: UserRole, created_by: str, auth_method: str = 'local',
                   obtain_email_on_login: bool = False, obtain_display_name_on_login: bool = False) -> bool:
        """Create a new user with specified authentication method"""
        try:
            # Handle obtain_email_on_login flag for AD users
            if obtain_email_on_login and auth_method == 'ad':
                email = None  # Will be fetched from AD on first login
            
            # Handle obtain_display_name_on_login flag for AD users
            if obtain_display_name_on_login and auth_method == 'ad':
                display_name = None  # Will be fetched from AD on first login
            
            # For AD users, we don't need a real password, but we still need the field
            if auth_method == 'ad':
                password_hash = self._hash_password(secrets.token_hex(32))  # Random unused password
            else:
                password_hash = self._hash_password(password)
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO sys_users (username, password_hash, display_name, email, role, auth_method, 
                                       created_at, created_by, obtain_email_on_login, obtain_display_name_on_login)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    username, password_hash, display_name, email, 
                    role.value, auth_method, datetime.now().isoformat(), created_by,
                    1 if obtain_email_on_login else 0,
                    1 if obtain_display_name_on_login else 0
                ))
            
            logger.info(f"Created user: {username} with role {role.value} and auth_method {auth_method}")
            
            # Log to audit trail
            self.log_audit(
                username=created_by,
                action='create_user',
                category='user_management',
                success=True,
                details=f"Created user {username} with role {role.value} and auth method {auth_method}",
                target_user=username
            )
            
            return True
            
        except sqlite3.IntegrityError as e:
            logger.error(f"User {username} already exists - IntegrityError: {e}", exc_info=True)
            # Check what usernames actually exist
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT username FROM sys_users WHERE LOWER(username) = LOWER(?)", (username,))
                existing = cursor.fetchone()
                if existing:
                    logger.error(f"  Found existing user: '{existing[0]}' (searched for: '{username}')")
            return False
        except Exception as e:
            logger.error(f"Error creating user {username}: {e}", exc_info=True)
            return False
    
    def change_password(self, username: str, old_password: str, new_password: str) -> bool:
        """Change user password"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT password_hash FROM sys_users WHERE LOWER(username) = LOWER(?)", (username,))
            user = cursor.fetchone()
            
            if not user or not self._verify_password(old_password, user[0]):
                logger.warning(f"Password change failed for {username}: Invalid old password")
                return False
            
            new_hash = self._hash_password(new_password)
            conn.execute("UPDATE sys_users SET password_hash = ? WHERE LOWER(username) = LOWER(?)", (new_hash, username))
            logger.info(f"Password changed successfully for user: {username}")
            return True
    
    def list_users(self) -> List[Dict[str, Any]]:
        """List all users"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT username, display_name, email, role, auth_method, is_active, created_at, last_login
                FROM sys_users ORDER BY username
            """)
            
            return [dict(row) for row in cursor.fetchall()]
    
    def deactivate_user(self, username: str) -> bool:
        """Deactivate a user account"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("UPDATE sys_users SET is_active = 0 WHERE LOWER(username) = LOWER(?)", (username,))
            logger.info(f"Deactivated user: {username}")
            return True
        except Exception as e:
            logger.error(f"Error deactivating user {username}: {e}")
            return False
    
    def activate_user(self, username: str) -> bool:
        """Activate a user account"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("UPDATE sys_users SET is_active = 1 WHERE LOWER(username) = LOWER(?)", (username,))
            logger.info(f"Activated user: {username}")
            return True
        except Exception as e:
            logger.error(f"Error activating user {username}: {e}")
            return False
    
    def toggle_user_status(self, username: str) -> tuple[bool, str]:
        """
        Toggle user active status (activate if inactive, deactivate if active)
        Returns (success, new_status) where new_status is 'active' or 'inactive'
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Get current status
                cursor = conn.execute("SELECT is_active FROM sys_users WHERE LOWER(username) = LOWER(?)", (username,))
                row = cursor.fetchone()
                
                if not row:
                    logger.error(f"User not found: {username}")
                    return False, None
                
                current_status = row[0]
                new_status = 0 if current_status else 1
                
                # Toggle status
                conn.execute("UPDATE sys_users SET is_active = ? WHERE LOWER(username) = LOWER(?)", (new_status, username))
                
                status_str = 'active' if new_status else 'inactive'
                logger.info(f"Toggled user {username} to {status_str}")
                return True, status_str
        except Exception as e:
            logger.error(f"Error toggling user status {username}: {e}")
            return False, None
    
    def log_audit(self, username: str, action: str, category: str, success: bool = True,
                  details: str = None, ip_address: str = None, user_agent: str = None,
                  error_message: str = None, session_id: str = None, 
                  target_user: str = None, target_resource: str = None):
        """
        Log audit trail entry for any system activity
        
        Categories: 'auth', 'user_management', 'etl', 'data_access', 'system', 'admin'
        Actions: 'login_success', 'login_failed', 'logout', 'create_user', 'edit_user',
                 'delete_user', 'change_password', 'etl_start', 'etl_complete', 'etl_failed',
                 'view_data', 'export_data', 'config_change', etc.
        """
        try:
            # Also log to unified audit logger
            unified_audit = get_audit_logger()
            unified_audit.log(
                username=username,
                action=action,
                category=category,
                success=success,
                details=details,
                ip_address=ip_address,
                user_agent=user_agent,
                error_message=error_message,
                session_id=session_id,
                target_user=target_user,
                target_resource=target_resource
            )
            
            # Legacy logging to local database for backwards compatibility
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO sys_audit_trail 
                    (timestamp, username, action, category, details, ip_address, user_agent, 
                     success, error_message, session_id, target_user, target_resource)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    datetime.now().isoformat(),
                    username,
                    action,
                    category,
                    details,
                    ip_address,
                    user_agent,
                    1 if success else 0,
                    error_message,
                    session_id,
                    target_user,
                    target_resource
                ))
        except Exception as e:
            logger.error(f"Error logging audit trail: {e}")
    
    def get_audit_logs(self, limit: int = 100, category: str = None, username: str = None) -> List[Dict[str, Any]]:
        """Get audit log entries with optional filtering"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            query = """
                SELECT id, timestamp, username, action, category, details, ip_address, 
                       success, error_message, target_user, target_resource
                FROM sys_audit_trail 
                WHERE 1=1
            """
            params = []
            
            if category:
                query += " AND category = ?"
                params.append(category)
            
            if username:
                query += " AND username = ?"
                params.append(username)
            
            query += " ORDER BY timestamp DESC LIMIT ?"
            params.append(limit)
            
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_user_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        """Get user information by username"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT username, display_name, email, role, auth_method, is_active
                FROM sys_users 
                WHERE LOWER(username) = LOWER(?)
            """, (username,))
            
            user = cursor.fetchone()
            return dict(user) if user else None


class AuthenticationService:
    """Unified authentication service supporting multiple modes"""
    
    def __init__(self, mode: AuthMode = AuthMode.LOCAL_DATABASE):
        self.mode = mode
        self.sessions: Dict[str, UserSession] = {}
        self.session_timeout_minutes = DEFAULT_SESSION_TIMEOUT_MINUTES
        
        # Local user database (always available)
        self.local_db = LocalUserDatabase()
        
        # Load existing sessions from database on startup
        self._load_sessions_from_db()
        
        # Active Directory configuration
        self.ad_enabled = False
        if LDAP_AVAILABLE and mode in [AuthMode.ACTIVE_DIRECTORY, AuthMode.HYBRID]:
            # Use discovered configuration from test_ad_discovery.py
            self.ad_server = "ldap://CCGDC2"
            self.ad_domain = "ORION"
            self.ad_search_base = "DC=calco,DC=local"
            self.ad_enabled = True
            
            # AD Group to Role mapping - customize based on your AD groups
            self.ad_group_role_mapping = {
                # Groups that should get ADMIN role
                'admin': ['Domain Admins', 'ETL-Admins', 'IT-Admin', 'Administrators'],
                # Groups that should get OPERATOR role
                'operator': ['ETL-Operators', 'Data-Analysts', 'Power Users'],
                # Everyone else gets VIEWER role
            }
            
            logger.info(f"Authentication mode: {mode.value} (AD enabled)")
        else:
            logger.info(f"Authentication mode: {mode.value} (AD disabled)")
        
        # IP restrictions (optional - set to empty list to disable)
        self.allowed_ip_ranges = [
            "192.168.",
            "10.",
            "172.",
            "127.0.0.1",
        ]
        
        logger.info(f"Authentication service initialized (mode: {mode.value})")
    
    def validate_ip(self, ip_address: str) -> bool:
        """Check if IP address is from allowed range"""
        if not self.allowed_ip_ranges:
            return True
        
        return any(ip_address.startswith(allowed) for allowed in self.allowed_ip_ranges)
    
    def authenticate(self, username: str, password: str, ip_address: str = None) -> Optional[Dict[str, Any]]:
        """Authenticate user based on configured mode"""
        
        if self.mode == AuthMode.ACTIVE_DIRECTORY:
            return self._authenticate_ad(username, password, ip_address)
        
        elif self.mode == AuthMode.LOCAL_DATABASE:
            return self._authenticate_local(username, password, ip_address)
        
        elif self.mode == AuthMode.HYBRID:
            # Try AD first
            if self.ad_enabled:
                user_info = self._authenticate_ad(username, password, ip_address)
                if user_info:
                    return user_info
            
            # Fallback to local
            return self._authenticate_local(username, password, ip_address)
        
        return None
    
    def _authenticate_ad(self, username: str, password: str, ip_address: str = None) -> Optional[Dict[str, Any]]:
        """Authenticate against Active Directory with pre-validation"""
        if not self.ad_enabled:
            logger.debug(f"AD authentication skipped for {username} - AD not enabled")
            return None
        
        # PRE-VALIDATION: Check if user exists in local database
        local_user = self.local_db.get_user_by_username(username)
        
        # If user doesn't exist, we'll create them automatically after AD authentication succeeds
        # This allows new AD users to log in and be created with NEW_USER role
        auto_create_new_user = (local_user is None)
        
        if not auto_create_new_user:
            # Existing user - validate auth method and status
            if local_user['auth_method'] != 'ad':
                logger.warning(f"AD authentication rejected for {username} - user auth_method is '{local_user['auth_method']}', not 'ad'")
                self.local_db.log_audit(
                    username=username,
                    action='login_failed',
                    category='auth',
                    success=False,
                    details=f"AD login rejected - user configured for {local_user['auth_method']} authentication",
                    ip_address=ip_address,
                    error_message='User not authorized for AD authentication'
                )
                return None
            
            if not local_user['is_active']:
                logger.warning(f"AD authentication rejected for {username} - user account is inactive")
                self.local_db.log_audit(
                    username=username,
                    action='login_failed',
                    category='auth',
                    success=False,
                    details='AD login rejected - account is inactive',
                    ip_address=ip_address,
                    error_message='User account is inactive'
                )
                return None
            
            logger.info(f"AD pre-validation passed for {username} - proceeding to Active Directory authentication")
        else:
            logger.info(f"AD user {username} not in database - will auto-create with NEW_USER role after successful authentication")
        
        try:
            # Try UPN format first (username@domain)
            user_formats = [
                f"{username}@{self.ad_domain.lower()}.local",  # UPN format
                f"{self.ad_domain}\\{username}",                 # NTLM format
                username                                          # Plain username
            ]
            
            server = Server(self.ad_server, get_info=ALL)
            conn = None
            successful_format = None
            
            # Try different user formats
            for user_format in user_formats:
                try:
                    conn = Connection(server, user=user_format, password=password, authentication=SIMPLE, auto_bind=True)
                    if conn.bind():
                        successful_format = user_format
                        logger.info(f"AD authentication successful for {username} using format: {user_format}")
                        break
                    conn = None
                except Exception as e:
                    logger.debug(f"AD auth attempt failed with format '{user_format}': {str(e)[:100]}")
                    continue
            
            if not conn or not conn.bound:
                logger.warning(f"AD authentication failed for user '{username}' - all username formats rejected by server: {self.ad_server}")
                logger.warning(f"  └─ Attempted formats: {', '.join(user_formats)}")
                
                # Log failed authentication
                self.local_db.log_audit(
                    username=username,
                    action='login_failed',
                    category='auth',
                    success=False,
                    details='AD credentials rejected by Active Directory server',
                    ip_address=ip_address,
                    error_message='Invalid credentials'
                )
                return None
            
            # Search for user details
            search_filter = f"(sAMAccountName={username})"
            logger.info(f"Searching AD for user: {username} in {self.ad_search_base}")
            conn.search(
                self.ad_search_base,
                search_filter,
                attributes=['displayName', 'mail', 'memberOf', 'title', 'department']
            )
            
            if len(conn.entries) == 0:
                logger.warning(f"AD user '{username}' authenticated but not found in directory search")
                self.local_db.log_audit(
                    username=username,
                    action='login_failed',
                    category='auth',
                    success=False,
                    details='AD user authenticated but not found in directory',
                    ip_address=ip_address,
                    error_message='User not found in AD directory search'
                )
                conn.unbind()
                return None
            
            if len(conn.entries) > 0:
                entry = conn.entries[0]
                logger.info(f"Found AD user entry for: {username}")
                
                display_name = str(entry.displayName) if hasattr(entry, 'displayName') else username
                email = str(entry.mail) if hasattr(entry, 'mail') else None
                title = str(entry.title) if hasattr(entry, 'title') else None
                department = str(entry.department) if hasattr(entry, 'department') else None
                
                # Auto-create user if they don't exist (double-check to prevent duplicates)
                if auto_create_new_user:
                    # Double-check user doesn't exist (case-insensitive) to prevent race conditions
                    existing_user = self.local_db.get_user_by_username(username)
                    if existing_user:
                        logger.info(f"User {username} already exists in database, skipping auto-creation")
                        local_user = existing_user
                    else:
                        logger.info(f"Auto-creating new user {username} with NEW_USER role")
                        success = self.local_db.create_user(
                            username=username,
                            password=secrets.token_hex(32),  # Random password (not used for AD auth)
                            display_name=display_name,
                            email=email,
                            role=UserRole.NEW_USER,
                            created_by='system',
                            auth_method='ad',
                            obtain_email_on_login=False,
                            obtain_display_name_on_login=False
                        )
                        if success:
                            logger.info(f"Successfully created new user {username} with NEW_USER role")
                            self.local_db.log_audit(
                                username=username,
                                action='user_auto_created',
                                category='user_management',
                                success=True,
                                details=f'User auto-created on first AD login with NEW_USER role',
                                ip_address=ip_address
                            )
                            # Reload user from database
                            local_user = self.local_db.get_user_by_username(username)
                            if not local_user:
                                logger.error(f"Failed to retrieve newly created user {username}")
                                conn.unbind()
                                return None
                        else:
                            logger.error(f"Failed to auto-create user {username}")
                            conn.unbind()
                            return None
                
                # Use role from local database (either existing or newly created)
                role = UserRole(local_user['role'])
                
                # Check if we need to update display name from AD on first login
                if local_user.get('obtain_display_name_on_login') and not local_user.get('display_name') and display_name:
                    logger.info(f"Obtaining display name from AD for {username}: {display_name}")
                    with sqlite3.connect(self.local_db.db_path) as db_conn:
                        db_conn.execute(
                            "UPDATE sys_users SET display_name = ?, obtain_display_name_on_login = 0 WHERE LOWER(username) = LOWER(?)",
                            (display_name, username)
                        )
                    self.local_db.log_audit(
                        username=username,
                        action='display_name_obtained',
                        category='user_management',
                        success=True,
                        details=f'Display name obtained from AD on first login: {display_name}',
                        ip_address=ip_address
                    )
                
                # Check if we need to update email from AD on first login
                if local_user.get('obtain_email_on_login') and not local_user.get('email') and email:
                    logger.info(f"Obtaining email from AD for {username}: {email}")
                    with sqlite3.connect(self.local_db.db_path) as db_conn:
                        db_conn.execute(
                            "UPDATE sys_users SET email = ?, obtain_email_on_login = 0 WHERE LOWER(username) = LOWER(?)",
                            (email, username)
                        )
                    self.local_db.log_audit(
                        username=username,
                        action='email_obtained',
                        category='user_management',
                        success=True,
                        details=f'Email obtained from AD on first login: {email}',
                        ip_address=ip_address
                    )
                
                # Get AD groups for logging
                groups = []
                matched_groups = []
                if hasattr(entry, 'memberOf'):
                    groups = [str(g) for g in entry.memberOf.values] if hasattr(entry.memberOf, 'values') else [str(entry.memberOf)]
                    # Just log admin/operator groups for informational purposes
                    for group in groups:
                        if any(admin_group.lower() in group.lower() for admin_group in self.ad_group_role_mapping['admin']):
                            matched_groups.append(group)
                        elif any(op_group.lower() in group.lower() for op_group in self.ad_group_role_mapping['operator']):
                            matched_groups.append(group)
                
                user_info = {
                    'username': username,
                    'display_name': display_name if display_name else local_user.get('display_name', username),  # Use updated display name if available
                    'email': email if email else local_user.get('email'),  # Use updated email if available
                    'role': role,
                    'auth_method': 'ad'
                }
                
                conn.unbind()
                
                # Enhanced logging with AD details
                logger.info(f"AD authentication successful for: {username} (role: {role.value})")
                logger.info(f"  └─ Display Name: {display_name}")
                if email:
                    logger.info(f"  └─ Email: {email}")
                if title:
                    logger.info(f"  └─ Title: {title}")
                if department:
                    logger.info(f"  └─ Department: {department}")
                if matched_groups:
                    logger.info(f"  └─ Matched Groups: {', '.join([g.split(',')[0].replace('CN=', '') for g in matched_groups])}")
                logger.info(f"  └─ Total AD Groups: {len(groups)}")
                logger.info(f"  └─ Role assigned from local database: {role.value}")
                
                # Log successful authentication
                self.local_db.log_audit(
                    username=username,
                    action='login_success',
                    category='auth',
                    success=True,
                    details=f'AD login successful - {display_name} ({role.value})',
                    ip_address=ip_address
                )
                
                return user_info
            
            conn.unbind()
            
        except Exception as e:
            logger.error(f"AD authentication error for {username}: {e}")
            self.local_db.log_audit(
                username=username,
                action='login_failed',
                category='auth',
                success=False,
                details='AD authentication exception occurred',
                ip_address=ip_address,
                error_message=str(e)
            )
        
        return None
    
    def _authenticate_local(self, username: str, password: str, ip_address: str = None) -> Optional[Dict[str, Any]]:
        """Authenticate against local database"""
        user_info = self.local_db.authenticate(username, password)
        if user_info:
            user_info['auth_method'] = 'local'
            # Log successful local authentication
            self.local_db.log_audit(
                username=username,
                action='login_success',
                category='auth',
                success=True,
                details=f"Local login successful ({user_info['role'].value})",
                ip_address=ip_address
            )
        else:
            # Log failed local authentication
            self.local_db.log_audit(
                username=username,
                action='login_failed',
                category='auth',
                success=False,
                details='Local login failed - invalid credentials',
                ip_address=ip_address,
                error_message='Invalid username or password'
            )
        return user_info
    
    def create_session(self, user_info: Dict[str, Any], ip_address: str, user_agent: str) -> str:
        """Create a new user session and persist to database"""
        session_id = secrets.token_urlsafe(32)
        
        session = UserSession(
            session_id=session_id,
            username=user_info['username'],
            display_name=user_info['display_name'],
            email=user_info.get('email'),
            role=user_info['role'],
            login_time=datetime.now(),
            last_activity=datetime.now(),
            ip_address=ip_address,
            user_agent=user_agent,
            auth_method=user_info.get('auth_method', 'unknown')
        )
        
        # Store in memory
        self.sessions[session_id] = session
        
        # Persist to database
        self._save_session_to_db(session)
        
        logger.info(f"Session created for {session.username} (role: {session.role.value}, method: {session.auth_method}, IP: {ip_address})")
        
        return session_id
    
    def get_session(self, session_id: str) -> Optional[UserSession]:
        """Get and validate session, loading from database if needed"""
        # Check memory first
        session = self.sessions.get(session_id)
        
        # If not in memory, try loading from database
        if not session:
            session = self._load_session_from_db(session_id)
            if session:
                self.sessions[session_id] = session
        
        if not session:
            return None
        
        if session.is_expired(self.session_timeout_minutes):
            self.destroy_session(session_id)
            logger.info(f"Session expired for user: {session.username}")
            return None
        
        session.update_activity()
        
        # Update last activity in database
        self._update_session_activity(session_id, session.last_activity)
        
        return session
    
    def destroy_session(self, session_id: str, reason: str = 'logout'):
        """Destroy a session (logout) and remove from database"""
        # Try to get session from memory or database
        session = self.sessions.get(session_id)
        if not session:
            session = self._load_session_from_db(session_id)
        
        if session:
            logger.info(f"Session destroyed for user: {session.username} (reason: {reason})")
            
            # Log logout to audit trail
            self.local_db.log_audit(
                username=session.username,
                action='logout',
                category='auth',
                success=True,
                details=f'User logged out (reason: {reason})',
                ip_address=session.ip_address,
                session_id=session_id
            )
        
        # Remove from memory
        if session_id in self.sessions:
            del self.sessions[session_id]
        
        # Remove from database
        self._delete_session_from_db(session_id)
    
    def get_active_sessions(self) -> List[UserSession]:
        """Get all active sessions from both memory and database"""
        # Load all sessions from database to ensure consistency
        self._sync_sessions_from_db()
        return list(self.sessions.values())
    
    def _load_sessions_from_db(self):
        """Load all active sessions from database on startup"""
        try:
            with sqlite3.connect(self.local_db.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT * FROM sys_sessions
                """)
                
                loaded_count = 0
                expired_count = 0
                
                for row in cursor.fetchall():
                    session = self._row_to_session(row)
                    if session:
                        # Check if session is expired
                        if not session.is_expired(self.session_timeout_minutes):
                            self.sessions[session.session_id] = session
                            loaded_count += 1
                        else:
                            # Delete expired session
                            self._delete_session_from_db(session.session_id)
                            expired_count += 1
                
                if loaded_count > 0:
                    logger.info(f"Loaded {loaded_count} active session(s) from database")
                if expired_count > 0:
                    logger.info(f"Cleaned up {expired_count} expired session(s)")
        except sqlite3.OperationalError as e:
            if "no such table: sys_sessions" in str(e):
                logger.info("Sessions table not found - will be created on first use")
            else:
                logger.error(f"Error loading sessions from database: {e}")
        except Exception as e:
            logger.error(f"Error loading sessions from database: {e}")
    
    def _sync_sessions_from_db(self):
        """Sync sessions from database (similar to load but doesn't delete expired)"""
        try:
            with sqlite3.connect(self.local_db.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("SELECT * FROM sys_sessions")
                
                for row in cursor.fetchall():
                    session_id = row['session_id']
                    if session_id not in self.sessions:
                        session = self._row_to_session(row)
                        if session and not session.is_expired(self.session_timeout_minutes):
                            self.sessions[session_id] = session
        except Exception as e:
            logger.debug(f"Error syncing sessions: {e}")
    
    def _load_session_from_db(self, session_id: str) -> Optional[UserSession]:
        """Load a specific session from database"""
        try:
            with sqlite3.connect(self.local_db.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT * FROM sys_sessions WHERE session_id = ?
                """, (session_id,))
                
                row = cursor.fetchone()
                if row:
                    return self._row_to_session(row)
        except Exception as e:
            logger.debug(f"Error loading session from database: {e}")
        return None
    
    def _row_to_session(self, row: sqlite3.Row) -> Optional[UserSession]:
        """Convert database row to UserSession object"""
        try:
            return UserSession(
                session_id=row['session_id'],
                username=row['username'],
                display_name=row['display_name'],
                email=row['email'],
                role=UserRole(row['role']),
                login_time=datetime.fromisoformat(row['login_time']),
                last_activity=datetime.fromisoformat(row['last_activity']),
                ip_address=row['ip_address'],
                user_agent=row['user_agent'],
                auth_method=row['auth_method']
            )
        except Exception as e:
            logger.error(f"Error converting row to session: {e}")
            return None
    
    def _save_session_to_db(self, session: UserSession):
        """Save session to database"""
        try:
            with sqlite3.connect(self.local_db.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO sys_sessions 
                    (session_id, username, display_name, email, role, login_time, 
                     last_activity, ip_address, user_agent, auth_method)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    session.session_id,
                    session.username,
                    session.display_name,
                    session.email,
                    session.role.value,
                    session.login_time.isoformat(),
                    session.last_activity.isoformat(),
                    session.ip_address,
                    session.user_agent,
                    session.auth_method
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"Error saving session to database: {e}")
    
    def _update_session_activity(self, session_id: str, last_activity: datetime):
        """Update session last activity in database"""
        try:
            with sqlite3.connect(self.local_db.db_path) as conn:
                conn.execute("""
                    UPDATE sys_sessions SET last_activity = ? WHERE session_id = ?
                """, (last_activity.isoformat(), session_id))
                conn.commit()
        except Exception as e:
            logger.debug(f"Error updating session activity: {e}")
    
    def _delete_session_from_db(self, session_id: str):
        """Delete session from database"""
        try:
            with sqlite3.connect(self.local_db.db_path) as conn:
                conn.execute("DELETE FROM sys_sessions WHERE session_id = ?", (session_id,))
                conn.commit()
        except Exception as e:
            logger.debug(f"Error deleting session from database: {e}")


# Initialize authentication service based on config
# HYBRID mode: Try Active Directory first, fall back to local database
auth_service = AuthenticationService(mode=AuthMode.HYBRID)


def get_auth_service() -> AuthenticationService:
    """Get the global authentication service"""
    return auth_service


# ============================================================================
# FastAPI Dependencies for Route Protection
# ============================================================================

async def get_current_session(request: Request) -> Optional[UserSession]:
    """Get current session without requiring authentication"""
    session_id = request.cookies.get("session_id")
    if session_id:
        return auth_service.get_session(session_id)
    return None


async def require_auth(request: Request) -> UserSession:
    """
    FastAPI dependency to require authentication for API endpoints
    
    Usage:
        @app.get("/api/protected")
        async def protected_route(session: UserSession = Depends(require_auth)):
            return {"user": session.username}
    """
    # Check IP address first
    client_ip = request.client.host
    if not auth_service.validate_ip(client_ip):
        logger.warning(f"Access denied from unauthorized IP: {client_ip}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied: Not from authorized network"
        )
    
    # Get session from cookie
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated"
        )
    
    # Validate session
    session = auth_service.get_session(session_id)
    if not session:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired session"
        )
    
    return session


def require_role(required_role: UserRole):
    """
    FastAPI dependency to require specific role
    
    Usage:
        @app.post("/api/admin/users")
        async def create_user(session: UserSession = Depends(require_role(UserRole.ADMIN))):
            # Only admins can access
            pass
    """
    async def dependency(session: UserSession = Depends(require_auth)):
        if not session.has_permission(required_role):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Requires {required_role.value} role or higher"
            )
        return session
    return dependency


def require_auth_redirect(redirect_url: str = "/login"):
    """
    Decorator for HTML pages that redirects to login if not authenticated
    
    Usage:
        @app.get("/dashboard")
        @require_auth_redirect()
        async def dashboard(request: Request):
            session = request.state.user  # Access user session
            return templates.TemplateResponse("dashboard.html", {
                "request": request,
                "user": session
            })
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(request: Request, *args, **kwargs):
            # Check session
            session_id = request.cookies.get("session_id")
            if session_id:
                session = auth_service.get_session(session_id)
                if session:
                    # Add session to request state for template access
                    request.state.user = session
                    return await func(request, *args, **kwargs)
            
            # Not authenticated - redirect to login
            return RedirectResponse(url=redirect_url, status_code=status.HTTP_303_SEE_OTHER)
        
        return wrapper
    return decorator
