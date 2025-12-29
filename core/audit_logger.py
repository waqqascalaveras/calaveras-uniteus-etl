"""
Unified Audit Logging System

Centralized audit logging system that captures all system activities including
authentication events, user management, ETL operations, data access, and system
changes. All logs stored in a single unified table for easy searching and reporting.

Author: Waqqas Hanafi
Copyright: © 2025 Calaveras County Health and Human Services Agency
"""

import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from enum import Enum

logger = logging.getLogger(__name__)


class AuditCategory(Enum):
    """Audit log categories"""
    AUTHENTICATION = "authentication"
    USER_MANAGEMENT = "user_management"
    ETL = "etl"
    DATA_ACCESS = "data_access"
    DATA_EXPORT = "data_export"
    DATA_IMPORT = "data_import"
    SYSTEM = "system"
    SECURITY = "security"


class AuditAction(Enum):
    """Common audit actions"""
    # Authentication
    LOGIN_SUCCESS = "login_success"
    LOGIN_FAILED = "login_failed"
    LOGOUT = "logout"
    SESSION_EXPIRED = "session_expired"
    PASSWORD_CHANGE = "password_change"
    
    # User Management
    USER_CREATED = "user_created"
    USER_UPDATED = "user_updated"
    USER_DELETED = "user_deleted"
    USER_ACTIVATED = "user_activated"
    USER_DEACTIVATED = "user_deactivated"
    ROLE_CHANGED = "role_changed"
    
    # ETL Operations
    ETL_JOB_STARTED = "etl_job_started"
    ETL_JOB_COMPLETED = "etl_job_completed"
    ETL_JOB_FAILED = "etl_job_failed"
    ETL_JOB_CANCELLED = "etl_job_cancelled"
    FILE_PROCESSED = "file_processed"
    FILE_SKIPPED = "file_skipped"
    FILE_DOWNLOADED = "file_downloaded"
    FILE_FAILED = "file_failed"
    
    # Data Access
    DATABASE_VIEWED = "database_viewed"
    TABLE_QUERIED = "table_queried"
    QUERY_EXECUTED = "query_executed"
    REPORT_GENERATED = "report_generated"
    
    # Data Export/Import
    DATA_EXPORTED = "data_exported"
    TABLE_EXPORTED = "table_exported"
    DATABASE_EXPORTED = "database_exported"
    FILE_UPLOADED = "file_uploaded"
    
    # System
    CONFIG_CHANGED = "config_changed"
    CONFIGURATION_CHANGE = "configuration_change"
    SYSTEM_STARTED = "system_started"
    SYSTEM_STOPPED = "system_stopped"
    DATABASE_BACKUP = "database_backup"
    DATABASE_RESET = "database_reset"


class AuditLogger:
    """Unified audit logging system"""
    
    def __init__(self, db_path: str = "data/database/internal.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_table()
    
    def _ensure_table(self):
        """Ensure sys_audit_trail table exists with all necessary columns"""
        from .internal_schema import ensure_internal_schema
        
        try:
            # Use centralized schema manager for consistency
            ensure_internal_schema(str(self.db_path))
            logger.info("Audit trail table verified/initialized")
        except Exception as e:
            logger.error(f"Error ensuring sys_audit_trail table: {e}")
    
    def log(self,
            username: str,
            action: str,
            category: str,
            success: bool = True,
            details: str = None,
            ip_address: str = None,
            user_agent: str = None,
            session_id: str = None,
            target_user: str = None,
            target_resource: str = None,
            error_message: str = None,
            duration_ms: int = None,
            record_count: int = None,
            file_size: int = None):
        """
        Log an audit event
        
        Args:
            username: User who performed the action
            action: Action performed (use AuditAction enum)
            category: Category of action (use AuditCategory enum)
            success: Whether action succeeded
            details: Additional details about the action
            ip_address: IP address of the user
            user_agent: Browser/client user agent
            session_id: Session identifier
            target_user: Username affected by action (for user management)
            target_resource: Resource affected (file name, table name, etc.)
            error_message: Error message if action failed
            duration_ms: Duration of operation in milliseconds
            record_count: Number of records affected
            file_size: Size of file in bytes
        """
        try:
            # Convert enums to strings if passed
            if isinstance(action, AuditAction):
                action = action.value
            if isinstance(category, AuditCategory):
                category = category.value
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO sys_audit_trail 
                    (timestamp, username, action, category, success, details, ip_address, 
                     user_agent, session_id, target_user, target_resource, error_message,
                     duration_ms, record_count, file_size)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    datetime.now().isoformat(),
                    username,
                    action,
                    category,
                    1 if success else 0,
                    details,
                    ip_address,
                    user_agent,
                    session_id,
                    target_user,
                    target_resource,
                    error_message,
                    duration_ms,
                    record_count,
                    file_size
                ))
            
            # Also log to standard logger for immediate visibility
            level = logging.INFO if success else logging.WARNING
            log_msg = f"[AUDIT] {category}/{action} by {username}"
            if target_resource:
                log_msg += f" on {target_resource}"
            if not success and error_message:
                log_msg += f" - ERROR: {error_message}"
            logger.log(level, log_msg)
            
        except Exception as e:
            logger.error(f"Failed to write audit log: {e}")
    
    def get_logs(self,
                 limit: int = 100,
                 offset: int = 0,
                 category: str = None,
                 username: str = None,
                 action: str = None,
                 success: bool = None,
                 start_date: str = None,
                 end_date: str = None,
                 search: str = None) -> List[Dict[str, Any]]:
        """
        Retrieve audit logs with filtering
        
        Args:
            limit: Maximum number of records to return
            offset: Number of records to skip
            category: Filter by category
            username: Filter by username
            action: Filter by action
            success: Filter by success status (True/False)
            start_date: Filter by start date (ISO format)
            end_date: Filter by end date (ISO format)
            search: Search in details, target_resource, or error_message
        
        Returns:
            List of audit log dictionaries
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                
                query = """
                    SELECT id, timestamp, username, action, category, success, details,
                           ip_address, user_agent, session_id, target_user, target_resource,
                           error_message, duration_ms, record_count, file_size
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
                
                if action:
                    query += " AND action = ?"
                    params.append(action)
                
                if success is not None:
                    query += " AND success = ?"
                    params.append(1 if success else 0)
                
                if start_date:
                    # Use strftime() to extract date part for reliable comparison with ISO timestamps
                    query += " AND strftime('%Y-%m-%d', timestamp) >= ?"
                    params.append(start_date)
                
                if end_date:
                    # Use < to exclude the end_date day itself
                    query += " AND strftime('%Y-%m-%d', timestamp) < ?"
                    params.append(end_date)
                
                if search:
                    query += """ AND (
                        details LIKE ? OR 
                        target_resource LIKE ? OR 
                        error_message LIKE ?
                    )"""
                    search_pattern = f"%{search}%"
                    params.extend([search_pattern, search_pattern, search_pattern])
                
                query += " ORDER BY timestamp DESC LIMIT ? OFFSET ?"
                params.extend([limit, offset])
                
                cursor = conn.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error retrieving audit logs: {e}")
            return []
    
    def get_statistics(self, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """Get audit log statistics"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                where_clause = ""
                params = []
                
                if start_date:
                    # Use strftime() to extract date part for reliable comparison with ISO timestamps
                    # This handles ISO format timestamps with microseconds correctly
                    where_clause = " WHERE strftime('%Y-%m-%d', timestamp) >= ?"
                    params.append(start_date)
                    if end_date:
                        # Use < to exclude the end_date day itself
                        where_clause += " AND strftime('%Y-%m-%d', timestamp) < ?"
                        params.append(end_date)
                elif end_date:
                    where_clause = " WHERE strftime('%Y-%m-%d', timestamp) < ?"
                    params.append(end_date)
                
                # Total events
                cursor = conn.execute(f"SELECT COUNT(*) FROM sys_audit_trail{where_clause}", params)
                total_events = cursor.fetchone()[0]
                
                # Events by category
                cursor = conn.execute(f"""
                    SELECT category, COUNT(*) as count 
                    FROM sys_audit_trail{where_clause}
                    GROUP BY category
                    ORDER BY count DESC
                """, params)
                by_category = {row[0]: row[1] for row in cursor.fetchall()}
                
                # Events by user
                cursor = conn.execute(f"""
                    SELECT username, COUNT(*) as count 
                    FROM sys_audit_trail{where_clause}
                    GROUP BY username
                    ORDER BY count DESC
                    LIMIT 10
                """, params)
                by_user = {row[0]: row[1] for row in cursor.fetchall()}
                
                # Success vs failure
                cursor = conn.execute(f"""
                    SELECT success, COUNT(*) as count 
                    FROM sys_audit_trail{where_clause}
                    GROUP BY success
                """, params)
                success_stats = {('success' if row[0] else 'failure'): row[1] for row in cursor.fetchall()}
                
                # Recent failed logins
                failed_login_where = "WHERE action = 'login_failed'"
                failed_login_params = []
                if start_date:
                    failed_login_where += " AND date(timestamp) >= date(?)"
                    failed_login_params.append(start_date)
                cursor = conn.execute(f"""
                    SELECT username, COUNT(*) as count, MAX(timestamp) as last_attempt
                    FROM sys_audit_trail
                    {failed_login_where}
                    GROUP BY username
                    ORDER BY count DESC
                    LIMIT 5
                """, failed_login_params)
                failed_logins = [
                    {'username': row[0], 'count': row[1], 'last_attempt': row[2]}
                    for row in cursor.fetchall()
                ]
                
                return {
                    'total_events': total_events,
                    'by_category': by_category,
                    'by_user': by_user,
                    'success_failure': success_stats,
                    'failed_logins': failed_logins,
                    'start_date': start_date,
                    'end_date': end_date
                }
        except Exception as e:
            logger.error(f"Error getting audit statistics: {e}")
            return {}
    
    def get_user_activity(self, username: str, days: int = 30) -> Dict[str, Any]:
        """Get activity summary for a specific user"""
        try:
            start_date = (datetime.now() - timedelta(days=days)).isoformat()
            
            with sqlite3.connect(self.db_path) as conn:
                # Total actions
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM sys_audit_trail
                    WHERE username = ? AND timestamp >= ?
                """, (username, start_date))
                total_actions = cursor.fetchone()[0]
                
                # Actions by category
                cursor = conn.execute("""
                    SELECT category, COUNT(*) as count
                    FROM sys_audit_trail
                    WHERE username = ? AND timestamp >= ?
                    GROUP BY category
                    ORDER BY count DESC
                """, (username, start_date))
                by_category = {row[0]: row[1] for row in cursor.fetchall()}
                
                # Recent actions
                cursor = conn.execute("""
                    SELECT timestamp, action, category, target_resource
                    FROM sys_audit_trail
                    WHERE username = ?
                    ORDER BY timestamp DESC
                    LIMIT 10
                """, (username,))
                conn.row_factory = sqlite3.Row
                recent_actions = [dict(row) for row in cursor.fetchall()]
                
                # Last login
                cursor = conn.execute("""
                    SELECT timestamp, ip_address
                    FROM sys_audit_trail
                    WHERE username = ? AND action = 'login_success'
                    ORDER BY timestamp DESC
                    LIMIT 1
                """, (username,))
                last_login = cursor.fetchone()
                
                return {
                    'username': username,
                    'days': days,
                    'total_actions': total_actions,
                    'by_category': by_category,
                    'recent_actions': recent_actions,
                    'last_login': dict(last_login) if last_login else None
                }
        except Exception as e:
            logger.error(f"Error getting user activity: {e}")
            return {}
    
    def cleanup_old_logs(self, days: int = 365):
        """Delete audit logs older than specified days"""
        try:
            cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    DELETE FROM sys_audit_trail WHERE timestamp < ?
                """, (cutoff_date,))
                deleted_count = cursor.rowcount
                
                logger.info(f"Deleted {deleted_count} audit log entries older than {days} days")
                return deleted_count
        except Exception as e:
            logger.error(f"Error cleaning up old logs: {e}")
            return 0


# Global audit logger instance
_audit_logger = None

def get_audit_logger() -> AuditLogger:
    """Get the global audit logger instance"""
    global _audit_logger
    if _audit_logger is None:
        _audit_logger = AuditLogger()
    return _audit_logger
