"""
Internal Database Schema

Single source of truth for internal.db schema (sys_users and sys_audit_trail).
Ensures consistency across authentication and audit logging modules.

Author: Waqqas Hanafi
Copyright: © 2025 Calaveras County Health and Human Services Agency
"""

import sqlite3
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def get_internal_schema_sql() -> str:
    """
    Get the complete internal database schema SQL.
    This is the single source of truth for internal.db structure.
    """
    return """
-- Users table for authentication and authorization
CREATE TABLE IF NOT EXISTS sys_users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL COLLATE NOCASE,
    password_hash TEXT NOT NULL,
    display_name TEXT,
    email TEXT,
    role TEXT NOT NULL,
    auth_method TEXT NOT NULL DEFAULT 'local',
    is_active INTEGER DEFAULT 1,
    created_at TEXT NOT NULL,
    created_by TEXT,
    last_login TEXT,
    failed_login_attempts INTEGER DEFAULT 0,
    locked_until TEXT,
    obtain_email_on_login INTEGER DEFAULT 0,
    obtain_display_name_on_login INTEGER DEFAULT 0
);

-- Comprehensive audit trail for all system activities
CREATE TABLE IF NOT EXISTS sys_audit_trail (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    username TEXT NOT NULL,
    action TEXT NOT NULL,
    category TEXT NOT NULL,
    success INTEGER DEFAULT 1,
    details TEXT,
    ip_address TEXT,
    user_agent TEXT,
    session_id TEXT,
    target_user TEXT,
    target_resource TEXT,
    error_message TEXT,
    duration_ms INTEGER,
    record_count INTEGER,
    file_size INTEGER
);

-- ETL job history for persistence across server restarts
CREATE TABLE IF NOT EXISTS sys_etl_jobs (
    job_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    start_time TEXT NOT NULL,
    end_time TEXT,
    total_files INTEGER DEFAULT 0,
    files_completed INTEGER DEFAULT 0,
    files_failed INTEGER DEFAULT 0,
    files_skipped INTEGER DEFAULT 0,
    total_records INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_skipped INTEGER DEFAULT 0,
    error_message TEXT,
    username TEXT NOT NULL,
    created_at TEXT NOT NULL
);

-- Individual file results for each ETL job
CREATE TABLE IF NOT EXISTS sys_etl_job_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    filename TEXT NOT NULL,
    table_name TEXT NOT NULL,
    status TEXT NOT NULL,
    record_count INTEGER DEFAULT 0,
    inserted INTEGER DEFAULT 0,
    updated INTEGER DEFAULT 0,
    skipped INTEGER DEFAULT 0,
    error_message TEXT,
    processing_time_seconds REAL,
    FOREIGN KEY (job_id) REFERENCES sys_etl_jobs(job_id) ON DELETE CASCADE
);

-- User sessions for persistent session management across server restarts
CREATE TABLE IF NOT EXISTS sys_sessions (
    session_id TEXT PRIMARY KEY,
    username TEXT NOT NULL,
    display_name TEXT NOT NULL,
    email TEXT,
    role TEXT NOT NULL,
    login_time TEXT NOT NULL,
    last_activity TEXT NOT NULL,
    ip_address TEXT NOT NULL,
    user_agent TEXT NOT NULL,
    auth_method TEXT NOT NULL
);

-- SIEM configuration settings
CREATE TABLE IF NOT EXISTS sys_siem_config (
    id INTEGER PRIMARY KEY CHECK (id = 1),  -- Only allow one row
    enabled INTEGER DEFAULT 0,
    enable_windows_event_log INTEGER DEFAULT 0,
    syslog_enabled INTEGER DEFAULT 0,
    syslog_host TEXT,
    syslog_port INTEGER DEFAULT 514,
    syslog_protocol TEXT DEFAULT 'UDP',
    include_sensitive_data INTEGER DEFAULT 0,
    windows_event_log_min_severity TEXT DEFAULT 'ERROR',
    syslog_min_severity TEXT DEFAULT 'ERROR',
    updated_at TEXT NOT NULL,
    updated_by TEXT NOT NULL
);

-- SFTP configuration settings (encrypted fields should be handled separately)
CREATE TABLE IF NOT EXISTS sys_sftp_config (
    id INTEGER PRIMARY KEY CHECK (id = 1),  -- Only allow one row
    enabled INTEGER DEFAULT 0,
    host TEXT,
    port INTEGER DEFAULT 22,
    username TEXT,
    auth_method TEXT DEFAULT 'key',
    private_key_path TEXT,
    remote_directory TEXT DEFAULT '/data/exports',
    auto_download INTEGER DEFAULT 0,
    download_interval_minutes INTEGER DEFAULT 60,
    delete_after_download INTEGER DEFAULT 0,
    local_download_path TEXT,
    timeout_seconds INTEGER DEFAULT 30,
    max_retries INTEGER DEFAULT 3,
    verify_host_key INTEGER DEFAULT 1,
    known_hosts_path TEXT,
    updated_at TEXT NOT NULL,
    updated_by TEXT NOT NULL
);

-- SFTP file patterns (many-to-one relationship)
CREATE TABLE IF NOT EXISTS sftp_file_patterns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pattern TEXT NOT NULL,
    enabled INTEGER DEFAULT 1,
    created_at TEXT NOT NULL
);

-- Database configuration settings
CREATE TABLE IF NOT EXISTS database_config (
    id INTEGER PRIMARY KEY CHECK (id = 1),  -- Only allow one row
    db_type TEXT NOT NULL DEFAULT 'sqlite',
    path TEXT,
    mssql_server TEXT,
    mssql_port INTEGER,
    mssql_database TEXT,
    mssql_username TEXT,
    mssql_password TEXT,
    mssql_trusted_connection INTEGER DEFAULT 1,
    mssql_driver TEXT,
    postgresql_host TEXT,
    postgresql_port INTEGER,
    postgresql_database TEXT,
    postgresql_username TEXT,
    postgresql_password TEXT,
    mysql_host TEXT,
    mysql_port INTEGER,
    mysql_database TEXT,
    mysql_username TEXT,
    mysql_password TEXT,
    connection_timeout INTEGER DEFAULT 30,
    max_connections INTEGER DEFAULT 10,
    updated_at TEXT,
    updated_by TEXT
);

-- Schema errors tracking for import mismatches
CREATE TABLE IF NOT EXISTS schema_errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    error_type TEXT NOT NULL,
    table_name TEXT,
    file_name TEXT NOT NULL,
    error_message TEXT NOT NULL,
    detected_at TEXT NOT NULL,
    resolved_at TEXT,
    resolved_by TEXT,
    error_details TEXT,
    suggested_sql TEXT,
    severity TEXT DEFAULT 'critical'
);

-- File to table name mappings (configurable)
CREATE TABLE IF NOT EXISTS file_table_mappings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_pattern TEXT NOT NULL UNIQUE,
    table_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    created_by TEXT,
    is_active INTEGER DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON sys_audit_trail(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_audit_username ON sys_audit_trail(username);
CREATE INDEX IF NOT EXISTS idx_audit_category ON sys_audit_trail(category);
CREATE INDEX IF NOT EXISTS idx_audit_action ON sys_audit_trail(action);
CREATE INDEX IF NOT EXISTS idx_users_username ON sys_users(username);
CREATE INDEX IF NOT EXISTS idx_users_role ON sys_users(role);
CREATE INDEX IF NOT EXISTS idx_etl_jobs_start_time ON sys_etl_jobs(start_time DESC);
CREATE INDEX IF NOT EXISTS idx_etl_jobs_status ON sys_etl_jobs(status);
CREATE INDEX IF NOT EXISTS idx_etl_job_files_job_id ON sys_etl_job_files(job_id);
CREATE INDEX IF NOT EXISTS idx_sessions_username ON sys_sessions(username);
CREATE INDEX IF NOT EXISTS idx_sessions_last_activity ON sys_sessions(last_activity);
CREATE INDEX IF NOT EXISTS idx_schema_errors_detected_at ON schema_errors(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_schema_errors_table_name ON schema_errors(table_name);
CREATE INDEX IF NOT EXISTS idx_schema_errors_resolved_at ON schema_errors(resolved_at);
CREATE INDEX IF NOT EXISTS idx_file_table_mappings_pattern ON file_table_mappings(file_pattern);
"""


def ensure_internal_schema(db_path: str = "data/database/internal.db"):
    """
    Ensure internal database has correct schema.
    Safe to call multiple times - will migrate existing tables.
    
    Args:
        db_path: Path to internal database file
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with sqlite3.connect(db_path) as conn:
            # Execute base schema
            conn.executescript(get_internal_schema_sql())
            
            # Migrate sys_audit_trail if it exists with old schema
            cursor = conn.execute("PRAGMA table_info(sys_audit_trail)")
            existing_columns = [col[1] for col in cursor.fetchall()]
            
            # Add missing columns to sys_audit_trail
            audit_migrations = [
                ('duration_ms', 'INTEGER'),
                ('record_count', 'INTEGER'),
                ('file_size', 'INTEGER')
            ]
            
            for col_name, col_type in audit_migrations:
                if col_name not in existing_columns:
                    logger.info(f"Adding missing column sys_audit_trail.{col_name}")
                    conn.execute(f"ALTER TABLE sys_audit_trail ADD COLUMN {col_name} {col_type}")
            
            # Migrate users table if needed
            cursor = conn.execute("PRAGMA table_info(sys_users)")
            user_columns = [col[1] for col in cursor.fetchall()]
            
            user_migrations = [
                ('auth_method', 'TEXT NOT NULL DEFAULT \'local\''),
                ('obtain_email_on_login', 'INTEGER DEFAULT 0'),
                ('obtain_display_name_on_login', 'INTEGER DEFAULT 0')
            ]
            
            for col_name, col_def in user_migrations:
                if col_name not in user_columns:
                    logger.info(f"Adding missing column sys_users.{col_name}")
                    # Extract just the type and default for ALTER TABLE
                    if 'NOT NULL' in col_def:
                        col_type = col_def.split('NOT NULL')[0].strip()
                        default = col_def.split('DEFAULT')[1].strip() if 'DEFAULT' in col_def else "'local'"
                        conn.execute(f"ALTER TABLE sys_users ADD COLUMN {col_name} {col_type} DEFAULT {default}")
                    else:
                        conn.execute(f"ALTER TABLE sys_users ADD COLUMN {col_name} {col_def}")
            
            # Migrate old audit log table if it exists
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='user_audit_log'")
            if cursor.fetchone():
                logger.info("Migrating old user_audit_log to sys_audit_trail")
                conn.execute("""
                    INSERT OR IGNORE INTO sys_audit_trail (timestamp, username, action, category, details, ip_address, success)
                    SELECT timestamp, username, event_type, 'authentication', event_details, ip_address, 1
                    FROM user_audit_log
                """)
                conn.execute("DROP TABLE user_audit_log")
            
            conn.commit()
            logger.info(f"Internal database schema verified/updated at {db_path}")
            
    except Exception as e:
        logger.error(f"Error ensuring internal schema: {e}", exc_info=True)
        raise


def verify_internal_schema(db_path: str = "data/database/internal.db") -> bool:
    """
    Verify internal database has correct schema.
    
    Args:
        db_path: Path to internal database file
        
    Returns:
        True if schema is correct, False otherwise
    """
    try:
        with sqlite3.connect(db_path) as conn:
            # Check users table
            cursor = conn.execute("PRAGMA table_info(sys_users)")
            user_cols = {col[1] for col in cursor.fetchall()}
            required_user_cols = {
                'id', 'username', 'password_hash', 'display_name', 'email', 'role',
                'auth_method', 'is_active', 'created_at', 'created_by', 'last_login',
                'failed_login_attempts', 'locked_until', 'obtain_email_on_login',
                'obtain_display_name_on_login'
            }
            
            if not required_user_cols.issubset(user_cols):
                missing = required_user_cols - user_cols
                logger.warning(f"Users table missing columns: {missing}")
                return False
            
            # Check sys_audit_trail table
            cursor = conn.execute("PRAGMA table_info(sys_audit_trail)")
            audit_cols = {col[1] for col in cursor.fetchall()}
            required_audit_cols = {
                'id', 'timestamp', 'username', 'action', 'category', 'success',
                'details', 'ip_address', 'user_agent', 'session_id', 'target_user',
                'target_resource', 'error_message', 'duration_ms', 'record_count', 'file_size'
            }
            
            if not required_audit_cols.issubset(audit_cols):
                missing = required_audit_cols - audit_cols
                logger.warning(f"sys_audit_trail table missing columns: {missing}")
                return False
            
            logger.info("Internal database schema verified successfully")
            return True
            
    except Exception as e:
        logger.error(f"Error verifying internal schema: {e}")
        return False


if __name__ == '__main__':
    # Run as standalone script to initialize/migrate schema
    logging.basicConfig(level=logging.INFO)
    ensure_internal_schema()
    if verify_internal_schema():
        print("✅ Internal database schema is correct")
    else:
        print("❌ Internal database schema has issues")
