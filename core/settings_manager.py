"""
Settings Manager

Manages loading and saving SIEM and SFTP configuration settings from the
internal database. Provides API for admin panel to update settings.

Author: Waqqas Hanafi
Copyright: © 2025 Calaveras County Health and Human Services Agency
"""

import sqlite3
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
from dataclasses import asdict

from .config import config, SIEMConfig, SFTPConfig
from .audit_logger import get_audit_logger, AuditCategory, AuditAction


class SettingsManager:
    """Manages application settings stored in database"""
    
    def __init__(self, db_path: str = "data/database/internal.db"):
        self.db_path = Path(db_path)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.audit_logger = get_audit_logger()
        self._ensure_default_settings()
    
    def _ensure_default_settings(self):
        """Ensure default settings exist in database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Check if SIEM config exists
                cursor = conn.execute("SELECT COUNT(*) FROM sys_siem_config WHERE id = 1")
                if cursor.fetchone()[0] == 0:
                    # Insert default SIEM config
                    conn.execute("""
                        INSERT INTO sys_siem_config (
                            id, enabled, enable_windows_event_log,
                            syslog_enabled, syslog_host, syslog_port,
                            syslog_protocol, include_sensitive_data,
                            windows_event_log_min_severity, syslog_min_severity, updated_at, updated_by
                        ) VALUES (1, 0, 0, 0, 'localhost', 514, 'UDP', 0, 'ERROR', 'ERROR', ?, 'system')
                    """, (datetime.now().isoformat(),))
                
                # Check if SFTP config exists
                cursor = conn.execute("SELECT COUNT(*) FROM sys_sftp_config WHERE id = 1")
                if cursor.fetchone()[0] == 0:
                    # Insert default SFTP config
                    conn.execute("""
                        INSERT INTO sys_sftp_config (
                            id, enabled, host, port, username, auth_method, private_key_path,
                            remote_directory, auto_download, download_interval_minutes,
                            delete_after_download, local_download_path, timeout_seconds,
                            max_retries, verify_host_key, known_hosts_path, updated_at, updated_by
                        ) VALUES (1, 0, '', 22, '', 'key', ?, '/data/exports', 0, 60, 0, ?, 30, 3, 1, ?, ?, 'system')
                    """, (
                        str(config.sftp.private_key_path),
                        str(config.sftp.local_download_path),
                        str(config.sftp.known_hosts_path),
                        datetime.now().isoformat()
                    ))
                
                # Ensure default file patterns
                cursor = conn.execute("SELECT COUNT(*) FROM sftp_file_patterns")
                if cursor.fetchone()[0] == 0:
                    patterns = ["*.txt", "*.csv"]
                    for pattern in patterns:
                        conn.execute("""
                            INSERT INTO sftp_file_patterns (pattern, enabled, created_at)
                            VALUES (?, 1, ?)
                        """, (pattern, datetime.now().isoformat()))
                
                conn.commit()
        except Exception as e:
            self.logger.error(f"Error ensuring default settings: {e}")
    
    def get_siem_settings(self) -> Dict[str, Any]:
        """Load SIEM settings from database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("SELECT * FROM sys_siem_config WHERE id = 1")
                row = cursor.fetchone()
                
                if row:
                    return dict(row)
                else:
                    return {}
        except Exception as e:
            self.logger.error(f"Error loading SIEM settings: {e}")
            return {}
    
    def get_sftp_settings(self) -> Dict[str, Any]:
        """Load SFTP settings from database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                
                # Get main config
                cursor = conn.execute("SELECT * FROM sys_sftp_config WHERE id = 1")
                row = cursor.fetchone()
                
                if not row:
                    return {}
                
                settings = dict(row)
                
                # Default auto_download to True if not set
                if 'auto_download' not in settings or settings.get('auto_download') is None:
                    settings['auto_download'] = 1
                
                # Get file patterns
                cursor = conn.execute("SELECT pattern FROM sftp_file_patterns WHERE enabled = 1")
                patterns = [row[0] for row in cursor.fetchall()]
                settings['file_patterns'] = patterns
                
                return settings
        except Exception as e:
            self.logger.error(f"Error loading SFTP settings: {e}")
            return {}
    
    def save_siem_settings(self, settings: Dict[str, Any], username: str = "system") -> bool:
        """Save SIEM settings to database"""
        try:
            # Clean up old single-file config if it exists
            old_file = Path('data/logs/siem_events.json')
            if old_file.exists():
                try:
                    old_file.unlink()
                    self.logger.info(f"Removed old JSON log file: {old_file}")
                except Exception as e:
                    self.logger.warning(f"Could not remove old log file: {e}")
            
            # Ensure new directory structure exists
            json_log_path = settings.get('json_log_path', 'data/logs/siem')
            if json_log_path.endswith('siem_events.json'):
                json_log_path = 'data/logs/siem'
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    UPDATE sys_siem_config SET
                        enabled = ?,
                        enable_windows_event_log = ?,
                        syslog_enabled = ?,
                        syslog_host = ?,
                        syslog_port = ?,
                        syslog_protocol = ?,
                        include_sensitive_data = ?,
                        windows_event_log_min_severity = ?,
                        syslog_min_severity = ?,
                        updated_at = ?,
                        updated_by = ?
                    WHERE id = 1
                """, (
                    1 if settings.get('enabled') else 0,
                    1 if settings.get('enable_windows_event_log') else 0,
                    1 if settings.get('syslog_enabled') else 0,
                    settings.get('syslog_host', ''),
                    settings.get('syslog_port', 514),
                    settings.get('syslog_protocol', 'UDP'),
                    1 if settings.get('include_sensitive_data') else 0,
                    settings.get('windows_event_log_min_severity', 'ERROR'),
                    settings.get('syslog_min_severity', 'ERROR'),
                    datetime.now().isoformat(),
                    username
                ))
                conn.commit()
            
            # Update runtime config
            config.siem.enabled = settings.get('enabled', False)
            config.siem.enable_windows_event_log = settings.get('enable_windows_event_log', False)
            config.siem.syslog_enabled = settings.get('syslog_enabled', False)
            config.siem.syslog_host = settings.get('syslog_host', 'localhost')
            config.siem.syslog_port = settings.get('syslog_port', 514)
            config.siem.syslog_protocol = settings.get('syslog_protocol', 'UDP')
            config.siem.windows_event_log_min_severity = settings.get('windows_event_log_min_severity', 'ERROR')
            config.siem.syslog_min_severity = settings.get('syslog_min_severity', 'ERROR')
            
            # Audit log
            self.audit_logger.log(
                username=username,
                action=AuditAction.CONFIG_CHANGED,
                category=AuditCategory.SYSTEM,
                success=True,
                details="SIEM settings updated",
                target_resource="sys_siem_config"
            )
            
            self.logger.info(f"SIEM settings updated by {username}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving SIEM settings: {e}")
            return False
    
    def save_sftp_settings(self, settings: Dict[str, Any], username: str = "system") -> bool:
        """Save SFTP settings to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    UPDATE sys_sftp_config SET
                        enabled = ?,
                        host = ?,
                        port = ?,
                        username = ?,
                        auth_method = ?,
                        private_key_path = ?,
                        remote_directory = ?,
                        auto_download = ?,
                        download_interval_minutes = ?,
                        delete_after_download = ?,
                        local_download_path = ?,
                        timeout_seconds = ?,
                        max_retries = ?,
                        verify_host_key = ?,
                        known_hosts_path = ?,
                        updated_at = ?,
                        updated_by = ?
                    WHERE id = 1
                """, (
                    1 if settings.get('enabled') else 0,
                    settings.get('host', ''),
                    settings.get('port', 22),
                    settings.get('username', ''),
                    settings.get('auth_method', 'key'),
                    settings.get('private_key_path', str(config.sftp.private_key_path)),
                    settings.get('remote_directory', '/data/exports'),
                    1 if settings.get('auto_download') else 0,
                    settings.get('download_interval_minutes', 60),
                    1 if settings.get('delete_after_download') else 0,
                    settings.get('local_download_path', str(config.sftp.local_download_path)),
                    settings.get('timeout_seconds', 30),
                    settings.get('max_retries', 3),
                    1 if settings.get('verify_host_key', True) else 0,
                    settings.get('known_hosts_path', str(config.sftp.known_hosts_path)),
                    datetime.now().isoformat(),
                    username
                ))
                
                # Update file patterns if provided
                if 'file_patterns' in settings:
                    # Clear existing patterns
                    conn.execute("DELETE FROM sftp_file_patterns")
                    
                    # Insert new patterns
                    for pattern in settings['file_patterns']:
                        if pattern.strip():  # Only non-empty patterns
                            conn.execute("""
                                INSERT INTO sftp_file_patterns (pattern, enabled, created_at)
                                VALUES (?, 1, ?)
                            """, (pattern.strip(), datetime.now().isoformat()))
                
                conn.commit()
            
            # Update runtime config
            config.sftp.enabled = settings.get('enabled', False)
            config.sftp.host = settings.get('host', '')
            config.sftp.port = settings.get('port', 22)
            config.sftp.username = settings.get('username', '')
            config.sftp.auth_method = settings.get('auth_method', 'key')
            
            # Update authentication-related settings
            if 'private_key_path' in settings:
                from pathlib import Path
                config.sftp.private_key_path = Path(settings['private_key_path'])
            if 'password' in settings:
                config.sftp.password = settings.get('password')
            if 'private_key_passphrase' in settings:
                config.sftp.private_key_passphrase = settings.get('private_key_passphrase')
            if 'key_format' in settings:
                config.sftp.key_format = settings.get('key_format', 'auto')
            
            config.sftp.remote_directory = settings.get('remote_directory', '/data/exports')
            config.sftp.auto_download = settings.get('auto_download', True)  # Default to True
            config.sftp.delete_after_download = settings.get('delete_after_download', False)
            config.sftp.timeout_seconds = settings.get('timeout_seconds', 30)
            config.sftp.max_retries = settings.get('max_retries', 3)
            config.sftp.verify_host_key = settings.get('verify_host_key', True)
            
            if 'known_hosts_path' in settings:
                from pathlib import Path
                config.sftp.known_hosts_path = Path(settings['known_hosts_path'])
            if 'local_download_path' in settings:
                from pathlib import Path
                config.sftp.local_download_path = Path(settings['local_download_path'])
            
            if 'file_patterns' in settings:
                config.sftp.file_patterns = [p.strip() for p in settings['file_patterns'] if p.strip()]
            
            # Audit log
            self.audit_logger.log(
                username=username,
                action=AuditAction.CONFIG_CHANGED,
                category=AuditCategory.SYSTEM,
                success=True,
                details="SFTP settings updated",
                target_resource="sys_sftp_config"
            )
            
            self.logger.info(f"SFTP settings updated by {username}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving SFTP settings: {e}")
            return False
    
    def get_database_settings(self) -> Dict[str, Any]:
        """Load database settings from database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Ensure table exists
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS database_config (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
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
                    )
                """)
                conn.commit()
                
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("SELECT * FROM database_config WHERE id = 1")
                row = cursor.fetchone()
                
                if row:
                    return dict(row)
                else:
                    # Return defaults
                    return {
                        'db_type': 'sqlite',
                        'path': str(config.database.path),
                        'mssql_server': config.database.mssql_server,
                        'mssql_port': config.database.mssql_port,
                        'mssql_database': config.database.mssql_database,
                        'mssql_username': config.database.mssql_username,
                        'mssql_password': config.database.mssql_password,
                        'mssql_trusted_connection': config.database.mssql_trusted_connection,
                        'mssql_driver': config.database.mssql_driver,
                        'postgresql_host': config.database.postgresql_host,
                        'postgresql_port': config.database.postgresql_port,
                        'postgresql_database': config.database.postgresql_database,
                        'postgresql_username': config.database.postgresql_username,
                        'postgresql_password': config.database.postgresql_password,
                        'mysql_host': config.database.mysql_host,
                        'mysql_port': config.database.mysql_port,
                        'mysql_database': config.database.mysql_database,
                        'mysql_username': config.database.mysql_username,
                        'mysql_password': config.database.mysql_password,
                        'connection_timeout': config.database.connection_timeout,
                        'max_connections': config.database.max_connections
                    }
        except Exception as e:
            self.logger.error(f"Error loading database settings: {e}")
            return {}
    
    def save_database_settings(self, settings: Dict[str, Any], username: str = "system") -> bool:
        """Save database settings to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Check if table exists, create if not
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS database_config (
                        id INTEGER PRIMARY KEY,
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
                    )
                """)
                
                # Check if record exists
                cursor = conn.execute("SELECT COUNT(*) FROM database_config WHERE id = 1")
                if cursor.fetchone()[0] == 0:
                    # Insert new record
                    conn.execute("""
                        INSERT INTO database_config (
                            id, db_type, path, mssql_server, mssql_port, mssql_database,
                            mssql_username, mssql_password, mssql_trusted_connection,
                            mssql_driver, postgresql_host, postgresql_port, postgresql_database,
                            postgresql_username, postgresql_password, mysql_host, mysql_port,
                            mysql_database, mysql_username, mysql_password,
                            connection_timeout, max_connections, updated_at, updated_by
                        ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        settings.get('db_type', 'sqlite'),
                        settings.get('path', str(config.database.path)),
                        settings.get('mssql_server', ''),
                        settings.get('mssql_port', 1433),
                        settings.get('mssql_database', ''),
                        settings.get('mssql_username', ''),
                        settings.get('mssql_password', ''),
                        1 if settings.get('mssql_trusted_connection', True) else 0,
                        settings.get('mssql_driver', 'ODBC Driver 17 for SQL Server'),
                        settings.get('postgresql_host', ''),
                        settings.get('postgresql_port', 5432),
                        settings.get('postgresql_database', ''),
                        settings.get('postgresql_username', ''),
                        settings.get('postgresql_password', ''),
                        settings.get('mysql_host', ''),
                        settings.get('mysql_port', 3306),
                        settings.get('mysql_database', ''),
                        settings.get('mysql_username', ''),
                        settings.get('mysql_password', ''),
                        settings.get('connection_timeout', 30),
                        settings.get('max_connections', 10),
                        datetime.now().isoformat(),
                        username
                    ))
                else:
                    # Update existing record (only update password if provided)
                    update_fields = []
                    update_values = []
                    
                    if 'mssql_password' in settings and settings['mssql_password'] != '********':
                        update_fields.append('mssql_password = ?')
                        update_values.append(settings.get('mssql_password', ''))
                    
                    update_fields.extend([
                        'db_type = ?', 'path = ?', 'mssql_server = ?', 'mssql_port = ?',
                        'mssql_database = ?', 'mssql_username = ?', 'mssql_trusted_connection = ?',
                        'mssql_driver = ?', 'postgresql_host = ?', 'postgresql_port = ?',
                        'postgresql_database = ?', 'postgresql_username = ?', 'postgresql_password = ?',
                        'mysql_host = ?', 'mysql_port = ?', 'mysql_database = ?',
                        'mysql_username = ?', 'mysql_password = ?',
                        'connection_timeout = ?', 'max_connections = ?',
                        'updated_at = ?', 'updated_by = ?'
                    ])
                    
                    update_values.extend([
                        settings.get('db_type', 'sqlite'),
                        settings.get('path', str(config.database.path)),
                        settings.get('mssql_server', ''),
                        settings.get('mssql_port', 1433),
                        settings.get('mssql_database', ''),
                        settings.get('mssql_username', ''),
                        1 if settings.get('mssql_trusted_connection', True) else 0,
                        settings.get('mssql_driver', 'ODBC Driver 17 for SQL Server'),
                        settings.get('postgresql_host', ''),
                        settings.get('postgresql_port', 5432),
                        settings.get('postgresql_database', ''),
                        settings.get('postgresql_username', ''),
                        settings.get('postgresql_password', ''),
                        settings.get('mysql_host', ''),
                        settings.get('mysql_port', 3306),
                        settings.get('mysql_database', ''),
                        settings.get('mysql_username', ''),
                        settings.get('mysql_password', ''),
                        settings.get('connection_timeout', 30),
                        settings.get('max_connections', 10),
                        datetime.now().isoformat(),
                        username
                    ])
                    
                    update_values.append(1)  # WHERE id = 1
                    
                    conn.execute(f"""
                        UPDATE database_config SET
                            {', '.join(update_fields)}
                        WHERE id = ?
                    """, update_values)
                
                conn.commit()
            
            # Update runtime config
            config.database.db_type = settings.get('db_type', 'sqlite')
            if settings.get('db_type') == 'sqlite':
                config.database.path = Path(settings.get('path', config.database.path))
            elif settings.get('db_type') in ['mssql', 'azuresql']:
                config.database.mssql_server = settings.get('mssql_server', '')
                config.database.mssql_port = settings.get('mssql_port', 1433)
                config.database.mssql_database = settings.get('mssql_database', '')
                config.database.mssql_username = settings.get('mssql_username', '')
                if settings.get('mssql_password') != '********':
                    config.database.mssql_password = settings.get('mssql_password', '')
                config.database.mssql_trusted_connection = settings.get('mssql_trusted_connection', True) and settings.get('db_type') != 'azuresql'
                config.database.mssql_driver = settings.get('mssql_driver', 'ODBC Driver 17 for SQL Server')
            elif settings.get('db_type') == 'postgresql':
                config.database.postgresql_host = settings.get('postgresql_host', '')
                config.database.postgresql_port = settings.get('postgresql_port', 5432)
                config.database.postgresql_database = settings.get('postgresql_database', '')
                config.database.postgresql_username = settings.get('postgresql_username', '')
                if settings.get('postgresql_password') != '********':
                    config.database.postgresql_password = settings.get('postgresql_password', '')
            elif settings.get('db_type') == 'mysql':
                config.database.mysql_host = settings.get('mysql_host', '')
                config.database.mysql_port = settings.get('mysql_port', 3306)
                config.database.mysql_database = settings.get('mysql_database', '')
                config.database.mysql_username = settings.get('mysql_username', '')
                if settings.get('mysql_password') != '********':
                    config.database.mysql_password = settings.get('mysql_password', '')
            
            config.database.connection_timeout = settings.get('connection_timeout', 30)
            config.database.max_connections = settings.get('max_connections', 10)
            
            # Audit log
            self.audit_logger.log(
                username=username,
                action=AuditAction.CONFIG_CHANGED,
                category=AuditCategory.SYSTEM,
                success=True,
                details="Database settings updated",
                target_resource="database_config"
            )
            
            self.logger.info(f"Database settings updated by {username}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving database settings: {e}")
            return False
    
    def load_settings_into_config(self):
        """Load settings from database into runtime config"""
        # Load SIEM settings
        siem_settings = self.get_siem_settings()
        if siem_settings:
            config.siem.enabled = bool(siem_settings.get('enabled'))
            config.siem.enable_windows_event_log = bool(siem_settings.get('enable_windows_event_log'))
            config.siem.syslog_enabled = bool(siem_settings.get('syslog_enabled'))
            config.siem.syslog_host = siem_settings.get('syslog_host', 'localhost')
            config.siem.syslog_port = siem_settings.get('syslog_port', 514)
            config.siem.syslog_protocol = siem_settings.get('syslog_protocol', 'UDP')
        
        # Load SFTP settings
        sftp_settings = self.get_sftp_settings()
        if sftp_settings:
            config.sftp.enabled = bool(sftp_settings.get('enabled'))
            config.sftp.host = sftp_settings.get('host', '')
            config.sftp.port = sftp_settings.get('port', 22)
            config.sftp.username = sftp_settings.get('username', '')
            config.sftp.auth_method = sftp_settings.get('auth_method', 'key')
            config.sftp.remote_directory = sftp_settings.get('remote_directory', '/data/exports')
            config.sftp.auto_download = bool(sftp_settings.get('auto_download', True))  # Default to True
            config.sftp.delete_after_download = bool(sftp_settings.get('delete_after_download'))
            
            if 'file_patterns' in sftp_settings:
                config.sftp.file_patterns = sftp_settings['file_patterns']
        
        # Load database settings
        db_settings = self.get_database_settings()
        if db_settings:
            config.database.db_type = db_settings.get('db_type', 'sqlite')
            if db_settings.get('db_type') == 'sqlite':
                config.database.path = Path(db_settings.get('path', config.database.path))
            elif db_settings.get('db_type') in ['mssql', 'azuresql']:
                config.database.mssql_server = db_settings.get('mssql_server', '')
                config.database.mssql_port = db_settings.get('mssql_port', 1433)
                config.database.mssql_database = db_settings.get('mssql_database', '')
                config.database.mssql_username = db_settings.get('mssql_username', '')
                config.database.mssql_password = db_settings.get('mssql_password', '')
                config.database.mssql_trusted_connection = bool(db_settings.get('mssql_trusted_connection', True)) and db_settings.get('db_type') != 'azuresql'
                config.database.mssql_driver = db_settings.get('mssql_driver', 'ODBC Driver 17 for SQL Server')
            elif db_settings.get('db_type') == 'postgresql':
                config.database.postgresql_host = db_settings.get('postgresql_host', '')
                config.database.postgresql_port = db_settings.get('postgresql_port', 5432)
                config.database.postgresql_database = db_settings.get('postgresql_database', '')
                config.database.postgresql_username = db_settings.get('postgresql_username', '')
                config.database.postgresql_password = db_settings.get('postgresql_password', '')
            elif db_settings.get('db_type') == 'mysql':
                config.database.mysql_host = db_settings.get('mysql_host', '')
                config.database.mysql_port = db_settings.get('mysql_port', 3306)
                config.database.mysql_database = db_settings.get('mysql_database', '')
                config.database.mysql_username = db_settings.get('mysql_username', '')
                config.database.mysql_password = db_settings.get('mysql_password', '')
            
            config.database.connection_timeout = db_settings.get('connection_timeout', 30)
            config.database.max_connections = db_settings.get('max_connections', 10)


# Global settings manager instance
_settings_manager: Optional[SettingsManager] = None


def get_settings_manager() -> SettingsManager:
    """Get the global settings manager instance"""
    global _settings_manager
    if _settings_manager is None:
        _settings_manager = SettingsManager()
    return _settings_manager

