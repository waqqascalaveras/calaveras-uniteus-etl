"""
Configuration Management

Unified configuration management system that consolidates all application
settings with environment variable support, validation, and centralized
path management for data, logs, and database locations.

Author: Waqqas Hanafi
Copyright: © 2025 Calaveras County Health and Human Services Agency
"""

import os
import json
import logging
import hashlib
import secrets
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum


class Environment(Enum):
    """Supported environments"""
    DEVELOPMENT = "development"
    TESTING = "testing"
    PRODUCTION = "production"


class LogLevel(Enum):
    """Supported log levels"""
    DEBUG = "DEBUG"
    INFO = "INFO" 
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class DatabaseConfig:
    """Database configuration with connection settings"""
    # Database type: 'sqlite', 'mssql', 'azuresql', 'postgresql', or 'mysql'
    db_type: str = "sqlite"
    
    # SQLite settings
    path: Path = field(default_factory=lambda: Path("data/database/chhsca_data.db"))
    journal_mode: str = "WAL"  # Write-Ahead Logging for better concurrency
    
    # MS SQL Server / Azure SQL settings
    mssql_server: str = "localhost"
    mssql_port: int = 1433
    mssql_database: str = "chhsca_data"
    mssql_username: str = ""
    mssql_password: str = ""
    mssql_trusted_connection: bool = True  # Windows Authentication (on-premises only)
    mssql_driver: str = "ODBC Driver 17 for SQL Server"
    
    # PostgreSQL settings
    postgresql_host: str = "localhost"
    postgresql_port: int = 5432
    postgresql_database: str = "chhsca_data"
    postgresql_username: str = "postgres"
    postgresql_password: str = ""
    
    # MySQL settings
    mysql_host: str = "localhost"
    mysql_port: int = 3306
    mysql_database: str = "chhsca_data"
    mysql_username: str = "root"
    mysql_password: str = ""
    
    # Common settings
    connection_timeout: int = 30
    max_connections: int = 10
    enable_foreign_keys: bool = True
    
    def __post_init__(self):
        """Ensure database directory exists"""
        if self.db_type == "sqlite":
            self.path.parent.mkdir(parents=True, exist_ok=True)


@dataclass 
class DirectoryConfig:
    """Directory structure configuration"""
    project_root: Path = field(default_factory=lambda: Path.cwd())
    data_dir: Path = field(default_factory=lambda: Path("data"))
    input_dir: Path = field(default_factory=lambda: Path("temp_data_files"))
    output_dir: Path = field(default_factory=lambda: Path("data/output"))
    logs_dir: Path = field(default_factory=lambda: Path("data/logs"))
    database_dir: Path = field(default_factory=lambda: Path("data/database"))
    backup_dir: Path = field(default_factory=lambda: Path("data/backups"))
    
    def __post_init__(self):
        """Ensure all directories exist"""
        for dir_path in [self.data_dir, self.input_dir, self.output_dir, 
                        self.logs_dir, self.database_dir, self.backup_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)


@dataclass
class LoggingConfig:
    """Logging configuration"""
    level: LogLevel = LogLevel.INFO
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format: str = "%Y-%m-%d %H:%M:%S"
    file_rotation_size: int = 10 * 1024 * 1024  # 10MB
    file_retention_count: int = 5
    enable_console: bool = True
    enable_file: bool = True


@dataclass
class ETLConfig:
    """ETL processing configuration"""
    batch_size: int = 1000
    max_workers: int = 4
    timeout_seconds: int = 300
    retry_attempts: int = 3
    skip_processed_files: bool = True
    force_reprocess: bool = False
    latest_only: bool = False
    # Filename prefixes to ignore when extracting table names (e.g., 'SAMPLE', 'TEST', 'PROD', 'CHHSCA')
    # These will be skipped when parsing filenames like: SAMPLE_chhsca_people_20250828.txt → people
    ignored_filename_prefixes: List[str] = field(default_factory=lambda: ['SAMPLE', 'TEST', 'CHHSCA'])
    # File patterns to discover during ETL (glob patterns)
    file_patterns: List[str] = field(default_factory=lambda: ["*.txt", "*.csv", "*.tsv"])
    # File extensions to recognize (used in table name extraction)
    recognized_extensions: List[str] = field(default_factory=lambda: ['.txt', '.csv', '.tsv'])


@dataclass
class WebConfig:
    """Web interface configuration"""
    host: str = "0.0.0.0" 
    port: int = 8000
    reload: bool = False  # Set to True in development
    log_level: str = "info"
    cors_origins: List[str] = field(default_factory=lambda: ["*"])
    static_dir: str = "static"
    templates_dir: str = "templates"
    use_https: bool = False
    cert_file: Optional[str] = None
    key_file: Optional[str] = None


@dataclass
class SecurityConfig:
    """Security and PHI protection configuration"""
    enable_phi_hashing: bool = True  # Toggle PHI hashing on/off
    hash_on_import: bool = True  # Hash during ETL import (recommended)
    hash_on_export: bool = True  # Hash in reports/exports (additional layer)
    
    # Salt for one-way hashing (load from environment or generate)
    # IMPORTANT: Store this securely and never commit to version control
    phi_hash_salt: str = field(default_factory=lambda: os.getenv(
        'PHI_HASH_SALT', 
        secrets.token_hex(32)  # Generate 64-char hex string if not in env
    ))
    
    # Fields to hash by table (PII/PHI fields)
    fields_to_hash: Dict[str, List[str]] = field(default_factory=lambda: {
        'people': [
            'person_id',
            'first_name', 
            'middle_name',
            'last_name',
            'preferred_name',
            'person_email_address',
            'person_phone_number',
            'current_person_address_line1',
            'current_person_address_line2',
            'medicaid_id',
            'medicare_id',
            'person_external_id'
        ],
        'cases': [
            'case_id',
            'person_id',
            'case_external_id'
        ],
        'referrals': [
            'referral_id',
            'case_id',
            'person_id',
            'referral_created_by_id',
            'referral_external_id'
        ],
        'employees': [
            'employee_id',
            'first_name',
            'last_name',
            'email',
            'phone_number',
            'employee_external_id'
        ],
        'assistance_requests': [
            'assistance_request_id',
            'person_id',
            'case_id',
            'person_first_name',
            'person_last_name',
            'person_date_of_birth',
            'person_middle_name',
            'person_preferred_name',
            'person_email_address',
            'person_phone_number',
            'address_line_1',
            'address_line_2'
        ],
        'assistance_requests_supplemental_responses': [
            'ar_supplemental_response_id',
            'assistance_request_id'
        ],
        'resource_lists': [
            'resource_list_id'
        ],
        'resource_list_shares': [
            'share_id',
            'resource_list_id',
            'person_id'
        ]
    })
    
    def hash_value(self, value: str) -> str:
        """
        One-way hash a value using SHA-256 with salt
        
        Args:
            value: The value to hash (e.g., person_id, name, email)
            
        Returns:
            64-character hexadecimal hash string
        """
        if not value or value == '' or str(value).lower() in ['nan', 'none', 'null']:
            return value  # Don't hash empty/null values
        
        # Salt the value (prepend and append for extra security)
        salted = f"{self.phi_hash_salt}{value}{self.phi_hash_salt}"
        
        # Generate SHA-256 hash
        hash_obj = hashlib.sha256(salted.encode('utf-8'))
        return hash_obj.hexdigest()
    
    def should_hash_field(self, table_name: str, field_name: str) -> bool:
        """
        Check if a specific field should be hashed
        
        Args:
            table_name: Name of the table
            field_name: Name of the field
            
        Returns:
            True if field should be hashed, False otherwise
        """
        if not self.enable_phi_hashing:
            return False
        
        table_fields = self.fields_to_hash.get(table_name, [])
        return field_name in table_fields


@dataclass
class SIEMConfig:
    """SIEM integration configuration"""
    enabled: bool = False
    enable_windows_event_log: bool = False  # Windows Event Viewer integration for fatal errors
    syslog_enabled: bool = False  # Forward to remote syslog/SIEM server
    syslog_host: str = "localhost"
    syslog_port: int = 514
    syslog_protocol: str = "UDP"  # UDP or TCP
    include_sensitive_data: bool = False  # Whether to include PHI in SIEM logs
    log_categories: List[str] = field(default_factory=lambda: [
        "authentication", "etl_operations", "data_access", "system_events", "security_events"
    ])
    # Minimum severity levels for each destination (EMERGENCY=0, ALERT=1, CRITICAL=2, ERROR=3, WARNING=4, NOTICE=5, INFO=6, DEBUG=7)
    windows_event_log_min_severity: str = "ERROR"  # Windows Event Log: ERROR and above (3-0)
    syslog_min_severity: str = "ERROR"  # Syslog/SIEM: ERROR and above (3-0)


@dataclass
class SFTPConfig:
    """SFTP connection configuration"""
    enabled: bool = False
    host: str = "chhssftp.uniteus.com"  # UniteUs SFTP server
    port: int = 22
    username: str = "chhsca_data_prod"  # UniteUs provided username
    # Authentication method
    auth_method: str = "key"  # "key" or "password"
    # Key-based authentication (recommended)
    private_key_path: Path = field(default_factory=lambda: Path("keys/calco-uniteus-sftp"))
    private_key_passphrase: Optional[str] = None  # If key is encrypted
    key_format: str = "auto"  # "auto", "putty", "openssh", "pem", "ssh2"
    # Password authentication (fallback)
    password: Optional[str] = None
    # Remote paths
    remote_directory: str = "/data/exports"
    file_patterns: List[str] = field(default_factory=lambda: ["*.txt", "*.csv"])
    # Download settings
    auto_download: bool = False  # Automatically download before ETL
    download_interval_minutes: int = 60  # How often to check for new files
    delete_after_download: bool = False  # Remove files from server after download
    local_download_path: Path = field(default_factory=lambda: Path("data/input"))  # Changed from temp_data_files
    # Connection settings
    timeout_seconds: int = 30
    max_retries: int = 3
    verify_host_key: bool = True
    known_hosts_path: Path = field(default_factory=lambda: Path("data/sftp/known_hosts"))
    
    def __post_init__(self):
        """Ensure directories exist"""
        if self.private_key_path:
            self.private_key_path.parent.mkdir(parents=True, exist_ok=True)
        if self.known_hosts_path:
            self.known_hosts_path.parent.mkdir(parents=True, exist_ok=True)
        if self.local_download_path:
            self.local_download_path.mkdir(parents=True, exist_ok=True)


@dataclass
class DataQualityConfig:
    """Data quality and validation rules"""
    
    # Expected table schemas
    expected_tables: Dict[str, List[str]] = field(default_factory=lambda: {
        'people': ['person_id', 'first_name', 'last_name', 'people_created_at'],
        'employees': ['employee_id', 'first_name', 'last_name', 'employee_created_at'],
        'cases': ['case_id', 'person_id', 'case_created_at', 'case_status'],
        'referrals': ['referral_id', 'person_id', 'case_id', 'referral_created_at'],
        'assistance_requests': ['assistance_request_id', 'person_id', 'case_id', 'created_at'],
        'assistance_requests_supplemental_responses': ['ar_supplemental_response_id', 'assistance_request_id', 'created_at'],
        'resource_lists': ['id', 'resource_list_id', 'resource_list_created_at'],
        'resource_list_shares': ['id', 'resource_list_id', 'person_id', 'share_event_origin']
    })
    
    # Date fields that need validation
    date_fields: Dict[str, List[str]] = field(default_factory=lambda: {
        'people': ['people_created_at', 'people_updated_at', 'date_of_birth'],
        'employees': ['user_created_at', 'user_updated_at', 'employee_created_at', 'employee_updated_at'],
        'cases': ['case_created_at', 'case_updated_at', 'ar_submitted_on', 'case_processed_at'],
        'referrals': ['referral_created_at', 'referral_updated_at', 'referral_sent_at'],
        'assistance_requests': ['created_at', 'updated_at'],
        'assistance_requests_supplemental_responses': ['created_at', 'updated_at'],
        'resource_lists': ['resource_list_created_at', 'resource_list_updated_at'],
        'resource_list_shares': []
    })
    
    # Boolean fields that need normalization
    boolean_fields: Dict[str, List[str]] = field(default_factory=lambda: {
        'people': ['is_veteran', 'mil_discharged_due_to_disability', 'mil_service_connected_disability'],
        'employees': [],
        'cases': ['is_sensitive'],
        'referrals': [],
        'assistance_requests': ['mil_discharged_due_to_disability', 'mil_service_connected_disability'],
        'assistance_requests_supplemental_responses': [],
        'resource_lists': [],
        'resource_list_shares': []
    })
    
    # Required fields that cannot be null
    required_fields: Dict[str, List[str]] = field(default_factory=lambda: {
        'people': ['person_id'],
        'employees': ['employee_id'],
        'cases': ['case_id', 'person_id'],
        'referrals': ['referral_id'],
        'assistance_requests': ['assistance_request_id'],
        'assistance_requests_supplemental_responses': ['ar_supplemental_response_id', 'assistance_request_id'],
        'resource_lists': ['id', 'resource_list_id'],
        'resource_list_shares': ['id', 'resource_list_id']
    })
    
    # Primary key fields for duplicate detection
    primary_keys: Dict[str, str] = field(default_factory=lambda: {
        'people': 'person_id',
        'employees': 'employee_id', 
        'cases': 'case_id',
        'referrals': 'referral_id',
        'assistance_requests': 'assistance_request_id',
        'assistance_requests_supplemental_responses': 'ar_supplemental_response_id',
        'resource_lists': 'id',
        'resource_list_shares': 'id'
    })


class UnifiedConfig:
    """
    Central configuration management system
    Implements singleton pattern and environment-aware configuration
    Loads from .config.json file with environment variable overrides
    """
    
    _instance: Optional['UnifiedConfig'] = None
    _initialized: bool = False
    _config_file = Path(".config.json")
    _json_config: Optional[Dict[str, Any]] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        # Load JSON configuration file if it exists
        self._load_json_config()
        
        # Load environment (from JSON or env var)
        env_mode = self._get_config_value('environment', 'mode', default='development')
        # Ensure we have a valid string value
        if not env_mode or not isinstance(env_mode, str):
            env_mode = 'development'
        env_var = os.getenv('ETL_ENVIRONMENT')
        if env_var:
            env_mode = env_var
        self.environment = Environment(env_mode)
        
        # Initialize configuration sections
        self.database = self._load_database_config()
        self.directories = self._load_directory_config()
        self.logging = self._load_logging_config()
        self.etl = self._load_etl_config()
        self.web = self._load_web_config()
        self.security = self._load_security_config()
        self.data_quality = self._load_data_quality_config()
        self.siem = self._load_siem_config()
        self.sftp = self._load_sftp_config()
        
        self._initialized = True
    
    def _load_json_config(self):
        """Load configuration from .config.json file"""
        if self._config_file.exists():
            try:
                with open(self._config_file, 'r', encoding='utf-8') as f:
                    self._json_config = json.load(f)
                logging.getLogger(__name__).info(f"Loaded configuration from {self._config_file}")
            except (json.JSONDecodeError, IOError) as e:
                logging.getLogger(__name__).warning(f"Error loading {self._config_file}: {e}. Using defaults.")
                self._json_config = None
        else:
            logging.getLogger(__name__).debug(f"Config file {self._config_file} not found. Using defaults.")
            self._json_config = None
    
    def _get_config_value(self, *keys, default=None):
        """
        Get a value from JSON config using nested keys
        Example: _get_config_value('database', 'sqlite', 'path', default='data/database/chhsca_data.db')
        """
        if not self._json_config:
            return default
        
        value = self._json_config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        # Skip documentation keys (keys starting with _)
        if isinstance(value, dict):
            return {k: v for k, v in value.items() if not k.startswith('_')} if value else default
        
        return value if value is not None else default
    
    def _load_database_config(self) -> DatabaseConfig:
        """Load database configuration from JSON and environment overrides"""
        config = DatabaseConfig()
        
        # Load from JSON config
        db_type = self._get_config_value('database', 'type', default='sqlite')
        config.db_type = os.getenv('ETL_DATABASE_TYPE', db_type)
        
        if config.db_type == 'sqlite':
            sqlite_path = self._get_config_value('database', 'sqlite', 'path', default='data/database/chhsca_data.db')
            config.path = Path(os.getenv('ETL_DATABASE_PATH', sqlite_path))
            config.journal_mode = self._get_config_value('database', 'sqlite', 'journal_mode', default='WAL')
        elif config.db_type in ['mssql', 'azuresql']:
            mssql_config = self._get_config_value('database', 'mssql', default={})
            config.mssql_server = os.getenv('ETL_MSSQL_SERVER', mssql_config.get('server', 'localhost'))
            config.mssql_port = int(os.getenv('ETL_MSSQL_PORT', str(mssql_config.get('port', 1433))))
            config.mssql_database = os.getenv('ETL_MSSQL_DATABASE', mssql_config.get('database', 'chhsca_data'))
            config.mssql_username = os.getenv('ETL_MSSQL_USERNAME', mssql_config.get('username', ''))
            config.mssql_password = os.getenv('ETL_MSSQL_PASSWORD', mssql_config.get('password', ''))
            config.mssql_trusted_connection = os.getenv('ETL_MSSQL_TRUSTED_CONNECTION', '').lower() == 'true' if os.getenv('ETL_MSSQL_TRUSTED_CONNECTION') else mssql_config.get('trusted_connection', True)
            config.mssql_driver = os.getenv('ETL_MSSQL_DRIVER', mssql_config.get('driver', 'ODBC Driver 17 for SQL Server'))
        elif config.db_type == 'postgresql':
            pg_config = self._get_config_value('database', 'postgresql', default={})
            config.postgresql_host = os.getenv('ETL_POSTGRESQL_HOST', pg_config.get('host', 'localhost'))
            config.postgresql_port = int(os.getenv('ETL_POSTGRESQL_PORT', str(pg_config.get('port', 5432))))
            config.postgresql_database = os.getenv('ETL_POSTGRESQL_DATABASE', pg_config.get('database', 'chhsca_data'))
            config.postgresql_username = os.getenv('ETL_POSTGRESQL_USERNAME', pg_config.get('username', 'postgres'))
            config.postgresql_password = os.getenv('ETL_POSTGRESQL_PASSWORD', pg_config.get('password', ''))
        elif config.db_type == 'mysql':
            mysql_config = self._get_config_value('database', 'mysql', default={})
            config.mysql_host = os.getenv('ETL_MYSQL_HOST', mysql_config.get('host', 'localhost'))
            config.mysql_port = int(os.getenv('ETL_MYSQL_PORT', str(mysql_config.get('port', 3306))))
            config.mysql_database = os.getenv('ETL_MYSQL_DATABASE', mysql_config.get('database', 'chhsca_data'))
            config.mysql_username = os.getenv('ETL_MYSQL_USERNAME', mysql_config.get('username', 'root'))
            config.mysql_password = os.getenv('ETL_MYSQL_PASSWORD', mysql_config.get('password', ''))
        
        # Common settings
        common_config = self._get_config_value('database', 'common', default={})
        config.connection_timeout = int(os.getenv('ETL_DATABASE_TIMEOUT', str(common_config.get('connection_timeout', 30))))
        config.max_connections = int(os.getenv('ETL_DATABASE_MAX_CONNECTIONS', str(common_config.get('max_connections', 10))))
        config.enable_foreign_keys = common_config.get('enable_foreign_keys', True)
        
        return config
    
    def _reload_database_config(self):
        """Reload database configuration from settings manager"""
        from .settings_manager import get_settings_manager
        settings_manager = get_settings_manager()
        settings_manager.load_settings_into_config()
    
    def _load_directory_config(self) -> DirectoryConfig:
        """Load directory configuration from JSON and environment overrides"""
        dirs_config = self._get_config_value('directories', default={})
        config = DirectoryConfig()
        
        config.project_root = Path(os.getenv('ETL_PROJECT_ROOT', dirs_config.get('project_root', '.')))
        config.data_dir = Path(os.getenv('ETL_DATA_DIR', dirs_config.get('data_dir', 'data')))
        config.input_dir = Path(os.getenv('ETL_INPUT_DIR', dirs_config.get('input_dir', 'temp_data_files')))
        config.output_dir = Path(os.getenv('ETL_OUTPUT_DIR', dirs_config.get('output_dir', 'data/output')))
        config.logs_dir = Path(os.getenv('ETL_LOGS_DIR', dirs_config.get('logs_dir', 'data/logs')))
        config.database_dir = Path(dirs_config.get('database_dir', 'data/database'))
        config.backup_dir = Path(dirs_config.get('backup_dir', 'data/backups'))
        
        return config
    
    def _load_logging_config(self) -> LoggingConfig:
        """Load logging configuration from JSON and environment overrides"""
        log_config = self._get_config_value('logging', default={})
        config = LoggingConfig()
        
        # Load from JSON or environment
        log_level = os.getenv('ETL_LOG_LEVEL', log_config.get('level', 'INFO'))
        try:
            config.level = LogLevel(log_level.upper())
        except ValueError:
            config.level = LogLevel.INFO
        
        config.format = os.getenv('ETL_LOG_FORMAT', log_config.get('format', config.format))
        config.date_format = log_config.get('date_format', config.date_format)
        rotation_mb = log_config.get('file_rotation_size_mb', 10)
        config.file_rotation_size = rotation_mb * 1024 * 1024
        config.file_retention_count = log_config.get('file_retention_count', 5)
        config.enable_console = log_config.get('enable_console', True)
        config.enable_file = log_config.get('enable_file', True)
        
        # Adjust for environment
        if self.environment == Environment.DEVELOPMENT:
            config.level = LogLevel.DEBUG
        elif self.environment == Environment.PRODUCTION:
            config.level = LogLevel.INFO
            config.enable_console = False
        
        return config
    
    def _load_etl_config(self) -> ETLConfig:
        """Load ETL configuration from JSON and environment overrides"""
        etl_config = self._get_config_value('etl', default={})
        config = ETLConfig()
        
        config.batch_size = int(os.getenv('ETL_BATCH_SIZE', str(etl_config.get('batch_size', 1000))))
        config.max_workers = int(os.getenv('ETL_MAX_WORKERS', str(etl_config.get('max_workers', 4))))
        config.timeout_seconds = int(os.getenv('ETL_TIMEOUT', str(etl_config.get('timeout_seconds', 300))))
        config.retry_attempts = etl_config.get('retry_attempts', 3)
        config.skip_processed_files = etl_config.get('skip_processed_files', True) if os.getenv('ETL_SKIP_PROCESSED') != 'false' else False
        config.force_reprocess = etl_config.get('force_reprocess', False)
        config.latest_only = etl_config.get('latest_only', False)
        config.ignored_filename_prefixes = etl_config.get('ignored_filename_prefixes', ['SAMPLE', 'TEST', 'CHHSCA'])
        
        # File patterns from environment (comma-separated) or JSON
        if os.getenv('ETL_FILE_PATTERNS'):
            config.file_patterns = [p.strip() for p in os.getenv('ETL_FILE_PATTERNS').split(',')]
        else:
            config.file_patterns = etl_config.get('file_patterns', ["*.txt", "*.csv", "*.tsv"])
        
        # Recognized extensions from environment (comma-separated) or JSON
        if os.getenv('ETL_RECOGNIZED_EXTENSIONS'):
            config.recognized_extensions = [e.strip() for e in os.getenv('ETL_RECOGNIZED_EXTENSIONS').split(',')]
        else:
            config.recognized_extensions = etl_config.get('recognized_extensions', ['.txt', '.csv', '.tsv'])
        
        return config
    
    def _load_web_config(self) -> WebConfig:
        """Load web configuration from JSON and environment overrides"""
        web_config = self._get_config_value('web', default={})
        config = WebConfig()
        
        config.host = os.getenv('WEB_HOST', web_config.get('host', '0.0.0.0'))
        config.port = int(os.getenv('WEB_PORT', str(web_config.get('port', 8000))))
        config.reload = web_config.get('reload', False)
        config.log_level = os.getenv('WEB_LOG_LEVEL', web_config.get('log_level', 'info'))
        config.cors_origins = web_config.get('cors_origins', ['*'])
        config.static_dir = web_config.get('static_dir', 'static')
        config.templates_dir = web_config.get('templates_dir', 'templates')
        
        # HTTPS settings (from unified .config.json)
        config.use_https = web_config.get('use_https', False)
        config.cert_file = web_config.get('cert_file')
        config.key_file = web_config.get('key_file')
        
        # Adjust for environment
        if self.environment == Environment.DEVELOPMENT:
            config.reload = True
            config.log_level = "debug"
        
        return config
    
    def _load_security_config(self) -> SecurityConfig:
        """Load security configuration from JSON and environment overrides"""
        security_config = self._get_config_value('security', default={})
        phi_config = security_config.get('phi_hashing', {}) if isinstance(security_config, dict) else {}
        
        config = SecurityConfig()
        config.enable_phi_hashing = phi_config.get('enabled', True)
        config.hash_on_import = phi_config.get('hash_on_import', True)
        config.hash_on_export = phi_config.get('hash_on_export', True)
        
        # PHI hash salt from environment (never from JSON for security)
        config.phi_hash_salt = os.getenv('PHI_HASH_SALT', secrets.token_hex(32))
        
        # Fields to hash from JSON
        fields_to_hash = phi_config.get('fields_to_hash', {})
        if fields_to_hash and isinstance(fields_to_hash, dict):
            config.fields_to_hash = {k: v for k, v in fields_to_hash.items() if not k.startswith('_')}
        
        return config
    
    def _load_siem_config(self) -> SIEMConfig:
        """Load SIEM configuration from JSON and environment overrides"""
        siem_config = self._get_config_value('siem', default={})
        config = SIEMConfig()
        
        config.enabled = siem_config.get('enabled', False)
        config.enable_json_logging = siem_config.get('enable_json_logging', True)
        config.enable_windows_event_log = siem_config.get('enable_windows_event_log', False)
        json_log_path = siem_config.get('json_log_path', 'data/logs/siem')
        config.json_log_path = Path(json_log_path)
        
        syslog_config = siem_config.get('syslog', {}) if isinstance(siem_config, dict) else {}
        config.syslog_enabled = syslog_config.get('enabled', False)
        config.syslog_host = syslog_config.get('host', 'localhost')
        config.syslog_port = syslog_config.get('port', 514)
        config.syslog_protocol = syslog_config.get('protocol', 'UDP')
        
        config.include_sensitive_data = siem_config.get('include_sensitive_data', False)
        config.log_categories = siem_config.get('log_categories', [
            "authentication", "etl_operations", "data_access", "system_events", "security_events"
        ])
        
        return config
    
    def _load_sftp_config(self) -> SFTPConfig:
        """Load SFTP configuration from JSON and environment overrides"""
        sftp_config = self._get_config_value('sftp', default={})
        config = SFTPConfig()
        
        config.enabled = sftp_config.get('enabled', False)
        config.host = sftp_config.get('host', 'chhssftp.uniteus.com')
        config.port = sftp_config.get('port', 22)
        config.username = sftp_config.get('username', 'chhsca_data_prod')
        config.auth_method = sftp_config.get('auth_method', 'key')
        config.private_key_path = Path(sftp_config.get('private_key_path', 'keys/calco-uniteus-sftp'))
        config.private_key_passphrase = sftp_config.get('private_key_passphrase')
        config.key_format = sftp_config.get('key_format', 'auto')
        config.password = sftp_config.get('password')
        config.remote_directory = sftp_config.get('remote_directory', '/data/exports')
        config.file_patterns = sftp_config.get('file_patterns', ['*.txt', '*.csv'])
        config.auto_download = sftp_config.get('auto_download', False)
        config.download_interval_minutes = sftp_config.get('download_interval_minutes', 60)
        config.delete_after_download = sftp_config.get('delete_after_download', False)
        local_download_path = sftp_config.get('local_download_path', 'data/input')
        config.local_download_path = Path(local_download_path)
        config.timeout_seconds = sftp_config.get('timeout_seconds', 30)
        config.max_retries = sftp_config.get('max_retries', 3)
        config.verify_host_key = sftp_config.get('verify_host_key', True)
        known_hosts_path = sftp_config.get('known_hosts_path', 'data/sftp/known_hosts')
        config.known_hosts_path = Path(known_hosts_path)
        
        return config
    
    def _load_data_quality_config(self) -> DataQualityConfig:
        """Load data quality configuration from JSON"""
        dq_config = self._get_config_value('data_quality', default={})
        config = DataQualityConfig()
        
        if dq_config:
            config.expected_tables = dq_config.get('expected_tables', config.expected_tables)
            config.date_fields = dq_config.get('date_fields', config.date_fields)
            config.boolean_fields = dq_config.get('boolean_fields', config.boolean_fields)
            config.required_fields = dq_config.get('required_fields', config.required_fields)
            config.primary_keys = dq_config.get('primary_keys', config.primary_keys)
        
        return config
    
    def get_connection_string(self) -> str:
        """Get database connection string"""
        return f"sqlite:///{self.database.path}"
    
    def is_development(self) -> bool:
        """Check if running in development mode"""
        return self.environment == Environment.DEVELOPMENT
    
    def is_production(self) -> bool:
        """Check if running in production mode"""
        return self.environment == Environment.PRODUCTION
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary for serialization"""
        return {
            'environment': self.environment.value,
            'database': {
                'path': str(self.database.path),
                'connection_timeout': self.database.connection_timeout,
                'max_connections': self.database.max_connections
            },
            'directories': {
                'project_root': str(self.directories.project_root),
                'data_dir': str(self.directories.data_dir),
                'input_dir': str(self.directories.input_dir),
                'output_dir': str(self.directories.output_dir),
                'logs_dir': str(self.directories.logs_dir)
            },
            'etl': {
                'batch_size': self.etl.batch_size,
                'max_workers': self.etl.max_workers,
                'timeout_seconds': self.etl.timeout_seconds
            },
            'web': {
                'host': self.web.host,
                'port': self.web.port,
                'reload': self.web.reload
            }
        }


# Global configuration instance (singleton)
config = UnifiedConfig()

# Backward compatibility exports for existing code
PROJECT_ROOT = config.directories.project_root
DATA_DIR = config.directories.data_dir
INPUT_DIR = config.directories.input_dir
OUTPUT_DIR = config.directories.output_dir
LOGS_DIR = config.directories.logs_dir
DATABASE_PATH = config.database.path
LOG_LEVEL = config.logging.level.value
LOG_FORMAT = config.logging.format
EXPECTED_TABLES = config.data_quality.expected_tables


def _ensure_directories():
    """Ensure all required directories exist (backward compatibility)"""
    # This is now handled automatically in DirectoryConfig.__post_init__
    pass


def get_config() -> UnifiedConfig:
    """Get the global configuration instance"""
    return config


def setup_logging():
    """Setup logging configuration based on current config"""
    import logging as log_module
    import logging.handlers
    from datetime import datetime
    
    log_config = config.logging
    root_logger = log_module.getLogger()
    
    # Configure root logger (only works once, but safe to call multiple times)
    # Use force=True if available (Python 3.8+) to allow reconfiguration
    try:
        log_module.basicConfig(
            level=getattr(log_module, log_config.level.value),
            format=log_config.format,
            datefmt=log_config.date_format,
            force=True  # Python 3.8+: force reconfiguration if already configured
        )
    except TypeError:
        # Python < 3.8 doesn't support force parameter
        # Only configure if not already configured
        if not root_logger.handlers:
            log_module.basicConfig(
                level=getattr(log_module, log_config.level.value),
                format=log_config.format,
                datefmt=log_config.date_format
            )
        else:
            # Just update the level if already configured
            root_logger.setLevel(getattr(log_module, log_config.level.value))
    
    # Create file handler if enabled
    if log_config.enable_file:
        log_file = config.directories.logs_dir / f"etl_{datetime.now().strftime('%Y%m%d')}.log"
        
        # Check if a RotatingFileHandler for this log file already exists
        existing_file_handler = None
        for handler in root_logger.handlers:
            if isinstance(handler, log_module.handlers.RotatingFileHandler):
                if handler.baseFilename == str(log_file):
                    existing_file_handler = handler
                    break
        
        # Only add handler if it doesn't already exist
        if existing_file_handler is None:
            file_handler = log_module.handlers.RotatingFileHandler(
                log_file,
                maxBytes=log_config.file_rotation_size,
                backupCount=log_config.file_retention_count
            )
            file_handler.setFormatter(log_module.Formatter(log_config.format, log_config.date_format))
            root_logger.addHandler(file_handler)
    
    # Disable console logging in production if configured
    if not log_config.enable_console and config.is_production():
        root_logger.handlers = [h for h in root_logger.handlers 
                                       if not isinstance(h, logging.StreamHandler)]


# Auto-setup logging when module is imported
from datetime import datetime
setup_logging()