# Calaveras UniteUs ETL

A robust ETL (Extract, Transform, Load) platform for Calaveras County Health and Human Services Agency, designed to integrate, process, and report on UniteUs data securely and efficiently.

## Features
- **Automated ETL Pipeline:** Extracts data from multiple sources, transforms it to internal schema, and loads it into a secure database.
- **Admin Control Panel:** Web-based dashboard for monitoring system health, resource usage, and diagnostics.
- **Audit Logging:** Tracks all system actions for compliance and troubleshooting.
- **Security Health Checks:** Built-in tools to monitor and report on system security.
- **SFTP Integration:** Secure file transfer for data imports and exports.
- **Custom Reporting:** Flexible reporting engine for demographics, referrals, outcomes, and more.
- **Configurable Settings:** Easily adjust system parameters, batch sizes, worker threads, and more.

## Project Structure
```
launch.pyw                # Main GUI launcher; handles startup, dependency checks, and user controls
migrate_db.py             # Database migration utility for schema updates and data migrations
network_discovery.py      # Network setup and diagnostics for deployment and troubleshooting
pytest.ini                # Pytest configuration for running tests

core/                     # Main application logic
    app.py                # FastAPI server entry point
    audit_logger.py       # Audit logging for compliance and traceability
    auth.py               # Authentication and authorization logic
    config.py             # System configuration management
    database.py           # Database connection and operations
    database_adapter.py   # Database adapter for different DB engines
    database_schema.py    # Internal schema definitions
    database_schema_converter.py # Schema conversion utilities
    etl_service.py        # Core ETL pipeline logic
    internal_schema.py    # Internal data models
    report_export.py      # Report generation and export
    schema_validator.py   # Data validation against schemas
    security_health_check.py # Security checks and diagnostics
    settings_manager.py   # System settings manager
    sftp_service.py       # SFTP integration for secure file transfer
    siem_logger.py        # SIEM logging for security event management
    reports/              # Reporting engine and analytics
    utils/                # Utility functions and helpers
    web/                  # Web dashboard (templates, static files)

core/web/templates/       # Jinja2 HTML templates for the admin dashboard and user interface
core/web/static/          # Static assets (CSS, JS) for the web dashboard

archive/                  # Legacy scripts, reference data, and historical utilities
    debug_scripts/        # Old debug and test scripts
    dev_scripts/          # Developer utilities
    one_time_scripts/     # One-off migration and data fix scripts

data/                     # Data storage (excluded from repo)
    backups/              # Database and file backups
    database/             # Main database files
    input/                # Raw input files
    logs/                 # System and ETL logs
    output/               # Processed output files
    sftp/                 # SFTP transfer files
    ssl/                  # SSL certificates and keys

dev_setup/                # Developer scripts and test utilities
    test_phi_hashing.py   # Test scripts for PHI hashing
    test_schema_output.py # Test scripts for schema output
    test_view_conversion.py # Test scripts for view conversion
    migrate_siem_severity.py # SIEM severity migration utility
    new_migration_modal.html # Migration modal HTML template

docs/                     # Documentation and setup guides
    QUICK_SETUP.md        # Quick setup instructions
    NETWORK_DEPLOYMENT.md # Network deployment guide
    HTTPS_SETUP.md        # HTTPS setup instructions
    TROUBLESHOOTING.md    # Troubleshooting guide
    README.md             # Project documentation

tests/                    # Unit and integration tests
    api/                  # API endpoint tests
    integration/          # Integration tests for database and services
    unit/                 # Unit tests for core modules
    README.md             # Test suite documentation
```


## Getting Started
1. **Clone the repository:**
   - Download or clone from https://github.com/waqqascalaveras/calaveras-uniteus-etl.git
2. **Install Python:**
   - Ensure Python 3.7+ is installed on your system.
3. **Launch the Application:**
   - Double-click `launch.pyw` to start the GUI.
   - All required dependencies will be installed automatically by the launcher.
4. **Configuration:**
   - Place your config files in `core/config.py` and secrets in environment variables as needed.
   - Sensitive folders (keys/, data/, temp_data_files/) are excluded from version control.

## Security & Privacy
- Sensitive files and data folders are excluded via `.gitignore`.
- All access and actions are logged for audit purposes.
- SFTP and SSL support for secure data transfer.

## Documentation
- See the `docs/` folder for setup, troubleshooting, and migration guides.
- Key files: `QUICK_SETUP.md`, `NETWORK_DEPLOYMENT.md`, `HTTPS_SETUP.md`, `TROUBLESHOOTING.md`

## Contributing
Pull requests and issues are welcome! Please follow best practices and ensure tests pass before submitting changes.


## License
This software and all associated code, documentation, and materials are the property of Calaveras County Health and Human Services Agency. Permission is required for any use, modification, distribution, or derivative works. Contact the agency for licensing inquiries or authorization.

---
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency
