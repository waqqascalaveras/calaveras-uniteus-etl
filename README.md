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
launch.pyw                # Main GUI launcher
migrate_db.py             # Database migration utility
network_discovery.py      # Network setup and diagnostics
core/                     # Main application logic
    app.py                # FastAPI server entry point
    ...                   # ETL, auth, config, reporting modules
core/web/                 # Web dashboard (templates, static files)
data/                     # Data storage (excluded from repo)
dev_setup/                # Developer scripts and test utilities
docs/                     # Documentation and setup guides
tests/                    # Unit and integration tests
archive/                  # Legacy scripts and reference data
```

## Getting Started
1. **Clone the repository:**
   ```bash
   git clone https://github.com/waqqascalaveras/calaveras-uniteus-etl.git
   cd calaveras-uniteus-etl
   ```
2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
3. **Configure environment:**
   - Place your config files in `core/config.py` and secrets in environment variables.
   - Sensitive folders (keys/, data/, temp_data_files/) are excluded from version control.
4. **Run the server:**
   ```bash
   python -m uvicorn core.app:app --reload --port 8000
   ```
5. **Launch the GUI:**
   ```bash
   python launch.pyw
   ```

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
This project is licensed for use by Calaveras County Health and Human Services Agency. Contact the author for other usage.

---
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency
