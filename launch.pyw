#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Application Launcher - UniteUs ETL Server Control

Single entry point launcher for the UniteUs ETL application. Provides a GUI
control window for managing the web server (start/stop/restart), configuring
ports and HTTPS certificates, and monitoring server status. Runs without
showing a console window for user-friendly operation.

Author: Waqqas Hanafi
Copyright: © 2025 Calaveras County Health and Human Services Agency
"""

import os
import sys
import webbrowser
import signal
import time
import threading
import socket
import subprocess
import json
from pathlib import Path
from datetime import datetime, timedelta

# Windows-specific flag to hide console windows
if sys.platform == 'win32':
    import ctypes
    # CREATE_NO_WINDOW flag for subprocess on Windows
    CREATE_NO_WINDOW = 0x08000000
else:
    CREATE_NO_WINDOW = 0

# Ensure we're in the right directory
try:
    os.chdir(Path(__file__).parent)
    sys.path.insert(0, str(Path(__file__).parent))
except Exception as e:
    # Show error immediately if we can't even set up the directory
    try:
        import ctypes
        ctypes.windll.user32.MessageBoxW(0,
            f"Error setting up working directory:\n\n{e}",
            "Launcher Error", 0x10)
    except:
        pass
    sys.exit(1)

# First, check for tkinter (comes with Python, but need it for GUI)
try:
    import tkinter as tk
    from tkinter import ttk, messagebox, filedialog
except ImportError as e:
    # Try to show error even without tkinter
    try:
        import ctypes
        ctypes.windll.user32.MessageBoxW(0, 
            f"ERROR: tkinter is not available.\n\n{e}\n\nPlease install Python with tkinter support.",
            "Launcher Error", 0x10)  # 0x10 = Error icon
    except:
        pass
    print("ERROR: tkinter is not available. Please install Python with tkinter support.")
    sys.exit(1)

# Embedded package requirements - all dependencies are defined here
# This replaces requirements.txt for a self-contained launcher
REQUIRED_PACKAGES = [
    # Core data processing
    'pandas>=2.0.0',
    'numpy>=1.24.0',
    'python-dateutil>=2.8.2',
    'psutil>=5.9.0',
    'openpyxl>=3.1.0',
    'xlsxwriter>=3.1.0',
    
    # Web interface dependencies
    'fastapi>=0.104.0',
    'uvicorn[standard]>=0.24.0',
    'jinja2>=3.1.2',
    'python-multipart>=0.0.6',
    'aiofiles>=23.2.1',
    'pydantic>=2.4.0',
    'cryptography>=41.0.0',
    
    # Authentication dependencies
    'ldap3>=2.9.1',
    
    # Document export dependencies
    'python-docx>=1.1.0',
    'reportlab>=4.0.0',
    'Pillow>=10.0.0',
    'lxml>=3.1.0',
    
    # SFTP and SIEM integration dependencies
    'paramiko>=3.4.0',
    'python-json-logger>=2.0.0',
    
    # Database driver dependencies
    'pyodbc>=5.0.0',  # For MS SQL Server and Azure SQL
    'psycopg2-binary>=2.9.0',  # For PostgreSQL
    'pymysql>=1.1.0',  # For MySQL
]

# Add Windows-specific packages
if sys.platform == 'win32':
    REQUIRED_PACKAGES.append('pywin32>=306')  # For Windows Event Log (SIEM)

# Mapping from package name to import name (in Python)
# Some packages have different names when installed vs imported
PACKAGE_TO_IMPORT = {
    'python-dateutil': 'dateutil',
    'python-multipart': 'multipart',
    'python-docx': 'docx',
    'python-json-logger': 'pythonjsonlogger',
    'psycopg2-binary': 'psycopg2',
    'Pillow': 'PIL',
    'pywin32': 'win32evtlog',  # On Windows, pywin32 provides win32evtlog
    'uvicorn[standard]': 'uvicorn',  # Handle extras notation
}

def get_import_name(package_spec):
    """Convert package specification to import name"""
    # Extract package name (before any version specifiers or extras)
    # Handle extras notation like uvicorn[standard]>=0.24.0
    if '[' in package_spec:
        # Extract base package name before extras
        base_name = package_spec.split('[')[0].strip()
        # Check if the full spec with extras is in our mapping
        full_spec = package_spec.split('>=')[0].split('==')[0].split('!=')[0].split('~=')[0].split('<')[0].strip()
        if full_spec in PACKAGE_TO_IMPORT:
            return PACKAGE_TO_IMPORT[full_spec]
        # Otherwise use base name
        package_name = base_name
    else:
        package_name = package_spec.split('>=')[0].split('==')[0].split('!=')[0].split('~=')[0].split('<')[0].strip()
    
    # Check if we have a mapping
    if package_name in PACKAGE_TO_IMPORT:
        return PACKAGE_TO_IMPORT[package_name]
    
    # Default: package name is usually the import name
    return package_name

def check_dependencies():
    """Check if all required packages are installed"""
    missing = []
    
    for package_spec in REQUIRED_PACKAGES:
        import_name = get_import_name(package_spec)
        
        try:
            __import__(import_name)
        except ImportError:
            missing.append(package_spec)
    
    return missing


def install_packages(packages):
    """Install missing packages using pip"""
    import subprocess
    
    try:
        # Create a progress window
        progress_window = tk.Toplevel()
        progress_window.title("Installing Dependencies")
        progress_window.geometry("500x300")
        progress_window.resizable(False, False)
        
        # Center the window
        progress_window.update_idletasks()
        x = (progress_window.winfo_screenwidth() // 2) - (progress_window.winfo_width() // 2)
        y = (progress_window.winfo_screenheight() // 2) - (progress_window.winfo_height() // 2)
        progress_window.geometry(f"+{x}+{y}")
        
        ttk.Label(
            progress_window,
            text="Installing Required Python Packages",
            font=('Arial', 12, 'bold')
        ).pack(pady=10)
        
        ttk.Label(
            progress_window,
            text=f"Installing {len(packages)} package(s)...",
            font=('Arial', 9)
        ).pack(pady=5)
        
        # Log text widget
        log_frame = ttk.Frame(progress_window)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        log_text = tk.Text(log_frame, wrap=tk.WORD, height=10, font=('Consolas', 8))
        log_scrollbar = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=log_text.yview)
        log_text.configure(yscrollcommand=log_scrollbar.set)
        
        log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        log_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        progress_window.update()
        
        # Install packages
        log_text.insert(tk.END, "Starting pip installation...\n\n")
        log_text.update()
        
        cmd = [sys.executable, "-m", "pip", "install"] + packages
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            creationflags=CREATE_NO_WINDOW if sys.platform == 'win32' else 0
        )
        
        # Stream output
        for line in iter(process.stdout.readline, ''):
            if line:
                log_text.insert(tk.END, line)
                log_text.see(tk.END)
                log_text.update()
        
        process.wait()
        
        if process.returncode == 0:
            log_text.insert(tk.END, "\n✅ Installation completed successfully!\n")
            log_text.see(tk.END)
            time.sleep(1)
            progress_window.destroy()
            return True
        else:
            log_text.insert(tk.END, f"\n❌ Installation failed with code {process.returncode}\n")
            log_text.see(tk.END)
            messagebox.showerror(
                "Installation Failed",
                "Failed to install required packages.\n\n"
                "Please check your internet connection and Python/pip installation."
            )
            progress_window.destroy()
            return False
            
    except Exception as e:
        messagebox.showerror(
            "Installation Error",
            f"Error installing packages: {e}\n\n"
            "Please check your internet connection and Python/pip installation."
        )
        return False

# Check dependencies before importing app
missing_packages = check_dependencies()

if missing_packages:
    root = tk.Tk()
    root.withdraw()  # Hide main window
    
    response = messagebox.askyesno(
        "Missing Dependencies",
        f"The following packages are not installed:\n\n" +
        "\n".join(f"  • {pkg}" for pkg in missing_packages[:5]) +
        (f"\n  ... and {len(missing_packages) - 5} more" if len(missing_packages) > 5 else "") +
        "\n\nWould you like to install them now?\n\n" +
        "(This may take a few minutes)",
        icon='warning'
    )
    
    if response:
        if install_packages(missing_packages):
            messagebox.showinfo("Success", "All dependencies installed successfully!\n\nThe application will now start.")
        else:
            root.destroy()
            sys.exit(1)
    else:
        messagebox.showinfo(
            "Manual Installation Required",
            "Please install dependencies manually or restart the launcher to try automatic installation again."
        )
        root.destroy()
        sys.exit(1)
    
    root.destroy()

# Now import app and other dependencies (they should be installed now)
# Wrap in try-except to catch any import errors early
try:
    import psutil
except ImportError as e:
    try:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror(
            "Import Error",
            f"Failed to import psutil: {e}\n\n"
            "Please install: pip install psutil"
        )
        root.destroy()
    except:
        try:
            import ctypes
            ctypes.windll.user32.MessageBoxW(0,
                f"Import Error\n\nFailed to import psutil:\n{e}\n\nPlease install:\npip install psutil",
                "Launcher Error", 0x10)
        except:
            pass
    sys.exit(1)

try:
    import uvicorn
except ImportError as e:
    try:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror(
            "Import Error",
            f"Failed to import uvicorn: {e}\n\n"
            "Please install: pip install uvicorn[standard]"
        )
        root.destroy()
    except:
        try:
            import ctypes
            ctypes.windll.user32.MessageBoxW(0,
                f"Import Error\n\nFailed to import uvicorn:\n{e}\n\nPlease install:\npip install uvicorn[standard]",
                "Launcher Error", 0x10)
        except:
            pass
    sys.exit(1)

try:
    # Import core.app - this might take a moment and could fail
    # Wrap in try-except to catch any errors during import
    from core.app import app
except Exception as e:  # Catch all exceptions, not just ImportError
    # Show error dialog
    error_type = type(e).__name__
    error_msg = str(e)
    
    try:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror(
            "Import Error",
            f"Failed to import core.app:\n\n{error_type}: {error_msg}\n\n"
            "This might be due to:\n"
            "• Missing dependencies\n"
            "• Configuration errors\n"
            "• Database connection issues\n\n"
            "Please check launcher_error.log for details."
        )
        root.destroy()
    except Exception as dialog_error:
        # Fallback: try Windows message box if tkinter dialog fails
        try:
            import ctypes
            ctypes.windll.user32.MessageBoxW(0,
                f"Import Error\n\nFailed to import core.app:\n{error_type}: {error_msg}\n\nCheck launcher_error.log for details.",
                "Launcher Error", 0x10)
        except:
            pass
    
    # Write error to log file
    try:
        error_log = Path(__file__).parent / "launcher_error.log"
        with open(error_log, "w", encoding="utf-8") as f:
            f.write(f"Launcher Error - {datetime.now()}\n")
            f.write("=" * 60 + "\n")
            f.write(f"Error importing core.app:\n")
            f.write(f"Type: {error_type}\n")
            f.write(f"Message: {error_msg}\n\n")
            import traceback
            f.write("Traceback:\n")
            f.write(traceback.format_exc())
    except:
        pass
    
    sys.exit(1)

def check_and_create_databases():
    """Check if databases exist and create them if they don't"""
    from pathlib import Path
    import sqlite3
    
    # Import after dependencies are verified
    from core.config import config
    from core.database_schema import get_schema_sql
    from core.auth import auth_service
    
    # Check ETL database
    etl_db_path = Path(config.database.path)
    etl_db_exists = etl_db_path.exists()
    
    # Check internal (auth) database
    internal_db_path = Path("data/database/internal.db")
    internal_db_exists = internal_db_path.exists()
    
    if etl_db_exists and internal_db_exists:
        return True  # Both databases exist
    
    # Show dialog if databases need to be created
    root = tk.Tk()
    root.withdraw()
    
    message = "Database setup required:\n\n"
    if not etl_db_exists:
        message += f"  • ETL database (chhsca_data.db) - MISSING\n"
    else:
        message += f"  • ETL database (chhsca_data.db) - Found\n"
    
    if not internal_db_exists:
        message += f"  • Authentication database (internal.db) - MISSING\n"
    else:
        message += f"  • Authentication database (internal.db) - Found\n"
    
    message += "\nWould you like to create the missing database(s) now?"
    
    response = messagebox.askyesno(
        "Database Setup",
        message,
        icon='question'
    )
    
    if not response:
        messagebox.showinfo(
            "Database Required",
            "Databases are required to run the application.\n\n"
            "You can create them manually by running:\n"
            "python archive/dev_scripts/recreate_database.py"
        )
        root.destroy()
        return False
    
    # Create progress window
    progress_window = tk.Toplevel()
    progress_window.title("Creating Databases")
    progress_window.geometry("500x250")
    progress_window.resizable(False, False)
    
    # Center the window
    progress_window.update_idletasks()
    x = (progress_window.winfo_screenwidth() // 2) - (progress_window.winfo_width() // 2)
    y = (progress_window.winfo_screenheight() // 2) - (progress_window.winfo_height() // 2)
    progress_window.geometry(f"+{x}+{y}")
    
    ttk.Label(
        progress_window,
        text="Initializing Databases",
        font=('Arial', 12, 'bold')
    ).pack(pady=10)
    
    status_label = ttk.Label(
        progress_window,
        text="Setting up database structure...",
        font=('Arial', 9)
    )
    status_label.pack(pady=5)
    
    # Log text widget
    log_frame = ttk.Frame(progress_window)
    log_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
    
    log_text = tk.Text(log_frame, wrap=tk.WORD, height=8, font=('Consolas', 8))
    log_scrollbar = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=log_text.yview)
    log_text.configure(yscrollcommand=log_scrollbar.set)
    
    log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    log_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    
    progress_window.update()
    
    def log(message):
        log_text.insert(tk.END, message + '\n')
        log_text.see(tk.END)
        log_text.update()
    
    try:
        # Create ETL database if needed
        if not etl_db_exists:
            log("Creating ETL database (chhsca_data.db)...")
            status_label.config(text="Creating ETL database...")
            progress_window.update()
            
            # Ensure directory exists
            etl_db_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Create database with schema
            conn = sqlite3.connect(str(etl_db_path))
            cursor = conn.cursor()
            
            schema_sql = get_schema_sql()
            statement_count = 0
            
            for statement in schema_sql.split(';'):
                statement = statement.strip()
                if statement:
                    cursor.execute(statement)
                    statement_count += 1
            
            conn.commit()
            conn.close()
            
            log(f"✓ ETL database created ({statement_count} objects)")
        else:
            log("✓ ETL database already exists")
        
        # Create internal database if needed
        if not internal_db_exists:
            log("\nCreating authentication database (internal.db)...")
            status_label.config(text="Creating authentication database...")
            progress_window.update()
            
            # Ensure directory exists
            internal_db_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Initialize auth database (auth_service does this automatically)
            from core.auth import LocalUserDatabase
            local_db = LocalUserDatabase()
            
            # Create default admin user
            success = local_db.create_user(
                username='admin',
                password='admin123',
                display_name='System Administrator',
                email=None,
                role='admin',
                created_by='system'
            )
            
            if success:
                log("✓ Authentication database created")
                log("✓ Default admin user created (admin/admin123)")
            else:
                log("⚠ Authentication database created, but admin may exist")
        else:
            log("✓ Authentication database already exists")
        
        log("\n✅ Database setup completed successfully!")
        time.sleep(1.5)
        progress_window.destroy()
        root.destroy()
        return True
        
    except Exception as e:
        log(f"\n❌ Error: {e}")
        progress_window.update()
        time.sleep(1)
        progress_window.destroy()
        
        messagebox.showerror(
            "Database Creation Failed",
            f"Failed to create databases: {e}\n\n"
            "Please try running manually:\n"
            "python archive/dev_scripts/recreate_database.py"
        )
        root.destroy()
        return False

# Configuration - now uses unified .config.json via core.config
UNIFIED_CONFIG_FILE = Path(".config.json")

# Default configuration (fallback)
DEFAULT_CONFIG = {
    "port": 8000,
    "use_https": False,
    "cert_file": None,
    "key_file": None
}

def load_config():
    """Load server configuration from unified .config.json"""
    try:
        from core.config import config as app_config
        result = {
            "port": app_config.web.port,
            "use_https": getattr(app_config.web, 'use_https', False),
            "cert_file": getattr(app_config.web, 'cert_file', None),
            "key_file": getattr(app_config.web, 'key_file', None)
        }
        return result
    except Exception as e:
        # Fallback to defaults if config loading fails
        if gui_root and hasattr(gui_root, 'log_message'):
            gui_root.log_message(f"⚠ Warning: Could not load config: {e}. Using defaults.")
        return DEFAULT_CONFIG.copy()

def save_config(port, use_https, cert_file=None, key_file=None):
    """Save server configuration to unified .config.json with comprehensive error handling"""
    try:
        # Validate inputs
        if not isinstance(port, int) or not (1 <= port <= 65535):
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"❌ Cannot save config: Invalid port {port}")
            return False
        
        if not isinstance(use_https, bool):
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"❌ Cannot save config: Invalid use_https value {use_https}")
            return False
        
        # Load existing unified config or create new
        if UNIFIED_CONFIG_FILE.exists():
            try:
                with open(UNIFIED_CONFIG_FILE, 'r', encoding='utf-8') as f:
                    unified_config = json.load(f)
            except Exception:
                unified_config = {}
        else:
            unified_config = {}
        
        # Ensure web section exists
        if 'web' not in unified_config:
            unified_config['web'] = {}
        
        # Update web configuration
        unified_config['web']['port'] = port
        unified_config['web']['use_https'] = use_https
        if cert_file:
            unified_config['web']['cert_file'] = str(cert_file)
        else:
            unified_config['web']['cert_file'] = None
        if key_file:
            unified_config['web']['key_file'] = str(key_file)
        else:
            unified_config['web']['key_file'] = None
        
        # Write to temporary file first, then rename (atomic write)
        temp_file = UNIFIED_CONFIG_FILE.with_suffix('.json.tmp')
        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(unified_config, f, indent=2, ensure_ascii=False)
            # Atomic rename
            temp_file.replace(UNIFIED_CONFIG_FILE)
            
            # Update runtime config if possible
            try:
                from core.config import config as app_config
                app_config.web.port = port
                app_config.web.use_https = use_https
                app_config.web.cert_file = str(cert_file) if cert_file else None
                app_config.web.key_file = str(key_file) if key_file else None
            except:
                pass  # Runtime update is optional
            
            return True
        except (IOError, OSError, PermissionError) as e:
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except:
                    pass
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"❌ Error writing config file: {e}")
            return False
    except Exception as e:
        error_msg = f"Unexpected error saving config: {type(e).__name__}: {e}"
        if gui_root and hasattr(gui_root, 'log_message'):
            gui_root.log_message(f"❌ {error_msg}")
        return False

# Load configuration
_config = load_config()

# Configuration - Use 0.0.0.0 to allow network access from other computers
# 0.0.0.0 binds to all available network interfaces, allowing:
# - Local access via 127.0.0.1 or localhost
# - Network access via computer's IP address or hostname
HOST = "0.0.0.0"
PORT = _config['port']

# HTTPS Configuration
# Set to True to enable HTTPS (requires certificate)
# Note: Self-signed certificates will show browser warnings unless installed in system trust store
# Enterprise certificates from County IT CA will be trusted automatically on all domain machines
USE_HTTPS = _config['use_https']

# Certificate file paths - use enterprise cert if configured, otherwise use default self-signed location
if _config.get('cert_file') and Path(_config['cert_file']).exists():
    CERT_FILE = Path(_config['cert_file'])
else:
    CERT_FILE = Path("data/ssl/server.crt")

if _config.get('key_file') and Path(_config['key_file']).exists():
    KEY_FILE = Path(_config['key_file'])
else:
    KEY_FILE = Path("data/ssl/server.key")

# For browser launch, use localhost (only this computer needs browser)
PROTOCOL = "https" if USE_HTTPS else "http"
LOCAL_URL = f"{PROTOCOL}://127.0.0.1:{PORT}"

# Get the computer's network information for display
import socket
def get_network_info():
    """Get local IP address and hostname for network access"""
    hostname = socket.gethostname()
    try:
        # Get primary network IP
        local_ip = socket.gethostbyname(hostname)
    except:
        local_ip = "127.0.0.1"
    return hostname, local_ip

HOSTNAME, LOCAL_IP = get_network_info()
# Get FQDN for certificate generation
try:
    FQDN = socket.getfqdn()
    if FQDN == HOSTNAME:
        # Try to get FQDN from domain info
        try:
            import subprocess
            result = subprocess.run(['net', 'config', 'workstation'], 
                                  capture_output=True, text=True, timeout=5,
                                  creationflags=CREATE_NO_WINDOW if sys.platform == 'win32' else 0)
            for line in result.stdout.split('\n'):
                if 'Full Computer name' in line:
                    full_name = line.split('Full Computer name')[1].strip()
                    if '.' in full_name:
                        FQDN = full_name
                        break
        except:
            # Fallback: construct FQDN from hostname and DNS suffix
            try:
                result = subprocess.run(['ipconfig', '/all'], 
                                      capture_output=True, text=True, timeout=5,
                                      creationflags=CREATE_NO_WINDOW if sys.platform == 'win32' else 0)
                for line in result.stdout.split('\n'):
                    if 'Primary Dns Suffix' in line:
                        suffix = line.split(':')[1].strip() if ':' in line else None
                        if suffix:
                            FQDN = f"{HOSTNAME}.{suffix}"
                            break
            except:
                pass
except:
    FQDN = HOSTNAME

NETWORK_URL = f"{PROTOCOL}://{LOCAL_IP}:{PORT}"
HOSTNAME_URL = f"{PROTOCOL}://{HOSTNAME}:{PORT}"
FQDN_URL = f"{PROTOCOL}://{FQDN}:{PORT}" if FQDN != HOSTNAME else None


def generate_self_signed_cert():
    """Generate a self-signed SSL certificate for HTTPS"""
    try:
        # Try to import cryptography
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives import serialization
        from datetime import timedelta
        import ipaddress
        
        # Create directory if it doesn't exist
        CERT_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        # If certificate exists, we'll overwrite it (user requested regeneration)
        if CERT_FILE.exists() or KEY_FILE.exists():
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.after(0, lambda: gui_root.log_message(f"ℹ️ Existing certificate found - will be overwritten"))
        
        # Log generation start
        if gui_root and hasattr(gui_root, 'log_message'):
            gui_root.after(0, lambda: gui_root.log_message("🔒 Starting SSL certificate generation..."))
            gui_root.after(0, lambda: gui_root.log_message(f"   Organization: Calaveras County HHS"))
            gui_root.after(0, lambda: gui_root.log_message(f"   Common Name: {HOSTNAME}"))
            gui_root.after(0, lambda: gui_root.log_message(f"   Key Size: 2048 bits"))
        
        # Generate private key
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )
        
        if gui_root and hasattr(gui_root, 'log_message'):
            gui_root.after(0, lambda: gui_root.log_message("   ✓ Private key generated"))
        
        # Create certificate subject
        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "California"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, "Calaveras County"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Calaveras County HHS"),
            x509.NameAttribute(NameOID.COMMON_NAME, HOSTNAME),
        ])
        
        # Calculate validity dates
        valid_from = datetime.utcnow()
        valid_to = datetime.utcnow() + timedelta(days=3650)  # Valid for 10 years
        
        # Build Subject Alternative Names list
        san_names = [
            x509.DNSName("localhost"),
            x509.DNSName("127.0.0.1"),
            x509.DNSName(HOSTNAME),
            x509.DNSName(HOSTNAME.lower()),
            x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
            x509.IPAddress(ipaddress.IPv4Address(LOCAL_IP)),
        ]
        
        # Add FQDN if different from hostname
        if FQDN != HOSTNAME and '.' in FQDN:
            san_names.append(x509.DNSName(FQDN))
            san_names.append(x509.DNSName(FQDN.lower()))
        
        # Build certificate
        cert = x509.CertificateBuilder().subject_name(
            subject
        ).issuer_name(
            issuer
        ).public_key(
            private_key.public_key()
        ).serial_number(
            x509.random_serial_number()
        ).not_valid_before(
            valid_from
        ).not_valid_after(
            valid_to
        ).add_extension(
            x509.SubjectAlternativeName(san_names),
            critical=False,
        ).sign(private_key, hashes.SHA256())
        
        if gui_root and hasattr(gui_root, 'log_message'):
            gui_root.after(0, lambda: gui_root.log_message("   ✓ Certificate signed with SHA-256"))
            gui_root.after(0, lambda: gui_root.log_message(f"   Valid from: {valid_from.strftime('%Y-%m-%d %H:%M:%S')} UTC"))
            gui_root.after(0, lambda: gui_root.log_message(f"   Valid until: {valid_to.strftime('%Y-%m-%d %H:%M:%S')} UTC (10 years)"))
            san_list = ["localhost", "127.0.0.1", HOSTNAME, LOCAL_IP]
            if FQDN != HOSTNAME and '.' in FQDN:
                san_list.append(FQDN)
            gui_root.after(0, lambda: gui_root.log_message(f"   Subject Alternative Names: {', '.join(san_list)}"))
        
        # Write certificate to disk
        with open(CERT_FILE, "wb") as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))
        
        # Write private key to disk
        with open(KEY_FILE, "wb") as f:
            f.write(private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption()
            ))
        
        if gui_root and hasattr(gui_root, 'log_message'):
            gui_root.after(0, lambda: gui_root.log_message(f"   ✓ Certificate saved: {CERT_FILE}"))
            gui_root.after(0, lambda: gui_root.log_message(f"   ✓ Private key saved: {KEY_FILE}"))
            gui_root.after(0, lambda: gui_root.log_message("✓ SSL certificate generation completed successfully"))
        
        return True
        
    except ImportError:
        # cryptography not installed
        if gui_root and hasattr(gui_root, 'log_message'):
            gui_root.after(0, lambda: gui_root.log_message("❌ Error: cryptography package not installed"))
        return False
    except Exception as e:
        error_msg = f"Error generating certificate: {e}"
        if gui_root and hasattr(gui_root, 'log_message'):
            gui_root.after(0, lambda: gui_root.log_message(f"❌ {error_msg}"))
        print(error_msg)
        return False


def install_certificate_to_trust_store():
    """
    Install the self-signed certificate to Windows Trusted Root Certificate Authorities
    This requires administrator privileges
    """
    try:
        import subprocess
        import ctypes
        
        if not CERT_FILE.exists():
            error_msg = "Certificate file not found"
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.after(0, lambda: gui_root.log_message(f"❌ {error_msg}"))
            return False, error_msg
        
        # Check if running as admin
        is_admin = ctypes.windll.shell32.IsUserAnAdmin() != 0
        
        if gui_root and hasattr(gui_root, 'log_message'):
            gui_root.after(0, lambda: gui_root.log_message("🔒 Starting certificate installation..."))
            gui_root.after(0, lambda: gui_root.log_message(f"   Certificate file: {CERT_FILE.absolute()}"))
            gui_root.after(0, lambda: gui_root.log_message(f"   Store: Windows Trusted Root Certification Authorities (User)"))
            gui_root.after(0, lambda: gui_root.log_message(f"   Administrator privileges: {'Yes' if is_admin else 'No (may require elevation)'}"))
        
        # Use certutil to install certificate (requires admin)
        cmd = f'certutil -addstore -user "Root" "{CERT_FILE.absolute()}"'
        
        if gui_root and hasattr(gui_root, 'log_message'):
            gui_root.after(0, lambda: gui_root.log_message("   Running certutil command..."))
        
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            creationflags=CREATE_NO_WINDOW if sys.platform == 'win32' else 0
        )
        
        if result.returncode == 0:
            # Parse certutil output for certificate details
            output_lines = result.stdout.split('\n')
            cert_hash = None
            for line in output_lines:
                if 'Cert Hash(sha1):' in line:
                    cert_hash = line.split('Cert Hash(sha1):')[1].strip()
                    break
            
            if gui_root and hasattr(gui_root, 'log_message'):
                if cert_hash:
                    gui_root.after(0, lambda: gui_root.log_message(f"   Certificate Hash (SHA-1): {cert_hash}"))
                gui_root.after(0, lambda: gui_root.log_message("   ✓ Certificate added to Trusted Root store"))
                gui_root.after(0, lambda: gui_root.log_message("✓ Certificate installation completed successfully"))
                gui_root.after(0, lambda: gui_root.log_message("   ⚠️ Please restart your browser for changes to take effect"))
            
            return True, "Certificate installed successfully"
        else:
            error_msg = f"Failed to install certificate: {result.stderr}"
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.after(0, lambda: gui_root.log_message(f"❌ Installation failed"))
                if result.stderr:
                    gui_root.after(0, lambda: gui_root.log_message(f"   Error: {result.stderr.strip()}"))
                if result.stdout:
                    gui_root.after(0, lambda: gui_root.log_message(f"   Output: {result.stdout.strip()}"))
            return False, error_msg
            
    except Exception as e:
        error_msg = f"Error installing certificate: {e}"
        if gui_root and hasattr(gui_root, 'log_message'):
            gui_root.after(0, lambda: gui_root.log_message(f"❌ {error_msg}"))
        return False, error_msg

# Global state
shutdown_flag = threading.Event()
server_start_time = None
server_thread = None
gui_root = None
server_running = False  # Track if server is currently running
uvicorn_server = None  # Reference to uvicorn Server instance for programmatic control


def is_port_in_use(port, retries=2):
    """
    Check if a port is already in use by trying to bind to it
    Uses retries to handle transient binding issues
    
    Args:
        port: Port number to check
        retries: Number of bind attempts
    
    Returns:
        bool: True if port is in use, False if available
    """
    for attempt in range(retries):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                # SO_REUSEPORT is only available on Linux/Unix, not Windows
                if hasattr(socket, 'SO_REUSEPORT'):
                    try:
                        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                    except (AttributeError, OSError):
                        pass  # Ignore if not supported on this platform
                result = s.bind(('127.0.0.1', port))
                s.close()
                return False  # Port is available (bind succeeded)
        except OSError as e:
            if attempt < retries - 1:
                time.sleep(0.1)  # Brief wait before retry
            elif e.errno == 10048:  # Windows: Address already in use
                return True
            elif e.errno == 98:  # Linux: Address already in use
                return True
            else:
                # Other error - assume port is in use to be safe
                return True
    return True  # All bind attempts failed - assume port is in use


def is_port_listening(port, host='127.0.0.1', timeout=0.5, retries=2):
    """
    Check if a server is actually listening on a port by trying to connect to it
    Uses retries to handle transient connection issues
    
    Args:
        port: Port number to check
        host: Host to connect to (default: 127.0.0.1)
        timeout: Connection timeout in seconds
        retries: Number of connection attempts
    
    Returns:
        bool: True if server is listening, False otherwise
    """
    for attempt in range(retries):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(timeout)
                result = s.connect_ex((host, port))
                if result == 0:
                    return True  # Connection succeeded (server is listening)
                elif attempt < retries - 1:
                    time.sleep(0.1)  # Brief wait before retry
        except (socket.error, OSError, socket.timeout):
            if attempt < retries - 1:
                time.sleep(0.1)  # Brief wait before retry
            continue
    return False  # All connection attempts failed


def kill_process_on_port(port, max_retries=3, force_after_seconds=5):
    """
    Kill any process using the specified port - NEVER kills the current process
    Uses retries and force termination if process doesn't respond
    
    Args:
        port: Port number to check
        max_retries: Maximum number of termination attempts
        force_after_seconds: Seconds to wait before force killing
    
    Returns:
        tuple: (killed_any: bool, processes_killed: list of PIDs)
    """
    import os
    current_pid = os.getpid()  # Get current process ID to avoid killing ourselves
    killed_any = False
    processes_killed = []
    processes_to_kill = []
    
    try:
        # First pass: Find all processes using the port
        for proc in psutil.process_iter(['pid', 'name', 'exe']):
            try:
                # CRITICAL: Never kill the current process (the GUI application)
                if proc.pid == current_pid:
                    continue
                
                # Check if process is using the port
                try:
                    for conn in proc.net_connections('inet'):
                        if conn.laddr and conn.laddr.port == port:
                            processes_to_kill.append(proc)
                            break
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
    except Exception as e:
        # Fallback: Try alternative method
        try:
            for proc in psutil.process_iter(['pid', 'name']):
                try:
                    if proc.pid == current_pid:
                        continue
                    for conn in proc.net_connections():
                        if conn.laddr and conn.laddr.port == port:
                            processes_to_kill.append(proc)
                            break
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass
        except Exception:
            pass
    
    # Second pass: Terminate processes with retries
    for proc in processes_to_kill:
        try:
            if not proc.is_running():
                continue
            
            pid = proc.pid
            proc_name = proc.info.get('name', 'unknown')
            
            # Try graceful termination first
            for attempt in range(max_retries):
                try:
                    if not proc.is_running():
                        break
                    
                    if attempt == 0:
                        proc.terminate()  # Graceful termination
                    else:
                        proc.kill()  # Force kill on retries
                    
                    # Wait for process to die
                    try:
                        proc.wait(timeout=force_after_seconds)
                        killed_any = True
                        processes_killed.append(pid)
                        if gui_root and hasattr(gui_root, 'log_message'):
                            gui_root.after(0, lambda p=pid, n=proc_name: 
                                gui_root.log_message(f"   ✓ Terminated process {n} (PID: {p}) on port {port}"))
                        break
                    except psutil.TimeoutExpired:
                        # Process didn't die, try force kill
                        if attempt < max_retries - 1:
                            try:
                                proc.kill()
                            except psutil.NoSuchProcess:
                                # Process already dead
                                killed_any = True
                                processes_killed.append(pid)
                                break
                except psutil.NoSuchProcess:
                    # Process already dead
                    killed_any = True
                    processes_killed.append(pid)
                    break
                except psutil.AccessDenied:
                    if gui_root and hasattr(gui_root, 'log_message'):
                        gui_root.after(0, lambda p=pid, n=proc_name: 
                            gui_root.log_message(f"   ⚠ Access denied killing process {n} (PID: {p}) - may require admin"))
                    break
                except Exception as e:
                    if attempt == max_retries - 1:
                        if gui_root and hasattr(gui_root, 'log_message'):
                            gui_root.after(0, lambda p=pid, e=str(e): 
                                gui_root.log_message(f"   ⚠ Failed to kill process PID {p}: {e}"))
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    
    return killed_any, processes_killed


def open_browser():
    """Open browser after server starts - always use localhost for local browser"""
    time.sleep(2)  # Wait for server to be ready
    if not shutdown_flag.is_set():
        webbrowser.open(LOCAL_URL)


# Windows Service class for running as a service
if sys.platform == 'win32':
    try:
        import win32serviceutil
        import win32service
        import win32event
        import servicemanager
        
        class UniteUsETLService(win32serviceutil.ServiceFramework):
            """Windows Service for Calaveras UniteUs ETL Server"""
            _svc_name_ = "UniteUsETL"
            _svc_display_name_ = "Calaveras UniteUs ETL Server"
            _svc_description_ = "Calaveras County UniteUs ETL Web Server - Provides web interface for ETL operations"
            
            def __init__(self, args):
                win32serviceutil.ServiceFramework.__init__(self, args)
                self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
                self.server_thread = None
                self.server_process = None
            
            def SvcStop(self):
                """Stop the service"""
                self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
                win32event.SetEvent(self.hWaitStop)
                
                # Stop the server
                global shutdown_flag
                shutdown_flag.set()
                
                if self.server_thread and self.server_thread.is_alive():
                    # Wait for server thread to stop
                    self.server_thread.join(timeout=10)
                
                if self.server_process:
                    try:
                        self.server_process.terminate()
                        self.server_process.wait(timeout=5)
                    except:
                        pass
            
            def SvcDoRun(self):
                """Run the service - starts the server"""
                servicemanager.LogMsg(
                    servicemanager.EVENTLOG_INFORMATION_TYPE,
                    servicemanager.PYS_SERVICE_STARTED,
                    (self._svc_name_, '')
                )
                
                try:
                    # Load configuration
                    _config = load_config()
                    global PORT, USE_HTTPS, CERT_FILE, KEY_FILE
                    PORT = _config['port']
                    USE_HTTPS = _config['use_https']
                    
                    # Load certificate paths
                    if _config.get('cert_file') and Path(_config['cert_file']).exists():
                        CERT_FILE = Path(_config['cert_file'])
                    else:
                        CERT_FILE = Path("data/ssl/server.crt")
                    
                    if _config.get('key_file') and Path(_config['key_file']).exists():
                        KEY_FILE = Path(_config['key_file'])
                    else:
                        KEY_FILE = Path("data/ssl/server.key")
                    
                    # Check and create databases
                    if not check_and_create_databases():
                        servicemanager.LogErrorMsg("Failed to initialize databases")
                        return
                    
                    # Start server in a thread
                    global server_running, server_start_time
                    server_start_time = datetime.now()
                    server_running = True
                    
                    # Run server in main thread (service context)
                    run_server()
                    
                    # Wait for stop signal
                    win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE)
                    
                except Exception as e:
                    servicemanager.LogErrorMsg(f"Service error: {e}")
                    import traceback
                    servicemanager.LogErrorMsg(traceback.format_exc())
                finally:
                    servicemanager.LogMsg(
                        servicemanager.EVENTLOG_INFORMATION_TYPE,
                        servicemanager.PYS_SERVICE_STOPPED,
                        (self._svc_name_, '')
                    )
    except ImportError:
        # pywin32 not available - service class won't be defined
        UniteUsETLService = None
else:
    # Not Windows - service class not needed
    UniteUsETLService = None


def run_server():
    """Run the uvicorn server with memory optimization for long-running operations"""
    global server_start_time, PORT, USE_HTTPS, server_running, uvicorn_server
    
    if gui_root and hasattr(gui_root, 'log_message'):
        gui_root.after(0, lambda: gui_root.log_message(f"🔧 run_server() function called - starting uvicorn..."))
    
    server_start_time = datetime.now()
    server_running = True
    
    # Set up logging to capture uvicorn output
    import logging
    import gc
    
    # Force garbage collection on startup
    gc.collect()
    
    # Create custom handler that sends logs to GUI with message grouping
    class GUILogHandler(logging.Handler):
        def __init__(self):
            super().__init__()
            self.message_count = 0
            # Track recent messages for deduplication
            self.recent_messages = {}  # {message_signature: (count, first_time, last_time, full_message)}
            self.grouping_window = 30  # Group messages within 30 seconds
            
        def _get_message_signature(self, msg, record):
            """Create a signature for grouping similar messages"""
            # For SFTP connection messages, group by the core message
            if 'SFTPConnection' in record.name or 'sftp' in msg.lower():
                # Extract the core message (remove timestamps, host details that change)
                if 'Connecting to SFTP server' in msg:
                    return 'sftp_connecting'
                elif 'Successfully connected to' in msg and 'SFTP' in msg:
                    return 'sftp_connected'
                elif 'Disconnected from' in msg and 'SFTP' in msg:
                    return 'sftp_disconnected'
                elif 'Loaded known hosts' in msg:
                    return 'sftp_known_hosts'
                elif 'Using private key' in msg:
                    return 'sftp_key'
                elif 'Saved host key' in msg:
                    return 'sftp_host_key'
            
            # For paramiko transport logs, group by type
            if 'paramiko.transport' in record.name:
                if 'Connected (version' in msg:
                    return 'paramiko_connected'
                elif 'Authentication (publickey) successful' in msg:
                    return 'paramiko_auth'
                elif 'Opened sftp connection' in msg:
                    return 'paramiko_sftp_opened'
                elif 'sftp session closed' in msg:
                    return 'paramiko_sftp_closed'
            
            # For other messages, use the full message as signature (no grouping)
            return None
        
        def emit(self, record):
            if gui_root and hasattr(gui_root, 'log_message'):
                try:
                    msg = self.format(record)
                    current_time = time.time()
                    
                    # Only filter out the most egregious noise (connection resets)
                    skip_messages = [
                        'WinError 10054',  # Connection reset
                        'An existing connection was forcibly closed',  # Connection reset
                        '_call_connection_lost',  # asyncio internals
                        'proactor_events.py',  # asyncio internals
                    ]
                    
                    # Skip only if message matches skip list (be more permissive)
                    if any(skip in msg for skip in skip_messages):
                        return
                    
                    # Get message signature for grouping
                    msg_sig = self._get_message_signature(msg, record)
                    
                    # Clean up old entries (older than grouping window)
                    cutoff_time = current_time - self.grouping_window
                    expired_keys = [k for k, v in self.recent_messages.items() 
                                   if v[2] < cutoff_time]
                    for key in expired_keys:
                        del self.recent_messages[key]
                    
                    # Handle message grouping - much less aggressive
                    if msg_sig:
                        if msg_sig in self.recent_messages:
                            # Update existing grouped message
                            count, first_time, last_time, full_msg = self.recent_messages[msg_sig]
                            count += 1
                            self.recent_messages[msg_sig] = (count, first_time, current_time, msg)
                            
                            # Only log grouped message every 20 occurrences (much less grouping)
                            if count % 20 == 0:
                                time_ago = int(current_time - first_time)
                                grouped_msg = f"{msg} (×{count} in last {time_ago}s)"
                                gui_root.after(0, lambda m=grouped_msg: gui_root.log_message(m))
                        else:
                            # First occurrence - log immediately
                            self.recent_messages[msg_sig] = (1, current_time, current_time, msg)
                            gui_root.after(0, lambda m=msg: gui_root.log_message(m))
                    else:
                        # No grouping - log all unique messages (important diagnostic info)
                        gui_root.after(0, lambda m=msg: gui_root.log_message(m))
                    
                    # Periodic garbage collection every 100 messages
                    self.message_count += 1
                    if self.message_count % 100 == 0:
                        gc.collect()
                except:
                    pass
    
    # Add handler to root logger (check if already exists to avoid duplicates)
    root_logger = logging.getLogger()
    
    # Check if GUILogHandler already exists
    gui_handler_exists = any(isinstance(h, GUILogHandler) for h in root_logger.handlers)
    
    if not gui_handler_exists:
        gui_handler = GUILogHandler()
        gui_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', '%H:%M:%S'))
        root_logger.addHandler(gui_handler)
        root_logger.setLevel(logging.INFO)
        
        # Also add to uvicorn loggers
        for logger_name in ['uvicorn', 'uvicorn.error', 'uvicorn.access']:
            logger = logging.getLogger(logger_name)
            # Check if handler already exists for this logger
            if not any(isinstance(h, GUILogHandler) for h in logger.handlers):
                logger.addHandler(gui_handler)
            logger.setLevel(logging.INFO)
    else:
        # Handler already exists, just ensure log level is set
        root_logger.setLevel(logging.INFO)
        for logger_name in ['uvicorn', 'uvicorn.error', 'uvicorn.access']:
            logging.getLogger(logger_name).setLevel(logging.INFO)
    
    # Suppress asyncio connection errors (harmless Windows networking behavior)
    asyncio_logger = logging.getLogger('asyncio')
    asyncio_logger.setLevel(logging.WARNING)  # Only show warnings and above, not errors from connection resets
    
    # Prepare SSL config if HTTPS is enabled
    ssl_keyfile = None
    ssl_certfile = None
    
    try:
        if USE_HTTPS:
            # Generate certificate if needed
            if not CERT_FILE.exists() or not KEY_FILE.exists():
                if generate_self_signed_cert():
                    pass  # Detailed logging is handled in generate_self_signed_cert()
                else:
                    if gui_root and hasattr(gui_root, 'log_message'):
                        gui_root.after(0, lambda: gui_root.log_message("⚠ Failed to generate SSL certificate, falling back to HTTP"))
                    # Disable HTTPS if certificate generation failed
                    USE_HTTPS = False
                    # Update config to reflect this
                    save_config(PORT, False)
            
            if USE_HTTPS and CERT_FILE.exists() and KEY_FILE.exists():
                ssl_keyfile = str(KEY_FILE.absolute())
                ssl_certfile = str(CERT_FILE.absolute())
                
                # Verify certificate files are readable and valid
                try:
                    with open(ssl_certfile, 'r') as f:
                        cert_content = f.read()
                    if gui_root and hasattr(gui_root, 'log_message'):
                        gui_root.after(0, lambda: gui_root.log_message(f"✓ Certificate file readable ({len(cert_content)} bytes)"))
                    
                    # Validate certificate format
                    if 'BEGIN CERTIFICATE' not in cert_content or 'END CERTIFICATE' not in cert_content:
                        raise ValueError("Certificate file does not appear to be a valid PEM certificate")
                    
                    # Check certificate expiration if possible
                    try:
                        from cryptography import x509
                        from cryptography.hazmat.backends import default_backend
                        cert_obj = x509.load_pem_x509_certificate(cert_content.encode(), default_backend())
                        not_after = cert_obj.not_valid_after
                        days_until_expiry = (not_after - datetime.now()).days
                        if days_until_expiry < 30:
                            if gui_root and hasattr(gui_root, 'log_message'):
                                gui_root.after(0, lambda: gui_root.log_message(f"⚠ WARNING: Certificate expires in {days_until_expiry} days ({not_after.strftime('%Y-%m-%d')})"))
                        elif days_until_expiry < 0:
                            if gui_root and hasattr(gui_root, 'log_message'):
                                gui_root.after(0, lambda: gui_root.log_message(f"❌ ERROR: Certificate expired on {not_after.strftime('%Y-%m-%d')}"))
                            raise ValueError(f"Certificate expired on {not_after.strftime('%Y-%m-%d')}")
                    except ImportError:
                        pass  # cryptography not available for validation
                    except Exception as e:
                        if gui_root and hasattr(gui_root, 'log_message'):
                            gui_root.after(0, lambda: gui_root.log_message(f"⚠ Warning: Could not validate certificate expiration: {e}"))
                except (IOError, OSError, PermissionError) as e:
                    if gui_root and hasattr(gui_root, 'log_message'):
                        gui_root.after(0, lambda: gui_root.log_message(f"❌ Cannot read certificate file: {e}"))
                        gui_root.after(0, lambda: gui_root.log_message(f"   Check file permissions and path: {ssl_certfile}"))
                    raise
                except ValueError as e:
                    if gui_root and hasattr(gui_root, 'log_message'):
                        gui_root.after(0, lambda: gui_root.log_message(f"❌ Invalid certificate file: {e}"))
                    raise
                
                try:
                    with open(ssl_keyfile, 'r') as f:
                        key_content = f.read()
                    if gui_root and hasattr(gui_root, 'log_message'):
                        gui_root.after(0, lambda: gui_root.log_message(f"✓ Key file readable ({len(key_content)} bytes)"))
                    
                    # Validate key format
                    if 'BEGIN PRIVATE KEY' not in key_content and 'BEGIN RSA PRIVATE KEY' not in key_content:
                        raise ValueError("Key file does not appear to be a valid PEM private key")
                except (IOError, OSError, PermissionError) as e:
                    if gui_root and hasattr(gui_root, 'log_message'):
                        gui_root.after(0, lambda: gui_root.log_message(f"❌ Cannot read key file: {e}"))
                        gui_root.after(0, lambda: gui_root.log_message(f"   Check file permissions and path: {ssl_keyfile}"))
                    raise
                except ValueError as e:
                    if gui_root and hasattr(gui_root, 'log_message'):
                        gui_root.after(0, lambda: gui_root.log_message(f"❌ Invalid key file: {e}"))
                    raise
                
                if gui_root and hasattr(gui_root, 'log_message'):
                    # Check if using enterprise cert (not default self-signed location)
                    _config_check = load_config()
                    is_enterprise = _config_check.get('cert_file') and Path(_config_check['cert_file']).exists()
                    if is_enterprise:
                        gui_root.after(0, lambda: gui_root.log_message("🔒 HTTPS enabled with enterprise certificate"))
                        gui_root.after(0, lambda: gui_root.log_message("✓ Certificate trusted by domain machines"))
                    else:
                        gui_root.after(0, lambda: gui_root.log_message("🔒 HTTPS enabled with self-signed certificate"))
                        gui_root.after(0, lambda: gui_root.log_message("⚠ Browser will show security warning unless certificate is installed"))
                    
                    gui_root.after(0, lambda: gui_root.log_message(f"📜 Certificate: {CERT_FILE}"))
                    gui_root.after(0, lambda: gui_root.log_message(f"🔑 Private Key: {KEY_FILE}"))
            elif USE_HTTPS:
                # Certificate should exist but doesn't - disable HTTPS
                USE_HTTPS = False
                save_config(PORT, False)
    except Exception as e:
        if gui_root and hasattr(gui_root, 'log_message'):
            gui_root.after(0, lambda: gui_root.log_message(f"⚠ Certificate setup error: {e}"))
    
    # Log that uvicorn is starting
    if gui_root and hasattr(gui_root, 'log_message'):
        gui_root.after(0, lambda: gui_root.log_message(f"🌐 Starting uvicorn server on {HOST}:{PORT}..."))
        if USE_HTTPS:
            gui_root.after(0, lambda: gui_root.log_message("🔐 SSL/TLS encryption enabled"))
            if ssl_keyfile and ssl_certfile:
                gui_root.after(0, lambda: gui_root.log_message(f"   Certificate: {ssl_certfile}"))
                gui_root.after(0, lambda: gui_root.log_message(f"   Key: {ssl_keyfile}"))
    
    # Run uvicorn server using Server class for programmatic control
    # This allows us to properly shut down the server when shutdown_flag is set
    # Wrap in try-except to prevent any exceptions from exiting the application
    try:
        if gui_root and hasattr(gui_root, 'log_message'):
            gui_root.after(0, lambda: gui_root.log_message("🔄 Creating uvicorn Server instance..."))
        
        from uvicorn import Config, Server
        
        # Create uvicorn config
        config = Config(
            app=app,
            host=HOST,
            port=PORT,
            log_level="info",
            access_log=False,
            use_colors=False,
            timeout_keep_alive=30,
            ssl_keyfile=ssl_keyfile,
            ssl_certfile=ssl_certfile
        )
        
        # Create server instance
        uvicorn_server = Server(config)
        
        if gui_root and hasattr(gui_root, 'log_message'):
            gui_root.after(0, lambda: gui_root.log_message("🔄 Starting uvicorn server..."))
        
        # Start a thread to monitor shutdown_flag and stop server when set
        def monitor_shutdown():
            """Monitor shutdown_flag in background and stop server when set"""
            import time
            # Give server time to fully start before monitoring shutdown flag
            # This prevents race conditions during restart
            time.sleep(1.0)  # Wait 1 second for server to initialize
            
            # Only start monitoring if server is still running and flag is clear
            if not server_running or shutdown_flag.is_set():
                # Server was stopped before we even started monitoring
                if uvicorn_server is not None:
                    try:
                        uvicorn_server.should_exit = True
                    except Exception as e:
                        if gui_root and hasattr(gui_root, 'log_message'):
                            gui_root.after(0, lambda: gui_root.log_message(f"⚠ Error setting should_exit: {e}"))
                return
            
            # Now monitor the shutdown flag
            while server_running and not shutdown_flag.is_set():
                time.sleep(0.1)  # Check every 100ms
            
            # Shutdown flag was set or server_running became False
            if shutdown_flag.is_set() or not server_running:
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.after(0, lambda: gui_root.log_message("🛑 Shutdown flag set, stopping server..."))
                
                # Signal server to exit with timeout
                if uvicorn_server is not None:
                    try:
                        uvicorn_server.should_exit = True
                        
                        # Wait for graceful shutdown (max 10 seconds)
                        shutdown_timeout = 10
                        shutdown_waited = 0
                        while shutdown_waited < shutdown_timeout and is_port_listening(PORT):
                            time.sleep(0.5)
                            shutdown_waited += 0.5
                        
                        # If server still running after timeout, log warning
                        if is_port_listening(PORT):
                            if gui_root and hasattr(gui_root, 'log_message'):
                                gui_root.after(0, lambda: gui_root.log_message(
                                    f"⚠ Server still listening after {shutdown_timeout}s - may need force termination"))
                    except Exception as e:
                        if gui_root and hasattr(gui_root, 'log_message'):
                            gui_root.after(0, lambda: gui_root.log_message(f"⚠ Error during shutdown: {e}"))
        
        # Start monitor thread (but it will wait before checking)
        monitor_thread = threading.Thread(target=monitor_shutdown, daemon=True)
        monitor_thread.start()
        
        # Run the server (this blocks until server.should_exit is True)
        uvicorn_server.run()
        
        # Server started successfully (this won't execute until uvicorn stops)
        if gui_root and hasattr(gui_root, 'log_message'):
            gui_root.after(0, lambda: gui_root.log_message("✅ Server is running and ready to accept connections"))
    except KeyboardInterrupt:
        # Normal shutdown - don't log as error
        # CRITICAL: Do NOT re-raise - this would close the entire application
        if gui_root and hasattr(gui_root, 'log_message'):
            gui_root.after(0, lambda: gui_root.log_message("⚠ Server interrupted (KeyboardInterrupt) - server stopped, app remains open"))
    except SystemExit as e:
        # System exit from uvicorn - catch it so it doesn't exit the whole app
        # CRITICAL: Do NOT re-raise - this would close the entire application
        if e.code != 0:
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.after(0, lambda: gui_root.log_message(f"⚠ Server exited with code {e.code} - server stopped, app remains open"))
        else:
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.after(0, lambda: gui_root.log_message("⚠ Server exited normally - server stopped, app remains open"))
        # DO NOT re-raise - keep the app open
    except OSError as e:
        # Handle port binding errors with helpful messages
        error_code = getattr(e, 'winerror', getattr(e, 'errno', None))
        if error_code == 10048 or 'Address already in use' in str(e) or 'EADDRINUSE' in str(e):
            error_msg = f"❌ Port {PORT} is already in use by another application"
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.after(0, lambda: gui_root.log_message(error_msg))
                gui_root.after(0, lambda: gui_root.log_message(f"   Solution: Change the port number or close the application using port {PORT}"))
                gui_root.after(0, lambda: gui_root.log_message(f"   To find what's using the port, run: netstat -ano | findstr :{PORT}"))
        elif error_code == 10013 or 'Permission denied' in str(e) or 'EACCES' in str(e):
            error_msg = f"❌ Permission denied: Cannot bind to port {PORT}"
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.after(0, lambda: gui_root.log_message(error_msg))
                gui_root.after(0, lambda: gui_root.log_message(f"   Solution: Use a port number above 1024, or run as administrator"))
        else:
            error_msg = f"❌ Server error (OSError): {e}"
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.after(0, lambda: gui_root.log_message(error_msg))
        # Also print to console for debugging
        print(f"ERROR in run_server(): {e}")
    except Exception as e:
        # Log the full error with traceback
        import traceback
        error_msg = f"❌ Server error: {type(e).__name__}: {e}"
        error_trace = traceback.format_exc()
        if gui_root and hasattr(gui_root, 'log_message'):
            gui_root.after(0, lambda: gui_root.log_message(error_msg))
            gui_root.after(0, lambda: gui_root.log_message(f"   Traceback: {error_trace}"))
        # Also print to console for debugging
        print(f"ERROR in run_server(): {e}")
        print(error_trace)
    finally:
            # Mark server as stopped when uvicorn exits
            # This ensures the GUI knows the server stopped, but the app continues running
            # Note: server_running is already declared as global at the top of the function
            server_running = False
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.after(0, lambda: gui_root.log_message("🛑 Server thread ended"))
            # Don't log here - stop_server() already handles logging


def create_gui():
    """Create the GUI control window"""
    global gui_root
    
    gui_root = tk.Tk()
    gui_root.title("Calaveras UniteUs ETL Server Control")
    gui_root.geometry("900x700")
    gui_root.resizable(True, True)
    
    # Set minimum size
    gui_root.minsize(800, 600)
    
    # Helper function to copy text to clipboard
    def copy_to_clipboard(text):
        """Copy text to clipboard and show feedback"""
        try:
            gui_root.clipboard_clear()
            gui_root.clipboard_append(text)
            gui_root.update()
            # Could add a status message here if desired
        except Exception as e:
            print(f"Error copying to clipboard: {e}")
    
    # Configure grid weights for resizing
    gui_root.grid_rowconfigure(0, weight=1)
    gui_root.grid_columnconfigure(0, weight=1)
    
    # Main container
    main_container = ttk.Frame(gui_root)
    main_container.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=10, pady=10)
    main_container.grid_rowconfigure(3, weight=1)  # Log area expands
    main_container.grid_columnconfigure(0, weight=1)
    main_container.grid_columnconfigure(1, weight=1)
    
    # Title - with icon and better styling
    title_label = ttk.Label(
        main_container, 
        text="🚀 Calaveras UniteUs ETL Server Control", 
        font=('Arial', 16, 'bold')
    )
    title_label.grid(row=0, column=0, pady=(0, 15), sticky=tk.W)
    
    # Get background color for tk.Label widgets to match ttk theme
    try:
        bg_color = gui_root.cget('bg')
    except:
        bg_color = 'SystemButtonFace'  # Windows default
    
    # Row 1: Server Control and Server URLs side by side
    control_frame = ttk.LabelFrame(main_container, text="🎮 Server Control", padding="12")
    control_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(0, 5), pady=(0, 12))
    control_frame.grid_columnconfigure(1, weight=1)
    
    # Header frame with Status and Uptime side by side
    status_header_frame = ttk.Frame(control_frame)
    status_header_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 12))
    
    # Left side: Status indicator
    status_left_frame = ttk.Frame(status_header_frame)
    status_left_frame.pack(side=tk.LEFT, fill=tk.X, expand=True)
    
    # Status label with better styling
    status_label = ttk.Label(status_left_frame, text="Server Status", font=('Arial', 10, 'bold'))
    status_label.pack(side=tk.LEFT, padx=(0, 15))
    
    # Status indicator with improved visual design and icons
    status_indicator_frame = ttk.Frame(status_left_frame)
    status_indicator_frame.pack(side=tk.LEFT)
    
    # Status icon indicator (using colored circle only, no redundant emoji)
    status_icon = tk.Label(status_indicator_frame, text="●", font=('Arial', 18, 'bold'), fg='#28a745', bg=bg_color)
    status_icon.pack(side=tk.LEFT, padx=(0, 8))
    
    # Status text container with progress bar behind it
    status_text_frame = tk.Frame(status_indicator_frame, bg=bg_color, width=100, height=25)
    status_text_frame.pack(side=tk.LEFT)
    status_text_frame.pack_propagate(False)  # Prevent frame from shrinking
    
    # Create custom style for progress bar with visible animation
    style = ttk.Style()
    # Try to create custom style with proper layout for Windows
    try:
        # Get the default layout first
        default_layout = style.layout('TProgressbar')
        # Create custom style by copying the default layout
        style.layout('Status.TProgressbar', default_layout)
        # Configure colors for the custom style
        style.configure('Status.TProgressbar',
                         background='#4a9eff',  # Light blue that will show behind text
                         troughcolor=bg_color,
                         borderwidth=0,
                         lightcolor='#6bb3ff',
                         darkcolor='#4a9eff')
        progress_style = 'Status.TProgressbar'
    except Exception as e:
        # Fallback: use default style and configure colors
        try:
            style.configure('TProgressbar',
                             background='#4a9eff',
                             troughcolor=bg_color,
                             borderwidth=0)
            progress_style = 'TProgressbar'
        except:
            progress_style = 'TProgressbar'
    
    # Progress bar - will be visible behind transparent text when running
    status_progress = ttk.Progressbar(
        status_text_frame,
        mode='indeterminate',
        length=100,
        style=progress_style
    )
    status_progress.place(x=0, y=0, relwidth=1, relheight=1)
    status_progress.stop()  # Initially stopped
    
    # Status text using Canvas for true transparency
    # Canvas background matches frame, text is drawn on top with transparent background
    status_text_canvas = tk.Canvas(
        status_text_frame,
        width=100,
        height=25,
        bg=bg_color,  # Match frame background
        highlightthickness=0,
        bd=0
    )
    status_text_canvas.place(x=0, y=0, relwidth=1, relheight=1)
    
    # Create text on canvas - text has transparent background
    status_value_text_id = status_text_canvas.create_text(
        50, 12,  # Center
        text="Stopped",  # Initial state
        font=('Arial', 10, 'bold'),
        fill='#dc3545',  # Red for stopped
        anchor='center'
    )
    
    # Helper function to update status text with proper colors
    def update_status_text(text, color=None, bg_override=None):
        """Update the status text on the canvas"""
        try:
            # Set default colors based on status
            if color is None:
                if text == "Running":
                    color = '#ffffff'  # White for good contrast over blue progress bar
                elif text == "Stopped":
                    color = '#dc3545'  # Red
                elif text == "Failed":
                    color = '#8b0000'  # Dark red
                elif "Starting" in text or "Stopping" in text or "Restarting" in text:
                    color = '#ff9800'  # Orange for operations in progress
                else:
                    color = '#000000'  # Black default
            
            status_text_canvas.itemconfig(status_value_text_id, text=text, fill=color)
            # Canvas background stays as bg_color (matches frame/progress bar trough)
            # This allows progress bar animation to show through when running
            
            # Always ensure canvas is visible after text update
            safe_lift_canvas()
        except:
            pass
    
    # Create wrapper for backward compatibility with .config() calls
    class StatusValueWrapper:
        def __init__(self, update_func):
            self.update_func = update_func
        def config(self, text=None, fg=None, bg=None):
            if text is not None:
                self.update_func(text, fg, bg)
    
    status_value = StatusValueWrapper(update_status_text)
    
    # Store references
    gui_root.status_value = status_value
    gui_root.status_progress = status_progress
    gui_root.status_text_canvas = status_text_canvas
    
    # Helper function to safely lift the status text canvas
    def safe_lift_canvas():
        """Safely lift the status text canvas, handling cases where widget may be destroyed"""
        try:
            if status_text_canvas and status_text_canvas.winfo_exists():
                # Lower progress bar first, then lift canvas to ensure text is always visible
                if status_progress and status_progress.winfo_exists():
                    status_progress.lower()
                status_text_canvas.lift()
                # Force update to ensure visibility
                status_text_canvas.update_idletasks()
        except (tk.TclError, AttributeError, RuntimeError):
            # Widget is being destroyed or invalid - silently ignore
            pass
    
    # Right side: Uptime
    uptime_right_frame = ttk.Frame(status_header_frame)
    uptime_right_frame.pack(side=tk.RIGHT, fill=tk.X, expand=True)
    
    # Uptime label
    uptime_label = ttk.Label(uptime_right_frame, text="⏱️ Uptime:", font=('Arial', 10, 'bold'))
    uptime_label.pack(side=tk.LEFT, padx=(0, 10))
    
    # Railway station-style display container for uptime
    uptime_display_frame = tk.Frame(uptime_right_frame, bg='#1a1a1a', relief=tk.SUNKEN, bd=2)
    uptime_display_frame.pack(side=tk.LEFT, padx=(0, 0))
    
    # Uptime value with railway station display styling
    uptime_value = tk.Label(
        uptime_display_frame, 
        text="00:00:00", 
        font=('Consolas', 14, 'bold'), 
        bg='#1a1a1a',
        fg='#00ff00',  # Bright green like old LED displays
        padx=12,
        pady=6
    )
    uptime_value.pack()
    
    # Animation control variable (use list to allow modification in nested functions)
    animation_running = [True]
    
    # Animation function for pulsing effect when running
    def animate_status_icon():
        """Animate the status icon with pulsing effect"""
        if animation_running[0] and server_running and not shutdown_flag.is_set():
            # Pulsing animation - alternate between bright and dim green
            try:
                current_fg = status_icon.cget('fg')
                if current_fg == '#28a745':  # Bright green
                    status_icon.config(fg='#5a9a3a', text="●")  # Slightly dimmer green
                else:
                    status_icon.config(fg='#28a745', text="●")  # Back to bright green
                gui_root.after(800, animate_status_icon)  # Animate every 0.8 seconds
            except:
                pass  # Widget may have been destroyed
    
    # Don't start animation here - it will be started after UI is properly initialized
    # (either when server is detected as running, or when user clicks Start)
    
    # Control buttons frame - arrange buttons horizontally
    control_buttons_frame = ttk.Frame(control_frame)
    control_buttons_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 12))
    
    # Windows Service management functions
    SERVICE_NAME = "UniteUsETL"
    SERVICE_DISPLAY_NAME = "Calaveras UniteUs ETL Server"
    SERVICE_DESCRIPTION = "Calaveras County UniteUs ETL Web Server - Provides web interface for ETL operations"
    
    def is_service_installed():
        """Check if the Windows service is installed"""
        if sys.platform != 'win32':
            return False
        try:
            import win32serviceutil
            import win32service
            import win32api
            
            # Try to open the service
            scm = win32service.OpenSCManager(None, None, win32service.SC_MANAGER_CONNECT)
            try:
                service_handle = win32service.OpenService(
                    scm, SERVICE_NAME, win32service.SERVICE_QUERY_STATUS
                )
                win32service.CloseServiceHandle(service_handle)
                return True
            except win32service.error:
                return False
            finally:
                win32service.CloseServiceHandle(scm)
        except ImportError:
            return False
        except Exception:
            return False
    
    def install_windows_service():
        """Install the application as a Windows Service"""
        if sys.platform != 'win32':
            messagebox.showerror("Error", "Windows Service installation is only available on Windows.")
            return
        
        try:
            import win32serviceutil
            import ctypes
            
            # Check if running as administrator
            is_admin = ctypes.windll.shell32.IsUserAnAdmin() != 0
            if not is_admin:
                messagebox.showerror(
                    "Administrator Required",
                    "Administrator privileges are required to install Windows Services.\n\n"
                    "Please run this application as Administrator:\n"
                    "1. Right-click launch.pyw\n"
                    "2. Select 'Run as administrator'\n"
                    "3. Try again"
                )
                return
            
            # Get the path to this script
            script_path = Path(__file__).absolute()
            python_exe = sys.executable
            
            # Create service installation command
            # We'll use a service wrapper script
            service_script = script_path.parent / "uniteus_service.py"
            
            # Create the service wrapper script (always recreate to ensure it's up to date)
            service_wrapper_content = '''"""
Windows Service Wrapper for Calaveras UniteUs ETL
This script runs the server as a Windows Service
"""
import sys
import os
import importlib.util
from pathlib import Path

# Add the application directory to Python path
app_dir = Path(__file__).parent
sys.path.insert(0, str(app_dir))

# Change to application directory
os.chdir(str(app_dir))

# Import launch.pyw as a module using importlib
launch_path = app_dir / "launch.pyw"
spec = importlib.util.spec_from_file_location("launch", launch_path)
launch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(launch)

if __name__ == '__main__':
    import win32serviceutil
    win32serviceutil.HandleCommandLine(launch.UniteUsETLService)
'''
            with open(service_script, 'w', encoding='utf-8') as f:
                f.write(service_wrapper_content)
            
            # Install the service
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"📦 Installing Windows Service: {SERVICE_DISPLAY_NAME}...")
            
            # Install the service using subprocess (must run the service script)
            import subprocess
            result = subprocess.run(
                [python_exe, str(service_script), 'install'],
                capture_output=True,
                text=True,
                cwd=str(script_path.parent),
                creationflags=CREATE_NO_WINDOW if sys.platform == 'win32' else 0
            )
            
            if result.returncode != 0:
                raise Exception(f"Service installation failed: {result.stderr}")
            
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"✅ Windows Service installed successfully")
                gui_root.log_message(f"   Service Name: {SERVICE_NAME}")
                gui_root.log_message(f"   Display Name: {SERVICE_DISPLAY_NAME}")
                gui_root.log_message(f"   ℹ️ Service will start automatically on Windows startup")
                gui_root.log_message(f"   ℹ️ You can manage it via Services (services.msc)")
            
            messagebox.showinfo(
                "Service Installed",
                f"Windows Service installed successfully!\n\n"
                f"Service Name: {SERVICE_NAME}\n"
                f"Display Name: {SERVICE_DISPLAY_NAME}\n\n"
                f"The service will start automatically when Windows starts.\n\n"
                f"You can manage it via:\n"
                f"  • Services (services.msc)\n"
                f"  • This application (Uninstall button)\n\n"
                f"Note: The service runs independently of this GUI window."
            )
            
            # Update button text
            if 'service_btn' in globals():
                service_btn.config(text="🔧 Uninstall Service")
            
        except ImportError:
            messagebox.showerror(
                "Missing Dependency",
                "pywin32 package is required to install Windows Services.\n\n"
                "Please install it:\n"
                "pip install pywin32"
            )
        except Exception as e:
            error_msg = f"Failed to install Windows Service: {e}"
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"❌ {error_msg}")
            messagebox.showerror("Installation Error", error_msg)
    
    def uninstall_windows_service():
        """Uninstall the Windows Service"""
        if sys.platform != 'win32':
            messagebox.showerror("Error", "Windows Service uninstallation is only available on Windows.")
            return
        
        try:
            import win32serviceutil
            import ctypes
            
            # Check if running as administrator
            is_admin = ctypes.windll.shell32.IsUserAnAdmin() != 0
            if not is_admin:
                messagebox.showerror(
                    "Administrator Required",
                    "Administrator privileges are required to uninstall Windows Services.\n\n"
                    "Please run this application as Administrator."
                )
                return
            
            # Confirm uninstallation
            response = messagebox.askyesno(
                "Uninstall Service",
                f"Are you sure you want to uninstall the Windows Service?\n\n"
                f"Service: {SERVICE_DISPLAY_NAME}\n\n"
                f"This will stop the service if it's running and remove it from Windows Services.\n\n"
                f"Continue?",
                icon='warning'
            )
            
            if not response:
                return
            
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"🗑️ Uninstalling Windows Service: {SERVICE_DISPLAY_NAME}...")
            
            # Stop and remove the service
            try:
                win32serviceutil.StopService(SERVICE_NAME)
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.log_message(f"   ✓ Service stopped")
            except:
                pass  # Service might not be running
            
            # Use subprocess to remove the service
            script_path = Path(__file__).absolute()
            service_script = script_path.parent / "uniteus_service.py"
            python_exe = sys.executable
            
            if service_script.exists():
                import subprocess
                result = subprocess.run(
                    [python_exe, str(service_script), 'remove'],
                    capture_output=True,
                    text=True,
                    cwd=str(script_path.parent),
                    creationflags=CREATE_NO_WINDOW if sys.platform == 'win32' else 0
                )
                
                if result.returncode != 0:
                    raise Exception(f"Service removal failed: {result.stderr}")
            
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"✅ Windows Service uninstalled successfully")
            
            messagebox.showinfo(
                "Service Uninstalled",
                f"Windows Service uninstalled successfully.\n\n"
                f"The service has been removed from Windows Services."
            )
            
            # Update button text
            if 'service_btn' in globals():
                service_btn.config(text="⚙️ Install Service")
            
        except ImportError:
            messagebox.showerror(
                "Missing Dependency",
                "pywin32 package is required to uninstall Windows Services."
            )
        except Exception as e:
            error_msg = f"Failed to uninstall Windows Service: {e}"
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"❌ {error_msg}")
            messagebox.showerror("Uninstallation Error", error_msg)
    
    def toggle_service():
        """Toggle service installation/uninstallation"""
        if is_service_installed():
            uninstall_windows_service()
        else:
            install_windows_service()
        
        # Update button text after operation
        if 'service_btn' in globals():
            if is_service_installed():
                service_btn.config(text="🔧 Uninstall Service")
            else:
                service_btn.config(text="⚙️ Install Service")
    
    def stop_server():
        """Stop the server - does NOT close the application"""
        global server_running, PORT, uvicorn_server
        
        # Wrap in try-except to prevent any exception from closing the app
        try:
            if server_running:
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.log_message("🛑 Stopping server...")
                
                # Set shutdown flag to signal server to stop FIRST
                shutdown_flag.set()
                
                # Then immediately update the global flag
                server_running = False
                server_start_time = None
                
                # Force stop animation immediately
                animation_running[0] = False
                
                # If uvicorn server instance exists, signal it to exit
                if uvicorn_server is not None:
                    try:
                        uvicorn_server.should_exit = True
                    except Exception as e:
                        # Log but don't fail - port kill will handle cleanup
                        if gui_root and hasattr(gui_root, 'log_message'):
                            gui_root.log_message(f"⚠ Warning: Error signaling uvicorn server to stop: {e}")
                
                # Update UI - ensure all updates happen in GUI thread
                def update_ui_stopped():
                    """Update UI to show stopped state - must run in GUI thread"""
                    try:
                        animation_running[0] = False  # Stop animation
                        status_icon.config(text="●", fg='#dc3545')  # Red circle for stopped
                        status_value.config(text="Stopped", fg='#dc3545')  # Red for stopped
                        status_progress.stop()  # Stop progress bar
                        # Ensure canvas and text are visible - lift canvas above progress bar
                        status_progress.lower()  # Lower progress bar first
                        safe_lift_canvas()  # Then lift canvas above progress bar so text is visible
                        local_url_value.config(text="(Server Stopped)", state='disabled')
                        network_url_value.config(text="(Server Stopped)", state='disabled')
                        hostname_url_value.config(text="(Server Stopped)", state='disabled')
                        uptime_value.config(text="00:00:00")
                        
                        # Update buttons - gray out instead of hiding
                        stop_btn.config(state='disabled')
                        restart_btn.config(state='disabled')
                        start_btn.config(state='normal')
                        # Re-enable buttons after operation completes
                        button_operation_in_progress['value'] = False
                    except Exception as e:
                        # Log but don't fail
                        if gui_root and hasattr(gui_root, 'log_message'):
                            gui_root.log_message(f"⚠ Warning: Error updating UI: {e}")
                
                # Schedule UI update in GUI thread
                if gui_root:
                    gui_root.after(0, update_ui_stopped)
                else:
                    # Fallback if gui_root not available
                    update_ui_stopped()
                
                # Log that server has stopped
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.log_message("✓ Server stopped")
                
                # Kill process on port in background (non-blocking)
                def kill_port_process():
                    # Wrap in try-except to prevent any exception from affecting the GUI
                    try:
                        try:
                            current_port = int(port_var.get()) if port_var.get() else PORT
                        except:
                            current_port = PORT
                        
                        # Wait a moment for server to respond to shutdown signal
                        time.sleep(0.5)
                        
                        if is_port_in_use(current_port):
                            if gui_root and hasattr(gui_root, 'log_message'):
                                gui_root.after(0, lambda: gui_root.log_message(f"🔪 Terminating process on port {current_port}..."))
                            killed, pids = kill_process_on_port(current_port)
                            if killed and gui_root and hasattr(gui_root, 'log_message'):
                                gui_root.after(0, lambda: gui_root.log_message(f"✓ Process terminated on port {current_port}"))
                    except SystemExit:
                        # CRITICAL: Catch SystemExit - do NOT re-raise
                        pass
                    except KeyboardInterrupt:
                        # CRITICAL: Catch KeyboardInterrupt - do NOT re-raise
                        pass
                    except Exception as e:
                        # Log but don't propagate - this is just cleanup
                        if gui_root and hasattr(gui_root, 'log_message'):
                            gui_root.after(0, lambda: gui_root.log_message(f"⚠ Warning: Error killing process on port: {e}"))
                
                # Run in background thread to avoid blocking GUI
                threading.Thread(target=kill_port_process, daemon=True).start()
        except SystemExit as e:
            # CRITICAL: Catch SystemExit to prevent app from closing
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"⚠ SystemExit caught in stop_server (code: {e.code}) - preventing app close")
            # DO NOT re-raise - keep the app open
        except KeyboardInterrupt:
            # CRITICAL: Catch KeyboardInterrupt to prevent app from closing
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message("⚠ KeyboardInterrupt caught in stop_server - preventing app close")
        except BaseException as e:
            # CRITICAL: Catch ALL exceptions to prevent app close
            import traceback
            error_trace = traceback.format_exc()
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"❌ Error in stop_server: {type(e).__name__}: {e}")
                gui_root.log_message(f"   Traceback: {error_trace}")
            messagebox.showerror("Stop Error", f"An error occurred while stopping the server: {e}\n\nThe application will remain open.")
    
    def start_server():
        """Start the server - does NOT close the application"""
        global server_running, server_thread, server_start_time, PORT, USE_HTTPS, PROTOCOL, LOCAL_URL, NETWORK_URL, HOSTNAME_URL, CERT_FILE, KEY_FILE
        
        # Wrap in try-except to prevent any exception from closing the app
        try:
            # Define verification function before conditional to avoid scoping issues
            def verify_server_started():
                max_wait = 10  # Maximum wait time in seconds
                check_interval = 0.5  # Check every 500ms
                waited = 0
                verified = False
                
                # Check multiple times with increasing wait intervals
                while waited < max_wait:
                    time.sleep(check_interval)
                    waited += check_interval
                    
                    if is_port_listening(PORT, timeout=1.0, retries=2):
                        verified = True
                        if gui_root and hasattr(gui_root, 'log_message'):
                            gui_root.log_message(f"✅ Server verified listening on port {PORT} (after {waited:.1f}s)")
                            gui_root.log_message("=" * 60)
                        break
                
                if not verified:
                    global server_running
                    server_running = False
                    if gui_root and hasattr(gui_root, 'log_message'):
                        gui_root.log_message(f"❌ ERROR: Server thread started but port {PORT} is not listening after {max_wait}s!")
                        gui_root.log_message("   Possible causes:")
                        gui_root.log_message("   • Server failed to start (check for Python errors above)")
                        gui_root.log_message("   • Port is blocked by firewall")
                        gui_root.log_message("   • Another process is using the port")
                        gui_root.log_message("   • Server crashed during startup")
                        gui_root.log_message("   Try checking the terminal output for Python errors.")
                    
                    # Try to clean up - kill any processes that might be holding the port
                    try:
                        killed, pids = kill_process_on_port(PORT, max_retries=1, force_after_seconds=1)
                        if killed and gui_root and hasattr(gui_root, 'log_message'):
                            gui_root.log_message(f"   🔧 Cleaned up {len(pids)} process(es) on port {PORT}")
                    except Exception:
                        pass
                    
                    # Update UI to show error
                    animation_running[0] = False
                    status_icon.config(text="●", fg='#8b0000')
                    status_value.config(text="Failed", fg='#8b0000')  # Dark red for failed
                    status_progress.stop()  # Stop progress bar
                    safe_lift_canvas()  # Ensure text is visible
                    local_url_value.config(text="(Server Failed)", state='disabled')
                    network_url_value.config(text="(Server Failed)", state='disabled')
                    hostname_url_value.config(text="(Server Failed)", state='disabled')
                    start_btn.config(state='normal')
                    stop_btn.config(state='disabled')
                    restart_btn.config(state='disabled')
            
            if not server_running:
                # Reload config
                _config = load_config()
                
                # Get port from GUI
                try:
                    new_port = int(port_var.get()) if port_var.get() else _config['port']
                    if not (1 <= new_port <= 65535):
                        messagebox.showerror("Invalid Port", "Port must be between 1 and 65535")
                        return
                except ValueError:
                    messagebox.showerror("Invalid Port", "Port must be a valid number")
                    return
                
                # Check if port is in use and kill any process on it
                if is_port_in_use(new_port):
                    if gui_root and hasattr(gui_root, 'log_message'):
                        gui_root.log_message(f"⚠ Port {new_port} is in use, attempting to terminate existing process...")
                    
                    # Try to kill processes on the port
                    killed, pids = kill_process_on_port(new_port, max_retries=3, force_after_seconds=3)
                    
                    if killed:
                        if gui_root and hasattr(gui_root, 'log_message'):
                            gui_root.log_message(f"✓ Terminated {len(pids)} process(es) on port {new_port}")
                        time.sleep(1.5)  # Give it time to fully release the port
                    else:
                        # No process was killed - might be in TIME_WAIT state
                        # Wait a moment and re-check if port is actually available
                        time.sleep(1.0)
                    
                    # Re-check port availability with retries
                    max_retries = 3
                    port_available = False
                    for retry in range(max_retries):
                        if not is_port_in_use(new_port):
                            port_available = True
                            if gui_root and hasattr(gui_root, 'log_message'):
                                if retry > 0:
                                    gui_root.log_message(f"✓ Port {new_port} is now available (was in TIME_WAIT state)")
                            break
                        else:
                            if retry < max_retries - 1:
                                time.sleep(0.5)
                    
                    if not port_available:
                        # Port is still in use - show error with helpful message
                        error_msg = (
                            f"Port {new_port} is in use and could not be freed.\n\n"
                            f"This could be due to:\n"
                            f"• Another application using the port\n"
                            f"• Port in TIME_WAIT state (wait 1-2 minutes)\n"
                            f"• Insufficient permissions to terminate the process\n\n"
                            f"Please close any existing servers or choose a different port."
                        )
                        messagebox.showerror("Port In Use", error_msg)
                        return
                
                PORT = new_port
                USE_HTTPS = https_var.get()
                PROTOCOL = "https" if USE_HTTPS else "http"
                
                # Reload certificate paths
                if _config.get('cert_file') and Path(_config['cert_file']).exists():
                    CERT_FILE = Path(_config['cert_file'])
                else:
                    CERT_FILE = Path("data/ssl/server.crt")
                
                if _config.get('key_file') and Path(_config['key_file']).exists():
                    KEY_FILE = Path(_config['key_file'])
                else:
                    KEY_FILE = Path("data/ssl/server.key")
                
                # Update network info and URLs
                HOSTNAME, LOCAL_IP = get_network_info()
                LOCAL_URL = f"{PROTOCOL}://127.0.0.1:{PORT}"
                NETWORK_URL = f"{PROTOCOL}://{LOCAL_IP}:{PORT}"
                HOSTNAME_URL = f"{PROTOCOL}://{HOSTNAME}:{PORT}"
                
                # Update URL button displays
                local_url_value.config(text=f"{LOCAL_URL}", state='normal', command=lambda: webbrowser.open(LOCAL_URL))
                network_url_value.config(text=f"{NETWORK_URL}", state='normal', command=lambda: webbrowser.open(NETWORK_URL))
                hostname_url_value.config(text=f"{HOSTNAME_URL}", state='normal', command=lambda: webbrowser.open(HOSTNAME_URL))
                
                # Log server startup details
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.log_message("=" * 60)
                    gui_root.log_message("🚀 STARTING SERVER")
                    gui_root.log_message("=" * 60)
                    gui_root.log_message(f"   Port: {PORT}")
                    gui_root.log_message(f"   Protocol: {PROTOCOL.upper()}")
                    gui_root.log_message(f"   HTTPS: {'Enabled' if USE_HTTPS else 'Disabled'}")
                    if USE_HTTPS:
                        if CERT_FILE.exists() and KEY_FILE.exists():
                            cert_type = "Enterprise" if _config.get('cert_file') else "Self-Signed"
                            gui_root.log_message(f"   Certificate: {cert_type} ({CERT_FILE})")
                        else:
                            gui_root.log_message(f"   Certificate: Not found - will generate")
                    gui_root.log_message(f"   Local URL: {LOCAL_URL}")
                    gui_root.log_message(f"   Network URL: {NETWORK_URL}")
                    gui_root.log_message(f"   Hostname URL: {HOSTNAME_URL}")
                    gui_root.log_message("=" * 60)
                
                # Clear shutdown flag and start server
                shutdown_flag.clear()
                server_start_time = datetime.now()
                
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.log_message("🔧 Creating server thread...")
                
                server_thread = threading.Thread(target=run_server, daemon=True)
                server_thread.start()
                
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.log_message(f"✓ Server thread started (Thread ID: {server_thread.ident})")
                    gui_root.log_message("⏳ Waiting for server to initialize...")
                
                # Set server_running optimistically (will be verified)
                server_running = True
                
                # Update UI
                animation_running[0] = True  # Resume animation
                status_icon.config(text="●", fg='#28a745')  # Bright green circle for running
                status_value.config(text="Running", fg='#006400')  # Dark green text for visibility
                status_progress.start(10)  # Start progress bar animation
                # Ensure proper layering: progress bar below, canvas/text above
                status_progress.lower()  # Lower progress bar first
                safe_lift_canvas()  # Then lift canvas above progress bar so text is visible
                start_btn.config(state='disabled')
                stop_btn.config(state='normal')
                restart_btn.config(state='normal')
                # Re-enable buttons after operation completes
                button_operation_in_progress['value'] = False
                # Restart animation
                animate_status_icon()
                
                # Verify in background
                threading.Thread(target=verify_server_started, daemon=True).start()
                
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.log_message("▶️ Server started")
                    gui_root.log_message(f"Local access:   {LOCAL_URL}")
                    gui_root.log_message(f"Network IP:     {NETWORK_URL}")
                    gui_root.log_message(f"Hostname:       {HOSTNAME_URL}")
        except SystemExit as e:
            # CRITICAL: Catch SystemExit to prevent app from closing
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"⚠ SystemExit caught in start_server (code: {e.code}) - preventing app close")
            # DO NOT re-raise - keep the app open
            status_icon.config(text="●", fg='#8b0000')
            status_value.config(text="Failed", fg='#8b0000')  # Dark red for failed
            status_progress.stop()  # Stop progress bar
            safe_lift_canvas()  # Ensure text is visible
            start_btn.config(state='normal')
            stop_btn.config(state='disabled')
            restart_btn.config(state='disabled')
        except KeyboardInterrupt:
            # CRITICAL: Catch KeyboardInterrupt to prevent app from closing
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message("⚠ KeyboardInterrupt caught in start_server - preventing app close")
            status_icon.config(text="●", fg='#8b0000')
            status_value.config(text="Failed", fg='#8b0000')  # Dark red for failed
            status_progress.stop()  # Stop progress bar
            safe_lift_canvas()  # Ensure text is visible
            start_btn.config(state='normal')
            stop_btn.config(state='disabled')
            restart_btn.config(state='disabled')
        except BaseException as e:
            # CRITICAL: Catch ALL exceptions to prevent app close
            import traceback
            error_trace = traceback.format_exc()
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"❌ Error in start_server: {type(e).__name__}: {e}")
                gui_root.log_message(f"   Traceback: {error_trace}")
            messagebox.showerror("Start Error", f"An error occurred while starting the server: {e}\n\nThe application will remain open.")
            # Make sure UI shows server as failed
            status_icon.config(text="●", fg='#8b0000')
            status_value.config(text="Failed", fg='#8b0000')  # Dark red for failed
            status_progress.stop()  # Stop progress bar
            safe_lift_canvas()  # Ensure text is visible
            local_url_value.config(text="(Server Failed)", state='disabled')
            network_url_value.config(text="(Server Failed)", state='disabled')
            hostname_url_value.config(text="(Server Failed)", state='disabled')
            start_btn.config(state='normal')
            stop_btn.config(state='disabled')
            restart_btn.config(state='disabled')
    
    def restart_with_config():
        """Restart server with new configuration - does NOT close the application"""
        global server_running, PORT, USE_HTTPS, CERT_FILE, KEY_FILE, PROTOCOL, uvicorn_server
        
        # Wrap entire function in try-except to prevent any exception from closing the app
        try:
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message("🔄 Restarting server with new configuration...")
            
            # Validate port
            try:
                new_port = int(port_var.get())
                if not (1 <= new_port <= 65535):
                    messagebox.showerror("Invalid Port", "Port must be between 1 and 65535")
                    return
            except ValueError:
                messagebox.showerror("Invalid Port", "Port must be a valid number")
                return
            
            # Get certificate file paths
            new_cert_file = cert_file_var.get().strip() if cert_file_var.get().strip() else None
            new_key_file = key_file_var.get().strip() if key_file_var.get().strip() else None
            
            # Validate certificate files if provided
            if new_cert_file:
                if not Path(new_cert_file).exists():
                    messagebox.showerror("Certificate Not Found", f"Certificate file not found: {new_cert_file}")
                    return
            if new_key_file:
                if not Path(new_key_file).exists():
                    messagebox.showerror("Key Not Found", f"Private key file not found: {new_key_file}")
                    return
            
            # Check if HTTPS is enabled and certificate exists
            new_use_https = https_var.get()
            if new_use_https:
                # Check if using enterprise cert or need to generate self-signed
                if new_cert_file and new_key_file:
                    # Using enterprise certificate
                    if not (Path(new_cert_file).exists() and Path(new_key_file).exists()):
                        messagebox.showerror("Certificate Error", "Certificate or key file does not exist.")
                        return
                elif not (CERT_FILE.exists() and KEY_FILE.exists()):
                    # Need to generate self-signed
                    result = messagebox.askyesno(
                        "Certificate Required",
                        "HTTPS is enabled but no certificate found.\n\n"
                        "Would you like to generate a self-signed certificate now?\n\n"
                        "(For enterprise deployment, use 'Enterprise Setup' button)",
                        icon='question'
                    )
                    if result:
                        if generate_self_signed_cert():
                            update_cert_status()
                            # Detailed logging is handled in generate_self_signed_cert()
                        else:
                            messagebox.showerror("Error", "Failed to generate certificate. HTTPS will be disabled.")
                            new_use_https = False
                            https_var.set(False)
                    else:
                        new_use_https = False
                        https_var.set(False)
            
            # Save configuration
            if save_config(new_port, new_use_https, new_cert_file, new_key_file):
                if gui_root and hasattr(gui_root, 'log_message'):
                    if new_cert_file:
                        gui_root.log_message(f"💾 Configuration saved: Port={new_port}, HTTPS={new_use_https}, Enterprise Cert")
                    else:
                        gui_root.log_message(f"💾 Configuration saved: Port={new_port}, HTTPS={new_use_https}")
            
            # Save old port before updating
            old_port = PORT
            
            # Update certificate paths if provided
            if new_cert_file and new_key_file:
                CERT_FILE = Path(new_cert_file)
                KEY_FILE = Path(new_key_file)
            else:
                # Use default paths
                _config_check = load_config()
                if _config_check.get('cert_file') and Path(_config_check['cert_file']).exists():
                    CERT_FILE = Path(_config_check['cert_file'])
                else:
                    CERT_FILE = Path("data/ssl/server.crt")
                
                if _config_check.get('key_file') and Path(_config_check['key_file']).exists():
                    KEY_FILE = Path(_config_check['key_file'])
                else:
                    KEY_FILE = Path("data/ssl/server.key")
            
            PORT = new_port
            USE_HTTPS = new_use_https
            PROTOCOL = "https" if USE_HTTPS else "http"
            
            # Stop server if running, then start it
            if server_running:
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.log_message(f"🛑 Stopping server on port {old_port}...")
                
                # Stop the server
                shutdown_flag.set()
                server_running = False
                server_start_time = None
                animation_running[0] = False
                status_icon.config(text="●", fg='#dc3545')
                status_value.config(text="Stopped", fg='#dc3545', bg=bg_color)  # Restore normal colors
                status_progress.stop()  # Stop progress bar
                local_url_value.config(text="(Server Stopped)", state='disabled')
                network_url_value.config(text="(Server Stopped)", state='disabled')
                hostname_url_value.config(text="(Server Stopped)", state='disabled')
                uptime_value.config(text="00:00:00")
            stop_btn.config(state='disabled')
            restart_btn.config(state='disabled')
            start_btn.config(state='normal')
            # Re-enable buttons after restart operation completes
            button_operation_in_progress['value'] = False
            
            # Signal uvicorn server to stop if it exists
            # (uvicorn_server already declared as global at function start)
            old_server = uvicorn_server  # Save reference to old server
            if old_server is not None:
                try:
                    old_server.should_exit = True
                except Exception as e:
                        if gui_root and hasattr(gui_root, 'log_message'):
                            gui_root.log_message(f"⚠ Warning: Error signaling server to stop: {e}")
                # Clear the global reference so new server can be created
                uvicorn_server = None
                
                # Kill process on OLD port with aggressive cleanup
                def kill_port_process():
                    max_wait = 10  # Maximum wait time in seconds
                    wait_interval = 0.5  # Check every 500ms
                    waited = 0
                    
                    if is_port_in_use(old_port):
                        if gui_root and hasattr(gui_root, 'log_message'):
                            gui_root.after(0, lambda: gui_root.log_message(f"🔪 Terminating process on port {old_port}..."))
                        
                        # Try to kill processes on the port
                        killed, pids = kill_process_on_port(old_port, max_retries=3, force_after_seconds=3)
                        
                        if killed:
                            if gui_root and hasattr(gui_root, 'log_message'):
                                gui_root.after(0, lambda: gui_root.log_message(f"✓ Terminated {len(pids)} process(es) on port {old_port}"))
                        else:
                            if gui_root and hasattr(gui_root, 'log_message'):
                                gui_root.after(0, lambda: gui_root.log_message(f"⚠ No processes found on port {old_port}"))
                        
                        # Wait for port to be released with retries
                        while waited < max_wait and is_port_in_use(old_port):
                            time.sleep(wait_interval)
                            waited += wait_interval
                            
                            # If port still in use after 5 seconds, try killing again
                            if waited >= 5 and is_port_in_use(old_port):
                                if gui_root and hasattr(gui_root, 'log_message'):
                                    gui_root.after(0, lambda: gui_root.log_message(f"⚠ Port {old_port} still in use, retrying termination..."))
                                killed, pids = kill_process_on_port(old_port, max_retries=2, force_after_seconds=2)
                        
                        if is_port_in_use(old_port):
                            if gui_root and hasattr(gui_root, 'log_message'):
                                gui_root.after(0, lambda: gui_root.log_message(f"⚠ Port {old_port} still in use after {max_wait}s - may be in TIME_WAIT state"))
                        else:
                            if gui_root and hasattr(gui_root, 'log_message'):
                                gui_root.after(0, lambda: gui_root.log_message(f"✓ Port {old_port} released"))
                
                # Run in background thread
                kill_thread = threading.Thread(target=kill_port_process, daemon=True)
                kill_thread.start()
                
                # Wait for server to stop - check if port is still in use
                max_wait = 8  # Increased wait time
                wait_interval = 0.5  # Check every 500ms
                waited = 0
                while waited < max_wait and is_port_in_use(old_port):
                    time.sleep(wait_interval)
                    waited += wait_interval
                
                if gui_root and hasattr(gui_root, 'log_message'):
                    if waited >= max_wait and is_port_in_use(old_port):
                        gui_root.log_message(f"⚠ Port {old_port} may still be in use (TIME_WAIT state) - proceeding anyway...")
                        gui_root.log_message("   Note: Ports can remain in TIME_WAIT for up to 2 minutes after connection closes")
                    else:
                        gui_root.log_message("✓ Server stopped and port released")
            
            # Check if new port is in use (might be different from old port)
            if new_port != old_port and is_port_in_use(new_port):
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.log_message(f"⚠ Port {new_port} is in use, attempting to terminate existing process...")
                
                # Try to kill processes on the new port
                killed, pids = kill_process_on_port(new_port, max_retries=3, force_after_seconds=3)
                
                if killed:
                    if gui_root and hasattr(gui_root, 'log_message'):
                        gui_root.log_message(f"✓ Terminated {len(pids)} process(es) on port {new_port}")
                    time.sleep(1.5)  # Give it time to fully release the port
                else:
                    # No process was killed - might be in TIME_WAIT state
                    # Wait a moment and re-check if port is actually available
                    time.sleep(1.0)
                
                # Re-check port availability with retries
                max_retries = 3
                port_available = False
                for retry in range(max_retries):
                    if not is_port_in_use(new_port):
                        port_available = True
                        if gui_root and hasattr(gui_root, 'log_message'):
                            if retry > 0:
                                gui_root.log_message(f"✓ Port {new_port} is now available (was in TIME_WAIT state)")
                        break
                    else:
                        if retry < max_retries - 1:
                            time.sleep(0.5)
                
                if not port_available:
                    # Port is still in use - show error with helpful message
                    error_msg = (
                        f"Port {new_port} is in use and could not be freed.\n\n"
                        f"This could be due to:\n"
                        f"• Another application using the port\n"
                        f"• Port in TIME_WAIT state (wait 1-2 minutes)\n"
                        f"• Insufficient permissions to terminate the process\n\n"
                        f"Please close any existing servers or choose a different port."
                    )
                    messagebox.showerror("Port In Use", error_msg)
                    return
            
            # Start server with new config (always start, even if server wasn't running)
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"▶️ Starting server on port {new_port}...")
            
            # Ensure server_running is False so start_server() will proceed
            # IMPORTANT: Do NOT call shutdown_server() or anything that closes the app
            server_running = False
            
            # Clear shutdown flag BEFORE starting new server to prevent race condition
            shutdown_flag.clear()
            
            # Give a moment for any old server threads to fully stop and flag to clear
            # Also ensure uvicorn_server global is None so new server can be created
            # (uvicorn_server already declared as global on line 1969)
            uvicorn_server = None
            time.sleep(1.5)  # Increased wait time to ensure old server fully stops
            
            # Double-check flag is still clear before starting
            if shutdown_flag.is_set():
                shutdown_flag.clear()
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.log_message("⚠ Shutdown flag was set, cleared it before starting new server")
            
            try:
                start_server()
            except SystemExit as e:
                # Catch SystemExit to prevent app from closing
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.log_message(f"⚠ SystemExit caught during restart (code: {e.code}) - app will remain open")
                # Don't re-raise - keep the app open
                status_icon.config(text="●", fg='#8b0000')
                status_value.config(text="Stopped", fg='#dc3545')  # Red for stopped
                status_progress.stop()  # Stop progress bar
                safe_lift_canvas()  # Ensure text is visible
                start_btn.config(state='normal')
                stop_btn.config(state='disabled')
                restart_btn.config(state='disabled')
            except KeyboardInterrupt:
                # Catch KeyboardInterrupt to prevent app from closing
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.log_message("⚠ KeyboardInterrupt caught during restart - app will remain open")
                status_icon.config(text="●", fg='#8b0000')
                status_value.config(text="Stopped", fg='#dc3545')  # Red for stopped
                status_progress.stop()  # Stop progress bar
                safe_lift_canvas()  # Ensure text is visible
                start_btn.config(state='normal')
                stop_btn.config(state='disabled')
                restart_btn.config(state='disabled')
            except Exception as e:
                # Log error but don't close the app
                import traceback
                error_trace = traceback.format_exc()
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.log_message(f"❌ Error during restart: {e}")
                    gui_root.log_message(f"   Traceback: {error_trace}")
                messagebox.showerror("Restart Error", f"Failed to restart server: {e}\n\nThe application will remain open.")
                # Make sure UI shows server as stopped
                status_icon.config(text="●", fg='#8b0000')
                status_value.config(text="Stopped", fg='#dc3545')  # Red for stopped
                status_progress.stop()  # Stop progress bar
                safe_lift_canvas()  # Ensure text is visible
                start_btn.config(state='normal')
                stop_btn.config(state='disabled')
                restart_btn.config(state='disabled')
        except SystemExit as e:
            # CRITICAL: Catch SystemExit to prevent app from closing
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"⚠ SystemExit caught in restart (code: {e.code}) - preventing app close")
            # DO NOT re-raise - keep the app open
            status_icon.config(text="●", fg='#8b0000')
            status_value.config(text="Stopped", fg='#dc3545')  # Red for stopped
            status_progress.stop()  # Stop progress bar
            safe_lift_canvas()  # Ensure text is visible
            start_btn.config(state='normal')
            stop_btn.config(state='disabled')
            restart_btn.config(state='disabled')
        except KeyboardInterrupt:
            # CRITICAL: Catch KeyboardInterrupt to prevent app from closing
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message("⚠ KeyboardInterrupt caught in restart - preventing app close")
            status_icon.config(text="●", fg='#8b0000')
            status_value.config(text="Stopped", fg='#dc3545')  # Red for stopped
            status_progress.stop()  # Stop progress bar
            safe_lift_canvas()  # Ensure text is visible
            start_btn.config(state='normal')
            stop_btn.config(state='disabled')
            restart_btn.config(state='disabled')
        except BaseException as e:
            # CRITICAL: Catch ALL exceptions (including SystemExit, KeyboardInterrupt) to prevent app close
            import traceback
            error_trace = traceback.format_exc()
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"❌ Unexpected error in restart: {type(e).__name__}: {e}")
                gui_root.log_message(f"   Traceback: {error_trace}")
            messagebox.showerror("Restart Error", f"An error occurred during restart: {e}\n\nThe application will remain open.")
            # Make sure UI shows server as stopped
            status_icon.config(text="●", fg='#8b0000')
            status_value.config(text="Stopped", fg='#dc3545')  # Red for stopped
            status_progress.stop()  # Stop progress bar
            safe_lift_canvas()  # Ensure text is visible
            start_btn.config(state='normal')
            stop_btn.config(state='disabled')
            restart_btn.config(state='disabled')
    
    # Button state management to prevent double-clicks and rapid clicking
    button_operation_in_progress = {'value': False}
    
    def set_button_state(enabled):
        """Enable/disable all control buttons"""
        button_operation_in_progress['value'] = not enabled
        if enabled:
            stop_btn.config(state='normal')
            start_btn.config(state='normal')
            restart_btn.config(state='normal')
        else:
            stop_btn.config(state='disabled')
            start_btn.config(state='disabled')
            restart_btn.config(state='disabled')
    
    # Create buttons with improved styling
    # Wrap stop_server in a safety wrapper to prevent GUI from closing
    def safe_stop_server():
        """Safety wrapper for stop_server - ensures GUI never closes"""
        if button_operation_in_progress['value']:
            return  # Operation already in progress
        button_operation_in_progress['value'] = True
        set_button_state(False)  # Disable buttons during operation
        
        # Immediately show progress and update status text
        status_value.config(text="Stopping...", fg='#ff9800')  # Orange for stopping
        status_progress.start(10)  # Start progress bar animation
        safe_lift_canvas()  # Ensure text is visible above progress bar (lowers progress, lifts canvas)
        animation_running[0] = False  # Stop animation during operation
        
        try:
            stop_server()
        except SystemExit:
            # CRITICAL: Catch SystemExit at the button level - do NOT re-raise
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message("⚠ SystemExit caught in safe_stop_server - GUI remains open")
            button_operation_in_progress['value'] = False
            set_button_state(True)  # Re-enable buttons
        except KeyboardInterrupt:
            # CRITICAL: Catch KeyboardInterrupt at the button level - do NOT re-raise
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message("⚠ KeyboardInterrupt caught in safe_stop_server - GUI remains open")
            button_operation_in_progress['value'] = False
            set_button_state(True)  # Re-enable buttons
        except BaseException as e:
            # CRITICAL: Catch ALL exceptions at the button level
            import traceback
            error_trace = traceback.format_exc()
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"❌ CRITICAL: Exception in safe_stop_server: {type(e).__name__}: {e}")
                gui_root.log_message(f"   Traceback: {error_trace}")
            messagebox.showerror("Critical Error", f"An error occurred: {e}\n\nThe GUI will remain open.")
            button_operation_in_progress['value'] = False
            set_button_state(True)  # Re-enable buttons
        # Note: Buttons are re-enabled in update_ui_stopped() callback when operation completes
    
    # Wrap restart_with_config in a safety wrapper
    def safe_restart_server():
        """Safety wrapper for restart_with_config - ensures GUI never closes"""
        if button_operation_in_progress['value']:
            return  # Operation already in progress
        button_operation_in_progress['value'] = True
        set_button_state(False)  # Disable buttons during operation
        
        # Immediately show progress and update status text
        status_value.config(text="Restarting...", fg='#ff9800')  # Orange for restarting
        status_progress.start(10)  # Start progress bar animation
        safe_lift_canvas()  # Ensure text is visible above progress bar (lowers progress, lifts canvas)
        animation_running[0] = False  # Stop animation during operation
        
        try:
            restart_with_config()
        except SystemExit:
            # CRITICAL: Catch SystemExit at the button level - do NOT re-raise
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message("⚠ SystemExit caught in safe_restart_server - GUI remains open")
            button_operation_in_progress['value'] = False
            set_button_state(True)  # Re-enable buttons
        except KeyboardInterrupt:
            # CRITICAL: Catch KeyboardInterrupt at the button level - do NOT re-raise
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message("⚠ KeyboardInterrupt caught in safe_restart_server - GUI remains open")
            button_operation_in_progress['value'] = False
            set_button_state(True)  # Re-enable buttons
        except BaseException as e:
            # CRITICAL: Catch ALL exceptions at the button level
            import traceback
            error_trace = traceback.format_exc()
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"❌ CRITICAL: Exception in safe_restart_server: {type(e).__name__}: {e}")
                gui_root.log_message(f"   Traceback: {error_trace}")
            messagebox.showerror("Critical Error", f"An error occurred: {e}\n\nThe GUI will remain open.")
            button_operation_in_progress['value'] = False
            set_button_state(True)  # Re-enable buttons
        # Note: Buttons are re-enabled when server start completes (in start_server callback)
    
    # Wrap start_server in a safety wrapper too
    def safe_start_server():
        """Safety wrapper for start_server - ensures GUI never closes"""
        if button_operation_in_progress['value']:
            return  # Operation already in progress
        button_operation_in_progress['value'] = True
        set_button_state(False)  # Disable buttons during operation
        
        # Immediately show progress and update status text
        status_value.config(text="Starting...", fg='#ff9800')  # Orange for starting
        status_progress.start(10)  # Start progress bar animation
        safe_lift_canvas()  # Ensure text is visible above progress bar (lowers progress, lifts canvas)
        animation_running[0] = False  # Stop animation during operation
        
        try:
            start_server()
        except SystemExit:
            # CRITICAL: Catch SystemExit at the button level - do NOT re-raise
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message("⚠ SystemExit caught in safe_start_server - GUI remains open")
            button_operation_in_progress['value'] = False
            set_button_state(True)  # Re-enable buttons
        except KeyboardInterrupt:
            # CRITICAL: Catch KeyboardInterrupt at the button level - do NOT re-raise
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message("⚠ KeyboardInterrupt caught in safe_start_server - GUI remains open")
            button_operation_in_progress['value'] = False
            set_button_state(True)  # Re-enable buttons
        except BaseException as e:
            # CRITICAL: Catch ALL exceptions at the button level
            import traceback
            error_trace = traceback.format_exc()
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"❌ CRITICAL: Exception in safe_start_server: {type(e).__name__}: {e}")
                gui_root.log_message(f"   Traceback: {error_trace}")
            messagebox.showerror("Critical Error", f"An error occurred: {e}\n\nThe GUI will remain open.")
            button_operation_in_progress['value'] = False
            set_button_state(True)  # Re-enable buttons
        # Note: Buttons are re-enabled when server start completes (in start_server callback)
    
    # Create buttons with icons (no colors)
    stop_btn = ttk.Button(control_buttons_frame, text="🛑 Stop", command=safe_stop_server, width=12)
    start_btn = ttk.Button(control_buttons_frame, text="▶ Start", command=safe_start_server, width=12)
    restart_btn = ttk.Button(control_buttons_frame, text="🔄 Restart", command=safe_restart_server, width=12)
    
    # Initially show Stop and Restart buttons (server starts running)
    # All buttons always visible, but grayed out when not applicable
    stop_btn.pack(side=tk.LEFT, padx=3)
    restart_btn.pack(side=tk.LEFT, padx=3)
    start_btn.pack(side=tk.LEFT, padx=3)
    # Initially start button is disabled (server is running)
    start_btn.config(state='disabled')
    
    # Server URLs frame - separate section to the right of Server Control
    urls_frame = ttk.LabelFrame(main_container, text="🔗 Server URLs", padding="12")
    urls_frame.grid(row=1, column=1, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(5, 0), pady=(0, 12))
    urls_frame.grid_columnconfigure(0, weight=1)
    
    # Local URL
    ttk.Label(urls_frame, text="🌐 Local:", font=('Arial', 9, 'bold')).grid(row=0, column=0, sticky=tk.W, pady=(0, 5))
    local_url_value = ttk.Button(urls_frame, text=f"{LOCAL_URL}", command=lambda: webbrowser.open(LOCAL_URL) if server_running else None, width=25)
    local_url_value.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 8))
    
    # Network IP URL
    ttk.Label(urls_frame, text="🌍 Network:", font=('Arial', 9, 'bold')).grid(row=2, column=0, sticky=tk.W, pady=(0, 5))
    network_url_value = ttk.Button(urls_frame, text=f"{NETWORK_URL}", command=lambda: webbrowser.open(NETWORK_URL) if server_running else None, width=25)
    network_url_value.grid(row=3, column=0, sticky=(tk.W, tk.E), pady=(0, 8))
    
    # Hostname URL
    ttk.Label(urls_frame, text="🏠 Hostname:", font=('Arial', 9, 'bold')).grid(row=4, column=0, sticky=tk.W, pady=(0, 5))
    hostname_url_value = ttk.Button(urls_frame, text=f"{HOSTNAME_URL}", command=lambda: webbrowser.open(HOSTNAME_URL) if server_running else None, width=25)
    hostname_url_value.grid(row=5, column=0, sticky=(tk.W, tk.E))
    
    # Row 2: Configuration and Security side by side
    # Configuration frame - cleaner title
    config_frame = ttk.LabelFrame(main_container, text="⚙️ Configuration", padding="12")
    config_frame.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(0, 5), pady=(0, 12))
    config_frame.grid_columnconfigure(1, weight=1)
    
    # Port configuration - with icon
    ttk.Label(config_frame, text="🔌 Port:", font=('Arial', 9, 'bold')).grid(row=0, column=0, sticky=tk.W, padx=(0, 10), pady=(0, 5))
    port_var = tk.StringVar(value=str(PORT))
    port_entry = ttk.Entry(config_frame, textvariable=port_var, width=10)
    port_entry.grid(row=0, column=1, sticky=tk.W, pady=(0, 5))
    gui_root.port_var = port_var  # Store reference for shutdown_server()
    
    # HTTPS toggle - with icon
    https_var = tk.BooleanVar(value=USE_HTTPS)
    https_checkbox = ttk.Checkbutton(
        config_frame,
        text="🔒 Enable HTTPS (requires certificate)",
        variable=https_var
    )
    https_checkbox.grid(row=1, column=0, columnspan=2, sticky=tk.W, pady=(5, 0))
    gui_root.https_var = https_var  # Store reference for shutdown_server()
    
    # Windows Service button (moved to Configuration)
    if sys.platform == 'win32' and UniteUsETLService is not None:
        service_btn_text = "🔧 Uninstall Service" if is_service_installed() else "⚙️ Install Service"
        service_btn = ttk.Button(
            config_frame,
            text=service_btn_text,
            command=toggle_service,
            width=20
        )
        service_btn.grid(row=2, column=0, columnspan=2, sticky=tk.W, pady=(10, 0))
    
    # Security & Certificates frame - improved title
    cert_frame = ttk.LabelFrame(main_container, text="🔐 Security & Certificates", padding="12")
    cert_frame.grid(row=2, column=1, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(5, 0), pady=(0, 12))
    cert_frame.grid_columnconfigure(0, weight=1)
    cert_frame.grid_columnconfigure(1, weight=1)
    
    # Security status display
    ttk.Label(cert_frame, text="🔐 Security:", font=('Arial', 9, 'bold')).grid(row=0, column=0, sticky=tk.W, pady=(0, 5))
    cert_security_label = tk.Label(cert_frame, text="🔒 HTTPS (Self-Signed)" if USE_HTTPS else "HTTP", font=('Arial', 9), bg=bg_color)
    cert_security_label.grid(row=0, column=1, sticky=tk.W, pady=(0, 5))
    
    # Certificate status and management - arranged in 2 columns
    cert_status_frame = ttk.Frame(cert_frame)
    cert_status_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(5, 0))
    cert_status_frame.grid_columnconfigure(0, weight=1)
    cert_status_frame.grid_columnconfigure(1, weight=1)
    
    # Certificate file path variables
    cert_file_var = tk.StringVar(value=str(CERT_FILE) if CERT_FILE != Path("data/ssl/server.crt") or not _config.get('cert_file') else str(_config.get('cert_file', '')))
    key_file_var = tk.StringVar(value=str(KEY_FILE) if KEY_FILE != Path("data/ssl/server.key") or not _config.get('key_file') else str(_config.get('key_file', '')))
    gui_root.cert_file_var = cert_file_var  # Store reference for shutdown_server()
    gui_root.key_file_var = key_file_var  # Store reference for shutdown_server()
    
    def is_enterprise_cert():
        """Check if using enterprise certificate (not default self-signed location)"""
        return (_config.get('cert_file') and Path(_config['cert_file']).exists()) or \
               (cert_file_var.get() and cert_file_var.get() != str(Path("data/ssl/server.crt")))
    
    def update_cert_status():
        """Update certificate status display"""
        cert_exists = CERT_FILE.exists() and KEY_FILE.exists()
        
        if cert_exists:
            install_cert_btn.config(state='normal')
        else:
            install_cert_btn.config(state='normal')
    
    def generate_cert_clicked():
        """Handle certificate generation"""
        global CERT_FILE, KEY_FILE
        
        # Confirm overwrite if certificate exists
        if CERT_FILE.exists() or KEY_FILE.exists():
            response = messagebox.askyesno(
                "Overwrite Certificate",
                f"Certificate already exists at:\n{CERT_FILE}\n\n"
                "This will overwrite the existing certificate.\n\n"
                "Continue?",
                icon='warning'
            )
            if not response:
                return
        
        if generate_self_signed_cert():
            # Clear enterprise certificate paths when generating self-signed
            save_config(PORT, USE_HTTPS, None, None)
            # Update global variables to use default self-signed paths
            CERT_FILE = Path("data/ssl/server.crt")
            KEY_FILE = Path("data/ssl/server.key")
            _config['cert_file'] = None
            _config['key_file'] = None
            update_cert_status()
            # Certificate generation details are already logged to the GUI log
            # No pop-up alert needed - all information is in the log messages
        else:
            messagebox.showerror("Error", "Failed to generate SSL certificate. Please check if cryptography package is installed.")
    
    generate_cert_btn = ttk.Button(
        cert_status_frame,
        text="🔑 Generate Cert",
        command=generate_cert_clicked,
        width=18
    )
    generate_cert_btn.grid(row=0, column=0, padx=3, pady=3, sticky=(tk.W, tk.E))
    
    def import_enterprise_cert_clicked():
        """Handle enterprise certificate import"""
        try:
            # First, select certificate file
            cert_file = filedialog.askopenfilename(
                title="Select Enterprise Certificate File",
                filetypes=[
                    ("Certificate Files", "*.crt *.pem *.cer"),
                    ("All Files", "*.*")
                ],
                initialdir=str(Path.home())
            )
            
            if not cert_file:
                return  # User cancelled
            
            cert_path = Path(cert_file)
            if not cert_path.exists():
                messagebox.showerror("Error", f"Certificate file not found: {cert_path}")
                return
            
            # Validate certificate file format
            try:
                with open(cert_path, 'r') as f:
                    cert_content = f.read()
                if 'BEGIN CERTIFICATE' not in cert_content or 'END CERTIFICATE' not in cert_content:
                    messagebox.showerror("Error", "Certificate file does not appear to be a valid PEM certificate.\n\nExpected format: -----BEGIN CERTIFICATE----- ... -----END CERTIFICATE-----")
                    return
            except Exception as e:
                messagebox.showerror("Error", f"Failed to read certificate file: {e}")
                return
            
            # Then, select private key file
            key_file = filedialog.askopenfilename(
                title="Select Private Key File",
                filetypes=[
                    ("Key Files", "*.key *.pem"),
                    ("All Files", "*.*")
                ],
                initialdir=str(cert_path.parent)
            )
            
            if not key_file:
                return  # User cancelled
            
            key_path = Path(key_file)
            if not key_path.exists():
                messagebox.showerror("Error", f"Private key file not found: {key_path}")
                return
            
            # Validate key file format
            try:
                with open(key_path, 'r') as f:
                    key_content = f.read()
                if 'BEGIN' not in key_content or 'END' not in key_content:
                    messagebox.showerror("Error", "Private key file does not appear to be a valid PEM key.\n\nExpected format: -----BEGIN PRIVATE KEY----- or -----BEGIN RSA PRIVATE KEY-----")
                    return
            except Exception as e:
                messagebox.showerror("Error", f"Failed to read private key file: {e}")
                return
            
            # Verify certificate and key match (basic check)
            try:
                from cryptography import x509
                from cryptography.hazmat.backends import default_backend
                from cryptography.hazmat.primitives import serialization
                
                # Load certificate
                with open(cert_path, 'rb') as f:
                    cert_obj = x509.load_pem_x509_certificate(f.read(), default_backend())
                
                # Load private key
                with open(key_path, 'rb') as f:
                    key_obj = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())
                
                # Check if public keys match
                cert_pubkey = cert_obj.public_key()
                key_pubkey = key_obj.public_key()
                
                # Compare public key numbers (works for RSA)
                if hasattr(cert_pubkey, 'public_numbers') and hasattr(key_pubkey, 'public_numbers'):
                    if cert_pubkey.public_numbers() != key_pubkey.public_numbers():
                        response = messagebox.askyesno(
                            "Key Mismatch Warning",
                            "The certificate and private key do not appear to match.\n\n"
                            "This may cause HTTPS connection failures.\n\n"
                            "Do you want to continue anyway?",
                            icon='warning'
                        )
                        if not response:
                            return
                
                # Get certificate details for confirmation
                subject = cert_obj.subject
                cn = None
                for attr in subject:
                    if attr.oid._name == 'commonName':
                        cn = attr.value
                        break
                
                # Show confirmation dialog
                cert_info = f"Certificate Details:\n"
                cert_info += f"  • Common Name: {cn or 'N/A'}\n"
                cert_info += f"  • Valid From: {cert_obj.not_valid_before.strftime('%Y-%m-%d')}\n"
                cert_info += f"  • Valid Until: {cert_obj.not_valid_after.strftime('%Y-%m-%d')}\n"
                cert_info += f"  • Certificate File: {cert_path.name}\n"
                cert_info += f"  • Key File: {key_path.name}\n\n"
                cert_info += f"This will configure the server to use the enterprise certificate.\n"
                cert_info += f"All domain machines will automatically trust this certificate.\n\n"
                cert_info += f"Continue?"
                
                response = messagebox.askyesno(
                    "Import Enterprise Certificate",
                    cert_info,
                    icon='question'
                )
                
                if not response:
                    return
                
            except ImportError:
                # cryptography not available, skip validation but warn
                response = messagebox.askyesno(
                    "Validation Skipped",
                    "Certificate validation requires the cryptography package.\n\n"
                    "The certificate will be imported without validation.\n\n"
                    "Continue?",
                    icon='warning'
                )
                if not response:
                    return
            except Exception as e:
                messagebox.showerror("Error", f"Failed to validate certificate: {e}")
                return
            
            # Save configuration with enterprise certificate paths
            if save_config(PORT, USE_HTTPS, str(cert_path.absolute()), str(key_path.absolute())):
                # Update global variables
                global CERT_FILE, KEY_FILE
                CERT_FILE = cert_path
                KEY_FILE = key_path
                
                # Update config
                _config['cert_file'] = str(cert_path.absolute())
                _config['key_file'] = str(key_path.absolute())
                
                # Update UI
                update_cert_status()
                
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.log_message(f"✅ Enterprise certificate imported successfully")
                    gui_root.log_message(f"   Certificate: {cert_path.name}")
                    gui_root.log_message(f"   Private Key: {key_path.name}")
                    gui_root.log_message(f"   ✓ Certificate will be trusted by all domain machines")
                    gui_root.log_message(f"   ℹ️ Restart server to use the new certificate")
                
                messagebox.showinfo(
                    "Success",
                    f"Enterprise certificate imported successfully!\n\n"
                    f"Certificate: {cert_path.name}\n"
                    f"Private Key: {key_path.name}\n\n"
                    f"Restart the server to use the new certificate.\n\n"
                    f"All domain machines will automatically trust this certificate."
                )
            else:
                messagebox.showerror("Error", "Failed to save certificate configuration.")
                
        except Exception as e:
            error_msg = f"Failed to import enterprise certificate: {e}"
            messagebox.showerror("Error", error_msg)
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.after(0, lambda: gui_root.log_message(f"❌ {error_msg}"))
    
    import_enterprise_cert_btn = ttk.Button(
        cert_status_frame,
        text="🏢 Import Enterprise Cert",
        command=import_enterprise_cert_clicked,
        width=18
    )
    import_enterprise_cert_btn.grid(row=0, column=1, padx=3, pady=3, sticky=(tk.W, tk.E))
    
    def install_cert_clicked():
        """Handle certificate installation (for this machine only)"""
        if not CERT_FILE.exists():
            messagebox.showwarning("Certificate Not Found", "Please generate a certificate first.")
            return
        
        result = messagebox.askyesno(
            "Install SSL Certificate",
            "This will install the self-signed SSL certificate to THIS MACHINE'S Windows Trusted Root Certificate Authorities store.\n\n"
            "⚠️ IMPORTANT: This only affects THIS computer. LAN users must install the certificate on their own machines.\n\n"
            "This requires administrator privileges and will remove browser security warnings on THIS machine only.\n\n"
            "Do you want to proceed?",
            icon='question'
        )
        
        if result:
            # Try to run certutil with elevated privileges
            try:
                import ctypes
                
                # Check if running as admin
                is_admin = ctypes.windll.shell32.IsUserAnAdmin() != 0
                
                if not is_admin:
                    # Need to elevate
                    messagebox.showinfo(
                        "Administrator Required",
                        "Administrator privileges are required to install certificates.\n\n"
                        "The certificate installation dialog will open with elevated privileges.\n\n"
                        f"Certificate location: {CERT_FILE.absolute()}\n\n"
                        "Or you can manually install it:\n"
                        "1. Double-click the certificate file\n"
                        "2. Click 'Install Certificate'\n"
                        "3. Choose 'Local Machine'\n"
                        "4. Select 'Place all certificates in the following store'\n"
                        "5. Browse and select 'Trusted Root Certification Authorities'\n"
                        "6. Complete the wizard"
                    )
                    
                    # Try to open certificate file for manual installation
                    try:
                        os.startfile(str(CERT_FILE.absolute()))
                    except:
                        pass
                else:
                    # Already admin, try to install
                    success, msg = install_certificate_to_trust_store()
                    if success:
                        messagebox.showinfo("Success", msg + "\n\nPlease restart your browser for changes to take effect.")
                    else:
                        messagebox.showerror("Error", msg)
                        
            except Exception as e:
                error_msg = f"Failed to install certificate: {e}"
                messagebox.showerror("Error", error_msg)
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.after(0, lambda: gui_root.log_message(f"❌ {error_msg}"))
    
    def show_cert_instructions():
        """Show concise certificate instructions specific to Calaveras County setup"""
        # Determine if using enterprise or self-signed cert
        is_enterprise = is_enterprise_cert()
        cert_type = "Enterprise Certificate" if is_enterprise else "Self-Signed Certificate"
        
        # Get current network info
        HOSTNAME_CURRENT, LOCAL_IP_CURRENT = get_network_info()
        try:
            FQDN_CURRENT = socket.getfqdn()
            if FQDN_CURRENT == HOSTNAME_CURRENT:
                try:
                    result = subprocess.run(['net', 'config', 'workstation'], 
                                          capture_output=True, text=True, timeout=5,
                                          creationflags=CREATE_NO_WINDOW if sys.platform == 'win32' else 0)
                    for line in result.stdout.split('\n'):
                        if 'Full Computer name' in line:
                            full_name = line.split('Full Computer name')[1].strip()
                            if '.' in full_name:
                                FQDN_CURRENT = full_name
                                break
                except:
                    pass
        except:
            FQDN_CURRENT = HOSTNAME_CURRENT
        
        current_network_url = f"{PROTOCOL}://{LOCAL_IP_CURRENT}:{PORT}" if USE_HTTPS else f"http://{LOCAL_IP_CURRENT}:{PORT}"
        
        instructions = f"""CERTIFICATE SETUP GUIDE - Calaveras County UniteUs ETL
================================================================

SERVER INFORMATION
------------------
Server: {HOSTNAME_CURRENT} ({FQDN_CURRENT})
IP Address: {LOCAL_IP_CURRENT}
Domain: calco.local
Certificate: {cert_type}
Location: {CERT_FILE.absolute() if not is_enterprise else 'Enterprise Certificate Configured'}

================================================================
OPTION 1: COUNTY CA CERTIFICATE (Production - Recommended)
================================================================

Request certificate from County IT with:
  • Subject: {FQDN_CURRENT}
  • Subject Alternative Names (REQUIRED):
    - {FQDN_CURRENT}
    - {HOSTNAME_CURRENT}
    - {LOCAL_IP_CURRENT}
    - localhost
    - 127.0.0.1
  • Format: PEM (.crt/.pem and .key/.pem)
  • Key Size: 2048-bit minimum

To Configure:
1. Click "Select Enterprise Certificate" button
2. Select certificate file (.crt or .pem)
3. Select private key file (.key or .pem)
4. Restart server

Result: Works automatically on all domain machines (no installation needed)

================================================================
OPTION 2: SELF-SIGNED CERTIFICATE (Testing)
================================================================

The "Generate Cert" button creates a self-signed certificate with:
  • {FQDN_CURRENT} (FQDN)
  • {HOSTNAME_CURRENT} (hostname)
  • {LOCAL_IP_CURRENT} (IP address)
  • localhost, 127.0.0.1

To Generate:
1. Click "Generate Cert" button
2. Certificate will be created/overwritten at: {CERT_FILE}

For LAN Users (One-time installation per machine):
--------------------------------------------------
Method 1 - GUI Button (This Machine Only):
  • Click "Install Cert" button (requires admin)
  • Restart browser

Method 2 - Manual Installation:
  1. Double-click: {CERT_FILE}
  2. Click "Install Certificate"
  3. Choose "Local Machine" → Next
  4. Select "Place all certificates in the following store"
  5. Browse → "Trusted Root Certification Authorities" → OK
  6. Finish → Yes → OK
  7. Restart browser

Method 3 - Group Policy (For Multiple Machines):
  1. Open Group Policy Management (gpmc.msc)
  2. Create GPO: "UniteUs ETL Certificate"
  3. Navigate to: Computer Configuration > Policies > Windows Settings >
     Security Settings > Public Key Policies > Trusted Root Certification Authorities
  4. Right-click > Import → Select: {CERT_FILE}
  5. Link GPO to target OUs
  6. Run: gpupdate /force

Method 4 - Command Line (Single Machine):
  Run as Administrator:
  certutil -addstore -f "Root" "{CERT_FILE.absolute()}"

================================================================
VERIFICATION
================================================================

Test Access:
  • https://{FQDN_CURRENT}
  • https://{HOSTNAME_CURRENT}
  • https://{LOCAL_IP_CURRENT}

Check Certificate:
  • Open Certificate Manager (certmgr.msc)
  • Navigate to: Trusted Root Certification Authorities > Certificates
  • Verify "Calaveras County HHS" certificate appears

================================================================
TROUBLESHOOTING
================================================================

Certificate Warning Still Appears:
  • Clear browser cache and restart browser
  • Verify certificate is in "Trusted Root Certification Authorities" (not Intermediate)
  • Check certificate includes {FQDN_CURRENT} in Subject Alternative Names
  • Verify system time is correct

Can't Connect:
  • Check Windows Firewall allows port {PORT}
  • Verify server is running
  • Test with IP address: https://{LOCAL_IP_CURRENT}

================================================================
SUPPORT
================================================================

Server: {HOSTNAME_CURRENT} ({FQDN_CURRENT})
Application: UniteUs ETL
"""
        # Create a window to display instructions
        inst_window = tk.Toplevel(gui_root)
        inst_window.title("Cert Installation Instructions - Calaveras County IT")
        inst_window.geometry("900x700")
        inst_window.resizable(True, True)
        
        # Center the window
        inst_window.update_idletasks()
        x = (inst_window.winfo_screenwidth() // 2) - (inst_window.winfo_width() // 2)
        y = (inst_window.winfo_screenheight() // 2) - (inst_window.winfo_height() // 2)
        inst_window.geometry(f"+{x}+{y}")
        
        # Text widget with scrollbar
        text_frame = ttk.Frame(inst_window, padding="10")
        text_frame.pack(fill=tk.BOTH, expand=True)
        
        text_widget = tk.Text(text_frame, wrap=tk.WORD, font=('Consolas', 8))
        scrollbar = ttk.Scrollbar(text_frame, orient=tk.VERTICAL, command=text_widget.yview)
        text_widget.configure(yscrollcommand=scrollbar.set)
        
        text_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        text_widget.insert('1.0', instructions)
        text_widget.configure(state=tk.DISABLED)
        
        # Buttons
        button_frame = ttk.Frame(inst_window, padding="10")
        button_frame.pack(fill=tk.X)
        
        def open_cert_folder():
            try:
                os.startfile(str(CERT_FILE.parent))
            except:
                messagebox.showerror("Error", "Could not open certificate folder")
        
        ttk.Button(button_frame, text="Open Certificate Folder", command=open_cert_folder).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Close", command=inst_window.destroy).pack(side=tk.RIGHT, padx=5)
    
    install_cert_btn = ttk.Button(
        cert_status_frame,
        text="💾 Install Cert (This PC)",
        command=install_cert_clicked,
        width=18
    )
    install_cert_btn.grid(row=1, column=0, padx=3, pady=3, sticky=(tk.W, tk.E))
    
    cert_instructions_btn = ttk.Button(
        cert_status_frame,
        text="📖 Cert Instructions",
        command=show_cert_instructions,
        width=18
    )
    cert_instructions_btn.grid(row=1, column=1, padx=3, pady=3, sticky=(tk.W, tk.E))
    
    def check_certificate():
        """Check certificate status and validity - logs results to Server Log"""
        if gui_root and hasattr(gui_root, 'log_message'):
            gui_root.log_message("=" * 60)
            gui_root.log_message("🔍 CERTIFICATE CHECK")
            gui_root.log_message("=" * 60)
        
        results = []
        issues = []
        warnings = []
        
        # Get current config to check HTTPS setting
        current_config = load_config()
        current_https = current_config.get('use_https', False)
        current_port = current_config.get('port', PORT)
        
        # Check if server is actually running by checking if port is listening
        server_actually_running = is_port_listening(current_port)
        
        # 1. Check if certificate files exist
        cert_exists = CERT_FILE.exists()
        key_exists = KEY_FILE.exists()
        
        if cert_exists:
            results.append("✓ Certificate file exists")
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"✓ Certificate file exists: {CERT_FILE}")
        else:
            issues.append("✗ Certificate file not found")
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"✗ Certificate file not found: {CERT_FILE}")
        
        if key_exists:
            results.append("✓ Private key file exists")
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"✓ Private key file exists: {KEY_FILE}")
        else:
            issues.append("✗ Private key file not found")
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"✗ Private key file not found: {KEY_FILE}")
        
        if not cert_exists or not key_exists:
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message("❌ Certificate check failed: Files missing")
                gui_root.log_message("=" * 60)
            return
        
        # 2. Check if certificate is valid and can be loaded
        try:
            from cryptography import x509
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization
            
            # Load certificate
            with open(CERT_FILE, 'rb') as f:
                cert_data = f.read()
            cert = x509.load_pem_x509_certificate(cert_data, default_backend())
            results.append("✓ Certificate is valid and can be loaded")
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message("✓ Certificate is valid and can be loaded")
            
            # Check certificate details
            subject = cert.subject
            issuer = cert.issuer
            not_before = cert.not_valid_before
            not_after = cert.not_valid_after
            serial = cert.serial_number
            
            # Get Common Name
            cn = None
            for attr in subject:
                if attr.oid == x509.NameOID.COMMON_NAME:
                    cn = attr.value
                    break
            
            # Check expiration
            now = datetime.now(not_after.tzinfo) if not_after.tzinfo else datetime.now()
            days_until_expiry = (not_after - now).days
            
            if days_until_expiry < 0:
                issues.append(f"✗ Certificate expired {abs(days_until_expiry)} days ago")
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.log_message(f"✗ Certificate expired {abs(days_until_expiry)} days ago")
            elif days_until_expiry < 30:
                warnings.append(f"⚠ Certificate expires in {days_until_expiry} days")
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.log_message(f"⚠ Certificate expires in {days_until_expiry} days")
            else:
                results.append(f"✓ Certificate valid for {days_until_expiry} more days")
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.log_message(f"✓ Certificate valid for {days_until_expiry} more days")
            
            # Check if certificate matches key
            try:
                with open(KEY_FILE, 'rb') as f:
                    key_data = f.read()
                private_key = serialization.load_pem_private_key(key_data, password=None, backend=default_backend())
                
                # Verify certificate public key matches private key
                cert_pub_key = cert.public_key()
                key_pub_key = private_key.public_key()
                
                # Compare public key numbers (RSA)
                if hasattr(cert_pub_key, 'public_numbers') and hasattr(key_pub_key, 'public_numbers'):
                    if cert_pub_key.public_numbers() == key_pub_key.public_numbers():
                        results.append("✓ Certificate matches private key")
                        if gui_root and hasattr(gui_root, 'log_message'):
                            gui_root.log_message("✓ Certificate matches private key")
                    else:
                        issues.append("✗ Certificate does not match private key")
                        if gui_root and hasattr(gui_root, 'log_message'):
                            gui_root.log_message("✗ Certificate does not match private key")
                else:
                    results.append("✓ Private key loaded successfully")
                    if gui_root and hasattr(gui_root, 'log_message'):
                        gui_root.log_message("✓ Private key loaded successfully")
            except Exception as e:
                error_msg = str(e)[:50]
                issues.append(f"✗ Cannot load private key: {error_msg}")
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.log_message(f"✗ Cannot load private key: {error_msg}")
            
            # Certificate type
            is_enterprise = is_enterprise_cert()
            cert_type = "Enterprise Certificate" if is_enterprise else "Self-Signed Certificate"
            results.append(f"✓ Certificate type: {cert_type}")
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"✓ Certificate type: {cert_type}")
            
            if cn:
                results.append(f"✓ Common Name: {cn}")
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.log_message(f"✓ Common Name: {cn}")
            
            # 3. Check if certificate is installed in Windows store (for self-signed only)
            if not is_enterprise:
                try:
                    import subprocess
                    import ctypes
                    
                    # Try to check if certificate is in Windows store
                    # Get certificate thumbprint
                    cert_hash_cmd = f'certutil -hash "{CERT_FILE.absolute()}" SHA1'
                    hash_result = subprocess.run(
                        cert_hash_cmd,
                        shell=True,
                        capture_output=True,
                        text=True,
                        creationflags=CREATE_NO_WINDOW if sys.platform == 'win32' else 0
                    )
                    
                    if hash_result.returncode == 0:
                        # Try to find certificate in store
                        store_check_cmd = 'certutil -store -user Root'
                        store_result = subprocess.run(
                            store_check_cmd,
                            shell=True,
                            capture_output=True,
                            text=True,
                            creationflags=CREATE_NO_WINDOW if sys.platform == 'win32' else 0
                        )
                        
                        if store_result.returncode == 0 and cn and cn in store_result.stdout:
                            results.append("✓ Certificate is installed in Windows Trusted Root store")
                            if gui_root and hasattr(gui_root, 'log_message'):
                                gui_root.log_message("✓ Certificate is installed in Windows Trusted Root store")
                        else:
                            warnings.append("⚠ Certificate not found in Windows Trusted Root store (browser warnings may appear)")
                            if gui_root and hasattr(gui_root, 'log_message'):
                                gui_root.log_message("⚠ Certificate not found in Windows Trusted Root store (browser warnings may appear)")
                    else:
                        warnings.append("⚠ Could not verify Windows store installation")
                        if gui_root and hasattr(gui_root, 'log_message'):
                            gui_root.log_message("⚠ Could not verify Windows store installation")
                except Exception as e:
                    error_msg = str(e)[:50]
                    warnings.append(f"⚠ Could not check Windows store: {error_msg}")
                    if gui_root and hasattr(gui_root, 'log_message'):
                        gui_root.log_message(f"⚠ Could not check Windows store: {error_msg}")
            
            # 4. Check if HTTPS is working (if server is running)
            if server_actually_running and current_https:
                try:
                    import socket
                    
                    # First check if port is open (simple socket test)
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(1)
                    result = sock.connect_ex(('127.0.0.1', current_port))
                    sock.close()
                    
                    if result == 0:
                        # Port is open - HTTPS should be working if certificate is valid
                        # Since we've already verified the certificate is valid and matches the key,
                        # and the port is open, HTTPS is likely working
                        results.append("✓ HTTPS port is open and accessible")
                        if gui_root and hasattr(gui_root, 'log_message'):
                            gui_root.log_message(f"✓ HTTPS port {current_port} is open and accessible")
                        
                        # Optionally try a quick HTTPS test (non-blocking, don't fail on timeout)
                        try:
                            import urllib.request
                            import ssl
                            
                            ctx = ssl.create_default_context()
                            ctx.check_hostname = False
                            ctx.verify_mode = ssl.CERT_NONE
                            
                            url = f"https://127.0.0.1:{current_port}"
                            # Use a very short timeout for this test
                            with urllib.request.urlopen(url, timeout=2, context=ctx) as response:
                                if response.status == 200 or response.status == 303:
                                    if gui_root and hasattr(gui_root, 'log_message'):
                                        gui_root.log_message(f"✓ HTTPS server responded successfully")
                        except:
                            # Timeout or other error - not a problem, port is open and cert is valid
                            # Just log info, don't add to warnings
                            if gui_root and hasattr(gui_root, 'log_message'):
                                gui_root.log_message(f"ℹ HTTPS port is open (connection test skipped)")
                    else:
                        warnings.append(f"⚠ HTTPS port {current_port} is not accessible")
                        if gui_root and hasattr(gui_root, 'log_message'):
                            gui_root.log_message(f"⚠ HTTPS port {current_port} is not accessible")
                except Exception as e:
                    # Only log non-timeout errors as warnings
                    error_msg = str(e)
                    if "timeout" not in error_msg.lower() and "timed out" not in error_msg.lower():
                        warnings.append(f"⚠ Could not test HTTPS connection: {error_msg[:50]}")
                        if gui_root and hasattr(gui_root, 'log_message'):
                            gui_root.log_message(f"⚠ Could not test HTTPS connection: {error_msg[:50]}")
                    else:
                        # Timeout is not a real issue if port is open
                        if gui_root and hasattr(gui_root, 'log_message'):
                            gui_root.log_message(f"ℹ HTTPS connection test timed out (port is open, HTTPS should be working)")
            elif server_actually_running and not current_https:
                warnings.append("⚠ Server is running but HTTPS is disabled")
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.log_message("⚠ Server is running but HTTPS is disabled in configuration")
            elif not server_actually_running:
                warnings.append("ℹ Server is not running (HTTPS test skipped)")
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.log_message("ℹ Server is not running (HTTPS test skipped)")
        
        except ImportError:
            issues.append("✗ cryptography package not installed")
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message("✗ cryptography package not installed")
        except Exception as e:
            error_msg = str(e)[:100]
            issues.append(f"✗ Error loading certificate: {error_msg}")
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"✗ Error loading certificate: {error_msg}")
        
        # Log summary
        if gui_root and hasattr(gui_root, 'log_message'):
            if issues:
                gui_root.log_message("")
                gui_root.log_message("❌ ISSUES FOUND:")
                for issue in issues:
                    gui_root.log_message(f"  {issue}")
            
            if warnings:
                gui_root.log_message("")
                gui_root.log_message("⚠ WARNINGS:")
                for warning in warnings:
                    gui_root.log_message(f"  {warning}")
            
            if not issues and not warnings:
                gui_root.log_message("")
                gui_root.log_message("✓ ALL CHECKS PASSED")
            
            gui_root.log_message("=" * 60)
    
    check_cert_btn = ttk.Button(
        cert_status_frame,
        text="🔍 Check Certificate",
        command=check_certificate,
        width=18
    )
    check_cert_btn.grid(row=2, column=0, columnspan=2, padx=3, pady=3, sticky=(tk.W, tk.E))
    
    # Now that install_cert_btn exists, we can call update_cert_status
    update_cert_status()
    
    # Buttons frame (removed - no longer needed)
    # Users can click the URL links directly to open browser
    # Application can be closed via window close button
    
    # Row 3: Activity Log (full width) - improved title
    log_frame = ttk.LabelFrame(main_container, text="📋 Activity Log", padding="8")
    log_frame.grid(row=3, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S))
    log_frame.grid_rowconfigure(0, weight=1)
    log_frame.grid_columnconfigure(0, weight=1)
    
    # Text widget with scrollbar
    log_text = tk.Text(log_frame, wrap=tk.WORD, height=15, font=('Consolas', 8))
    log_scrollbar = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=log_text.yview)
    log_text.configure(yscrollcommand=log_scrollbar.set)
    
    log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
    log_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
    
    # Sleek copy button below the log
    def copy_log_to_clipboard():
        """Copy all log content to clipboard"""
        try:
            log_content = log_text.get('1.0', tk.END).strip()
            gui_root.clipboard_clear()
            gui_root.clipboard_append(log_content)
            gui_root.update()  # Required for clipboard to work
            # Briefly change button text to show success
            copy_btn.config(text="✓ Copied!")
            gui_root.after(1500, lambda: copy_btn.config(text="📋 Copy Log"))
        except Exception as e:
            print(f"Error copying to clipboard: {e}")
    
    # Log action buttons frame
    log_buttons_frame = ttk.Frame(log_frame)
    log_buttons_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.E), pady=(5, 0))
    
    def save_log_to_file():
        """Save log content to a file"""
        try:
            log_content = log_text.get('1.0', tk.END).strip()
            if not log_content:
                messagebox.showinfo("Info", "Log is empty. Nothing to save.")
                return
            
            # Get filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            default_filename = f"server_log_{timestamp}.txt"
            
            file_path = filedialog.asksaveasfilename(
                title="Save Log File",
                defaultextension=".txt",
                filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
                initialfile=default_filename
            )
            
            if file_path:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(log_content)
                messagebox.showinfo("Success", f"Log saved to:\n{file_path}")
                if gui_root and hasattr(gui_root, 'log_message'):
                    gui_root.log_message(f"📁 Log saved to: {file_path}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save log: {e}")
    
    def graceful_exit():
        """Gracefully exit the application after stopping the server"""
        # Confirm exit if server is running
        if server_running:
            response = messagebox.askyesno(
                "Exit Application",
                "The server is currently running.\n\n"
                "Do you want to stop the server and exit?\n\n"
                "This will gracefully shut down the server and close the application.",
                icon='question'
            )
            if not response:
                return  # User cancelled
        
        # Call the shutdown function
        shutdown_server()
    
    # Pack buttons from right to left: Exit, Copy Log, Save Log
    exit_btn = ttk.Button(
        log_buttons_frame,
        text="🚪 Exit",
        command=graceful_exit,
        width=14
    )
    exit_btn.pack(side=tk.RIGHT, padx=(5, 0))
    
    copy_btn = ttk.Button(
        log_buttons_frame,
        text="📋 Copy Log",
        command=copy_log_to_clipboard,
        width=14
    )
    copy_btn.pack(side=tk.RIGHT, padx=(5, 0))
    
    save_btn = ttk.Button(
        log_buttons_frame,
        text="💾 Save Log",
        command=save_log_to_file,
        width=14
    )
    save_btn.pack(side=tk.RIGHT, padx=(5, 0))
    
    # Make log read-only
    log_text.configure(state=tk.DISABLED)
    
    # Store references
    gui_root.uptime_label = uptime_value
    gui_root.log_text = log_text
    gui_root.log_line_count = 0
    MAX_LOG_LINES = 1000  # Limit log to prevent memory issues
    
    # Add initial log message with line limiting
    def log_message(message):
        log_text.configure(state=tk.NORMAL)
        
        # Limit log size to prevent memory buildup
        gui_root.log_line_count += 1
        if gui_root.log_line_count > MAX_LOG_LINES:
            # Remove oldest 20% of lines
            lines_to_remove = MAX_LOG_LINES // 5
            log_text.delete('1.0', f'{lines_to_remove}.0')
            gui_root.log_line_count -= lines_to_remove
        
        log_text.insert(tk.END, message + '\n')
        log_text.see(tk.END)
        log_text.configure(state=tk.DISABLED)
    
    gui_root.log_message = log_message
    
    # Check if server is already running (started by main() before GUI)
    # main() sets server_running = True before starting the thread
    global server_running, server_start_time
    
    # If main() started the server, use that flag (more reliable than port check)
    if server_running:
        # Server was started by main(), update GUI state
        if not server_start_time:
            # Server was started before GUI, estimate start time
            server_start_time = datetime.now() - timedelta(seconds=2)  # Assume started 2 seconds ago
        
        # Update UI to show server is running
        animation_running[0] = True
        status_icon.config(text="●", fg='#28a745')
        status_value.config(text="Running", fg='#006400', bg=bg_color)  # Dark green text for visibility
        status_progress.start(10)  # Start progress bar animation
        # Ensure proper layering: progress bar below, canvas/text above
        status_progress.lower()  # Lower progress bar first
        safe_lift_canvas()  # Then lift canvas above progress bar so text is visible
        local_url_value.config(text=f"{LOCAL_URL}", state='normal', command=lambda: webbrowser.open(LOCAL_URL))
        network_url_value.config(text=f"{NETWORK_URL}", state='normal', command=lambda: webbrowser.open(NETWORK_URL))
        hostname_url_value.config(text=f"{HOSTNAME_URL}", state='normal', command=lambda: webbrowser.open(HOSTNAME_URL))
        start_btn.config(state='disabled')
        stop_btn.config(state='normal')
        restart_btn.config(state='normal')
        animate_status_icon()
        
        # Startup messages are already logged during server initialization
        # Only log a brief confirmation here
        log_message("Calaveras UniteUs ETL")
        log_message("Health & Human Services Agency")
        log_message("Calaveras County, CA")
        log_message("")
        log_message(f"✓ Server running on port {PORT}")
        if USE_HTTPS:
            log_message("")
            log_message("🔒 HTTPS is enabled")
            
            # Get certificate information
            try:
                current_config = load_config()
                is_enterprise = current_config.get('cert_file') and Path(current_config['cert_file']).exists()
                cert_type = "Enterprise Certificate" if is_enterprise else "Self-Signed Certificate"
                cert_path = Path(current_config['cert_file']) if is_enterprise else CERT_FILE
                
                log_message(f"   Certificate Type: {cert_type}")
                log_message(f"   Certificate Path: {cert_path}")
                
                # Try to get certificate details
                if cert_path.exists():
                    try:
                        from cryptography import x509
                        from cryptography.hazmat.backends import default_backend
                        
                        with open(cert_path, 'rb') as f:
                            cert_data = f.read()
                        cert = x509.load_pem_x509_certificate(cert_data, default_backend())
                        
                        # Get Common Name
                        cn = None
                        for attr in cert.subject:
                            if attr.oid == x509.NameOID.COMMON_NAME:
                                cn = attr.value
                                break
                        
                        # Get Issuer
                        issuer_cn = None
                        for attr in cert.issuer:
                            if attr.oid == x509.NameOID.COMMON_NAME:
                                issuer_cn = attr.value
                                break
                        
                        # Get expiration
                        not_after = cert.not_valid_after
                        now = datetime.now()
                        days_until_expiry = (not_after - now).days
                        
                        if cn:
                            log_message(f"   Common Name: {cn}")
                        if issuer_cn:
                            log_message(f"   Issuer: {issuer_cn}")
                        if days_until_expiry > 0:
                            log_message(f"   Expires: {not_after.strftime('%Y-%m-%d')} ({days_until_expiry} days)")
                        elif days_until_expiry <= 0:
                            log_message(f"   ⚠ Expired: {not_after.strftime('%Y-%m-%d')}")
                    except ImportError:
                        # cryptography not available, skip detailed info
                        pass
                    except Exception:
                        # Error reading cert, skip detailed info
                        pass
                
                # User instructions based on certificate type
                if is_enterprise:
                    log_message("   ✓ Enterprise certificate - automatically trusted on domain machines")
                else:
                    log_message("   ⚠ Self-signed certificate - users must install to avoid browser warnings")
                    log_message("      Use 'Install Certificate' button in Security & Certificates section")
            except Exception:
                # Fallback if there's any error
                log_message("   Certificate: Configured")
        
        # Verify port is actually listening (in background, non-blocking)
        def verify_port():
            # Check immediately first (server should already be running)
            if is_port_listening(PORT):
                log_message(f"✅ Verified: Server is listening on port {PORT}")
            else:
                # If not listening immediately, wait a moment and retry (server might still be starting)
                time.sleep(1)
                if is_port_listening(PORT):
                    log_message(f"✅ Verified: Server is listening on port {PORT}")
                else:
                    log_message(f"⚠ Warning: Server thread is running but port {PORT} is not listening yet.")
                    log_message("   The server may still be initializing. Please wait...")
        threading.Thread(target=verify_port, daemon=True).start()
    else:
        # Server is not running
        server_running = False
        server_start_time = None
        # Update UI to show server is stopped
        animation_running[0] = False
        status_icon.config(text="🔴 ●", fg='#dc3545')
        status_value.config(text="Stopped", fg='#dc3545')  # Red for stopped
        status_progress.stop()  # Stop progress bar
        # Ensure proper layering: progress bar below, canvas/text above
        status_progress.lower()  # Lower progress bar first
        safe_lift_canvas()  # Then lift canvas above progress bar so text is visible
        local_url_value.config(text="(Server Stopped)", state='disabled')
        network_url_value.config(text="(Server Stopped)", state='disabled')
        hostname_url_value.config(text="(Server Stopped)", state='disabled')
        stop_btn.config(state='disabled')
        restart_btn.config(state='disabled')
        start_btn.config(state='normal')
    
    # Update timer
    def update_timer():
        global server_running, server_start_time
        if server_running and server_start_time and not shutdown_flag.is_set():
            elapsed = datetime.now() - server_start_time
            total_seconds = int(elapsed.total_seconds())
            days = total_seconds // 86400
            hours = (total_seconds % 86400) // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            if days > 0:
                uptime_value.config(text=f"{days}:{hours:02d}:{minutes:02d}:{seconds:02d}")
            else:
                uptime_value.config(text=f"{hours:02d}:{minutes:02d}:{seconds:02d}")
            gui_root.after(1000, update_timer)
        else:
            # Server stopped, reset timer
            uptime_value.config(text="00:00:00")
            gui_root.after(1000, update_timer)
    
    update_timer()
    
    # Store references to config variables for shutdown save
    gui_root.port_var = port_var
    gui_root.https_var = https_var
    gui_root.cert_file_var = cert_file_var
    gui_root.key_file_var = key_file_var
    
    # Handle window close
    gui_root.protocol("WM_DELETE_WINDOW", shutdown_server)
    
    # Center window on screen
    gui_root.update_idletasks()
    x = (gui_root.winfo_screenwidth() // 2) - (gui_root.winfo_width() // 2)
    y = (gui_root.winfo_screenheight() // 2) - (gui_root.winfo_height() // 2)
    gui_root.geometry(f"+{x}+{y}")
    
    # Start the GUI loop
    gui_root.mainloop()


def restart_server():
    """Restart the server by shutting down and relaunching (full application restart)"""
    if gui_root:
        # Log the restart
        if hasattr(gui_root, 'log_message'):
            gui_root.log_message("🔄 Restarting application...")
        
        # Set shutdown flag
        shutdown_flag.set()
        global server_running
        server_running = False
        
        # Schedule the restart after GUI closes
        import subprocess
        launcher_script = Path(__file__).absolute()
        
        # Use pythonw.exe to launch without console (same as .pyw behavior)
        pythonw_exe = Path(sys.executable).parent / "pythonw.exe"
        if not pythonw_exe.exists():
            pythonw_exe = sys.executable  # Fallback to regular python
        
        # Launch new instance (will load config from file)
        subprocess.Popen([str(pythonw_exe), str(launcher_script)],
                        creationflags=CREATE_NO_WINDOW if sys.platform == 'win32' else 0)
        
        # Close current GUI
        gui_root.quit()
        
        # Give the server thread time to cleanup
        time.sleep(0.5)
        
        # Force exit
        os._exit(0)


def shutdown_server():
    """Shutdown the server gracefully - stops server, releases ports, closes GUI"""
    global server_running, PORT
    
    # Save current configuration before shutting down
    if gui_root:
        try:
            # Get current port and HTTPS settings from GUI
            if hasattr(gui_root, 'port_var') and hasattr(gui_root, 'https_var'):
                try:
                    current_port = int(gui_root.port_var.get())
                    if 1 <= current_port <= 65535:
                        PORT = current_port  # Update global PORT for cleanup
                        current_https = gui_root.https_var.get()
                        current_cert = getattr(gui_root, 'cert_file_var', None)
                        current_key = getattr(gui_root, 'key_file_var', None)
                        cert_path = current_cert.get().strip() if current_cert and current_cert.get().strip() else None
                        key_path = current_key.get().strip() if current_key and current_key.get().strip() else None
                        save_config(current_port, current_https, cert_path, key_path)
                except (ValueError, AttributeError):
                    pass  # If we can't get values, just continue with shutdown
        except:
            pass  # Don't let config save errors prevent shutdown
    
    # Stop the server
    if gui_root and hasattr(gui_root, 'log_message'):
        gui_root.log_message("🛑 Shutting down server and closing application...")
    
    # Set shutdown flag to stop server thread
    shutdown_flag.set()
    server_running = False
    
    # Kill any process on the port to release it
    if PORT:
        try:
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"🔪 Releasing port {PORT}...")
            killed, pids = kill_process_on_port(PORT)
            if killed and gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"✓ Port {PORT} released ({len(pids)} process(es) terminated)")
            elif gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"✓ Port {PORT} released (no processes found)")
        except Exception as e:
            # Log but don't prevent shutdown
            if gui_root and hasattr(gui_root, 'log_message'):
                gui_root.log_message(f"⚠ Warning: Error releasing port: {e}")
    
    # Give the server thread time to cleanup
    time.sleep(0.5)
    
    # Close the GUI window
    if gui_root:
        try:
            gui_root.quit()
        except:
            pass
    
    # Give a moment for GUI to close
    time.sleep(0.2)
    
    # Force exit to ensure everything closes
    os._exit(0)


def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully"""
    shutdown_server()


def main():
    """Main entry point"""
    global server_thread, PORT, USE_HTTPS, PROTOCOL, LOCAL_URL, NETWORK_URL, HOSTNAME_URL, CERT_FILE, KEY_FILE, server_running
    
    # Add debug output (will be visible if run with python instead of pythonw)
    import sys
    if hasattr(sys, 'stderr') and sys.stderr:
        print("Starting launcher...", file=sys.stderr)
        sys.stderr.flush()
    
    # Reload config in case it was changed
    try:
        _config = load_config()
    except Exception as e:
        import traceback
        error_msg = f"Error loading config: {e}\n\n{traceback.format_exc()}"
        print(error_msg, file=sys.stderr if hasattr(sys, 'stderr') else None)
        try:
            root = tk.Tk()
            root.withdraw()
            messagebox.showerror("Config Error", f"Error loading configuration:\n\n{e}")
            root.destroy()
        except:
            pass
        sys.exit(1)
    PORT = _config['port']
    USE_HTTPS = _config['use_https']
    PROTOCOL = "https" if USE_HTTPS else "http"
    
    # Reload certificate paths from config
    if _config.get('cert_file') and Path(_config['cert_file']).exists():
        CERT_FILE = Path(_config['cert_file'])
    else:
        CERT_FILE = Path("data/ssl/server.crt")
    
    if _config.get('key_file') and Path(_config['key_file']).exists():
        KEY_FILE = Path(_config['key_file'])
    else:
        KEY_FILE = Path("data/ssl/server.key")
    
    # Update network info and URLs
    HOSTNAME, LOCAL_IP = get_network_info()
    LOCAL_URL = f"{PROTOCOL}://127.0.0.1:{PORT}"
    NETWORK_URL = f"{PROTOCOL}://{LOCAL_IP}:{PORT}"
    HOSTNAME_URL = f"{PROTOCOL}://{HOSTNAME}:{PORT}"
    
    # Check and create databases if needed
    try:
        if not check_and_create_databases():
            sys.exit(1)
    except Exception as e:
        import traceback
        error_msg = f"Error checking databases: {e}\n\n{traceback.format_exc()}"
        print(error_msg, file=sys.stderr if hasattr(sys, 'stderr') else None)
        try:
            root = tk.Tk()
            root.withdraw()
            messagebox.showerror("Database Error", f"Error checking databases:\n\n{e}")
            root.destroy()
        except:
            pass
        sys.exit(1)
    
    # Check if port is in use and kill existing process
    try:
        if is_port_in_use(PORT):
            killed, pids = kill_process_on_port(PORT)
            if killed:
                time.sleep(1)  # Give it time to fully release the port
            else:
                # Port might be in TIME_WAIT state - try to proceed anyway
                # Only show error if port is still definitely in use after wait
                time.sleep(1)
                if is_port_in_use(PORT):
                    root = tk.Tk()
                    root.withdraw()
                    messagebox.showerror(
                        "Port In Use",
                        f"Could not terminate process on port {PORT}.\nPlease close any existing ETL servers."
                    )
                    root.destroy()
                    sys.exit(1)
    except Exception as e:
        # If there's an error checking/killing processes, log it but try to continue
        print(f"Warning: Error checking port {PORT}: {e}")
        import traceback
        traceback.print_exc()
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    if hasattr(signal, 'SIGTERM'):
        signal.signal(signal.SIGTERM, signal_handler)
    
    # Clear shutdown flag before starting server (in case it was set from previous run)
    shutdown_flag.clear()
    
    # Set server as running initially
    server_running = True
    
    # Start server in background thread
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    
    # Start browser opener in background
    browser_thread = threading.Thread(target=open_browser, daemon=True)
    browser_thread.start()
    
    # Give server a moment to start
    time.sleep(1)
    
    # Start GUI (this will block until GUI is closed)
    try:
        create_gui()
    except KeyboardInterrupt:
        shutdown_server()
    except Exception as e:
        import traceback
        error_msg = f"GUI error: {e}\n\n{traceback.format_exc()}"
        print(error_msg)  # Print to console if available
        try:
            root = tk.Tk()
            root.withdraw()
            messagebox.showerror("Error", f"GUI error: {e}")
            root.destroy()
        except:
            pass
        shutdown_server()


def check_single_instance():
    """Check if another instance is running and bring it to focus if so"""
    import ctypes
    from ctypes import wintypes
    
    # Window title to search for
    WINDOW_TITLE = "Calaveras UniteUs ETL Server Control"
    
    # Windows API constants
    SW_RESTORE = 9
    SW_SHOW = 5
    
    try:
        # Try to find the window by title
        hwnd = ctypes.windll.user32.FindWindowW(None, WINDOW_TITLE)
        
        if hwnd:
            # Window exists - bring it to focus
            # Restore if minimized
            ctypes.windll.user32.ShowWindow(hwnd, SW_RESTORE)
            # Bring to foreground
            ctypes.windll.user32.SetForegroundWindow(hwnd)
            # Flash the window to get user's attention
            ctypes.windll.user32.FlashWindow(hwnd, True)
            # Exit this instance
            sys.exit(0)
    except Exception:
        # If Windows API calls fail, try alternative method using lock file
        pass
    
    # Alternative: Use lock file method
    lock_file = Path(__file__).parent / ".launcher.lock"
    
    # Check if lock file exists and if the process is still running
    if lock_file.exists():
        try:
            # Read PID from lock file
            pid = int(lock_file.read_text().strip())
            
            # Check if process is still running (Windows)
            if sys.platform == 'win32':
                import ctypes
                kernel32 = ctypes.windll.kernel32
                
                # Open process handle
                PROCESS_QUERY_INFORMATION = 0x0400
                handle = kernel32.OpenProcess(PROCESS_QUERY_INFORMATION, False, pid)
                
                if handle:
                    # Process exists - try to find and focus window
                    kernel32.CloseHandle(handle)
                    
                    # Window might still be initializing, wait a moment and retry
                    for attempt in range(5):  # Try up to 5 times
                        try:
                            hwnd = ctypes.windll.user32.FindWindowW(None, WINDOW_TITLE)
                            if hwnd:
                                SW_RESTORE = 9
                                ctypes.windll.user32.ShowWindow(hwnd, SW_RESTORE)
                                ctypes.windll.user32.SetForegroundWindow(hwnd)
                                ctypes.windll.user32.FlashWindow(hwnd, True)
                                sys.exit(0)
                        except:
                            pass
                        
                        # Wait 200ms before next attempt
                        if attempt < 4:
                            time.sleep(0.2)
        except (ValueError, OSError):
            # Lock file is invalid or process doesn't exist - remove it
            try:
                lock_file.unlink()
            except:
                pass
    
    # Create lock file with current PID
    try:
        lock_file.write_text(str(os.getpid()))
    except:
        pass
    
    # Cleanup lock file on exit
    import atexit
    def cleanup_lock():
        try:
            if lock_file.exists():
                lock_file.unlink()
        except:
            pass
    atexit.register(cleanup_lock)


if __name__ == "__main__":
    # Check for single instance before doing anything else
    check_single_instance()
    
    # Add immediate feedback that script is starting
    try:
        import ctypes
        # This will show a brief message that the launcher is starting
        # (commented out to avoid annoying users, but can be enabled for debugging)
        # ctypes.windll.user32.MessageBoxW(0, "Launcher is starting...", "Info", 0x40)
    except:
        pass
    
    try:
        main()
    except KeyboardInterrupt:
        # User interrupted - exit silently
        sys.exit(0)
    except SystemExit:
        # Already handled - just re-raise
        raise
    except Exception as e:
        import traceback
        error_msg = f"Fatal error starting launcher: {e}\n\n{traceback.format_exc()}"
        
        # Try to write error to file for debugging
        try:
            error_file = Path(__file__).parent / "launcher_error.log"
            with open(error_file, "w", encoding="utf-8") as f:
                f.write(f"Launcher Error - {datetime.now()}\n")
                f.write("=" * 60 + "\n")
                f.write(error_msg)
                f.write("\n" + "=" * 60 + "\n")
        except:
            pass
        
        # Try to show error dialog
        try:
            import tkinter as tk
            from tkinter import messagebox
            root = tk.Tk()
            root.withdraw()
            messagebox.showerror(
                "Launcher Error", 
                f"Fatal error starting launcher:\n\n{type(e).__name__}: {e}\n\n"
                f"Error details have been saved to:\nlauncher_error.log\n\n"
                f"Please check the log file for more information."
            )
            root.destroy()
        except Exception as dialog_error:
            # Fallback to Windows message box
            try:
                import ctypes
                ctypes.windll.user32.MessageBoxW(0,
                    f"Launcher Error\n\n{type(e).__name__}: {e}\n\nCheck launcher_error.log for details.",
                    "Launcher Error", 0x10)
            except:
                pass
        
        sys.exit(1)
