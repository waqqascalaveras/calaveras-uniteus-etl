"""
SFTP File Download Service

SFTP service for automated file download from remote servers using key-based
authentication. Integrates with the ETL pipeline to automatically download
data files before processing.

Author: Waqqas Hanafi
Copyright: © 2025 Calaveras County Health and Human Services Agency
"""

import logging
import time
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import hashlib

try:
    import paramiko
    PARAMIKO_AVAILABLE = True
except ImportError:
    PARAMIKO_AVAILABLE = False
    logging.warning("paramiko not installed - SFTP functionality disabled")

from .config import config

# Lazy import to avoid circular dependencies
try:
    from .audit_logger import get_audit_logger, AuditCategory, AuditAction
    AUDIT_AVAILABLE = True
except ImportError as e:
    AUDIT_AVAILABLE = False
    logging.warning(f"Audit logger not available: {e}")
    # Create dummy classes for when audit isn't available
    class AuditCategory:
        SYSTEM = "system"
        ETL = "etl"
    class AuditAction:
        CONFIGURATION_CHANGE = "config_change"
        FILE_DOWNLOADED = "file_downloaded"
        FILE_FAILED = "file_failed"


@dataclass
class SFTPFileInfo:
    """Information about a remote SFTP file"""
    filename: str
    remote_path: str
    size: int
    modified_time: datetime
    is_directory: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'filename': self.filename,
            'remote_path': self.remote_path,
            'size': self.size,
            'modified_time': self.modified_time.isoformat(),
            'is_directory': self.is_directory
        }


@dataclass
class SFTPDownloadResult:
    """Result of an SFTP download operation"""
    success: bool
    filename: str
    local_path: Optional[Path] = None
    remote_path: Optional[str] = None
    file_size: int = 0
    download_time_seconds: float = 0.0
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'success': self.success,
            'filename': self.filename,
            'local_path': str(self.local_path) if self.local_path else None,
            'remote_path': self.remote_path,
            'file_size': self.file_size,
            'download_time_seconds': self.download_time_seconds,
            'error_message': self.error_message
        }


class SFTPConnection:
    """Manages SFTP connection with authentication and error handling"""
    
    def __init__(self, host: str, port: int, username: str,
                 private_key_path: Optional[Path] = None,
                 private_key_passphrase: Optional[str] = None,
                 password: Optional[str] = None,
                 known_hosts_path: Optional[Path] = None,
                 verify_host_key: bool = True,
                 timeout: int = 30):
        
        self.host = host
        self.port = port
        self.username = username
        self.private_key_path = private_key_path
        self.private_key_passphrase = private_key_passphrase
        self.password = password
        self.known_hosts_path = known_hosts_path
        self.verify_host_key = verify_host_key
        self.timeout = timeout
        
        self.logger = logging.getLogger(self.__class__.__name__)
        self.ssh_client: Optional[paramiko.SSHClient] = None
        self.sftp_client: Optional[paramiko.SFTPClient] = None
        self.connected = False
    
    def connect(self) -> bool:
        """Establish SFTP connection"""
        if not PARAMIKO_AVAILABLE:
            self.logger.error("paramiko not available - cannot establish SFTP connection")
            return False
        
        try:
            self.logger.info(f"Connecting to SFTP server {self.host}:{self.port} as {self.username}")
            
            # Create SSH client
            self.ssh_client = paramiko.SSHClient()
            
            # Load and handle known hosts
            if self.known_hosts_path:
                # Ensure directory exists
                self.known_hosts_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Create empty known_hosts if it doesn't exist
                if not self.known_hosts_path.exists():
                    self.known_hosts_path.touch()
                    self.logger.info(f"Created new known_hosts file: {self.known_hosts_path}")
                
                # Load existing hosts
                self.ssh_client.load_host_keys(str(self.known_hosts_path))
                self.logger.info(f"Loaded known hosts from {self.known_hosts_path}")
            
            # Auto-accept and save new host keys (secure for first connection)
            # Future connections will verify against the saved key
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            if self.verify_host_key:
                self.logger.info("Host key verification enabled - will auto-accept and save new hosts")
            else:
                self.logger.warning("Host key verification disabled")
                self.logger.warning("Host key verification disabled - connection may be insecure")
            
            # Prepare authentication
            connect_kwargs = {
                'hostname': self.host,
                'port': self.port,
                'username': self.username,
                'timeout': self.timeout,
                'look_for_keys': False,  # Don't auto-load keys from .ssh
                'allow_agent': False  # Don't use SSH agent
            }
            
            # Use private key if provided
            if self.private_key_path and self.private_key_path.exists():
                self.logger.info(f"Using private key: {self.private_key_path}")
                
                # Detect key format
                key_file_lower = str(self.private_key_path).lower()
                is_putty_key = key_file_lower.endswith('.ppk')
                
                # Try different key types and formats
                key_loaded = False
                
                # For PuTTY keys, try PKey.from_private_key_file which can handle various formats
                if is_putty_key:
                    self.logger.info("Detected PuTTY format key (.ppk)")
                    try:
                        # Try to read the file to detect format
                        with open(self.private_key_path, 'r') as f:
                            first_line = f.readline().strip()
                        
                        # Check if it's a PuTTY v3 key (which paramiko might support)
                        if first_line.startswith('PuTTY-User-Key-File-3'):
                            self.logger.info("Detected PuTTY v3 format - attempting to load")
                            # Try each key type with PuTTY v3 format
                            # Build list dynamically to handle deprecated key types
                            putty_key_types = [paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey]
                            if hasattr(paramiko, 'DSSKey'):
                                putty_key_types.append(paramiko.DSSKey)
                            
                            for key_class in putty_key_types:
                                try:
                                    private_key = key_class.from_private_key_file(
                                        str(self.private_key_path),
                                        password=self.private_key_passphrase
                                    )
                                    connect_kwargs['pkey'] = private_key
                                    key_loaded = True
                                    self.logger.info(f"Loaded PuTTY key successfully")
                                    break
                                except:
                                    continue
                        
                        if not key_loaded:
                            # PuTTY v2 or v1 format - needs conversion
                            raise Exception(
                                f"PuTTY format key detected (.ppk). Please convert to OpenSSH format:\n"
                                f"  1. Open PuTTYgen\n"
                                f"  2. Load your .ppk file\n"
                                f"  3. Go to: Conversions → Export OpenSSH key\n"
                                f"  4. Save without .ppk extension\n"
                                f"  5. Update the key path in settings\n"
                                f"Or use command: puttygen {self.private_key_path} -O private-openssh -o keys/calco-uniteus-sftp"
                            )
                    except FileNotFoundError:
                        raise Exception(f"Private key file not found: {self.private_key_path}")
                    except Exception as e:
                        if "convert" in str(e):
                            raise  # Re-raise our conversion message
                        self.logger.error(f"Failed to load PuTTY key: {e}")
                        raise Exception(f"Could not load PuTTY key. Please convert to OpenSSH format. Error: {e}")
                
                # Try standard formats (OpenSSH, PEM, SSH2)
                if not key_loaded:
                    # Build list of supported key types
                    # Note: DSS/DSA deprecated in paramiko 3.4.0+ (weak security)
                    key_types = [
                        (paramiko.RSAKey, "RSA"),
                        (paramiko.Ed25519Key, "Ed25519"),
                        (paramiko.ECDSAKey, "ECDSA")
                    ]
                    
                    # Add DSS support if available (older paramiko versions)
                    if hasattr(paramiko, 'DSSKey'):
                        key_types.append((paramiko.DSSKey, "DSS"))
                    
                    for key_class, key_name in key_types:
                        try:
                            private_key = key_class.from_private_key_file(
                                str(self.private_key_path),
                                password=self.private_key_passphrase
                            )
                            connect_kwargs['pkey'] = private_key
                            key_loaded = True
                            self.logger.info(f"Loaded {key_name} private key successfully")
                            break
                        except paramiko.SSHException as e:
                            self.logger.debug(f"Not a {key_name} key: {e}")
                            continue
                        except Exception as e:
                            self.logger.debug(f"Failed to load as {key_name} key: {e}")
                            continue
                
                if not key_loaded:
                    raise Exception(
                        f"Could not load private key from {self.private_key_path}.\n"
                        f"Supported formats: OpenSSH, PEM, SSH2/RFC 4716.\n"
                        f"If you have a PuTTY .ppk file, please convert it to OpenSSH format."
                    )
            
            # Fallback to password authentication
            elif self.password:
                self.logger.info("Using password authentication")
                connect_kwargs['password'] = self.password
            else:
                raise Exception("No authentication method provided (need private_key_path or password)")
            
            # Connect
            self.ssh_client.connect(**connect_kwargs)
            
            # Open SFTP session
            self.sftp_client = self.ssh_client.open_sftp()
            self.connected = True
            
            # Save host key to known_hosts after successful connection
            if self.known_hosts_path:
                try:
                    self.ssh_client.save_host_keys(str(self.known_hosts_path))
                    self.logger.info(f"Saved host key to {self.known_hosts_path}")
                except Exception as e:
                    self.logger.debug(f"Could not save host key: {e}")
            
            self.logger.info(f"Successfully connected to {self.host}:{self.port}")
            return True
            
        except paramiko.AuthenticationException as e:
            self.logger.error(f"Authentication failed: {e}")
            return False
        except paramiko.SSHException as e:
            self.logger.error(f"SSH error: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Connection error: {e}", exc_info=True)
            return False
    
    def disconnect(self):
        """Close SFTP connection"""
        try:
            if self.sftp_client:
                self.sftp_client.close()
                self.sftp_client = None
            
            if self.ssh_client:
                self.ssh_client.close()
                self.ssh_client = None
            
            self.connected = False
            self.logger.info(f"Disconnected from {self.host}")
        except Exception as e:
            self.logger.error(f"Error during disconnect: {e}")
    
    def list_files(self, remote_directory: str, patterns: List[str] = None) -> List[SFTPFileInfo]:
        """List files in remote directory matching patterns"""
        if not self.connected or not self.sftp_client:
            raise Exception("Not connected to SFTP server")
        
        try:
            files = []
            
            # List directory
            for entry in self.sftp_client.listdir_attr(remote_directory):
                # Build full remote path
                remote_path = f"{remote_directory}/{entry.filename}".replace("//", "/")
                
                # Get file info
                is_dir = self._is_directory(entry)
                
                # Skip directories
                if is_dir:
                    continue
                
                # Check patterns
                if patterns:
                    import fnmatch
                    if not any(fnmatch.fnmatch(entry.filename, pattern) for pattern in patterns):
                        continue
                
                # Get modified time
                modified_time = datetime.fromtimestamp(entry.st_mtime)
                
                file_info = SFTPFileInfo(
                    filename=entry.filename,
                    remote_path=remote_path,
                    size=entry.st_size,
                    modified_time=modified_time,
                    is_directory=is_dir
                )
                
                files.append(file_info)
            
            self.logger.info(f"Found {len(files)} file(s) in {remote_directory}")
            return files
            
        except Exception as e:
            self.logger.error(f"Error listing files in {remote_directory}: {e}")
            raise
    
    def download_file(self, remote_path: str, local_path: Path, 
                     progress_callback=None) -> Tuple[bool, Optional[str]]:
        """
        Download file from SFTP server
        
        Returns:
            (success, error_message)
        """
        if not self.connected or not self.sftp_client:
            return False, "Not connected to SFTP server"
        
        try:
            # Ensure local directory exists
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Download with progress callback
            if progress_callback:
                self.sftp_client.get(remote_path, str(local_path), callback=progress_callback)
            else:
                self.sftp_client.get(remote_path, str(local_path))
            
            self.logger.info(f"Downloaded {remote_path} to {local_path}")
            return True, None
            
        except Exception as e:
            error_msg = f"Failed to download {remote_path}: {e}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def delete_file(self, remote_path: str) -> Tuple[bool, Optional[str]]:
        """Delete file from SFTP server"""
        if not self.connected or not self.sftp_client:
            return False, "Not connected to SFTP server"
        
        try:
            self.sftp_client.remove(remote_path)
            self.logger.info(f"Deleted remote file: {remote_path}")
            return True, None
        except Exception as e:
            error_msg = f"Failed to delete {remote_path}: {e}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def _is_directory(self, entry) -> bool:
        """Check if entry is a directory"""
        import stat
        return stat.S_ISDIR(entry.st_mode)
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()


class SFTPService:
    """
    High-level SFTP service for automated file downloads
    Integrates with ETL pipeline
    """
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        if AUDIT_AVAILABLE:
            self.audit_logger = get_audit_logger()
        else:
            self.audit_logger = None
    
    def test_connection(self, username: str = "system") -> Tuple[bool, str]:
        """
        Test SFTP connection with current configuration
        
        Returns:
            (success, message)
        """
        if not config.sftp.enabled:
            return False, "SFTP is not enabled in configuration"
        
        if not config.sftp.host:
            return False, "SFTP host not configured"
        
        if not PARAMIKO_AVAILABLE:
            return False, "paramiko library not installed. Install with: pip install paramiko>=3.4.0"
        
        try:
            # Determine which authentication method to use
            use_key_auth = config.sftp.auth_method == "key"
            
            # Only use key path if key authentication is explicitly selected
            if use_key_auth:
                key_path_to_use = config.sftp.private_key_path if config.sftp.private_key_path else None
                key_passphrase_to_use = config.sftp.private_key_passphrase
                password_to_use = None
            else:
                # Password authentication - explicitly set key to None to prevent accidental key usage
                key_path_to_use = None
                key_passphrase_to_use = None
                password_to_use = config.sftp.password
            
            with SFTPConnection(
                host=config.sftp.host,
                port=config.sftp.port,
                username=config.sftp.username,
                private_key_path=key_path_to_use,
                private_key_passphrase=key_passphrase_to_use,
                password=password_to_use,
                known_hosts_path=config.sftp.known_hosts_path,
                verify_host_key=config.sftp.verify_host_key,
                timeout=config.sftp.timeout_seconds
            ) as conn:
                if conn.connected:
                    # Build detailed success message
                    auth_method_str = "SSH key" if use_key_auth else "Password"
                    message_parts = [
                        f"Successfully connected to SFTP server",
                        f"Host: {config.sftp.host}",
                        f"Port: {config.sftp.port}",
                        f"Username: {config.sftp.username}",
                        f"Authentication: {auth_method_str}"
                    ]
                    # Only show key file if key authentication was explicitly used
                    if use_key_auth and key_path_to_use:
                        message_parts.append(f"Key file: {key_path_to_use}")
                    message = " | ".join(message_parts)
                    
                    # Audit log
                    if self.audit_logger:
                        self.audit_logger.log(
                            username=username,
                            action=AuditAction.CONFIGURATION_CHANGE,
                            category=AuditCategory.SYSTEM,
                            success=True,
                            details=f"SFTP connection test successful to {config.sftp.host}",
                            target_resource="sftp_connection"
                        )
                    return True, message
                else:
                    return False, "Connection failed (check logs for details)"
        
        except Exception as e:
            if self.audit_logger:
                self.audit_logger.log(
                    username=username,
                    action=AuditAction.CONFIGURATION_CHANGE,
                    category=AuditCategory.SYSTEM,
                    success=False,
                    details=f"SFTP connection test failed",
                    target_resource="sftp_connection",
                    error_message=str(e)
                )
            return False, f"Connection failed: {str(e)}"
    
    def discover_files(self, username: str = "system") -> List[SFTPFileInfo]:
        """Discover available files on SFTP server"""
        if not config.sftp.enabled:
            self.logger.warning("SFTP not enabled")
            return []
        
        try:
            with SFTPConnection(
                host=config.sftp.host,
                port=config.sftp.port,
                username=config.sftp.username,
                private_key_path=config.sftp.private_key_path if config.sftp.auth_method == "key" else None,
                private_key_passphrase=config.sftp.private_key_passphrase,
                password=config.sftp.password if config.sftp.auth_method == "password" else None,
                known_hosts_path=config.sftp.known_hosts_path,
                verify_host_key=config.sftp.verify_host_key,
                timeout=config.sftp.timeout_seconds
            ) as conn:
                files = conn.list_files(
                    config.sftp.remote_directory,
                    config.sftp.file_patterns
                )
                
                return files
        
        except Exception as e:
            self.logger.error(f"Failed to discover files: {e}")
            return []
    
    def download_files(self, files: List[SFTPFileInfo] = None, username: str = "system") -> List[SFTPDownloadResult]:
        """
        Download files from SFTP server
        
        Args:
            files: Specific files to download (or None to discover all)
            username: User initiating download
        
        Returns:
            List of download results
        """
        if not config.sftp.enabled:
            self.logger.warning("SFTP not enabled")
            return []
        
        # Discover files if not provided
        if files is None:
            files = self.discover_files(username)
        
        if not files:
            self.logger.info("No files to download")
            return []
        
        results = []
        
        try:
            with SFTPConnection(
                host=config.sftp.host,
                port=config.sftp.port,
                username=config.sftp.username,
                private_key_path=config.sftp.private_key_path if config.sftp.auth_method == "key" else None,
                private_key_passphrase=config.sftp.private_key_passphrase,
                password=config.sftp.password if config.sftp.auth_method == "password" else None,
                known_hosts_path=config.sftp.known_hosts_path,
                verify_host_key=config.sftp.verify_host_key,
                timeout=config.sftp.timeout_seconds
            ) as conn:
                # First discover files if we just have strings
                if files and isinstance(files[0], str):
                    # We have filenames, need to discover full file info
                    all_files = conn.list_files(config.sftp.remote_directory, config.sftp.file_patterns)
                    # Filter to only the requested files
                    files = [f for f in all_files if f.filename in files]
                
                for file_info in files:
                    start_time = time.time()
                    local_path = config.sftp.local_download_path / file_info.filename
                    
                    self.logger.info(f"Downloading {file_info.filename} ({file_info.size} bytes)")
                    
                    success, error_msg = conn.download_file(file_info.remote_path, local_path)
                    download_time = time.time() - start_time
                    
                    result = SFTPDownloadResult(
                        success=success,
                        filename=file_info.filename,
                        local_path=local_path if success else None,
                        remote_path=file_info.remote_path,
                        file_size=file_info.size,
                        download_time_seconds=download_time,
                        error_message=error_msg
                    )
                    
                    results.append(result)
                    
                    # Audit log
                    if self.audit_logger:
                        self.audit_logger.log(
                            username=username,
                            action=AuditAction.FILE_DOWNLOADED if success else AuditAction.FILE_FAILED,
                            category=AuditCategory.ETL,
                            success=success,
                            details=f"SFTP download from {config.sftp.host}",
                            target_resource=file_info.filename,
                            duration_ms=int(download_time * 1000),
                            file_size=file_info.size,
                            error_message=error_msg
                        )
                    
                    # Delete from server if configured
                    if success and config.sftp.delete_after_download:
                        delete_success, delete_error = conn.delete_file(file_info.remote_path)
                        if delete_success:
                            self.logger.info(f"Deleted {file_info.filename} from server")
                        else:
                            self.logger.warning(f"Failed to delete {file_info.filename}: {delete_error}")
        
        except Exception as e:
            self.logger.error(f"Error during file downloads: {e}")
        
        return results
    
    def download_and_process(self, username: str = "system") -> Dict[str, Any]:
        """
        Download files from SFTP and trigger ETL processing
        
        Returns:
            Dictionary with download results and ETL job ID
        """
        self.logger.info("Starting SFTP download and ETL process")
        
        # Download files
        results = self.download_files(username=username)
        
        success_count = sum(1 for r in results if r.success)
        failed_count = len(results) - success_count
        
        self.logger.info(f"Downloaded {success_count}/{len(results)} files successfully")
        
        return {
            'total_files': len(results),
            'successful_downloads': success_count,
            'failed_downloads': failed_count,
            'results': [r.to_dict() for r in results]
        }


# Global service instance
_sftp_service: Optional[SFTPService] = None


def get_sftp_service() -> SFTPService:
    """Get the global SFTP service instance"""
    global _sftp_service
    if _sftp_service is None:
        _sftp_service = SFTPService()
    return _sftp_service

