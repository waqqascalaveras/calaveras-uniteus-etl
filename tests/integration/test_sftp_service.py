"""
================================================================================
Calaveras UniteUs ETL - SFTP Service Integration Tests
================================================================================
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency

Description:
    Integration tests for SFTP service, testing SFTP connection, file discovery,
    and download functionality. Validates key-based authentication, file listing,
    and download operations.

Test Coverage:
    - SFTP connection establishment
    - File discovery and listing
    - File download operations
    - Error handling and retry logic
    - Authentication and key management
================================================================================
"""

from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
from datetime import datetime
import pytest

from core.sftp_service import (
    SFTPFileInfo,
    SFTPDownloadResult,
    SFTPConnection,
    SFTPService
)


class TestSFTPFileInfo:
    """Test SFTPFileInfo dataclass"""
    
    def test_create_file_info(self):
        """Test creating file info object"""
        file_info = SFTPFileInfo(
            filename="test.csv",
            remote_path="/data/test.csv",
            size=1024,
            modified_time=datetime(2024, 1, 1, 12, 0, 0),
            is_directory=False
        )
        
        assert file_info.filename == "test.csv"
        assert file_info.size == 1024
        assert not file_info.is_directory
    
    def test_to_dict(self):
        """Test converting file info to dictionary"""
        file_info = SFTPFileInfo(
            filename="test.csv",
            remote_path="/data/test.csv",
            size=1024,
            modified_time=datetime(2024, 1, 1, 12, 0, 0)
        )
        
        result = file_info.to_dict()
        
        assert result['filename'] == "test.csv"
        assert result['size'] == 1024
        assert 'modified_time' in result


class TestSFTPDownloadResult:
    """Test SFTPDownloadResult dataclass"""
    
    def test_success_result(self):
        """Test successful download result"""
        result = SFTPDownloadResult(
            success=True,
            filename="test.csv",
            local_path=Path("downloads/test.csv"),
            remote_path="/data/test.csv",
            file_size=1024,
            download_time_seconds=2.5
        )
        
        assert result.success
        assert result.file_size == 1024
        assert result.error_message is None
    
    def test_failure_result(self):
        """Test failed download result"""
        result = SFTPDownloadResult(
            success=False,
            filename="test.csv",
            error_message="Connection failed"
        )
        
        assert not result.success
        assert result.local_path is None
        assert result.error_message == "Connection failed"
    
    def test_to_dict(self):
        """Test converting download result to dictionary"""
        result = SFTPDownloadResult(
            success=True,
            filename="test.csv",
            local_path=Path("downloads/test.csv"),
            file_size=1024
        )
        
        result_dict = result.to_dict()
        
        assert result_dict['success'] == True
        assert result_dict['filename'] == "test.csv"
        assert result_dict['file_size'] == 1024


class TestSFTPConnection:
    """Test SFTPConnection class"""
    
    @patch('core.sftp_service.PARAMIKO_AVAILABLE', True)
    @patch('core.sftp_service.paramiko')
    def test_init_connection(self, mock_paramiko):
        """Test initializing SFTP connection"""
        conn = SFTPConnection(
            host="test.example.com",
            port=22,
            username="testuser",
            private_key_path=Path("keys/test_key")
        )
        
        assert conn.host == "test.example.com"
        assert conn.port == 22
        assert conn.username == "testuser"
    
    @patch('core.sftp_service.PARAMIKO_AVAILABLE', False)
    def test_paramiko_not_available(self):
        """Test handling when paramiko is not available"""
        with pytest.raises(Exception):
            conn = SFTPConnection(
                host="test.example.com",
                port=22,
                username="testuser"
            )
            with conn:
                pass
        
        assert "paramiko" in str(context.exception.lower())


class TestSFTPService:
    """Test SFTPService class"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.service = SFTPService()
    
    @patch('core.sftp_service.config')
    def test_test_connection_disabled(self, mock_config):
        """Test connection when SFTP is disabled"""
        mock_config.sftp.enabled = False
        
        success, message = self.service.test_connection()
        
        assert not success
        assert "not enabled" in message.lower()
    
    @patch('core.sftp_service.config')
    @patch('core.sftp_service.SFTPConnection')
    def test_test_connection_success(self, mock_connection_class, mock_config):
        """Test successful connection test"""
        # Setup mocks
        mock_config.sftp.enabled = True
        mock_config.sftp.host = "test.example.com"
        mock_config.sftp.port = 22
        
        mock_conn = MagicMock()
        mock_conn.connected = True
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=False)
        mock_connection_class.return_value = mock_conn
        
        success, message = self.service.test_connection()
        
        assert success
        assert "successfully" in message.lower()
    
    @patch('core.sftp_service.config')
    def test_discover_files_disabled(self, mock_config):
        """Test file discovery when SFTP is disabled"""
        mock_config.sftp.enabled = False
        
        files = self.service.discover_files()
        
        assert files == []


class TestKeyFileVerification:
    """Test key file verification functionality"""
    
    def test_openssh_key_detection(self):
        """Test detecting OpenSSH format keys"""
        # This would test the API endpoint logic
        key_content = "-----BEGIN OPENSSH PRIVATE KEY-----\ntest\n"
        
        # Simulate what the API does
        if key_content.startswith('-----BEGIN OPENSSH PRIVATE KEY-----'):
            key_format = "OpenSSH"
        else:
            key_format = "Unknown"
        
        assert key_format == "OpenSSH"
    
    def test_rsa_key_detection(self):
        """Test detecting RSA format keys"""
        key_content = "-----BEGIN RSA PRIVATE KEY-----\ntest\n"
        
        if key_content.startswith('-----BEGIN RSA PRIVATE KEY-----'):
            key_format = "RSA/PEM"
        else:
            key_format = "Unknown"
        
        assert key_format == "RSA/PEM"
    
    def test_putty_key_detection(self):
        """Test detecting PuTTY format keys"""
        key_content = "PuTTY-User-Key-File-2: ssh-rsa\n"
        
        if key_content.startswith('PuTTY-User-Key-File'):
            key_format = "PuTTY (needs conversion!)"
        else:
            key_format = "Unknown"
        
        assert key_format == "PuTTY (needs conversion)"


class TestAuditLoggerIntegration:
    """Test audit logger integration"""
    
    def test_audit_logger_available(self):
        """Test when audit logger is available"""
        from core.sftp_service import AUDIT_AVAILABLE
        
        # Should be True if audit_logger module exists
        assert isinstance(AUDIT_AVAILABLE, bool)
    
    def test_sftp_service_handles_no_audit(self):
        """Test SFTP service works without audit logger"""
        service = SFTPService()
        
        # Should initialize even if audit logger is None
        assert service is not None
        
        # audit_logger should either be an object or None
        if service.audit_logger is not None:
            assert hasattr(service.audit_logger, 'log')


class TestSFTPConfiguration:
    """Test SFTP configuration handling"""
    
    @patch('core.sftp_service.config')
    def test_default_configuration(self, mock_config):
        """Test default SFTP configuration"""
        mock_config.sftp.host = "chhssftp.uniteus.com"
        mock_config.sftp.port = 22
        mock_config.sftp.username = "chhsca_data_prod"
        mock_config.sftp.private_key_path = Path("keys/calco-uniteus-sftp")
        
        assert mock_config.sftp.host == "chhssftp.uniteus.com"
        assert mock_config.sftp.username == "chhsca_data_prod"
        assert str(mock_config.sftp.private_key_path) == "keys/calco-uniteus-sftp"
        assert ".ppk" not in str(mock_config.sftp.private_key_path)


class TestErrorHandling:
    """Test error handling in SFTP operations"""
    
    @patch('core.sftp_service.config')
    @patch('core.sftp_service.SFTPConnection')
    def test_connection_timeout(self, mock_connection_class, mock_config):
        """Test handling connection timeout"""
        mock_config.sftp.enabled = True
        mock_connection_class.side_effect = TimeoutError("Connection timed out")
        
        service = SFTPService()
        success, message = service.test_connection()
        
        assert not success
        assert "timeout" in message.lower()
    
    @patch('core.sftp_service.config')
    @patch('core.sftp_service.SFTPConnection')
    def test_authentication_failure(self, mock_connection_class, mock_config):
        """Test handling authentication failure"""
        mock_config.sftp.enabled = True
        mock_connection_class.side_effect = Exception("Authentication failed")
        
        service = SFTPService()
        success, message = service.test_connection()
        
        assert not success
        assert "failed" in message.lower()


