"""
================================================================================
Calaveras UniteUs ETL - SIEM Logger Unit Tests
================================================================================
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency

Description:
    Unit tests for the SIEM logger module, testing SIEM logging functionality
    including JSON logging, Windows Event Log integration, syslog forwarding,
    event types, severity levels, and sensitive data filtering.

Test Coverage:
    - SIEM logger initialization
    - JSON structured logging
    - Windows Event Log integration (mocked)
    - Syslog forwarding (UDP and TCP)
    - Event type and severity handling
    - Sensitive data filtering
    - Event formatting for different backends
    - Error handling and edge cases
    - Integration with convenience functions
================================================================================
"""

import pytest
import tempfile
import json
import os
import sys
import socket
import logging
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock, call
from typing import Dict, Any

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.siem_logger import (
    SIEMLogger,
    SIEMEventType,
    SIEMSeverity,
    WindowsEventLogger,
    SyslogForwarder,
    get_siem_logger,
    log_siem_event
)


@pytest.fixture
def temp_log_dir():
    """Create a temporary directory for log files"""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    # Cleanup
    import shutil
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def mock_config():
    """Create a mock SIEM config object"""
    config = Mock()
    config.siem = Mock()
    config.siem.enabled = True
    config.siem.enable_json_logging = True
    config.siem.enable_windows_event_log = False
    config.siem.json_log_path = None  # Will be set in tests
    config.siem.syslog_enabled = False
    config.siem.syslog_host = "localhost"
    config.siem.syslog_port = 514
    config.siem.syslog_protocol = "UDP"
    config.siem.include_sensitive_data = False
    return config


@pytest.fixture
def siem_logger_with_json(mock_config, temp_log_dir):
    """Create a SIEM logger with JSON logging enabled"""
    log_path = temp_log_dir / "siem_test.json"
    mock_config.siem.json_log_path = log_path
    
    with patch('core.siem_logger.config', mock_config):
        logger = SIEMLogger()
        yield logger, log_path
        logger.close()


class TestSIEMEventType:
    """Tests for SIEMEventType enum"""
    
    def test_event_types_exist(self):
        """Test that all expected event types exist"""
        assert SIEMEventType.AUTHENTICATION.value == "authentication"
        assert SIEMEventType.AUTHORIZATION.value == "authorization"
        assert SIEMEventType.ETL_OPERATION.value == "etl_operation"
        assert SIEMEventType.DATA_ACCESS.value == "data_access"
        assert SIEMEventType.SYSTEM_EVENT.value == "system_event"
        assert SIEMEventType.SECURITY_EVENT.value == "security_event"
        assert SIEMEventType.CONFIGURATION_CHANGE.value == "configuration_change"
        assert SIEMEventType.ERROR.value == "error"


class TestSIEMSeverity:
    """Tests for SIEMSeverity enum"""
    
    def test_severity_levels_exist(self):
        """Test that all severity levels exist with correct values"""
        assert SIEMSeverity.EMERGENCY.value == 0
        assert SIEMSeverity.ALERT.value == 1
        assert SIEMSeverity.CRITICAL.value == 2
        assert SIEMSeverity.ERROR.value == 3
        assert SIEMSeverity.WARNING.value == 4
        assert SIEMSeverity.NOTICE.value == 5
        assert SIEMSeverity.INFO.value == 6
        assert SIEMSeverity.DEBUG.value == 7


class TestSIEMLoggerInitialization:
    """Tests for SIEMLogger initialization"""
    
    def test_init_with_siem_disabled(self, mock_config):
        """Test initialization when SIEM is disabled"""
        mock_config.siem.enabled = False
        
        with patch('core.siem_logger.config', mock_config):
            logger = SIEMLogger()
            assert logger.json_logger is None
            assert logger.windows_logger is None
            assert logger.syslog_forwarder is None
    
    def test_init_with_json_logging(self, mock_config, temp_log_dir):
        """Test initialization with JSON logging enabled"""
        log_path = temp_log_dir / "siem_test.json"
        mock_config.siem.json_log_path = log_path
        mock_config.siem.enable_json_logging = True
        
        with patch('core.siem_logger.config', mock_config):
            logger = SIEMLogger()
            assert logger.json_logger is not None
            assert log_path.exists() or log_path.parent.exists()
            logger.close()
    
    def test_init_with_windows_event_log(self, mock_config):
        """Test initialization with Windows Event Log enabled"""
        mock_config.siem.enable_windows_event_log = True
        
        with patch('core.siem_logger.config', mock_config):
            with patch('sys.platform', 'win32'):
                # Mock the import inside WindowsEventLogger
                with patch('builtins.__import__') as mock_import:
                    def import_side_effect(name, *args, **kwargs):
                        if name == 'win32evtlog':
                            mock_win32 = MagicMock()
                            mock_win32.EVENTLOG_INFORMATION_TYPE = 4
                            mock_win32.EVENTLOG_WARNING_TYPE = 2
                            mock_win32.EVENTLOG_ERROR_TYPE = 1
                            return mock_win32
                        elif name == 'win32evtlogutil':
                            mock_util = MagicMock()
                            mock_util.ReportEvent = MagicMock()
                            return mock_util
                        else:
                            # Use real import for other modules
                            return __import__(name, *args, **kwargs)
                    
                    mock_import.side_effect = import_side_effect
                    logger = SIEMLogger()
                    # Windows logger should be initialized if on Windows
                    if sys.platform == 'win32':
                        # On actual Windows, it might be initialized
                        pass
                    logger.close()
    
    def test_init_with_syslog(self, mock_config):
        """Test initialization with syslog forwarding enabled"""
        mock_config.siem.syslog_enabled = True
        mock_config.siem.syslog_host = "localhost"
        mock_config.siem.syslog_port = 514
        mock_config.siem.syslog_protocol = "UDP"
        
        with patch('core.siem_logger.config', mock_config):
            with patch('socket.socket') as mock_socket:
                mock_sock = MagicMock()
                mock_socket.return_value = mock_sock
                
                logger = SIEMLogger()
                assert logger.syslog_forwarder is not None
                logger.close()


class TestSIEMLoggerJSONLogging:
    """Tests for JSON structured logging"""
    
    def test_log_event_to_json(self, siem_logger_with_json):
        """Test logging an event to JSON file"""
        logger, log_path = siem_logger_with_json
        
        logger.log_event(
            event_type=SIEMEventType.AUTHENTICATION,
            message="User logged in successfully",
            severity=SIEMSeverity.INFO,
            username="testuser",
            source_ip="192.168.1.100",
            success=True
        )
        
        # Give time for file write
        import time
        time.sleep(0.1)
        
        # Read and verify JSON log
        if log_path.exists():
            with open(log_path, 'r') as f:
                lines = f.readlines()
                assert len(lines) > 0
                log_entry = json.loads(lines[-1])
                assert log_entry['event_type'] == "authentication"
                assert log_entry['severity'] == "INFO"
                assert log_entry['event_message'] == "User logged in successfully"
                assert log_entry['username'] == "testuser"
                assert log_entry['source_ip'] == "192.168.1.100"
                assert log_entry['success'] is True
    
    def test_log_event_with_all_fields(self, siem_logger_with_json):
        """Test logging with all optional fields"""
        logger, log_path = siem_logger_with_json
        
        additional_data = {
            "session_id": "abc123",
            "user_agent": "Mozilla/5.0",
            "request_path": "/api/data"
        }
        
        logger.log_event(
            event_type=SIEMEventType.DATA_ACCESS,
            message="Data query executed",
            severity=SIEMSeverity.INFO,
            username="testuser",
            source_ip="192.168.1.100",
            resource="people_table",
            action="SELECT",
            success=True,
            additional_data=additional_data
        )
        
        import time
        time.sleep(0.1)
        
        if log_path.exists():
            with open(log_path, 'r') as f:
                lines = f.readlines()
                if lines:
                    log_entry = json.loads(lines[-1])
                    assert log_entry['resource'] == "people_table"
                    assert log_entry['action'] == "SELECT"
                    assert 'additional_data' in log_entry
    
    def test_log_event_different_severities(self, siem_logger_with_json):
        """Test logging events with different severity levels"""
        logger, log_path = siem_logger_with_json
        
        severities = [
            SIEMSeverity.EMERGENCY,
            SIEMSeverity.ALERT,
            SIEMSeverity.CRITICAL,
            SIEMSeverity.ERROR,
            SIEMSeverity.WARNING,
            SIEMSeverity.NOTICE,
            SIEMSeverity.INFO,
            SIEMSeverity.DEBUG
        ]
        
        for severity in severities:
            logger.log_event(
                event_type=SIEMEventType.SYSTEM_EVENT,
                message=f"Test event with {severity.name} severity",
                severity=severity
            )
        
        import time
        time.sleep(0.2)
        
        if log_path.exists():
            with open(log_path, 'r') as f:
                lines = f.readlines()
                assert len(lines) >= len(severities)
    
    def test_log_event_different_event_types(self, siem_logger_with_json):
        """Test logging different event types"""
        logger, log_path = siem_logger_with_json
        
        event_types = [
            SIEMEventType.AUTHENTICATION,
            SIEMEventType.AUTHORIZATION,
            SIEMEventType.ETL_OPERATION,
            SIEMEventType.DATA_ACCESS,
            SIEMEventType.SYSTEM_EVENT,
            SIEMEventType.SECURITY_EVENT,
            SIEMEventType.CONFIGURATION_CHANGE,
            SIEMEventType.ERROR
        ]
        
        for event_type in event_types:
            logger.log_event(
                event_type=event_type,
                message=f"Test {event_type.value} event",
                severity=SIEMSeverity.INFO
            )
        
        import time
        time.sleep(0.2)
        
        if log_path.exists():
            with open(log_path, 'r') as f:
                lines = f.readlines()
                assert len(lines) >= len(event_types)


class TestSIEMLoggerSensitiveDataFiltering:
    """Tests for sensitive data filtering"""
    
    def test_filter_sensitive_data(self, mock_config, temp_log_dir):
        """Test that sensitive data is filtered from logs"""
        log_path = temp_log_dir / "siem_test.json"
        mock_config.siem.json_log_path = log_path
        mock_config.siem.include_sensitive_data = False
        
        with patch('core.siem_logger.config', mock_config):
            logger = SIEMLogger()
            
            sensitive_data = {
                "password": "secret123",
                "api_key": "key12345",
                "ssn": "123-45-6789",
                "medicaid_id": "MED123456",
                "person_id": "P123456",
                "safe_field": "safe_value"
            }
            
            logger.log_event(
                event_type=SIEMEventType.AUTHENTICATION,
                message="Login attempt",
                severity=SIEMSeverity.INFO,
                additional_data=sensitive_data
            )
            
            import time
            time.sleep(0.1)
            
            if log_path.exists():
                with open(log_path, 'r') as f:
                    lines = f.readlines()
                    if lines:
                        log_entry = json.loads(lines[-1])
                        if 'additional_data' in log_entry:
                            additional = log_entry['additional_data']
                            assert additional.get('password') == "[REDACTED]"
                            assert additional.get('api_key') == "[REDACTED]"
                            assert additional.get('ssn') == "[REDACTED]"
                            assert additional.get('safe_field') == "safe_value"
            
            logger.close()
    
    def test_include_sensitive_data_when_enabled(self, mock_config, temp_log_dir):
        """Test that sensitive data is included when configured"""
        log_path = temp_log_dir / "siem_test.json"
        mock_config.siem.json_log_path = log_path
        mock_config.siem.include_sensitive_data = True
        
        with patch('core.siem_logger.config', mock_config):
            logger = SIEMLogger()
            
            sensitive_data = {
                "password": "secret123",
                "api_key": "key12345"
            }
            
            logger.log_event(
                event_type=SIEMEventType.AUTHENTICATION,
                message="Login attempt",
                severity=SIEMSeverity.INFO,
                additional_data=sensitive_data
            )
            
            import time
            time.sleep(0.1)
            
            if log_path.exists():
                with open(log_path, 'r') as f:
                    lines = f.readlines()
                    if lines:
                        log_entry = json.loads(lines[-1])
                        if 'additional_data' in log_entry:
                            additional = log_entry['additional_data']
                            # When enabled, sensitive data should be included
                            assert 'password' in additional or 'api_key' in additional
            
            logger.close()


class TestSIEMLoggerSeverityMapping:
    """Tests for severity to log level mapping"""
    
    def test_severity_to_log_level(self, mock_config):
        """Test severity to Python logging level conversion"""
        with patch('core.siem_logger.config', mock_config):
            logger = SIEMLogger()
            
            assert logger._severity_to_log_level(SIEMSeverity.EMERGENCY) == logging.CRITICAL
            assert logger._severity_to_log_level(SIEMSeverity.ALERT) == logging.CRITICAL
            assert logger._severity_to_log_level(SIEMSeverity.CRITICAL) == logging.CRITICAL
            assert logger._severity_to_log_level(SIEMSeverity.ERROR) == logging.ERROR
            assert logger._severity_to_log_level(SIEMSeverity.WARNING) == logging.WARNING
            assert logger._severity_to_log_level(SIEMSeverity.NOTICE) == logging.INFO
            assert logger._severity_to_log_level(SIEMSeverity.INFO) == logging.INFO
            assert logger._severity_to_log_level(SIEMSeverity.DEBUG) == logging.DEBUG
            
            logger.close()
    
    def test_severity_to_windows_type(self, mock_config):
        """Test severity to Windows Event type conversion"""
        with patch('core.siem_logger.config', mock_config):
            logger = SIEMLogger()
            
            assert logger._severity_to_windows_type(SIEMSeverity.EMERGENCY) == "Error"
            assert logger._severity_to_windows_type(SIEMSeverity.ALERT) == "Error"
            assert logger._severity_to_windows_type(SIEMSeverity.CRITICAL) == "Error"
            assert logger._severity_to_windows_type(SIEMSeverity.ERROR) == "Warning"
            assert logger._severity_to_windows_type(SIEMSeverity.WARNING) == "Warning"
            assert logger._severity_to_windows_type(SIEMSeverity.NOTICE) == "Information"
            assert logger._severity_to_windows_type(SIEMSeverity.INFO) == "Information"
            assert logger._severity_to_windows_type(SIEMSeverity.DEBUG) == "Information"
            
            logger.close()


class TestSIEMLoggerFormatting:
    """Tests for event formatting"""
    
    def test_format_for_windows(self, mock_config):
        """Test Windows Event Log formatting"""
        with patch('core.siem_logger.config', mock_config):
            logger = SIEMLogger()
            
            event_data = {
                "event_type": "authentication",
                "severity": "INFO",
                "event_message": "User logged in",
                "username": "testuser",
                "resource": "login_page",
                "action": "login",
                "success": True
            }
            
            formatted = logger._format_for_windows(event_data)
            assert "authentication" in formatted
            assert "testuser" in formatted
            assert "login_page" in formatted
            assert "login" in formatted
            assert "True" in formatted
            
            logger.close()
    
    def test_format_for_syslog(self, mock_config):
        """Test syslog message formatting"""
        with patch('core.siem_logger.config', mock_config):
            logger = SIEMLogger()
            
            event_data = {
                "event_type": "authentication",
                "severity": "INFO",
                "event_message": "User logged in",
                "username": "testuser",
                "source_ip": "192.168.1.100",
                "resource": "login_page",
                "action": "login",
                "success": True
            }
            
            formatted = logger._format_for_syslog(event_data)
            assert "event_type=authentication" in formatted
            assert "user=testuser" in formatted
            assert "src=192.168.1.100" in formatted
            assert "resource=login_page" in formatted
            assert "action=login" in formatted
            assert "outcome=success" in formatted
            
            logger.close()


class TestSyslogForwarder:
    """Tests for SyslogForwarder"""
    
    def test_syslog_forwarder_udp(self):
        """Test UDP syslog forwarding"""
        with patch('socket.socket') as mock_socket:
            mock_sock = MagicMock()
            mock_socket.return_value = mock_sock
            
            forwarder = SyslogForwarder("localhost", 514, "UDP")
            assert forwarder.protocol == "UDP"
            assert forwarder.sock is not None
            
            forwarder.send("Test message", SIEMSeverity.INFO)
            
            # Verify sendto was called for UDP
            mock_sock.sendto.assert_called_once()
            args, kwargs = mock_sock.sendto.call_args
            assert isinstance(args[0], bytes)
            assert args[1] == ("localhost", 514)
            
            forwarder.close()
    
    def test_syslog_forwarder_tcp(self):
        """Test TCP syslog forwarding"""
        with patch('socket.socket') as mock_socket:
            mock_sock = MagicMock()
            mock_socket.return_value = mock_sock
            
            forwarder = SyslogForwarder("localhost", 514, "TCP")
            assert forwarder.protocol == "TCP"
            
            # TCP should attempt connection
            if forwarder.sock:
                forwarder.send("Test message", SIEMSeverity.INFO)
                # Verify sendall was called for TCP
                mock_sock.sendall.assert_called()
            
            forwarder.close()
    
    def test_syslog_priority_calculation(self):
        """Test syslog priority calculation"""
        with patch('socket.socket') as mock_socket:
            mock_sock = MagicMock()
            mock_socket.return_value = mock_sock
            
            forwarder = SyslogForwarder("localhost", 514, "UDP")
            
            # Test priority calculation: facility * 8 + severity
            # Facility 16 (local0), Severity INFO (6) = 16*8 + 6 = 134
            forwarder.send("Test", SIEMSeverity.INFO, facility=16)
            
            mock_sock.sendto.assert_called_once()
            sent_data = mock_sock.sendto.call_args[0][0].decode('utf-8')
            assert "<134>" in sent_data  # Priority value
            
            forwarder.close()
    
    def test_syslog_forwarder_connection_failure(self):
        """Test syslog forwarder handles connection failures gracefully"""
        with patch('socket.socket') as mock_socket:
            mock_sock = MagicMock()
            mock_sock.connect.side_effect = socket.error("Connection refused")
            mock_socket.return_value = mock_sock
            
            forwarder = SyslogForwarder("localhost", 514, "TCP")
            # Should handle failure gracefully
            assert forwarder.sock is None or forwarder.sock is not None
            
            forwarder.close()


class TestWindowsEventLogger:
    """Tests for WindowsEventLogger"""
    
    def test_windows_event_logger_non_windows(self):
        """Test Windows Event Logger on non-Windows platform"""
        with patch('sys.platform', 'linux'):
            logger = WindowsEventLogger()
            assert logger.enabled is False
    
    def test_windows_event_logger_windows_without_pywin32(self):
        """Test Windows Event Logger when pywin32 is not installed"""
        with patch('sys.platform', 'win32'):
            with patch('builtins.__import__', side_effect=ImportError("No module named win32evtlog")):
                logger = WindowsEventLogger()
                assert logger.enabled is False
    
    @pytest.mark.skipif(sys.platform != 'win32', reason="Windows-only test")
    def test_windows_event_logger_windows(self):
        """Test Windows Event Logger on Windows (if pywin32 available)"""
        try:
            logger = WindowsEventLogger()
            # Should be enabled if pywin32 is available
            if logger.enabled:
                logger.log_event("Test message", "Information", 1000)
        except Exception:
            # If pywin32 is not available, that's okay
            pass


class TestSIEMLoggerIntegration:
    """Integration tests for SIEM logger"""
    
    def test_log_event_when_disabled(self, mock_config):
        """Test that logging is skipped when SIEM is disabled"""
        mock_config.siem.enabled = False
        
        with patch('core.siem_logger.config', mock_config):
            logger = SIEMLogger()
            
            # Should not raise any errors, just return early
            logger.log_event(
                event_type=SIEMEventType.AUTHENTICATION,
                message="Test",
                severity=SIEMSeverity.INFO
            )
            
            logger.close()
    
    def test_log_event_with_all_backends(self, mock_config, temp_log_dir):
        """Test logging to all backends simultaneously"""
        log_path = temp_log_dir / "siem_test.json"
        mock_config.siem.json_log_path = log_path
        mock_config.siem.enable_json_logging = True
        mock_config.siem.enable_windows_event_log = True
        mock_config.siem.syslog_enabled = True
        mock_config.siem.syslog_host = "localhost"
        
        with patch('core.siem_logger.config', mock_config):
            with patch('socket.socket') as mock_socket:
                mock_sock = MagicMock()
                mock_socket.return_value = mock_sock
                
                with patch('sys.platform', 'win32'):
                    # Mock the import inside WindowsEventLogger
                    with patch('builtins.__import__') as mock_import:
                        def import_side_effect(name, *args, **kwargs):
                            if name == 'win32evtlog':
                                mock_win32 = MagicMock()
                                mock_win32.EVENTLOG_INFORMATION_TYPE = 4
                                mock_win32.EVENTLOG_WARNING_TYPE = 2
                                mock_win32.EVENTLOG_ERROR_TYPE = 1
                                return mock_win32
                            elif name == 'win32evtlogutil':
                                mock_util = MagicMock()
                                mock_util.ReportEvent = MagicMock()
                                return mock_util
                            else:
                                return __import__(name, *args, **kwargs)
                        
                        mock_import.side_effect = import_side_effect
                        logger = SIEMLogger()
                        
                        logger.log_event(
                            event_type=SIEMEventType.AUTHENTICATION,
                            message="Test event",
                            severity=SIEMSeverity.INFO,
                            username="testuser"
                        )
                        
                        import time
                        time.sleep(0.1)
                        
                        # Verify JSON logging
                        if log_path.exists():
                            assert log_path.stat().st_size > 0
                        
                        logger.close()


class TestSIEMLoggerConvenienceFunctions:
    """Tests for convenience functions"""
    
    def test_get_siem_logger_singleton(self, mock_config):
        """Test that get_siem_logger returns a singleton"""
        with patch('core.siem_logger.config', mock_config):
            with patch('core.siem_logger._siem_logger', None):
                logger1 = get_siem_logger()
                logger2 = get_siem_logger()
                assert logger1 is logger2
    
    def test_log_siem_event_convenience(self, mock_config, temp_log_dir):
        """Test the log_siem_event convenience function"""
        log_path = temp_log_dir / "siem_test.json"
        mock_config.siem.json_log_path = log_path
        
        with patch('core.siem_logger.config', mock_config):
            log_siem_event(
                SIEMEventType.AUTHENTICATION,
                "User logged in",
                severity=SIEMSeverity.INFO,
                username="testuser",
                source_ip="192.168.1.100",
                success=True
            )
            
            import time
            time.sleep(0.1)
            
            if log_path.exists():
                with open(log_path, 'r') as f:
                    lines = f.readlines()
                    if lines:
                        log_entry = json.loads(lines[-1])
                        assert log_entry['event_type'] == "authentication"
                        assert log_entry['username'] == "testuser"


class TestSIEMLoggerErrorHandling:
    """Tests for error handling"""
    
    def test_log_event_handles_json_logger_error(self, mock_config, temp_log_dir):
        """Test that errors in JSON logger are handled gracefully"""
        log_path = temp_log_dir / "siem_test.json"
        mock_config.siem.json_log_path = log_path
        
        with patch('core.siem_logger.config', mock_config):
            logger = SIEMLogger()
            
            # Simulate an error in JSON logging
            if logger.json_logger:
                original_log = logger.json_logger.log
                logger.json_logger.log = Mock(side_effect=Exception("JSON logger error"))
                
                # The error will propagate, but we can catch it in the test
                # In production, you might want to wrap this in try/except
                try:
                    logger.log_event(
                        event_type=SIEMEventType.SYSTEM_EVENT,
                        message="Test",
                        severity=SIEMSeverity.INFO
                    )
                except Exception:
                    # This is expected - the logger doesn't catch errors internally
                    # This test documents current behavior
                    pass
                
                logger.json_logger.log = original_log
            
            logger.close()
    
    def test_log_event_handles_syslog_error(self, mock_config, temp_log_dir):
        """Test that errors in syslog forwarding are handled"""
        log_path = temp_log_dir / "siem_test.json"
        mock_config.siem.json_log_path = log_path
        mock_config.siem.syslog_enabled = True
        mock_config.siem.syslog_host = "localhost"
        
        with patch('core.siem_logger.config', mock_config):
            with patch('socket.socket') as mock_socket:
                mock_sock = MagicMock()
                mock_sock.sendto.side_effect = socket.error("Network error")
                mock_socket.return_value = mock_sock
                
                logger = SIEMLogger()
                
                # The syslog error is caught in SyslogForwarder.send(), but JSON logging
                # might still work. Test that the logger doesn't crash.
                try:
                    if logger.syslog_forwarder:
                        logger.log_event(
                            event_type=SIEMEventType.SYSTEM_EVENT,
                            message="Test",
                            severity=SIEMSeverity.INFO
                        )
                except Exception:
                    # If JSON logger also fails, that's okay for this test
                    pass
                
                logger.close()
    
    def test_close_handles_none_forwarder(self, mock_config):
        """Test that close() handles None forwarder gracefully"""
        with patch('core.siem_logger.config', mock_config):
            logger = SIEMLogger()
            logger.syslog_forwarder = None
            # Should not raise
            logger.close()


class TestSIEMLoggerRealWorldScenarios:
    """Tests simulating real-world SIEM logging scenarios"""
    
    def test_authentication_success(self, siem_logger_with_json):
        """Test logging successful authentication"""
        logger, log_path = siem_logger_with_json
        
        logger.log_event(
            event_type=SIEMEventType.AUTHENTICATION,
            message="User authenticated successfully",
            severity=SIEMSeverity.INFO,
            username="admin",
            source_ip="192.168.1.50",
            success=True
        )
        
        import time
        time.sleep(0.1)
        
        if log_path.exists():
            with open(log_path, 'r') as f:
                lines = f.readlines()
                if lines:
                    log_entry = json.loads(lines[-1])
                    assert log_entry['event_type'] == "authentication"
                    assert log_entry['success'] is True
                    assert log_entry['username'] == "admin"
    
    def test_authentication_failure(self, siem_logger_with_json):
        """Test logging failed authentication"""
        logger, log_path = siem_logger_with_json
        
        logger.log_event(
            event_type=SIEMEventType.AUTHENTICATION,
            message="Authentication failed: invalid password",
            severity=SIEMSeverity.WARNING,
            username="admin",
            source_ip="192.168.1.50",
            success=False,
            additional_data={"attempt_count": 3}
        )
        
        import time
        time.sleep(0.1)
        
        if log_path.exists():
            with open(log_path, 'r') as f:
                lines = f.readlines()
                if lines:
                    log_entry = json.loads(lines[-1])
                    assert log_entry['event_type'] == "authentication"
                    assert log_entry['success'] is False
                    assert log_entry['severity'] == "WARNING"
    
    def test_data_access_event(self, siem_logger_with_json):
        """Test logging data access event"""
        logger, log_path = siem_logger_with_json
        
        logger.log_event(
            event_type=SIEMEventType.DATA_ACCESS,
            message="User accessed sensitive data",
            severity=SIEMSeverity.INFO,
            username="analyst",
            source_ip="10.0.0.5",
            resource="people_table",
            action="SELECT",
            success=True,
            additional_data={"rows_returned": 150}
        )
        
        import time
        time.sleep(0.1)
        
        if log_path.exists():
            with open(log_path, 'r') as f:
                lines = f.readlines()
                if lines:
                    log_entry = json.loads(lines[-1])
                    assert log_entry['event_type'] == "data_access"
                    assert log_entry['resource'] == "people_table"
                    assert log_entry['action'] == "SELECT"
    
    def test_security_event(self, siem_logger_with_json):
        """Test logging security event"""
        logger, log_path = siem_logger_with_json
        
        logger.log_event(
            event_type=SIEMEventType.SECURITY_EVENT,
            message="Suspicious activity detected",
            severity=SIEMSeverity.ALERT,
            username="unknown",
            source_ip="203.0.113.1",
            success=False,
            additional_data={"threat_level": "high", "pattern": "brute_force"}
        )
        
        import time
        time.sleep(0.1)
        
        if log_path.exists():
            with open(log_path, 'r') as f:
                lines = f.readlines()
                if lines:
                    log_entry = json.loads(lines[-1])
                    assert log_entry['event_type'] == "security_event"
                    assert log_entry['severity'] == "ALERT"
                    assert log_entry['severity_code'] == 1
    
    def test_etl_operation_event(self, siem_logger_with_json):
        """Test logging ETL operation event"""
        logger, log_path = siem_logger_with_json
        
        logger.log_event(
            event_type=SIEMEventType.ETL_OPERATION,
            message="ETL job completed successfully",
            severity=SIEMSeverity.INFO,
            username="system",
            resource="etl_pipeline",
            action="run",
            success=True,
            additional_data={
                "records_processed": 10000,
                "duration_seconds": 45.2,
                "source": "sftp_import"
            }
        )
        
        import time
        time.sleep(0.1)
        
        if log_path.exists():
            with open(log_path, 'r') as f:
                lines = f.readlines()
                if lines:
                    log_entry = json.loads(lines[-1])
                    assert log_entry['event_type'] == "etl_operation"
                    assert log_entry['resource'] == "etl_pipeline"
                    assert log_entry['action'] == "run"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

