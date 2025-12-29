"""
SIEM Integration Logger

SIEM (Security Information and Event Management) integration module providing
structured logging for enterprise security monitoring systems. Supports JSON
structured logs, Windows Event Log, and syslog forwarding.

Author: Waqqas Hanafi
Copyright: © 2025 Calaveras County Health and Human Services Agency
"""

import logging
import json
import socket
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
from enum import Enum

from .config import config


class SIEMEventType(Enum):
    """SIEM event type categories"""
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    ETL_OPERATION = "etl_operation"
    DATA_ACCESS = "data_access"
    SYSTEM_EVENT = "system_event"
    SECURITY_EVENT = "security_event"
    CONFIGURATION_CHANGE = "configuration_change"
    ERROR = "error"


class SIEMSeverity(Enum):
    """SIEM event severity levels (aligned with syslog)"""
    EMERGENCY = 0  # System is unusable
    ALERT = 1      # Action must be taken immediately
    CRITICAL = 2   # Critical conditions
    ERROR = 3      # Error conditions
    WARNING = 4    # Warning conditions
    NOTICE = 5     # Normal but significant condition
    INFO = 6       # Informational messages
    DEBUG = 7      # Debug-level messages


class WindowsEventLogger:
    """Windows Event Log integration (Windows only)"""
    
    def __init__(self, app_name: str = "UniteUsETL"):
        self.app_name = app_name
        self.enabled = False
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Try to import Windows-specific modules
        if sys.platform == 'win32':
            try:
                import win32evtlog
                import win32evtlogutil
                self.win32evtlog = win32evtlog
                self.win32evtlogutil = win32evtlogutil
                self.enabled = True
            except ImportError:
                self.logger.warning("pywin32 not installed - Windows Event Log integration disabled")
        else:
            self.logger.info("Not running on Windows - Windows Event Log integration disabled")
    
    def log_event(self, message: str, event_type: str = "Information", event_id: int = 0):
        """Log event to Windows Event Log
        
        Uses Event ID 0 to avoid message file registration requirements.
        The message is included directly in the event data.
        """
        if not self.enabled:
            return
        
        try:
            # Map event type to Windows constants
            event_type_map = {
                "Information": self.win32evtlog.EVENTLOG_INFORMATION_TYPE,
                "Warning": self.win32evtlog.EVENTLOG_WARNING_TYPE,
                "Error": self.win32evtlog.EVENTLOG_ERROR_TYPE,
            }
            
            win_event_type = event_type_map.get(event_type, self.win32evtlog.EVENTLOG_INFORMATION_TYPE)
            
            # Use Event ID 0 to avoid message file registration requirements
            # The message will appear directly in the event data
            self.win32evtlogutil.ReportEvent(
                self.app_name,
                0,  # Event ID 0 doesn't require message file registration
                eventType=win_event_type,
                strings=[message]
            )
        except Exception as e:
            self.logger.error(f"Failed to log to Windows Event Log: {e}")


class SyslogForwarder:
    """Forward logs to remote syslog/SIEM server"""
    
    def __init__(self, host: str, port: int = 514, protocol: str = "UDP"):
        self.host = host
        self.port = port
        self.protocol = protocol.upper()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sock = None
        
        if self.protocol == "TCP":
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                self.sock.connect((self.host, self.port))
            except Exception as e:
                self.logger.error(f"Failed to connect to syslog server {host}:{port}: {e}")
                self.sock = None
        elif self.protocol == "UDP":
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    def send(self, message: str, severity: SIEMSeverity = SIEMSeverity.INFO, facility: int = 16):
        """Send syslog message (RFC 3164 format)"""
        if not self.sock:
            return
        
        try:
            # Calculate priority (facility * 8 + severity)
            priority = (facility * 8) + severity.value
            
            # Format syslog message
            timestamp = datetime.now().strftime("%b %d %H:%M:%S")
            hostname = socket.gethostname()
            syslog_msg = f"<{priority}>{timestamp} {hostname} UniteUsETL: {message}\n"
            
            if self.protocol == "TCP":
                self.sock.sendall(syslog_msg.encode('utf-8'))
            else:  # UDP
                self.sock.sendto(syslog_msg.encode('utf-8'), (self.host, self.port))
        except Exception as e:
            self.logger.error(f"Failed to send syslog message: {e}")
    
    def close(self):
        """Close syslog connection"""
        if self.sock:
            self.sock.close()
            self.sock = None


class SIEMLogger:
    """
    Main SIEM logging service
    Coordinates Windows Event Log and syslog forwarding to IT's SIEM server
    """
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.windows_logger = None
        self.syslog_forwarder = None
        
        # Initialize based on configuration
        if config.siem.enabled:
            self._initialize_loggers()
    
    def _initialize_loggers(self):
        """Initialize all configured logging backends"""
        
        # Windows Event Log
        if config.siem.enable_windows_event_log:
            self.windows_logger = WindowsEventLogger()
        
        # Syslog forwarding to IT's SIEM server
        if config.siem.syslog_enabled and config.siem.syslog_host:
            try:
                self.syslog_forwarder = SyslogForwarder(
                    config.siem.syslog_host,
                    config.siem.syslog_port,
                    config.siem.syslog_protocol
                )
            except Exception as e:
                self.logger.error(f"Failed to initialize syslog forwarder: {e}")
    
    def log_event(
        self,
        event_type: SIEMEventType,
        message: str,
        severity: SIEMSeverity = SIEMSeverity.INFO,
        username: Optional[str] = None,
        source_ip: Optional[str] = None,
        resource: Optional[str] = None,
        action: Optional[str] = None,
        success: bool = True,
        additional_data: Optional[Dict[str, Any]] = None
    ):
        """
        Log a SIEM event with structured data
        
        Args:
            event_type: Type/category of event
            message: Human-readable message
            severity: Event severity level
            username: User who triggered the event
            source_ip: Source IP address (for network events)
            resource: Affected resource (file, table, etc.)
            action: Action performed (read, write, delete, etc.)
            success: Whether the action succeeded
            additional_data: Any additional context data
        """
        if not config.siem.enabled:
            return
        
        # Build structured event data
        # Note: Don't use 'message' as a key in extra, it's reserved by logging
        event_data = {
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type.value,
            "severity": severity.name,
            "severity_code": severity.value,
            "event_message": message,  # Changed from 'message' to avoid conflict
            "username": username,
            "source_ip": source_ip,
            "resource": resource,
            "action": action,
            "success": success,
            "application": "UniteUs_ETL",
            "hostname": socket.gethostname()
        }
        
        # Add additional context
        if additional_data:
            # Filter out sensitive data if not configured to include it
            if not config.siem.include_sensitive_data:
                additional_data = self._filter_sensitive_data(additional_data)
            event_data["additional_data"] = additional_data
        
        # Log to Windows Event Log (with severity filtering)
        if self.windows_logger and config.siem.enable_windows_event_log:
            if self._should_log_to_destination(severity, config.siem.windows_event_log_min_severity):
                windows_event_type = self._severity_to_windows_type(severity)
                formatted_message = self._format_for_windows(event_data)
                self.windows_logger.log_event(formatted_message, windows_event_type)
        
        # Forward to syslog (with severity filtering)
        if self.syslog_forwarder and config.siem.syslog_enabled:
            if self._should_log_to_destination(severity, config.siem.syslog_min_severity):
                syslog_message = self._format_for_syslog(event_data)
                self.syslog_forwarder.send(syslog_message, severity)
    
    def _should_log_to_destination(self, event_severity: SIEMSeverity, min_severity_str: str) -> bool:
        """
        Check if event should be logged based on minimum severity level
        
        Args:
            event_severity: The severity level of the event
            min_severity_str: Minimum severity level as string (e.g., 'WARNING', 'ERROR')
        
        Returns:
            True if event severity is >= minimum severity (lower number = higher priority)
        """
        # Map severity names to enum
        severity_map = {
            'EMERGENCY': SIEMSeverity.EMERGENCY,
            'ALERT': SIEMSeverity.ALERT,
            'CRITICAL': SIEMSeverity.CRITICAL,
            'ERROR': SIEMSeverity.ERROR,
            'WARNING': SIEMSeverity.WARNING,
            'NOTICE': SIEMSeverity.NOTICE,
            'INFO': SIEMSeverity.INFO,
            'DEBUG': SIEMSeverity.DEBUG
        }
        
        min_severity = severity_map.get(min_severity_str.upper(), SIEMSeverity.INFO)
        
        # Lower value = higher severity (EMERGENCY=0, DEBUG=7)
        # So log if event severity value <= min severity value
        return event_severity.value <= min_severity.value
    
    def _filter_sensitive_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Remove sensitive fields from log data"""
        sensitive_keys = [
            'password', 'token', 'secret', 'api_key', 'ssn', 'social_security',
            'credit_card', 'medicaid_id', 'medicare_id', 'person_id'
        ]
        
        filtered = {}
        for key, value in data.items():
            if any(sensitive in key.lower() for sensitive in sensitive_keys):
                filtered[key] = "[REDACTED]"
            elif isinstance(value, dict):
                filtered[key] = self._filter_sensitive_data(value)
            else:
                filtered[key] = value
        
        return filtered
    
    def _severity_to_log_level(self, severity: SIEMSeverity) -> int:
        """Convert SIEM severity to Python logging level"""
        mapping = {
            SIEMSeverity.EMERGENCY: logging.CRITICAL,
            SIEMSeverity.ALERT: logging.CRITICAL,
            SIEMSeverity.CRITICAL: logging.CRITICAL,
            SIEMSeverity.ERROR: logging.ERROR,
            SIEMSeverity.WARNING: logging.WARNING,
            SIEMSeverity.NOTICE: logging.INFO,
            SIEMSeverity.INFO: logging.INFO,
            SIEMSeverity.DEBUG: logging.DEBUG,
        }
        return mapping.get(severity, logging.INFO)
    
    def _severity_to_windows_type(self, severity: SIEMSeverity) -> str:
        """Convert SIEM severity to Windows Event type"""
        if severity.value <= 2:  # Emergency, Alert, Critical
            return "Error"
        elif severity.value <= 4:  # Error, Warning
            return "Warning"
        else:
            return "Information"
    
    def _format_for_windows(self, event_data: Dict[str, Any]) -> str:
        """Format event data for Windows Event Log"""
        parts = [
            f"Event: {event_data['event_type']}",
            f"Severity: {event_data['severity']}",
            f"Message: {event_data['event_message']}",
        ]
        
        if event_data.get('username'):
            parts.append(f"User: {event_data['username']}")
        
        if event_data.get('resource'):
            parts.append(f"Resource: {event_data['resource']}")
        
        if event_data.get('action'):
            parts.append(f"Action: {event_data['action']}")
        
        parts.append(f"Success: {event_data['success']}")
        
        return " | ".join(parts)
    
    def _format_for_syslog(self, event_data: Dict[str, Any]) -> str:
        """Format event data for syslog"""
        # CEF (Common Event Format) style message
        parts = [
            f"event_type={event_data['event_type']}",
            f"severity={event_data['severity']}",
            f"msg={event_data['event_message']}",
        ]
        
        if event_data.get('username'):
            parts.append(f"user={event_data['username']}")
        
        if event_data.get('source_ip'):
            parts.append(f"src={event_data['source_ip']}")
        
        if event_data.get('resource'):
            parts.append(f"resource={event_data['resource']}")
        
        if event_data.get('action'):
            parts.append(f"action={event_data['action']}")
        
        parts.append(f"outcome={'success' if event_data['success'] else 'failure'}")
        
        return " ".join(parts)
    
    def close(self):
        """Close all logging connections"""
        if self.syslog_forwarder:
            self.syslog_forwarder.close()


# Global SIEM logger instance
_siem_logger: Optional[SIEMLogger] = None


def get_siem_logger() -> SIEMLogger:
    """Get the global SIEM logger instance"""
    global _siem_logger
    if _siem_logger is None:
        _siem_logger = SIEMLogger()
    return _siem_logger


def log_siem_event(
    event_type: SIEMEventType,
    message: str,
    severity: SIEMSeverity = SIEMSeverity.INFO,
    **kwargs
):
    """
    Convenience function to log a SIEM event
    
    Example:
        log_siem_event(
            SIEMEventType.AUTHENTICATION,
            "User logged in successfully",
            severity=SIEMSeverity.INFO,
            username="admin",
            source_ip="192.168.1.100",
            success=True
        )
    """
    siem_logger = get_siem_logger()
    siem_logger.log_event(event_type, message, severity, **kwargs)

