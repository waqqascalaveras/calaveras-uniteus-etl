"""
================================================================================
Calaveras UniteUs ETL - Unified Test Configuration and Fixtures
================================================================================
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency

Description:
    Shared pytest configuration and fixtures for all tests (unit, integration, API).
    Provides reusable test data, mock objects, and temporary resources.

Fixtures:
    - temp_dir: Temporary directory for test files
    - sample_people_data: Sample people DataFrame
    - sample_cases_data: Sample cases DataFrame
    - sample_referrals_data: Sample referrals DataFrame

Features:
    - Automatic cleanup of temporary resources
    - Isolated test environments
    - Reusable test data
    - Consistent test configuration

================================================================================
"""
import pytest
import pandas as pd
import tempfile
import shutil
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path for all tests
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


@pytest.fixture
def client():
    """Create a FastAPI test client for API endpoint tests"""
    from fastapi.testclient import TestClient
    from core.app import app
    return TestClient(app)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests"""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def sample_people_data():
    """Sample people data for testing"""
    return pd.DataFrame({
        'person_id': ['p1', 'p2', 'p3'],
        'first_name': ['John', 'Jane', 'José'],
        'last_name': ['Doe', 'Smith', 'García'],
        'people_created_at': ['2024-01-01 10:00:00', '2024-01-02 11:00:00', None]
    })


@pytest.fixture
def sample_cases_data():
    """Sample cases data for testing"""
    return pd.DataFrame({
        'case_id': ['c1', 'c2', 'c3'],
        'person_id': ['p1', 'p2', 'p3'],
        'case_status': ['open', 'managed', 'processed'],
        'case_created_at': ['2024-01-01 10:00:00', '2024-01-02 11:00:00', '2024-01-03 10:00:00']
    })


@pytest.fixture
def sample_referrals_data():
    """Sample referrals data for testing"""
    return pd.DataFrame({
        'referral_id': ['r1', 'r2', 'r3'],
        'person_id': ['p1', 'p2', 'p3'],
        'referral_status': ['sent', 'accepted', 'completed'],
        'referral_created_at': ['2024-01-01', '2024-01-02', '2024-01-03']
    })


@pytest.fixture
def project_root_path():
    """Return the project root path"""
    return project_root

