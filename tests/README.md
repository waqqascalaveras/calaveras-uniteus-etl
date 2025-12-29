# UniteUs ETL - Test Suite

This directory contains all tests for the UniteUs ETL application, organized by test type.

## Directory Structure

```
tests/
├── unit/              # Unit tests (fast, isolated, no external dependencies)
├── integration/       # Integration tests (test component interactions)
├── api/               # API endpoint tests
├── conftest.py        # Shared pytest fixtures and configuration
├── test_orchestrator.py  # Test orchestrator with timeout protection
└── README.md          # This file
```

## Test Categories

### Unit Tests (`tests/unit/`)
Fast, isolated tests that test individual components in isolation:
- Configuration management
- Database operations
- ETL service logic
- Report generation
- Authentication
- Audit logging

**Run unit tests only:**
```bash
python run_tests.py --unit
# or
pytest tests/unit/
```

### Integration Tests (`tests/integration/`)
Tests that verify component interactions:
- SFTP service integration
- Database API endpoints
- Security health checks
- Database schema conversion
- MSSQL adapter

**Run integration tests only:**
```bash
python run_tests.py --integration
# or
pytest tests/integration/
```

### API Tests (`tests/api/`)
Tests for API endpoints:
- SFTP key verification endpoints
- Security configuration endpoints
- Authentication endpoints
- Error handling

**Run API tests only:**
```bash
python run_tests.py --api
# or
pytest tests/api/
```

## Running Tests

### Run All Tests
```bash
python run_tests.py
```

### Run Specific Test File
```bash
pytest tests/unit/test_config.py
pytest tests/integration/test_sftp_service.py
```

### Run Specific Test Class
```bash
pytest tests/unit/test_config.py::TestConfigStructure
```

### Run Specific Test Method
```bash
pytest tests/unit/test_config.py::TestConfigStructure::test_config_exists
```

### Run with Coverage
```bash
pytest --cov=core --cov-report=html tests/
```

### Run with Verbose Output
```bash
pytest -v tests/
```

## Test Framework

All tests use **pytest** as the testing framework. This provides:
- Better assertion messages
- Fixture system for test setup/teardown
- Parametrized tests
- Plugin ecosystem
- Better test discovery

## Shared Fixtures

Common fixtures are defined in `tests/conftest.py`:
- `temp_dir`: Temporary directory for test files
- `sample_people_data`: Sample people DataFrame
- `sample_cases_data`: Sample cases DataFrame
- `sample_referrals_data`: Sample referrals DataFrame
- `project_root_path`: Project root path

## Test Orchestrator

The test orchestrator (`tests/test_orchestrator.py`) provides timeout protection for tests that might hang. It's useful for integration tests that make network calls.

**Run with orchestrator:**
```bash
python tests/test_orchestrator.py
python tests/test_orchestrator.py --timeout 60
python tests/test_orchestrator.py --test tests/integration/test_sftp_service
```

## Writing New Tests

### Unit Test Example
```python
import pytest
from core.config import config

class TestMyFeature:
    def test_something(self):
        """Test description"""
        result = my_function()
        assert result == expected_value
    
    def test_with_fixture(self, temp_dir):
        """Test using a fixture"""
        test_file = temp_dir / "test.txt"
        test_file.write_text("test")
        assert test_file.exists()
```

### Integration Test Example
```python
import pytest
from unittest.mock import patch, MagicMock

class TestMyIntegration:
    @patch('core.some_module.SomeClass')
    def test_integration(self, mock_class):
        """Test component integration"""
        mock_instance = MagicMock()
        mock_class.return_value = mock_instance
        # Test code
        assert something
```

## Best Practices

1. **One assertion per test** - Each test should test one specific thing
2. **Use descriptive names** - Test names should describe what they test
3. **Arrange-Act-Assert** - Structure tests clearly:
   - Arrange: Set up test data
   - Act: Execute the code being tested
   - Assert: Verify the results
4. **Use fixtures** - Don't duplicate setup code
5. **Mock external dependencies** - Don't make real network calls or database operations in unit tests
6. **Test edge cases** - Empty strings, None values, large numbers, etc.
7. **Test error handling** - Verify exceptions are raised when expected

## Test Coverage

Current test coverage: **36%** (target: 80%+)

To generate coverage report:
```bash
pytest --cov=core --cov-report=html tests/
# Open htmlcov/index.html in browser
```

## Continuous Integration

Tests are designed to work with CI/CD pipelines:

```yaml
# GitHub Actions example
- name: Run Tests
  run: python run_tests.py

# With coverage
- name: Run Tests with Coverage
  run: pytest --cov=core --cov-report=xml tests/
```

## Troubleshooting

### ImportError: No module named 'core'
Make sure you're running tests from the project root directory.

### Tests failing due to missing dependencies
Install required packages: `pip install -r requirements.txt`

### Tests timing out
Use the test orchestrator with a longer timeout:
```bash
python tests/test_orchestrator.py --timeout 60
```

## Migration Notes

This test suite was consolidated from:
- `unit_tests/` - Now `tests/unit/`
- `tests/` (old integration tests) - Now `tests/integration/` and `tests/api/`

All tests have been migrated from `unittest` to `pytest` for consistency and better features.

## Contact

For questions about tests, contact: Waqqas Hanafi  
Organization: Calaveras County Health and Human Services Agency
