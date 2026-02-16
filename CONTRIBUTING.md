# Contributing to RushTI

Thank you for your interest in contributing to RushTI! This document provides guidelines and instructions for contributing.

## Development Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/cubewise-code/rushti.git
   cd rushti
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## Running Tests

RushTI uses pytest for testing. Tests are organized into two categories:

- **Unit tests** (`tests/unit/`): No TM1 connection required, fast execution
- **Integration tests** (`tests/integration/`): Require a TM1 server connection

### Test Directory Structure

```
tests/
├── conftest.py            # Shared fixtures and configuration
├── config.ini.template    # Template for TM1 test configuration
├── resources/             # Test resource files (taskfiles, etc.)
├── unit/                  # Unit tests (408 tests, no TM1 required)
│   ├── test_checkpoint.py
│   ├── test_config_resolution.py
│   ├── test_dag.py
│   ├── test_dag_visualization.py
│   ├── test_exclusive.py
│   ├── test_logging.py
│   ├── test_run_modes.py
│   ├── test_settings.py
│   ├── test_taskfile.py
│   └── test_utils.py
└── integration/           # Integration tests (TM1 required)
    ├── test_checkpoint_resume.py
    ├── test_dag_execution.py
    ├── test_v11_v12_execution.py
    └── test_v11_v12_features.py
```

### Running Unit Tests (No TM1 Required)

```bash
# Run all unit tests
pytest tests/unit/ -v

# Run a specific unit test file
pytest tests/unit/test_dag.py -v

# Run tests matching a pattern
pytest tests/unit/ -k "checkpoint" -v
```

### Running Integration Tests (TM1 Required)

Integration tests require a TM1 server connection. Configure using a config.ini file.

**Default: tests/config.ini**
```bash
# Copy the template and fill in your TM1 credentials
cp tests/config.ini.template tests/config.ini
# Edit tests/config.ini with your TM1 server settings

pytest tests/integration/ -v
```

**Alternative: Custom config path**
```bash
# Use RUSHTI_TEST_CONFIG to point to any config.ini file
export RUSHTI_TEST_CONFIG="/path/to/your/config.ini"

pytest tests/integration/ -v
```

The config.ini file uses the standard RushTI format with TM1 instance sections. See `tests/config.ini.template` for the expected format.

Integration tests are automatically skipped when no TM1 configuration is available.

### Running All Tests

```bash
# Run all tests (unit tests pass, integration tests skip if no TM1)
pytest tests/ -v

# Run only tests that don't require TM1
pytest -m "not requires_tm1" -v

# Run only tests that require TM1
pytest -m "requires_tm1" -v
```

### Adding New Tests

1. **Unit tests**: Add to the appropriate file in `tests/unit/` based on the module being tested
2. **Integration tests**: Add to `tests/integration/` and decorate with `@pytest.mark.requires_tm1`

Example unit test:
```python
# tests/unit/test_taskfile.py
class TestMyNewFeature(unittest.TestCase):
    def test_something(self):
        # Test implementation
        pass
```

Example integration test:
```python
# tests/integration/test_my_feature.py
import pytest
import unittest

@pytest.mark.requires_tm1
class TestMyFeatureIntegration(unittest.TestCase):
    def test_with_tm1(self):
        # Test implementation requiring TM1
        pass
```

Tests are automatically run on all pull requests via GitHub Actions. Unit tests run on every PR, while integration tests run when TM1 secrets are configured.

## Making Changes

1. **Create a branch** for your changes:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** and ensure tests pass

3. **Commit your changes** with a clear commit message

4. **Push and create a Pull Request**

## Pull Request Process

1. Ensure your code follows the existing style
2. Update documentation if needed
3. Add tests for new functionality
4. Fill out the PR template completely

### Release Labels

Maintainers will apply one of these labels to trigger a release:

| Label | Version Bump | Example |
|-------|--------------|---------|
| `release:patch` | Patch | 1.5.0 → 1.5.1 |
| `release:minor` | Minor | 1.5.0 → 1.6.0 |
| `release:major` | Major | 1.5.0 → 2.0.0 |

**No label = no release.** This is used for documentation, CI changes, or batching multiple PRs before a release.

## Release Process

Releases are automated via GitHub Actions:

1. PR is merged to `master`
2. CI tests run automatically
3. If tests pass and a release label is present:
   - Version is bumped in code
   - Git tag is created
   - Windows executable is built
   - GitHub Release is created with the executable attached

## Code Style

- Follow PEP 8 guidelines
- Use meaningful variable and function names
- Add docstrings to functions
- Keep functions focused and reasonably sized

## Questions?

If you have questions, feel free to:
- Open an issue with the "question" template
- Check existing issues and discussions

Thank you for contributing!
