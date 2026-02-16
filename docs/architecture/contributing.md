# Contributing

How to set up a development environment and contribute to RushTI.

---

## Development Setup

### Prerequisites

- Python 3.9 or later
- Git
- (Optional) A TM1/Planning Analytics server for running integration tests

### Clone and Install

```bash
git clone https://github.com/cubewise-code/rushti.git
cd rushti
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

The `[dev]` extra installs all development dependencies: pytest, pytest-cov,
black, isort, flake8, mypy, and pyinstaller.

---

## Project Structure

```
rushti/
├── src/rushti/             # Source code (all modules)
│   ├── cli.py              #   CLI entry point and argument parsing
│   ├── commands.py          #   Subcommand handler functions
│   ├── task.py              #   Task domain model (Task, OptimizedTask, Wait, ExecutionMode)
│   ├── dag.py               #   DAG domain model (DAG, TaskStatus, CircularDependencyError)
│   ├── taskfile.py          #   Task file parsing (TXT and JSON)
│   ├── parsing.py           #   DAG construction from task files
│   ├── taskfile_ops.py      #   Taskfile operations (expand, validate, visualize, analyze)
│   ├── execution.py         #   Thread-based parallel execution engine (ExecutionContext)
│   ├── settings.py          #   Settings loading and precedence resolution
│   ├── stats.py             #   SQLite statistics database operations
│   ├── optimizer.py         #   EWMA-based task runtime optimization
│   ├── dashboard.py         #   HTML dashboard generation (Chart.js)
│   ├── checkpoint.py        #   Checkpoint save/load for resume support
│   ├── exclusive.py         #   Exclusive mode session management
│   ├── tm1_integration.py   #   TM1 cube read/write
│   ├── tm1_build.py         #   Create TM1 logging objects
│   ├── tm1_assets.py        #   TM1 object structure definitions
│   ├── utils.py             #   Stateless helper functions
│   ├── messages.py          #   Centralized message templates
│   ├── logging.py           #   Structured execution logging
│   ├── db_admin.py          #   Database administration utilities
│   └── templates/           #   HTML templates
│       └── visualization.html  # DAG visualization template
├── tests/
│   ├── unit/               # Unit tests (408 tests, no TM1 required)
│   ├── integration/        # Integration tests (requires TM1 server)
│   ├── resources/          # Test fixtures and sample task files
│   ├── conftest.py         # Shared pytest fixtures
│   └── config.ini.template # Template for TM1 test credentials
├── docs/                   # MkDocs documentation source
├── docs/samples/           # Sample task files for documentation
├── config/                 # Configuration templates
│   ├── config.ini.template #   TM1 connection template
│   ├── settings.ini.template # Execution settings template
│   └── logging_config.ini  #   Python logging configuration
├── site/homepage/          # React website source
├── mkdocs.yml              # MkDocs site configuration
└── pyproject.toml          # Build system and project metadata
```

---

## Running Tests

RushTI uses **pytest**. Tests are organized into two categories.

### Unit Tests (No TM1 Required)

```bash
# Run all unit tests
python -m pytest tests/unit/ -q

# Run a specific test file
python -m pytest tests/unit/test_dag.py -v

# Run tests matching a keyword
python -m pytest tests/unit/ -k "checkpoint" -v

# Run with coverage report
python -m pytest tests/unit/ --cov=rushti --cov-report=term-missing
```

### Integration Tests (Requires TM1 Server)

Integration tests connect to a live TM1 instance. Configure credentials first:

```bash
# Copy the template and fill in your TM1 server details
cp tests/config.ini.template tests/config.ini
# Edit tests/config.ini with your TM1 address, port, and credentials

# Run integration tests
python -m pytest tests/integration/ -q

# Or point to a custom config location
export RUSHTI_TEST_CONFIG="/path/to/your/config.ini"
python -m pytest tests/integration/ -v
```

Integration tests are automatically skipped when no TM1 configuration is
available. They are decorated with `@pytest.mark.requires_tm1`.

### Running All Tests

```bash
# Unit tests pass; integration tests skip if no TM1 config
python -m pytest tests/ -v

# Only tests that do NOT require TM1
python -m pytest -m "not requires_tm1" -v

# Only tests that require TM1
python -m pytest -m "requires_tm1" -v
```

---

## Code Style

- **Python version**: 3.9+ (match the `requires-python` in `pyproject.toml`)
- **Formatting**: [black](https://github.com/psf/black) (line length 100)
- **Linting**: [ruff](https://docs.astral.sh/ruff/) (replaces flake8 and isort)
- **Type checking**: [mypy](https://mypy-lang.org/) -- type hints are encouraged on all public functions
- **Docstrings**: Required on modules, classes, and public functions. Use the
  triple-quote style shown throughout the codebase.

```bash
# Format code
black src/ tests/

# Lint (includes import sorting)
ruff check src/ tests/

# Auto-fix lint issues
ruff check --fix src/ tests/

# Type check
mypy src/rushti/
```

---

## Adding New Tests

### Unit Test Example

```python
# tests/unit/test_taskfile.py
import unittest

class TestMyNewFeature(unittest.TestCase):
    def test_something(self):
        result = my_function("input")
        self.assertEqual(result, "expected")
```

### Integration Test Example

```python
# tests/integration/test_my_feature.py
import pytest
import unittest

@pytest.mark.requires_tm1
class TestMyFeatureIntegration(unittest.TestCase):
    def test_with_tm1(self):
        # Requires a running TM1 server
        pass
```

---

## Pull Request Process

1. **Fork** the repository (external contributors) or create a branch (team
   members).
2. **Branch** from `rushti2dot0`:
   ```bash
   git checkout rushti2dot0
   git pull origin rushti2dot0
   git checkout -b feature/your-feature-name
   ```
3. **Implement** your changes. Follow the code style guidelines above.
4. **Add or update tests**. Unit tests are mandatory for new logic; integration
   tests are encouraged when the change touches TM1 interaction.
5. **Run the test suite** locally and ensure all unit tests pass:
   ```bash
   python -m pytest tests/unit/ -q
   ```
6. **Commit** with a clear, descriptive message.
7. **Push** and open a Pull Request against the `rushti2dot0` branch.

!!! tip "PR Checklist"
    - [ ] All unit tests pass
    - [ ] New code has type hints and docstrings
    - [ ] No linting errors (`ruff check`)
    - [ ] Documentation updated if user-facing behavior changed
    - [ ] Commit messages are clear and descriptive

### Release Labels

Maintainers apply a label to the merged PR to trigger a release:

| Label | Version Bump | Example |
|-------|-------------|---------|
| `release:patch` | Patch | 2.0.0 --> 2.0.1 |
| `release:minor` | Minor | 2.0.0 --> 2.1.0 |
| `release:major` | Major | 2.0.0 --> 3.0.0 |

No label means no release. This is used for documentation changes, CI
adjustments, or batching multiple PRs before a release.

---

## Sample Task Files

Two directories contain task file examples:

| Directory | Purpose |
|-----------|---------|
| `docs/samples/` | Human-readable examples used in the documentation site. |
| `tests/resources/examples/` | Machine-readable examples consumed by the test suite. |

When adding a new task file feature, provide samples in both locations.

---

## CI / CD

Tests run automatically on every pull request via GitHub Actions:

- **Unit tests** run on every PR across supported Python versions.
- **Integration tests** run when TM1 secrets are configured in the repository.
- **Releases** are automated: when a PR with a release label is merged to
  `master`, CI bumps the version, creates a Git tag, builds a Windows
  executable via PyInstaller, and publishes a GitHub Release.

---

## Questions?

- Open a GitHub issue with the **question** template.
- Check existing issues and discussions.
- Review the [Architecture docs](design.md) for deeper technical context.
