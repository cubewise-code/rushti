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

Run the test suite using:

```bash
python -m unittest discover tests/ -v
```

Tests are automatically run on all pull requests via GitHub Actions.

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
