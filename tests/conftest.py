"""Pytest configuration and fixtures for RushTI tests.

This module provides:
- Python path setup for package imports
- Test configuration loading from config.ini
- TM1 connection fixtures (single and multi-instance)
- Common test utilities

Test Configuration:
    Tests look for TM1 configuration in this order:
    1. RUSHTI_TEST_CONFIG environment variable (path to config.ini)
    2. tests/config.ini (default location)
    3. Skip gracefully if neither is available

    The config.ini file uses standard RushTI format with TM1 instance sections.
    See tests/config.ini.template for the expected format.
"""

import configparser
import os
import shutil
import sys
import tempfile
from typing import Dict, Optional, Tuple

import pytest

# Add src/ directory to path for package imports
_src_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)


# =============================================================================
# Test Configuration
# =============================================================================


class TM1TestConfig:
    """TM1 connection configuration for tests.

    Stores all config options from a config.ini section and passes them
    directly to TM1py. This supports any TM1 deployment type (local, cloud,
    CAM auth, etc.) without hardcoding specific parameters.
    """

    def __init__(self, instance: str, config_dict: Dict[str, str]):
        """Initialize with instance name and config dictionary.

        Args:
            instance: The TM1 instance name (section name from config.ini)
            config_dict: All key-value pairs from the config section
        """
        self.instance = instance
        self._config = config_dict

    def to_dict(self) -> Dict[str, str]:
        """Return config dictionary for TM1Service."""
        return self._config.copy()


def _is_tm1_section(config: configparser.ConfigParser, section: str) -> bool:
    """Check if a config section contains TM1 connection info.

    Supports multiple auth methods:
    - address + user (local TM1)
    - base_url + user (cloud with password/CAM/LDAP)
    - base_url + api_key (v12 SaaS)
    """
    has_address = config.has_option(section, "address") or config.has_option(section, "base_url")
    has_auth = config.has_option(section, "user") or config.has_option(section, "api_key")
    return has_address and has_auth


def _get_config_path() -> Optional[str]:
    """Get the path to the test configuration file.

    Checks in order:
    1. RUSHTI_TEST_CONFIG environment variable
    2. tests/config.ini (default location)

    Returns:
        Path to config file if found, None otherwise.
    """
    # Check environment variable first
    env_config = os.environ.get("RUSHTI_TEST_CONFIG")
    if env_config and os.path.exists(env_config):
        return env_config

    # Fall back to default location
    default_path = os.path.join(os.path.dirname(__file__), "config.ini")
    if os.path.exists(default_path):
        return default_path

    return None


def _load_config_from_file(config_path: str) -> Optional[TM1TestConfig]:
    """Load first TM1 test configuration from a config.ini file.

    Args:
        config_path: Path to the config.ini file.

    Returns:
        TM1TestConfig if valid configuration found, None otherwise.
    """
    config = configparser.ConfigParser()
    config.read(config_path)

    for section in config.sections():
        if _is_tm1_section(config, section):
            config_dict = dict(config.items(section))
            return TM1TestConfig(instance=section, config_dict=config_dict)

    return None


def _load_all_configs_from_file(config_path: str) -> Dict[str, TM1TestConfig]:
    """Load all TM1 test configurations from a config.ini file.

    Args:
        config_path: Path to the config.ini file.

    Returns:
        Dict of {instance_name: TM1TestConfig} for all valid TM1 sections.
    """
    configs = {}
    config = configparser.ConfigParser()
    config.read(config_path)

    for section in config.sections():
        if _is_tm1_section(config, section):
            config_dict = dict(config.items(section))
            configs[section] = TM1TestConfig(instance=section, config_dict=config_dict)

    return configs


def get_test_tm1_config() -> Tuple[Optional[TM1TestConfig], str]:
    """Get first TM1 test configuration (backward compatible).

    Returns:
        Tuple of (config, source) where source indicates where config came from.
        If config is None, source contains the reason.
    """
    config_path = _get_config_path()

    if config_path is None:
        return (
            None,
            "No test configuration found. Set RUSHTI_TEST_CONFIG env var or create tests/config.ini",
        )

    config = _load_config_from_file(config_path)
    if config:
        return config, config_path

    return None, f"Config file found at {config_path} but no valid TM1 section"


def _get_test_settings_path() -> Optional[str]:
    """Get the path to the test settings.ini file.

    Checks in order:
    1. RUSHTI_TEST_SETTINGS environment variable
    2. tests/settings.ini (default location)

    Returns:
        Path to settings file if found, None otherwise.
    """
    env_settings = os.environ.get("RUSHTI_TEST_SETTINGS")
    if env_settings and os.path.exists(env_settings):
        return env_settings

    default_path = os.path.join(os.path.dirname(__file__), "settings.ini")
    if os.path.exists(default_path):
        return default_path

    return None


def get_test_tm1_names() -> dict:
    """Load TM1 cube/dimension names from test settings.ini.

    Returns dict with keys: cube_name, dim_workflow, dim_task, dim_run, dim_measure.
    Falls back to production defaults if no test settings found.
    """
    path = _get_test_settings_path()
    if path:
        config = configparser.ConfigParser()
        config.read(path)
        if config.has_section("tm1_integration"):
            return {
                "cube_name": config.get(
                    "tm1_integration", "default_rushti_cube", fallback="rushti"
                ),
                "dim_workflow": config.get(
                    "tm1_integration", "default_workflow_dim", fallback="rushti_workflow"
                ),
                "dim_task": config.get(
                    "tm1_integration", "default_task_id_dim", fallback="rushti_task_id"
                ),
                "dim_run": config.get(
                    "tm1_integration", "default_run_id_dim", fallback="rushti_run_id"
                ),
                "dim_measure": config.get(
                    "tm1_integration", "default_measure_dim", fallback="rushti_measure"
                ),
            }
    # Defaults (production names)
    return {
        "cube_name": "rushti",
        "dim_workflow": "rushti_workflow",
        "dim_task": "rushti_task_id",
        "dim_run": "rushti_run_id",
        "dim_measure": "rushti_measure",
    }


def get_all_test_tm1_configs() -> Tuple[Dict[str, TM1TestConfig], str]:
    """Get all TM1 test configurations from config.ini.

    Returns:
        Tuple of (configs_dict, source) where configs_dict maps instance names
        to TM1TestConfig objects. If empty, source contains the reason.
    """
    config_path = _get_config_path()

    if config_path is None:
        return (
            {},
            "No test configuration found. Set RUSHTI_TEST_CONFIG env var or create tests/config.ini",
        )

    configs = _load_all_configs_from_file(config_path)
    if configs:
        return configs, config_path

    return {}, f"Config file found at {config_path} but no valid TM1 sections"


# =============================================================================
# Pytest Configuration
# =============================================================================


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "requires_tm1: marks tests as requiring TM1 connection")
    config.addinivalue_line(
        "markers", "requires_multi_instance: marks tests as requiring multiple TM1 instances"
    )
    config.addinivalue_line("markers", "slow: marks tests as slow running")
    config.addinivalue_line("markers", "integration: marks tests as integration tests")


def pytest_collection_modifyitems(config, items):
    """Automatically skip TM1 tests if no configuration available."""
    tm1_config, source = get_test_tm1_config()
    all_configs, _ = get_all_test_tm1_configs()

    if tm1_config is None:
        skip_tm1 = pytest.mark.skip(reason=f"TM1 tests skipped: {source}")
        for item in items:
            if "requires_tm1" in item.keywords:
                item.add_marker(skip_tm1)

    if len(all_configs) < 2:
        skip_multi = pytest.mark.skip(
            reason=f"Multi-instance tests skipped: only {len(all_configs)} instance(s) configured"
        )
        for item in items:
            if "requires_multi_instance" in item.keywords:
                item.add_marker(skip_multi)


# =============================================================================
# Single-Instance Fixtures (backward compatible)
# =============================================================================


@pytest.fixture(scope="session")
def tm1_test_config():
    """Get first TM1 test configuration.

    Returns TM1TestConfig or None if not available.
    """
    config, source = get_test_tm1_config()
    if config:
        print(f"\nTM1 test config loaded from {source}")
    return config


@pytest.fixture(scope="session")
def tm1_available(tm1_test_config):
    """Check if TM1 instance is available for testing.

    Returns True if TM1 config exists and connection succeeds.
    """
    if tm1_test_config is None:
        return False

    try:
        from TM1py import TM1Service

        with TM1Service(**tm1_test_config.to_dict()) as tm1:
            tm1.server.get_server_name()
            return True
    except Exception as e:
        print(f"\nTM1 connection failed: {e}")
        return False


@pytest.fixture(scope="session")
def tm1_services(tm1_test_config, tm1_available):
    """Provide single TM1 service connection for integration tests.

    Returns dict of {instance_name: TM1Service} or empty dict if unavailable.
    """
    if not tm1_available:
        return {}

    from TM1py import TM1Service

    services = {}

    try:
        tm1 = TM1Service(**tm1_test_config.to_dict())
        services[tm1_test_config.instance] = tm1
        yield services
    finally:
        for tm1 in services.values():
            try:
                tm1.logout()
            except Exception:
                pass


# =============================================================================
# Multi-Instance Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def tm1_all_configs():
    """Get all TM1 test configurations.

    Returns dict of {instance_name: TM1TestConfig} for all configured instances.
    """
    configs, source = get_all_test_tm1_configs()
    if configs:
        print(f"\nTM1 configs loaded from {source}: {list(configs.keys())}")
    return configs


@pytest.fixture(scope="session")
def config_ini_path():
    """Get path to the tests/config.ini file.

    Returns the path or None if not found. Works identically for local
    development and CI (where the secret is written to tests/config.ini).
    """
    return _get_config_path()


@pytest.fixture(scope="session")
def tm1_all_services(tm1_all_configs):
    """Provide TM1 service connections for all configured instances.

    Returns dict of {instance_name: TM1Service}. Only includes instances
    that successfully connect. May be empty if no instances are reachable.
    """
    if not tm1_all_configs:
        yield {}
        return

    from TM1py import TM1Service

    services = {}

    for instance, config in tm1_all_configs.items():
        try:
            tm1 = TM1Service(**config.to_dict())
            tm1.server.get_server_name()  # connectivity check
            services[instance] = tm1
            print(f"\n  Connected to {instance}")
        except Exception as e:
            print(f"\n  Failed to connect to {instance}: {e}")

    yield services

    for instance, tm1 in services.items():
        try:
            tm1.logout()
        except Exception:
            pass


@pytest.fixture(scope="session")
def tm1_test_environment(tm1_all_services):
    """Auto-create required TM1 objects on all connected instances.

    Creates (if they don't already exist):
    - rushti.dimension.counter dimension (for expandable task tests)
    - rushti cube and dimensions (for cube-based task tests)
    - }rushti.load.results TI process (for auto-load tests)

    This fixture is NOT autouse — only integration tests that need it
    should request it explicitly or via test class setup.
    """
    if not tm1_all_services:
        return

    try:
        from tests.integration.tm1_setup import setup_tm1_test_objects
    except ImportError:
        # tm1_setup module may not exist yet during development
        return

    tm1_names = get_test_tm1_names()
    for instance, tm1 in tm1_all_services.items():
        try:
            setup_tm1_test_objects(tm1, **tm1_names)
            print(f"\n  TM1 test objects verified on {instance} (names: {tm1_names['cube_name']})")
        except Exception as e:
            print(f"\n  Warning: Failed to setup test objects on {instance}: {e}")


# =============================================================================
# Common Fixtures
# =============================================================================


@pytest.fixture
def temp_dir():
    """Provide a temporary directory that's cleaned up after the test."""
    dirpath = tempfile.mkdtemp()
    yield dirpath
    shutil.rmtree(dirpath, ignore_errors=True)


@pytest.fixture
def temp_file(temp_dir):
    """Factory fixture for creating temporary files."""
    created_files = []

    def _create_temp_file(content: str, filename: str = "test_file.txt") -> str:
        filepath = os.path.join(temp_dir, filename)
        with open(filepath, "w") as f:
            f.write(content)
        created_files.append(filepath)
        return filepath

    yield _create_temp_file
    # Files are cleaned up with temp_dir


@pytest.fixture
def sample_task_content():
    """Provide sample task file content for tests."""
    return 'instance="tm1srv01" process="}bedrock.server.wait" pWaitSec="1"\n'


@pytest.fixture
def sample_json_taskfile():
    """Provide sample JSON taskfile content."""
    return {
        "version": "2.0",
        "metadata": {"workflow": "test-taskfile", "description": "Test taskfile"},
        "tasks": [
            {
                "id": "task1",
                "instance": "tm1srv01",
                "process": "}bedrock.server.wait",
                "parameters": {"pWaitSec": "1"},
            }
        ],
    }
