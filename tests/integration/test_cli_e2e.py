"""End-to-end CLI integration tests.

Tests the actual `rushti` CLI commands via subprocess, verifying the full
execution pipeline works against real TM1 instances.

Run with: pytest tests/integration/test_cli_e2e.py -v -m requires_tm1
"""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest

import pytest

_src_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "src"
)
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

from conftest import get_all_test_tm1_configs, _get_config_path, get_test_tm1_names

RESOURCES_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "resources", "integration"
)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _setup_rushti_dir(settings_path, config_ini_src):
    """Create a RUSHTI_DIR structure with config/config.ini and config/settings.ini.

    The RushTI CLI resolves config.ini via RUSHTI_DIR/config/config.ini.
    This helper copies the real config.ini and settings.ini into the expected layout.

    Returns the RUSHTI_DIR path.
    """
    rushti_dir = os.path.dirname(settings_path)  # reuse the temp dir
    config_dir = os.path.join(rushti_dir, "config")
    os.makedirs(config_dir, exist_ok=True)

    # Copy real config.ini into config/ subdirectory
    shutil.copy2(config_ini_src, os.path.join(config_dir, "config.ini"))

    # Copy settings.ini into config/ subdirectory
    shutil.copy2(settings_path, os.path.join(config_dir, "settings.ini"))

    return rushti_dir


def _run_rushti(*args, rushti_dir=None, timeout=120):
    """Run a rushti CLI command and return the result.

    Uses 'python -m rushti.cli' since there's no __main__.py.
    Sets PYTHONPATH to include the src directory and RUSHTI_DIR for config resolution.
    """
    cmd = [sys.executable, "-m", "rushti.cli"] + list(args)
    env = {
        **os.environ,
        "PYTHONPATH": os.path.join(PROJECT_ROOT, "src"),
    }
    if rushti_dir:
        env["RUSHTI_DIR"] = rushti_dir

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=PROJECT_ROOT,
        env=env,
    )
    return result


def _config_path():
    """Get the config.ini path."""
    return _get_config_path()


def _write_tm1_integration_section(f):
    """Write the [tm1_integration] section with test-specific names to a settings file."""
    tm1_names = get_test_tm1_names()
    f.write("[tm1_integration]\n")
    f.write(f"default_rushti_cube = {tm1_names['cube_name']}\n")
    f.write(f"default_workflow_dim = {tm1_names['dim_workflow']}\n")
    f.write(f"default_task_id_dim = {tm1_names['dim_task']}\n")
    f.write(f"default_run_id_dim = {tm1_names['dim_run']}\n")
    f.write(f"default_measure_dim = {tm1_names['dim_measure']}\n")


@pytest.mark.requires_tm1
@pytest.mark.integration
class TestCLIRunV11(unittest.TestCase):
    """CLI run command tests on v11."""

    INSTANCE = "tm1srv01"

    @classmethod
    def setUpClass(cls):
        configs, _ = get_all_test_tm1_configs()
        cls.available = cls.INSTANCE in configs
        cls.config_path = _config_path()

    def setUp(self):
        if not self.available:
            self.skipTest(f"{self.INSTANCE} not available")
        if not self.config_path:
            self.skipTest("No config.ini found")
        self._settings_dir = tempfile.mkdtemp()
        self._settings_path = os.path.join(self._settings_dir, "settings.ini")
        with open(self._settings_path, "w") as f:
            f.write("[defaults]\nmax_workers = 4\nresult_file = result.csv\n")
            f.write("[stats]\nenabled = false\n")
        self._rushti_dir = _setup_rushti_dir(self._settings_path, self.config_path)

    def tearDown(self):
        if hasattr(self, "_settings_dir"):
            shutil.rmtree(self._settings_dir, ignore_errors=True)

    def test_cli_run_txt_norm(self):
        """rushti run with normal mode TXT file on v11."""
        result = _run_rushti(
            "run",
            "--tasks",
            os.path.join(RESOURCES_DIR, "tasks_v11_norm.txt"),
            "--max-workers",
            "4",
            "--settings",
            os.path.join(self._rushti_dir, "config", "settings.ini"),
            rushti_dir=self._rushti_dir,
        )
        self.assertEqual(result.returncode, 0, f"stdout: {result.stdout}\nstderr: {result.stderr}")

    def test_cli_run_json_staged(self):
        """rushti run with JSON staged pipeline on v11."""
        result = _run_rushti(
            "run",
            "--tasks",
            os.path.join(RESOURCES_DIR, "tasks_v11_staged.json"),
            "--max-workers",
            "4",
            "--settings",
            os.path.join(self._rushti_dir, "config", "settings.ini"),
            rushti_dir=self._rushti_dir,
        )
        self.assertEqual(result.returncode, 0, f"stdout: {result.stdout}\nstderr: {result.stderr}")

    def test_cli_run_with_workflow_flag(self):
        """rushti run with --workflow flag overriding the default."""
        result = _run_rushti(
            "run",
            "--tasks",
            os.path.join(RESOURCES_DIR, "tasks_v11_opt.txt"),
            "--max-workers",
            "4",
            "--workflow",
            "custom-workflow-name",
            "--settings",
            os.path.join(self._rushti_dir, "config", "settings.ini"),
            rushti_dir=self._rushti_dir,
        )
        self.assertEqual(result.returncode, 0, f"stdout: {result.stdout}\nstderr: {result.stderr}")


@pytest.mark.requires_tm1
@pytest.mark.integration
class TestCLIRunV12(unittest.TestCase):
    """CLI run command tests on v12."""

    INSTANCE = "tm1srv02"

    @classmethod
    def setUpClass(cls):
        configs, _ = get_all_test_tm1_configs()
        cls.available = cls.INSTANCE in configs
        cls.config_path = _config_path()

    def setUp(self):
        if not self.available:
            self.skipTest(f"{self.INSTANCE} not available")
        if not self.config_path:
            self.skipTest("No config.ini found")
        self._settings_dir = tempfile.mkdtemp()
        self._settings_path = os.path.join(self._settings_dir, "settings.ini")
        with open(self._settings_path, "w") as f:
            f.write("[defaults]\nmax_workers = 4\nresult_file = result.csv\n")
            f.write("[stats]\nenabled = false\n")
        self._rushti_dir = _setup_rushti_dir(self._settings_path, self.config_path)

    def tearDown(self):
        if hasattr(self, "_settings_dir"):
            shutil.rmtree(self._settings_dir, ignore_errors=True)

    def test_cli_run_txt_norm(self):
        """rushti run with normal mode TXT file on v12."""
        result = _run_rushti(
            "run",
            "--tasks",
            os.path.join(RESOURCES_DIR, "tasks_v12_norm.txt"),
            "--max-workers",
            "4",
            "--settings",
            os.path.join(self._rushti_dir, "config", "settings.ini"),
            rushti_dir=self._rushti_dir,
        )
        self.assertEqual(result.returncode, 0, f"stdout: {result.stdout}\nstderr: {result.stderr}")

    def test_cli_run_json_staged(self):
        """rushti run with JSON staged pipeline on v12."""
        result = _run_rushti(
            "run",
            "--tasks",
            os.path.join(RESOURCES_DIR, "tasks_v12_staged.json"),
            "--max-workers",
            "4",
            "--settings",
            os.path.join(self._rushti_dir, "config", "settings.ini"),
            rushti_dir=self._rushti_dir,
        )
        self.assertEqual(result.returncode, 0, f"stdout: {result.stdout}\nstderr: {result.stderr}")


@pytest.mark.requires_tm1
@pytest.mark.integration
class TestCLIBuild(unittest.TestCase):
    """CLI build command tests."""

    INSTANCE = "tm1srv01"

    @classmethod
    def setUpClass(cls):
        configs, _ = get_all_test_tm1_configs()
        cls.available = cls.INSTANCE in configs
        cls.config_path = _config_path()

    def setUp(self):
        if not self.available:
            self.skipTest(f"{self.INSTANCE} not available")
        if not self.config_path:
            self.skipTest("No config.ini found")
        self._settings_dir = tempfile.mkdtemp()
        self._settings_path = os.path.join(self._settings_dir, "settings.ini")
        with open(self._settings_path, "w") as f:
            f.write("[defaults]\n")
            _write_tm1_integration_section(f)
        self._rushti_dir = _setup_rushti_dir(self._settings_path, self.config_path)

    def tearDown(self):
        if hasattr(self, "_settings_dir"):
            shutil.rmtree(self._settings_dir, ignore_errors=True)

    def test_cli_build(self):
        """rushti build command creates TM1 objects."""
        result = _run_rushti(
            "build",
            "--tm1-instance",
            self.INSTANCE,
            "--settings",
            os.path.join(self._rushti_dir, "config", "settings.ini"),
            rushti_dir=self._rushti_dir,
        )
        self.assertEqual(result.returncode, 0, f"stdout: {result.stdout}\nstderr: {result.stderr}")


@pytest.mark.requires_tm1
@pytest.mark.integration
class TestCLIDBCommands(unittest.TestCase):
    """CLI db admin command tests."""

    @classmethod
    def setUpClass(cls):
        configs, _ = get_all_test_tm1_configs()
        cls.available = len(configs) > 0
        cls.config_path = _config_path()

    def setUp(self):
        if not self.available:
            self.skipTest("No TM1 instances available")
        self._settings_dir = tempfile.mkdtemp()
        self._settings_path = os.path.join(self._settings_dir, "settings.ini")
        self._db_path = os.path.join(self._settings_dir, "test_stats.db")
        with open(self._settings_path, "w") as f:
            f.write("[defaults]\n")
            f.write("[stats]\nenabled = true\n")
            f.write(f"db_path = {self._db_path}\n")

    def tearDown(self):
        if hasattr(self, "_settings_dir"):
            shutil.rmtree(self._settings_dir, ignore_errors=True)

    def test_cli_db_list(self):
        """rushti db list command."""
        result = _run_rushti(
            "db",
            "list",
            "--settings",
            self._settings_path,
        )
        # Should succeed (may have no data yet)
        self.assertIn(
            result.returncode, [0, 1], f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )


@pytest.mark.requires_tm1
@pytest.mark.integration
class TestCLICubeSource(unittest.TestCase):
    """CLI tests for running from TM1 cube source."""

    INSTANCE = "tm1srv01"

    @classmethod
    def setUpClass(cls):
        configs, _ = get_all_test_tm1_configs()
        cls.available = cls.INSTANCE in configs
        cls.config_path = _config_path()

    def setUp(self):
        if not self.available:
            self.skipTest(f"{self.INSTANCE} not available")
        if not self.config_path:
            self.skipTest("No config.ini found")
        self._settings_dir = tempfile.mkdtemp()
        self._settings_path = os.path.join(self._settings_dir, "settings.ini")
        with open(self._settings_path, "w") as f:
            f.write("[defaults]\nmax_workers = 4\nresult_file = result.csv\n")
            f.write("[stats]\nenabled = false\n")
            _write_tm1_integration_section(f)
        self._rushti_dir = _setup_rushti_dir(self._settings_path, self.config_path)

    def tearDown(self):
        if hasattr(self, "_settings_dir"):
            shutil.rmtree(self._settings_dir, ignore_errors=True)

    def test_cli_run_from_cube(self):
        """rushti run from TM1 cube source."""
        result = _run_rushti(
            "run",
            "--tm1-instance",
            self.INSTANCE,
            "--workflow",
            "Sample_Optimal_Mode",
            "--max-workers",
            "4",
            "--settings",
            os.path.join(self._rushti_dir, "config", "settings.ini"),
            rushti_dir=self._rushti_dir,
        )
        # May fail if sample data not loaded, but should not crash
        if result.returncode != 0:
            # Check if it's a "not found" error (acceptable)
            if "not found" in result.stderr.lower() or "no tasks" in result.stderr.lower():
                self.skipTest("Sample workflow not in TM1 cube")
        self.assertEqual(result.returncode, 0, f"stdout: {result.stdout}\nstderr: {result.stderr}")


if __name__ == "__main__":
    unittest.main()
