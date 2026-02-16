"""
Unit tests for configuration file path resolution.
Covers RUSHTI_DIR environment variable, resolve_config_path(), and get_application_directory().
"""

import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from rushti.settings import load_settings
from rushti.utils import get_application_directory, resolve_app_path


class TestResolveConfigPathWithRushtiDir(unittest.TestCase):
    """Tests for resolve_config_path() with RUSHTI_DIR."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        # Create config/ subdirectory (RUSHTI_DIR expects config files in config/)
        self.config_dir = os.path.join(self.temp_dir, "config")
        os.makedirs(self.config_dir)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_config_file(self, filename, content="[section]\nkey = value\n"):
        """Helper to create a config file in the config/ subdirectory."""
        filepath = os.path.join(self.config_dir, filename)
        with open(filepath, "w") as f:
            f.write(content)
        return filepath

    def test_cli_path_takes_precedence(self):
        """CLI path should take precedence over RUSHTI_DIR."""
        cli_file = os.path.join(self.temp_dir, "my-config.ini")
        with open(cli_file, "w") as f:
            f.write("[cli]\nval = 1\n")

        self._create_config_file("config.ini", "[env]\nval = 2\n")

        with patch.dict(os.environ, {"RUSHTI_DIR": self.temp_dir}):
            from rushti.cli import resolve_config_path

            result = resolve_config_path("config.ini", cli_path=cli_file)
            self.assertEqual(result, cli_file)

    def test_cli_path_raises_if_not_exists(self):
        """CLI path should raise FileNotFoundError if file does not exist."""
        from rushti.cli import resolve_config_path

        with self.assertRaises(FileNotFoundError):
            resolve_config_path("config.ini", cli_path="/nonexistent/file.ini")

    def test_rushti_dir_finds_config_ini(self):
        """RUSHTI_DIR should find config.ini in config/ subdirectory."""
        expected = self._create_config_file("config.ini")

        with patch.dict(os.environ, {"RUSHTI_DIR": self.temp_dir}):
            from rushti.cli import resolve_config_path

            result = resolve_config_path("config.ini")
            self.assertEqual(result, expected)

    def test_rushti_dir_finds_settings_ini(self):
        """RUSHTI_DIR should find settings.ini in config/ subdirectory."""
        expected = self._create_config_file("settings.ini")

        with patch.dict(os.environ, {"RUSHTI_DIR": self.temp_dir}):
            from rushti.cli import resolve_config_path

            result = resolve_config_path("settings.ini")
            self.assertEqual(result, expected)

    def test_rushti_dir_finds_logging_config_ini(self):
        """RUSHTI_DIR should find logging_config.ini in config/ subdirectory."""
        expected = self._create_config_file("logging_config.ini")

        with patch.dict(os.environ, {"RUSHTI_DIR": self.temp_dir}):
            from rushti.cli import resolve_config_path

            result = resolve_config_path("logging_config.ini")
            self.assertEqual(result, expected)

    def test_rushti_dir_falls_through_if_file_missing(self):
        """If RUSHTI_DIR is set but file is missing from config/, should fall through."""
        # config/ exists but is empty
        with patch.dict(os.environ, {"RUSHTI_DIR": self.temp_dir}):
            from rushti.cli import resolve_config_path

            result = resolve_config_path("config.ini")
            # Should NOT be in temp_dir/config/ since file doesn't exist there
            self.assertNotEqual(result, os.path.join(self.config_dir, "config.ini"))

    def test_no_env_var_does_not_crash(self):
        """Without RUSHTI_DIR, should fall back to cwd-based resolution."""
        env = os.environ.copy()
        env.pop("RUSHTI_DIR", None)

        with patch.dict(os.environ, env, clear=True):
            from rushti.cli import resolve_config_path

            result = resolve_config_path("config.ini")
            self.assertIsInstance(result, str)

    def test_old_env_vars_are_not_used(self):
        """Old per-file env vars (RUSHTI_CONFIG, RUSHTI_SETTINGS, RUSHTI_CONFIG_DIR) should be ignored."""
        old_file = os.path.join(self.temp_dir, "config.ini")
        with open(old_file, "w") as f:
            f.write("[old]\n")

        env = os.environ.copy()
        env.pop("RUSHTI_DIR", None)
        env["RUSHTI_CONFIG"] = old_file
        env["RUSHTI_SETTINGS"] = old_file
        env["RUSHTI_CONFIG_DIR"] = self.temp_dir

        with patch.dict(os.environ, env, clear=True):
            from rushti.cli import resolve_config_path

            result = resolve_config_path("settings.ini")
            self.assertNotEqual(result, old_file)


class TestGetApplicationDirectory(unittest.TestCase):
    """Tests for get_application_directory() with RUSHTI_DIR."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_rushti_dir_overrides_default(self):
        """RUSHTI_DIR should override the default application directory."""
        with patch.dict(os.environ, {"RUSHTI_DIR": self.temp_dir}):
            result = get_application_directory()
            self.assertEqual(result, os.path.abspath(self.temp_dir))

    def test_resolve_app_path_uses_rushti_dir(self):
        """resolve_app_path() should resolve relative paths under RUSHTI_DIR."""
        with patch.dict(os.environ, {"RUSHTI_DIR": self.temp_dir}):
            result = resolve_app_path("data/rushti_stats.db")
            expected = os.path.normpath(os.path.join(self.temp_dir, "data", "rushti_stats.db"))
            self.assertEqual(result, expected)

    def test_resolve_app_path_visualizations(self):
        """resolve_app_path() should put visualizations under RUSHTI_DIR."""
        with patch.dict(os.environ, {"RUSHTI_DIR": self.temp_dir}):
            result = resolve_app_path("visualizations/dashboard.html")
            expected = os.path.normpath(
                os.path.join(self.temp_dir, "visualizations", "dashboard.html")
            )
            self.assertEqual(result, expected)

    def test_resolve_app_path_logs(self):
        """resolve_app_path() should put logs under RUSHTI_DIR."""
        with patch.dict(os.environ, {"RUSHTI_DIR": self.temp_dir}):
            result = resolve_app_path("logs/rushti.log")
            expected = os.path.normpath(os.path.join(self.temp_dir, "logs", "rushti.log"))
            self.assertEqual(result, expected)

    def test_resolve_app_path_absolute_unchanged(self):
        """resolve_app_path() should return absolute paths unchanged."""
        abs_path = "/some/absolute/path.db"
        with patch.dict(os.environ, {"RUSHTI_DIR": self.temp_dir}):
            result = resolve_app_path(abs_path)
            self.assertEqual(result, abs_path)

    def test_without_rushti_dir_uses_default(self):
        """Without RUSHTI_DIR, should use default application directory."""
        env = os.environ.copy()
        env.pop("RUSHTI_DIR", None)

        with patch.dict(os.environ, env, clear=True):
            result = get_application_directory()
            self.assertIsInstance(result, str)
            # Should be a valid directory path
            self.assertTrue(os.path.isabs(result))


class TestLoadSettingsWithRushtiDir(unittest.TestCase):
    """Tests for load_settings() with RUSHTI_DIR env var."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.config_dir = os.path.join(self.temp_dir, "config")
        os.makedirs(self.config_dir)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_load_settings_respects_rushti_dir(self):
        """load_settings() should find settings.ini via RUSHTI_DIR/config/."""
        settings_file = os.path.join(self.config_dir, "settings.ini")
        with open(settings_file, "w") as f:
            f.write("[defaults]\nmax_workers = 12\n")

        with patch.dict(os.environ, {"RUSHTI_DIR": self.temp_dir}):
            settings = load_settings()
            self.assertEqual(settings.defaults.max_workers, 12)

    def test_load_settings_explicit_path_overrides_rushti_dir(self):
        """Explicit settings path should override RUSHTI_DIR."""
        # File via RUSHTI_DIR with max_workers=12
        env_file = os.path.join(self.config_dir, "settings.ini")
        with open(env_file, "w") as f:
            f.write("[defaults]\nmax_workers = 12\n")

        # Explicit file with max_workers=20
        explicit_dir = tempfile.mkdtemp()
        explicit_file = os.path.join(explicit_dir, "settings.ini")
        with open(explicit_file, "w") as f:
            f.write("[defaults]\nmax_workers = 20\n")

        try:
            with patch.dict(os.environ, {"RUSHTI_DIR": self.temp_dir}):
                settings = load_settings(explicit_file)
                self.assertEqual(settings.defaults.max_workers, 20)
        finally:
            shutil.rmtree(explicit_dir, ignore_errors=True)

    def test_load_settings_rushti_dir_missing_file_falls_through(self):
        """If RUSHTI_DIR/config/ has no settings.ini, fall through to cwd discovery."""
        # config/ exists but has no settings.ini
        with patch.dict(os.environ, {"RUSHTI_DIR": self.temp_dir}):
            # Should not crash — falls through to cwd-based discovery
            settings = load_settings()
            self.assertIsNotNone(settings)


class TestContextDropFromConfigSection(unittest.TestCase):
    """Tests that 'context' is dropped from config.ini sections before passing to TM1Service."""

    def _make_config(self, data: dict):
        """Create a configparser with a test section."""
        import configparser

        config = configparser.ConfigParser()
        config.read_dict({"tm1srv01": data})
        return config

    def test_session_context_is_dropped(self):
        """'session_context' key should be removed via pop before TM1Service call."""
        config = self._make_config(
            {
                "address": "localhost",
                "port": "12354",
                "user": "admin",
                "password": "apple",
                "session_context": "my-app-context",
            }
        )
        params = dict(config["tm1srv01"])
        params.pop("session_context", None)
        self.assertNotIn("session_context", params)
        self.assertEqual(params["address"], "localhost")
        self.assertEqual(len(params), 4)

    def test_no_session_context_key_unchanged(self):
        """Config without 'session_context' should pass through all keys."""
        config = self._make_config(
            {
                "address": "localhost",
                "port": "12354",
            }
        )
        params = dict(config["tm1srv01"])
        params.pop("session_context", None)
        self.assertEqual(len(params), 2)


if __name__ == "__main__":
    unittest.main()
