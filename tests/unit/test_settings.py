"""
Unit tests for settings functionality.
Covers settings.ini loading, validation, and settings precedence.
"""

import os
import tempfile
import unittest
from pathlib import Path

from rushti.settings import (
    load_settings,
    get_effective_settings,
    parse_bool,
    parse_value,
    validate_setting,
    resolve_settings_path,
    Settings,
)


class TestSettingsLoading(unittest.TestCase):
    """Tests for settings.ini loading functionality"""

    def test_load_settings_defaults(self):
        """Test that defaults are returned when no settings.ini exists"""
        settings = load_settings("/nonexistent/path/settings.ini")
        self.assertEqual(settings.defaults.max_workers, 4)
        self.assertEqual(settings.defaults.retries, 0)
        self.assertEqual(settings.defaults.result_file, "")
        self.assertEqual(settings.defaults.mode, "norm")

    def test_load_settings_valid_file(self):
        """Test loading a valid settings.ini file"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".ini", delete=False) as f:
            f.write("[defaults]\n")
            f.write("max_workers = 8\n")
            f.write("retries = 2\n")
            f.write("result_file = custom.csv\n")
            f.write("mode = opt\n")
            f.flush()
            settings_path = f.name

        try:
            settings = load_settings(settings_path)
            self.assertEqual(settings.defaults.max_workers, 8)
            self.assertEqual(settings.defaults.retries, 2)
            self.assertEqual(settings.defaults.result_file, "custom.csv")
            self.assertEqual(settings.defaults.mode, "opt")
        finally:
            os.unlink(settings_path)

    def test_load_settings_invalid_value(self):
        """Test that invalid values raise ValueError"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".ini", delete=False) as f:
            f.write("[defaults]\n")
            f.write("max_workers = abc\n")  # Invalid integer
            f.flush()
            settings_path = f.name

        try:
            with self.assertRaises(ValueError) as ctx:
                load_settings(settings_path)
            self.assertIn("Invalid integer value", str(ctx.exception))
        finally:
            os.unlink(settings_path)

    def test_load_settings_all_sections(self):
        """Test loading settings with multiple sections"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".ini", delete=False) as f:
            f.write("[defaults]\n")
            f.write("max_workers = 10\n")
            f.write("\n")
            f.write("[exclusive_mode]\n")
            f.write("enabled = true\n")
            f.write("polling_interval = 45\n")
            f.write("timeout = 900\n")
            f.flush()
            settings_path = f.name

        try:
            settings = load_settings(settings_path)
            self.assertEqual(settings.defaults.max_workers, 10)
            self.assertTrue(settings.exclusive_mode.enabled)
            self.assertEqual(settings.exclusive_mode.polling_interval, 45)
            self.assertEqual(settings.exclusive_mode.timeout, 900)
        finally:
            os.unlink(settings_path)


class TestSettingsValidation(unittest.TestCase):
    """Tests for settings validation"""

    def test_parse_bool_true_values(self):
        """Test parsing boolean true values"""
        for val in ["true", "True", "TRUE", "yes", "1", "on"]:
            self.assertTrue(parse_bool(val))

    def test_parse_bool_false_values(self):
        """Test parsing boolean false values"""
        for val in ["false", "False", "FALSE", "no", "0", "off"]:
            self.assertFalse(parse_bool(val))

    def test_parse_bool_invalid_value(self):
        """Test that invalid boolean raises ValueError"""
        with self.assertRaises(ValueError):
            parse_bool("maybe")

    def test_parse_value_int(self):
        """Test parsing integer values"""
        self.assertEqual(parse_value("42", int), 42)
        self.assertEqual(parse_value("0", int), 0)

    def test_parse_value_int_invalid(self):
        """Test that invalid integer raises ValueError"""
        with self.assertRaises(ValueError):
            parse_value("not_a_number", int)

    def test_validate_setting_negative_workers(self):
        """Test that negative max_workers raises ValueError"""
        with self.assertRaises(ValueError):
            validate_setting("defaults", "max_workers", -1)

    def test_validate_setting_zero_workers(self):
        """Test that zero max_workers raises ValueError"""
        with self.assertRaises(ValueError):
            validate_setting("defaults", "max_workers", 0)

    def test_validate_setting_valid_workers(self):
        """Test that valid max_workers passes validation"""
        validate_setting("defaults", "max_workers", 8)  # Should not raise


class TestSettingsPrecedence(unittest.TestCase):
    """Tests for settings merge precedence (CLI > JSON > settings.ini > defaults)"""

    def test_cli_overrides_settings(self):
        """Test that CLI args override settings.ini values"""
        settings = Settings()
        settings.defaults.max_workers = 8  # Simulate settings.ini value

        cli_args = {"max_workers": 16}
        result = get_effective_settings(settings, cli_args=cli_args)

        # CLI override should be applied via the function
        self.assertEqual(result.defaults.max_workers, 16)

    def test_json_overrides_settings(self):
        """Test that JSON settings override settings.ini values"""
        settings = Settings()
        settings.defaults.retries = 1  # Simulate settings.ini value

        json_settings = {"retries": 3}
        result = get_effective_settings(settings, json_settings=json_settings)

        self.assertEqual(result.defaults.retries, 3)

    def test_cli_overrides_json(self):
        """Test that CLI args override JSON settings"""
        settings = Settings()

        json_settings = {"max_workers": 10}
        cli_args = {"max_workers": 20}
        result = get_effective_settings(settings, cli_args=cli_args, json_settings=json_settings)

        # CLI should win
        self.assertEqual(result.defaults.max_workers, 20)


class TestConfigPathResolution(unittest.TestCase):
    """Tests for configuration path resolution with fallback"""

    def test_resolve_settings_path_prefers_config_dir(self):
        """Test that config/ directory is preferred over root"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create both config/ and root settings.ini
            config_dir = tmpdir / "config"
            config_dir.mkdir()
            (config_dir / "settings.ini").write_text("[defaults]\nmax_workers = 8\n")
            (tmpdir / "settings.ini").write_text("[defaults]\nmax_workers = 4\n")

            resolved_path, is_legacy = resolve_settings_path(tmpdir)
            self.assertEqual(resolved_path, config_dir / "settings.ini")
            self.assertFalse(is_legacy)

    def test_resolve_settings_path_falls_back_to_root(self):
        """Test fallback to root directory when config/ doesn't have file"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Only create root settings.ini
            (tmpdir / "settings.ini").write_text("[defaults]\nmax_workers = 4\n")

            resolved_path, is_legacy = resolve_settings_path(tmpdir)
            self.assertEqual(resolved_path, tmpdir / "settings.ini")
            self.assertTrue(is_legacy)

    def test_resolve_settings_path_returns_new_path_when_neither_exists(self):
        """Test returns config/ path when neither location has file"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            resolved_path, is_legacy = resolve_settings_path(tmpdir)
            self.assertEqual(resolved_path, tmpdir / "config" / "settings.ini")
            self.assertFalse(is_legacy)


if __name__ == "__main__":
    unittest.main()
