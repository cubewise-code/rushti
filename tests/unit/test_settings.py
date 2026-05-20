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
    resolve_tm1_instance,
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

    def test_load_settings_stats_dynamodb_fields(self):
        """Test loading DynamoDB-specific stats settings."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".ini", delete=False) as f:
            f.write("[stats]\n")
            f.write("enabled = true\n")
            f.write("backend = dynamodb\n")
            f.write("dynamodb_region = eu-west-1\n")
            f.write("dynamodb_runs_table = custom_runs\n")
            f.write("dynamodb_task_results_table = custom_task_results\n")
            f.write("dynamodb_endpoint_url = http://localhost:4566\n")
            f.flush()
            settings_path = f.name

        try:
            settings = load_settings(settings_path)
            self.assertEqual(settings.stats.backend, "dynamodb")
            self.assertEqual(settings.stats.dynamodb_region, "eu-west-1")
            self.assertEqual(settings.stats.dynamodb_runs_table, "custom_runs")
            self.assertEqual(settings.stats.dynamodb_task_results_table, "custom_task_results")
            self.assertEqual(settings.stats.dynamodb_endpoint_url, "http://localhost:4566")
        finally:
            os.unlink(settings_path)

    def test_load_settings_stats_invalid_backend(self):
        """Test invalid stats backend raises ValueError."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".ini", delete=False) as f:
            f.write("[stats]\n")
            f.write("backend = postgresql\n")
            f.flush()
            settings_path = f.name

        try:
            with self.assertRaises(ValueError) as ctx:
                load_settings(settings_path)
            self.assertIn("backend", str(ctx.exception))
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

    def test_detailed_results_default_is_false(self):
        settings = Settings()
        self.assertFalse(settings.tm1_integration.detailed_results)

    def test_detailed_results_json_overrides_settings(self):
        settings = Settings()
        settings.tm1_integration.detailed_results = False  # ini default
        json_settings = {"detailed_results": True}
        result = get_effective_settings(settings, json_settings=json_settings)
        self.assertTrue(result.tm1_integration.detailed_results)

    def test_detailed_results_cli_overrides_json(self):
        # CLI True must beat JSON False (and vice-versa).
        settings = Settings()
        json_settings = {"detailed_results": False}
        cli_args = {"detailed_results": True}
        result = get_effective_settings(settings, cli_args=cli_args, json_settings=json_settings)
        self.assertTrue(result.tm1_integration.detailed_results)

    def test_detailed_results_cli_none_does_not_override(self):
        # Argparse default for the flag is None when not passed; that must
        # NOT clobber an ini-supplied True.
        settings = Settings()
        settings.tm1_integration.detailed_results = True
        cli_args = {"detailed_results": None}
        result = get_effective_settings(settings, cli_args=cli_args)
        self.assertTrue(result.tm1_integration.detailed_results)


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


class TestTm1InstanceResolution(unittest.TestCase):
    """Per-workflow tm1_instance resolution: precedence chain CLI > taskfile JSON >
    settings.ini tm1_instance > settings.ini default_tm1_instance (deprecated)."""

    def setUp(self):
        # logging.config.fileConfig() in cli.py disables existing loggers by
        # default when invoked by other tests. Re-enable so assertLogs works.
        import logging as _logging

        _logging.getLogger("rushti.settings").disabled = False

    def _write_ini(self, body: str) -> str:
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".ini", delete=False)
        f.write(body)
        f.flush()
        f.close()
        return f.name

    def test_tm1_instance_in_settings_ini_parses(self):
        path = self._write_ini("[tm1_integration]\ntm1_instance = ini_target\n")
        try:
            settings = load_settings(path)
            self.assertEqual(settings.tm1_integration.tm1_instance, "ini_target")
            self.assertIsNone(settings.tm1_integration.default_tm1_instance)
        finally:
            os.unlink(path)

    def test_taskfile_json_overrides_settings_ini(self):
        settings = Settings()
        settings.tm1_integration.tm1_instance = "from_ini"
        result = get_effective_settings(settings, json_settings={"tm1_instance": "from_json"})
        self.assertEqual(result.tm1_integration.tm1_instance, "from_json")

    def test_cli_overrides_taskfile_and_settings_ini(self):
        settings = Settings()
        settings.tm1_integration.tm1_instance = "from_ini"
        json_settings = {"tm1_instance": "from_json"}
        value, source = resolve_tm1_instance("from_cli", settings, json_settings)
        self.assertEqual(value, "from_cli")
        self.assertEqual(source, "cli")

    def test_deprecated_default_alone_is_honoured(self):
        path = self._write_ini("[tm1_integration]\ndefault_tm1_instance = legacy_target\n")
        try:
            with self.assertLogs("rushti.settings", level="WARNING") as cm:
                settings = load_settings(path)
            self.assertEqual(settings.tm1_integration.default_tm1_instance, "legacy_target")
            value, source = resolve_tm1_instance(None, settings)
            self.assertEqual(value, "legacy_target")
            self.assertEqual(source, "settings.default_tm1_instance")
            self.assertTrue(
                any("default_tm1_instance" in m and "deprecated" in m.lower() for m in cm.output)
            )
        finally:
            os.unlink(path)

    def test_canonical_set_alongside_deprecated_does_not_warn(self):
        path = self._write_ini(
            "[tm1_integration]\n"
            "tm1_instance = canon_target\n"
            "default_tm1_instance = legacy_target\n"
        )
        try:
            import logging as _logging

            handler_logs = []

            class _CaptureHandler(_logging.Handler):
                def emit(self, record):
                    handler_logs.append(self.format(record))

            target_logger = _logging.getLogger("rushti.settings")
            handler = _CaptureHandler(level=_logging.WARNING)
            target_logger.addHandler(handler)
            try:
                settings = load_settings(path)
            finally:
                target_logger.removeHandler(handler)

            self.assertEqual(settings.tm1_integration.tm1_instance, "canon_target")
            self.assertEqual(settings.tm1_integration.default_tm1_instance, "legacy_target")
            self.assertFalse(
                any("default_tm1_instance" in m for m in handler_logs),
                f"Did not expect deprecation warning, got: {handler_logs}",
            )
            value, source = resolve_tm1_instance(None, settings)
            self.assertEqual(value, "canon_target")
            self.assertEqual(source, "settings.tm1_instance")
        finally:
            os.unlink(path)

    def test_deprecation_warning_fires_exactly_once_at_load(self):
        path = self._write_ini("[tm1_integration]\ndefault_tm1_instance = legacy_target\n")
        try:
            with self.assertLogs("rushti.settings", level="WARNING") as cm:
                load_settings(path)
            depr_msgs = [m for m in cm.output if "default_tm1_instance" in m]
            self.assertEqual(len(depr_msgs), 1, depr_msgs)
        finally:
            os.unlink(path)

    def test_empty_string_falls_through_to_next_tier(self):
        settings = Settings()
        settings.tm1_integration.tm1_instance = "from_ini"
        # CLI is empty string → treat as unset; should fall through to JSON
        json_settings = {"tm1_instance": "from_json"}
        value, source = resolve_tm1_instance("", settings, json_settings)
        self.assertEqual(value, "from_json")
        self.assertEqual(source, "taskfile")
        # JSON has empty string → fall through to settings.ini
        value, source = resolve_tm1_instance(None, settings, {"tm1_instance": ""})
        self.assertEqual(value, "from_ini")
        self.assertEqual(source, "settings.tm1_instance")

    def test_all_empty_returns_none_for_graceful_warning(self):
        settings = Settings()
        value, source = resolve_tm1_instance(None, settings)
        self.assertIsNone(value)
        self.assertEqual(source, "none")

    def test_resolve_tm1_instance_returns_label_for_each_tier(self):
        # tier 1: CLI
        settings = Settings()
        v, s = resolve_tm1_instance("cli_val", settings, {"tm1_instance": "json_val"})
        settings.tm1_integration.tm1_instance = "ini_val"
        settings.tm1_integration.default_tm1_instance = "depr_val"
        self.assertEqual((v, s), ("cli_val", "cli"))

        # tier 2: taskfile JSON
        v, s = resolve_tm1_instance(None, Settings(), {"tm1_instance": "json_val"})
        self.assertEqual((v, s), ("json_val", "taskfile"))

        # tier 3: settings.tm1_instance
        s_only = Settings()
        s_only.tm1_integration.tm1_instance = "ini_val"
        v, s = resolve_tm1_instance(None, s_only, json_settings=None)
        self.assertEqual((v, s), ("ini_val", "settings.tm1_instance"))

        # tier 4: settings.default_tm1_instance (deprecated)
        s_dep = Settings()
        s_dep.tm1_integration.default_tm1_instance = "depr_val"
        v, s = resolve_tm1_instance(None, s_dep, json_settings=None)
        self.assertEqual((v, s), ("depr_val", "settings.default_tm1_instance"))


class TestJsonSettingsFlowThrough(unittest.TestCase):
    """Spec §12 tests 11–13: taskfile JSON push_results / auto_load_results /
    tm1_instance actually take effect via _apply_json_settings."""

    def test_json_push_results_overrides_settings_ini(self):
        settings = Settings()
        settings.tm1_integration.push_results = False
        result = get_effective_settings(settings, json_settings={"push_results": True})
        self.assertTrue(result.tm1_integration.push_results)

    def test_json_auto_load_results_overrides_settings_ini(self):
        settings = Settings()
        settings.tm1_integration.auto_load_results = False
        result = get_effective_settings(settings, json_settings={"auto_load_results": True})
        self.assertTrue(result.tm1_integration.auto_load_results)

    def test_cli_still_overrides_json_for_tm1_instance(self):
        # CLI takes precedence over JSON in the resolver (tier 1 > tier 2)
        settings = Settings()
        json_settings = {"tm1_instance": "json_val"}
        value, source = resolve_tm1_instance("cli_val", settings, json_settings)
        self.assertEqual(value, "cli_val")
        self.assertEqual(source, "cli")


class TestCubeSourceAsymmetry(unittest.TestCase):
    """Spec §12 tests 14–15: cube-sourced runs have no tier 2 (taskfile JSON);
    settings.tm1_instance (tier 3) still resolves correctly."""

    def test_cube_source_resolves_to_settings_tm1_instance(self):
        """When the cube provides no taskfile-level settings, tier 3 wins."""
        settings = Settings()
        settings.tm1_integration.tm1_instance = "cube_target"
        # Cube taskfile yields TaskfileSettings() → empty dict
        value, source = resolve_tm1_instance(None, settings, json_settings={})
        self.assertEqual(value, "cube_target")
        self.assertEqual(source, "settings.tm1_instance")

    def test_cube_source_with_nothing_configured_returns_none(self):
        """Cube source with no settings.ini tm1_instance → resolver returns
        (None, 'none'); caller logs the graceful warning and skips push."""
        settings = Settings()
        value, source = resolve_tm1_instance(None, settings, json_settings={})
        self.assertIsNone(value)
        self.assertEqual(source, "none")


if __name__ == "__main__":
    unittest.main()
