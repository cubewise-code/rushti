"""Integration tests for settings module with real configuration files.

Tests settings module by loading actual configuration files and verifying
that settings cascade and merge correctly across different sources.
"""

import tempfile
import unittest
from pathlib import Path

from rushti.settings import (
    get_effective_settings,
    load_settings,
    resolve_settings_path,
)


class TestSettingsFileLoading(unittest.TestCase):
    """Integration tests for loading settings from files."""

    def setUp(self):
        """Create temporary directory for test config files."""
        self.temp_dir = tempfile.mkdtemp()
        self.config_dir = Path(self.temp_dir) / "config"
        self.config_dir.mkdir()

    def tearDown(self):
        """Clean up temporary files."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_load_settings_from_config_directory(self):
        """Test loading settings from config/ directory."""
        settings_path = self.config_dir / "settings.ini"
        settings_path.write_text("""
[defaults]
max_workers = 8
retries = 3
result_file = custom_results.csv

[optimization]
lookback_runs = 20
        """)

        settings = load_settings(str(settings_path))

        self.assertEqual(settings.defaults.max_workers, 8)
        self.assertEqual(settings.defaults.retries, 3)
        self.assertEqual(settings.defaults.result_file, "custom_results.csv")
        self.assertEqual(settings.optimization.lookback_runs, 20)

    def test_load_settings_with_all_sections(self):
        """Test loading comprehensive settings file with all sections."""
        settings_path = self.config_dir / "settings.ini"
        settings_path.write_text("""
[defaults]
max_workers = 6
retries = 2
result_file = results.csv
mode = opt

[optimization]
lookback_runs = 15
time_of_day_weighting = true
min_samples = 5
cache_duration_hours = 48

[stats]
enabled = true
db_path = /tmp/rushti/custom_stats.db
retention_days = 60

[resume]
enabled = true
checkpoint_interval = 120
checkpoint_dir = ./checkpoints
auto_resume = true

[exclusive_mode]
enabled = true
polling_interval = 60
timeout = 1200
        """)

        settings = load_settings(str(settings_path))

        # Verify all sections
        self.assertEqual(settings.defaults.max_workers, 6)
        self.assertEqual(settings.defaults.mode, "opt")
        self.assertEqual(settings.optimization.lookback_runs, 15)
        self.assertTrue(settings.optimization.time_of_day_weighting)
        self.assertTrue(settings.stats.enabled)
        self.assertEqual(settings.stats.db_path, "/tmp/rushti/custom_stats.db")
        self.assertEqual(settings.stats.retention_days, 60)
        self.assertTrue(settings.resume.enabled)
        self.assertEqual(settings.resume.checkpoint_interval, 120)
        self.assertTrue(settings.exclusive_mode.enabled)
        self.assertEqual(settings.exclusive_mode.timeout, 1200)

    def test_effective_settings_cli_overrides(self):
        """Test that CLI arguments override settings file."""
        settings_path = self.config_dir / "settings.ini"
        settings_path.write_text("""
[defaults]
max_workers = 4
retries = 1
        """)

        base_settings = load_settings(str(settings_path))

        # Apply CLI overrides
        cli_args = {"max_workers": 12, "retries": 5}

        effective = get_effective_settings(base_settings, cli_args=cli_args)

        # CLI values should override file values
        self.assertEqual(effective.defaults.max_workers, 12)
        self.assertEqual(effective.defaults.retries, 5)

    def test_effective_settings_json_overrides(self):
        """Test that JSON task file settings override settings.ini."""
        settings_path = self.config_dir / "settings.ini"
        settings_path.write_text("""
[defaults]
max_workers = 4
retries = 1
        """)

        base_settings = load_settings(str(settings_path))

        # Apply JSON overrides
        json_settings = {"max_workers": 8, "exclusive": True}

        effective = get_effective_settings(base_settings, json_settings=json_settings)

        # JSON values should override file values
        self.assertEqual(effective.defaults.max_workers, 8)
        self.assertTrue(effective.exclusive_mode.enabled)

    def test_effective_settings_precedence(self):
        """Test full precedence chain: CLI > JSON > settings.ini > defaults."""
        settings_path = self.config_dir / "settings.ini"
        settings_path.write_text("""
[defaults]
max_workers = 4
retries = 2
result_file = file.csv
        """)

        base_settings = load_settings(str(settings_path))

        # Apply JSON overrides (medium priority)
        json_settings = {"max_workers": 8, "retries": 3}

        # Apply CLI overrides (highest priority)
        cli_args = {"max_workers": 16}

        effective = get_effective_settings(
            base_settings, cli_args=cli_args, json_settings=json_settings
        )

        # Check precedence
        self.assertEqual(effective.defaults.max_workers, 16)  # CLI wins
        self.assertEqual(effective.defaults.retries, 3)  # JSON wins over settings.ini
        self.assertEqual(effective.defaults.result_file, "file.csv")  # settings.ini (no override)

    def test_resolve_settings_path_new_location(self):
        """Test resolving settings path prefers new config/ location."""
        # Create settings in new location
        settings_path = self.config_dir / "settings.ini"
        settings_path.write_text("[defaults]\nmax_workers = 8")

        resolved_path, is_legacy = resolve_settings_path(Path(self.temp_dir))

        self.assertEqual(resolved_path, settings_path)
        self.assertFalse(is_legacy)

    def test_resolve_settings_path_legacy_fallback(self):
        """Test resolving settings path falls back to legacy location."""
        # Create settings in legacy location only
        legacy_path = Path(self.temp_dir) / "settings.ini"
        legacy_path.write_text("[defaults]\nmax_workers = 4")

        resolved_path, is_legacy = resolve_settings_path(Path(self.temp_dir))

        self.assertEqual(resolved_path, legacy_path)
        self.assertTrue(is_legacy)

    def test_resolve_settings_path_prefers_new_over_legacy(self):
        """Test that new location is preferred even if legacy exists."""
        # Create settings in both locations
        new_path = self.config_dir / "settings.ini"
        new_path.write_text("[defaults]\nmax_workers = 8")

        legacy_path = Path(self.temp_dir) / "settings.ini"
        legacy_path.write_text("[defaults]\nmax_workers = 4")

        resolved_path, is_legacy = resolve_settings_path(Path(self.temp_dir))

        # Should prefer new location
        self.assertEqual(resolved_path, new_path)
        self.assertFalse(is_legacy)

    def test_load_settings_invalid_values(self):
        """Test loading settings with invalid values raises errors."""
        settings_path = self.config_dir / "settings.ini"
        settings_path.write_text("""
[defaults]
max_workers = -5
        """)

        with self.assertRaises(ValueError) as ctx:
            load_settings(str(settings_path))

        self.assertIn("max_workers", str(ctx.exception))

    def test_load_settings_invalid_enum_values(self):
        """Test loading settings with invalid enum values."""
        settings_path = self.config_dir / "settings.ini"
        settings_path.write_text("""
[defaults]
mode = invalid_mode
        """)

        with self.assertRaises(ValueError) as ctx:
            load_settings(str(settings_path))

        self.assertIn("mode", str(ctx.exception))

    def test_load_settings_unknown_section_warning(self):
        """Test that unknown sections generate warnings but don't fail."""
        settings_path = self.config_dir / "settings.ini"
        settings_path.write_text("""
[defaults]
max_workers = 4

[unknown_section]
some_key = some_value
        """)

        # Should not raise error, just log warning
        settings = load_settings(str(settings_path))

        # Defaults should still load correctly
        self.assertEqual(settings.defaults.max_workers, 4)

    def test_load_settings_unknown_key_warning(self):
        """Test that unknown keys generate warnings but don't fail."""
        settings_path = self.config_dir / "settings.ini"
        settings_path.write_text("""
[defaults]
max_workers = 4
unknown_key = value
        """)

        # Should not raise error, just log warning
        settings = load_settings(str(settings_path))

        # Known keys should still load correctly
        self.assertEqual(settings.defaults.max_workers, 4)


if __name__ == "__main__":
    unittest.main()
