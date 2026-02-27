"""
Unit tests for logging config path resolution.
Covers _resolve_logging_config() which pre-processes logging_config.ini
to resolve relative file handler paths against the application directory.
"""

import os
import shutil
import tempfile
import unittest
from unittest.mock import patch


class TestResolveLoggingConfig(unittest.TestCase):
    """Tests for _resolve_logging_config()."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_logging_config(self, content):
        """Write a logging config ini file and return its path."""
        filepath = os.path.join(self.temp_dir, "logging_config.ini")
        with open(filepath, "w") as f:
            f.write(content)
        return filepath

    def test_relative_path_is_resolved(self):
        """Relative log file path should be resolved against app directory."""
        config_path = self._create_logging_config(
            "[loggers]\n"
            "keys=root\n"
            "[handlers]\n"
            "keys=file_handler\n"
            "[formatters]\n"
            "keys=formatter\n"
            "[logger_root]\n"
            "level=INFO\n"
            "handlers=file_handler\n"
            "[handler_file_handler]\n"
            "class=handlers.RotatingFileHandler\n"
            "level=INFO\n"
            "formatter=formatter\n"
            "args=('rushti.log', 'a', 5*1024*1024, 10, 'utf-8')\n"
            "[formatter_formatter]\n"
            "format=%(asctime)s - %(message)s\n"
        )

        with patch.dict(os.environ, {"RUSHTI_DIR": self.temp_dir}):
            from rushti.cli import _resolve_logging_config

            cp = _resolve_logging_config(config_path)

        args_str = cp.get("handler_file_handler", "args")
        # The path should now be absolute, pointing to temp_dir
        expected_path = os.path.join(self.temp_dir, "rushti.log").replace("\\", "/")
        self.assertIn(expected_path, args_str)
        # The rest of the args should be preserved
        self.assertIn("5*1024*1024", args_str)
        self.assertIn("utf-8", args_str)

    def test_absolute_path_is_unchanged(self):
        """Absolute log file path should not be modified."""
        abs_path = "/var/log/rushti.log"
        config_path = self._create_logging_config(
            "[loggers]\n"
            "keys=root\n"
            "[handlers]\n"
            "keys=file_handler\n"
            "[formatters]\n"
            "keys=formatter\n"
            "[logger_root]\n"
            "level=INFO\n"
            "handlers=file_handler\n"
            "[handler_file_handler]\n"
            "class=handlers.RotatingFileHandler\n"
            "level=INFO\n"
            "formatter=formatter\n"
            f"args=('{abs_path}', 'a', 5*1024*1024, 10, 'utf-8')\n"
            "[formatter_formatter]\n"
            "format=%(asctime)s - %(message)s\n"
        )

        with patch.dict(os.environ, {"RUSHTI_DIR": self.temp_dir}):
            from rushti.cli import _resolve_logging_config

            cp = _resolve_logging_config(config_path)

        args_str = cp.get("handler_file_handler", "args")
        self.assertIn(abs_path, args_str)

    def test_stream_handler_is_unaffected(self):
        """Stream handlers should not be modified."""
        config_path = self._create_logging_config(
            "[loggers]\n"
            "keys=root\n"
            "[handlers]\n"
            "keys=stream_handler\n"
            "[formatters]\n"
            "keys=formatter\n"
            "[logger_root]\n"
            "level=INFO\n"
            "handlers=stream_handler\n"
            "[handler_stream_handler]\n"
            "class=StreamHandler\n"
            "level=WARN\n"
            "formatter=formatter\n"
            "args=(sys.stderr,)\n"
            "[formatter_formatter]\n"
            "format=%(asctime)s - %(message)s\n"
        )

        with patch.dict(os.environ, {"RUSHTI_DIR": self.temp_dir}):
            from rushti.cli import _resolve_logging_config

            cp = _resolve_logging_config(config_path)

        args_str = cp.get("handler_stream_handler", "args")
        self.assertEqual(args_str, "(sys.stderr,)")

    def test_double_quoted_path_is_resolved(self):
        """Double-quoted paths should also be resolved."""
        config_path = self._create_logging_config(
            "[loggers]\n"
            "keys=root\n"
            "[handlers]\n"
            "keys=file_handler\n"
            "[formatters]\n"
            "keys=formatter\n"
            "[logger_root]\n"
            "level=INFO\n"
            "handlers=file_handler\n"
            "[handler_file_handler]\n"
            "class=logging.FileHandler\n"
            "level=INFO\n"
            "formatter=formatter\n"
            'args=("app.log", "a")\n'
            "[formatter_formatter]\n"
            "format=%(asctime)s - %(message)s\n"
        )

        with patch.dict(os.environ, {"RUSHTI_DIR": self.temp_dir}):
            from rushti.cli import _resolve_logging_config

            cp = _resolve_logging_config(config_path)

        args_str = cp.get("handler_file_handler", "args")
        expected_path = os.path.join(self.temp_dir, "app.log").replace("\\", "/")
        self.assertIn(expected_path, args_str)

    def test_mixed_handlers(self):
        """Config with both stream and file handlers should only modify the file handler."""
        config_path = self._create_logging_config(
            "[loggers]\n"
            "keys=root\n"
            "[handlers]\n"
            "keys=stream_handler, file_handler\n"
            "[formatters]\n"
            "keys=formatter\n"
            "[logger_root]\n"
            "level=INFO\n"
            "handlers=stream_handler, file_handler\n"
            "[handler_stream_handler]\n"
            "class=StreamHandler\n"
            "level=WARN\n"
            "formatter=formatter\n"
            "args=(sys.stderr,)\n"
            "[handler_file_handler]\n"
            "class=handlers.RotatingFileHandler\n"
            "level=INFO\n"
            "formatter=formatter\n"
            "args=('rushti.log', 'a', 5*1024*1024, 10, 'utf-8')\n"
            "[formatter_formatter]\n"
            "format=%(asctime)s - %(message)s\n"
        )

        with patch.dict(os.environ, {"RUSHTI_DIR": self.temp_dir}):
            from rushti.cli import _resolve_logging_config

            cp = _resolve_logging_config(config_path)

        # Stream handler unchanged
        stream_args = cp.get("handler_stream_handler", "args")
        self.assertEqual(stream_args, "(sys.stderr,)")

        # File handler resolved
        file_args = cp.get("handler_file_handler", "args")
        expected_path = os.path.join(self.temp_dir, "rushti.log").replace("\\", "/")
        self.assertIn(expected_path, file_args)

    def test_returns_configparser_instance(self):
        """Should return a ConfigParser instance suitable for fileConfig()."""
        import configparser

        config_path = self._create_logging_config(
            "[loggers]\n"
            "keys=root\n"
            "[handlers]\n"
            "keys=stream_handler\n"
            "[formatters]\n"
            "keys=formatter\n"
            "[logger_root]\n"
            "level=INFO\n"
            "handlers=stream_handler\n"
            "[handler_stream_handler]\n"
            "class=StreamHandler\n"
            "level=WARN\n"
            "formatter=formatter\n"
            "args=(sys.stderr,)\n"
            "[formatter_formatter]\n"
            "format=%(asctime)s - %(message)s\n"
        )

        with patch.dict(os.environ, {"RUSHTI_DIR": self.temp_dir}):
            from rushti.cli import _resolve_logging_config

            cp = _resolve_logging_config(config_path)

        self.assertIsInstance(cp, configparser.ConfigParser)


if __name__ == "__main__":
    unittest.main()
