"""Logging configuration helpers for the RushTI CLI.

Owns the pre-processing of the logging config file (resolving relative
file-handler paths against the application directory so log files don't
get written into ``C:\\windows\\system32`` when invoked from TM1's
``ExecuteCommand``), the ``--log-level`` argparse helper, and the
runtime application of an override level.
"""

import argparse
import configparser
import logging
import os
from typing import Optional

from rushti.messages import LOG_LEVELS

__all__ = [
    "resolve_logging_config",
    "add_log_level_arg",
    "apply_log_level",
]


_FILE_HANDLER_CLASSES = frozenset(
    {
        "FileHandler",
        "logging.FileHandler",
        "handlers.RotatingFileHandler",
        "logging.handlers.RotatingFileHandler",
        "handlers.TimedRotatingFileHandler",
        "logging.handlers.TimedRotatingFileHandler",
    }
)


def resolve_logging_config(config_path: str) -> configparser.ConfigParser:
    """Pre-process logging config to resolve relative file handler paths.

    Python's ``fileConfig()`` resolves relative paths in handler args
    against the current working directory. When rushti is invoked from
    TM1's ExecuteCommand, CWD is often ``C:\\windows\\system32``, causing
    PermissionError.

    This reads the logging config into a ConfigParser, resolves any
    relative file paths in file-handler args against the application
    directory, and returns the modified ConfigParser. Absolute paths are
    left unchanged.

    Since Python 3.4, ``fileConfig()`` accepts a ConfigParser instance
    directly.
    """
    from rushti.utils import resolve_app_path

    cp = configparser.ConfigParser()
    cp.read(config_path)

    for section in cp.sections():
        if not section.startswith("handler_"):
            continue
        handler_class = cp.get(section, "class", fallback="")
        if handler_class not in _FILE_HANDLER_CLASSES:
            continue
        args_str = cp.get(section, "args", fallback="")
        if not args_str:
            continue

        # Extract the first string argument (the filename) from the args tuple.
        # The args value looks like: ('rushti.log', 'a', 5*1024*1024, 10, 'utf-8')
        # We can't use ast.literal_eval due to expressions like 5*1024*1024,
        # so we find the first quoted string and do a targeted replacement.
        for quote_char in ("'", '"'):
            start_idx = args_str.find(quote_char)
            if start_idx == -1:
                continue
            end_idx = args_str.find(quote_char, start_idx + 1)
            if end_idx == -1:
                continue
            file_path = args_str[start_idx + 1 : end_idx]
            if not file_path:
                continue

            if not os.path.isabs(file_path):
                resolved = resolve_app_path(file_path)
                # Use forward slashes — Python handles them on all platforms,
                # and avoids backslash escaping issues inside the args string
                resolved_fwd = resolved.replace("\\", "/")
                # Replace only the filename portion in the args string
                new_args = args_str[: start_idx + 1] + resolved_fwd + args_str[end_idx:]
                cp.set(section, "args", new_args)
            break  # Only process the first string (the filename)

    return cp


def add_log_level_arg(parser: argparse.ArgumentParser) -> None:
    """Add --log-level argument to a parser."""
    parser.add_argument(
        "--log-level",
        "-L",
        dest="log_level",
        choices=LOG_LEVELS,
        default=None,
        metavar="LEVEL",
        help=(
            "Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). "
            "Overrides logging_config.ini"
        ),
    )


def apply_log_level(log_level: Optional[str]) -> None:
    """Apply log level override if specified.

    Updates the root logger and all its handlers to the specified level.
    """
    if log_level is None:
        return

    level = getattr(logging, log_level.upper(), None)
    if level is None:
        return

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    for handler in root_logger.handlers:
        handler.setLevel(level)
