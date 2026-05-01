"""Application path resolution and legacy-path deprecation tracking.

Resolves configuration file locations across the supported lookup
precedence (CLI flag > ``RUSHTI_DIR`` env var > legacy CWD > ``config/``)
and tracks which legacy paths have been used so a deprecation warning
can be emitted once logging is initialized.

Extracted from ``rushti.cli`` in Phase 1 of the architecture refactor
(see ``docs/architecture/refactoring-plan.md``).
"""

import logging
import os

from rushti.utils import set_current_directory

__all__ = [
    "CURRENT_DIRECTORY",
    "resolve_config_path",
    "log_legacy_path_warnings",
]


CURRENT_DIRECTORY = set_current_directory()

# Track which legacy paths have been warned about (avoids duplicate warnings)
_legacy_path_warnings: set = set()


def resolve_config_path(filename: str, warn_on_legacy: bool = True, cli_path: str = None) -> str:
    """Resolve configuration file path with fallback to legacy location.

    Checks for configuration files in this order:
    1. CLI argument (if provided)
    2. RUSHTI_DIR environment variable (looks in {RUSHTI_DIR}/config/)
    3. Current directory (legacy location with deprecation warning)
    4. config/ directory (recommended location)

    :param filename: Name of the configuration file (e.g., "config.ini")
    :param warn_on_legacy: Whether to record a deprecation warning if
        the legacy path is used
    :param cli_path: Optional CLI-provided path (takes precedence)
    :return: Path to the configuration file
    """
    # 1. CLI argument takes precedence
    if cli_path:
        if os.path.exists(cli_path):
            return cli_path
        raise FileNotFoundError(f"Config file not found: {cli_path}")

    # 2. RUSHTI_DIR environment variable (config files live in config/ subdirectory)
    rushti_dir = os.environ.get("RUSHTI_DIR")
    if rushti_dir:
        env_path = os.path.join(rushti_dir, "config", filename)
        if os.path.exists(env_path):
            return env_path
        # Warn but continue to fallback
        logging.warning(
            f"RUSHTI_DIR is set to '{rushti_dir}' but '{filename}' "
            f"was not found in '{rushti_dir}/config/'"
        )

    # 3. Current directory (legacy location)
    legacy_path = os.path.join(CURRENT_DIRECTORY, filename)
    if os.path.exists(legacy_path):
        if warn_on_legacy and filename not in _legacy_path_warnings:
            _legacy_path_warnings.add(filename)
        return legacy_path

    # 4. config/ subdirectory (recommended location)
    new_path = os.path.join(CURRENT_DIRECTORY, "config", filename)
    if os.path.exists(new_path):
        return new_path

    # Neither exists - return new path for error messaging
    return new_path


def log_legacy_path_warnings(logger: logging.Logger) -> None:
    """Log deprecation warnings for any legacy paths that were used.

    Call this after logging is initialized to emit any pending warnings.
    """
    for filename in _legacy_path_warnings:
        logger.warning(
            f"DEPRECATION: '{filename}' found in root directory. "
            f"Please move it to 'config/{filename}' or set environment variable. "
            f"Legacy path support will be removed in a future version."
        )
