"""Settings module for RushTI configuration.

This module provides configuration management through settings.ini file,
separate from TM1 connection settings in config.ini.

Settings precedence (highest to lowest):
1. CLI arguments
2. JSON task file settings section
3. settings.ini file
4. Built-in defaults
"""

import configparser
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

from rushti.utils import resolve_app_path

logger = logging.getLogger(__name__)


@dataclass
class DefaultsSettings:
    """Default execution settings."""

    max_workers: int = 4
    retries: int = 0
    result_file: str = ""
    mode: str = "norm"


@dataclass
class OptimizationSettings:
    """EWMA tuning parameters for task runtime estimation.

    These settings control how the optimizer calculates runtime estimates
    from historical execution data. They are system-wide and rarely changed.

    Optimization itself is activated per-taskfile via:
    - CLI: --optimize <algorithm>
    - JSON taskfile: "optimization_algorithm": "<algorithm>"
    """

    lookback_runs: int = 10
    time_of_day_weighting: bool = False
    min_samples: int = 3
    cache_duration_hours: int = 24


@dataclass
class LoggingSettings:
    """Enhanced logging settings."""

    level: str = "INFO"
    format: str = "%(asctime)s %(levelname)s %(message)s"
    file: Optional[str] = None
    max_file_size_mb: int = 10
    backup_count: int = 5


@dataclass
class TM1IntegrationSettings:
    """TM1 integration settings for reading taskfiles and pushing results.

    Dimensions and cube created by build mode:
    - rushti_workflow: Workflow identifiers
    - rushti_task_id: Task sequence elements (1-5000)
    - rushti_run_id: Run timestamps (YYYYMMDD_HHMMSS) + Input element
    - rushti_measure: Log field measures
    - rushti: Cube for task definitions and log storage

    When push_results is enabled, the results CSV is uploaded to TM1 files.
    When auto_load_results is also enabled, }rushti.load.results TI process
    is called after upload to load the CSV into the rushti cube.
    """

    push_results: bool = False
    auto_load_results: bool = False
    default_tm1_instance: Optional[str] = None
    default_rushti_cube: str = "rushti"
    default_workflow_dim: str = "rushti_workflow"
    default_task_id_dim: str = "rushti_task_id"
    default_run_id_dim: str = "rushti_run_id"
    default_measure_dim: str = "rushti_measure"


@dataclass
class ExclusiveModeSettings:
    """Exclusive mode execution settings.

    Uses TM1 session context fields for detection:
    - Normal mode: RushTI{workflow}
    - Exclusive mode: RushTIX{workflow}
    """

    enabled: bool = False
    polling_interval: int = 30
    timeout: int = 600


@dataclass
class ResumeSettings:
    """Resume/checkpoint feature settings."""

    enabled: bool = False
    checkpoint_interval: int = 60
    checkpoint_dir: str = "./checkpoints"
    auto_resume: bool = False


@dataclass
class StatsSettings:
    """SQLite stats database settings.

    The stats database stores execution statistics for:
    - Optimization features (EWMA runtime estimation)
    - TM1 cube logging data source
    - Historical analysis
    """

    enabled: bool = False
    retention_days: int = 90
    db_path: str = ""


@dataclass
class Settings:
    """Main settings container with all configuration sections."""

    defaults: DefaultsSettings = field(default_factory=DefaultsSettings)
    optimization: OptimizationSettings = field(default_factory=OptimizationSettings)
    logging: LoggingSettings = field(default_factory=LoggingSettings)
    tm1_integration: TM1IntegrationSettings = field(default_factory=TM1IntegrationSettings)
    exclusive_mode: ExclusiveModeSettings = field(default_factory=ExclusiveModeSettings)
    resume: ResumeSettings = field(default_factory=ResumeSettings)
    stats: StatsSettings = field(default_factory=StatsSettings)


# Known sections and their valid keys with types
SETTINGS_SCHEMA = {
    "defaults": {
        "max_workers": int,
        "retries": int,
        "result_file": str,
        "mode": str,
    },
    "optimization": {
        "lookback_runs": int,
        "time_of_day_weighting": bool,
        "min_samples": int,
        "cache_duration_hours": int,
    },
    "logging": {
        "level": str,
        "format": str,
        "file": str,
        "max_file_size_mb": int,
        "backup_count": int,
    },
    "tm1_integration": {
        "push_results": bool,
        "auto_load_results": bool,
        "default_tm1_instance": str,
        "default_rushti_cube": str,
        "default_workflow_dim": str,
        "default_task_id_dim": str,
        "default_run_id_dim": str,
        "default_measure_dim": str,
    },
    "exclusive_mode": {
        "enabled": bool,
        "polling_interval": int,
        "timeout": int,
    },
    "resume": {
        "enabled": bool,
        "checkpoint_interval": int,
        "checkpoint_dir": str,
        "auto_resume": bool,
    },
    "stats": {
        "enabled": bool,
        "retention_days": int,
        "db_path": str,
    },
}

# Valid values for string settings
VALID_VALUES = {
    "mode": ["norm", "opt"],
    "level": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
}


def parse_bool(value: str) -> bool:
    """Parse a string value as boolean.

    :param value: String value to parse
    :return: Boolean value
    :raises ValueError: If value is not a valid boolean string
    """
    if value.lower() in ("true", "yes", "1", "on"):
        return True
    elif value.lower() in ("false", "no", "0", "off"):
        return False
    else:
        raise ValueError(f"Invalid boolean value: '{value}'")


def parse_value(value: str, expected_type: type) -> Any:
    """Parse a string value to the expected type.

    :param value: String value to parse
    :param expected_type: Expected Python type
    :return: Parsed value
    :raises ValueError: If value cannot be parsed to expected type
    """
    if expected_type is bool:
        return parse_bool(value)
    elif expected_type is int:
        try:
            return int(value)
        except ValueError:
            raise ValueError(f"Invalid integer value: '{value}'")
    elif expected_type is str:
        return value
    else:
        return value


def validate_setting(section: str, key: str, value: Any) -> None:
    """Validate a setting value.

    :param section: Section name
    :param key: Setting key
    :param value: Setting value
    :raises ValueError: If value is invalid
    """
    # Check for valid values constraint
    if key in VALID_VALUES:
        valid = VALID_VALUES[key]
        if isinstance(value, str) and value.lower() not in [v.lower() for v in valid]:
            raise ValueError(
                f"Invalid value '{value}' for {section}.{key}. " f"Valid values: {', '.join(valid)}"
            )

    # Check integer ranges
    if isinstance(value, int):
        if key in (
            "max_workers",
            "retries",
            "port",
            "lookback_runs",
            "min_samples",
            "polling_interval",
            "timeout",
            "checkpoint_interval",
            "max_file_size_mb",
            "backup_count",
            "cache_duration_hours",
        ):
            if value < 0:
                raise ValueError(f"{section}.{key} must be non-negative, got {value}")
        if key == "max_workers" and value < 1:
            raise ValueError(f"{section}.{key} must be at least 1, got {value}")


def resolve_settings_path(script_dir: Path) -> tuple[Path, bool]:
    """Resolve settings.ini path with fallback to legacy location.

    :param script_dir: Directory containing the script
    :return: Tuple of (resolved path, is_legacy_path)
    """
    new_path = script_dir / "config" / "settings.ini"
    legacy_path = script_dir / "settings.ini"

    # Check new location first
    if new_path.exists():
        return new_path, False

    # Fallback to legacy location
    if legacy_path.exists():
        return legacy_path, True

    # Neither exists - return new path
    return new_path, False


def load_settings(settings_path: Optional[str] = None) -> Settings:
    """Load settings from settings.ini file.

    :param settings_path: Path to settings.ini file. If None, looks for
                         settings.ini in config/ directory first, then
                         falls back to current working directory for backward compatibility.
    :return: Settings object with loaded configuration
    """
    settings = Settings()

    # Determine settings file path
    if settings_path is None:
        # 1. Check RUSHTI_DIR environment variable (config files in config/ subdir)
        rushti_dir = os.environ.get("RUSHTI_DIR")
        if rushti_dir:
            env_path = Path(rushti_dir) / "config" / "settings.ini"
            if env_path.exists():
                settings_path = env_path
            else:
                logger.warning(
                    f"RUSHTI_DIR is set to '{rushti_dir}' but "
                    f"'settings.ini' was not found in '{rushti_dir}/config/'"
                )

        # 2. Fall back to directory-based discovery
        if settings_path is None:
            cwd = Path.cwd()
            settings_path, is_legacy = resolve_settings_path(cwd)
            if is_legacy:
                logger.warning(
                    "DEPRECATION: 'settings.ini' found in root directory. "
                    "Please move it to 'config/settings.ini'. "
                    "Legacy path support will be removed in a future version."
                )
    else:
        settings_path = Path(settings_path)

    # If settings file doesn't exist, return defaults
    if not settings_path.exists():
        logger.debug(f"Settings file not found at {settings_path}, using defaults")
        return settings

    logger.info(f"Loading settings from {settings_path}")

    config = configparser.ConfigParser()
    try:
        config.read(settings_path)
    except configparser.Error as e:
        logger.error(f"Error parsing settings.ini: {e}")
        raise ValueError(f"Invalid settings.ini format: {e}")

    # Process each section
    for section in config.sections():
        if section not in SETTINGS_SCHEMA:
            logger.warning(f"Unknown section '{section}' in settings.ini (possible typo?)")
            continue

        section_schema = SETTINGS_SCHEMA[section]
        section_obj = getattr(settings, section.replace("-", "_"))

        for key, value in config.items(section):
            if key not in section_schema:
                logger.warning(f"Unknown setting '{key}' in [{section}] (possible typo?)")
                continue

            expected_type = section_schema[key]
            try:
                parsed_value = parse_value(value, expected_type)
                validate_setting(section, key, parsed_value)
                setattr(section_obj, key, parsed_value)
            except ValueError as e:
                raise ValueError(f"Error in settings.ini [{section}].{key}: {e}")

    # Resolve paths relative to application directory
    settings.resume.checkpoint_dir = resolve_app_path(settings.resume.checkpoint_dir)

    return settings


def get_effective_settings(
    settings: Settings,
    cli_args: Optional[Dict[str, Any]] = None,
    json_settings: Optional[Dict[str, Any]] = None,
) -> Settings:
    """Merge settings from all sources with proper precedence.

    Precedence (highest to lowest):
    1. CLI arguments
    2. JSON task file settings section
    3. settings.ini values (already in settings object)
    4. Built-in defaults (already in settings object)

    :param settings: Base settings loaded from settings.ini
    :param cli_args: CLI arguments that override settings
    :param json_settings: JSON task file settings section
    :return: Settings with all overrides applied
    """
    # Apply JSON settings first (lower precedence than CLI)
    if json_settings:
        _apply_json_settings(settings, json_settings)

    # Apply CLI arguments last (highest precedence)
    if cli_args:
        _apply_cli_args(settings, cli_args)

    # Log effective settings at debug level
    logger.debug("Effective settings:")
    logger.debug(f"  defaults.max_workers: {settings.defaults.max_workers}")
    logger.debug(f"  defaults.retries: {settings.defaults.retries}")
    logger.debug(f"  defaults.result_file: {settings.defaults.result_file}")
    logger.debug(f"  defaults.mode: {settings.defaults.mode}")

    return settings


def _apply_json_settings(settings: Settings, json_settings: Dict[str, Any]) -> None:
    """Apply settings from JSON task file.

    :param settings: Settings object to update
    :param json_settings: Settings dict from JSON task file
    """
    # Map JSON keys to settings attributes
    json_to_settings = {
        "max_workers": ("defaults", "max_workers"),
        "retries": ("defaults", "retries"),
        "result_file": ("defaults", "result_file"),
        "exclusive": ("exclusive_mode", "enabled"),
        "push_results": ("tm1_integration", "push_results"),
        "auto_load_results": ("tm1_integration", "auto_load_results"),
    }

    for json_key, (section, attr) in json_to_settings.items():
        if json_key in json_settings and json_settings[json_key] is not None:
            section_obj = getattr(settings, section)
            value = json_settings[json_key]
            setattr(section_obj, attr, value)
            logger.debug(f"JSON override: {section}.{attr} = {value}")


def _apply_cli_args(settings: Settings, cli_args: Dict[str, Any]) -> None:
    """Apply CLI argument overrides.

    :param settings: Settings object to update
    :param cli_args: CLI arguments dict
    """
    # Map CLI args to settings attributes
    cli_to_settings = {
        "max_workers": ("defaults", "max_workers"),
        "retries": ("defaults", "retries"),
        "result_file": ("defaults", "result_file"),
        "execution_mode": ("defaults", "mode"),
    }

    for cli_key, (section, attr) in cli_to_settings.items():
        if cli_key in cli_args and cli_args[cli_key] is not None:
            section_obj = getattr(settings, section)
            value = cli_args[cli_key]
            # Convert ExecutionMode enum to string if needed
            if hasattr(value, "name"):
                value = value.name.lower()
            setattr(section_obj, attr, value)
            logger.debug(f"CLI override: {section}.{attr} = {value}")
