"""Stats backend defaults and path/backend resolution.

Extracted from ``rushti.stats`` (formerly ``stats.py``) in Phase 3 of
the architecture refactor.
"""

from rushti.utils import resolve_app_path

# Default database location relative to application directory
DEFAULT_DB_PATH = resolve_app_path("data/rushti_stats.db")

# Default retention period in days
DEFAULT_RETENTION_DAYS = 90

# Default storage backend
DEFAULT_STATS_BACKEND = "sqlite"

# Default DynamoDB resource names
DEFAULT_DYNAMODB_RUNS_TABLE = "rushti_runs"
DEFAULT_DYNAMODB_TASK_RESULTS_TABLE = "rushti_task_results"

# Schema version (development - no migrations needed)
SCHEMA_VERSION = 1


def get_db_path(settings=None) -> str:
    """Resolve database path from settings, falling back to default.

    :param settings: Optional Settings object (or its stats sub-object)
    :return: Resolved database path
    """
    if settings is not None:
        # Accept either full Settings or StatsSettings
        stats = getattr(settings, "stats", settings)
        custom_path = getattr(stats, "db_path", "")
        if custom_path:
            return resolve_app_path(custom_path)
    return DEFAULT_DB_PATH


def get_stats_backend(settings=None) -> str:
    """Resolve stats backend from settings, falling back to SQLite."""
    if settings is None:
        return DEFAULT_STATS_BACKEND

    stats = getattr(settings, "stats", settings)
    backend = getattr(stats, "backend", DEFAULT_STATS_BACKEND)
    return (backend or DEFAULT_STATS_BACKEND).lower()
