"""RushTI execution-statistics package.

Submodules:

- ``signature.py``  — ``calculate_task_signature``
- ``paths.py``      — ``DEFAULT_*`` constants, ``get_db_path``,
                      ``get_stats_backend``
- ``sqlite.py``     — ``StatsDatabase`` (SQLite adapter)
- ``dynamodb.py``   — ``DynamoDBStatsDatabase`` (DynamoDB adapter)
- ``repository.py`` — ``StatsRepository`` Protocol +
                      ``create_stats_database`` factory

Callers should ideally import from the focused submodules, but
``from rushti.stats import StatsDatabase`` etc. is also supported
via the re-exports below.
"""

from rushti.stats.dynamodb import DynamoDBStatsDatabase
from rushti.stats.paths import (
    DEFAULT_DB_PATH,
    DEFAULT_DYNAMODB_RUNS_TABLE,
    DEFAULT_DYNAMODB_TASK_RESULTS_TABLE,
    DEFAULT_RETENTION_DAYS,
    DEFAULT_STATS_BACKEND,
    SCHEMA_VERSION,
    get_db_path,
    get_stats_backend,
)
from rushti.stats.repository import StatsRepository, create_stats_database
from rushti.stats.signature import calculate_task_signature
from rushti.stats.sqlite import StatsDatabase

__all__ = [
    # constants
    "DEFAULT_DB_PATH",
    "DEFAULT_DYNAMODB_RUNS_TABLE",
    "DEFAULT_DYNAMODB_TASK_RESULTS_TABLE",
    "DEFAULT_RETENTION_DAYS",
    "DEFAULT_STATS_BACKEND",
    "SCHEMA_VERSION",
    # functions
    "calculate_task_signature",
    "create_stats_database",
    "get_db_path",
    "get_stats_backend",
    # classes / protocols
    "DynamoDBStatsDatabase",
    "StatsDatabase",
    "StatsRepository",
]
