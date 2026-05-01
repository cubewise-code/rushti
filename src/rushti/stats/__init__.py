"""RushTI execution-statistics package.

Re-exports the public surface that used to live in the single
``rushti.stats`` module. After Phase 3 of the architecture refactor:

- ``signature.py`` — ``calculate_task_signature``
- ``paths.py``     — ``DEFAULT_*`` constants, ``get_db_path``,
                     ``get_stats_backend``
- ``sqlite.py``    — ``StatsDatabase`` (SQLite adapter)
- ``dynamodb.py``  — ``DynamoDBStatsDatabase`` (DynamoDB adapter)
- ``repository.py``— ``StatsRepository`` Protocol +
                     ``create_stats_database`` factory

Callers should ideally import from the focused submodules, but the
legacy ``from rushti.stats import StatsDatabase`` etc. paths keep
working unchanged via these re-exports.
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
