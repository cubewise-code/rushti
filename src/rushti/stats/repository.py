"""StatsRepository Protocol and ``create_stats_database`` factory.

This module defines the structural typing seam between the two storage
backends — ``rushti.stats.sqlite.StatsDatabase`` and
``rushti.stats.dynamodb.DynamoDBStatsDatabase`` — so callers depend on
the abstract surface (``StatsRepository``) rather than a concrete class.

Per PEP 544 the Protocol uses *structural* typing: classes don't need
to inherit from it; they only need to expose matching method shapes.
Both existing adapters already do, so no changes are required to make
them satisfy the Protocol.
"""

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Protocol, Union, runtime_checkable

from rushti.stats.paths import (
    DEFAULT_DB_PATH,
    DEFAULT_DYNAMODB_RUNS_TABLE,
    DEFAULT_DYNAMODB_TASK_RESULTS_TABLE,
    DEFAULT_RETENTION_DAYS,
    DEFAULT_STATS_BACKEND,
)

logger = logging.getLogger(__name__)


@runtime_checkable
class StatsRepository(Protocol):
    """Abstract storage surface for execution stats.

    Both ``StatsDatabase`` (SQLite) and ``DynamoDBStatsDatabase``
    satisfy this Protocol structurally; callers should type-hint
    against ``StatsRepository`` rather than a concrete class so the
    backend stays swappable.

    The Protocol covers the methods needed by the rest of RushTI —
    optimizer, contention analyzer, db_admin, dashboard, and the CLI
    handlers under ``rushti.commands.stats``. Backend-specific
    operations (e.g., SQLite ``VACUUM``, raw cursor access) are not
    promoted here and remain on the concrete adapter.
    """

    enabled: bool

    # ----- write path -----

    def start_run(
        self,
        run_id: str,
        workflow: str,
        taskfile_path: Optional[str] = ...,
        task_count: int = ...,
        taskfile_name: Optional[str] = ...,
        taskfile_description: Optional[str] = ...,
        taskfile_author: Optional[str] = ...,
        max_workers: Optional[int] = ...,
        retries: Optional[int] = ...,
        result_file: Optional[str] = ...,
        exclusive: Optional[bool] = ...,
        optimize: Optional[bool] = ...,
        optimization_algorithm: Optional[str] = ...,
    ) -> None: ...

    def record_task(
        self,
        run_id: str,
        task_id: str,
        instance: str,
        process: str,
        parameters: Optional[Dict[str, Any]],
        success: bool,
        start_time: datetime,
        end_time: datetime,
        retry_count: int = ...,
        error_message: Optional[str] = ...,
        predecessors: Optional[List[str]] = ...,
        stage: Optional[str] = ...,
        safe_retry: Optional[bool] = ...,
        timeout: Optional[int] = ...,
        cancel_at_timeout: Optional[bool] = ...,
        require_predecessor_success: Optional[bool] = ...,
        succeed_on_minor_errors: Optional[bool] = ...,
        workflow: Optional[str] = ...,
    ) -> None: ...

    def batch_record_tasks(self, tasks: List[Dict[str, Any]]) -> None: ...

    def complete_run(
        self,
        run_id: str,
        status: str = ...,
        success_count: int = ...,
        failure_count: int = ...,
    ) -> None: ...

    def cleanup_old_data(self, retention_days: int) -> int: ...

    # ----- read path -----

    def get_task_history(self, task_signature: str, limit: int = ...) -> List[Dict[str, Any]]: ...

    def get_workflow_signatures(self, workflow: str) -> List[str]: ...

    def get_task_sample_count(self, task_signature: str) -> int: ...

    def get_task_durations(self, task_signature: str, limit: int = ...) -> List[float]: ...

    def get_run_results(self, run_id: str) -> List[Dict[str, Any]]: ...

    def get_run_info(self, run_id: str) -> Optional[Dict[str, Any]]: ...

    def get_runs_for_workflow(self, workflow: str) -> List[Dict[str, Any]]: ...

    def get_all_runs(self) -> List[Dict[str, Any]]: ...

    def get_run_task_stats(self, run_id: str) -> Optional[Dict[str, Any]]: ...

    def get_concurrent_task_counts(self, run_id: str) -> List[Dict[str, Any]]: ...

    # ----- lifecycle -----

    def close(self) -> None: ...

    def __enter__(self) -> "StatsRepository": ...

    def __exit__(self, exc_type, exc_val, exc_tb) -> None: ...


def create_stats_database(
    enabled: bool = False,
    db_path: str = DEFAULT_DB_PATH,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    backend: str = DEFAULT_STATS_BACKEND,
    dynamodb_region: Optional[str] = None,
    dynamodb_runs_table: str = DEFAULT_DYNAMODB_RUNS_TABLE,
    dynamodb_task_results_table: str = DEFAULT_DYNAMODB_TASK_RESULTS_TABLE,
    dynamodb_endpoint_url: Optional[str] = None,
) -> StatsRepository:
    """Factory function to create a stats database with the configured backend.

    :param enabled: Whether stats collection is enabled
    :param db_path: Path to SQLite database file (sqlite backend only)
    :param retention_days: Data retention period in days; 0 keeps data indefinitely
    :param backend: Storage backend — ``"sqlite"`` (default) or ``"dynamodb"``
    :param dynamodb_region: AWS region for DynamoDB (required when backend is dynamodb
        and AWS_DEFAULT_REGION / AWS_REGION env vars are not set)
    :param dynamodb_runs_table: DynamoDB table name for run-level records
    :param dynamodb_task_results_table: DynamoDB table name for task-level records
    :param dynamodb_endpoint_url: Optional custom endpoint URL (e.g. LocalStack)
    :return: Configured StatsRepository (SQLite or DynamoDB adapter)
    """
    # Imports deferred to avoid pulling boto3 / sqlite3 into module-import
    # time when only one backend is actually used.
    from rushti.stats.dynamodb import DynamoDBStatsDatabase
    from rushti.stats.sqlite import StatsDatabase

    backend_normalized = (backend or DEFAULT_STATS_BACKEND).lower()

    if backend_normalized == "sqlite":
        db: Union[StatsDatabase, DynamoDBStatsDatabase] = StatsDatabase(
            db_path=db_path, enabled=enabled
        )
    elif backend_normalized == "dynamodb":
        if (
            enabled
            and not dynamodb_region
            and not os.environ.get("AWS_DEFAULT_REGION")
            and not os.environ.get("AWS_REGION")
        ):
            raise ValueError(
                "DynamoDB backend requires a region. Set 'dynamodb_region' in [stats] settings "
                "or the AWS_DEFAULT_REGION / AWS_REGION environment variable."
            )
        db = DynamoDBStatsDatabase(
            enabled=enabled,
            region_name=dynamodb_region,
            runs_table_name=dynamodb_runs_table,
            task_results_table_name=dynamodb_task_results_table,
            endpoint_url=dynamodb_endpoint_url,
        )
    else:
        raise ValueError(
            f"Unsupported stats backend '{backend}'. " "Supported backends: sqlite, dynamodb"
        )

    # Run cleanup if enabled
    if enabled and retention_days > 0:
        try:
            db.cleanup_old_data(retention_days)
        except Exception as e:
            logger.warning(f"Stats cleanup failed (non-blocking): {e}")

    return db
