"""SQLite stats storage module for RushTI execution statistics.

This module provides optional local storage for execution statistics,
enabling optimization features and serving as the data source for
TM1 cube logging when enabled.

The stats database is disabled by default and must be explicitly enabled
in settings.ini:

    [stats]
    enabled = true
    retention_days = 90
"""

import hashlib
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from rushti.utils import ensure_shared_file, makedirs_shared, resolve_app_path

logger = logging.getLogger(__name__)

# Default database location relative to application directory
DEFAULT_DB_PATH = resolve_app_path("data/rushti_stats.db")

# Default retention period in days
DEFAULT_RETENTION_DAYS = 90

# Schema version (development - no migrations needed)
SCHEMA_VERSION = 1


def calculate_task_signature(
    instance: str, process: str, parameters: Optional[Dict[str, Any]]
) -> str:
    """Calculate deterministic signature for task identity.

    The signature uniquely identifies a task configuration for
    runtime estimation across multiple runs. Tasks with the same
    instance, process, and parameters will have the same signature.

    :param instance: TM1 instance name
    :param process: TI process name
    :param parameters: Process parameters dictionary
    :return: 16-character hex signature
    """
    # Sort parameters for deterministic hash
    sorted_params = json.dumps(parameters, sort_keys=True) if parameters else "{}"

    # Combine components
    signature_input = f"{instance}|{process}|{sorted_params}"

    # SHA256 hash (truncated for readability)
    return hashlib.sha256(signature_input.encode()).hexdigest()[:16]


class StatsDatabase:
    """SQLite database for execution statistics.

    Provides storage and retrieval of task execution results for:
    - Optimization features (EWMA runtime estimation)
    - TM1 cube logging (read stats, push to TM1)
    - Historical analysis
    """

    def __init__(self, db_path: str = DEFAULT_DB_PATH, enabled: bool = False):
        """Initialize the stats database.

        :param db_path: Path to SQLite database file
        :param enabled: Whether stats collection is enabled
        """
        self.db_path = db_path
        self.enabled = enabled
        self._conn: Optional[sqlite3.Connection] = None

        if self.enabled:
            self._initialize_database()

    def _initialize_database(self) -> None:
        """Create database file and tables if they don't exist."""
        # Create directory if needed (shared permissions for multi-user access)
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            makedirs_shared(db_dir)
            logger.info(f"Created stats directory: {db_dir}")

        # Connect and create tables
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row

        # Ensure DB file is writable by all users
        ensure_shared_file(self.db_path)

        # Enable WAL mode for better concurrency
        self._conn.execute("PRAGMA journal_mode=WAL")

        # Ensure WAL and SHM files are also shared (created by WAL mode)
        for suffix in ("-wal", "-shm"):
            wal_path = self.db_path + suffix
            if os.path.exists(wal_path):
                ensure_shared_file(wal_path)

        # Create tables
        self._create_tables()

        logger.info(f"Stats database initialized: {self.db_path}")

    def _create_tables(self) -> None:
        """Create database tables and indexes."""
        cursor = self._conn.cursor()

        # Runs table - includes taskfile metadata and settings for analysis
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                workflow TEXT NOT NULL,
                taskfile_path TEXT,
                start_time TEXT NOT NULL,
                end_time TEXT,
                duration_seconds REAL,
                status TEXT,
                task_count INTEGER,
                success_count INTEGER,
                failure_count INTEGER,
                -- Taskfile metadata
                taskfile_name TEXT,
                taskfile_description TEXT,
                taskfile_author TEXT,
                -- Taskfile settings
                max_workers INTEGER,
                retries INTEGER,
                result_file TEXT,
                exclusive BOOLEAN,
                optimize BOOLEAN,
                optimization_algorithm TEXT
            )
        """)

        # Task results table - includes task configuration for analysis
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS task_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                workflow TEXT,
                task_id TEXT NOT NULL,
                task_signature TEXT NOT NULL,
                instance TEXT NOT NULL,
                process TEXT NOT NULL,
                parameters TEXT,
                status TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                duration_seconds REAL NOT NULL,
                retry_count INTEGER DEFAULT 0,
                error_message TEXT,
                -- Task configuration
                predecessors TEXT,
                stage TEXT,
                safe_retry BOOLEAN,
                timeout INTEGER,
                cancel_at_timeout BOOLEAN,
                require_predecessor_success BOOLEAN,
                succeed_on_minor_errors BOOLEAN,
                FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
            )
        """)

        # Indexes for optimization queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_task_results_signature
            ON task_results(task_signature)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_task_results_start_time
            ON task_results(start_time)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_runs_workflow
            ON runs(workflow)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_runs_start_time
            ON runs(start_time)
        """)

        # Enable foreign key support
        cursor.execute("PRAGMA foreign_keys = ON")

        # Migration: add columns if they don't exist (for existing databases)
        cursor.execute("PRAGMA table_info(runs)")
        columns = [row[1] for row in cursor.fetchall()]
        if "duration_seconds" not in columns:
            cursor.execute("ALTER TABLE runs ADD COLUMN duration_seconds REAL")
            logger.info("Added duration_seconds column to runs table")
        if "optimization_algorithm" not in columns:
            cursor.execute("ALTER TABLE runs ADD COLUMN optimization_algorithm TEXT")
            logger.info("Added optimization_algorithm column to runs table")

        self._conn.commit()

    def start_run(
        self,
        run_id: str,
        workflow: str,
        taskfile_path: Optional[str] = None,
        task_count: int = 0,
        # Taskfile metadata
        taskfile_name: Optional[str] = None,
        taskfile_description: Optional[str] = None,
        taskfile_author: Optional[str] = None,
        # Taskfile settings
        max_workers: Optional[int] = None,
        retries: Optional[int] = None,
        result_file: Optional[str] = None,
        exclusive: Optional[bool] = None,
        optimize: Optional[bool] = None,
        optimization_algorithm: Optional[str] = None,
    ) -> None:
        """Record a new execution run.

        :param run_id: Unique run identifier (timestamp-based)
        :param workflow: Workflow name
        :param taskfile_path: Path to task file
        :param task_count: Total number of tasks in the run
        :param taskfile_name: Taskfile name from metadata
        :param taskfile_description: Taskfile description from metadata
        :param taskfile_author: Taskfile author from metadata
        :param max_workers: Max workers setting
        :param retries: Retries setting
        :param result_file: Result file setting
        :param exclusive: Exclusive mode setting
        :param optimize: Optimization setting (deprecated, kept for DB compat)
        :param optimization_algorithm: Scheduling algorithm name (e.g. ``shortest_first``)
        """
        if not self.enabled:
            return

        cursor = self._conn.cursor()
        cursor.execute(
            """
            INSERT INTO runs (
                run_id, workflow, taskfile_path, start_time, task_count,
                taskfile_name, taskfile_description, taskfile_author,
                max_workers, retries, result_file, exclusive, optimize,
                optimization_algorithm
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                workflow,
                taskfile_path,
                datetime.now().isoformat(),
                task_count,
                taskfile_name,
                taskfile_description,
                taskfile_author,
                max_workers,
                retries,
                result_file,
                1 if exclusive else (0 if exclusive is False else None),
                1 if optimize else (0 if optimize is False else None),
                optimization_algorithm,
            ),
        )
        self._conn.commit()
        logger.debug(f"Started run: {run_id}")

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
        retry_count: int = 0,
        error_message: Optional[str] = None,
        # Task configuration
        predecessors: Optional[List[str]] = None,
        stage: Optional[str] = None,
        safe_retry: Optional[bool] = None,
        timeout: Optional[int] = None,
        cancel_at_timeout: Optional[bool] = None,
        require_predecessor_success: Optional[bool] = None,
        succeed_on_minor_errors: Optional[bool] = None,
        # Workflow context for TM1 cube alignment
        workflow: Optional[str] = None,
    ) -> None:
        """Record a task execution result.

        :param run_id: Run identifier this task belongs to
        :param task_id: Task identifier
        :param instance: TM1 instance name
        :param process: TI process name
        :param parameters: Process parameters
        :param success: Whether execution succeeded
        :param start_time: Task start time
        :param end_time: Task end time
        :param retry_count: Number of retries attempted
        :param error_message: Error message if failed
        :param predecessors: List of predecessor task IDs
        :param stage: Task stage name
        :param safe_retry: Whether safe retry is enabled
        :param timeout: Task timeout in seconds
        :param cancel_at_timeout: Whether to cancel at timeout
        :param require_predecessor_success: Whether predecessor success is required
        :param succeed_on_minor_errors: Whether to succeed on minor errors
        :param workflow: Workflow name for TM1 cube alignment
        """
        if not self.enabled:
            return

        duration = (end_time - start_time).total_seconds()
        task_signature = calculate_task_signature(instance, process, parameters)
        params_json = json.dumps(parameters) if parameters else "{}"
        predecessors_json = json.dumps(predecessors) if predecessors else None
        status = "Success" if success else "Fail"

        cursor = self._conn.cursor()
        cursor.execute(
            """
            INSERT INTO task_results (
                run_id, workflow, task_id, task_signature, instance, process, parameters,
                status, start_time, end_time, duration_seconds, retry_count, error_message,
                predecessors, stage, safe_retry, timeout, cancel_at_timeout,
                require_predecessor_success, succeed_on_minor_errors
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                workflow,
                task_id,
                task_signature,
                instance,
                process,
                params_json,
                status,
                start_time.isoformat(),
                end_time.isoformat(),
                round(duration, 3),
                retry_count,
                error_message if not success else None,
                predecessors_json,
                stage,
                1 if safe_retry else (0 if safe_retry is False else None),
                timeout,
                1 if cancel_at_timeout else (0 if cancel_at_timeout is False else None),
                (
                    1
                    if require_predecessor_success
                    else (0 if require_predecessor_success is False else None)
                ),
                1 if succeed_on_minor_errors else (0 if succeed_on_minor_errors is False else None),
            ),
        )
        self._conn.commit()

    def batch_record_tasks(self, tasks: List[Dict[str, Any]]) -> None:
        """Record multiple task execution results in a single transaction.

        This method is preferred over multiple record_task() calls as it
        avoids concurrent write issues with SQLite. Call this at the end
        of a run with all collected task data.

        :param tasks: List of task data dictionaries with keys matching
                     record_task() parameters
        """
        if not self.enabled or not tasks or not self._conn:
            return

        cursor = self._conn.cursor()
        try:
            for task in tasks:
                run_id = task["run_id"]
                workflow = task.get("workflow")
                task_id = task["task_id"]
                instance = task["instance"]
                process = task["process"]
                parameters = task.get("parameters")
                success = task["success"]
                start_time = task["start_time"]
                end_time = task["end_time"]
                retry_count = task.get("retry_count", 0)
                error_message = task.get("error_message")
                predecessors = task.get("predecessors")
                stage = task.get("stage")
                safe_retry = task.get("safe_retry")
                timeout = task.get("timeout")
                cancel_at_timeout = task.get("cancel_at_timeout")
                require_predecessor_success = task.get("require_predecessor_success")
                succeed_on_minor_errors = task.get("succeed_on_minor_errors")

                duration = (end_time - start_time).total_seconds()
                task_signature = calculate_task_signature(instance, process, parameters)
                params_json = json.dumps(parameters) if parameters else "{}"
                predecessors_json = json.dumps(predecessors) if predecessors else None
                status = "Success" if success else "Fail"

                cursor.execute(
                    """
                    INSERT INTO task_results (
                        run_id, workflow, task_id, task_signature, instance, process, parameters,
                        status, start_time, end_time, duration_seconds, retry_count, error_message,
                        predecessors, stage, safe_retry, timeout, cancel_at_timeout,
                        require_predecessor_success, succeed_on_minor_errors
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        workflow,
                        task_id,
                        task_signature,
                        instance,
                        process,
                        params_json,
                        status,
                        start_time.isoformat(),
                        end_time.isoformat(),
                        round(duration, 3),
                        retry_count,
                        error_message if not success else None,
                        predecessors_json,
                        stage,
                        1 if safe_retry else (0 if safe_retry is False else None),
                        timeout,
                        1 if cancel_at_timeout else (0 if cancel_at_timeout is False else None),
                        (
                            1
                            if require_predecessor_success
                            else (0 if require_predecessor_success is False else None)
                        ),
                        (
                            1
                            if succeed_on_minor_errors
                            else (0 if succeed_on_minor_errors is False else None)
                        ),
                    ),
                )
            self._conn.commit()
            logger.info(f"Batch recorded {len(tasks)} task results")
        except Exception as e:
            self._conn.rollback()
            raise RuntimeError(f"Failed to batch record tasks: {e}")

    def complete_run(
        self,
        run_id: str,
        status: str = "Success",
        success_count: int = 0,
        failure_count: int = 0,
    ) -> None:
        """Mark a run as complete with final counts.

        :param run_id: Run identifier to complete
        :param status: Final run status ("Success", "Partial", "Failed")
        :param success_count: Number of successful tasks
        :param failure_count: Number of failed tasks
        """
        if not self.enabled:
            return

        end_time = datetime.now()
        cursor = self._conn.cursor()

        # Get start_time to calculate duration
        cursor.execute("SELECT start_time FROM runs WHERE run_id = ?", (run_id,))
        row = cursor.fetchone()
        duration_seconds = None
        if row and row[0]:
            try:
                start_time = datetime.fromisoformat(row[0])
                duration_seconds = (end_time - start_time).total_seconds()
            except (ValueError, TypeError):
                pass

        cursor.execute(
            """
            UPDATE runs
            SET end_time = ?, duration_seconds = ?, status = ?, success_count = ?, failure_count = ?
            WHERE run_id = ?
            """,
            (end_time.isoformat(), duration_seconds, status, success_count, failure_count, run_id),
        )
        self._conn.commit()
        logger.debug(f"Completed run: {run_id} ({status})")

    def cleanup_old_data(self, retention_days: int) -> int:
        """Delete data older than retention period.

        :param retention_days: Number of days to retain (0 = keep forever)
        :return: Number of runs deleted
        """
        if not self.enabled or retention_days <= 0:
            return 0

        cutoff_date = (datetime.now() - timedelta(days=retention_days)).isoformat()

        cursor = self._conn.cursor()

        # Count runs to delete
        cursor.execute(
            "SELECT COUNT(*) FROM runs WHERE start_time < ?",
            (cutoff_date,),
        )
        count = cursor.fetchone()[0]

        if count > 0:
            # Delete old runs (task_results cascade via foreign key)
            cursor.execute("DELETE FROM runs WHERE start_time < ?", (cutoff_date,))
            self._conn.commit()
            logger.info(f"Cleaned up {count} runs older than {retention_days} days")

        return count

    def get_task_history(
        self,
        task_signature: str,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Get historical runtimes for a task signature.

        Used by optimization features to calculate EWMA runtime estimates.

        :param task_signature: Task signature hash
        :param limit: Maximum number of results (most recent first)
        :return: List of task result dictionaries
        """
        if not self.enabled:
            return []

        cursor = self._conn.cursor()
        cursor.execute(
            """
            SELECT
                task_id, instance, process, parameters, status,
                start_time, end_time, duration_seconds, retry_count, error_message
            FROM task_results
            WHERE task_signature = ? AND status = 'Success'
            ORDER BY start_time DESC
            LIMIT ?
            """,
            (task_signature, limit),
        )

        results = []
        for row in cursor.fetchall():
            results.append(dict(row))

        return results

    def get_workflow_signatures(self, workflow: str) -> List[str]:
        """Get all unique task signatures for a workflow.

        Used by optimization features to retrieve all tasks that have been executed.

        :param workflow: Workflow name
        :return: List of unique task signature hashes
        """
        if not self.enabled:
            return []

        cursor = self._conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT task_signature
            FROM task_results
            WHERE workflow = ?
            ORDER BY task_signature
            """,
            (workflow,),
        )

        return [row[0] for row in cursor.fetchall()]

    def get_task_sample_count(self, task_signature: str) -> int:
        """Get the count of successful historical runs for a task signature.

        Used by optimizer to check min_samples threshold before applying
        optimization to a task.

        :param task_signature: Task signature hash
        :return: Number of successful executions recorded
        """
        if not self.enabled:
            return 0

        cursor = self._conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(*) FROM task_results
            WHERE task_signature = ? AND status = 'Success'
            """,
            (task_signature,),
        )
        return cursor.fetchone()[0]

    def get_task_durations(
        self,
        task_signature: str,
        limit: int = 10,
    ) -> List[float]:
        """Get historical durations for EWMA calculation.

        More efficient than get_task_history() when only durations are needed.
        Returns durations in most-recent-first order for EWMA calculation.

        :param task_signature: Task signature hash
        :param limit: Maximum number of results
        :return: List of duration_seconds values (most recent first)
        """
        if not self.enabled:
            return []

        cursor = self._conn.cursor()
        cursor.execute(
            """
            SELECT duration_seconds
            FROM task_results
            WHERE task_signature = ? AND status = 'Success'
            ORDER BY start_time DESC
            LIMIT ?
            """,
            (task_signature, limit),
        )
        return [row[0] for row in cursor.fetchall() if row[0] is not None]

    def get_run_results(self, run_id: str) -> List[Dict[str, Any]]:
        """Get all task results for a run including task configuration.

        Used by TM1 cube logging to export results.

        :param run_id: Run identifier
        :return: List of task result dictionaries
        """
        if not self.enabled:
            return []

        cursor = self._conn.cursor()
        cursor.execute(
            """
            SELECT
                workflow, task_id, task_signature, instance, process, parameters,
                status, start_time, end_time, duration_seconds, retry_count, error_message,
                predecessors, stage, safe_retry, timeout, cancel_at_timeout,
                require_predecessor_success, succeed_on_minor_errors
            FROM task_results
            WHERE run_id = ?
            ORDER BY id
            """,
            (run_id,),
        )

        # Boolean fields that need conversion from SQLite integers
        bool_fields = [
            "safe_retry",
            "cancel_at_timeout",
            "require_predecessor_success",
            "succeed_on_minor_errors",
        ]

        results = []
        for row in cursor.fetchall():
            result = dict(row)
            # Convert SQLite integers to Python booleans
            for field in bool_fields:
                if field in result and result[field] is not None:
                    result[field] = bool(result[field])
            results.append(result)

        return results

    def get_run_info(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get run metadata including taskfile metadata and settings.

        :param run_id: Run identifier
        :return: Run info dictionary or None if not found
        """
        if not self.enabled:
            return None

        cursor = self._conn.cursor()
        cursor.execute(
            """
            SELECT run_id, workflow, taskfile_path, start_time, end_time,
                   duration_seconds, status, task_count, success_count, failure_count,
                   taskfile_name, taskfile_description, taskfile_author,
                   max_workers, retries, result_file, exclusive, optimize
            FROM runs WHERE run_id = ?
            """,
            (run_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None

        result = dict(row)
        # Convert SQLite integers to Python booleans
        for field in ["exclusive", "optimize"]:
            if field in result and result[field] is not None:
                result[field] = bool(result[field])
        return result

    def get_runs_for_workflow(self, workflow: str) -> List[Dict[str, Any]]:
        """Get all runs for a specific workflow.

        :param workflow: Workflow name
        :return: List of run info dictionaries, ordered by start_time descending
        """
        if not self.enabled or not self._conn:
            return []

        cursor = self._conn.cursor()
        cursor.execute(
            """
            SELECT run_id, start_time, end_time, duration_seconds, status, task_count,
                   success_count, failure_count
            FROM runs
            WHERE workflow = ?
            ORDER BY start_time DESC
            """,
            (workflow,),
        )

        return [dict(row) for row in cursor.fetchall()]

    def close(self) -> None:
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "StatsDatabase":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()


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


def create_stats_database(
    enabled: bool = False,
    db_path: str = DEFAULT_DB_PATH,
    retention_days: int = DEFAULT_RETENTION_DAYS,
) -> StatsDatabase:
    """Factory function to create a StatsDatabase with configuration.

    :param enabled: Whether stats collection is enabled
    :param db_path: Path to database file
    :param retention_days: Data retention period in days
    :return: Configured StatsDatabase instance
    """
    db = StatsDatabase(db_path=db_path, enabled=enabled)

    # Run cleanup if enabled
    if enabled and retention_days > 0:
        try:
            db.cleanup_old_data(retention_days)
        except Exception as e:
            logger.warning(f"Stats cleanup failed (non-blocking): {e}")

    return db
