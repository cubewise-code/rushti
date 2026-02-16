"""Database administration utilities for RushTI stats database.

Provides simple commands for managing the SQLite stats database:
- View statistics and summaries
- List taskfiles, runs, and tasks
- Clear/delete data with safety confirmations
- Export data to CSV
- Maintenance operations (vacuum, cleanup)
"""

import csv
import json
import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from rushti.stats import DEFAULT_DB_PATH

logger = logging.getLogger(__name__)


def get_db_stats(db_path: str = DEFAULT_DB_PATH) -> Dict[str, Any]:
    """Get overall database statistics.

    :param db_path: Path to SQLite database
    :return: Dictionary with statistics
    """
    if not Path(db_path).exists():
        return {"exists": False, "message": f"Database not found at {db_path}"}

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Total records
    cursor.execute("SELECT COUNT(*) FROM task_results")
    total_records = cursor.fetchone()[0]

    # Unique workflows
    cursor.execute("SELECT COUNT(DISTINCT workflow) FROM task_results")
    workflow_count = cursor.fetchone()[0]

    # Unique runs
    cursor.execute("SELECT COUNT(DISTINCT run_id) FROM task_results")
    run_count = cursor.fetchone()[0]

    # Unique task signatures
    cursor.execute("SELECT COUNT(DISTINCT task_signature) FROM task_results")
    signature_count = cursor.fetchone()[0]

    # Date range
    cursor.execute("SELECT MIN(start_time), MAX(start_time) FROM task_results")
    date_range = cursor.fetchone()

    # Database file size
    db_size = Path(db_path).stat().st_size

    # Success rate
    cursor.execute("""
        SELECT
            COUNT(CASE WHEN status = 'Success' THEN 1 END) as successes,
            COUNT(*) as total
        FROM task_results
    """)
    success_data = cursor.fetchone()
    success_rate = (success_data[0] / success_data[1] * 100) if success_data[1] > 0 else 0

    conn.close()

    return {
        "exists": True,
        "path": db_path,
        "size_bytes": db_size,
        "size_mb": round(db_size / 1024 / 1024, 2),
        "total_records": total_records,
        "workflow_count": workflow_count,
        "run_count": run_count,
        "unique_tasks": signature_count,
        "date_range": {"first": date_range[0], "last": date_range[1]},
        "success_rate": round(success_rate, 1),
    }


def get_workflow_stats(workflow: str, db_path: str = DEFAULT_DB_PATH) -> Dict[str, Any]:
    """Get statistics for a specific workflow.

    :param workflow: Workflow name to query
    :param db_path: Path to SQLite database
    :return: Dictionary with workflow statistics
    """
    if not Path(db_path).exists():
        return {"exists": False, "message": f"Database not found at {db_path}"}

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check if workflow exists
    cursor.execute("SELECT COUNT(*) FROM task_results WHERE workflow = ?", (workflow,))
    total_records = cursor.fetchone()[0]

    if total_records == 0:
        conn.close()
        return {"exists": False, "message": f"No data found for workflow: {workflow}"}

    # Unique runs
    cursor.execute(
        "SELECT COUNT(DISTINCT run_id) FROM task_results WHERE workflow = ?", (workflow,)
    )
    run_count = cursor.fetchone()[0]

    # Unique tasks
    cursor.execute(
        "SELECT COUNT(DISTINCT task_signature) FROM task_results WHERE workflow = ?", (workflow,)
    )
    task_count = cursor.fetchone()[0]

    # Date range
    cursor.execute(
        """
        SELECT MIN(start_time), MAX(start_time)
        FROM task_results
        WHERE workflow = ?
    """,
        (workflow,),
    )
    date_range = cursor.fetchone()

    # Success rate
    cursor.execute(
        """
        SELECT
            COUNT(CASE WHEN status = 'Success' THEN 1 END) as successes,
            COUNT(*) as total
        FROM task_results
        WHERE workflow = ?
    """,
        (workflow,),
    )
    success_data = cursor.fetchone()
    success_rate = (success_data[0] / success_data[1] * 100) if success_data[1] > 0 else 0

    # Average duration
    cursor.execute(
        """
        SELECT AVG(duration_seconds), MIN(duration_seconds), MAX(duration_seconds)
        FROM task_results
        WHERE workflow = ? AND status = 'Success'
    """,
        (workflow,),
    )
    duration_stats = cursor.fetchone()

    conn.close()

    return {
        "exists": True,
        "workflow": workflow,
        "total_records": total_records,
        "run_count": run_count,
        "unique_tasks": task_count,
        "date_range": {"first": date_range[0], "last": date_range[1]},
        "success_rate": round(success_rate, 1),
        "duration": {
            "avg": round(duration_stats[0], 2) if duration_stats[0] else 0,
            "min": round(duration_stats[1], 2) if duration_stats[1] else 0,
            "max": round(duration_stats[2], 2) if duration_stats[2] else 0,
        },
    }


def list_workflows(db_path: str = DEFAULT_DB_PATH) -> List[Dict[str, Any]]:
    """List all workflows in database with summary statistics.

    :param db_path: Path to SQLite database
    :return: List of workflow summaries
    """
    if not Path(db_path).exists():
        return []

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            workflow,
            COUNT(*) as record_count,
            COUNT(DISTINCT run_id) as run_count,
            COUNT(DISTINCT task_signature) as task_count,
            MAX(start_time) as last_run
        FROM task_results
        GROUP BY workflow
        ORDER BY last_run DESC
    """)

    workflows = []
    for row in cursor.fetchall():
        workflows.append(
            {
                "workflow": row[0],
                "record_count": row[1],
                "run_count": row[2],
                "task_count": row[3],
                "last_run": row[4],
            }
        )

    conn.close()
    return workflows


def list_runs(
    workflow: str, db_path: str = DEFAULT_DB_PATH, limit: int = 20
) -> List[Dict[str, Any]]:
    """List runs for a specific workflow.

    :param workflow: Workflow name to query
    :param db_path: Path to SQLite database
    :param limit: Maximum number of runs to return
    :return: List of run summaries
    """
    if not Path(db_path).exists():
        return []

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Query from runs table for run-level data, with task stats from task_results
    cursor.execute(
        """
        SELECT
            r.run_id,
            r.start_time,
            r.end_time,
            r.duration_seconds,
            r.task_count,
            r.success_count,
            (SELECT SUM(duration_seconds) FROM task_results WHERE run_id = r.run_id) as total_task_duration
        FROM runs r
        WHERE r.workflow = ?
        ORDER BY r.start_time DESC
        LIMIT ?
    """,
        (workflow, limit),
    )

    runs = []
    for row in cursor.fetchall():
        task_count = row[4] or 0
        success_count = row[5] or 0
        success_rate = (success_count / task_count * 100) if task_count > 0 else 0
        runs.append(
            {
                "run_id": row[0],
                "start_time": row[1],
                "end_time": row[2],
                "duration_seconds": round(row[3], 2) if row[3] else None,
                "task_count": task_count,
                "success_count": success_count,
                "success_rate": round(success_rate, 1),
                "total_task_duration": round(row[6], 2) if row[6] else 0,
            }
        )

    conn.close()
    return runs


def list_tasks(workflow: str, db_path: str = DEFAULT_DB_PATH) -> List[Dict[str, Any]]:
    """List unique tasks for a specific workflow.

    :param workflow: Workflow name to query
    :param db_path: Path to SQLite database
    :return: List of task summaries
    """
    if not Path(db_path).exists():
        return []

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            task_signature,
            task_id,
            instance,
            process,
            COUNT(*) as run_count,
            AVG(duration_seconds) as avg_duration,
            COUNT(CASE WHEN status = 'Success' THEN 1 END) as success_count
        FROM task_results
        WHERE workflow = ?
        GROUP BY task_signature
        ORDER BY task_id
    """,
        (workflow,),
    )

    tasks = []
    for row in cursor.fetchall():
        success_rate = (row[6] / row[4] * 100) if row[4] > 0 else 0
        tasks.append(
            {
                "task_signature": row[0],
                "task_id": row[1],
                "instance": row[2],
                "process": row[3],
                "run_count": row[4],
                "avg_duration": round(row[5], 2) if row[5] else 0,
                "success_rate": round(success_rate, 1),
            }
        )

    conn.close()
    return tasks


def clear_workflow(workflow: str, db_path: str = DEFAULT_DB_PATH, dry_run: bool = False) -> int:
    """Delete all data for a specific workflow.

    :param workflow: Workflow name to delete
    :param db_path: Path to SQLite database
    :param dry_run: If True, only count records without deleting
    :return: Number of records deleted (or would be deleted in dry_run)
    """
    if not Path(db_path).exists():
        return 0

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Count records to be deleted
    cursor.execute("SELECT COUNT(*) FROM task_results WHERE workflow = ?", (workflow,))
    count = cursor.fetchone()[0]

    if not dry_run and count > 0:
        cursor.execute("DELETE FROM task_results WHERE workflow = ?", (workflow,))
        conn.commit()
        logger.info(f"Deleted {count} records for workflow: {workflow}")

    conn.close()
    return count


def clear_run(run_id: str, db_path: str = DEFAULT_DB_PATH, dry_run: bool = False) -> int:
    """Delete all data for a specific run.

    :param run_id: Run ID to delete
    :param db_path: Path to SQLite database
    :param dry_run: If True, only count records without deleting
    :return: Number of records deleted (or would be deleted in dry_run)
    """
    if not Path(db_path).exists():
        return 0

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Count records to be deleted
    cursor.execute("SELECT COUNT(*) FROM task_results WHERE run_id = ?", (run_id,))
    count = cursor.fetchone()[0]

    if not dry_run and count > 0:
        cursor.execute("DELETE FROM task_results WHERE run_id = ?", (run_id,))
        conn.commit()
        logger.info(f"Deleted {count} records for run: {run_id}")

    conn.close()
    return count


def clear_before_date(
    before_date: str, db_path: str = DEFAULT_DB_PATH, dry_run: bool = False
) -> int:
    """Delete all data before a specific date.

    :param before_date: ISO date string (YYYY-MM-DD)
    :param db_path: Path to SQLite database
    :param dry_run: If True, only count records without deleting
    :return: Number of records deleted (or would be deleted in dry_run)
    """
    if not Path(db_path).exists():
        return 0

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Count records to be deleted
    cursor.execute("SELECT COUNT(*) FROM task_results WHERE start_time < ?", (before_date,))
    count = cursor.fetchone()[0]

    if not dry_run and count > 0:
        cursor.execute("DELETE FROM task_results WHERE start_time < ?", (before_date,))
        conn.commit()
        logger.info(f"Deleted {count} records before {before_date}")

    conn.close()
    return count


def clear_all(db_path: str = DEFAULT_DB_PATH, dry_run: bool = False) -> int:
    """Delete all data from the database.

    :param db_path: Path to SQLite database
    :param dry_run: If True, only count records without deleting
    :return: Number of records deleted (or would be deleted in dry_run)
    """
    if not Path(db_path).exists():
        return 0

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Count records to be deleted
    cursor.execute("SELECT COUNT(*) FROM task_results")
    count = cursor.fetchone()[0]

    if not dry_run and count > 0:
        cursor.execute("DELETE FROM task_results")
        conn.commit()
        logger.info(f"Deleted all {count} records from database")

    conn.close()
    return count


def vacuum_database(db_path: str = DEFAULT_DB_PATH) -> Tuple[int, int]:
    """Optimize database by reclaiming unused space.

    :param db_path: Path to SQLite database
    :return: Tuple of (size_before, size_after) in bytes
    """
    if not Path(db_path).exists():
        return (0, 0)

    size_before = Path(db_path).stat().st_size

    conn = sqlite3.connect(db_path)
    conn.execute("VACUUM")
    conn.close()

    size_after = Path(db_path).stat().st_size

    logger.info(
        f"Database vacuumed: {size_before} -> {size_after} bytes "
        f"({round((size_before - size_after) / 1024 / 1024, 2)} MB saved)"
    )

    return (size_before, size_after)


def export_to_csv(
    output_path: str,
    workflow: Optional[str] = None,
    run_id: Optional[str] = None,
    db_path: str = DEFAULT_DB_PATH,
) -> int:
    """Export task results to CSV file.

    :param output_path: Path to output CSV file
    :param workflow: Optional workflow name filter
    :param run_id: Optional run ID filter
    :param db_path: Path to SQLite database
    :return: Number of records exported
    """
    if not Path(db_path).exists():
        return 0

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Build query with optional filters
    query = "SELECT * FROM task_results"
    params = []

    if workflow and run_id:
        query += " WHERE workflow = ? AND run_id = ?"
        params = [workflow, run_id]
    elif workflow:
        query += " WHERE workflow = ?"
        params = [workflow]
    elif run_id:
        query += " WHERE run_id = ?"
        params = [run_id]

    query += " ORDER BY start_time, task_id"

    cursor.execute(query, params)

    # Get column names
    columns = [description[0] for description in cursor.description]

    # Write to CSV
    with open(output_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)

        count = 0
        for row in cursor.fetchall():
            writer.writerow(row)
            count += 1

    conn.close()
    logger.info(f"Exported {count} records to {output_path}")

    return count


def show_run_details(run_id: str, db_path: str = DEFAULT_DB_PATH) -> Dict[str, Any]:
    """Get detailed information about a specific run.

    :param run_id: Run ID to query
    :param db_path: Path to SQLite database
    :return: Dictionary with run details
    """
    if not Path(db_path).exists():
        return {"exists": False, "message": f"Database not found at {db_path}"}

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get run info from runs table (includes duration_seconds)
    cursor.execute(
        """
        SELECT workflow, start_time, end_time, duration_seconds,
               task_count, success_count, failure_count
        FROM runs
        WHERE run_id = ?
    """,
        (run_id,),
    )
    run_row = cursor.fetchone()

    # If not found in runs table, fall back to task_results aggregation
    if not run_row:
        cursor.execute(
            """
            SELECT
                workflow,
                MIN(start_time) as start_time,
                MAX(end_time) as end_time,
                COUNT(*) as task_count,
                COUNT(CASE WHEN status = 'Success' THEN 1 END) as success_count,
                COUNT(CASE WHEN status != 'Success' THEN 1 END) as error_count,
                SUM(duration_seconds) as total_task_duration
            FROM task_results
            WHERE run_id = ?
            GROUP BY workflow
        """,
            (run_id,),
        )
        summary = cursor.fetchone()

        if not summary:
            conn.close()
            return {"exists": False, "message": f"No data found for run: {run_id}"}

        workflow = summary[0]
        start_time = summary[1]
        end_time = summary[2]
        duration_seconds = None  # Not available from aggregation
        task_count = summary[3]
        success_count = summary[4]
        error_count = summary[5]
        total_task_duration = summary[6]
    else:
        workflow = run_row[0]
        start_time = run_row[1]
        end_time = run_row[2]
        duration_seconds = run_row[3]
        task_count = run_row[4] or 0
        success_count = run_row[5] or 0
        error_count = run_row[6] or 0

        # Get total task duration from task_results
        cursor.execute(
            """
            SELECT SUM(duration_seconds)
            FROM task_results WHERE run_id = ?
        """,
            (run_id,),
        )
        total_task_duration = cursor.fetchone()[0]

    # Get task breakdown
    cursor.execute(
        """
        SELECT task_id, status, duration_seconds, error_message
        FROM task_results
        WHERE run_id = ?
        ORDER BY start_time
    """,
        (run_id,),
    )

    tasks = []
    for row in cursor.fetchall():
        tasks.append(
            {
                "task_id": row[0],
                "status": row[1],
                "duration": round(row[2], 2) if row[2] else 0,
                "error": row[3] if row[3] else None,
            }
        )

    conn.close()

    success_rate = (success_count / task_count * 100) if task_count > 0 else 0

    return {
        "exists": True,
        "run_id": run_id,
        "workflow": workflow,
        "start_time": start_time,
        "end_time": end_time,
        "duration_seconds": round(duration_seconds, 2) if duration_seconds else None,
        "task_count": task_count,
        "success_count": success_count,
        "error_count": error_count,
        "success_rate": round(success_rate, 1),
        "total_task_duration": round(total_task_duration, 2) if total_task_duration else 0,
        "tasks": tasks,
    }


def show_task_history(
    task_signature: str, db_path: str = DEFAULT_DB_PATH, limit: int = 20
) -> Dict[str, Any]:
    """Get execution history for a specific task signature.

    :param task_signature: Task signature to query
    :param db_path: Path to SQLite database
    :param limit: Maximum number of executions to return
    :return: Dictionary with task history
    """
    if not Path(db_path).exists():
        return {"exists": False, "message": f"Database not found at {db_path}"}

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get task details
    cursor.execute(
        """
        SELECT task_id, instance, process, parameters
        FROM task_results
        WHERE task_signature = ?
        LIMIT 1
    """,
        (task_signature,),
    )

    task_info = cursor.fetchone()

    if not task_info:
        conn.close()
        return {"exists": False, "message": f"No data found for task signature: {task_signature}"}

    # Get execution history
    cursor.execute(
        """
        SELECT run_id, start_time, end_time, duration_seconds, status
        FROM task_results
        WHERE task_signature = ?
        ORDER BY start_time DESC
        LIMIT ?
    """,
        (task_signature, limit),
    )

    executions = []
    for row in cursor.fetchall():
        executions.append(
            {
                "run_id": row[0],
                "start_time": row[1],
                "end_time": row[2],
                "duration": round(row[3], 2) if row[3] else 0,
                "status": row[4],
            }
        )

    conn.close()

    return {
        "exists": True,
        "task_signature": task_signature,
        "task_id": task_info[0],
        "instance": task_info[1],
        "process": task_info[2],
        "parameters": json.loads(task_info[3]) if task_info[3] else {},
        "execution_count": len(executions),
        "executions": executions,
    }


def get_visualization_data(workflow: str, db_path: str = DEFAULT_DB_PATH) -> Dict[str, Any]:
    """Get all data needed for the HTML dashboard visualization.

    Returns all runs and their task results for a workflow in a single
    efficient query batch.

    :param workflow: Workflow name to query
    :param db_path: Path to SQLite database
    :return: Dictionary with 'runs' and 'task_results' lists, or error info
    """
    if not Path(db_path).exists():
        return {"exists": False, "message": f"Database not found: {db_path}"}

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get all runs for this taskfile
    cursor.execute(
        """
        SELECT run_id, workflow, taskfile_path, start_time, end_time,
               duration_seconds, status, task_count, success_count, failure_count,
               taskfile_name, taskfile_description, taskfile_author,
               max_workers, retries, result_file, exclusive, optimize
        FROM runs
        WHERE workflow = ?
        ORDER BY start_time DESC
        """,
        (workflow,),
    )
    runs_rows = cursor.fetchall()

    if not runs_rows:
        conn.close()
        return {
            "exists": False,
            "message": f"No runs found for workflow: {workflow}",
        }

    runs = []
    run_ids = []
    for row in runs_rows:
        run = dict(row)
        # Convert SQLite integers to Python booleans
        for field in ["exclusive", "optimize"]:
            if field in run and run[field] is not None:
                run[field] = bool(run[field])
        runs.append(run)
        run_ids.append(run["run_id"])

    # Get all task results for these runs
    placeholders = ",".join("?" * len(run_ids))
    cursor.execute(
        f"""
        SELECT run_id, task_id, task_signature, instance, process, parameters,
               status, start_time, end_time, duration_seconds, retry_count,
               error_message, predecessors, stage
        FROM task_results
        WHERE run_id IN ({placeholders})
        ORDER BY run_id, id
        """,
        run_ids,
    )

    task_results = []
    for row in cursor.fetchall():
        result = dict(row)
        task_results.append(result)

    conn.close()

    return {
        "exists": True,
        "workflow": workflow,
        "runs": runs,
        "task_results": task_results,
    }
