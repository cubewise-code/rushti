"""TM1 integration module for RushTI.

This module provides functionality to:
- Read task definitions from a TM1 cube
- Write execution results to TM1 Applications folder as CSV files
- Export results from SQLite to local CSV files

Usage:
    # Read taskfile from TM1
    python rushti run --tm1-instance tm1srv01 --workflow Sample_Optimal_Mode

    # Export results to CSV
    rushti stats export --workflow daily-etl --output results.csv

    # Auto-upload (when push_results=true in settings.ini)
    python rushti run --tasks tasks.txt
    # Results automatically uploaded to TM1 files as rushti_{workflow}_{run_id}.csv
"""

import configparser
import json
import logging
import shlex
from typing import Any, Dict, List, Optional

import pandas as pd
from TM1py import TM1Service

from rushti.stats import StatsDatabase
from rushti.taskfile import Taskfile, TaskDefinition, TaskfileMetadata, TaskfileSettings

logger = logging.getLogger(__name__)

# TM1 Applications folder for results
APPLICATIONS_FOLDER = "rushti"

# Measure elements that are inputs (for reading taskfiles)
INPUT_MEASURES = [
    "instance",
    "process",
    "parameters",
    "predecessors",
    "stage",
    "safe_retry",
    "timeout",
    "cancel_at_timeout",
    "require_predecessor_success",
    "succeed_on_minor_errors",
    "wait",  # Used in norm mode as sequence separator
]

# All measure elements (for writing results)
ALL_MEASURES = [
    "instance",
    "process",
    "parameters",
    "status",
    "start_time",
    "end_time",
    "duration_seconds",
    "retries",
    "retry_count",
    "error_message",
    "predecessors",
    "stage",
    "safe_retry",
    "timeout",
    "cancel_at_timeout",
    "require_predecessor_success",
    "succeed_on_minor_errors",
]


def connect_to_tm1_instance(
    instance_name: str,
    config_path: str,
) -> TM1Service:
    """Connect to a specific TM1 instance from config.ini.

    :param instance_name: TM1 instance name as defined in config.ini
    :param config_path: Path to config.ini file
    :return: Connected TM1Service instance
    :raises ValueError: If instance not found in config
    :raises ConnectionError: If connection fails
    """
    if not config_path:
        raise ValueError("config_path is required")

    config = configparser.ConfigParser()
    config.read(config_path, encoding="utf-8")

    if instance_name not in config.sections():
        raise ValueError(
            f"TM1 instance '{instance_name}' not found in {config_path}. "
            f"Available instances: {', '.join(config.sections())}"
        )

    try:
        params = dict(config[instance_name])
        params.pop("session_context", None)
        tm1 = TM1Service(**params)
        logger.info(f"Connected to TM1 instance: {instance_name}")
        return tm1
    except Exception as e:
        raise ConnectionError(f"Failed to connect to TM1 instance '{instance_name}': {e}")


def read_taskfile_from_tm1(
    tm1: TM1Service,
    workflow: str,
    cube_name: str = "rushti",
    dim_workflow: str = "rushti_workflow",
    dim_task: str = "rushti_task_id",
    dim_run: str = "rushti_run_id",
    dim_measure: str = "rushti_measure",
    mode: str = "norm",
) -> Taskfile:
    """Read task definitions from TM1 cube using MDX.

    Queries the cube for task definitions stored under the "Input" run_id
    element for the specified workflow.

    :param tm1: Connected TM1Service instance
    :param workflow: Element name in rushti_workflow dimension
    :param cube_name: Name of the cube to query (default: rushti)
    :param dim_workflow: Name of the workflow dimension
    :param dim_task: Name of the task dimension
    :param dim_run: Name of the run dimension
    :param dim_measure: Name of the measure dimension
    :param mode: Execution mode - "norm" (default) or "opt"
                 In "norm" mode, uses wait-based sequencing
                 In "opt" mode, uses explicit predecessors
    :return: Taskfile object with task definitions
    :raises ValueError: If no tasks found or cube doesn't exist
    """
    # Build MDX query
    mdx = f"""
    SELECT
        NON EMPTY
        {{TM1FILTERBYPATTERN({{[{dim_measure}].[{dim_measure}].MEMBERS}}, "Y","inputs")}}
        ON COLUMNS,
        NON EMPTY
        {{[{dim_task}].[{dim_task}].Members}}
        ON ROWS
    FROM [{cube_name}]
    WHERE
    (
        [{dim_run}].[{dim_run}].[Input],
        [{dim_workflow}].[{dim_workflow}].[{workflow}]
    )
    """

    logger.debug(f"Executing '{mdx}' query for workflow '{workflow}'")

    try:
        # Execute MDX and get DataFrame (shaped puts measures as columns)
        df = tm1.cells.execute_mdx_dataframe_shaped(mdx)
    except Exception as e:
        raise ValueError(f"Failed to read workflow '{workflow}' from TM1: {e}")

    if df.empty:
        raise ValueError(f"No tasks found for workflow '{workflow}' in cube '{cube_name}'")

    # Debug: Log DataFrame structure to diagnose parsing issues
    logger.debug(f"DataFrame shape: {df.shape}")
    logger.debug(f"DataFrame columns: {list(df.columns)}")
    logger.debug(f"DataFrame index: {list(df.index)}")
    if not df.empty:
        logger.debug(f"First row: {df.iloc[0].to_dict()}")

    # Convert DataFrame to TaskDefinition objects
    tasks = _dataframe_to_task_definitions(df, dim_task=dim_task, mode=mode)

    if not tasks:
        raise ValueError(f"No valid tasks found for workflow '{workflow}'")

    # Create Taskfile
    taskfile = Taskfile(
        version="2.0",
        metadata=TaskfileMetadata(
            workflow=workflow,
            name=f"TM1 Taskfile: {workflow}",
            description=f"Task definitions loaded from TM1 cube '{cube_name}'",
        ),
        settings=TaskfileSettings(),
        tasks=tasks,
    )

    logger.info(f"Loaded {len(tasks)} tasks from TM1 for workflow '{workflow}'")
    return taskfile


def _dataframe_to_task_definitions(
    df: pd.DataFrame,
    dim_task: str = "rushti_task_id",
    mode: str = "norm",
) -> List[TaskDefinition]:
    """Convert MDX result DataFrame to TaskDefinition objects.

    The DataFrame has the task dimension as a column and measures as other columns
    (from execute_mdx_dataframe_shaped).

    :param df: DataFrame from execute_mdx_dataframe_shaped
    :param dim_task: Name of the task dimension (column name in DataFrame)
    :param mode: Execution mode - "norm" or "opt"
                 In "norm" mode, uses wait-based sequencing (ignores predecessors)
                 In "opt" mode, uses explicit predecessors (ignores wait)
    :return: List of TaskDefinition objects
    """
    tasks = []
    # Track sequences for norm mode (sequence 0 is the first group)
    current_sequence = 0
    sequence_tasks: Dict[int, List[str]] = {0: []}

    # Iterate over rows (each row is a task or wait marker)
    for idx, row in df.iterrows():
        # Check if this is a wait marker row
        is_wait = _parse_bool(row.get("wait", False))

        if is_wait:
            if mode == "norm":
                # In norm mode, wait markers advance the sequence
                current_sequence += 1
                sequence_tasks[current_sequence] = []
                logger.debug(f"Wait marker encountered, advancing to sequence {current_sequence}")
            # Skip wait rows - they are markers only, not tasks
            continue

        # Extract task_id from the task dimension column
        task_id = str(row.get(dim_task, idx)).strip()
        if not task_id:
            task_id = str(len(tasks) + 1)

        # Skip if no instance or process defined (empty row)
        instance = str(row.get("instance", "")).strip()
        process = str(row.get("process", "")).strip()

        if not instance or not process:
            continue

        # Parse parameters (supports JSON or space-separated key=value)
        parameters_str = str(row.get("parameters", "")).strip()
        parameters = _parse_parameters_string(parameters_str)

        # Parse predecessors based on mode
        predecessors: List[str] = []
        if mode == "opt":
            # In opt mode, use explicit predecessors
            predecessors_str = str(row.get("predecessors", "")).strip()
            predecessors = [p.strip() for p in predecessors_str.split(",") if p.strip()]
        else:
            # In norm mode, predecessors will be set later based on sequences
            # Warn if predecessors are defined but will be ignored
            predecessors_str = str(row.get("predecessors", "")).strip()
            if predecessors_str:
                logger.warning(
                    f"Task '{task_id}': predecessors ignored in norm mode "
                    f"(use --mode opt to use explicit predecessors)"
                )

        # Parse boolean fields
        safe_retry = _parse_bool(row.get("safe_retry", False))
        cancel_at_timeout = _parse_bool(row.get("cancel_at_timeout", False))
        require_predecessor_success = _parse_bool(row.get("require_predecessor_success", False))
        succeed_on_minor_errors = _parse_bool(row.get("succeed_on_minor_errors", False))

        # Parse timeout (integer or None)
        timeout_val = row.get("timeout")
        timeout = int(timeout_val) if timeout_val and str(timeout_val).strip() else None

        # Parse stage
        stage = str(row.get("stage", "")).strip() or None

        task = TaskDefinition(
            id=task_id,
            instance=instance,
            process=process,
            parameters=parameters,
            predecessors=predecessors,
            stage=stage,
            safe_retry=safe_retry,
            timeout=timeout,
            cancel_at_timeout=cancel_at_timeout,
            require_predecessor_success=require_predecessor_success,
            succeed_on_minor_errors=succeed_on_minor_errors,
        )
        tasks.append(task)

        # Track task in current sequence for norm mode
        if mode == "norm":
            sequence_tasks[current_sequence].append(task_id)

    # In norm mode, set predecessors based on sequences
    if mode == "norm" and current_sequence > 0:
        task_by_id = {t.id: t for t in tasks}
        for seq in range(1, current_sequence + 1):
            prev_seq_tasks = sequence_tasks.get(seq - 1, [])
            for tid in sequence_tasks.get(seq, []):
                if tid in task_by_id:
                    task_by_id[tid].predecessors = prev_seq_tasks.copy()
        logger.debug(
            f"Applied wait-based sequencing: {current_sequence + 1} sequences, "
            f"{len(tasks)} tasks"
        )

    return tasks


def _parse_parameters_string(parameters_str: str) -> Dict[str, str]:
    """Parse a parameters string into a dictionary.

    Supports two formats:
    - **JSON**: ``{"pWaitSec": "1", "pLogOutput": "Yes"}``
    - **Space-separated key=value**: ``pWaitSec=1 pLogOutput="Yes"``

    JSON is tried first.  If it fails, the string is parsed as
    space-separated key=value pairs using :func:`shlex.split` (the same
    logic used for TXT task-file lines).

    :param parameters_str: Raw parameter string from TM1 cube cell
    :return: Dictionary mapping parameter names to values
    """
    if not parameters_str:
        return {}

    # Try JSON first
    try:
        result = json.loads(parameters_str)
        if isinstance(result, dict):
            return result
    except (json.JSONDecodeError, TypeError):
        pass

    # Fall back to space-separated key=value parsing (same as TXT taskfiles)
    params: Dict[str, str] = {}
    try:
        parts = shlex.split(parameters_str, posix=True)
    except ValueError:
        # shlex.split can fail on unmatched quotes
        logger.warning("Failed to parse parameters (bad quoting?): %s", parameters_str)
        return {}

    for part in parts:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        if key:
            params[key] = value

    return params


def _parse_bool(value: Any) -> bool:
    """Parse a value as boolean.

    Handles various representations: True, "true", "1", 1, etc.

    :param value: Value to parse
    :return: Boolean result
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes", "t")
    return False


def build_results_dataframe(
    stats_db: StatsDatabase,
    workflow: str,
    run_id: str,
) -> pd.DataFrame:
    """Build DataFrame from SQLite stats for a specific run.

    :param stats_db: StatsDatabase instance
    :param workflow: Workflow identifier
    :param run_id: Run identifier
    :return: DataFrame with task results
    """
    # Get results from database
    results = stats_db.get_run_results(run_id)

    if not results:
        logger.warning(f"No results found for run_id '{run_id}'")
        return pd.DataFrame()

    # Build rows for DataFrame
    rows = []
    for result in results:
        row = {
            "task_id": result.get("task_id", ""),
            "instance": result.get("instance", ""),
            "process": result.get("process", ""),
            "parameters": result.get("parameters", "{}"),
            "status": result.get("status", ""),
            "start_time": result.get("start_time", ""),
            "end_time": result.get("end_time", ""),
            "duration_seconds": result.get("duration_seconds", 0),
            "retries": result.get("retry_count", 0),
            "retry_count": result.get("retry_count", 0),
            "error_message": result.get("error_message", "") or "",
            "predecessors": result.get("predecessors", "") or "",
            "stage": result.get("stage", "") or "",
            "safe_retry": result.get("safe_retry", False),
            "timeout": result.get("timeout", "") or "",
            "cancel_at_timeout": result.get("cancel_at_timeout", False),
            "require_predecessor_success": result.get("require_predecessor_success", False),
            "succeed_on_minor_errors": result.get("succeed_on_minor_errors", False),
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    logger.debug(f"Built results DataFrame with {len(df)} rows")
    return df


def export_results_to_csv(
    stats_db: StatsDatabase,
    workflow: str,
    run_id: Optional[str],
    output_path: str,
) -> int:
    """Export results from SQLite to local CSV file.

    :param stats_db: StatsDatabase instance
    :param workflow: Workflow identifier
    :param run_id: Specific run ID, or None to export all runs
    :param output_path: Path to output CSV file
    :return: Number of rows exported
    """
    if run_id:
        # Export single run
        df = build_results_dataframe(stats_db, workflow, run_id)
        # Add run_id column
        df.insert(0, "run_id", run_id)
    else:
        # Export all runs for workflow
        runs = _get_runs_for_workflow(stats_db, workflow)
        if not runs:
            logger.warning(f"No runs found for workflow '{workflow}'")
            return 0

        all_dfs = []
        for run in runs:
            rid = run["run_id"]
            df = build_results_dataframe(stats_db, workflow, rid)
            if not df.empty:
                df.insert(0, "run_id", rid)
                all_dfs.append(df)

        if not all_dfs:
            return 0

        df = pd.concat(all_dfs, ignore_index=True)

    if df.empty:
        logger.warning("No results to export")
        return 0

    # Write to CSV
    df.to_csv(output_path, index=False)
    logger.info(f"Exported {len(df)} rows to {output_path}")
    return len(df)


def _get_runs_for_workflow(
    stats_db: StatsDatabase,
    workflow: str,
) -> List[Dict[str, Any]]:
    """Get all runs for a workflow from the database.

    :param stats_db: StatsDatabase instance
    :param workflow: Workflow identifier
    :return: List of run info dictionaries
    """
    return stats_db.get_runs_for_workflow(workflow)


def upload_results_to_tm1(
    tm1: TM1Service,
    workflow: str,
    run_id: str,
    results_df: pd.DataFrame,
) -> str:
    """Upload results CSV to TM1 files.

    File is uploaded as: rushti_{workflow}_{run_id}.csv

    :param tm1: Connected TM1Service instance
    :param workflow: Workflow identifier (used in filename)
    :param run_id: Run identifier (used in filename)
    :param results_df: DataFrame with results to upload
    :return: The filename that was uploaded
    :raises RuntimeError: If upload fails or results are empty
    """
    if results_df.empty:
        raise RuntimeError("No results to upload")

    # Simple filename: workflow_run_id.csv
    file_name = f"rushti_{workflow}_{run_id}.csv"

    try:
        # Add workflow and run_id columns for cube import
        df = results_df.copy()
        df.insert(0, "run_id", run_id)
        df.insert(0, "workflow", workflow)

        # Convert DataFrame to CSV bytes
        csv_content = df.to_csv(index=False).encode("utf-8")

        # Upload using tm1.files.create()
        tm1.files.create(file_name=file_name, file_content=csv_content)
        logger.info(f"Uploaded results to TM1: {file_name}")

    except Exception as e:
        raise RuntimeError(f"Failed to upload results to TM1: {e}")

    return file_name
