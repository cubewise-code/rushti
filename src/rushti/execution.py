"""
RushTI task execution engine.

This module contains the core execution logic for running TM1 processes,
including:
- TM1 service setup and connection management
- Process execution with retry and timeout support
- DAG-based task scheduling with parallel workers
- Task validation against TM1 server metadata
- Structured execution logging and statistics collection
"""

import asyncio
import configparser
import dataclasses
import functools
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

import keyring
from TM1py import TM1Service
from TM1py.Exceptions import TM1pyTimeout

from rushti.task import Task, OptimizedTask
from rushti.dag import DAG
from rushti.messages import (
    MSG_PROCESS_EXECUTE,
    MSG_PROCESS_SUCCESS,
    MSG_PROCESS_FAIL_INSTANCE_NOT_IN_CONFIG_FILE,
    MSG_PROCESS_FAIL_WITH_ERROR_FILE,
    MSG_PROCESS_HAS_MINOR_ERRORS,
    MSG_PROCESS_FAIL_UNEXPECTED,
    MSG_PROCESS_ABORTED_FAILED_PREDECESSOR,
    MSG_PROCESS_ABORTED_UNCOMPLETE_PREDECESSOR,
    MSG_PROCESS_NOT_EXISTS,
    MSG_PROCESS_PARAMS_INCORRECT,
    MSG_PROCESS_TIMEOUT,
)
from rushti.parsing import get_instances_from_tasks_file
from rushti.exclusive import build_session_context

if TYPE_CHECKING:
    from rushti.checkpoint import CheckpointManager
    from rushti.optimizer import TaskOptimizer

APP_NAME = "RushTI"

logger = logging.getLogger()


@dataclasses.dataclass
class ExecutionContext:
    """Bundles the mutable per-run state that was previously stored as module globals.

    Created by ``cli.main()`` before each run and threaded through to every
    function that needs it, making execution fully re-entrant.
    """

    #: Structured execution logger (ExecutionLogger or None)
    execution_logger: Optional[object] = None
    #: SQLite statistics database (StatsDatabase or None)
    stats_db: Optional[object] = None
    #: Lock protecting ``stats_data`` list
    stats_data_lock: threading.Lock = dataclasses.field(default_factory=threading.Lock)
    #: Collected task stats for batch write at end of run
    stats_data: list = dataclasses.field(default_factory=list)
    #: Execution results per task id (for predecessor checking)
    task_execution_results: dict = dataclasses.field(default_factory=dict)


def _collect_task_stats(
    ctx: ExecutionContext,
    task: Task,
    success: bool,
    start_time: datetime,
    end_time: datetime,
    retry_count: int = 0,
    error_message: Optional[str] = None,
) -> None:
    """Collect task execution stats for batch write at end of run.

    Thread-safe collection of task statistics into ctx.stats_data list.
    Called after each task completes (success or failure).

    :param ctx: The current execution context
    :param task: The executed task
    :param success: Whether the task succeeded
    :param start_time: Task start timestamp
    :param end_time: Task end timestamp
    :param retry_count: Number of retries performed
    :param error_message: Error message if task failed
    """
    if not ctx.stats_db or not ctx.stats_db.enabled:
        return

    with ctx.stats_data_lock:
        stats_entry = {
            "run_id": ctx.execution_logger.run_id if ctx.execution_logger else "",
            "workflow": ctx.execution_logger.workflow if ctx.execution_logger else None,
            "task_id": task.id,
            "instance": task.instance_name,
            "process": task.process_name,
            "parameters": task.parameters if isinstance(task.parameters, dict) else {},
            "success": success,
            "start_time": start_time,
            "end_time": end_time,
            "retry_count": retry_count,
            "predecessors": getattr(task, "predecessors", None),
            "stage": getattr(task, "stage", None),
            "safe_retry": getattr(task, "safe_retry", None),
            "timeout": getattr(task, "timeout", None),
            "cancel_at_timeout": getattr(task, "cancel_at_timeout", None),
            "require_predecessor_success": getattr(task, "require_predecessor_success", None),
            "succeed_on_minor_errors": getattr(task, "succeed_on_minor_errors", None),
        }
        if error_message:
            stats_entry["error_message"] = error_message
        ctx.stats_data.append(stats_entry)


def setup_tm1_services(
    max_workers: int,
    tasks_file_path: Optional[str] = None,
    workflow: str = "",
    exclusive: bool = False,
    config_path: Optional[str] = None,
    tm1_instances: Optional[set] = None,
) -> Tuple[dict, dict]:
    """Return Dictionary with TM1ServerName (as in config.ini) : Instantiated TM1Service

    :param max_workers: Maximum number of parallel workers (used for connection pool size)
    :param tasks_file_path: Path to the tasks file (can be None if tm1_instances provided)
    :param workflow: Workflow identifier for session context
    :param exclusive: Whether running in exclusive mode
    :param config_path: Optional path to config.ini file (defaults to resolved CONFIG)
    :param tm1_instances: Optional set of TM1 instance names (used when reading from TM1)
    :return: Dictionary server_names and TM1py.TM1Service instances pairs
    """
    if config_path is None:
        from rushti.cli import CONFIG

        config_path = CONFIG

    config_file = config_path
    if not os.path.isfile(config_file):
        raise ValueError("{config} does not exist".format(config=config_file))

    # Use provided instances or extract from file
    if tm1_instances is not None:
        tm1_instances_in_tasks = tm1_instances
    elif tasks_file_path is not None:
        tm1_instances_in_tasks = get_instances_from_tasks_file(tasks_file_path)
    else:
        raise ValueError("Either tm1_instances or tasks_file_path must be provided")
    tm1_preserve_connections = dict()
    tm1_services = dict()

    # Build session context for RushTI identification
    session_context = build_session_context(workflow, exclusive)

    # parse .ini
    config = configparser.ConfigParser()
    config.read(config_file, encoding="utf-8")
    # build tm1_services dictionary
    for tm1_server_name, params in config.items():
        if tm1_server_name not in tm1_instances_in_tasks:
            continue

        # handle default values from configparser
        if tm1_server_name != config.default_section:
            try:
                use_keyring = config.getboolean(tm1_server_name, "use_keyring", fallback=False)
                if use_keyring:
                    password = keyring.get_password(tm1_server_name, params.get("user"))
                    params["password"] = password

                connection_file = config.get(tm1_server_name, "connection_file", fallback=None)

                # restore connection from file. In practice faster than creating a new one
                if connection_file:
                    tm1_preserve_connections[tm1_server_name] = True
                    try:
                        connection_file_path = Path(__file__).parent.parent.parent / connection_file
                        tm1_services[tm1_server_name] = TM1Service.restore_from_file(
                            file_name=connection_file_path
                        )

                    except Exception as e:
                        logger.warning(
                            "Failed to restore connection from file. Error: {error}".format(
                                error=str(e)
                            )
                        )

                # case no connection file provided or connection file expired
                if tm1_server_name not in tm1_services:
                    params.pop("session_context", None)
                    tm1_services[tm1_server_name] = TM1Service(
                        **params,
                        session_context=session_context,
                        connection_pool_size=max_workers,
                    )

                if connection_file:
                    # implicitly re-connects if session is timed out
                    tm1_services[tm1_server_name].server.get_product_version()
                    tm1_services[tm1_server_name].save_to_file(
                        file_name=Path(__file__).parent.parent.parent / connection_file
                    )

            # Instance not running, Firewall or wrong connection parameters
            except Exception as e:
                logger.error(
                    "TM1 instance {} not accessible. Error: {}".format(tm1_server_name, str(e))
                )

    return tm1_services, tm1_preserve_connections


def execute_process_with_retries(tm1: TM1Service, task: Task, retries: int):
    for attempt in range(retries + 1):
        try:
            # Execute process using TM1py's native timeout and cancel_at_timeout support
            success, status, error_log_file = tm1.processes.execute_with_return(
                process_name=task.process_name,
                timeout=task.timeout,
                cancel_at_timeout=task.cancel_at_timeout,
                **task.parameters,
            )

            # Handle minor errors
            if not success and task.succeed_on_minor_errors and status == "HasMinorErrors":
                success = True
                msg = MSG_PROCESS_HAS_MINOR_ERRORS.format(
                    process=task.process_name,
                    parameters=task.parameters,
                    status=status,
                    retries=retries,
                    instance=task.instance_name,
                    error_file=error_log_file,
                )
                logging.warning(msg)

            if success:
                return success, status, error_log_file, attempt

        except TM1pyTimeout:
            # Timeout should not be retried, raise immediately
            raise

        except Exception as e:
            if attempt == retries:
                # Raise exception on the final attempt
                raise e

    # If all retries fail
    return False, status, error_log_file, retries


def update_task_execution_results(func):
    @functools.wraps(func)
    def wrapper(ctx: ExecutionContext, task: Task, *args, **kwargs):
        task_success = False
        try:
            task_success = func(ctx, task, *args, **kwargs)

        finally:
            # two optimized tasks can have the same id !
            previous_task_success = ctx.task_execution_results.get(task.id, True)
            ctx.task_execution_results[task.id] = previous_task_success and task_success

        return task_success

    return wrapper


@update_task_execution_results
def execute_task(
    ctx: ExecutionContext, task: Task, retries: int, tm1_services: Dict[str, TM1Service]
) -> bool:
    """Execute one line from the txt file
    :param task:
    :param retries:
    :param tm1_services:
    :return:
    """

    # check predecessors success
    if isinstance(task, OptimizedTask) and task.require_predecessor_success:
        predecessors_ok = verify_predecessors_ok(ctx, task)
        if not predecessors_ok:
            return False

    if task.instance_name not in tm1_services:
        msg = MSG_PROCESS_FAIL_INSTANCE_NOT_IN_CONFIG_FILE.format(
            process_name=task.process_name, instance_name=task.instance_name
        )
        logger.error(msg)
        return False

    tm1 = tm1_services[task.instance_name]
    # Execute it - include stage in log if present
    stage_info = f" [stage: {task.stage}]" if task.stage else ""
    msg = (
        MSG_PROCESS_EXECUTE.format(
            process_name=task.process_name,
            parameters=task.parameters,
            instance_name=task.instance_name,
        )
        + stage_info
    )
    logger.info(msg)
    start_time = datetime.now()

    try:
        success, status, error_log_file, attempts = execute_process_with_retries(
            tm1=tm1, task=task, retries=retries
        )
        end_time = datetime.now()
        elapsed_time = end_time - start_time

        if success:
            msg = MSG_PROCESS_SUCCESS
            msg = msg.format(
                process=task.process_name,
                parameters=task.parameters,
                instance=task.instance_name,
                retries=attempts,
                time=elapsed_time,
            )
            logger.info(msg)

            # Log to structured execution logger
            if ctx.execution_logger:
                ctx.execution_logger.log_task_execution(
                    task_id=task.id,
                    instance=task.instance_name,
                    process=task.process_name,
                    parameters=task.parameters if isinstance(task.parameters, dict) else {},
                    success=True,
                    start_time=start_time,
                    end_time=end_time,
                    retry_count=attempts,
                )

            _collect_task_stats(
                ctx,
                task,
                success=True,
                start_time=start_time,
                end_time=end_time,
                retry_count=attempts,
            )

            return True

        else:
            msg = MSG_PROCESS_FAIL_WITH_ERROR_FILE.format(
                process=task.process_name,
                parameters=task.parameters,
                status=status,
                instance=task.instance_name,
                retries=attempts,
                time=elapsed_time,
                error_file=error_log_file,
            )
            logger.error(msg)

            # Log to structured execution logger
            if ctx.execution_logger:
                ctx.execution_logger.log_task_execution(
                    task_id=task.id,
                    instance=task.instance_name,
                    process=task.process_name,
                    parameters=task.parameters if isinstance(task.parameters, dict) else {},
                    success=False,
                    start_time=start_time,
                    end_time=end_time,
                    retry_count=attempts,
                    error_message=f"Status: {status}, Error file: {error_log_file}",
                )

            _collect_task_stats(
                ctx,
                task,
                success=False,
                start_time=start_time,
                end_time=end_time,
                retry_count=attempts,
                error_message=f"Status: {status}, Error file: {error_log_file}",
            )

            return False

    except TM1pyTimeout as e:
        # TM1py handles timeout and cancellation natively
        end_time = datetime.now()
        elapsed_time = end_time - start_time
        msg = MSG_PROCESS_TIMEOUT.format(
            process=task.process_name,
            parameters=task.parameters,
            timeout=task.timeout,
            instance=task.instance_name,
            time=elapsed_time,
        )
        logger.error(msg)
        if ctx.execution_logger:
            ctx.execution_logger.log_task_execution(
                task_id=task.id,
                instance=task.instance_name,
                process=task.process_name,
                parameters=task.parameters if isinstance(task.parameters, dict) else {},
                success=False,
                start_time=start_time,
                end_time=end_time,
                retry_count=0,
                error_message=f"Task execution timed out: {str(e)}",
            )
        _collect_task_stats(
            ctx,
            task,
            success=False,
            start_time=start_time,
            end_time=end_time,
            retry_count=0,
            error_message=f"Task execution timed out: {str(e)}",
        )
        return False

    except Exception as e:
        end_time = datetime.now()
        elapsed_time = end_time - start_time
        msg = MSG_PROCESS_FAIL_UNEXPECTED.format(
            process=task.process_name,
            parameters=task.parameters,
            error=str(e),
            time=elapsed_time,
        )
        logger.error(msg)

        # Log to structured execution logger
        if ctx.execution_logger:
            ctx.execution_logger.log_task_execution(
                task_id=task.id,
                instance=task.instance_name,
                process=task.process_name,
                parameters=task.parameters if isinstance(task.parameters, dict) else {},
                success=False,
                start_time=start_time,
                end_time=end_time,
                retry_count=0,
                error_message=str(e),
            )

        _collect_task_stats(
            ctx,
            task,
            success=False,
            start_time=start_time,
            end_time=end_time,
            retry_count=0,
            error_message=str(e),
        )

        return False


def verify_predecessors_ok(ctx: ExecutionContext, task: OptimizedTask) -> bool:
    for predecessor_id in task.predecessors:
        if predecessor_id not in ctx.task_execution_results:
            msg = MSG_PROCESS_ABORTED_UNCOMPLETE_PREDECESSOR.format(
                instance=task.instance_name,
                process=task.process_name,
                parameters=task.parameters,
                predecessor=predecessor_id,
            )
            logger.error(msg)
            return False

        if not ctx.task_execution_results[predecessor_id]:
            msg = MSG_PROCESS_ABORTED_FAILED_PREDECESSOR.format(
                instance=task.instance_name,
                process=task.process_name,
                parameters=task.parameters,
                predecessor=predecessor_id,
            )
            logger.error(msg)
            return False

    return True


def validate_tasks(tasks: List[Task], tm1_services: Dict[str, TM1Service]) -> bool:
    validated_tasks = []
    validation_ok = True

    tasks = [task for task in tasks if isinstance(task, Task)]  # --> ignore Wait(s)
    for task in tasks:
        current_task = {
            "instance": task.instance_name,
            "process": task.process_name,
            "parameters": task.parameters.keys(),
        }

        tm1 = tm1_services[task.instance_name]

        # avoid repeated validations
        if current_task["process"] in validated_tasks:
            continue

        # check for process existence
        if not tm1.processes.exists(task.process_name):
            msg = MSG_PROCESS_NOT_EXISTS.format(
                process=task.process_name, instance=task.instance_name
            )
            logger.error(msg)
            validated_tasks.append(current_task["process"])
            validation_ok = False
            continue

        # check for parameters
        task_params = task.parameters.keys()
        if task_params:
            process_params = [
                param["Name"] for param in tm1.processes.get(task.process_name).parameters
            ]

            # check for missing parameter names
            missing_params = [param for param in task_params if param not in process_params]
            if len(missing_params) > 0:
                msg = MSG_PROCESS_PARAMS_INCORRECT.format(
                    process=task.process_name,
                    parameters=missing_params,
                    instance=task.instance_name,
                )
                logger.error(msg)
                validation_ok = False

        validated_tasks.append(current_task["process"])

    return validation_ok


async def work_through_tasks_dag(
    ctx: ExecutionContext,
    dag: DAG,
    max_workers: int,
    retries: int,
    tm1_services: dict,
    checkpoint_manager: "CheckpointManager" = None,
    task_optimizer: "TaskOptimizer" = None,
) -> List[bool]:
    """Execute tasks using DAG-based scheduling.

    Tasks are executed as soon as their predecessors complete, maximizing
    parallelism within the max_workers constraint.

    When task_optimizer is provided, ready tasks are sorted using the
    configured scheduling algorithm to improve parallel efficiency.

    :param ctx: The current execution context
    :param dag: DAG containing tasks and their dependencies
    :param max_workers: Maximum number of concurrent workers
    :param retries: Number of retries for failed tasks
    :param tm1_services: Dictionary of TM1Service instances
    :param checkpoint_manager: Optional CheckpointManager for resume support
    :param task_optimizer: Optional TaskOptimizer for runtime-based scheduling
    :return: List of execution outcomes (True/False for each task)
    """
    outcomes = []
    loop = asyncio.get_event_loop()
    task_start_times: Dict[str, datetime] = {}

    with ThreadPoolExecutor(int(max_workers)) as executor:
        # Map futures to tasks
        pending_futures: Dict[asyncio.Future, Task] = {}

        def submit_ready_tasks():
            """Submit ready tasks up to max_workers limit.

            If task_optimizer is provided, sorts ready tasks using the
            configured scheduling algorithm before submission.
            """
            ready_tasks = dag.get_ready_tasks()

            # Apply optimization if available (sort using configured algorithm)
            if task_optimizer and ready_tasks:
                ready_tasks = task_optimizer.sort_tasks(ready_tasks)

            for task in ready_tasks:
                if len(pending_futures) >= max_workers:
                    break
                # Mark as running before submitting
                dag.mark_running(task)
                task_start_times[str(task.id)] = datetime.now()

                # Update checkpoint
                if checkpoint_manager:
                    checkpoint_manager.mark_running(str(task.id))

                future = loop.run_in_executor(
                    executor, execute_task, ctx, task, retries, tm1_services
                )
                pending_futures[future] = task

        # Initial submission of ready tasks
        submit_ready_tasks()

        # Process until DAG is complete
        while pending_futures:
            # Wait for at least one task to complete
            done, _ = await asyncio.wait(
                pending_futures.keys(), return_when=asyncio.FIRST_COMPLETED
            )

            for future in done:
                task = pending_futures.pop(future)
                task_id = str(task.id)
                try:
                    success = future.result()
                except Exception as e:
                    logger.error(f"Unexpected error executing task {task_id}: {e}")
                    success = False

                outcomes.append(success)
                dag.mark_complete(task, success)

                # Calculate duration and update checkpoint
                start_time = task_start_times.pop(task_id, datetime.now())
                duration = (datetime.now() - start_time).total_seconds()

                if checkpoint_manager:
                    # Note: Retry count is tracked in stats database via _collect_task_stats.
                    # Checkpoint only needs success/failure for resume; retry count is metadata.
                    checkpoint_manager.mark_completed(
                        task_id=task_id,
                        success=success,
                        duration_seconds=duration,
                        retry_count=0,
                        error_message=None if success else "Task failed",
                    )

            # Submit newly ready tasks
            submit_ready_tasks()

        # Handle any remaining tasks that couldn't be executed (shouldn't happen with valid DAG)
        if not dag.is_complete():
            logger.warning("DAG execution incomplete - some tasks may have unmet dependencies")

    return outcomes


def logout(
    tm1_services: Dict,
    tm1_preserve_connections: Dict,
    force: bool = False,
):
    """Logout from all TM1 instances.

    :param tm1_services: Dictionary of TM1Service instances
    :param tm1_preserve_connections: Dictionary indicating which connections to preserve
    :param force: If True, logout from ALL connections including preserved ones.
                  Use this for exclusive mode to ensure sessions are properly closed.
    :return: None
    """
    for connection in tm1_services:
        # Skip preserved connections unless force is True
        if not force and tm1_preserve_connections.get(connection, False) is True:
            logger.debug(f"Preserving connection to {connection}")
            continue

        try:
            tm1_services[connection].logout()
            logger.debug(f"Logged out from {connection}")
        except Exception as e:
            logger.warning(f"Failed to logout from {connection}: {e}")
