"""Task file parsing and DAG construction for RushTI.

This module handles:
- Parsing TXT task file lines into Task / OptimizedTask objects
- Reading and expanding task files (both ``norm`` and ``opt`` formats)
- Building a DAG from any supported task file format (JSON or TXT)
- File pre-processing (encoding normalization)
"""

import importlib.util
import logging
from itertools import product
from typing import Dict, List, Tuple, Type, Union

from TM1py import TM1Service

from rushti.taskfile import (
    detect_file_type,
    detect_execution_mode,
    parse_json_taskfile,
    parse_line_arguments,
    Taskfile,
    TaskfileValidationError,
)
from rushti.task import Task, OptimizedTask, Wait
from rushti.dag import DAG, convert_norm_to_dag, convert_opt_to_dag
from rushti.utils import flatten_to_list

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Line-level parsers
# ---------------------------------------------------------------------------


def extract_task_or_wait_from_line(line: str) -> Union[Task, Wait]:
    if line.strip().lower() == "wait":
        return Wait()

    return extract_task_from_line(line, task_class=Task)


def extract_task_from_line_type_opt(line: str) -> OptimizedTask:
    return extract_task_from_line(line, task_class=OptimizedTask)


def extract_task_from_line(
    line: str, task_class: Union[Type[Task], Type[OptimizedTask]]
) -> Union[Task, OptimizedTask]:
    line_arguments = parse_line_arguments(line)

    # Extract common new parameters
    safe_retry = line_arguments.pop("safe_retry", False)
    stage = line_arguments.pop("stage", None)
    timeout = line_arguments.pop("timeout", None)
    cancel_at_timeout = line_arguments.pop("cancel_at_timeout", False)

    if task_class == OptimizedTask:
        task_id = line_arguments.pop("id")
        predecessors = line_arguments.pop("predecessors", [])
        require_predecessor_success = line_arguments.pop("require_predecessor_success", False)
        succeed_on_minor_errors = line_arguments.pop("succeed_on_minor_errors", False)

        return OptimizedTask(
            task_id=task_id,
            instance_name=line_arguments.pop("instance"),
            process_name=line_arguments.pop("process"),
            predecessors=predecessors,
            require_predecessor_success=require_predecessor_success,
            succeed_on_minor_errors=succeed_on_minor_errors,
            safe_retry=safe_retry,
            stage=stage,
            timeout=timeout,
            cancel_at_timeout=cancel_at_timeout,
            parameters=line_arguments,
        )
    else:
        return Task(
            instance_name=line_arguments.pop("instance"),
            succeed_on_minor_errors=line_arguments.pop("succeed_on_minor_errors", False),
            process_name=line_arguments.pop("process"),
            safe_retry=safe_retry,
            stage=stage,
            timeout=timeout,
            cancel_at_timeout=cancel_at_timeout,
            parameters=line_arguments,
        )


# ---------------------------------------------------------------------------
# Task expansion
# ---------------------------------------------------------------------------


def expand_task(
    tm1_services: Dict[str, TM1Service], task: Union[Task, OptimizedTask]
) -> List[Union[Task, OptimizedTask]]:
    """Expand task parameters with wildcard MDX expressions.

    If no wildcard parameters (ending with *) exist, returns the original task
    without creating new Task objects.

    :param tm1_services: Dictionary of TM1Service instances
    :param task: Task to expand
    :return: List of expanded tasks (or single-element list with original task)
    """
    # Handle None or empty parameters - no expansion needed
    if not task.parameters:
        return [task]

    # Check if any expansion is needed (params ending with *)
    has_wildcard = any(param.endswith("*") for param in task.parameters.keys())
    if not has_wildcard:
        # No expansion needed - return original task to preserve ID
        return [task]

    tm1 = tm1_services[task.instance_name]
    list_params = []
    result = []
    for param, value in task.parameters.items():
        if param.endswith("*"):
            mdx = value[1:]
            try:
                elements = tm1.dimensions.hierarchies.elements.execute_set_mdx(
                    mdx,
                    member_properties=["Name"],
                    parent_properties=None,
                    element_properties=None,
                )
            except Exception as e:
                raise RuntimeError(f"Failed to execute MDX '{mdx}': {str(e)}")
            list_params.append([(param[:-1], element[0]["Name"]) for element in elements])
        else:
            list_params.append([(param, value)])
    for expanded_params in [dict(combo) for combo in product(*list_params)]:
        if isinstance(task, OptimizedTask):
            result.append(
                OptimizedTask(
                    task.id,
                    task.instance_name,
                    task.process_name,
                    parameters=expanded_params,
                    predecessors=task.predecessors,
                    require_predecessor_success=task.require_predecessor_success,
                    succeed_on_minor_errors=task.succeed_on_minor_errors,
                    safe_retry=task.safe_retry,
                    stage=task.stage,
                    timeout=task.timeout,
                    cancel_at_timeout=task.cancel_at_timeout,
                )
            )
        elif isinstance(task, Task):
            result.append(
                Task(
                    task.instance_name,
                    task.process_name,
                    parameters=expanded_params,
                    succeed_on_minor_errors=task.succeed_on_minor_errors,
                    safe_retry=task.safe_retry,
                    stage=task.stage,
                    timeout=task.timeout,
                    cancel_at_timeout=task.cancel_at_timeout,
                )
            )
    return result


# ---------------------------------------------------------------------------
# File-level readers
# ---------------------------------------------------------------------------


def get_instances_from_tasks_file(tasks_file_path: str) -> set:
    """Extract TM1 instance names from a tasks file.

    File format and execution mode are auto-detected from content.

    :param tasks_file_path: Path to the tasks file
    :return: Set of TM1 instance names
    """
    tm1_instances_in_tasks = set()

    # Check if this is a JSON file first
    file_type = detect_file_type(tasks_file_path)
    if file_type == "json":
        # Parse JSON taskfile and extract instances
        taskfile = parse_json_taskfile(tasks_file_path)
        for task in taskfile.tasks:
            tm1_instances_in_tasks.add(task.instance)
        return tm1_instances_in_tasks

    # TXT file - auto-detect execution mode from content
    detected_mode = detect_execution_mode(tasks_file_path)

    if detected_mode == "norm":
        tasks_and_waits = extract_ordered_tasks_and_waits_from_file_type_norm(
            tasks_file_path, expand=False
        )
        for item in tasks_and_waits:
            if not isinstance(item, Wait):
                tm1_instances_in_tasks.add(item.instance_name)
    else:
        tasks = extract_tasks_from_file_type_opt(tasks_file_path, expand=False)
        for task_list in tasks.values():
            for task in task_list:
                tm1_instances_in_tasks.add(task.instance_name)

    return tm1_instances_in_tasks


def extract_ordered_tasks_and_waits_from_file_type_norm(
    file_path: str, expand: bool = False, tm1_services: Dict[str, TM1Service] = None
):
    with open(file_path, encoding="utf-8") as file:
        original_tasks = [
            extract_task_or_wait_from_line(line)
            for line in file.readlines()
            if not line.startswith("#")
        ]
        if not expand:
            return original_tasks

        return flatten_to_list(
            [
                expand_task(tm1_services, task) if isinstance(task, Task) else Wait()
                for task in original_tasks
            ]
        )


def extract_tasks_from_file_type_opt(
    file_path: str, expand: bool = False, tm1_services: Dict[str, TM1Service] = None
) -> Dict:
    """
    :param file_path:
    :param expand:
    :param tm1_services:
    :return: tasks
    """
    # Mapping of id against task
    tasks = dict()
    with open(file_path, encoding="utf-8") as input_file:
        lines = input_file.readlines()
        # Build tasks dictionary
        for line in lines:
            # exclude comments
            if not line.startswith("#"):
                # skip empty lines
                if not line.strip():
                    continue
                task = extract_task_from_line_type_opt(line)
                if task.id not in tasks:
                    tasks[task.id] = [task]
                else:
                    tasks[task.id].append(task)

    # expand tasks
    if expand:
        for task_id in list(tasks.keys()):  # Iterate over copy of keys
            original_tasks = tasks[task_id][:]  # Copy the list to avoid modifying during iteration
            tasks[task_id] = []  # Clear the list
            for task in original_tasks:
                expanded = expand_task(tm1_services, task)
                tasks[task_id].extend(expanded)

    # Populate the successors attribute
    for task_id in tasks:
        for task in tasks[task_id]:
            predecessors = task.predecessors
            for predecessor_id in predecessors:
                for pre_task in tasks[predecessor_id]:
                    pre_task.successors.append(task.id)
    return tasks


def pre_process_file(file_path: str):
    """Preprocess file for Python to change encoding from 'utf-8-sig' to 'utf-8'

    Background: Under certain circumstances TM1 / Turbo Integrator generates files with utf-8-sig

    :param file_path:
    :return:
    """
    import chardet

    with open(file_path, "rb") as file:
        raw = file.read(32)  # at most 32 bytes are returned
        encoding = chardet.detect(raw)["encoding"]

    if encoding and encoding.upper() == "UTF-8-SIG":
        with open(file_path, mode="r", encoding="utf-8-sig") as file:
            text = file.read()
        with open(file_path, mode="w", encoding="utf-8") as file:
            file.write(text)


# ---------------------------------------------------------------------------
# DAG construction
# ---------------------------------------------------------------------------


def convert_json_to_dag(
    taskfile: Taskfile,
    expand: bool = False,
    tm1_services: Dict[str, TM1Service] = None,
) -> DAG:
    """Convert a JSON taskfile to a DAG.

    :param taskfile: Parsed Taskfile object
    :param expand: Whether to expand wildcard parameters
    :param tm1_services: Dictionary of TM1Service instances (needed for expansion)
    :return: DAG containing all tasks with their dependencies
    """
    dag = DAG()

    for task_def in taskfile.tasks:
        # Create OptimizedTask from TaskDefinition
        task = OptimizedTask(
            task_id=task_def.id,
            instance_name=task_def.instance,
            process_name=task_def.process,
            parameters=task_def.parameters.copy(),
            predecessors=task_def.predecessors.copy(),
            require_predecessor_success=task_def.require_predecessor_success,
            succeed_on_minor_errors=task_def.succeed_on_minor_errors,
            safe_retry=task_def.safe_retry,
            stage=task_def.stage,
            timeout=task_def.timeout,
            cancel_at_timeout=task_def.cancel_at_timeout,
        )

        # Handle expandable parameters if expand is True
        if expand and tm1_services:
            expanded_tasks = expand_task(tm1_services, task)
            for exp_task in expanded_tasks:
                dag.add_task(exp_task)
        else:
            dag.add_task(task)

    # Add dependencies
    for task_def in taskfile.tasks:
        for predecessor_id in task_def.predecessors:
            dag.add_dependency(task_def.id, predecessor_id)

    # Apply stage ordering if defined in settings
    if taskfile.settings.stage_order:
        dag.apply_stage_ordering(taskfile.settings.stage_order)

    return dag


def build_dag(
    file_path: str,
    expand: bool = False,
    tm1_services: Dict[str, TM1Service] = None,
) -> Union[DAG, Tuple[DAG, Taskfile]]:
    """Build a DAG from a tasks file.

    File format and execution mode are auto-detected from content.

    :param file_path: Path to the tasks file (.json or .txt)
    :param expand: Whether to expand wildcard parameters
    :param tm1_services: Dictionary of TM1Service instances (needed for expansion)
    :return: DAG containing all tasks, or tuple of (DAG, Taskfile) for JSON files
    """
    # Detect file type
    file_type = detect_file_type(file_path)

    if file_type == "json":
        # Parse JSON task file
        try:
            taskfile = parse_json_taskfile(file_path)
            logger.info(
                f"Loaded JSON task file: {taskfile.metadata.name or taskfile.metadata.workflow or file_path}"
            )
            dag = convert_json_to_dag(taskfile, expand, tm1_services)
            dag.validate()
            return dag, taskfile
        except TaskfileValidationError as e:
            logger.error(str(e))
            raise
    else:
        # Legacy TXT file processing
        if importlib.util.find_spec("chardet") is not None:
            pre_process_file(file_path)
        else:
            logging.info(
                f"Function '{pre_process_file.__name__}' skipped. Optional dependency 'chardet' not installed"
            )

        # Reset task ID counter so executed tasks start from 1
        Task.reset_id_counter()

        # Auto-detect execution mode from file content
        detected_mode = detect_execution_mode(file_path)

        if detected_mode == "norm":
            # Read tasks and waits from norm file
            tasks_and_waits = extract_ordered_tasks_and_waits_from_file_type_norm(
                file_path, expand, tm1_services
            )
            # Convert to DAG
            dag = convert_norm_to_dag(tasks_and_waits)
        else:
            # Read tasks from opt file
            tasks = extract_tasks_from_file_type_opt(file_path, expand, tm1_services)
            # Convert to DAG
            dag = convert_opt_to_dag(tasks)

        # Validate the DAG has no circular dependencies
        dag.validate()

        return dag
