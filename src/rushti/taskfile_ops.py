"""
RushTI Run Modes

This module implements the different run modes for RushTI:
- run: Execute task file (default)
- expand: Expand MDX expressions and output new task file
- visualize: Generate DAG visualization (HTML/SVG)
- validate: Validate task file structure and optionally TM1 connectivity
- analyze: Analyze historical runs and generate optimized task file
- build: Create TM1 dimensions/cubes for logging (implemented in tm1_build.py)
"""

import configparser
import json
import logging
import statistics
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from enum import Enum

from TM1py import TM1Service

from rushti.taskfile import (
    convert_txt_to_json,
    detect_file_type,
    parse_json_taskfile,
    Taskfile,
    TaskDefinition,
    TaskfileSource,
    TaskfileValidationError,
    validate_taskfile,
    get_expandable_parameters,
    load_taskfile_from_source,
)
from rushti.stats import StatsDatabase

logger = logging.getLogger(__name__)


class RunMode(Enum):
    """Available run modes for RushTI."""

    RUN = "run"
    EXPAND = "expand"
    VISUALIZE = "visualize"
    VALIDATE = "validate"
    ANALYZE = "analyze"
    BUILD = "build"


@dataclass
class ValidationResult:
    """Result of task file validation."""

    valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    info: List[str] = field(default_factory=list)
    tm1_checks: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "valid": self.valid,
            "errors": self.errors,
            "warnings": self.warnings,
            "info": self.info,
            "tm1_checks": self.tm1_checks,
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2)

    def print_human_readable(self) -> None:
        """Print results in human-readable format."""
        status = "VALID" if self.valid else "INVALID"
        print(f"\nValidation Result: {status}")
        print("=" * 40)

        if self.errors:
            print(f"\nErrors ({len(self.errors)}):")
            for error in self.errors:
                print(f"  - {error}")

        if self.warnings:
            print(f"\nWarnings ({len(self.warnings)}):")
            for warning in self.warnings:
                print(f"  - {warning}")

        if self.info:
            print(f"\nInfo ({len(self.info)}):")
            for info_msg in self.info:
                print(f"  - {info_msg}")

        if self.tm1_checks:
            print("\nTM1 Connectivity Checks:")
            for instance, result in self.tm1_checks.items():
                status = "OK" if result.get("connected") else "FAILED"
                print(f"  {instance}: {status}")
                if result.get("processes"):
                    valid = result["processes"].get("valid", 0)
                    invalid = result["processes"].get("invalid", 0)
                    print(f"    Processes: {valid} valid, {invalid} not found")


# =============================================================================
# EXPAND MODE
# =============================================================================


def expand_taskfile(
    source: Union[str, TaskfileSource, Taskfile],
    output_path: str,
    config_path: str,
    output_format: Optional[str] = None,
    mode: str = "opt",
) -> Taskfile:
    """Expand MDX expressions in a task file and write to output.

    :param source: Taskfile source - can be:
        - str: Path to input task file (TXT or JSON)
        - TaskfileSource: Source specification (file or TM1)
        - Taskfile: Already loaded Taskfile object
    :param output_path: Path to output expanded task file
    :param config_path: Path to config.ini for TM1 connections
    :param output_format: Output format ('json' or 'txt'), inferred from extension if None
    :param mode: Execution mode for TM1 loading - "norm" or "opt" (default: "opt")
    :return: Expanded Taskfile object
    """
    output_path = Path(output_path)

    # Load taskfile from source and track input path for metadata
    input_path_str: str = "unknown"
    if isinstance(source, Taskfile):
        taskfile = source
        input_path_str = taskfile.metadata.source_file or "in-memory"
    elif isinstance(source, TaskfileSource):
        taskfile = load_taskfile_from_source(source, config_path, mode=mode)
        if source.is_file_source():
            input_path_str = str(source.file_path)
        else:
            input_path_str = f"tm1://{source.tm1_instance}/{source.workflow}"
    else:
        # String path - legacy support
        input_path = Path(source)
        input_path_str = str(input_path)
        file_type = detect_file_type(str(input_path))
        if file_type == "json":
            taskfile = parse_json_taskfile(input_path)
        else:
            taskfile = convert_txt_to_json(input_path)

    # Determine output format
    if output_format is None:
        output_format = output_path.suffix.lower().lstrip(".")
        if output_format not in ("json", "txt"):
            output_format = "json"

    # Get TM1 connections for MDX expansion
    config = configparser.ConfigParser()
    config.read(config_path, encoding="utf-8")

    tm1_services = {}
    instances_needed = set()

    # Find which instances we need for expansion
    for task in taskfile.tasks:
        expandable = get_expandable_parameters(task)
        if expandable:
            instances_needed.add(task.instance)

    if not instances_needed:
        logger.info("No MDX expressions to expand")
        # Write output as-is
        _write_taskfile(taskfile, output_path, output_format)
        return taskfile

    # Connect to needed TM1 instances
    try:
        for instance in instances_needed:
            if instance not in config.sections():
                raise ValueError(f"Instance '{instance}' not found in config.ini")
            logger.info(f"Connecting to TM1 instance: {instance}")
            params = dict(config[instance])
            params.pop("session_context", None)
            tm1_services[instance] = TM1Service(**params)

        # Expand tasks and track ID mappings
        expanded_tasks = []
        expansion_map = {}  # Maps original task ID to list of expanded task IDs

        for task in taskfile.tasks:
            expandable = get_expandable_parameters(task)
            if not expandable:
                expanded_tasks.append(task)
                expansion_map[task.id] = [task.id]  # No expansion, maps to itself
                continue

            # Get TM1 connection for this task's instance
            tm1 = tm1_services.get(task.instance)
            if not tm1:
                raise ValueError(f"No TM1 connection for instance '{task.instance}'")

            # Expand each expandable parameter
            expansion_results = {}
            for param_name, mdx_expr in expandable.items():
                try:
                    # Strip leading '*' from the value (TXT format: param*=*"MDX")
                    if mdx_expr.startswith("*"):
                        mdx_expr = mdx_expr[1:]
                    # Execute MDX set expression to get elements
                    elements = tm1.dimensions.hierarchies.elements.execute_set_mdx(
                        mdx_expr,
                        member_properties=["Name"],
                        parent_properties=None,
                        element_properties=None,
                    )
                    # Extract element names from the result
                    expansion_results[param_name] = [elem[0]["Name"] for elem in elements]
                    logger.info(
                        f"Expanded {param_name}: {len(expansion_results[param_name])} elements"
                    )
                except Exception as e:
                    raise RuntimeError(f"Failed to execute MDX for {param_name}: {e}")

            # Generate expanded tasks (cartesian product if multiple params)
            expanded = _expand_task_parameters(task, expansion_results)
            expanded_tasks.extend(expanded)

            # Track the mapping from original ID to expanded IDs
            expansion_map[task.id] = [t.id for t in expanded]

        # Update predecessors in all tasks to reference expanded task IDs
        # When a task depends on an expanded task, it should depend on ALL expanded versions
        for task in expanded_tasks:
            if task.predecessors:
                updated_predecessors = []
                for pred_id in task.predecessors:
                    if pred_id in expansion_map:
                        # Replace with all expanded task IDs
                        updated_predecessors.extend(expansion_map[pred_id])
                    else:
                        # Keep the original (might be external reference)
                        updated_predecessors.append(pred_id)
                task.predecessors = updated_predecessors

        # Create new taskfile with expanded tasks
        expanded_taskfile = Taskfile(
            version=taskfile.version,
            tasks=expanded_tasks,
            settings=taskfile.settings,
            metadata=taskfile.metadata,
        )

        # Update metadata to indicate expansion
        expanded_taskfile.metadata.expanded_from = input_path_str
        expanded_taskfile.metadata.expanded_at = datetime.now().isoformat()

        # Write output
        _write_taskfile(expanded_taskfile, output_path, output_format)
        logger.info(f"Wrote expanded task file to {output_path} ({len(expanded_tasks)} tasks)")

        return expanded_taskfile

    finally:
        # Logout from TM1 instances
        for tm1 in tm1_services.values():
            try:
                tm1.logout()
            except Exception:
                pass  # Ignore logout errors; session may already be expired


def _expand_task_parameters(
    task: TaskDefinition,
    expansion_results: Dict[str, List[str]],
) -> List[TaskDefinition]:
    """Expand a task with multiple parameter values.

    :param task: Original task definition
    :param expansion_results: Dict mapping param names to list of values
    :return: List of expanded task definitions
    """
    from itertools import product

    # Get the base parameters (without expandable markers)
    base_params = {k.rstrip("*"): v for k, v in task.parameters.items() if not k.endswith("*")}

    # Get expansion keys and values
    expand_keys = list(expansion_results.keys())
    expand_values = [expansion_results[k] for k in expand_keys]

    if not expand_keys:
        return [task]

    # Generate all combinations
    expanded_tasks = []
    for i, combo in enumerate(product(*expand_values)):
        # Create new parameters dict
        new_params = base_params.copy()
        for j, key in enumerate(expand_keys):
            # Remove the * suffix from parameter name
            clean_key = key.rstrip("*")
            new_params[clean_key] = combo[j]

        # Create new task with unique ID
        new_task = TaskDefinition(
            id=f"{task.id}_{i+1}" if len(list(product(*expand_values))) > 1 else task.id,
            instance=task.instance,
            process=task.process,
            parameters=new_params,
            predecessors=task.predecessors,
            stage=task.stage,
            safe_retry=task.safe_retry,
            timeout=task.timeout,
            cancel_at_timeout=task.cancel_at_timeout,
            require_predecessor_success=task.require_predecessor_success,
            succeed_on_minor_errors=task.succeed_on_minor_errors,
        )
        expanded_tasks.append(new_task)

    return expanded_tasks


def _write_taskfile(taskfile: Taskfile, output_path: Path, format: str) -> None:
    """Write taskfile to file.

    :param taskfile: Taskfile object to write
    :param output_path: Output file path
    :param format: Output format ('json' or 'txt')
    """
    if format == "json":
        data = {
            "version": taskfile.version,
            "tasks": [t.to_dict() for t in taskfile.tasks],
        }
        if taskfile.settings:
            if hasattr(taskfile.settings, "to_dict"):
                data["settings"] = taskfile.settings.to_dict()
            else:
                data["settings"] = taskfile.settings
        if taskfile.metadata:
            if hasattr(taskfile.metadata, "to_dict"):
                data["metadata"] = taskfile.metadata.to_dict()
            else:
                data["metadata"] = taskfile.metadata

        with open(output_path, "w") as f:
            json.dump(data, f, indent=2)
    else:
        raise NotImplementedError("TXT output format not yet implemented")


# =============================================================================
# VISUALIZE MODE
# =============================================================================


def visualize_dag(
    source: Union[str, TaskfileSource, Taskfile],
    output_path: str,
    config_path: Optional[str] = None,
    show_parameters: bool = False,
    mode: str = "opt",
    dashboard_url: Optional[str] = None,
) -> str:
    """Generate interactive HTML DAG visualization from a task file.

    Uses vis.js for interactive visualization with no external dependencies.
    Features: zoom, pan, search, filtering, multiple view modes, and task details panel.

    :param source: Taskfile source - can be:
        - str: Path to input task file
        - TaskfileSource: Source specification (file or TM1)
        - Taskfile: Already loaded Taskfile object
    :param output_path: Path to output HTML file
    :param config_path: Path to config.ini (required for TM1 source)
    :param show_parameters: Whether to include task parameters in node labels
    :param mode: Execution mode for TM1 loading - "norm" or "opt" (default: "opt")
                 Use "norm" for taskfiles with wait-based sequencing
    :return: Path to generated HTML visualization file
    """
    output_path = Path(output_path)

    # Load taskfile from source
    if isinstance(source, Taskfile):
        taskfile = source
        tasks = taskfile.tasks
    elif isinstance(source, TaskfileSource):
        if not config_path:
            raise ValueError("config_path is required when using TaskfileSource")
        taskfile = load_taskfile_from_source(source, config_path, mode=mode)
        tasks = taskfile.tasks
    else:
        # String path - legacy support
        input_path = Path(source)
        file_type = detect_file_type(str(input_path))
        if file_type == "json":
            taskfile = parse_json_taskfile(input_path)
            tasks = taskfile.tasks
        else:
            taskfile = convert_txt_to_json(input_path)
            tasks = taskfile.tasks

    # Build tasks_by_id dict and adjacency list
    tasks_by_id = {task.id: task for task in tasks}
    adjacency = {}  # parent -> children (reverse of predecessors)

    for task in tasks:
        for pred in task.predecessors:
            if pred not in adjacency:
                adjacency[pred] = []
            adjacency[pred].append(task.id)

    # Get output base path (without extension)
    output_base = str(output_path)
    if output_path.suffix:
        output_base = str(output_path.with_suffix(""))

    # Generate HTML visualization
    result_path = _visualize_dag_html(
        adjacency=adjacency,
        tasks_by_id=tasks_by_id,
        filename=output_base,
        show_parameters=show_parameters,
        dashboard_url=dashboard_url,
    )

    logger.info(f"Generated DAG visualization: {result_path}")
    return result_path


def _visualize_dag_html(
    adjacency: Dict[str, List[str]],
    tasks_by_id: Dict[str, TaskDefinition],
    filename: str,
    show_parameters: bool = False,
    title: str = "RushTI DAG Visualization",
    dashboard_url: Optional[str] = None,
) -> str:
    """Generate an interactive HTML visualization using vis.js (no external dependencies).

    Features:
    - Three view modes: Compact DAG, Detailed DAG, Table View
    - Search/Filter by task ID, process, instance, or stage
    - Task Details Panel on click
    - Zoom, pan, and physics controls

    :param adjacency: Dictionary mapping parent task IDs to lists of child task IDs
    :param tasks_by_id: Dictionary mapping task IDs to TaskDefinition objects
    :param filename: Output filename (should end with .html)
    :param show_parameters: Whether to include task parameters in node labels
    :param title: Title for the HTML page
    :return: Path to the generated HTML file
    """
    # Stage colors for visual distinction (dark theme optimized)
    stage_colors = {
        "extract": "#3b82f6",  # blue
        "load": "#10b981",  # green
        "input": "#f59e0b",  # amber
        "analysis": "#8b5cf6",  # purple
        "export": "#ec4899",  # pink
        "import": "#06b6d4",  # cyan
        "reporting": "#6366f1",  # indigo
        "NoStage": "#6b7280",  # gray
        "default": "#6b7280",  # gray
    }

    # Build nodes list for vis.js with all data needed for all views
    nodes = []
    for task_id, task in tasks_by_id.items():
        stage_name = task.stage if task.stage else "NoStage"
        color = stage_colors.get(stage_name, stage_colors["default"])

        # Compact label (just ID)
        compact_label = f"{task_id}"

        # Detailed label (ID, process, instance, optionally parameters)
        detailed_parts = [f"ID: {task_id}", f"Process: {task.process}"]
        if task.instance:
            detailed_parts.append(f"Instance: {task.instance}")
        if show_parameters and task.parameters:
            params_str = ", ".join(f"{k}={v}" for k, v in task.parameters.items())
            if len(params_str) > 40:
                params_str = params_str[:37] + "..."
            detailed_parts.append(f"Params: {params_str}")
        detailed_label = "\n".join(detailed_parts)

        # Build tooltip with full details (plain text for proper rendering)
        tooltip_parts = [f"ID: {task_id}", f"Process: {task.process}"]
        if task.instance:
            tooltip_parts.append(f"Instance: {task.instance}")
        if task.stage:
            tooltip_parts.append(f"Stage: {task.stage}")
        if task.predecessors:
            tooltip_parts.append(f"Predecessors: {', '.join(task.predecessors)}")
        if task.parameters:
            params_str = ", ".join(f"{k}={v}" for k, v in task.parameters.items())
            tooltip_parts.append(f"Parameters: {params_str}")
        tooltip = "\n".join(tooltip_parts)

        # Parameters as object for table/details view
        params_obj = task.parameters if task.parameters else {}

        nodes.append(
            {
                "id": task_id,
                "label": compact_label,
                "compactLabel": compact_label,
                "detailedLabel": detailed_label,
                "title": tooltip,
                "color": {
                    "background": color,
                    "border": color,
                    "highlight": {"background": "#00AEEF", "border": "#0097D4"},
                    "hover": {"background": color, "border": "#64748B"},
                },
                "stage": stage_name,
                "process": task.process,
                "instance": task.instance or "",
                "predecessors": task.predecessors if task.predecessors else [],
                "parameters": params_obj,
                # Execution options
                "timeout": task.timeout,
                "cancel_at_timeout": task.cancel_at_timeout,
                "safe_retry": task.safe_retry,
                "require_predecessor_success": task.require_predecessor_success,
                "succeed_on_minor_errors": task.succeed_on_minor_errors,
            }
        )

    # Build edges list for vis.js
    edges = []
    for parent, children in adjacency.items():
        for child in children:
            edges.append({"from": parent, "to": child})

    # Gather stages for legend
    stages_used = sorted(
        set(task.stage if task.stage else "NoStage" for task in tasks_by_id.values())
    )

    # Generate HTML with embedded vis.js
    import json
    from string import Template

    from rushti.dashboard import _LOGO_SVG
    from rushti.visualization_template import VISUALIZATION_TEMPLATE

    nodes_json = json.dumps(nodes)
    edges_json = json.dumps(edges)
    stage_colors_json = json.dumps(stage_colors)

    # Build legend HTML
    legend_items = []
    for stage in stages_used:
        color = stage_colors.get(stage, stage_colors["default"])
        legend_items.append(
            f'<span class="legend-item" data-stage="{stage}">'
            f'<span class="legend-color" style="background-color:{color};"></span>'
            f"{stage}</span>"
        )
    legend_html = "".join(legend_items)

    # Build conditional dashboard link HTML
    dashboard_link_html = ""
    if dashboard_url:
        dashboard_link_html = (
            f'<a href="{dashboard_url}" style="display:inline-flex;'
            f"align-items:center;gap:6px;padding:8px 16px;"
            f"background:#00AEEF;color:white;border-radius:8px;"
            f"font-size:0.85rem;font-weight:500;text-decoration:none;"
            f'transition:all 0.3s ease;" '
            f"onmouseover=\"this.style.boxShadow='0 4px 12px rgba(0,174,239,0.3)'\" "
            f"onmouseout=\"this.style.boxShadow='none'\">"
            f"&#8592; Performance Dashboard</a>"
        )

    # Substitute variables into embedded template (no external file dependency)
    html_content = Template(VISUALIZATION_TEMPLATE).safe_substitute(
        title=title,
        dashboard_link_html=dashboard_link_html,
        legend_html=legend_html,
        nodes_json=nodes_json,
        edges_json=edges_json,
        stage_colors_json=stage_colors_json,
        logo_svg=_LOGO_SVG,
    )

    # Ensure filename ends with .html
    if not filename.endswith(".html"):
        filename = filename + ".html"

    output_path = Path(filename)
    output_path.write_text(html_content, encoding="utf-8")

    return str(output_path)


# =============================================================================
# VALIDATE MODE
# =============================================================================


def validate_taskfile_full(
    source: Union[str, TaskfileSource, Taskfile],
    config_path: str,
    check_tm1: bool = True,
    output_json: bool = False,
    mode: str = "opt",
) -> ValidationResult:
    """Perform full validation of a task file.

    :param source: Taskfile source - can be:
        - str: Path to task file
        - TaskfileSource: Source specification (file or TM1)
        - Taskfile: Already loaded Taskfile object
    :param config_path: Path to config.ini
    :param check_tm1: Whether to check TM1 connectivity and process existence
    :param output_json: Whether to output JSON format
    :param mode: Execution mode for TM1 loading - "norm" or "opt" (default: "opt")
    :return: ValidationResult object
    """
    result = ValidationResult(valid=True)
    taskfile = None
    tasks = []
    file_type = "json"  # Default for TM1/Taskfile sources

    # Handle different source types
    if isinstance(source, Taskfile):
        # Already loaded Taskfile
        taskfile = source
        tasks = taskfile.tasks
        result.info.append("Source: Taskfile object")

    elif isinstance(source, TaskfileSource):
        # Load from TaskfileSource
        try:
            if source.is_tm1_source():
                result.info.append(f"Source: TM1 ({source.tm1_instance}/{source.workflow})")
            else:
                result.info.append(f"Source: File ({source.file_path})")

            taskfile = load_taskfile_from_source(source, config_path, mode=mode)
            tasks = taskfile.tasks
        except Exception as e:
            result.valid = False
            result.errors.append(f"Failed to load taskfile: {e}")
            return result

    else:
        # String path - original behavior
        input_path = Path(source)

        # Check file exists
        if not input_path.exists():
            result.valid = False
            result.errors.append(f"File not found: {input_path}")
            return result

        # Detect and parse file
        file_type = detect_file_type(str(input_path))
        result.info.append(f"File type: {file_type}")

        try:
            if file_type == "json":
                with open(input_path, "r") as f:
                    data = json.load(f)

                # Use existing validation
                errors = validate_taskfile(data)
                if errors:
                    result.valid = False
                    result.errors.extend(errors)

                # Parse for further validation
                taskfile = parse_json_taskfile(input_path)
                tasks = taskfile.tasks

            else:
                # Basic TXT validation
                with open(input_path, "r") as f:
                    lines = f.readlines()

                tasks = []
                for i, line in enumerate(lines, 1):
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if line.lower() == "wait":
                        continue

                    # Check basic format
                    if "instance=" not in line.lower() or "process=" not in line.lower():
                        result.warnings.append(f"Line {i}: Missing instance or process")

                    # Create minimal task for validation
                    task = TaskDefinition(
                        id=str(i),
                        instance="",
                        process="",
                        parameters={},
                    )
                    tasks.append(task)

                result.info.append(f"Parsed {len(tasks)} tasks from TXT file")

        except json.JSONDecodeError as e:
            result.valid = False
            result.errors.append(f"Invalid JSON: {e}")
            return result
        except TaskfileValidationError as e:
            result.valid = False
            result.errors.append(str(e))
            return result
        except Exception as e:
            result.valid = False
            result.errors.append(f"Parse error: {e}")
            return result

    # Validate DAG (check for cycles) - always run for JSON/Taskfile/TM1 sources
    if tasks:
        cycle_errors = _check_dag_cycles(tasks)
        if cycle_errors:
            result.valid = False
            result.errors.extend(cycle_errors)

    # TM1 connectivity checks (for JSON, Taskfile, and TM1 sources - not TXT files)
    if check_tm1 and taskfile is not None:
        result.info.append("Checking TM1 connectivity...")

        config = configparser.ConfigParser()
        if not Path(config_path).exists():
            result.warnings.append(f"Config file not found: {config_path}")
        else:
            config.read(config_path, encoding="utf-8")

            # Get unique instances
            instances = {t.instance for t in tasks}

            for instance in instances:
                if instance not in config.sections():
                    result.errors.append(f"Instance '{instance}' not found in config.ini")
                    result.valid = False
                    result.tm1_checks[instance] = {"connected": False, "error": "Not in config"}
                    continue

                try:
                    params = dict(config[instance])
                    params.pop("session_context", None)
                    tm1 = TM1Service(**params)
                    result.tm1_checks[instance] = {"connected": True}

                    # Check processes exist
                    processes_to_check = {t.process for t in tasks if t.instance == instance}
                    valid_processes = 0
                    invalid_processes = []

                    for process_name in processes_to_check:
                        try:
                            if tm1.processes.exists(process_name):
                                valid_processes += 1
                            else:
                                invalid_processes.append(process_name)
                        except Exception:
                            invalid_processes.append(process_name)

                    result.tm1_checks[instance]["processes"] = {
                        "valid": valid_processes,
                        "invalid": len(invalid_processes),
                        "not_found": invalid_processes,
                    }

                    if invalid_processes:
                        for proc in invalid_processes:
                            result.errors.append(
                                f"Process '{proc}' not found on instance '{instance}'"
                            )
                        result.valid = False

                    tm1.logout()

                except Exception as e:
                    result.tm1_checks[instance] = {"connected": False, "error": str(e)}
                    result.errors.append(f"Cannot connect to instance '{instance}': {e}")
                    result.valid = False

    result.info.append(f"Total tasks: {len(tasks)}")
    if result.valid:
        result.info.append("Validation passed")
    else:
        result.info.append(f"Validation failed with {len(result.errors)} error(s)")

    return result


def _check_dag_cycles(tasks: List[TaskDefinition]) -> List[str]:
    """Check for cycles in task dependencies.

    :param tasks: List of task definitions
    :return: List of error messages for any cycles found
    """
    errors = []

    # Build adjacency list
    task_ids = {t.id for t in tasks}
    graph = {t.id: t.predecessors for t in tasks}

    # Check for references to non-existent tasks
    for task in tasks:
        for pred in task.predecessors:
            if pred not in task_ids:
                errors.append(f"Task '{task.id}' references non-existent predecessor '{pred}'")

    # DFS-based cycle detection
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {t: WHITE for t in task_ids}

    def dfs(node, path):
        if node not in color:
            return []
        if color[node] == GRAY:
            # Found cycle
            cycle_start = path.index(node)
            cycle = path[cycle_start:] + [node]
            return [f"Circular dependency detected: {' -> '.join(cycle)}"]
        if color[node] == BLACK:
            return []

        color[node] = GRAY
        path.append(node)

        cycle_errors = []
        for pred in graph.get(node, []):
            cycle_errors.extend(dfs(pred, path.copy()))

        color[node] = BLACK
        return cycle_errors

    for task_id in task_ids:
        if color[task_id] == WHITE:
            cycle_errors = dfs(task_id, [])
            errors.extend(cycle_errors)

    return errors


# =============================================================================
# ANALYZE MODE
# =============================================================================


@dataclass
class TaskAnalysis:
    """Analysis results for a single task."""

    task_id: str
    avg_duration: float
    ewma_duration: float
    run_count: int
    success_rate: float
    confidence: float = 0.0
    std_dev: float = 0.0
    coefficient_of_variation: float = 0.0
    estimated: bool = False
    recommended_order: int = 0


@dataclass
class AnalysisReport:
    """Complete analysis report for a taskfile."""

    workflow: str
    analysis_date: str
    run_count: int
    tasks: List[TaskAnalysis]
    recommendations: List[str]
    optimized_order: List[str]
    ewma_alpha: float = 0.3
    lookback_runs: int = 10

    def to_dict(self) -> Dict[str, Any]:
        return {
            "workflow": self.workflow,
            "analysis_date": self.analysis_date,
            "run_count": self.run_count,
            "ewma_alpha": self.ewma_alpha,
            "lookback_runs": self.lookback_runs,
            "tasks": [
                {
                    "task_id": t.task_id,
                    "avg_duration": t.avg_duration,
                    "ewma_duration": t.ewma_duration,
                    "run_count": t.run_count,
                    "success_rate": t.success_rate,
                    "confidence": t.confidence,
                    "std_dev": t.std_dev,
                    "coefficient_of_variation": t.coefficient_of_variation,
                    "estimated": t.estimated,
                }
                for t in self.tasks
            ],
            "recommendations": self.recommendations,
            "optimized_order": self.optimized_order,
        }


def analyze_runs(
    workflow: str,
    stats_db: StatsDatabase,
    output_path: Optional[str] = None,
    run_count: int = 10,
    ewma_alpha: float = 0.3,
) -> AnalysisReport:
    """Analyze historical runs and generate optimization recommendations.

    Uses SQLite stats database to retrieve historical execution data and calculate
    EWMA runtime estimates with confidence scores. Handles tasks without history
    by using a default estimate (fastest 25% average).

    :param workflow: Workflow name to analyze
    :param stats_db: StatsDatabase instance with historical execution data
    :param output_path: Optional path to write analysis report
    :param run_count: Number of recent runs to analyze (lookback window)
    :param ewma_alpha: EWMA smoothing factor (0-1, higher = more weight on recent)
    :return: AnalysisReport with task analyses and recommendations
    :raises ValueError: If stats database is not enabled
    """
    logger.info(f"Analyzing runs for workflow: {workflow}")

    # Verify stats database is enabled
    if not stats_db or not stats_db.enabled:
        raise ValueError(
            "Stats database must be enabled for optimization analysis. "
            "Enable it in settings.ini under [stats] section."
        )

    # Get all unique task signatures for this workflow
    signatures = stats_db.get_workflow_signatures(workflow)

    if not signatures:
        logger.warning(f"No historical data found for workflow: {workflow}")
        return AnalysisReport(
            workflow=workflow,
            analysis_date=datetime.now().isoformat(),
            run_count=0,
            tasks=[],
            recommendations=[
                "No historical data available for analysis. Run tasks at least once to gather data."
            ],
            optimized_order=[],
            ewma_alpha=ewma_alpha,
            lookback_runs=run_count,
        )

    logger.info(f"Found {len(signatures)} unique task signatures")

    # Analyze each task with history
    task_analyses = []
    processed_signatures = set()  # Track which signatures have been analyzed
    for signature in signatures:
        history = stats_db.get_task_history(signature, limit=run_count)

        if not history:
            # Will handle tasks without history after calculating baseline
            continue

        # Mark this signature as processed
        processed_signatures.add(signature)

        # Extract durations and calculate statistics
        durations = [h["duration_seconds"] for h in history if h.get("duration_seconds")]

        if not durations:
            continue

        # Calculate basic statistics
        avg_duration = statistics.mean(durations)

        # Calculate standard deviation and coefficient of variation
        if len(durations) >= 2:
            std_dev = statistics.stdev(durations)
            cv = std_dev / avg_duration if avg_duration > 0 else 1.0
        else:
            std_dev = 0.0
            cv = 0.0

        # Calculate EWMA with outlier dampening
        ewma = durations[0]  # Start with most recent
        for d in durations[1:]:
            # Outlier detection: if duration > 3x current estimate
            if ewma > 0 and d > ewma * 3.0:
                # Cap at 2x current estimate to prevent spikes
                d_dampened = min(d, ewma * 2.0)
                logger.debug(
                    f"Outlier detected for {signature}: {d:.2f}s capped to {d_dampened:.2f}s "
                    f"(EWMA: {ewma:.2f}s)"
                )
            else:
                d_dampened = d

            # Update EWMA
            ewma = ewma_alpha * d_dampened + (1 - ewma_alpha) * ewma

        # Calculate confidence score
        # Formula: (run_count/10)*0.5 + (1-cv)*0.5
        quantity_factor = min(1.0, len(history) / 10) * 0.5
        consistency_factor = (1 - min(1.0, cv)) * 0.5
        confidence = quantity_factor + consistency_factor

        # Success rate (all history results are already filtered for success)
        success_rate = 1.0

        # Use first history record's task_id
        task_id = history[0].get("task_id", signature)

        task_analyses.append(
            TaskAnalysis(
                task_id=task_id,
                avg_duration=avg_duration,
                ewma_duration=ewma,
                run_count=len(history),
                success_rate=success_rate,
                confidence=confidence,
                std_dev=std_dev,
                coefficient_of_variation=cv,
                estimated=False,
            )
        )

    # Calculate default estimate for tasks without history (fastest 25% average)
    default_estimate = 10.0  # Fallback if no tasks have history
    if task_analyses:
        all_ewma = [t.ewma_duration for t in task_analyses if t.ewma_duration > 0]
        all_ewma.sort()
        fastest_25_percent = all_ewma[: max(1, len(all_ewma) // 4)]
        if fastest_25_percent:
            default_estimate = statistics.mean(fastest_25_percent)
            logger.info(f"Default estimate for tasks without history: {default_estimate:.2f}s")

    # Add tasks without history using default estimate
    tasks_without_history = []
    for signature in signatures:
        # Check if this signature was already processed (has history)
        if signature in processed_signatures:
            continue

        # This task has no history - use default estimate
        tasks_without_history.append(signature)
        task_analyses.append(
            TaskAnalysis(
                task_id=signature,  # For tasks without history, use signature as ID
                avg_duration=default_estimate,
                ewma_duration=default_estimate,
                run_count=0,
                success_rate=0.0,
                confidence=0.0,
                std_dev=0.0,
                coefficient_of_variation=0.0,
                estimated=True,
            )
        )

    # Generate recommendations
    recommendations = []

    # Warn about low-confidence tasks
    low_confidence = [t for t in task_analyses if not t.estimated and t.confidence < 0.5]
    if low_confidence:
        recommendations.append(
            f"⚠ Low confidence estimates for {len(low_confidence)} tasks (confidence < 0.5). "
            f"Gather more historical data for better optimization."
        )

    # Warn about estimated tasks
    if tasks_without_history:
        recommendations.append(
            f"ℹ {len(tasks_without_history)} tasks have no history and use default estimate "
            f"({default_estimate:.2f}s). Run these tasks to improve accuracy."
        )

    # Find slow tasks (>2x average)
    if task_analyses:
        avg_all = sum(t.ewma_duration for t in task_analyses) / len(task_analyses)
        slow_tasks = [t for t in task_analyses if t.ewma_duration > avg_all * 2]
        if slow_tasks:
            slow_task_ids = [
                t.task_id[:16] + "..." if len(t.task_id) > 16 else t.task_id for t in slow_tasks[:5]
            ]
            recommendations.append(
                f"🐌 {len(slow_tasks)} slow tasks (>2x average): {', '.join(slow_task_ids)}"
            )

    # Calculate optimized order (longer tasks first)
    optimized_order = [t.task_id for t in sorted(task_analyses, key=lambda x: -x.ewma_duration)]

    # Count actual runs (from tasks with history)
    actual_run_count = 0
    if task_analyses:
        # Get max run_count from tasks with history
        run_counts = [t.run_count for t in task_analyses if t.run_count > 0]
        if run_counts:
            actual_run_count = max(run_counts)

    report = AnalysisReport(
        workflow=workflow,
        analysis_date=datetime.now().isoformat(),
        run_count=actual_run_count,
        tasks=task_analyses,
        recommendations=recommendations,
        optimized_order=optimized_order,
        ewma_alpha=ewma_alpha,
        lookback_runs=run_count,
    )

    # Write report if output path specified
    if output_path:
        with open(output_path, "w") as f:
            json.dump(report.to_dict(), f, indent=2)
        logger.info(f"Wrote analysis report to {output_path}")

    return report


def write_optimized_taskfile(
    original_taskfile_path: str,
    optimized_order: List[str],
    output_path: str,
    report: AnalysisReport,
) -> None:
    """Generate optimized task file with reordered tasks.

    Loads the original task file, reorders tasks based on EWMA estimates,
    and writes an executable optimized task file with metadata.

    :param original_taskfile_path: Path to original task file
    :param optimized_order: List of task IDs in optimized order
    :param output_path: Path to write optimized task file
    :param report: AnalysisReport with optimization metadata
    """
    logger.info(f"Writing optimized task file to {output_path}")

    # Load original task file
    try:
        with open(original_taskfile_path, "r") as f:
            taskfile_data = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Original task file not found: {original_taskfile_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in task file: {e}")

    # Create task lookup by ID
    tasks = taskfile_data.get("tasks", [])
    task_lookup = {}

    for task in tasks:
        task_id = task.get("id")
        if task_id:
            task_lookup[task_id] = task

    # Reorder tasks based on optimization
    optimized_tasks = []
    missing_tasks = []
    added_task_ids = set()  # Track which tasks have been added to avoid duplicates

    for task_id in optimized_order:
        if task_id in task_lookup:
            if task_id not in added_task_ids:
                optimized_tasks.append(task_lookup[task_id])
                added_task_ids.add(task_id)
        else:
            # Task not found by ID - this can happen if the optimized_order
            # contains task IDs from historical runs that don't exist in current taskfile
            missing_tasks.append(task_id)

    # Warn about missing tasks
    if missing_tasks:
        logger.warning(f"{len(missing_tasks)} tasks from optimization not found in original file")

    # Add any tasks not in optimization (shouldn't happen, but be safe)
    for task in tasks:
        task_id = task.get("id")
        if task_id and task_id not in added_task_ids:
            logger.warning(f"Task {task_id} not in optimization, appending to end")
            optimized_tasks.append(task)
            added_task_ids.add(task_id)

    # Update task file with optimized order
    taskfile_data["tasks"] = optimized_tasks

    # Add optimization metadata
    if "metadata" not in taskfile_data:
        taskfile_data["metadata"] = {}

    taskfile_data["metadata"].update(
        {
            "optimized": True,
            "optimization_date": report.analysis_date,
            "algorithm": "EWMA",
            "algorithm_version": "2.0",
            "ewma_alpha": report.ewma_alpha,
            "lookback_runs": report.lookback_runs,
            "run_count": report.run_count,
            "original_taskfile": original_taskfile_path,
            "task_count": len(optimized_tasks),
        }
    )

    # Write optimized task file
    output_path_obj = Path(output_path)
    output_path_obj.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(taskfile_data, f, indent=2)

    logger.info(
        f"Optimized task file written: {len(optimized_tasks)} tasks, "
        f"algorithm=EWMA(alpha={report.ewma_alpha})"
    )
