"""Task file module for RushTI.

This module provides:
- JSON task file parsing and validation
- TXT to JSON conversion utilities
- File type detection
"""

import json
import logging
import shlex
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from rushti.messages import TRUE_VALUES

logger = logging.getLogger(__name__)

# JSON Schema version
SCHEMA_VERSION = "2.0"

# Required task properties
REQUIRED_TASK_PROPERTIES = {"id", "instance", "process"}

# Default values for optional task properties
TASK_DEFAULTS = {
    "parameters": {},
    "predecessors": [],
    "stage": None,
    "safe_retry": False,
    "timeout": None,
    "cancel_at_timeout": False,
    "require_predecessor_success": False,
    "succeed_on_minor_errors": False,
}


@dataclass
class TaskfileMetadata:
    """Metadata section of a JSON task file."""

    workflow: str = ""
    name: str = ""
    description: str = ""
    author: str = ""
    expanded_from: Optional[str] = None
    expanded_at: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "workflow": self.workflow,
            "name": self.name,
            "description": self.description,
            "author": self.author,
        }
        if self.expanded_from is not None:
            result["expanded_from"] = self.expanded_from
        if self.expanded_at is not None:
            result["expanded_at"] = self.expanded_at
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskfileMetadata":
        return cls(
            workflow=data.get("workflow", ""),
            name=data.get("name", ""),
            description=data.get("description", ""),
            author=data.get("author", ""),
            expanded_from=data.get("expanded_from"),
            expanded_at=data.get("expanded_at"),
        )


@dataclass
class TaskfileSettings:
    """Settings section of a JSON task file."""

    max_workers: Optional[int] = None
    retries: Optional[int] = None
    result_file: Optional[str] = None
    exclusive: Optional[bool] = None
    optimization_algorithm: Optional[str] = None
    push_results: Optional[bool] = None
    auto_load_results: Optional[bool] = None
    stage_order: Optional[List[str]] = None
    stage_workers: Optional[Dict[str, int]] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {}
        for key, value in asdict(self).items():
            if value is not None:
                result[key] = value
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskfileSettings":
        return cls(
            max_workers=data.get("max_workers"),
            retries=data.get("retries"),
            result_file=data.get("result_file"),
            exclusive=data.get("exclusive"),
            optimization_algorithm=data.get("optimization_algorithm"),
            push_results=data.get("push_results"),
            auto_load_results=data.get("auto_load_results"),
            stage_order=data.get("stage_order"),
            stage_workers=data.get("stage_workers"),
        )


@dataclass
class TaskDefinition:
    """Unified task definition from JSON."""

    id: str
    instance: str
    process: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    predecessors: List[str] = field(default_factory=list)
    stage: Optional[str] = None
    safe_retry: bool = False
    timeout: Optional[int] = None
    cancel_at_timeout: bool = False
    require_predecessor_success: bool = False
    succeed_on_minor_errors: bool = False

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "id": self.id,
            "instance": self.instance,
            "process": self.process,
        }
        if self.parameters:
            result["parameters"] = self.parameters
        if self.predecessors:
            result["predecessors"] = self.predecessors
        if self.stage:
            result["stage"] = self.stage
        if self.safe_retry:
            result["safe_retry"] = self.safe_retry
        if self.timeout is not None:
            result["timeout"] = self.timeout
        if self.cancel_at_timeout:
            result["cancel_at_timeout"] = self.cancel_at_timeout
        if self.require_predecessor_success:
            result["require_predecessor_success"] = True
        if self.succeed_on_minor_errors:
            result["succeed_on_minor_errors"] = self.succeed_on_minor_errors
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskDefinition":
        return cls(
            id=str(data["id"]),
            instance=data["instance"],
            process=data["process"],
            parameters=data.get("parameters", {}),
            predecessors=[str(p) for p in data.get("predecessors", [])],
            stage=data.get("stage"),
            safe_retry=data.get("safe_retry", False),
            timeout=data.get("timeout"),
            cancel_at_timeout=data.get("cancel_at_timeout", False),
            require_predecessor_success=data.get("require_predecessor_success", False),
            succeed_on_minor_errors=data.get("succeed_on_minor_errors", False),
        )


@dataclass
class Taskfile:
    """Complete JSON task file structure."""

    version: str = SCHEMA_VERSION
    metadata: TaskfileMetadata = field(default_factory=TaskfileMetadata)
    settings: TaskfileSettings = field(default_factory=TaskfileSettings)
    tasks: List[TaskDefinition] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "metadata": self.metadata.to_dict(),
            "settings": self.settings.to_dict(),
            "tasks": [t.to_dict() for t in self.tasks],
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    def save(self, file_path: Union[str, Path]) -> None:
        """Save task file to disk."""
        with open(file_path, "w") as f:
            f.write(self.to_json())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Taskfile":
        return cls(
            version=data.get("version", SCHEMA_VERSION),
            metadata=TaskfileMetadata.from_dict(data.get("metadata", {})),
            settings=TaskfileSettings.from_dict(data.get("settings", {})),
            tasks=[TaskDefinition.from_dict(t) for t in data.get("tasks", [])],
        )


class TaskfileValidationError(Exception):
    """Raised when task file validation fails."""

    pass


@dataclass
class TaskfileSource:
    """Specification for loading a taskfile from file or TM1.

    Either file_path OR (tm1_instance + workflow) must be provided.

    Examples:
        # From local file
        source = TaskfileSource(file_path="tasks.json")

        # From TM1
        source = TaskfileSource(tm1_instance="tm1srv01", workflow="daily_etl")
    """

    file_path: Optional[str] = None
    tm1_instance: Optional[str] = None
    workflow: Optional[str] = None

    def is_tm1_source(self) -> bool:
        """Check if this source specifies a TM1 cube source."""
        return self.tm1_instance is not None

    def is_file_source(self) -> bool:
        """Check if this source specifies a local file source."""
        return self.file_path is not None

    def validate(self) -> None:
        """Validate the source specification.

        :raises ValueError: If the source specification is invalid
        """
        has_file = self.file_path is not None
        has_tm1 = self.tm1_instance is not None
        has_workflow = self.workflow is not None

        if not has_file and not has_tm1:
            raise ValueError("Either --tasks (file path) or --tm1-instance must be provided")

        if has_file and has_tm1:
            raise ValueError(
                "Cannot specify both --tasks and --tm1-instance. " "Use one or the other."
            )

        if has_tm1 and not has_workflow:
            raise ValueError("--workflow is required when using --tm1-instance")

        if has_workflow and not has_tm1:
            raise ValueError("--tm1-instance is required when using --workflow")

    def __str__(self) -> str:
        if self.is_file_source():
            return f"file:{self.file_path}"
        elif self.is_tm1_source():
            return f"tm1:{self.tm1_instance}/{self.workflow}"
        else:
            return "unspecified"

    @classmethod
    def from_args(cls, args) -> "TaskfileSource":
        """Create a TaskfileSource from parsed argparse arguments.

        Expects args to have taskfile, tm1_instance, and workflow attributes.

        :param args: Parsed argparse namespace
        :return: TaskfileSource instance
        """
        return cls(
            file_path=getattr(args, "taskfile", None),
            tm1_instance=getattr(args, "tm1_instance", None),
            workflow=getattr(args, "workflow", None),
        )


def load_taskfile_from_source(
    source: TaskfileSource,
    config_path: str,
    mode: str = "opt",
    **tm1_names,
) -> Taskfile:
    """Load a taskfile from the specified source.

    :param source: TaskfileSource specifying file path or TM1 location
    :param config_path: Path to config.ini (required for TM1 connections)
    :param mode: Execution mode for TM1 loading - "norm" or "opt" (default: "opt")
    :param tm1_names: Optional keyword arguments to override default TM1 object names
        (cube_name, dim_workflow, dim_task, dim_run, dim_measure)
    :return: Loaded Taskfile object
    :raises ValueError: If source is invalid
    :raises FileNotFoundError: If file not found
    :raises TaskfileValidationError: If taskfile validation fails
    """
    source.validate()

    if source.is_file_source():
        # Load from local file
        file_type = detect_file_type(source.file_path)
        if file_type == "json":
            return parse_json_taskfile(source.file_path)
        else:
            return convert_txt_to_json(source.file_path)

    elif source.is_tm1_source():
        # Load from TM1 cube
        # Import here to avoid circular imports
        from rushti.tm1_integration import connect_to_tm1_instance, read_taskfile_from_tm1

        tm1 = connect_to_tm1_instance(source.tm1_instance, config_path)
        try:
            return read_taskfile_from_tm1(tm1, source.workflow, mode=mode, **tm1_names)
        finally:
            try:
                tm1.logout()
            except Exception:
                pass  # Ignore logout errors; session may already be expired

    else:
        raise ValueError("Invalid TaskfileSource: no source specified")


def validate_task(task_data: Dict[str, Any], index: int) -> List[str]:
    """Validate a single task definition.

    :param task_data: Task dictionary
    :param index: Task index for error messages
    :return: List of validation error messages
    """
    errors = []

    # Check required properties
    for prop in REQUIRED_TASK_PROPERTIES:
        if prop not in task_data or not task_data[prop]:
            errors.append(f"Task {index}: Missing required property '{prop}'")

    # Validate types
    if "id" in task_data and not isinstance(task_data["id"], (str, int)):
        errors.append(f"Task {index}: 'id' must be a string or integer")

    if "predecessors" in task_data:
        if not isinstance(task_data["predecessors"], list):
            errors.append(f"Task {index}: 'predecessors' must be an array")

    if "timeout" in task_data and task_data["timeout"] is not None:
        if not isinstance(task_data["timeout"], int) or task_data["timeout"] < 0:
            errors.append(f"Task {index}: 'timeout' must be a non-negative integer")

    if "parameters" in task_data and not isinstance(task_data["parameters"], dict):
        errors.append(f"Task {index}: 'parameters' must be an object")

    return errors


def validate_taskfile(data: Dict[str, Any]) -> List[str]:
    """Validate a complete task file.

    :param data: Task file dictionary
    :return: List of validation error messages
    """
    errors = []

    # Validate version
    if "version" not in data:
        errors.append("Missing 'version' field")

    # Validate tasks array
    if "tasks" not in data:
        errors.append("Missing 'tasks' array")
    elif not isinstance(data["tasks"], list):
        errors.append("'tasks' must be an array")
    elif len(data["tasks"]) == 0:
        errors.append("'tasks' array cannot be empty")
    else:
        # Validate each task
        task_ids = set()
        for i, task in enumerate(data["tasks"]):
            task_errors = validate_task(task, i)
            errors.extend(task_errors)

            # Check for duplicate IDs
            if "id" in task:
                task_id = str(task["id"])
                if task_id in task_ids:
                    errors.append(f"Task {i}: Duplicate task ID '{task_id}'")
                task_ids.add(task_id)

    # Validate settings
    if "settings" in data and not isinstance(data["settings"], dict):
        errors.append("'settings' must be an object")

    if "settings" in data:
        settings = data["settings"]
        if "max_workers" in settings:
            if not isinstance(settings["max_workers"], int) or settings["max_workers"] < 1:
                errors.append("settings.max_workers must be a positive integer")
        if "retries" in settings:
            if not isinstance(settings["retries"], int) or settings["retries"] < 0:
                errors.append("settings.retries must be a non-negative integer")

    # Validate metadata
    if "metadata" in data and not isinstance(data["metadata"], dict):
        errors.append("'metadata' must be an object")

    return errors


def parse_json_taskfile(file_path: Union[str, Path]) -> Taskfile:
    """Parse a JSON task file.

    :param file_path: Path to the JSON task file
    :return: Taskfile object
    :raises TaskfileValidationError: If validation fails
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"Task file not found: {file_path}")

    logger.info(f"Parsing JSON task file: {file_path}")

    try:
        with open(file_path, "r") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise TaskfileValidationError(f"Invalid JSON syntax: {e}")

    # Validate
    errors = validate_taskfile(data)
    if errors:
        error_msg = "Task file validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
        raise TaskfileValidationError(error_msg)

    return Taskfile.from_dict(data)


def detect_file_type(file_path: Union[str, Path]) -> str:
    """Detect the task file type based on extension and content.

    :param file_path: Path to the task file
    :return: "json" or "txt"
    """
    file_path = Path(file_path)
    ext = file_path.suffix.lower()

    if ext == ".json":
        return "json"
    elif ext == ".txt":
        return "txt"
    else:
        # Try to detect from content
        try:
            with open(file_path, "r") as f:
                first_char = f.read(1).strip()
                if first_char == "{":
                    return "json"
        except Exception:
            pass  # JSON detection failed; will try other formats
        return "txt"


def detect_execution_mode(file_path: Union[str, Path]) -> str:
    """Detect the execution mode for a TXT task file by examining its content.

    - Lines starting with 'id=' indicate OPT mode (optimized/DAG-based)
    - Lines starting with 'instance=' indicate NORM mode (sequential with waits)

    :param file_path: Path to the TXT task file
    :return: "opt" or "norm"
    :raises ValueError: If the file format cannot be determined
    """
    file_path = Path(file_path)

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            # Skip empty lines and comments
            if not line or line.startswith("#"):
                continue

            # Check the first meaningful line
            if line.lower() == "wait":
                # 'wait' keyword only exists in NORM mode
                return "norm"
            elif line.startswith("id="):
                return "opt"
            elif line.startswith("instance="):
                return "norm"
            else:
                # Unknown format - raise error
                raise ValueError(
                    f"Cannot determine execution mode from file content. "
                    f"Expected line starting with 'id=' (opt) or 'instance=' (norm), "
                    f"got: {line[:50]}..."
                )

    # Empty file or only comments
    raise ValueError("Cannot determine execution mode: file contains no task definitions")


def parse_line_arguments(line: str) -> Dict[str, Any]:
    """Parse a task definition line into a dictionary of arguments.

    Handles key=value pairs with proper escaping via shlex. Boolean-like
    values for known keys (e.g. ``safe_retry``, ``succeed_on_minor_errors``)
    are automatically converted.

    :param line: Single line from a TXT task file
    :return: Dictionary of parsed arguments
    """
    line_arguments = {}

    # Use shlex to split the line with posix=True for proper escaping
    parts = shlex.split(line, posix=True)

    for part in parts:
        if "=" not in part:
            continue

        # Split on the first '=' to get argument and value
        argument, value = part.split("=", 1)

        # Handle specific keys with logic
        key_lower = argument.lower()
        if key_lower in ["process", "instance", "id"]:
            line_arguments[key_lower] = value
        elif key_lower == "require_predecessor_success":
            line_arguments[key_lower] = value.lower() in TRUE_VALUES
        elif key_lower == "predecessors":
            predecessors = value.split(",")
            line_arguments[key_lower] = [] if predecessors[0] in ["", "0", 0] else predecessors
        elif key_lower == "succeed_on_minor_errors":
            line_arguments[key_lower] = value.lower() in TRUE_VALUES
        elif key_lower == "safe_retry":
            line_arguments[key_lower] = value.lower() in TRUE_VALUES
        elif key_lower == "stage":
            line_arguments[key_lower] = value if value else None
        elif key_lower == "timeout":
            line_arguments[key_lower] = int(value) if value else None
        elif key_lower == "cancel_at_timeout":
            line_arguments[key_lower] = value.lower() in TRUE_VALUES
        else:
            # Directly assign the value without stripping quotes
            line_arguments[argument] = value

    return line_arguments


def convert_txt_to_json(
    txt_path: Union[str, Path],
    output_path: Optional[Union[str, Path]] = None,
    metadata: Optional[Dict[str, str]] = None,
) -> Taskfile:
    """Convert a TXT task file to JSON format.

    :param txt_path: Path to the TXT task file
    :param output_path: Optional path to save the JSON file
    :param metadata: Optional metadata to include
    :return: Taskfile object
    """

    txt_path = Path(txt_path)

    if not txt_path.exists():
        raise FileNotFoundError(f"TXT file not found: {txt_path}")

    logger.info(f"Converting TXT to JSON: {txt_path}")

    # Read and parse TXT file
    with open(txt_path, "r") as f:
        lines = f.readlines()

    tasks = []
    task_id = 1
    current_sequence = 0
    sequence_tasks: Dict[int, List[str]] = {0: []}

    for line in lines:
        line = line.strip()

        # Skip empty lines and comments
        if not line or line.startswith("#"):
            continue

        # Handle 'wait' keyword
        if line.lower() == "wait":
            current_sequence += 1
            sequence_tasks[current_sequence] = []
            continue

        # Parse the task line
        try:
            parsed = parse_line_arguments(line)
        except Exception as e:
            logger.warning(f"Failed to parse line: {line} - {e}")
            continue

        # Get or generate task ID
        if "id" in parsed:
            tid = str(parsed["id"])
        else:
            tid = str(task_id)
            task_id += 1

        # Build task definition
        task_def = TaskDefinition(
            id=tid,
            instance=parsed.get("instance", ""),
            process=parsed.get("process", ""),
            parameters={},
            predecessors=parsed.get("predecessors", []),
            stage=parsed.get("stage"),
            safe_retry=parsed.get("safe_retry", False),
            timeout=parsed.get("timeout"),
            cancel_at_timeout=parsed.get("cancel_at_timeout", False),
            require_predecessor_success=parsed.get("require_predecessor_success", False),
            succeed_on_minor_errors=parsed.get("succeed_on_minor_errors", False),
        )

        # Extract parameters (all other keys)
        reserved_keys = {
            "id",
            "instance",
            "process",
            "predecessors",
            "stage",
            "safe_retry",
            "timeout",
            "cancel_at_timeout",
            "require_predecessor_success",
            "succeed_on_minor_errors",
        }
        for key, value in parsed.items():
            if key not in reserved_keys:
                task_def.parameters[key] = value

        tasks.append(task_def)
        sequence_tasks[current_sequence].append(tid)

    # If we had 'wait' keywords (norm mode), add predecessor relationships
    if current_sequence > 0:
        # For each task in sequence N, predecessors are all tasks in sequence N-1
        task_by_id = {t.id: t for t in tasks}
        for seq in range(1, current_sequence + 1):
            prev_seq_tasks = sequence_tasks.get(seq - 1, [])
            for tid in sequence_tasks.get(seq, []):
                if tid in task_by_id:
                    task_by_id[tid].predecessors = prev_seq_tasks.copy()

    # Create taskfile
    taskfile = Taskfile(
        version=SCHEMA_VERSION,
        metadata=TaskfileMetadata.from_dict(metadata or {}),
        settings=TaskfileSettings(),
        tasks=tasks,
    )

    # Set default metadata if not provided
    if not taskfile.metadata.workflow:
        taskfile.metadata.workflow = txt_path.stem

    # Save if output path provided
    if output_path:
        taskfile.save(output_path)
        logger.info(f"Saved JSON task file: {output_path}")

    return taskfile


def get_expandable_parameters(task: TaskDefinition) -> Dict[str, str]:
    """Get parameters that need MDX expansion (keys ending with *)

    :param task: TaskDefinition object
    :return: Dictionary of expandable parameters
    """
    expandable = {}
    for key, value in task.parameters.items():
        if key.endswith("*"):
            expandable[key] = value
    return expandable


def archive_taskfile(taskfile: Taskfile, workflow: str, run_id: str) -> str:
    """Archive a taskfile as JSON for historical DAG reconstruction.

    Saves a JSON snapshot of the taskfile under the archive directory,
    organized by workflow name and run ID. This ensures the DAG can
    always be rebuilt regardless of the original source format.

    :param taskfile: Taskfile object to archive
    :param workflow: Workflow name (used as subdirectory)
    :param run_id: Run identifier (used as filename)
    :return: Absolute path to the archived JSON file
    """
    from rushti.utils import ensure_shared_file, makedirs_shared, resolve_app_path

    archive_dir = Path(resolve_app_path("archive")) / workflow
    makedirs_shared(str(archive_dir))

    archive_path = archive_dir / f"{run_id}.json"
    taskfile.save(archive_path)
    ensure_shared_file(str(archive_path))

    logger.info(f"Archived taskfile to {archive_path}")
    return str(archive_path)
