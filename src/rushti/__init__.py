"""
RushTI - Parallel TM1 TurboIntegrator Process Execution.

This package provides tools for executing TM1 TI processes in parallel
with dependency management, checkpoint/resume support, and execution logging.
"""

__version__ = "2.0.0"
__app_name__ = "RushTI"

# Core exports for programmatic use
from rushti.task import Task, OptimizedTask, ExecutionMode
from rushti.dag import DAG
from rushti.checkpoint import Checkpoint, CheckpointManager
from rushti.settings import Settings, load_settings
from rushti.taskfile import Taskfile, TaskDefinition, parse_json_taskfile

__all__ = [
    "__version__",
    "__app_name__",
    # Core classes
    "Task",
    "OptimizedTask",
    "ExecutionMode",
    "DAG",
    "Checkpoint",
    "CheckpointManager",
    "Settings",
    "load_settings",
    "Taskfile",
    "TaskDefinition",
    "parse_json_taskfile",
]
