"""Checkpoint and resume functionality for RushTI.

This module provides:
- Automatic checkpoint saving during task execution
- Checkpoint loading and validation for resume operations
- Integration with safe_retry for interrupted task handling

Checkpoint files are JSON documents that capture execution state:
- Completed tasks with their results
- In-progress tasks at the time of checkpoint
- Pending tasks yet to be executed
- Failed tasks

Usage:
    # During execution
    checkpoint = Checkpoint.create(taskfile_path, workflow)
    checkpoint.mark_completed("task-1", success=True, duration=10.5)
    save_checkpoint(checkpoint, checkpoint_path)

    # On resume
    checkpoint = load_checkpoint(checkpoint_path)
    checkpoint.validate_against_taskfile(taskfile_path)
"""

import json
import logging
import os
import tempfile
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set
import hashlib

logger = logging.getLogger(__name__)

# Bytes to read per iteration when hashing files
_FILE_HASH_CHUNK_SIZE = 8192


@dataclass
class TaskResult:
    """Result of a completed task execution."""

    task_id: str
    success: bool
    duration_seconds: float
    retry_count: int = 0
    error_message: Optional[str] = None
    completed_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "TaskResult":
        return cls(**data)


@dataclass
class Checkpoint:
    """Execution state checkpoint for resume capability.

    Attributes:
        taskfile_path: Path to the original task file
        workflow: Workflow name (from metadata or filename)
        taskfile_hash: Hash of taskfile content for validation
        run_started: ISO timestamp when execution started
        checkpoint_created: ISO timestamp when this checkpoint was created
        completed_tasks: Dict of task_id -> TaskResult for completed tasks
        in_progress_tasks: Set of task IDs that were running when checkpoint was saved
        pending_tasks: Set of task IDs not yet executed
        failed_tasks: Set of task IDs that failed
        skipped_tasks: Set of task IDs that were skipped (e.g., predecessor failed)
        total_tasks: Total number of tasks in the taskfile
        version: Checkpoint format version for compatibility
    """

    taskfile_path: str
    workflow: str
    taskfile_hash: str
    run_started: str
    checkpoint_created: str
    completed_tasks: Dict[str, TaskResult] = field(default_factory=dict)
    in_progress_tasks: Set[str] = field(default_factory=set)
    pending_tasks: Set[str] = field(default_factory=set)
    failed_tasks: Set[str] = field(default_factory=set)
    skipped_tasks: Set[str] = field(default_factory=set)
    total_tasks: int = 0
    version: str = "1.0"

    @classmethod
    def create(
        cls,
        taskfile_path: str,
        workflow: str,
        task_ids: List[str],
    ) -> "Checkpoint":
        """Create a new checkpoint at the start of execution.

        :param taskfile_path: Path to the task file
        :param workflow: Workflow name
        :param task_ids: List of all task IDs in the taskfile
        :return: New Checkpoint instance
        """
        taskfile_hash = _compute_file_hash(taskfile_path)
        now = datetime.now().isoformat()

        return cls(
            taskfile_path=str(Path(taskfile_path).absolute()),
            workflow=workflow,
            taskfile_hash=taskfile_hash,
            run_started=now,
            checkpoint_created=now,
            pending_tasks=set(task_ids),
            total_tasks=len(task_ids),
        )

    def mark_running(self, task_id: str) -> None:
        """Mark a task as currently running.

        :param task_id: Task ID that started running
        """
        self.pending_tasks.discard(task_id)
        self.in_progress_tasks.add(task_id)
        self.checkpoint_created = datetime.now().isoformat()

    def mark_completed(
        self,
        task_id: str,
        success: bool,
        duration_seconds: float,
        retry_count: int = 0,
        error_message: Optional[str] = None,
    ) -> None:
        """Mark a task as completed.

        :param task_id: Task ID that completed
        :param success: Whether the task succeeded
        :param duration_seconds: Execution duration
        :param retry_count: Number of retries performed
        :param error_message: Error message if failed
        """
        self.in_progress_tasks.discard(task_id)
        self.pending_tasks.discard(task_id)

        result = TaskResult(
            task_id=task_id,
            success=success,
            duration_seconds=duration_seconds,
            retry_count=retry_count,
            error_message=error_message,
        )
        self.completed_tasks[task_id] = result

        if not success:
            self.failed_tasks.add(task_id)

        self.checkpoint_created = datetime.now().isoformat()

    def mark_skipped(self, task_id: str, reason: str = "predecessor_failed") -> None:
        """Mark a task as skipped.

        :param task_id: Task ID that was skipped
        :param reason: Reason for skipping
        """
        self.pending_tasks.discard(task_id)
        self.in_progress_tasks.discard(task_id)
        self.skipped_tasks.add(task_id)

        # Record as completed with success=False
        result = TaskResult(
            task_id=task_id,
            success=False,
            duration_seconds=0.0,
            error_message=f"Skipped: {reason}",
        )
        self.completed_tasks[task_id] = result
        self.checkpoint_created = datetime.now().isoformat()

    def get_tasks_for_resume(
        self,
        task_safe_retry_map: Dict[str, bool],
    ) -> tuple:
        """Determine which tasks to execute on resume.

        :param task_safe_retry_map: Dict mapping task_id -> safe_retry flag
        :return: Tuple of (tasks_to_run, tasks_requiring_decision, error_message)
            - tasks_to_run: Set of task IDs to execute
            - tasks_requiring_decision: Set of in-progress non-safe-retry tasks
            - error_message: Error message if there are blocking issues
        """
        tasks_to_run = set(self.pending_tasks)
        tasks_requiring_decision = set()

        # Handle in-progress tasks based on safe_retry flag
        for task_id in self.in_progress_tasks:
            safe_retry = task_safe_retry_map.get(task_id, False)
            if safe_retry:
                # Safe to retry - add to tasks to run
                tasks_to_run.add(task_id)
                logger.info(f"Task '{task_id}' was in-progress with safe_retry=true, will retry")
            else:
                # Not safe to retry - requires user decision
                tasks_requiring_decision.add(task_id)
                logger.warning(
                    f"Task '{task_id}' was in-progress with safe_retry=false, "
                    f"requires --resume-from to specify handling"
                )

        error_message = None
        if tasks_requiring_decision:
            task_list = ", ".join(sorted(tasks_requiring_decision))
            error_message = (
                f"Cannot automatically resume: {len(tasks_requiring_decision)} task(s) were in-progress "
                f"with safe_retry=false: {task_list}. "
                f"Use --resume-from <task_id> to specify where to resume from."
            )

        return tasks_to_run, tasks_requiring_decision, error_message

    def get_resume_from_task(
        self,
        resume_from_task_id: str,
        all_task_ids: List[str],
    ) -> Set[str]:
        """Get tasks to run when resuming from a specific task.

        :param resume_from_task_id: Task ID to resume from
        :param all_task_ids: Ordered list of all task IDs
        :return: Set of task IDs to execute
        :raises ValueError: If resume_from_task_id is not found
        """
        if resume_from_task_id not in set(all_task_ids):
            raise ValueError(f"Task '{resume_from_task_id}' not found in taskfile")

        # Find the position of the resume-from task
        try:
            resume_index = all_task_ids.index(resume_from_task_id)
        except ValueError:
            raise ValueError(f"Task '{resume_from_task_id}' not found in taskfile")

        # Return all tasks from resume point onwards
        tasks_to_run = set(all_task_ids[resume_index:])

        # Also include any pending tasks that might have been skipped
        tasks_to_run.update(self.pending_tasks)

        return tasks_to_run

    @property
    def is_complete(self) -> bool:
        """Check if all tasks are complete (no pending or in-progress)."""
        return len(self.pending_tasks) == 0 and len(self.in_progress_tasks) == 0

    @property
    def success_count(self) -> int:
        """Count of successfully completed tasks."""
        return sum(1 for r in self.completed_tasks.values() if r.success)

    @property
    def failure_count(self) -> int:
        """Count of failed tasks."""
        return len(self.failed_tasks)

    @property
    def progress_percentage(self) -> float:
        """Percentage of tasks completed."""
        if self.total_tasks == 0:
            return 0.0
        completed = len(self.completed_tasks)
        return (completed / self.total_tasks) * 100

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "version": self.version,
            "taskfile_path": self.taskfile_path,
            "workflow": self.workflow,
            "taskfile_hash": self.taskfile_hash,
            "run_started": self.run_started,
            "checkpoint_created": self.checkpoint_created,
            "total_tasks": self.total_tasks,
            "completed_tasks": {k: v.to_dict() for k, v in self.completed_tasks.items()},
            "in_progress_tasks": list(self.in_progress_tasks),
            "pending_tasks": list(self.pending_tasks),
            "failed_tasks": list(self.failed_tasks),
            "skipped_tasks": list(self.skipped_tasks),
            "summary": {
                "completed": len(self.completed_tasks),
                "in_progress": len(self.in_progress_tasks),
                "pending": len(self.pending_tasks),
                "failed": len(self.failed_tasks),
                "skipped": len(self.skipped_tasks),
                "success_count": self.success_count,
                "progress_percentage": round(self.progress_percentage, 1),
            },
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Checkpoint":
        """Create Checkpoint from dictionary."""
        # Parse completed_tasks
        completed_tasks = {}
        for task_id, result_data in data.get("completed_tasks", {}).items():
            completed_tasks[task_id] = TaskResult.from_dict(result_data)

        return cls(
            taskfile_path=data["taskfile_path"],
            workflow=data["workflow"],
            taskfile_hash=data["taskfile_hash"],
            run_started=data["run_started"],
            checkpoint_created=data["checkpoint_created"],
            completed_tasks=completed_tasks,
            in_progress_tasks=set(data.get("in_progress_tasks", [])),
            pending_tasks=set(data.get("pending_tasks", [])),
            failed_tasks=set(data.get("failed_tasks", [])),
            skipped_tasks=set(data.get("skipped_tasks", [])),
            total_tasks=data.get("total_tasks", 0),
            version=data.get("version", "1.0"),
        )

    def validate_against_taskfile(
        self,
        taskfile_path: str,
        strict: bool = True,
    ) -> tuple:
        """Validate this checkpoint matches the given taskfile.

        :param taskfile_path: Path to the current taskfile
        :param strict: If True, require exact hash match; if False, just warn
        :return: Tuple of (is_valid, warnings)
        """
        warnings = []
        is_valid = True

        # Check file hash
        current_hash = _compute_file_hash(taskfile_path)
        if current_hash != self.taskfile_hash:
            msg = (
                f"Taskfile has been modified since checkpoint was created. "
                f"Original hash: {self.taskfile_hash[:8]}..., "
                f"Current hash: {current_hash[:8]}..."
            )
            if strict:
                is_valid = False
                warnings.append(f"ERROR: {msg}")
            else:
                warnings.append(f"WARNING: {msg}")

        # Check path matches (normalized)
        checkpoint_path = Path(self.taskfile_path).resolve()
        current_path = Path(taskfile_path).resolve()
        if checkpoint_path != current_path:
            warnings.append(
                f"WARNING: Checkpoint was created for '{checkpoint_path}', "
                f"but resuming with '{current_path}'"
            )

        return is_valid, warnings


def _compute_file_hash(file_path: str) -> str:
    """Compute SHA-256 hash of a file.

    :param file_path: Path to the file
    :return: Hex digest of the hash
    """
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(_FILE_HASH_CHUNK_SIZE), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def save_checkpoint(checkpoint: Checkpoint, file_path: str) -> None:
    """Save checkpoint to file atomically.

    Uses a write-to-temp-then-rename strategy to ensure the checkpoint
    file is never left in a partial/corrupt state.

    :param checkpoint: Checkpoint to save
    :param file_path: Target file path
    """
    from rushti.utils import ensure_shared_file, makedirs_shared

    file_path = Path(file_path)

    # Ensure directory exists (shared permissions for multi-user access)
    makedirs_shared(str(file_path.parent))

    # Write to temporary file first
    fd, temp_path = tempfile.mkstemp(
        suffix=".tmp",
        prefix="checkpoint_",
        dir=file_path.parent,
    )

    try:
        with os.fdopen(fd, "w") as f:
            json.dump(checkpoint.to_dict(), f, indent=2)

        # Atomic rename (on POSIX systems)
        # On Windows, need to remove target first if it exists
        if os.name == "nt" and file_path.exists():
            file_path.unlink()

        os.rename(temp_path, file_path)
        ensure_shared_file(str(file_path))
        logger.debug(f"Checkpoint saved to {file_path}")

    except Exception as e:
        # Clean up temp file on error
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        raise RuntimeError(f"Failed to save checkpoint: {e}") from e


def load_checkpoint(file_path: str) -> Checkpoint:
    """Load checkpoint from file.

    :param file_path: Path to checkpoint file
    :return: Loaded Checkpoint instance
    :raises FileNotFoundError: If checkpoint file doesn't exist
    :raises ValueError: If checkpoint file is invalid
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"Checkpoint file not found: {file_path}")

    try:
        with open(file_path, "r") as f:
            data = json.load(f)

        # Validate required fields
        required_fields = ["taskfile_path", "workflow", "run_started"]
        for field in required_fields:
            if field not in data:
                raise ValueError(f"Checkpoint missing required field: {field}")

        checkpoint = Checkpoint.from_dict(data)
        logger.info(
            f"Loaded checkpoint from {file_path}: "
            f"{checkpoint.success_count}/{checkpoint.total_tasks} completed, "
            f"{len(checkpoint.pending_tasks)} pending, "
            f"{len(checkpoint.in_progress_tasks)} in-progress"
        )

        return checkpoint

    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid checkpoint file format: {e}") from e


def delete_checkpoint(file_path: str) -> bool:
    """Delete a checkpoint file.

    :param file_path: Path to checkpoint file
    :return: True if deleted, False if didn't exist
    """
    file_path = Path(file_path)

    if file_path.exists():
        file_path.unlink()
        logger.info(f"Checkpoint deleted: {file_path}")
        return True

    return False


def get_checkpoint_path(
    checkpoint_dir: str,
    workflow: str,
) -> Path:
    """Get the standard checkpoint file path for a taskfile.

    :param checkpoint_dir: Base directory for checkpoints
    :param workflow: Workflow name
    :return: Path to the checkpoint file
    """
    # Sanitize workflow name for use in filename
    safe_id = "".join(c if c.isalnum() or c in "-_" else "_" for c in workflow)
    return Path(checkpoint_dir) / f"checkpoint_{safe_id}.json"


def find_checkpoint_for_taskfile(
    checkpoint_dir: str,
    taskfile_path: str,
) -> Optional[Path]:
    """Find an existing checkpoint for a taskfile.

    :param checkpoint_dir: Directory containing checkpoints
    :param taskfile_path: Path to the taskfile
    :return: Path to checkpoint file if found, None otherwise
    """
    checkpoint_dir = Path(checkpoint_dir)

    if not checkpoint_dir.exists():
        return None

    # Look for checkpoint files
    taskfile_path_resolved = Path(taskfile_path).resolve()

    for checkpoint_file in checkpoint_dir.glob("checkpoint_*.json"):
        try:
            checkpoint = load_checkpoint(checkpoint_file)
            if Path(checkpoint.taskfile_path).resolve() == taskfile_path_resolved:
                return checkpoint_file
        except (ValueError, FileNotFoundError):
            continue

    return None


class CheckpointManager:
    """Manages checkpoint creation, saving, and cleanup during execution.

    Usage:
        manager = CheckpointManager(
            checkpoint_dir="./checkpoints",
            taskfile_path="tasks.json",
            workflow="daily-etl",
            task_ids=["1", "2", "3"],
            checkpoint_interval=60,
        )

        # During execution
        manager.mark_running("1")
        manager.mark_completed("1", success=True, duration=10.5)

        # After successful completion
        manager.cleanup()
    """

    def __init__(
        self,
        checkpoint_dir: str,
        taskfile_path: str,
        workflow: str,
        task_ids: List[str],
        checkpoint_interval: int = 60,
        enabled: bool = True,
    ):
        """Initialize checkpoint manager.

        :param checkpoint_dir: Directory for checkpoint files
        :param taskfile_path: Path to the task file
        :param workflow: Workflow name
        :param task_ids: List of all task IDs
        :param checkpoint_interval: Seconds between automatic checkpoint saves
        :param enabled: Whether checkpointing is enabled
        """
        self.enabled = enabled
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_path = get_checkpoint_path(checkpoint_dir, workflow)
        self.checkpoint_interval = checkpoint_interval
        self._last_save_time = datetime.now()

        if enabled:
            self.checkpoint = Checkpoint.create(
                taskfile_path=taskfile_path,
                workflow=workflow,
                task_ids=task_ids,
            )
            # Create initial checkpoint
            self._save()
        else:
            self.checkpoint = None

    def mark_running(self, task_id: str) -> None:
        """Mark a task as running."""
        if not self.enabled or not self.checkpoint:
            return
        self.checkpoint.mark_running(task_id)
        self._maybe_save()

    def mark_completed(
        self,
        task_id: str,
        success: bool,
        duration_seconds: float,
        retry_count: int = 0,
        error_message: Optional[str] = None,
    ) -> None:
        """Mark a task as completed and save checkpoint."""
        if not self.enabled or not self.checkpoint:
            return

        self.checkpoint.mark_completed(
            task_id=task_id,
            success=success,
            duration_seconds=duration_seconds,
            retry_count=retry_count,
            error_message=error_message,
        )
        # Always save on task completion
        self._save()

    def mark_skipped(self, task_id: str, reason: str = "predecessor_failed") -> None:
        """Mark a task as skipped."""
        if not self.enabled or not self.checkpoint:
            return
        self.checkpoint.mark_skipped(task_id, reason)
        self._maybe_save()

    def _maybe_save(self) -> None:
        """Save checkpoint if interval has elapsed."""
        now = datetime.now()
        elapsed = (now - self._last_save_time).total_seconds()
        if elapsed >= self.checkpoint_interval:
            self._save()

    def _save(self) -> None:
        """Save the current checkpoint."""
        if not self.enabled or not self.checkpoint:
            return

        try:
            save_checkpoint(self.checkpoint, self.checkpoint_path)
            self._last_save_time = datetime.now()
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")

    def force_save(self) -> None:
        """Force immediate checkpoint save."""
        self._save()

    def cleanup(self, success: bool = True) -> None:
        """Clean up checkpoint after execution.

        :param success: If True, delete checkpoint; if False, retain for resume
        """
        if not self.enabled:
            return

        if success:
            delete_checkpoint(self.checkpoint_path)
        else:
            # Ensure final state is saved
            self._save()
            logger.info(f"Checkpoint retained for resume: {self.checkpoint_path}")
