"""Execution logging module for RushTI.

This module provides structured logging for task execution results
using Python's standard logging framework.
"""

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Default task elements (1-5000)
DEFAULT_TASK_ELEMENT_COUNT = 5000


@dataclass
class TaskExecutionLog:
    """Structured log entry for a single task execution.

    Contains all standardized fields for task execution logging as per
    the logging specification.
    """

    workflow: str
    task_id: str
    instance: str
    process: str
    parameters: str  # JSON string of parameters
    status: str  # "Success" or "Fail"
    start_time: datetime
    end_time: datetime
    duration_seconds: float
    retry_count: int = 0
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with serializable values.

        :return: Dictionary with all fields, dates as ISO strings
        """
        result = asdict(self)
        result["start_time"] = self.start_time.isoformat()
        result["end_time"] = self.end_time.isoformat()
        return result

    def to_json(self) -> str:
        """Convert to JSON string.

        :return: JSON representation of the log entry
        """
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_execution_result(
        cls,
        workflow: str,
        task_id: str,
        instance: str,
        process: str,
        parameters: Dict[str, Any],
        success: bool,
        start_time: datetime,
        end_time: datetime,
        retry_count: int = 0,
        error_message: Optional[str] = None,
    ) -> "TaskExecutionLog":
        """Create a log entry from execution result data.

        :param workflow: Workflow name
        :param task_id: Task identifier within the file
        :param instance: TM1 instance name
        :param process: TI process name
        :param parameters: Process parameters as dictionary
        :param success: Whether execution succeeded
        :param start_time: Execution start time
        :param end_time: Execution end time
        :param retry_count: Number of retries attempted
        :param error_message: Error message if failed
        :return: TaskExecutionLog instance
        """
        duration = (end_time - start_time).total_seconds()
        return cls(
            workflow=workflow,
            task_id=task_id,
            instance=instance,
            process=process,
            parameters=json.dumps(parameters) if parameters else "{}",
            status="Success" if success else "Fail",
            start_time=start_time,
            end_time=end_time,
            duration_seconds=round(duration, 3),
            retry_count=retry_count,
            error_message=error_message if not success else None,
        )


@dataclass
class ExecutionRun:
    """Container for a complete execution run with all task logs."""

    run_id: str  # Timestamp-based run identifier
    workflow: str
    start_time: datetime
    end_time: Optional[datetime] = None
    task_logs: List[TaskExecutionLog] = field(default_factory=list)

    def add_log(self, log: TaskExecutionLog) -> None:
        """Add a task execution log to this run.

        :param log: Task execution log entry
        """
        self.task_logs.append(log)

    def complete(self, end_time: Optional[datetime] = None) -> None:
        """Mark the run as complete.

        :param end_time: Run end time (defaults to now)
        """
        self.end_time = end_time or datetime.now()

    @property
    def success_count(self) -> int:
        """Count of successful task executions."""
        return sum(1 for log in self.task_logs if log.status == "Success")

    @property
    def failure_count(self) -> int:
        """Count of failed task executions."""
        return sum(1 for log in self.task_logs if log.status == "Fail")

    @property
    def total_duration_seconds(self) -> float:
        """Total duration of all task executions."""
        return sum(log.duration_seconds for log in self.task_logs)


class LogDestination(ABC):
    """Abstract base class for log destinations."""

    @abstractmethod
    def write_logs(self, run: ExecutionRun) -> bool:
        """Write execution logs to the destination.

        :param run: Execution run with task logs
        :return: True if successful, False otherwise
        """
        pass

    @abstractmethod
    def is_available(self) -> bool:
        """Check if the destination is available.

        :return: True if destination can accept logs
        """
        pass


class FileLogDestination(LogDestination):
    """Log destination that writes plain text logs to Python logging.

    This destination uses the existing logging_config.ini setup and
    writes human-readable plain text log entries for each task execution.
    Structured stats are stored separately in SQLite when enabled.
    """

    def __init__(self, logger_name: str = "rushti.execution"):
        """Initialize file log destination.

        :param logger_name: Logger name for execution logs
        """
        self.exec_logger = logging.getLogger(logger_name)

    def write_logs(self, run: ExecutionRun) -> bool:
        """Write execution logs to file via Python logging.

        Writes plain text summary for each task execution result.
        Structured stats should be written to SQLite separately.

        :param run: Execution run with task logs
        :return: True (file logging always succeeds or fails silently)
        """
        try:
            for log in run.task_logs:
                # Use DEBUG level for individual task logs since execution status
                # is already logged as tasks complete. Only failures at ERROR.
                log_level = logging.DEBUG if log.status == "Success" else logging.ERROR
                # Plain text format: concise, human-readable
                if log.status == "Success":
                    msg = (
                        f"Task completed: {log.instance}:{log.process} "
                        f"[{log.duration_seconds:.3f}s]"
                    )
                else:
                    error_info = f" - {log.error_message}" if log.error_message else ""
                    msg = (
                        f"Task failed: {log.instance}:{log.process} "
                        f"[{log.duration_seconds:.3f}s]{error_info}"
                    )
                self.exec_logger.log(log_level, msg)

            # Write run summary
            total_duration = run.total_duration_seconds
            self.exec_logger.info(
                f"Run {run.run_id} complete: "
                f"{run.success_count} succeeded, {run.failure_count} failed, "
                f"total duration {total_duration:.3f}s"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to write logs to file: {e}")
            return False

    def is_available(self) -> bool:
        """File logging is always available.

        :return: True
        """
        return True


class ExecutionLogger:
    """Main execution logger that routes to configured destinations.

    Manages the current execution run and writes logs to all
    configured destinations.
    """

    def __init__(
        self,
        workflow: str,
        destinations: Optional[List[LogDestination]] = None,
    ):
        """Initialize execution logger.

        :param workflow: Workflow name for this run
        :param destinations: List of log destinations (default: file only)
        """
        self.workflow = workflow
        self.destinations = destinations or [FileLogDestination()]

        # Create run with timestamp-based ID
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.current_run = ExecutionRun(
            run_id=run_id,
            workflow=workflow,
            start_time=datetime.now(),
        )

    def log_task_execution(
        self,
        task_id: str,
        instance: str,
        process: str,
        parameters: Dict[str, Any],
        success: bool,
        start_time: datetime,
        end_time: datetime,
        retry_count: int = 0,
        error_message: Optional[str] = None,
    ) -> TaskExecutionLog:
        """Log a single task execution result.

        :param task_id: Task identifier
        :param instance: TM1 instance name
        :param process: TI process name
        :param parameters: Process parameters
        :param success: Whether execution succeeded
        :param start_time: Execution start time
        :param end_time: Execution end time
        :param retry_count: Number of retries attempted
        :param error_message: Error message if failed
        :return: The created TaskExecutionLog entry
        """
        log = TaskExecutionLog.from_execution_result(
            workflow=self.workflow,
            task_id=task_id,
            instance=instance,
            process=process,
            parameters=parameters,
            success=success,
            start_time=start_time,
            end_time=end_time,
            retry_count=retry_count,
            error_message=error_message,
        )
        self.current_run.add_log(log)
        return log

    def flush(self) -> bool:
        """Write all accumulated logs to destinations.

        :return: True if all destinations succeeded
        """
        self.current_run.complete()
        all_success = True

        for destination in self.destinations:
            if destination.is_available():
                try:
                    success = destination.write_logs(self.current_run)
                    if not success:
                        all_success = False
                except Exception as e:
                    logger.error(f"Error writing to {type(destination).__name__}: {e}")
                    all_success = False
            else:
                logger.warning(f"{type(destination).__name__} is not available")

        return all_success

    @property
    def run_id(self) -> str:
        """Get the current run ID."""
        return self.current_run.run_id

    @property
    def log_count(self) -> int:
        """Get the number of logged tasks."""
        return len(self.current_run.task_logs)


def create_execution_logger(
    workflow: str,
) -> ExecutionLogger:
    """Factory function to create an ExecutionLogger.

    :param workflow: Workflow name
    :return: Configured ExecutionLogger instance with file destination
    """
    destinations = [FileLogDestination()]
    return ExecutionLogger(workflow, destinations)
