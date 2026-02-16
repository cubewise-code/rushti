"""RushTI task domain model.

This module defines the core task representations used throughout the
execution pipeline:
- Wait: Marker for norm-mode sequence boundaries
- Task: Base task for norm-mode execution
- OptimizedTask: Extended task with explicit predecessors for opt-mode
- ExecutionMode: Enum for execution mode selection (norm vs opt)
"""

from enum import Enum
from typing import Dict, List, Any, Optional


class Wait:
    def __init__(self):
        pass

    # useful for testing
    def __eq__(self, other):
        if isinstance(other, Wait):
            return True

        return False


class Task:
    id = 1

    @classmethod
    def reset_id_counter(cls):
        """Reset the task ID counter to 1.

        Call this at the start of each execution run to ensure
        task IDs start from 1 for each execution.
        """
        cls.id = 1

    def __init__(
        self,
        instance_name: str,
        process_name: str,
        parameters: Dict[str, Any] = None,
        succeed_on_minor_errors: bool = False,
        safe_retry: bool = False,
        stage: Optional[str] = None,
        timeout: Optional[int] = None,
        cancel_at_timeout: bool = False,
    ):
        self.id = Task.id
        self.instance_name = instance_name
        self.process_name = process_name
        self.parameters = parameters
        self.succeed_on_minor_errors = succeed_on_minor_errors
        self.safe_retry = safe_retry
        self.stage = stage
        self.timeout = timeout
        self.cancel_at_timeout = cancel_at_timeout

        Task.id = Task.id + 1

    def translate_to_line(self):
        parts = [
            f'instance="{self.instance_name}"',
            f'process="{self.process_name}"',
            f'succeed_on_minor_errors="{self.succeed_on_minor_errors}"',
        ]
        if self.safe_retry:
            parts.append(f'safe_retry="{self.safe_retry}"')
        if self.stage:
            parts.append(f'stage="{self.stage}"')
        if self.timeout is not None:
            parts.append(f'timeout="{self.timeout}"')
        if self.cancel_at_timeout:
            parts.append(f'cancel_at_timeout="{self.cancel_at_timeout}"')
        if self.parameters:
            parts.extend(f'{parameter}="{value}"' for parameter, value in self.parameters.items())
        return " ".join(parts) + "\n"


class OptimizedTask(Task):
    def __init__(
        self,
        task_id: str,
        instance_name: str,
        process_name: str,
        parameters: Dict[str, Any],
        predecessors: List,
        require_predecessor_success: bool,
        succeed_on_minor_errors: bool = False,
        safe_retry: bool = False,
        stage: Optional[str] = None,
        timeout: Optional[int] = None,
        cancel_at_timeout: bool = False,
    ):
        super().__init__(
            instance_name,
            process_name,
            parameters,
            succeed_on_minor_errors,
            safe_retry,
            stage,
            timeout,
            cancel_at_timeout,
        )
        self.id = task_id
        self.predecessors = predecessors
        self.require_predecessor_success = require_predecessor_success
        self.successors = list()

    @property
    def has_predecessors(self):
        return len(self.predecessors) > 0

    @property
    def has_successors(self):
        return len(self.successors) > 0

    def translate_to_line(self):
        parts = [
            f'id="{self.id}"',
            f'predecessors="{",".join(map(str, self.predecessors))}"',
            f'require_predecessor_success="{self.require_predecessor_success}"',
            f'succeed_on_minor_errors="{self.succeed_on_minor_errors}"',
        ]
        if self.safe_retry:
            parts.append(f'safe_retry="{self.safe_retry}"')
        if self.stage:
            parts.append(f'stage="{self.stage}"')
        if self.timeout is not None:
            parts.append(f'timeout="{self.timeout}"')
        if self.cancel_at_timeout:
            parts.append(f'cancel_at_timeout="{self.cancel_at_timeout}"')
        parts.append(f'instance="{self.instance_name}"')
        parts.append(f'process="{self.process_name}"')
        if self.parameters:
            parts.extend(f'{parameter}="{value}"' for parameter, value in self.parameters.items())
        return " ".join(parts) + "\n"


class ExecutionMode(Enum):
    NORM = 1
    OPT = 2

    @classmethod
    def _missing_(cls, value):
        for member in cls:
            if member.name.lower() == value.lower():
                return member
        # default
        return cls.NORM
