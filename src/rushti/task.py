"""RushTI task domain model.

This module defines the core task representations used throughout the
execution pipeline:
- Wait: Marker for norm-mode sequence boundaries
- Task: Base task for norm-mode execution
- OptimizedTask: Extended task with explicit predecessors for opt-mode
- ExecutionMode: Enum for execution mode selection (norm vs opt)

A task is polymorphic on its **kind**: it either executes a TI
``process`` (with parameters) or a TM1 ``chore`` (no parameters). The
field name is the discriminator; there is no separate ``kind`` field.
Mutual exclusion of ``process_name`` and ``chore_name`` is enforced as
a class invariant on ``Task.__init__`` so downstream code can rely on
exactly one being set without re-checking. See ADR 0002.
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
        process_name: Optional[str] = None,
        parameters: Dict[str, Any] = None,
        succeed_on_minor_errors: bool = False,
        safe_retry: bool = False,
        stage: Optional[str] = None,
        timeout: Optional[int] = None,
        cancel_at_timeout: bool = False,
        chore_name: Optional[str] = None,
    ):
        # Class invariant: exactly one of process_name / chore_name is set.
        # The five Task(...) construction sites in parsing.py plus internal
        # paths in execution.py make a class-level check cheaper than
        # auditing each site individually — see ADR 0002 §2.
        if process_name and chore_name:
            raise ValueError(
                "Task: 'process_name' and 'chore_name' are mutually exclusive — "
                "exactly one must be set"
            )
        if not process_name and not chore_name:
            raise ValueError("Task: exactly one of 'process_name' or 'chore_name' must be set")

        self.id = Task.id
        self.instance_name = instance_name
        self.process_name = process_name
        self.chore_name = chore_name
        self.parameters = parameters
        self.succeed_on_minor_errors = succeed_on_minor_errors
        self.safe_retry = safe_retry
        self.stage = stage
        self.timeout = timeout
        self.cancel_at_timeout = cancel_at_timeout

        Task.id = Task.id + 1

    def translate_to_line(self):
        if self.chore_name:
            # Chores are intentionally narrower than processes — no
            # parameters, no minor-error tier, no timeout, no
            # cancel_at_timeout. Only safe_retry and stage are meaningful
            # alongside the kind-specific identity.
            parts = [
                f'instance="{self.instance_name}"',
                f'chore="{self.chore_name}"',
            ]
            if self.safe_retry:
                parts.append(f'safe_retry="{self.safe_retry}"')
            if self.stage:
                parts.append(f'stage="{self.stage}"')
            return " ".join(parts) + "\n"

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
        process_name: Optional[str] = None,
        parameters: Dict[str, Any] = None,
        predecessors: List = None,
        require_predecessor_success: bool = False,
        succeed_on_minor_errors: bool = False,
        safe_retry: bool = False,
        stage: Optional[str] = None,
        timeout: Optional[int] = None,
        cancel_at_timeout: bool = False,
        chore_name: Optional[str] = None,
    ):
        super().__init__(
            instance_name=instance_name,
            process_name=process_name,
            parameters=parameters,
            succeed_on_minor_errors=succeed_on_minor_errors,
            safe_retry=safe_retry,
            stage=stage,
            timeout=timeout,
            cancel_at_timeout=cancel_at_timeout,
            chore_name=chore_name,
        )
        self.id = task_id
        self.predecessors = predecessors if predecessors is not None else []
        self.require_predecessor_success = require_predecessor_success
        self.successors = list()

    @property
    def has_predecessors(self):
        return len(self.predecessors) > 0

    @property
    def has_successors(self):
        return len(self.successors) > 0

    def translate_to_line(self):
        if self.chore_name:
            parts = [
                f'id="{self.id}"',
                f'predecessors="{",".join(map(str, self.predecessors))}"',
                f'require_predecessor_success="{self.require_predecessor_success}"',
            ]
            if self.safe_retry:
                parts.append(f'safe_retry="{self.safe_retry}"')
            if self.stage:
                parts.append(f'stage="{self.stage}"')
            parts.append(f'instance="{self.instance_name}"')
            parts.append(f'chore="{self.chore_name}"')
            return " ".join(parts) + "\n"

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
