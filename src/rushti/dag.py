"""RushTI DAG domain model.

This module defines the Directed Acyclic Graph used for task execution
scheduling and its supporting types:
- CircularDependencyError: Raised when cycle detection finds a loop
- TaskStatus: Enum tracking per-task execution state
- DAG: The core execution graph with dependency-aware scheduling
- convert_norm_to_dag: Build a DAG from norm-mode (Wait-separated) task lists
- convert_opt_to_dag: Build a DAG from opt-mode (explicit predecessor) task dicts
"""

from collections import deque
from enum import Enum
from typing import List, Dict, Any, Optional, Set

from rushti.task import Wait


class CircularDependencyError(Exception):
    """Raised when a circular dependency is detected in the task DAG."""

    def __init__(self, cycle_path: List[str]):
        self.cycle_path = cycle_path
        cycle_str = " -> ".join(cycle_path)
        super().__init__(f"Circular dependency detected: {cycle_str}")


class TaskStatus(Enum):
    """Status of a task in the DAG."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class DAG:
    """Directed Acyclic Graph for task execution scheduling.

    This class represents tasks and their dependencies, allowing for
    efficient scheduling of tasks as soon as their predecessors complete.
    """

    def __init__(self):
        # task_id -> list of Task objects (multiple tasks can share same id)
        self._tasks: Dict[str, List[Any]] = {}
        # task_id -> set of predecessor task_ids
        self._predecessors: Dict[str, Set[str]] = {}
        # task_id -> set of successor task_ids
        self._successors: Dict[str, Set[str]] = {}
        # task_id -> TaskStatus
        self._status: Dict[str, TaskStatus] = {}
        # task_id -> success (True/False) for completed tasks
        self._results: Dict[str, bool] = {}
        # Track which individual task instances have been dispatched (to prevent re-dispatch)
        self._dispatched_instances: Set[int] = set()
        # Track which individual task instances have completed (for completion check)
        self._completed_instances: Set[int] = set()

    def add_task(self, task) -> None:
        """Add a task to the DAG.

        :param task: Task or OptimizedTask instance
        """
        task_id = str(task.id)

        if task_id not in self._tasks:
            self._tasks[task_id] = []
            self._predecessors[task_id] = set()
            self._successors[task_id] = set()
            self._status[task_id] = TaskStatus.PENDING

        self._tasks[task_id].append(task)

        # Add dependencies from task's predecessors attribute if it exists
        if hasattr(task, "predecessors") and task.predecessors:
            for pred_id in task.predecessors:
                self.add_dependency(task_id, str(pred_id))

    def add_dependency(self, task_id: str, predecessor_id: str) -> None:
        """Add a dependency: task_id depends on predecessor_id.

        :param task_id: ID of the dependent task
        :param predecessor_id: ID of the predecessor task
        """
        task_id = str(task_id)
        predecessor_id = str(predecessor_id)

        # Initialize if not present
        if task_id not in self._predecessors:
            self._predecessors[task_id] = set()
        if task_id not in self._successors:
            self._successors[task_id] = set()
        if predecessor_id not in self._predecessors:
            self._predecessors[predecessor_id] = set()
        if predecessor_id not in self._successors:
            self._successors[predecessor_id] = set()

        self._predecessors[task_id].add(predecessor_id)
        self._successors[predecessor_id].add(task_id)

    def validate(self) -> None:
        """Validate the DAG has no circular dependencies.

        Uses Kahn's algorithm for topological sort which naturally detects cycles.

        :raises CircularDependencyError: If a cycle is detected
        """
        # Calculate in-degree for each node
        in_degree = {task_id: len(preds) for task_id, preds in self._predecessors.items()}

        # Queue of nodes with no incoming edges
        queue = deque([task_id for task_id, degree in in_degree.items() if degree == 0])

        visited_count = 0

        while queue:
            task_id = queue.popleft()
            visited_count += 1

            for successor_id in self._successors.get(task_id, set()):
                in_degree[successor_id] -= 1
                if in_degree[successor_id] == 0:
                    queue.append(successor_id)

        # If we didn't visit all nodes, there's a cycle
        if visited_count != len(self._tasks):
            # Find the cycle for error reporting
            cycle_path = self._find_cycle()
            raise CircularDependencyError(cycle_path)

    def _find_cycle(self) -> List[str]:
        """Find and return a cycle path for error reporting.

        Uses DFS to detect and return the cycle path.

        :return: List of task IDs forming the cycle
        """
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {task_id: WHITE for task_id in self._tasks}
        parent = {}

        def dfs(node: str) -> Optional[List[str]]:
            color[node] = GRAY

            for successor in self._successors.get(node, set()):
                if color.get(successor, WHITE) == GRAY:
                    # Found cycle, reconstruct path
                    cycle = [successor, node]
                    current = node
                    while parent.get(current) and parent[current] != successor:
                        current = parent[current]
                        cycle.append(current)
                    cycle.append(successor)
                    return list(reversed(cycle))

                if color.get(successor, WHITE) == WHITE:
                    parent[successor] = node
                    result = dfs(successor)
                    if result:
                        return result

            color[node] = BLACK
            return None

        for task_id in self._tasks:
            if color[task_id] == WHITE:
                result = dfs(task_id)
                if result:
                    return result

        return ["unknown cycle"]

    def get_ready_tasks(self) -> List[Any]:
        """Get all tasks that are ready to execute.

        A task is ready when all its predecessors have completed successfully
        (or the task doesn't require predecessor success).

        For task IDs with multiple instances (expanded tasks), this returns
        undispatched instances even when some instances are already running.

        :return: List of Task instances ready for execution
        """
        ready = []

        for task_id, status in self._status.items():
            # Skip completed, failed, or skipped tasks - they're done
            if status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.SKIPPED):
                continue

            # For RUNNING tasks, predecessors were already satisfied when the
            # first instance was dispatched. For PENDING tasks, check predecessors.
            if status == TaskStatus.PENDING:
                # Check if all predecessors are complete
                predecessors = self._predecessors.get(task_id, set())
                all_predecessors_done = all(
                    self._status.get(pred_id)
                    in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.SKIPPED)
                    for pred_id in predecessors
                )
                if not all_predecessors_done:
                    continue

            # Return undispatched instances (works for both PENDING and RUNNING)
            for task in self._tasks.get(task_id, []):
                # Skip already dispatched instances
                if id(task) not in self._dispatched_instances:
                    ready.append(task)

        return ready

    def mark_running(self, task) -> None:
        """Mark a task instance as currently running.

        :param task: The task instance being executed
        """
        task_id = str(task.id)
        self._status[task_id] = TaskStatus.RUNNING
        self._dispatched_instances.add(id(task))

    def mark_complete(self, task, success: bool) -> None:
        """Mark a task instance as completed.

        :param task: The task instance that completed, or task_id string to mark
                     ALL instances of that task_id as completed (for checkpoint resume)
        :param success: Whether the task succeeded
        """
        # Handle both task object and task_id string
        if isinstance(task, str):
            # String task_id: mark ALL instances as completed (for checkpoint resume)
            task_id = task
            for t in self._tasks.get(task_id, []):
                self._dispatched_instances.add(id(t))
                self._completed_instances.add(id(t))
        else:
            # Task object: mark only this specific instance as completed
            task_id = str(task.id)
            self._completed_instances.add(id(task))

        # Store result - if multiple tasks share same ID, aggregate with AND
        if task_id in self._results:
            self._results[task_id] = self._results[task_id] and success
        else:
            self._results[task_id] = success

        # Check if all task instances for this ID have completed
        all_completed = all(
            id(t) in self._completed_instances for t in self._tasks.get(task_id, [])
        )

        if all_completed:
            self._status[task_id] = (
                TaskStatus.COMPLETED if self._results[task_id] else TaskStatus.FAILED
            )

    def mark_skipped(self, task_id: str) -> None:
        """Mark a task as skipped (due to failed predecessor).

        :param task_id: ID of the task to skip
        """
        task_id = str(task_id)
        self._status[task_id] = TaskStatus.SKIPPED
        self._results[task_id] = False
        # Mark all instances as dispatched and completed (skipped)
        for task in self._tasks.get(task_id, []):
            self._dispatched_instances.add(id(task))
            self._completed_instances.add(id(task))

    def get_task_result(self, task_id: str) -> Optional[bool]:
        """Get the execution result for a task.

        :param task_id: ID of the task
        :return: True if succeeded, False if failed, None if not completed
        """
        return self._results.get(str(task_id))

    def is_complete(self) -> bool:
        """Check if all tasks in the DAG have completed.

        :return: True if all tasks are done (completed, failed, or skipped)
        """
        return all(
            status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.SKIPPED)
            for status in self._status.values()
        )

    def get_all_tasks(self) -> List[Any]:
        """Get all task instances in the DAG.

        :return: Flat list of all task instances
        """
        all_tasks = []
        for task_list in self._tasks.values():
            all_tasks.extend(task_list)
        return all_tasks

    def get_execution_results(self) -> Dict[str, bool]:
        """Get the execution results for all completed tasks.

        :return: Dictionary mapping task_id to success status
        """
        return dict(self._results)

    def __len__(self) -> int:
        """Return the number of unique task IDs in the DAG."""
        return len(self._tasks)

    def apply_stage_ordering(self, stage_order: List[str]) -> None:
        """Apply stage ordering to the DAG.

        When stage_order is defined, all tasks in stage N must complete before
        any task in stage N+1 can start. This is additive to explicit predecessors.

        :param stage_order: List of stage names in execution order
        """
        if not stage_order:
            return

        # Group task IDs by stage
        tasks_by_stage: Dict[str, List[str]] = {stage: [] for stage in stage_order}

        for task_id, task_list in self._tasks.items():
            # All tasks with same ID have same stage (use first one)
            if task_list:
                task_stage = getattr(task_list[0], "stage", None)
                if task_stage and task_stage in tasks_by_stage:
                    tasks_by_stage[task_stage].append(task_id)

        # Add dependencies: all tasks in stage N are predecessors of all tasks in stage N+1
        for i in range(1, len(stage_order)):
            prev_stage = stage_order[i - 1]
            curr_stage = stage_order[i]

            prev_stage_tasks = tasks_by_stage.get(prev_stage, [])
            curr_stage_tasks = tasks_by_stage.get(curr_stage, [])

            for curr_task_id in curr_stage_tasks:
                for prev_task_id in prev_stage_tasks:
                    self.add_dependency(curr_task_id, prev_task_id)


def convert_norm_to_dag(tasks_and_waits: List[Any]) -> DAG:
    """Convert a norm-mode task list (with Wait markers) to a DAG structure.

    In norm mode, tasks are grouped into sequences separated by 'wait' keywords.
    All tasks in sequence N become predecessors of all tasks in sequence N+1.

    Example:
        Sequence 0: [Task A, Task B]
        wait
        Sequence 1: [Task C, Task D]
        wait
        Sequence 2: [Task E]

        Converts to:
        - Task A: predecessors=[]
        - Task B: predecessors=[]
        - Task C: predecessors=[A, B]
        - Task D: predecessors=[A, B]
        - Task E: predecessors=[C, D]

    :param tasks_and_waits: List of Task and Wait objects from norm mode file
    :return: DAG structure with proper dependencies
    """
    dag = DAG()

    # Group tasks by sequence (separated by Wait markers)
    sequences: List[List[Any]] = []
    current_sequence: List[Any] = []

    for item in tasks_and_waits:
        if isinstance(item, Wait):
            if current_sequence:
                sequences.append(current_sequence)
                current_sequence = []
        else:
            current_sequence.append(item)

    # Don't forget the last sequence
    if current_sequence:
        sequences.append(current_sequence)

    # Build the DAG with proper dependencies
    previous_sequence_ids: List[str] = []

    for sequence in sequences:
        current_sequence_ids: List[str] = []

        for task in sequence:
            task_id = str(task.id)
            dag.add_task(task)
            current_sequence_ids.append(task_id)

            # Add dependencies to all tasks from previous sequence
            for pred_id in previous_sequence_ids:
                dag.add_dependency(task_id, pred_id)

        previous_sequence_ids = current_sequence_ids

    return dag


def convert_opt_to_dag(tasks: Dict[str, List[Any]]) -> DAG:
    """Convert an opt-mode task dictionary to a DAG structure.

    In opt mode, tasks already have explicit predecessors defined.

    :param tasks: Dictionary mapping task_id to list of Task objects
    :return: DAG structure with proper dependencies
    """
    dag = DAG()

    # Add all tasks first
    for task_id, task_list in tasks.items():
        for task in task_list:
            dag.add_task(task)

    # Dependencies are automatically added via task.predecessors in add_task()
    return dag
