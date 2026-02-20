"""Unit tests for per-stage worker limits in DAG execution.

Tests that stage_workers parameter in work_through_tasks_dag() correctly
limits concurrency per stage while respecting the global max_workers ceiling.
"""

import asyncio
import os
import sys
import threading
import time
import unittest
from unittest.mock import patch

# Path setup handled by conftest.py, but also support direct execution
_src_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "src"
)
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

from rushti.dag import DAG  # noqa: E402
from rushti.task import Task  # noqa: E402
from rushti.execution import work_through_tasks_dag, ExecutionContext  # noqa: E402


def _make_task(task_id, stage=None, instance="tm1srv01", process="proc"):
    """Helper to create a Task with a specific id and optional stage."""
    task = Task(instance, process, {}, stage=stage)
    task.id = task_id
    return task


def _build_concurrency_tracker():
    """Build a mock execute_task that tracks per-stage concurrency.

    Returns (mock_fn, tracker) where tracker has:
      - max_concurrent: dict[stage, int] - peak concurrency per stage
      - max_concurrent_total: int - peak total concurrency
    """
    lock = threading.Lock()
    running_per_stage = {}
    running_total = [0]
    tracker = {
        "max_concurrent": {},
        "max_concurrent_total": 0,
    }

    def mock_execute_task(ctx, task, retries, tm1_services):
        stage = getattr(task, "stage", None) or "__none__"
        with lock:
            running_per_stage[stage] = running_per_stage.get(stage, 0) + 1
            running_total[0] += 1
            # Track peak
            if running_per_stage[stage] > tracker["max_concurrent"].get(stage, 0):
                tracker["max_concurrent"][stage] = running_per_stage[stage]
            if running_total[0] > tracker["max_concurrent_total"]:
                tracker["max_concurrent_total"] = running_total[0]

        # Simulate some work so concurrent tasks overlap
        time.sleep(0.05)

        with lock:
            running_per_stage[stage] -= 1
            running_total[0] -= 1
        return True

    return mock_execute_task, tracker


class TestStageWorkers(unittest.TestCase):
    """Tests for per-stage concurrency limiting in work_through_tasks_dag."""

    def _run_dag(self, dag, max_workers, stage_workers=None):
        """Helper to run a DAG through work_through_tasks_dag with mocked execute_task."""
        mock_fn, tracker = _build_concurrency_tracker()
        with patch("rushti.execution.execute_task", mock_fn):
            loop = asyncio.new_event_loop()
            try:
                results = loop.run_until_complete(
                    work_through_tasks_dag(
                        ExecutionContext(),
                        dag,
                        max_workers,
                        0,
                        {},
                        stage_workers=stage_workers,
                    )
                )
            finally:
                loop.close()
        return results, tracker

    def test_no_stage_workers_all_tasks_run(self):
        """Without stage_workers, all ready tasks run up to max_workers."""
        dag = DAG()
        for i in range(5):
            dag.add_task(_make_task(str(i), stage="extract"))

        results, tracker = self._run_dag(dag, max_workers=5, stage_workers=None)

        self.assertEqual(len(results), 5)
        self.assertTrue(all(results))
        # All 5 should have been able to run concurrently
        self.assertEqual(tracker["max_concurrent_total"], 5)

    def test_stage_workers_limits_concurrency(self):
        """stage_workers limits how many tasks from a stage run concurrently."""
        dag = DAG()
        for i in range(6):
            dag.add_task(_make_task(str(i), stage="extract"))

        results, tracker = self._run_dag(dag, max_workers=6, stage_workers={"extract": 2})

        self.assertEqual(len(results), 6)
        self.assertTrue(all(results))
        # Only 2 extract tasks should have run concurrently
        self.assertLessEqual(tracker["max_concurrent"]["extract"], 2)

    def test_global_max_workers_still_caps(self):
        """Global max_workers caps even when stage_workers is higher."""
        dag = DAG()
        for i in range(6):
            dag.add_task(_make_task(str(i), stage="extract"))

        results, tracker = self._run_dag(dag, max_workers=3, stage_workers={"extract": 10})

        self.assertEqual(len(results), 6)
        self.assertTrue(all(results))
        # Global cap of 3 should apply
        self.assertLessEqual(tracker["max_concurrent_total"], 3)

    def test_multi_stage_independent_limits(self):
        """Different stages have independent concurrency limits."""
        dag = DAG()
        # 4 extract tasks, 4 load tasks — no dependencies between them
        for i in range(4):
            dag.add_task(_make_task(f"e{i}", stage="extract"))
        for i in range(4):
            dag.add_task(_make_task(f"l{i}", stage="load"))

        results, tracker = self._run_dag(
            dag, max_workers=8, stage_workers={"extract": 3, "load": 1}
        )

        self.assertEqual(len(results), 8)
        self.assertTrue(all(results))
        self.assertLessEqual(tracker["max_concurrent"]["extract"], 3)
        self.assertLessEqual(tracker["max_concurrent"]["load"], 1)

    def test_tasks_without_stage_not_limited(self):
        """Tasks without a stage attribute are not limited by stage_workers."""
        dag = DAG()
        # 4 tasks with no stage
        for i in range(4):
            dag.add_task(_make_task(str(i), stage=None))

        results, tracker = self._run_dag(dag, max_workers=4, stage_workers={"extract": 1})

        self.assertEqual(len(results), 4)
        self.assertTrue(all(results))
        # All 4 should run concurrently (no stage = no stage limit)
        self.assertEqual(tracker["max_concurrent_total"], 4)

    def test_stage_not_in_stage_workers_uses_global(self):
        """A stage not listed in stage_workers falls back to global max_workers."""
        dag = DAG()
        # "transform" stage not in stage_workers
        for i in range(4):
            dag.add_task(_make_task(str(i), stage="transform"))

        results, tracker = self._run_dag(dag, max_workers=4, stage_workers={"extract": 1})

        self.assertEqual(len(results), 4)
        self.assertTrue(all(results))
        # transform is not in stage_workers, so all 4 run concurrently (global cap)
        self.assertEqual(tracker["max_concurrent_total"], 4)

    def test_stage_workers_with_stage_ordering(self):
        """stage_workers works correctly with sequential stage ordering."""
        dag = DAG()
        for i in range(4):
            dag.add_task(_make_task(f"e{i}", stage="extract"))
        for i in range(4):
            dag.add_task(_make_task(f"l{i}", stage="load"))

        # Stage ordering: extract before load
        dag.apply_stage_ordering(["extract", "load"])

        results, tracker = self._run_dag(
            dag, max_workers=4, stage_workers={"extract": 2, "load": 1}
        )

        self.assertEqual(len(results), 8)
        self.assertTrue(all(results))
        self.assertLessEqual(tracker["max_concurrent"]["extract"], 2)
        self.assertLessEqual(tracker["max_concurrent"]["load"], 1)


if __name__ == "__main__":
    unittest.main()
