"""Integration tests for DAG execution on both TM1 v11 and v12 instances.

Tests normal mode, optimized mode, staged JSON, expandable tasks, and
cross-instance DAG execution.

Run with: pytest tests/integration/test_v11_v12_execution.py -v -m requires_tm1
"""

import asyncio
import os
import sys
import shutil
import tempfile
import time
import unittest

import pytest

_src_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "src"
)
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)
_integration_path = os.path.dirname(os.path.abspath(__file__))
if _integration_path not in sys.path:
    sys.path.insert(0, _integration_path)

from rushti.execution import setup_tm1_services, work_through_tasks_dag, logout, ExecutionContext
from rushti.parsing import build_dag
from conftest import get_all_test_tm1_configs, get_test_tm1_names
from tm1_setup import setup_tm1_test_objects

RESOURCES_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "resources", "integration"
)


class _BaseExecutionTest(unittest.TestCase):
    """Base class for execution tests with TM1 connection management."""

    INSTANCE = None  # Override in subclass
    _tm1_services = None
    _preserve_connections = None

    @classmethod
    def setUpClass(cls):
        configs, config_source = get_all_test_tm1_configs()
        if cls.INSTANCE and cls.INSTANCE not in configs:
            cls._tm1_available = False
            cls._config_path = None
            return

        cls._config_path = config_source

        # For cross-instance tests, use the first available task file
        if cls.INSTANCE:
            task_content = (
                f'instance="{cls.INSTANCE}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )
        else:
            # Cross-instance: need a task file referencing all instances
            instances = list(configs.keys())
            task_content = (
                "\n".join(
                    f'instance="{inst}" process="}}bedrock.server.wait" pWaitSec="1"'
                    for inst in instances
                )
                + "\n"
            )

        cls._test_dir = tempfile.mkdtemp()
        bootstrap_file = os.path.join(cls._test_dir, "bootstrap.txt")
        with open(bootstrap_file, "w") as f:
            f.write(task_content)

        try:
            cls._tm1_services, cls._preserve_connections = setup_tm1_services(
                max_workers=4,
                tasks_file_path=bootstrap_file,
                config_path=cls._config_path,
            )
            # Ensure test objects exist (with test-specific names)
            tm1_names = get_test_tm1_names()
            for inst, tm1 in cls._tm1_services.items():
                setup_tm1_test_objects(tm1, **tm1_names)

            if cls.INSTANCE:
                cls._tm1_available = cls.INSTANCE in cls._tm1_services
            else:
                cls._tm1_available = len(cls._tm1_services) >= 2
        except Exception as e:
            print(f"TM1 setup failed: {e}")
            cls._tm1_available = False
            cls._tm1_services = {}
            cls._preserve_connections = {}

    @classmethod
    def tearDownClass(cls):
        if cls._tm1_services:
            logout(cls._tm1_services, cls._preserve_connections or {})
        if hasattr(cls, "_test_dir"):
            shutil.rmtree(cls._test_dir, ignore_errors=True)

    def setUp(self):
        if not self._tm1_available:
            instance_desc = self.INSTANCE or "multiple instances"
            self.skipTest(f"{instance_desc} not available")

    def _run_dag(self, task_file, max_workers=4, expand=False):
        """Build and execute a DAG, return (results, elapsed_time)."""
        dag_result = build_dag(task_file, expand=expand, tm1_services=self._tm1_services)
        # build_dag returns either a DAG (for TXT) or a (DAG, Taskfile) tuple (for JSON)
        if isinstance(dag_result, tuple):
            dag = dag_result[0]
        else:
            dag = dag_result

        start = time.time()
        loop = asyncio.new_event_loop()
        results = loop.run_until_complete(
            work_through_tasks_dag(ExecutionContext(), dag, max_workers, 0, self._tm1_services)
        )
        loop.close()
        elapsed = time.time() - start
        return results, elapsed


@pytest.mark.requires_tm1
@pytest.mark.integration
class TestExecutionV11(_BaseExecutionTest):
    """Execution tests on TM1 v11 (tm1srv01)."""

    INSTANCE = "tm1srv01"

    def test_norm_mode_txt(self):
        """Normal mode execution from TXT file on v11."""
        results, elapsed = self._run_dag(os.path.join(RESOURCES_DIR, "tasks_v11_norm.txt"))
        self.assertEqual(len(results), 3)
        self.assertTrue(all(results))
        # 2 parallel + wait + 1 sequential = ~2s minimum
        self.assertGreaterEqual(elapsed, 2.0)

    def test_opt_mode_txt(self):
        """Optimized mode execution from TXT file on v11."""
        results, elapsed = self._run_dag(os.path.join(RESOURCES_DIR, "tasks_v11_opt.txt"))
        self.assertEqual(len(results), 3)
        self.assertTrue(all(results))
        # Tasks 1,2 parallel, then 3 after both = ~2s
        self.assertGreaterEqual(elapsed, 2.0)
        self.assertLess(elapsed, 8.0)

    def test_staged_json(self):
        """Staged pipeline from JSON file on v11."""
        results, elapsed = self._run_dag(os.path.join(RESOURCES_DIR, "tasks_v11_staged.json"))
        self.assertEqual(len(results), 4)
        self.assertTrue(all(results))

    def test_expanded_tasks(self):
        """Expandable tasks with MDX on v11."""
        results, elapsed = self._run_dag(
            os.path.join(RESOURCES_DIR, "tasks_v11_expanded.txt"),
            expand=True,
        )
        self.assertEqual(len(results), 3)
        self.assertTrue(all(results))


@pytest.mark.requires_tm1
@pytest.mark.integration
class TestExecutionV12(_BaseExecutionTest):
    """Execution tests on TM1 v12 (tm1srv02)."""

    INSTANCE = "tm1srv02"

    def test_norm_mode_txt(self):
        """Normal mode execution from TXT file on v12."""
        results, elapsed = self._run_dag(os.path.join(RESOURCES_DIR, "tasks_v12_norm.txt"))
        self.assertEqual(len(results), 3)
        self.assertTrue(all(results))
        self.assertGreaterEqual(elapsed, 2.0)

    def test_opt_mode_txt(self):
        """Optimized mode execution from TXT file on v12."""
        results, elapsed = self._run_dag(os.path.join(RESOURCES_DIR, "tasks_v12_opt.txt"))
        self.assertEqual(len(results), 3)
        self.assertTrue(all(results))
        self.assertGreaterEqual(elapsed, 2.0)
        self.assertLess(elapsed, 8.0)

    def test_staged_json(self):
        """Staged pipeline from JSON file on v12."""
        results, elapsed = self._run_dag(os.path.join(RESOURCES_DIR, "tasks_v12_staged.json"))
        self.assertEqual(len(results), 4)
        self.assertTrue(all(results))

    def test_expanded_tasks(self):
        """Expandable tasks with MDX on v12."""
        results, elapsed = self._run_dag(
            os.path.join(RESOURCES_DIR, "tasks_v12_expanded.txt"),
            expand=True,
        )
        self.assertEqual(len(results), 3)
        self.assertTrue(all(results))


@pytest.mark.requires_tm1
@pytest.mark.requires_multi_instance
@pytest.mark.integration
class TestCrossInstanceExecution(_BaseExecutionTest):
    """Cross-instance execution tests spanning v11 and v12."""

    INSTANCE = None  # Needs both instances

    def test_cross_instance_opt(self):
        """Cross-instance DAG from TXT file."""
        results, elapsed = self._run_dag(
            os.path.join(RESOURCES_DIR, "tasks_cross_instance_opt.txt")
        )
        self.assertEqual(len(results), 4)
        self.assertTrue(all(results))

    def test_cross_instance_staged_json(self):
        """Cross-instance staged pipeline from JSON file."""
        results, elapsed = self._run_dag(
            os.path.join(RESOURCES_DIR, "tasks_cross_instance_staged.json")
        )
        self.assertEqual(len(results), 4)
        self.assertTrue(all(results))


@pytest.mark.requires_tm1
@pytest.mark.integration
class TestCubeSourceExecution(_BaseExecutionTest):
    """Tests for reading tasks from TM1 cube and executing."""

    INSTANCE = "tm1srv01"

    def test_cube_source_sample_optimal_mode(self):
        """Read and execute Sample_Optimal_Mode from TM1 cube."""
        from rushti.taskfile import load_taskfile_from_source, TaskfileSource

        source = TaskfileSource(
            tm1_instance=self.INSTANCE,
            workflow="Sample_Optimal_Mode",
        )

        tm1_names = get_test_tm1_names()
        try:
            taskfile = load_taskfile_from_source(source, self._config_path, mode="opt", **tm1_names)
            self.assertIsNotNone(taskfile)
            self.assertGreater(len(taskfile.tasks), 0)
        except Exception as e:
            if "not found" in str(e).lower() or "no tasks" in str(e).lower():
                self.skipTest(f"Sample workflow not in cube: {e}")
            raise


if __name__ == "__main__":
    unittest.main()
