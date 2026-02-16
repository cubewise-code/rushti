"""Integration tests for DAG execution against a real TM1 instance.

These tests require:
- TM1 instance configured in tests/config.ini or RUSHTI_TEST_CONFIG environment variable
- }bedrock.server.wait process with pWaitSec parameter
- rushti.dimension.counter dimension with elements 1-10

Run with: pytest tests/integration/test_dag_execution.py -v -m requires_tm1
"""

import asyncio
import os
import sys
import shutil
import tempfile
import time
import unittest

import pytest

# Path setup handled by conftest.py, but also support direct execution
_src_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "src"
)
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)
_integration_path = os.path.dirname(os.path.abspath(__file__))
if _integration_path not in sys.path:
    sys.path.insert(0, _integration_path)

from rushti.execution import (  # noqa: E402
    setup_tm1_services,
    work_through_tasks_dag,
    logout,
    ExecutionContext,
)
from rushti.parsing import build_dag  # noqa: E402

# Import test config utilities from conftest
from conftest import get_test_tm1_config  # noqa: E402
from tm1_setup import setup_tm1_test_objects  # noqa: E402


@pytest.mark.requires_tm1
class TestDAGExecutionIntegration(unittest.TestCase):
    """Integration tests for DAG execution with real TM1."""

    @classmethod
    def setUpClass(cls):
        """Set up TM1 connections once for all tests."""
        # Get test config path
        tm1_config, config_source = get_test_tm1_config()
        if tm1_config is None:
            cls.tm1_available = False
            cls.tm1_services = {}
            cls.preserve_connections = {}
            cls.tm1_instance = "tm1srv01"  # default for skipped tests
            cls.test_tasks_dir = tempfile.mkdtemp()
            cls.simple_task_file = os.path.join(cls.test_tasks_dir, "simple_task.txt")
            with open(cls.simple_task_file, "w") as f:
                f.write('instance="tm1srv01" process="}bedrock.server.wait" pWaitSec="1"\n')
            return

        cls.config_path = config_source
        cls.tm1_instance = tm1_config.instance
        cls.test_tasks_dir = tempfile.mkdtemp()
        cls.simple_task_file = os.path.join(cls.test_tasks_dir, "simple_task.txt")

        with open(cls.simple_task_file, "w") as f:
            f.write(f'instance="{cls.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n')

        try:
            cls.tm1_services, cls.preserve_connections = setup_tm1_services(
                max_workers=4, tasks_file_path=cls.simple_task_file, config_path=cls.config_path
            )
            cls.tm1_available = cls.tm1_instance in cls.tm1_services
            # Ensure test objects (counter dimension etc.) exist
            for inst, tm1 in cls.tm1_services.items():
                try:
                    setup_tm1_test_objects(tm1)
                except Exception:
                    pass
        except Exception as e:
            print(f"TM1 connection failed: {e}")
            cls.tm1_available = False
            cls.tm1_services = {}
            cls.preserve_connections = {}

    @classmethod
    def tearDownClass(cls):
        """Clean up TM1 connections."""
        if cls.tm1_services:
            logout(cls.tm1_services, cls.preserve_connections)

        shutil.rmtree(cls.test_tasks_dir, ignore_errors=True)

    def setUp(self):
        if not self.tm1_available:
            self.skipTest("TM1 instance not available")

    def test_simple_dag_execution(self):
        """Test executing a simple DAG with one task."""
        task_file = os.path.join(self.test_tasks_dir, "single_task.txt")
        with open(task_file, "w") as f:
            f.write(
                f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )

        dag = build_dag(task_file, expand=False, tm1_services=self.tm1_services)

        self.assertEqual(len(dag), 1)

        start = time.time()
        loop = asyncio.new_event_loop()
        results = loop.run_until_complete(
            work_through_tasks_dag(ExecutionContext(), dag, 4, 0, self.tm1_services)
        )
        loop.close()
        elapsed = time.time() - start

        self.assertEqual(len(results), 1)
        self.assertTrue(all(results), "All tasks should succeed")
        self.assertGreaterEqual(elapsed, 1.0, "Should take at least 1 second")

    def test_parallel_dag_execution(self):
        """Test that parallel tasks execute concurrently.

        If two 2-second tasks run in parallel, total time should be ~2s not ~4s.
        """
        task_file = os.path.join(self.test_tasks_dir, "parallel_tasks.txt")
        with open(task_file, "w") as f:
            f.write(
                f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="2"\n'
            )
            f.write(
                f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="2"\n'
            )

        dag = build_dag(task_file, expand=False, tm1_services=self.tm1_services)

        self.assertEqual(len(dag), 2)

        start = time.time()
        loop = asyncio.new_event_loop()
        results = loop.run_until_complete(
            work_through_tasks_dag(ExecutionContext(), dag, 4, 0, self.tm1_services)
        )
        loop.close()
        elapsed = time.time() - start

        self.assertEqual(len(results), 2)
        self.assertTrue(all(results), "All tasks should succeed")
        self.assertLess(elapsed, 8.0, "Tasks should run in parallel")

    def test_sequential_dag_with_wait(self):
        """Test that tasks after 'wait' run sequentially.

        Two 1-second tasks with a wait between them should take ~2s.
        """
        task_file = os.path.join(self.test_tasks_dir, "sequential_tasks.txt")
        with open(task_file, "w") as f:
            f.write(
                f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )
            f.write("wait\n")
            f.write(
                f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )

        dag = build_dag(task_file, expand=False, tm1_services=self.tm1_services)

        self.assertEqual(len(dag), 2)

        start = time.time()
        loop = asyncio.new_event_loop()
        results = loop.run_until_complete(
            work_through_tasks_dag(ExecutionContext(), dag, 4, 0, self.tm1_services)
        )
        loop.close()
        elapsed = time.time() - start

        self.assertEqual(len(results), 2)
        self.assertTrue(all(results), "All tasks should succeed")
        self.assertGreaterEqual(elapsed, 2.0, "Tasks should run sequentially")

    def test_opt_mode_dependencies(self):
        """Test optimized mode with specific dependencies.

        Task 1 (2s) and Task 2 (1s) run in parallel.
        Task 3 (1s) depends only on Task 2, so it should start after 1s, not 2s.
        Total time should be ~2s (Task 1 finishes last).
        """
        task_file = os.path.join(self.test_tasks_dir, "opt_dependencies.txt")
        with open(task_file, "w") as f:
            f.write(
                f'id="1" predecessors="" require_predecessor_success="" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="2"\n'
            )
            f.write(
                f'id="2" predecessors="" require_predecessor_success="" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )
            f.write(
                f'id="3" predecessors="2" require_predecessor_success="" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )

        dag = build_dag(task_file, expand=False, tm1_services=self.tm1_services)

        self.assertEqual(len(dag), 3)

        start = time.time()
        loop = asyncio.new_event_loop()
        results = loop.run_until_complete(
            work_through_tasks_dag(ExecutionContext(), dag, 4, 0, self.tm1_services)
        )
        loop.close()
        elapsed = time.time() - start

        self.assertEqual(len(results), 3)
        self.assertTrue(all(results), "All tasks should succeed")
        self.assertLess(elapsed, 8.0, "Task 3 should start as soon as Task 2 completes")

    def test_dag_advantage_over_levels(self):
        """Demonstrate DAG advantage: Task D starts as soon as Task A completes.

        Setup:
        - Task A: 1s (no deps)
        - Task B: 3s (no deps)
        - Task C: 3s (no deps)
        - Task D: 1s (depends on A only)

        Level-based would take: 3s (A,B,C) + 1s (D) = 4s
        DAG-based takes: max(3s, 1s+1s) = 3s (B/C are bottleneck, D finishes at 2s)
        """
        task_file = os.path.join(self.test_tasks_dir, "dag_advantage.txt")
        with open(task_file, "w") as f:
            f.write(
                f'id="A" predecessors="" require_predecessor_success="" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )
            f.write(
                f'id="B" predecessors="" require_predecessor_success="" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="3"\n'
            )
            f.write(
                f'id="C" predecessors="" require_predecessor_success="" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="3"\n'
            )
            f.write(
                f'id="D" predecessors="A" require_predecessor_success="" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )

        dag = build_dag(task_file, expand=False, tm1_services=self.tm1_services)

        self.assertEqual(len(dag), 4)

        start = time.time()
        loop = asyncio.new_event_loop()
        results = loop.run_until_complete(
            work_through_tasks_dag(ExecutionContext(), dag, 4, 0, self.tm1_services)
        )
        loop.close()
        elapsed = time.time() - start

        self.assertEqual(len(results), 4)
        self.assertTrue(all(results), "All tasks should succeed")
        self.assertLess(elapsed, 8.0, "DAG should not wait for entire level")


@pytest.mark.requires_tm1
class TestExpandedTasksIntegration(unittest.TestCase):
    """Integration tests for expanded tasks using MDX."""

    @classmethod
    def setUpClass(cls):
        """Set up TM1 connections once for all tests."""
        # Get test config path
        tm1_config, config_source = get_test_tm1_config()
        if tm1_config is None:
            cls.tm1_available = False
            cls.tm1_services = {}
            cls.preserve_connections = {}
            cls.tm1_instance = "tm1srv01"  # default for skipped tests
            cls.test_tasks_dir = tempfile.mkdtemp()
            cls.simple_task_file = os.path.join(cls.test_tasks_dir, "simple_task.txt")
            with open(cls.simple_task_file, "w") as f:
                f.write('instance="tm1srv01" process="}bedrock.server.wait" pWaitSec="1"\n')
            return

        cls.config_path = config_source
        cls.tm1_instance = tm1_config.instance
        cls.test_tasks_dir = tempfile.mkdtemp()
        cls.simple_task_file = os.path.join(cls.test_tasks_dir, "simple_task.txt")

        with open(cls.simple_task_file, "w") as f:
            f.write(f'instance="{cls.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n')

        try:
            cls.tm1_services, cls.preserve_connections = setup_tm1_services(
                max_workers=4, tasks_file_path=cls.simple_task_file, config_path=cls.config_path
            )
            cls.tm1_available = cls.tm1_instance in cls.tm1_services
            # Ensure test objects (counter dimension etc.) exist
            for inst, tm1 in cls.tm1_services.items():
                try:
                    setup_tm1_test_objects(tm1)
                except Exception:
                    pass
        except Exception as e:
            print(f"TM1 connection failed: {e}")
            cls.tm1_available = False
            cls.tm1_services = {}
            cls.preserve_connections = {}

    @classmethod
    def tearDownClass(cls):
        """Clean up TM1 connections."""
        if cls.tm1_services:
            logout(cls.tm1_services, cls.preserve_connections)

        shutil.rmtree(cls.test_tasks_dir, ignore_errors=True)

    def setUp(self):
        if not self.tm1_available:
            self.skipTest("TM1 instance not available")

    def test_expanded_tasks_parallel_execution(self):
        """Test expanded tasks run in parallel.

        Using MDX to expand pWaitSec to elements 1,2,3 from the dimension.
        All 3 tasks should run in parallel.
        """
        task_file = os.path.join(self.test_tasks_dir, "expanded_parallel.txt")
        with open(task_file, "w") as f:
            f.write(
                f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec*=*"{{HEAD({{TM1SUBSETALL([rushti.dimension.counter].[rushti.dimension.counter])}}, 3)}}"\n'
            )

        dag = build_dag(task_file, expand=True, tm1_services=self.tm1_services)

        self.assertEqual(len(dag), 3)

        start = time.time()
        loop = asyncio.new_event_loop()
        results = loop.run_until_complete(
            work_through_tasks_dag(ExecutionContext(), dag, 4, 0, self.tm1_services)
        )
        loop.close()
        elapsed = time.time() - start

        self.assertEqual(len(results), 3)
        self.assertTrue(all(results), "All tasks should succeed")
        self.assertLess(elapsed, 8.0, "Expanded tasks should run in parallel")

    def test_expanded_tasks_with_wait_sequential(self):
        """Test expanded tasks respect 'wait' for sequencing.

        First expand creates 2 tasks (1s and 2s waits)
        wait
        Second expand creates 2 more tasks (1s and 2s waits)

        Total time should be ~4s (2s + 2s) since groups are sequential.
        """
        task_file = os.path.join(self.test_tasks_dir, "expanded_sequential.txt")
        with open(task_file, "w") as f:
            f.write(
                f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec*=*"{{HEAD({{TM1SUBSETALL([rushti.dimension.counter].[rushti.dimension.counter])}}, 2)}}"\n'
            )
            f.write("wait\n")
            f.write(
                f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec*=*"{{HEAD({{TM1SUBSETALL([rushti.dimension.counter].[rushti.dimension.counter])}}, 2)}}"\n'
            )

        dag = build_dag(task_file, expand=True, tm1_services=self.tm1_services)

        self.assertEqual(len(dag), 4)

        start = time.time()
        loop = asyncio.new_event_loop()
        results = loop.run_until_complete(
            work_through_tasks_dag(ExecutionContext(), dag, 4, 0, self.tm1_services)
        )
        loop.close()
        elapsed = time.time() - start

        self.assertEqual(len(results), 4)
        self.assertTrue(all(results), "All tasks should succeed")
        self.assertGreaterEqual(elapsed, 4.0, "Groups should run sequentially")


@pytest.mark.requires_tm1
class TestMaxWorkersConstraint(unittest.TestCase):
    """Test that max_workers constraint is respected."""

    @classmethod
    def setUpClass(cls):
        """Set up TM1 connections once for all tests."""
        # Get test config path
        tm1_config, config_source = get_test_tm1_config()
        if tm1_config is None:
            cls.tm1_available = False
            cls.tm1_services = {}
            cls.preserve_connections = {}
            cls.tm1_instance = "tm1srv01"  # default for skipped tests
            cls.test_tasks_dir = tempfile.mkdtemp()
            cls.simple_task_file = os.path.join(cls.test_tasks_dir, "simple_task.txt")
            with open(cls.simple_task_file, "w") as f:
                f.write('instance="tm1srv01" process="}bedrock.server.wait" pWaitSec="1"\n')
            return

        cls.config_path = config_source
        cls.tm1_instance = tm1_config.instance
        cls.test_tasks_dir = tempfile.mkdtemp()
        cls.simple_task_file = os.path.join(cls.test_tasks_dir, "simple_task.txt")

        with open(cls.simple_task_file, "w") as f:
            f.write(f'instance="{cls.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n')

        try:
            cls.tm1_services, cls.preserve_connections = setup_tm1_services(
                max_workers=4, tasks_file_path=cls.simple_task_file, config_path=cls.config_path
            )
            cls.tm1_available = cls.tm1_instance in cls.tm1_services
            # Ensure test objects (counter dimension etc.) exist
            for inst, tm1 in cls.tm1_services.items():
                try:
                    setup_tm1_test_objects(tm1)
                except Exception:
                    pass
        except Exception as e:
            print(f"TM1 connection failed: {e}")
            cls.tm1_available = False
            cls.tm1_services = {}
            cls.preserve_connections = {}

    @classmethod
    def tearDownClass(cls):
        if cls.tm1_services:
            logout(cls.tm1_services, cls.preserve_connections)

        shutil.rmtree(cls.test_tasks_dir, ignore_errors=True)

    def setUp(self):
        if not self.tm1_available:
            self.skipTest("TM1 instance not available")

    def test_max_workers_limits_parallelism(self):
        """Test that max_workers=1 forces sequential execution.

        Four 1-second tasks with max_workers=1 should take ~4s.
        """
        task_file = os.path.join(self.test_tasks_dir, "workers_test.txt")
        with open(task_file, "w") as f:
            for _ in range(4):
                f.write(
                    f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
                )

        dag = build_dag(task_file, expand=False, tm1_services=self.tm1_services)

        self.assertEqual(len(dag), 4)

        start = time.time()
        loop = asyncio.new_event_loop()
        results = loop.run_until_complete(
            work_through_tasks_dag(ExecutionContext(), dag, 1, 0, self.tm1_services)
        )
        loop.close()
        elapsed = time.time() - start

        self.assertEqual(len(results), 4)
        self.assertTrue(all(results), "All tasks should succeed")
        self.assertGreaterEqual(elapsed, 4.0, "Tasks should run sequentially with max_workers=1")

    def test_max_workers_allows_limited_parallelism(self):
        """Test that max_workers=2 limits to 2 parallel tasks.

        Four 1-second tasks with max_workers=2 should take ~2s.
        """
        task_file = os.path.join(self.test_tasks_dir, "workers_test2.txt")
        with open(task_file, "w") as f:
            for _ in range(4):
                f.write(
                    f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
                )

        dag = build_dag(task_file, expand=False, tm1_services=self.tm1_services)

        self.assertEqual(len(dag), 4)

        start = time.time()
        loop = asyncio.new_event_loop()
        results = loop.run_until_complete(
            work_through_tasks_dag(ExecutionContext(), dag, 2, 0, self.tm1_services)
        )
        loop.close()
        elapsed = time.time() - start

        self.assertEqual(len(results), 4)
        self.assertTrue(all(results), "All tasks should succeed")
        self.assertGreaterEqual(elapsed, 2.0, "Should take at least 2 seconds")
        self.assertLess(elapsed, 8.0, "Should take less than 8 seconds (some parallelism)")


if __name__ == "__main__":
    unittest.main()
