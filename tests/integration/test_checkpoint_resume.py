"""Integration tests for checkpoint and resume functionality.

These tests require:
- TM1 instance configured in tests/config.ini or RUSHTI_TEST_CONFIG environment variable
- }bedrock.server.wait process with pWaitSec parameter

Run with: pytest tests/integration/test_checkpoint_resume.py -v -m requires_tm1
"""

import asyncio
import json
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

from rushti.execution import (  # noqa: E402
    setup_tm1_services,
    work_through_tasks_dag,
    logout,
    ExecutionContext,
)
from rushti.parsing import build_dag  # noqa: E402
from rushti.dag import TaskStatus  # noqa: E402

# Import test config utilities from conftest
from conftest import get_test_tm1_config  # noqa: E402


@pytest.mark.requires_tm1
class TestCheckpointIntegration(unittest.TestCase):
    """Integration tests for checkpoint and resume functionality."""

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
            cls.checkpoint_dir = tempfile.mkdtemp()
            cls.simple_task_file = os.path.join(cls.test_tasks_dir, "simple_task.txt")
            with open(cls.simple_task_file, "w") as f:
                f.write('instance="tm1srv01" process="}bedrock.server.wait" pWaitSec="1"\n')
            return

        cls.config_path = config_source
        cls.tm1_instance = tm1_config.instance
        cls.test_tasks_dir = tempfile.mkdtemp()
        cls.checkpoint_dir = tempfile.mkdtemp()
        cls.simple_task_file = os.path.join(cls.test_tasks_dir, "simple_task.txt")

        with open(cls.simple_task_file, "w") as f:
            f.write(f'instance="{cls.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n')

        try:
            cls.tm1_services, cls.preserve_connections = setup_tm1_services(
                max_workers=4, tasks_file_path=cls.simple_task_file, config_path=cls.config_path
            )
            cls.tm1_available = cls.tm1_instance in cls.tm1_services
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
        shutil.rmtree(cls.checkpoint_dir, ignore_errors=True)

    def setUp(self):
        if not self.tm1_available:
            self.skipTest("TM1 instance not available")

    def test_checkpoint_manager_saves_during_execution(self):
        """Test that CheckpointManager saves checkpoints during task execution."""
        from rushti.checkpoint import CheckpointManager, load_checkpoint, get_checkpoint_path

        task_file = os.path.join(self.test_tasks_dir, "checkpoint_test.txt")
        with open(task_file, "w") as f:
            f.write(
                f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )
            f.write(
                f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )

        dag = build_dag(task_file, expand=False, tm1_services=self.tm1_services)

        task_ids = list(dag._tasks.keys())
        manager = CheckpointManager(
            checkpoint_dir=self.checkpoint_dir,
            taskfile_path=task_file,
            workflow="checkpoint-test",
            task_ids=task_ids,
            checkpoint_interval=5,
            enabled=True,
        )

        loop = asyncio.new_event_loop()
        results = loop.run_until_complete(
            work_through_tasks_dag(
                ExecutionContext(), dag, 4, 0, self.tm1_services, checkpoint_manager=manager
            )
        )
        loop.close()

        self.assertEqual(len(results), 2)
        self.assertTrue(all(results), "All tasks should succeed")

        checkpoint_path = get_checkpoint_path(self.checkpoint_dir, "checkpoint-test")
        self.assertTrue(os.path.exists(checkpoint_path), "Checkpoint file should exist")

        checkpoint = load_checkpoint(checkpoint_path)
        self.assertEqual(len(checkpoint.completed_tasks), 2)
        self.assertEqual(len(checkpoint.pending_tasks), 0)
        self.assertEqual(checkpoint.success_count, 2)

        manager.cleanup(success=True)
        self.assertFalse(os.path.exists(checkpoint_path), "Checkpoint should be deleted on success")

    def test_checkpoint_retained_on_failure(self):
        """Test that checkpoint is retained when execution fails."""
        from rushti.checkpoint import CheckpointManager, get_checkpoint_path

        task_file = os.path.join(self.test_tasks_dir, "failure_test.txt")
        with open(task_file, "w") as f:
            f.write(
                f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )

        dag = build_dag(task_file, expand=False, tm1_services=self.tm1_services)

        task_ids = list(dag._tasks.keys())
        manager = CheckpointManager(
            checkpoint_dir=self.checkpoint_dir,
            taskfile_path=task_file,
            workflow="failure-test",
            task_ids=task_ids,
            checkpoint_interval=5,
            enabled=True,
        )

        loop = asyncio.new_event_loop()
        loop.run_until_complete(
            work_through_tasks_dag(
                ExecutionContext(), dag, 4, 0, self.tm1_services, checkpoint_manager=manager
            )
        )
        loop.close()

        manager.cleanup(success=False)

        checkpoint_path = get_checkpoint_path(self.checkpoint_dir, "failure-test")
        self.assertTrue(os.path.exists(checkpoint_path), "Checkpoint should be retained on failure")

        if os.path.exists(checkpoint_path):
            os.remove(checkpoint_path)

    def test_checkpoint_tracks_task_progress(self):
        """Test that checkpoint accurately tracks task completion progress."""
        from rushti.checkpoint import CheckpointManager, load_checkpoint, get_checkpoint_path

        task_file = os.path.join(self.test_tasks_dir, "progress_test.txt")
        with open(task_file, "w") as f:
            f.write(
                f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )
            f.write(
                f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="2"\n'
            )
            f.write(
                f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )

        dag = build_dag(task_file, expand=False, tm1_services=self.tm1_services)

        task_ids = list(dag._tasks.keys())
        manager = CheckpointManager(
            checkpoint_dir=self.checkpoint_dir,
            taskfile_path=task_file,
            workflow="progress-test",
            task_ids=task_ids,
            checkpoint_interval=5,
            enabled=True,
        )

        loop = asyncio.new_event_loop()
        results = loop.run_until_complete(
            work_through_tasks_dag(
                ExecutionContext(), dag, 4, 0, self.tm1_services, checkpoint_manager=manager
            )
        )
        loop.close()

        self.assertEqual(len(results), 3)
        self.assertTrue(all(results))

        checkpoint_path = get_checkpoint_path(self.checkpoint_dir, "progress-test")
        checkpoint = load_checkpoint(checkpoint_path)

        self.assertEqual(checkpoint.total_tasks, 3)
        self.assertEqual(len(checkpoint.completed_tasks), 3)
        self.assertAlmostEqual(checkpoint.progress_percentage, 100.0)

        for task_id, result in checkpoint.completed_tasks.items():
            self.assertTrue(result.success)
            self.assertGreater(result.duration_seconds, 0)

        manager.cleanup(success=True)

    def test_checkpoint_with_sequential_execution(self):
        """Test checkpoint works correctly with sequential task execution (wait keyword)."""
        from rushti.checkpoint import CheckpointManager, load_checkpoint, get_checkpoint_path

        task_file = os.path.join(self.test_tasks_dir, "sequential_checkpoint.txt")
        with open(task_file, "w") as f:
            f.write(
                f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )
            f.write("wait\n")
            f.write(
                f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )

        dag = build_dag(task_file, expand=False, tm1_services=self.tm1_services)

        task_ids = list(dag._tasks.keys())
        manager = CheckpointManager(
            checkpoint_dir=self.checkpoint_dir,
            taskfile_path=task_file,
            workflow="sequential-checkpoint",
            task_ids=task_ids,
            checkpoint_interval=5,
            enabled=True,
        )

        start = time.time()
        loop = asyncio.new_event_loop()
        results = loop.run_until_complete(
            work_through_tasks_dag(
                ExecutionContext(), dag, 4, 0, self.tm1_services, checkpoint_manager=manager
            )
        )
        loop.close()
        elapsed = time.time() - start

        self.assertGreaterEqual(elapsed, 2.0)
        self.assertEqual(len(results), 2)
        self.assertTrue(all(results))

        checkpoint_path = get_checkpoint_path(self.checkpoint_dir, "sequential-checkpoint")
        checkpoint = load_checkpoint(checkpoint_path)
        self.assertEqual(len(checkpoint.completed_tasks), 2)

        manager.cleanup(success=True)

    def test_checkpoint_with_opt_mode_dependencies(self):
        """Test checkpoint works correctly with optimized mode task dependencies."""
        from rushti.checkpoint import CheckpointManager, load_checkpoint, get_checkpoint_path

        task_file = os.path.join(self.test_tasks_dir, "opt_checkpoint.txt")
        with open(task_file, "w") as f:
            f.write(
                f'id="1" predecessors="" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )
            f.write(
                f'id="2" predecessors="1" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )
            f.write(
                f'id="3" predecessors="2" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )

        dag = build_dag(task_file, expand=False, tm1_services=self.tm1_services)

        task_ids = list(dag._tasks.keys())
        manager = CheckpointManager(
            checkpoint_dir=self.checkpoint_dir,
            taskfile_path=task_file,
            workflow="opt-checkpoint",
            task_ids=task_ids,
            checkpoint_interval=5,
            enabled=True,
        )

        start = time.time()
        loop = asyncio.new_event_loop()
        results = loop.run_until_complete(
            work_through_tasks_dag(
                ExecutionContext(), dag, 4, 0, self.tm1_services, checkpoint_manager=manager
            )
        )
        loop.close()
        elapsed = time.time() - start

        self.assertGreaterEqual(elapsed, 3.0)
        self.assertEqual(len(results), 3)
        self.assertTrue(all(results))

        checkpoint_path = get_checkpoint_path(self.checkpoint_dir, "opt-checkpoint")
        checkpoint = load_checkpoint(checkpoint_path)

        self.assertEqual(len(checkpoint.completed_tasks), 3)
        self.assertIn("1", checkpoint.completed_tasks)
        self.assertIn("2", checkpoint.completed_tasks)
        self.assertIn("3", checkpoint.completed_tasks)

        manager.cleanup(success=True)

    def test_checkpoint_disabled_does_not_create_file(self):
        """Test that checkpoint disabled does not create checkpoint file."""
        from rushti.checkpoint import CheckpointManager, get_checkpoint_path

        task_file = os.path.join(self.test_tasks_dir, "disabled_test.txt")
        with open(task_file, "w") as f:
            f.write(
                f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )

        dag = build_dag(task_file, expand=False, tm1_services=self.tm1_services)

        task_ids = list(dag._tasks.keys())
        manager = CheckpointManager(
            checkpoint_dir=self.checkpoint_dir,
            taskfile_path=task_file,
            workflow="disabled-test",
            task_ids=task_ids,
            checkpoint_interval=5,
            enabled=False,
        )

        loop = asyncio.new_event_loop()
        results = loop.run_until_complete(
            work_through_tasks_dag(
                ExecutionContext(), dag, 4, 0, self.tm1_services, checkpoint_manager=manager
            )
        )
        loop.close()

        self.assertTrue(all(results))

        checkpoint_path = get_checkpoint_path(self.checkpoint_dir, "disabled-test")
        self.assertFalse(
            os.path.exists(checkpoint_path), "Checkpoint should not exist when disabled"
        )


@pytest.mark.requires_tm1
class TestResumeIntegration(unittest.TestCase):
    """Integration tests for resume functionality."""

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
            cls.checkpoint_dir = tempfile.mkdtemp()
            cls.simple_task_file = os.path.join(cls.test_tasks_dir, "simple_task.txt")
            with open(cls.simple_task_file, "w") as f:
                f.write('instance="tm1srv01" process="}bedrock.server.wait" pWaitSec="1"\n')
            return

        cls.config_path = config_source
        cls.tm1_instance = tm1_config.instance
        cls.test_tasks_dir = tempfile.mkdtemp()
        cls.checkpoint_dir = tempfile.mkdtemp()
        cls.simple_task_file = os.path.join(cls.test_tasks_dir, "simple_task.txt")

        with open(cls.simple_task_file, "w") as f:
            f.write(f'instance="{cls.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n')

        try:
            cls.tm1_services, cls.preserve_connections = setup_tm1_services(
                max_workers=4, tasks_file_path=cls.simple_task_file, config_path=cls.config_path
            )
            cls.tm1_available = cls.tm1_instance in cls.tm1_services
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
        shutil.rmtree(cls.checkpoint_dir, ignore_errors=True)

    def setUp(self):
        if not self.tm1_available:
            self.skipTest("TM1 instance not available")

    def test_resume_skips_completed_tasks(self):
        """Test that resume correctly skips tasks marked as completed in checkpoint."""
        from rushti.checkpoint import Checkpoint, save_checkpoint, get_checkpoint_path

        task_file = os.path.join(self.test_tasks_dir, "resume_skip_test.txt")
        with open(task_file, "w") as f:
            f.write(
                f'id="1" predecessors="" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )
            f.write(
                f'id="2" predecessors="1" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )
            f.write(
                f'id="3" predecessors="2" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )

        checkpoint = Checkpoint.create(
            taskfile_path=task_file,
            workflow="resume-skip-test",
            task_ids=["1", "2", "3"],
        )
        checkpoint.mark_completed("1", success=True, duration_seconds=1.0)
        checkpoint.mark_completed("2", success=True, duration_seconds=1.0)

        checkpoint_path = get_checkpoint_path(self.checkpoint_dir, "resume-skip-test")
        save_checkpoint(checkpoint, checkpoint_path)

        dag = build_dag(task_file, expand=False, tm1_services=self.tm1_services)

        for task_id in ["1", "2"]:
            for task in dag._tasks.get(task_id, []):
                dag._completed_instances.add(id(task))
            dag._status[task_id] = TaskStatus.COMPLETED
            dag._results[task_id] = True

        start = time.time()
        loop = asyncio.new_event_loop()
        results = loop.run_until_complete(
            work_through_tasks_dag(ExecutionContext(), dag, 4, 0, self.tm1_services)
        )
        loop.close()
        elapsed = time.time() - start

        self.assertLess(elapsed, 8.0, "Should only execute task 3")
        self.assertEqual(len(results), 1)
        self.assertTrue(all(results))

        if os.path.exists(checkpoint_path):
            os.remove(checkpoint_path)

    def test_checkpoint_validation_detects_modified_file(self):
        """Test that checkpoint validation detects when taskfile has been modified."""
        from rushti.checkpoint import (
            Checkpoint,
            save_checkpoint,
            load_checkpoint,
            get_checkpoint_path,
        )

        task_file = os.path.join(self.test_tasks_dir, "modified_test.txt")
        with open(task_file, "w") as f:
            f.write(
                f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )

        checkpoint = Checkpoint.create(
            taskfile_path=task_file,
            workflow="modified-test",
            task_ids=["1"],
        )
        checkpoint_path = get_checkpoint_path(self.checkpoint_dir, "modified-test")
        save_checkpoint(checkpoint, checkpoint_path)

        with open(task_file, "a") as f:
            f.write(
                f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="2"\n'
            )

        loaded_checkpoint = load_checkpoint(checkpoint_path)
        is_valid, warnings = loaded_checkpoint.validate_against_taskfile(task_file, strict=True)

        self.assertFalse(is_valid, "Should detect modified file")
        self.assertTrue(any("modified" in w.lower() for w in warnings))

        if os.path.exists(checkpoint_path):
            os.remove(checkpoint_path)

    def test_resume_with_safe_retry_tasks(self):
        """Test that tasks with safe_retry=true are automatically retried on resume."""
        from rushti.checkpoint import Checkpoint

        task_file = os.path.join(self.test_tasks_dir, "safe_retry_test.json")
        task_data = {
            "version": "2.0",
            "metadata": {"workflow": "safe-retry-test"},
            "tasks": [
                {
                    "id": "1",
                    "instance": self.tm1_instance,
                    "process": "}bedrock.server.wait",
                    "parameters": {"pWaitSec": "1"},
                    "safe_retry": True,
                },
                {
                    "id": "2",
                    "instance": self.tm1_instance,
                    "process": "}bedrock.server.wait",
                    "parameters": {"pWaitSec": "1"},
                    "predecessors": ["1"],
                    "safe_retry": False,
                },
            ],
        }
        with open(task_file, "w") as f:
            json.dump(task_data, f)

        checkpoint = Checkpoint.create(
            taskfile_path=task_file,
            workflow="safe-retry-test",
            task_ids=["1", "2"],
        )
        checkpoint.mark_running("1")

        task_safe_retry_map = {"1": True, "2": False}
        tasks_to_run, tasks_requiring_decision, error = checkpoint.get_tasks_for_resume(
            task_safe_retry_map
        )

        self.assertIn("1", tasks_to_run)
        self.assertIn("2", tasks_to_run)
        self.assertEqual(len(tasks_requiring_decision), 0)
        self.assertIsNone(error)

        if os.path.exists(task_file):
            os.remove(task_file)

    def test_resume_blocks_non_safe_retry_in_progress(self):
        """Test that non-safe-retry in-progress tasks block automatic resume."""
        from rushti.checkpoint import Checkpoint

        task_file = os.path.join(self.test_tasks_dir, "non_safe_test.txt")
        with open(task_file, "w") as f:
            f.write(
                f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )
            f.write(
                f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )

        checkpoint = Checkpoint.create(
            taskfile_path=task_file,
            workflow="non-safe-test",
            task_ids=["1", "2"],
        )
        checkpoint.mark_running("1")

        task_safe_retry_map = {"1": False, "2": False}
        tasks_to_run, tasks_requiring_decision, error = checkpoint.get_tasks_for_resume(
            task_safe_retry_map
        )

        self.assertIn("1", tasks_requiring_decision)
        self.assertIsNotNone(error)
        self.assertIn("safe_retry=false", error)

    def test_resume_from_specific_task(self):
        """Test resuming from a specific task ID."""
        from rushti.checkpoint import Checkpoint

        task_file = os.path.join(self.test_tasks_dir, "resume_from_test.txt")
        with open(task_file, "w") as f:
            f.write(
                f'id="1" predecessors="" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )
            f.write(
                f'id="2" predecessors="1" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )
            f.write(
                f'id="3" predecessors="2" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )
            f.write(
                f'id="4" predecessors="3" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )

        checkpoint = Checkpoint.create(
            taskfile_path=task_file,
            workflow="resume-from-test",
            task_ids=["1", "2", "3", "4"],
        )

        checkpoint.mark_completed("1", success=True, duration_seconds=1.0)
        checkpoint.mark_completed("2", success=True, duration_seconds=1.0)

        tasks_to_run = checkpoint.get_resume_from_task("3", ["1", "2", "3", "4"])

        self.assertIn("3", tasks_to_run)
        self.assertIn("4", tasks_to_run)
        self.assertNotIn("1", tasks_to_run)
        self.assertNotIn("2", tasks_to_run)

    def test_resume_from_invalid_task_raises_error(self):
        """Test that resume from non-existent task raises ValueError."""
        from rushti.checkpoint import Checkpoint

        task_file = os.path.join(self.test_tasks_dir, "invalid_resume_test.txt")
        with open(task_file, "w") as f:
            f.write(
                f'instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )

        checkpoint = Checkpoint.create(
            taskfile_path=task_file,
            workflow="invalid-resume-test",
            task_ids=["1"],
        )

        with self.assertRaises(ValueError) as context:
            checkpoint.get_resume_from_task("nonexistent", ["1"])

        self.assertIn("not found", str(context.exception))


@pytest.mark.requires_tm1
class TestResumeSubcommandIntegration(unittest.TestCase):
    """Integration tests for the 'rushti resume' subcommand.

    These tests verify:
    - Resume subcommand correctly discovers checkpoints
    - Resume subcommand correctly loads and validates checkpoints
    - Resume subcommand correctly marks completed tasks in TASK_EXECUTION_RESULTS
    - Resume subcommand correctly skips completed tasks
    """

    @classmethod
    def setUpClass(cls):
        """Set up TM1 connections once for all tests."""
        tm1_config, config_source = get_test_tm1_config()
        if tm1_config is None:
            cls.tm1_available = False
            cls.tm1_services = {}
            cls.preserve_connections = {}
            cls.tm1_instance = "tm1srv01"
            cls.test_tasks_dir = tempfile.mkdtemp()
            cls.checkpoint_dir = tempfile.mkdtemp()
            return

        cls.config_path = config_source
        cls.tm1_instance = tm1_config.instance
        cls.test_tasks_dir = tempfile.mkdtemp()
        cls.checkpoint_dir = tempfile.mkdtemp()

        try:
            cls.simple_task_file = os.path.join(cls.test_tasks_dir, "simple_task.txt")
            with open(cls.simple_task_file, "w") as f:
                f.write(
                    f'instance="{cls.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
                )

            cls.tm1_services, cls.preserve_connections = setup_tm1_services(
                max_workers=4, tasks_file_path=cls.simple_task_file, config_path=cls.config_path
            )
            cls.tm1_available = cls.tm1_instance in cls.tm1_services
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
        shutil.rmtree(cls.checkpoint_dir, ignore_errors=True)

    def setUp(self):
        if not self.tm1_available:
            self.skipTest("TM1 instance not available")

    def test_task_execution_results_populated_on_resume(self):
        """Test that task_execution_results is populated when resuming from checkpoint."""
        from rushti.checkpoint import Checkpoint, save_checkpoint, get_checkpoint_path
        from rushti.execution import ExecutionContext

        # Create task file with 3 sequential tasks
        task_file = os.path.join(self.test_tasks_dir, "results_test.txt")
        with open(task_file, "w") as f:
            f.write(
                f'id="1" predecessors="" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )
            f.write(
                f'id="2" predecessors="1" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )
            f.write(
                f'id="3" predecessors="2" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )

        # Create checkpoint with tasks 1 and 2 completed
        checkpoint = Checkpoint.create(
            taskfile_path=task_file,
            workflow="results-test",
            task_ids=["1", "2", "3"],
        )
        checkpoint.mark_completed("1", success=True, duration_seconds=1.0)
        checkpoint.mark_completed("2", success=True, duration_seconds=1.0)

        checkpoint_path = get_checkpoint_path(self.checkpoint_dir, "results-test")
        save_checkpoint(checkpoint, checkpoint_path)

        # Create ExecutionContext for the test
        ctx = ExecutionContext()

        # Build DAG
        dag = build_dag(task_file, expand=False, tm1_services=self.tm1_services)

        # Simulate what main() does during resume: populate ctx.task_execution_results
        completed_task_ids = set(checkpoint.completed_tasks.keys())
        for task_id in completed_task_ids:
            result = checkpoint.completed_tasks.get(task_id)
            if result:
                dag.mark_complete(task_id, result.success)
                ctx.task_execution_results[task_id] = result.success

        # Verify task_execution_results was populated
        self.assertIn("1", ctx.task_execution_results)
        self.assertIn("2", ctx.task_execution_results)
        self.assertTrue(ctx.task_execution_results["1"])
        self.assertTrue(ctx.task_execution_results["2"])
        self.assertNotIn("3", ctx.task_execution_results)

        # Clean up
        if os.path.exists(checkpoint_path):
            os.remove(checkpoint_path)

    def test_resume_only_executes_pending_tasks(self):
        """Test that resume only executes tasks not marked as completed."""
        from rushti.checkpoint import Checkpoint, save_checkpoint, get_checkpoint_path
        from rushti.execution import ExecutionContext

        task_file = os.path.join(self.test_tasks_dir, "pending_only_test.txt")
        with open(task_file, "w") as f:
            f.write(
                f'id="1" predecessors="" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )
            f.write(
                f'id="2" predecessors="1" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )
            f.write(
                f'id="3" predecessors="2" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )

        # Create checkpoint with task 1 and 2 completed
        initial_checkpoint = Checkpoint.create(
            taskfile_path=task_file,
            workflow="pending-only-test",
            task_ids=["1", "2", "3"],
        )
        initial_checkpoint.mark_completed("1", success=True, duration_seconds=1.0)
        initial_checkpoint.mark_completed("2", success=True, duration_seconds=1.0)

        checkpoint_path = get_checkpoint_path(self.checkpoint_dir, "pending-only-test")
        save_checkpoint(initial_checkpoint, checkpoint_path)

        # Build DAG and mark completed tasks
        dag = build_dag(task_file, expand=False, tm1_services=self.tm1_services)

        # Create ExecutionContext and simulate resume: mark completed tasks
        ctx = ExecutionContext()
        for task_id in ["1", "2"]:
            for task in dag._tasks.get(task_id, []):
                dag._completed_instances.add(id(task))
            dag._status[task_id] = TaskStatus.COMPLETED
            dag._results[task_id] = True
            ctx.task_execution_results[task_id] = True

        # Execute - should only run task 3
        start = time.time()
        loop = asyncio.new_event_loop()
        results = loop.run_until_complete(work_through_tasks_dag(ctx, dag, 4, 0, self.tm1_services))
        loop.close()
        elapsed = time.time() - start

        # Should complete quickly (only ~1 second for task 3)
        self.assertLess(elapsed, 2.0, "Should only execute task 3 (not 1 and 2)")
        self.assertEqual(len(results), 1, "Should return 1 result for task 3")
        self.assertTrue(all(results))

        # Clean up
        if os.path.exists(checkpoint_path):
            os.remove(checkpoint_path)

    def test_resume_respects_predecessor_success_status(self):
        """Test that resume correctly handles predecessor success status."""
        from rushti.checkpoint import Checkpoint, save_checkpoint, get_checkpoint_path
        from rushti.execution import ExecutionContext

        task_file = os.path.join(self.test_tasks_dir, "predecessor_test.txt")
        with open(task_file, "w") as f:
            f.write(
                f'id="1" predecessors="" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )
            f.write(
                f'id="2" predecessors="1" instance="{self.tm1_instance}" process="}}bedrock.server.wait" pWaitSec="1"\n'
            )

        # Create checkpoint with task 1 completed successfully
        checkpoint = Checkpoint.create(
            taskfile_path=task_file,
            workflow="predecessor-test",
            task_ids=["1", "2"],
        )
        checkpoint.mark_completed("1", success=True, duration_seconds=1.0)

        checkpoint_path = get_checkpoint_path(self.checkpoint_dir, "predecessor-test")
        save_checkpoint(checkpoint, checkpoint_path)

        # Build DAG
        dag = build_dag(task_file, expand=False, tm1_services=self.tm1_services)

        # Create ExecutionContext and simulate resume
        ctx = ExecutionContext()
        ctx.task_execution_results["1"] = True
        for task in dag._tasks.get("1", []):
            dag._completed_instances.add(id(task))
        dag._status["1"] = TaskStatus.COMPLETED
        dag._results["1"] = True

        # Execute - task 2 should run since predecessor succeeded
        loop = asyncio.new_event_loop()
        results = loop.run_until_complete(work_through_tasks_dag(ctx, dag, 4, 0, self.tm1_services))
        loop.close()

        self.assertEqual(len(results), 1)
        self.assertTrue(results[0], "Task 2 should succeed since predecessor completed")

        # Clean up
        if os.path.exists(checkpoint_path):
            os.remove(checkpoint_path)


if __name__ == "__main__":
    unittest.main()
