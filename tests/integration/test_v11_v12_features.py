"""Integration tests for RushTI features on v11 and v12.

Tests stats DB, dashboard generation, optimization estimates, checkpoint/resume,
exclusive mode, taskfile archiving, and db admin commands.

Run with: pytest tests/integration/test_v11_v12_features.py -v -m requires_tm1
"""

import asyncio
import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timedelta

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
from rushti.stats import StatsDatabase
from rushti.db_admin import list_workflows, clear_workflow
from conftest import get_all_test_tm1_configs, get_test_tm1_names
from tm1_setup import setup_tm1_test_objects

RESOURCES_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "resources", "integration"
)


class _BaseFeatureTest(unittest.TestCase):
    """Base class for feature tests."""

    INSTANCE = None

    @classmethod
    def setUpClass(cls):
        configs, config_source = get_all_test_tm1_configs()
        if cls.INSTANCE and cls.INSTANCE not in configs:
            cls._tm1_available = False
            cls._config_path = None
            return

        cls._config_path = config_source
        cls._test_dir = tempfile.mkdtemp()

        task_content = f'instance="{cls.INSTANCE}" process="}}bedrock.server.wait" pWaitSec="1"\n'
        bootstrap_file = os.path.join(cls._test_dir, "bootstrap.txt")
        with open(bootstrap_file, "w") as f:
            f.write(task_content)

        try:
            cls._tm1_services, cls._preserve_connections = setup_tm1_services(
                max_workers=4,
                tasks_file_path=bootstrap_file,
                config_path=cls._config_path,
            )
            tm1_names = get_test_tm1_names()
            for inst, tm1 in cls._tm1_services.items():
                setup_tm1_test_objects(tm1, **tm1_names)
            cls._tm1_available = cls.INSTANCE in cls._tm1_services
        except Exception as e:
            print(f"TM1 setup failed: {e}")
            cls._tm1_available = False
            cls._tm1_services = {}
            cls._preserve_connections = {}

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "_tm1_services") and cls._tm1_services:
            logout(cls._tm1_services, cls._preserve_connections or {})
        if hasattr(cls, "_test_dir"):
            shutil.rmtree(cls._test_dir, ignore_errors=True)

    def setUp(self):
        if not self._tm1_available:
            self.skipTest(f"{self.INSTANCE} not available")


@pytest.mark.requires_tm1
@pytest.mark.integration
class TestStatsDBV11(_BaseFeatureTest):
    """Stats DB tests on v11."""

    INSTANCE = "tm1srv01"

    def test_stats_db_populated_after_run(self):
        """Run tasks and verify stats DB is populated."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            stats_db = StatsDatabase(db_path=db_path, enabled=True)
            run_id = "statstest_" + datetime.now().strftime("%Y%m%d_%H%M%S")
            workflow = "test-stats-v11"

            stats_db.start_run(run_id=run_id, workflow=workflow)

            task_file = os.path.join(RESOURCES_DIR, "tasks_v11_opt.txt")
            dag = build_dag(task_file, expand=False, tm1_services=self._tm1_services)
            if isinstance(dag, tuple):
                dag = dag[1]

            loop = asyncio.new_event_loop()
            results = loop.run_until_complete(
                work_through_tasks_dag(ExecutionContext(), dag, 4, 0, self._tm1_services)
            )
            loop.close()

            # Record results in stats DB
            for i, success in enumerate(results, 1):
                stats_db.record_task(
                    run_id=run_id,
                    task_id=str(i),
                    instance=self.INSTANCE,
                    process="}bedrock.server.wait",
                    parameters={"pWaitSec": "1"},
                    success=success,
                    start_time=datetime.now(),
                    end_time=datetime.now() + timedelta(seconds=1),
                    retry_count=0,
                    error_message=None,
                    workflow=workflow,
                )

            stats_db.complete_run(
                run_id=run_id,
                success_count=sum(1 for r in results if r),
                failure_count=sum(1 for r in results if not r),
            )

            # Verify
            stored_results = stats_db.get_run_results(run_id)
            self.assertEqual(len(stored_results), len(results))
            stats_db.close()
        finally:
            os.unlink(db_path)


@pytest.mark.requires_tm1
@pytest.mark.integration
class TestStatsDBV12(_BaseFeatureTest):
    """Stats DB tests on v12."""

    INSTANCE = "tm1srv02"

    def test_stats_db_populated_after_run(self):
        """Run tasks and verify stats DB is populated on v12."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            stats_db = StatsDatabase(db_path=db_path, enabled=True)
            run_id = "statstest_" + datetime.now().strftime("%Y%m%d_%H%M%S")
            workflow = "test-stats-v12"

            stats_db.start_run(run_id=run_id, workflow=workflow)

            task_file = os.path.join(RESOURCES_DIR, "tasks_v12_opt.txt")
            dag = build_dag(task_file, expand=False, tm1_services=self._tm1_services)
            if isinstance(dag, tuple):
                dag = dag[1]

            loop = asyncio.new_event_loop()
            results = loop.run_until_complete(
                work_through_tasks_dag(ExecutionContext(), dag, 4, 0, self._tm1_services)
            )
            loop.close()

            for i, success in enumerate(results, 1):
                stats_db.record_task(
                    run_id=run_id,
                    task_id=str(i),
                    instance=self.INSTANCE,
                    process="}bedrock.server.wait",
                    parameters={"pWaitSec": "1"},
                    success=success,
                    start_time=datetime.now(),
                    end_time=datetime.now() + timedelta(seconds=1),
                    retry_count=0,
                    error_message=None,
                    workflow=workflow,
                )

            stats_db.complete_run(
                run_id=run_id,
                success_count=sum(1 for r in results if r),
                failure_count=sum(1 for r in results if not r),
            )

            stored_results = stats_db.get_run_results(run_id)
            self.assertEqual(len(stored_results), len(results))
            stats_db.close()
        finally:
            os.unlink(db_path)


@pytest.mark.requires_tm1
@pytest.mark.integration
class TestCheckpointV11(_BaseFeatureTest):
    """Checkpoint and resume tests on v11."""

    INSTANCE = "tm1srv01"

    def test_checkpoint_during_execution(self):
        """Checkpoint saves during task execution on v11."""
        from rushti.checkpoint import CheckpointManager, load_checkpoint, get_checkpoint_path

        checkpoint_dir = tempfile.mkdtemp()
        try:
            task_file = os.path.join(RESOURCES_DIR, "tasks_v11_opt.txt")
            dag = build_dag(task_file, expand=False, tm1_services=self._tm1_services)
            if isinstance(dag, tuple):
                dag = dag[1]

            task_ids = list(dag._tasks.keys())
            manager = CheckpointManager(
                checkpoint_dir=checkpoint_dir,
                taskfile_path=task_file,
                workflow="checkpoint-v11",
                task_ids=task_ids,
                checkpoint_interval=5,
                enabled=True,
            )

            loop = asyncio.new_event_loop()
            results = loop.run_until_complete(
                work_through_tasks_dag(
                    ExecutionContext(), dag, 4, 0, self._tm1_services, checkpoint_manager=manager
                )
            )
            loop.close()

            self.assertTrue(all(results))

            checkpoint_path = get_checkpoint_path(checkpoint_dir, "checkpoint-v11")
            self.assertTrue(os.path.exists(checkpoint_path))

            checkpoint = load_checkpoint(checkpoint_path)
            self.assertEqual(len(checkpoint.completed_tasks), len(results))

            manager.cleanup(success=True)
        finally:
            shutil.rmtree(checkpoint_dir, ignore_errors=True)


@pytest.mark.requires_tm1
@pytest.mark.integration
class TestExclusiveModeV11(_BaseFeatureTest):
    """Exclusive mode tests on v11."""

    INSTANCE = "tm1srv01"

    def test_session_context_detection(self):
        """Verify session context detection works on v11."""
        from rushti.exclusive import (
            build_session_context,
            check_active_rushti_sessions,
        )

        context = build_session_context("test-exclusive-v11", exclusive=False)
        sessions = check_active_rushti_sessions(
            self._tm1_services,
            exclude_context=context,
        )
        # Should return a list (likely empty if no other RushTI running)
        self.assertIsInstance(sessions, list)


@pytest.mark.requires_tm1
@pytest.mark.integration
class TestExclusiveModeV12(_BaseFeatureTest):
    """Exclusive mode tests on v12."""

    INSTANCE = "tm1srv02"

    def test_session_context_detection(self):
        """Verify session context detection works on v12."""
        from rushti.exclusive import (
            build_session_context,
            check_active_rushti_sessions,
        )

        context = build_session_context("test-exclusive-v12", exclusive=False)
        sessions = check_active_rushti_sessions(
            self._tm1_services,
            exclude_context=context,
        )
        self.assertIsInstance(sessions, list)


@pytest.mark.requires_tm1
@pytest.mark.integration
class TestTaskfileArchiveV11(_BaseFeatureTest):
    """Taskfile archive tests on v11."""

    INSTANCE = "tm1srv01"

    def test_archive_created_for_json_taskfile(self):
        """Verify archive/{workflow}/{run_id}.json is created."""
        from rushti.taskfile import parse_json_taskfile, archive_taskfile

        task_file = os.path.join(RESOURCES_DIR, "tasks_v11_staged.json")
        taskfile = parse_json_taskfile(task_file)

        archive_dir = tempfile.mkdtemp()
        try:
            # Mock resolve_app_path to use our temp dir
            import unittest.mock

            with unittest.mock.patch("rushti.utils.resolve_app_path", return_value=archive_dir):
                archived_path = archive_taskfile(taskfile, "test-archive-v11", "run_001")

            self.assertTrue(os.path.exists(archived_path))
            self.assertTrue(archived_path.endswith(".json"))

            import json

            with open(archived_path) as f:
                data = json.load(f)
            self.assertIn("tasks", data)
            self.assertEqual(len(data["tasks"]), 4)
        finally:
            shutil.rmtree(archive_dir, ignore_errors=True)


@pytest.mark.requires_tm1
@pytest.mark.integration
class TestDBAdminCommands(_BaseFeatureTest):
    """DB admin command tests."""

    INSTANCE = "tm1srv01"

    def test_list_workflows(self):
        """List workflows from stats DB."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            stats_db = StatsDatabase(db_path=db_path, enabled=True)

            # Create test data with task records (list_workflows queries task_results table)
            for workflow in ["workflow-a", "workflow-b"]:
                run_id = f"run_{workflow}"
                stats_db.start_run(run_id=run_id, workflow=workflow)
                stats_db.record_task(
                    run_id=run_id,
                    task_id="task1",
                    instance="tm1srv01",
                    process="}bedrock.server.wait",
                    parameters={"pWaitSec": "1"},
                    success=True,
                    start_time=datetime.now(),
                    end_time=datetime.now() + timedelta(seconds=1),
                    retry_count=0,
                    error_message=None,
                    workflow=workflow,
                )
                stats_db.complete_run(run_id=run_id, success_count=1, failure_count=0)
            stats_db.close()

            # Use module-level db_admin function
            workflows = list_workflows(db_path=db_path)
            workflow_names = [w["workflow"] for w in workflows]
            self.assertIn("workflow-a", workflow_names)
            self.assertIn("workflow-b", workflow_names)
        finally:
            os.unlink(db_path)

    def test_clear_workflow(self):
        """Clear a workflow from stats DB."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            stats_db = StatsDatabase(db_path=db_path, enabled=True)

            stats_db.start_run(run_id="run1", workflow="to-clear")
            stats_db.record_task(
                run_id="run1",
                task_id="task1",
                instance="tm1srv01",
                process="}bedrock.server.wait",
                parameters={"pWaitSec": "1"},
                success=True,
                start_time=datetime.now(),
                end_time=datetime.now() + timedelta(seconds=1),
                retry_count=0,
                error_message=None,
                workflow="to-clear",
            )
            stats_db.complete_run(run_id="run1", success_count=1, failure_count=0)
            stats_db.close()

            workflows_before = list_workflows(db_path=db_path)
            workflow_names_before = [w["workflow"] for w in workflows_before]
            self.assertIn("to-clear", workflow_names_before)

            clear_workflow("to-clear", db_path=db_path)

            workflows_after = list_workflows(db_path=db_path)
            workflow_names_after = [w["workflow"] for w in workflows_after]
            self.assertNotIn("to-clear", workflow_names_after)
        finally:
            os.unlink(db_path)


if __name__ == "__main__":
    unittest.main()
