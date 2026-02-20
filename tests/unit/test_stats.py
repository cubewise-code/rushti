"""Unit tests for the stats module."""

import os
import tempfile
from datetime import datetime, timedelta
from unittest import TestCase


from rushti.stats import (
    StatsDatabase,
    calculate_task_signature,
    create_stats_database,
)


class TestTaskSignature(TestCase):
    """Tests for task signature calculation."""

    def test_same_inputs_produce_same_signature(self):
        """Same inputs should produce identical signatures."""
        sig1 = calculate_task_signature("inst1", "proc1", {"p1": "v1"})
        sig2 = calculate_task_signature("inst1", "proc1", {"p1": "v1"})
        self.assertEqual(sig1, sig2)

    def test_different_instance_produces_different_signature(self):
        """Different instance should produce different signature."""
        sig1 = calculate_task_signature("inst1", "proc1", {"p1": "v1"})
        sig2 = calculate_task_signature("inst2", "proc1", {"p1": "v1"})
        self.assertNotEqual(sig1, sig2)

    def test_different_process_produces_different_signature(self):
        """Different process should produce different signature."""
        sig1 = calculate_task_signature("inst1", "proc1", {"p1": "v1"})
        sig2 = calculate_task_signature("inst1", "proc2", {"p1": "v1"})
        self.assertNotEqual(sig1, sig2)

    def test_different_params_produce_different_signature(self):
        """Different parameters should produce different signature."""
        sig1 = calculate_task_signature("inst1", "proc1", {"p1": "v1"})
        sig2 = calculate_task_signature("inst1", "proc1", {"p1": "v2"})
        self.assertNotEqual(sig1, sig2)

    def test_param_order_does_not_matter(self):
        """Parameter order should not affect signature (sorted for determinism)."""
        sig1 = calculate_task_signature("inst1", "proc1", {"a": "1", "b": "2"})
        sig2 = calculate_task_signature("inst1", "proc1", {"b": "2", "a": "1"})
        self.assertEqual(sig1, sig2)

    def test_none_params_handled(self):
        """None parameters should produce valid signature."""
        sig = calculate_task_signature("inst1", "proc1", None)
        self.assertEqual(len(sig), 16)  # 16-char hex

    def test_empty_params_handled(self):
        """Empty parameters should produce valid signature."""
        sig = calculate_task_signature("inst1", "proc1", {})
        self.assertEqual(len(sig), 16)

    def test_signature_is_hex(self):
        """Signature should be hexadecimal string."""
        sig = calculate_task_signature("inst1", "proc1", {"p1": "v1"})
        self.assertTrue(all(c in "0123456789abcdef" for c in sig))


class TestStatsDatabaseDisabled(TestCase):
    """Tests for StatsDatabase when disabled."""

    def test_disabled_database_does_not_create_file(self):
        """Disabled database should not create any files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            db = StatsDatabase(db_path=db_path, enabled=False)
            self.assertFalse(os.path.exists(db_path))
            db.close()

    def test_disabled_methods_are_no_ops(self):
        """All methods should be no-ops when disabled."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            db = StatsDatabase(db_path=db_path, enabled=False)

            # These should not raise exceptions
            db.start_run("run1", "taskfile1")
            db.record_task(
                "run1", "task1", "inst1", "proc1", {}, True, datetime.now(), datetime.now()
            )
            db.complete_run("run1")

            # Query methods should return empty results
            self.assertEqual(db.get_task_history("sig1"), [])
            self.assertEqual(db.get_run_results("run1"), [])
            self.assertIsNone(db.get_run_info("run1"))

            db.close()


class TestStatsDatabaseEnabled(TestCase):
    """Tests for StatsDatabase when enabled."""

    def setUp(self):
        """Create temporary database for each test."""
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test_stats.db")
        self.db = StatsDatabase(db_path=self.db_path, enabled=True)

    def tearDown(self):
        """Clean up database and all related files."""
        self.db.close()
        # Remove all files in tmpdir (including WAL files)
        import shutil

        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_enabled_database_creates_file(self):
        """Enabled database should create the SQLite file."""
        self.assertTrue(os.path.exists(self.db_path))

    def test_start_run_creates_record(self):
        """start_run should create a run record."""
        self.db.start_run("run1", "taskfile1", "/path/to/taskfile.txt", 5)
        run_info = self.db.get_run_info("run1")

        self.assertIsNotNone(run_info)
        self.assertEqual(run_info["run_id"], "run1")
        self.assertEqual(run_info["workflow"], "taskfile1")
        self.assertEqual(run_info["taskfile_path"], "/path/to/taskfile.txt")
        self.assertEqual(run_info["task_count"], 5)

    def test_record_task_creates_record(self):
        """record_task should create a task result record."""
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=5)

        self.db.start_run("run1", "taskfile1")
        self.db.record_task(
            run_id="run1",
            task_id="task1",
            instance="inst1",
            process="proc1",
            parameters={"p1": "v1"},
            success=True,
            start_time=start_time,
            end_time=end_time,
            retry_count=2,
        )

        results = self.db.get_run_results("run1")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["task_id"], "task1")
        self.assertEqual(results[0]["instance"], "inst1")
        self.assertEqual(results[0]["process"], "proc1")
        self.assertEqual(results[0]["status"], "Success")
        self.assertEqual(results[0]["retry_count"], 2)
        self.assertAlmostEqual(results[0]["duration_seconds"], 5.0, places=1)

    def test_record_failed_task(self):
        """record_task should record failure status and error message."""
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)

        self.db.start_run("run1", "taskfile1")
        self.db.record_task(
            run_id="run1",
            task_id="task1",
            instance="inst1",
            process="proc1",
            parameters={},
            success=False,
            start_time=start_time,
            end_time=end_time,
            error_message="Process failed with status 1",
        )

        results = self.db.get_run_results("run1")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "Fail")
        self.assertEqual(results[0]["error_message"], "Process failed with status 1")

    def test_complete_run_updates_record(self):
        """complete_run should update the run record with final status."""
        self.db.start_run("run1", "taskfile1")
        self.db.complete_run("run1", status="Success", success_count=3, failure_count=1)

        run_info = self.db.get_run_info("run1")
        self.assertEqual(run_info["status"], "Success")
        self.assertEqual(run_info["success_count"], 3)
        self.assertEqual(run_info["failure_count"], 1)
        self.assertIsNotNone(run_info["end_time"])

    def test_get_task_history_returns_successful_tasks(self):
        """get_task_history should return only successful executions."""
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=5)

        self.db.start_run("run1", "taskfile1")

        # Record successful task
        self.db.record_task(
            run_id="run1",
            task_id="task1",
            instance="inst1",
            process="proc1",
            parameters={"p1": "v1"},
            success=True,
            start_time=start_time,
            end_time=end_time,
        )

        # Record failed task with same signature
        self.db.record_task(
            run_id="run1",
            task_id="task2",
            instance="inst1",
            process="proc1",
            parameters={"p1": "v1"},
            success=False,
            start_time=start_time,
            end_time=end_time,
            error_message="Failed",
        )

        signature = calculate_task_signature("inst1", "proc1", {"p1": "v1"})
        history = self.db.get_task_history(signature)

        # Should only return the successful task
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["status"], "Success")

    def test_get_task_history_respects_limit(self):
        """get_task_history should respect the limit parameter."""
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)

        self.db.start_run("run1", "taskfile1")

        # Record 5 successful tasks
        for i in range(5):
            self.db.record_task(
                run_id="run1",
                task_id=f"task{i}",
                instance="inst1",
                process="proc1",
                parameters={},
                success=True,
                start_time=start_time + timedelta(seconds=i),
                end_time=end_time + timedelta(seconds=i),
            )

        signature = calculate_task_signature("inst1", "proc1", {})
        history = self.db.get_task_history(signature, limit=3)

        self.assertEqual(len(history), 3)


class TestStatsDatabaseRetention(TestCase):
    """Tests for data retention cleanup."""

    def test_cleanup_old_data_deletes_old_runs(self):
        """cleanup_old_data should delete runs older than retention period."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            db = StatsDatabase(db_path=db_path, enabled=True)

            # Create a run with old start_time by directly manipulating the DB
            old_time = (datetime.now() - timedelta(days=100)).isoformat()
            db._conn.execute(
                "INSERT INTO runs (run_id, workflow, start_time) VALUES (?, ?, ?)",
                ("old_run", "taskfile1", old_time),
            )
            db._conn.commit()

            # Create a recent run
            db.start_run("recent_run", "taskfile1")

            # Cleanup with 90 day retention
            deleted = db.cleanup_old_data(90)

            self.assertEqual(deleted, 1)
            self.assertIsNone(db.get_run_info("old_run"))
            self.assertIsNotNone(db.get_run_info("recent_run"))

            db.close()

    def test_cleanup_with_zero_retention_does_nothing(self):
        """cleanup_old_data with 0 retention should not delete anything."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            db = StatsDatabase(db_path=db_path, enabled=True)

            db.start_run("run1", "taskfile1")
            deleted = db.cleanup_old_data(0)

            self.assertEqual(deleted, 0)
            self.assertIsNotNone(db.get_run_info("run1"))

            db.close()


class TestCreateStatsDatabase(TestCase):
    """Tests for the factory function."""

    def test_factory_creates_disabled_database(self):
        """Factory should create disabled database when enabled=False."""
        db = create_stats_database(enabled=False)
        self.assertFalse(db.enabled)
        db.close()

    def test_factory_creates_enabled_database(self):
        """Factory should create enabled database when enabled=True."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            db = create_stats_database(enabled=True, db_path=db_path)
            self.assertTrue(db.enabled)
            self.assertTrue(os.path.exists(db_path))
            db.close()

    def test_factory_runs_cleanup(self):
        """Factory should run cleanup when enabled."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")

            # Create initial database with old data
            db1 = StatsDatabase(db_path=db_path, enabled=True)
            old_time = (datetime.now() - timedelta(days=100)).isoformat()
            db1._conn.execute(
                "INSERT INTO runs (run_id, workflow, start_time) VALUES (?, ?, ?)",
                ("old_run", "taskfile1", old_time),
            )
            db1._conn.commit()
            db1.close()

            # Factory should clean up old data
            db2 = create_stats_database(enabled=True, db_path=db_path, retention_days=90)
            self.assertIsNone(db2.get_run_info("old_run"))
            db2.close()


class TestStatsDatabaseContextManager(TestCase):
    """Tests for context manager protocol."""

    def test_context_manager_closes_connection(self):
        """Context manager should close connection on exit."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")

            with StatsDatabase(db_path=db_path, enabled=True) as db:
                db.start_run("run1", "taskfile1")
                self.assertIsNotNone(db._conn)

            # Connection should be closed after exiting context
            self.assertIsNone(db._conn)


class TestStatsDatabaseMetadataAndSettings(TestCase):
    """Tests for taskfile metadata and settings storage."""

    def setUp(self):
        """Create temporary database for each test."""
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test_stats.db")
        self.db = StatsDatabase(db_path=self.db_path, enabled=True)

    def tearDown(self):
        """Clean up database and all related files."""
        self.db.close()
        import shutil

        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_start_run_stores_metadata(self):
        """start_run should store taskfile metadata fields."""
        self.db.start_run(
            run_id="run1",
            workflow="taskfile1",
            taskfile_path="/path/to/tasks.json",
            task_count=10,
            taskfile_name="Daily Load",
            taskfile_description="Daily data load process",
            taskfile_author="admin@company.com",
        )
        run_info = self.db.get_run_info("run1")

        self.assertEqual(run_info["taskfile_name"], "Daily Load")
        self.assertEqual(run_info["taskfile_description"], "Daily data load process")
        self.assertEqual(run_info["taskfile_author"], "admin@company.com")

    def test_start_run_stores_settings(self):
        """start_run should store taskfile settings fields."""
        self.db.start_run(
            run_id="run1",
            workflow="taskfile1",
            max_workers=5,
            retries=3,
            result_file="results.csv",
            exclusive=True,
            optimize=False,
        )
        run_info = self.db.get_run_info("run1")

        self.assertEqual(run_info["max_workers"], 5)
        self.assertEqual(run_info["retries"], 3)
        self.assertEqual(run_info["result_file"], "results.csv")
        self.assertTrue(run_info["exclusive"])
        self.assertFalse(run_info["optimize"])

    def test_start_run_handles_none_boolean_settings(self):
        """start_run should handle None boolean settings correctly."""
        self.db.start_run(
            run_id="run1",
            workflow="taskfile1",
            exclusive=None,
            optimize=None,
        )
        run_info = self.db.get_run_info("run1")

        self.assertIsNone(run_info["exclusive"])
        self.assertIsNone(run_info["optimize"])


class TestStatsDatabaseTaskConfig(TestCase):
    """Tests for task configuration storage."""

    def setUp(self):
        """Create temporary database for each test."""
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test_stats.db")
        self.db = StatsDatabase(db_path=self.db_path, enabled=True)

    def tearDown(self):
        """Clean up database and all related files."""
        self.db.close()
        import shutil

        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_record_task_stores_predecessors(self):
        """record_task should store predecessors as JSON."""
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=5)

        self.db.start_run("run1", "taskfile1")
        self.db.record_task(
            run_id="run1",
            task_id="task3",
            instance="inst1",
            process="proc1",
            parameters={},
            success=True,
            start_time=start_time,
            end_time=end_time,
            predecessors=["task1", "task2"],
        )

        results = self.db.get_run_results("run1")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["predecessors"], '["task1", "task2"]')

    def test_record_task_stores_stage(self):
        """record_task should store stage field."""
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=5)

        self.db.start_run("run1", "taskfile1")
        self.db.record_task(
            run_id="run1",
            task_id="task1",
            instance="inst1",
            process="proc1",
            parameters={},
            success=True,
            start_time=start_time,
            end_time=end_time,
            stage="data_load",
        )

        results = self.db.get_run_results("run1")
        self.assertEqual(results[0]["stage"], "data_load")

    def test_record_task_stores_boolean_config_fields(self):
        """record_task should store boolean config fields correctly."""
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=5)

        self.db.start_run("run1", "taskfile1")
        self.db.record_task(
            run_id="run1",
            task_id="task1",
            instance="inst1",
            process="proc1",
            parameters={},
            success=True,
            start_time=start_time,
            end_time=end_time,
            safe_retry=True,
            cancel_at_timeout=False,
            require_predecessor_success=True,
            succeed_on_minor_errors=False,
        )

        results = self.db.get_run_results("run1")
        self.assertTrue(results[0]["safe_retry"])
        self.assertFalse(results[0]["cancel_at_timeout"])
        self.assertTrue(results[0]["require_predecessor_success"])
        self.assertFalse(results[0]["succeed_on_minor_errors"])

    def test_record_task_stores_timeout(self):
        """record_task should store timeout field."""
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=5)

        self.db.start_run("run1", "taskfile1")
        self.db.record_task(
            run_id="run1",
            task_id="task1",
            instance="inst1",
            process="proc1",
            parameters={},
            success=True,
            start_time=start_time,
            end_time=end_time,
            timeout=300,
        )

        results = self.db.get_run_results("run1")
        self.assertEqual(results[0]["timeout"], 300)

    def test_record_task_handles_none_config_fields(self):
        """record_task should handle None config fields correctly."""
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=5)

        self.db.start_run("run1", "taskfile1")
        self.db.record_task(
            run_id="run1",
            task_id="task1",
            instance="inst1",
            process="proc1",
            parameters={},
            success=True,
            start_time=start_time,
            end_time=end_time,
            # All config fields default to None
        )

        results = self.db.get_run_results("run1")
        self.assertIsNone(results[0]["predecessors"])
        self.assertIsNone(results[0]["stage"])
        self.assertIsNone(results[0]["safe_retry"])
        self.assertIsNone(results[0]["timeout"])
        self.assertIsNone(results[0]["cancel_at_timeout"])
        self.assertIsNone(results[0]["require_predecessor_success"])
        self.assertIsNone(results[0]["succeed_on_minor_errors"])


class TestStatsDatabaseTaskfileId(TestCase):
    """Tests for workflow in task_results."""

    def test_record_task_stores_workflow(self):
        """record_task() should store workflow for TM1 cube alignment."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            db = StatsDatabase(db_path=db_path, enabled=True)

            db.start_run("run1", "taskfile_abc")
            start = datetime.now()
            end = start + timedelta(seconds=5)

            db.record_task(
                run_id="run1",
                task_id="task1",
                instance="srv01",
                process="process1",
                parameters={"p1": "v1"},
                success=True,
                start_time=start,
                end_time=end,
                workflow="taskfile_abc",
            )

            results = db.get_run_results("run1")
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]["workflow"], "taskfile_abc")
            db.close()

    def test_record_task_handles_none_workflow(self):
        """record_task() should handle None workflow."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            db = StatsDatabase(db_path=db_path, enabled=True)

            db.start_run("run1", "taskfile1")
            start = datetime.now()
            end = start + timedelta(seconds=5)

            db.record_task(
                run_id="run1",
                task_id="task1",
                instance="srv01",
                process="process1",
                parameters=None,
                success=True,
                start_time=start,
                end_time=end,
                # workflow not provided (defaults to None)
            )

            results = db.get_run_results("run1")
            self.assertEqual(len(results), 1)
            self.assertIsNone(results[0]["workflow"])
            db.close()

    def test_batch_record_tasks_stores_workflow(self):
        """batch_record_tasks() should store workflow for each task."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            db = StatsDatabase(db_path=db_path, enabled=True)

            db.start_run("run1", "taskfile_xyz")
            start = datetime.now()
            end = start + timedelta(seconds=5)

            tasks = [
                {
                    "run_id": "run1",
                    "workflow": "taskfile_xyz",
                    "task_id": "task1",
                    "instance": "srv01",
                    "process": "process1",
                    "parameters": None,
                    "success": True,
                    "start_time": start,
                    "end_time": end,
                },
                {
                    "run_id": "run1",
                    "workflow": "taskfile_xyz",
                    "task_id": "task2",
                    "instance": "srv01",
                    "process": "process2",
                    "parameters": {"p1": "v1"},
                    "success": True,
                    "start_time": start,
                    "end_time": end,
                },
            ]
            db.batch_record_tasks(tasks)

            results = db.get_run_results("run1")
            self.assertEqual(len(results), 2)
            self.assertEqual(results[0]["workflow"], "taskfile_xyz")
            self.assertEqual(results[1]["workflow"], "taskfile_xyz")
            db.close()
