"""Unit tests for the stats module."""

import os
import tempfile
import unittest
from datetime import datetime, timedelta
from decimal import Decimal
from unittest import TestCase
from unittest.mock import MagicMock, patch


from rushti.stats import (
    DynamoDBStatsDatabase,
    StatsDatabase,
    calculate_task_signature,
    create_stats_database,
)

try:
    import botocore.exceptions  # noqa: F401

    BOTOCORE_AVAILABLE = True
except ImportError:
    BOTOCORE_AVAILABLE = False


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

    def test_factory_invalid_backend_raises(self):
        """Factory should reject unsupported storage backends."""
        with self.assertRaises(ValueError):
            create_stats_database(enabled=True, backend="unknown_backend")


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


# ---------------------------------------------------------------------------
# DynamoDB backend tests
# ---------------------------------------------------------------------------

_MOCK_TASK_ITEM = {
    "run_id": "run1",
    "task_result_id": "tr1",
    "task_id": "task1",
    "task_signature": "sig1",
    "workflow": "taskfile1",
    "instance": "inst1",
    "process": "proc1",
    "parameters": '{"p1": "v1"}',
    "status": "Success",
    "start_time": "2024-01-01T10:00:00",
    "end_time": "2024-01-01T10:00:05",
    "duration_seconds": Decimal("5.123"),
    "retry_count": Decimal("2"),
    "error_message": None,
    "predecessors": None,
    "stage": None,
    "safe_retry": None,
    "timeout": None,
    "cancel_at_timeout": None,
    "require_predecessor_success": None,
    "succeed_on_minor_errors": None,
}

_MOCK_RUN_ITEM = {
    "run_id": "run1",
    "workflow": "taskfile1",
    "taskfile_path": "/path/tasks.txt",
    "start_time": "2024-01-01T10:00:00",
    "end_time": "2024-01-01T10:05:00",
    "duration_seconds": Decimal("300.5"),
    "status": "Success",
    "task_count": Decimal("2"),
    "success_count": Decimal("2"),
    "failure_count": Decimal("0"),
    "taskfile_name": "Daily Load",
    "taskfile_description": None,
    "taskfile_author": None,
    "max_workers": Decimal("4"),
    "retries": Decimal("0"),
    "result_file": None,
    "exclusive": None,
    "optimize": None,
}


def _make_ddb(enabled=True):
    """Create a DynamoDBStatsDatabase with _initialize_database patched out."""
    with patch.object(DynamoDBStatsDatabase, "_initialize_database"):
        db = DynamoDBStatsDatabase(enabled=enabled)
    db._runs_table = MagicMock()
    db._task_results_table = MagicMock()
    return db


class TestDynamoDBStatsDatabaseNormalization(TestCase):
    """Tests for _normalize_task_item type conversion (Decimal → Python types)."""

    def setUp(self):
        self.db = _make_ddb()

    def test_duration_seconds_converted_to_float(self):
        result = self.db._normalize_task_item(_MOCK_TASK_ITEM)
        self.assertIsInstance(result["duration_seconds"], float)
        self.assertAlmostEqual(result["duration_seconds"], 5.123, places=3)

    def test_retry_count_converted_to_int(self):
        result = self.db._normalize_task_item(_MOCK_TASK_ITEM)
        self.assertIsInstance(result["retry_count"], int)
        self.assertEqual(result["retry_count"], 2)

    def test_none_duration_stays_none(self):
        item = dict(_MOCK_TASK_ITEM, duration_seconds=None)
        result = self.db._normalize_task_item(item)
        self.assertIsNone(result["duration_seconds"])

    def test_none_retry_count_defaults_to_zero(self):
        item = dict(_MOCK_TASK_ITEM, retry_count=None)
        result = self.db._normalize_task_item(item)
        self.assertEqual(result["retry_count"], 0)

    def test_all_expected_keys_present(self):
        result = self.db._normalize_task_item(_MOCK_TASK_ITEM)
        expected_keys = {
            "workflow",
            "task_id",
            "task_signature",
            "instance",
            "process",
            "parameters",
            "status",
            "start_time",
            "end_time",
            "duration_seconds",
            "retry_count",
            "error_message",
            "predecessors",
            "stage",
        }
        self.assertTrue(expected_keys.issubset(result.keys()))


class TestDynamoDBStatsDatabasePagination(TestCase):
    """Tests for _query_all and _scan_all global Limit handling."""

    def setUp(self):
        self.db = _make_ddb()

    def _items(self, n):
        return [{"id": str(i)} for i in range(n)]

    def test_query_all_paginates_to_exhaustion_without_limit(self):
        table = MagicMock()
        table.query.side_effect = [
            {"Items": self._items(3), "LastEvaluatedKey": {"id": "2"}},
            {"Items": self._items(3)},
        ]
        items = self.db._query_all(table)
        self.assertEqual(len(items), 6)
        self.assertEqual(table.query.call_count, 2)

    def test_query_all_respects_limit_as_global_cap(self):
        table = MagicMock()
        table.query.side_effect = [
            {"Items": self._items(3), "LastEvaluatedKey": {"id": "2"}},
            {"Items": self._items(3)},
        ]
        items = self.db._query_all(table, Limit=4)
        self.assertEqual(len(items), 4)

    def test_query_all_does_not_forward_limit_to_dynamodb(self):
        table = MagicMock()
        table.query.return_value = {"Items": self._items(2)}
        self.db._query_all(table, Limit=10, ScanIndexForward=False)
        call_kwargs = table.query.call_args[1]
        self.assertNotIn("Limit", call_kwargs)
        self.assertIn("ScanIndexForward", call_kwargs)

    def test_scan_all_respects_limit_as_global_cap(self):
        table = MagicMock()
        table.scan.side_effect = [
            {"Items": self._items(3), "LastEvaluatedKey": {"id": "2"}},
            {"Items": self._items(3)},
        ]
        items = self.db._scan_all(table, Limit=5)
        self.assertEqual(len(items), 5)

    def test_scan_all_does_not_forward_limit_to_dynamodb(self):
        table = MagicMock()
        table.scan.return_value = {"Items": self._items(2)}
        self.db._scan_all(table, Limit=10)
        call_kwargs = table.scan.call_args[1]
        self.assertNotIn("Limit", call_kwargs)

    def test_query_all_stops_when_no_last_evaluated_key(self):
        table = MagicMock()
        table.query.return_value = {"Items": self._items(3)}  # no continuation
        items = self.db._query_all(table)
        self.assertEqual(len(items), 3)
        self.assertEqual(table.query.call_count, 1)


@unittest.skipUnless(BOTOCORE_AVAILABLE, "botocore not installed")
class TestDynamoDBStatsDatabaseFallback(TestCase):
    """Tests for GSI-missing fallback in get_task_history and get_runs_for_workflow."""

    def setUp(self):
        self.db = _make_ddb()
        # get_task_history / get_runs_for_workflow do `from boto3.dynamodb.conditions import Key`
        # inside the try block. Without boto3 installed that import itself raises
        # ModuleNotFoundError before our mock side_effect fires, so we inject a stub.
        self._boto3_patch = patch.dict(
            "sys.modules",
            {
                "boto3": MagicMock(),
                "boto3.dynamodb": MagicMock(),
                "boto3.dynamodb.conditions": MagicMock(),
            },
        )
        self._boto3_patch.start()

    def tearDown(self):
        self._boto3_patch.stop()

    def _client_error(self, code):
        from botocore.exceptions import ClientError

        return ClientError({"Error": {"Code": code, "Message": "test"}}, "Query")

    def test_get_task_history_falls_back_to_scan_on_validation_exception(self):
        self.db._task_results_table.query.side_effect = self._client_error("ValidationException")
        self.db._task_results_table.scan.return_value = {"Items": []}
        self.db.get_task_history("sig1")
        self.db._task_results_table.scan.assert_called()

    def test_get_task_history_falls_back_to_scan_on_resource_not_found(self):
        self.db._task_results_table.query.side_effect = self._client_error(
            "ResourceNotFoundException"
        )
        self.db._task_results_table.scan.return_value = {"Items": []}
        self.db.get_task_history("sig1")
        self.db._task_results_table.scan.assert_called()

    def test_get_task_history_reraises_access_denied(self):
        from botocore.exceptions import ClientError

        self.db._task_results_table.query.side_effect = self._client_error("AccessDeniedException")
        with self.assertRaises(ClientError):
            self.db.get_task_history("sig1")

    def test_get_runs_for_workflow_falls_back_to_scan_on_validation_exception(self):
        self.db._runs_table.query.side_effect = self._client_error("ValidationException")
        self.db._runs_table.scan.return_value = {"Items": []}
        self.db.get_runs_for_workflow("wf1")
        self.db._runs_table.scan.assert_called()

    def test_get_runs_for_workflow_reraises_access_denied(self):
        from botocore.exceptions import ClientError

        self.db._runs_table.query.side_effect = self._client_error("AccessDeniedException")
        with self.assertRaises(ClientError):
            self.db.get_runs_for_workflow("wf1")


class TestDynamoDBStatsDatabaseOperations(TestCase):
    """Tests for DynamoDBStatsDatabase CRUD operations via mocked boto3 tables."""

    def setUp(self):
        self.db = _make_ddb()
        # Several methods import from boto3.dynamodb.conditions lazily; stub it out.
        self._boto3_patch = patch.dict(
            "sys.modules",
            {
                "boto3": MagicMock(),
                "boto3.dynamodb": MagicMock(),
                "boto3.dynamodb.conditions": MagicMock(),
            },
        )
        self._boto3_patch.start()

    def tearDown(self):
        self._boto3_patch.stop()

    def test_disabled_database_does_not_call_put_item(self):
        db = _make_ddb(enabled=False)
        db.start_run("run1", "wf1")
        db._runs_table.put_item.assert_not_called()

    def test_disabled_methods_return_empty(self):
        db = _make_ddb(enabled=False)
        self.assertEqual(db.get_task_history("sig1"), [])
        self.assertEqual(db.get_run_results("run1"), [])
        self.assertIsNone(db.get_run_info("run1"))
        self.assertEqual(db.get_runs_for_workflow("wf1"), [])

    def test_start_run_puts_item_with_correct_fields(self):
        self.db.start_run("run1", "taskfile1", "/path/tasks.txt", 5)
        self.db._runs_table.put_item.assert_called_once()
        item = self.db._runs_table.put_item.call_args[1]["Item"]
        self.assertEqual(item["run_id"], "run1")
        self.assertEqual(item["workflow"], "taskfile1")
        self.assertEqual(item["task_count"], 5)

    def test_complete_run_updates_item(self):
        self.db.complete_run("run1", status="Success", success_count=3, failure_count=1)
        self.db._runs_table.update_item.assert_called_once()
        call_kwargs = self.db._runs_table.update_item.call_args[1]
        self.assertEqual(call_kwargs["Key"], {"run_id": "run1"})
        self.assertEqual(call_kwargs["ExpressionAttributeValues"][":status"], "Success")

    def test_get_run_info_queries_by_run_id(self):
        self.db._runs_table.get_item.return_value = {"Item": _MOCK_RUN_ITEM}
        result = self.db.get_run_info("run1")
        self.db._runs_table.get_item.assert_called_once_with(Key={"run_id": "run1"})
        self.assertEqual(result["run_id"], "run1")

    def test_get_run_info_returns_none_when_not_found(self):
        self.db._runs_table.get_item.return_value = {}
        result = self.db.get_run_info("missing")
        self.assertIsNone(result)

    def test_get_run_results_returns_float_duration_and_int_retry(self):
        self.db._task_results_table.query.return_value = {"Items": [_MOCK_TASK_ITEM]}
        results = self.db.get_run_results("run1")
        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0]["duration_seconds"], float)
        self.assertAlmostEqual(results[0]["duration_seconds"], 5.123, places=3)
        self.assertIsInstance(results[0]["retry_count"], int)
        self.assertEqual(results[0]["retry_count"], 2)

    def test_record_task_puts_item_with_correct_fields(self):
        start = datetime(2024, 1, 1, 10, 0, 0)
        end = start + timedelta(seconds=5)
        self.db.record_task(
            run_id="run1",
            task_id="task1",
            instance="inst1",
            process="proc1",
            parameters={"p": "v"},
            success=True,
            start_time=start,
            end_time=end,
        )
        self.db._task_results_table.put_item.assert_called_once()
        item = self.db._task_results_table.put_item.call_args[1]["Item"]
        self.assertEqual(item["run_id"], "run1")
        self.assertEqual(item["task_id"], "task1")
        self.assertEqual(item["status"], "Success")

    def test_record_failed_task_sets_fail_status(self):
        start = datetime(2024, 1, 1, 10, 0, 0)
        end = start + timedelta(seconds=1)
        self.db.record_task(
            run_id="run1",
            task_id="task1",
            instance="inst1",
            process="proc1",
            parameters={},
            success=False,
            start_time=start,
            end_time=end,
            error_message="Process failed",
        )
        item = self.db._task_results_table.put_item.call_args[1]["Item"]
        self.assertEqual(item["status"], "Fail")
        self.assertEqual(item["error_message"], "Process failed")
