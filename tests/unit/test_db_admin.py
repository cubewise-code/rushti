"""Unit tests for database administration utilities.

Tests the db_admin module which provides SQLite database management
for the RushTI stats database including viewing statistics, listing data,
clearing records, and maintenance operations.
"""

import os
import tempfile
import unittest
from datetime import datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

from rushti.stats import StatsDatabase

from rushti.db_admin import (
    clear_all,
    clear_before_date,
    clear_run,
    clear_workflow,
    export_to_csv,
    get_db_stats,
    get_visualization_data,
    get_workflow_stats,
    list_runs,
    list_tasks,
    list_workflows,
    show_run_details,
    show_task_history,
    vacuum_database,
)


class TestDatabaseAdminUtilities(unittest.TestCase):
    """Tests for database admin utility functions."""

    def setUp(self):
        """Create a temporary database with sample data for testing."""
        self.temp_db = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".db")
        self.temp_db.close()
        self.db_path = self.temp_db.name

        # Use StatsDatabase to create the full schema and open a managed connection.
        self.stats_db = StatsDatabase(db_path=self.db_path, enabled=True)
        conn = self.stats_db._conn
        cursor = conn.cursor()

        # Insert sample data
        now = datetime.now()
        yesterday = now - timedelta(days=1)
        week_ago = now - timedelta(days=7)

        # Insert runs first (task_results has a FK on run_id)
        run_data = [
            ("run1", "taskfile1", week_ago.isoformat(), week_ago.isoformat(), 3.5, 2, 2, 0),
            ("run2", "taskfile1", yesterday.isoformat(), yesterday.isoformat(), 1.0, 1, 0, 1),
            ("run3", "taskfile2", now.isoformat(), now.isoformat(), 3.0, 1, 1, 0),
        ]

        cursor.executemany(
            """
            INSERT INTO runs
            (run_id, workflow, start_time, end_time, duration_seconds,
             task_count, success_count, failure_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            run_data,
        )

        # Sample task results
        sample_data = [
            (
                "run1",
                "taskfile1",
                "task1",
                "sig1",
                "tm1srv01",
                "proc1",
                "{}",
                "Success",
                week_ago.isoformat(),
                week_ago.isoformat(),
                1.5,
                0,
                None,
            ),
            (
                "run1",
                "taskfile1",
                "task2",
                "sig2",
                "tm1srv01",
                "proc2",
                "{}",
                "Success",
                week_ago.isoformat(),
                week_ago.isoformat(),
                2.0,
                0,
                None,
            ),
            (
                "run2",
                "taskfile1",
                "task1",
                "sig1",
                "tm1srv01",
                "proc1",
                "{}",
                "Fail",
                yesterday.isoformat(),
                yesterday.isoformat(),
                1.0,
                1,
                "Error message",
            ),
            (
                "run3",
                "taskfile2",
                "task1",
                "sig3",
                "tm1srv02",
                "proc3",
                "{}",
                "Success",
                now.isoformat(),
                now.isoformat(),
                3.0,
                0,
                None,
            ),
        ]

        cursor.executemany(
            """
            INSERT INTO task_results
            (run_id, workflow, task_id, task_signature, instance, process, parameters,
             status, start_time, end_time, duration_seconds, retry_count, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            sample_data,
        )

        conn.commit()

    def tearDown(self):
        """Remove temporary database."""
        self.stats_db.close()
        for suffix in ("", "-wal", "-shm"):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.unlink(path)

    def test_get_db_stats(self):
        """Test retrieving overall database statistics."""
        stats = get_db_stats(self.db_path)

        self.assertTrue(stats["exists"])
        self.assertEqual(stats["total_records"], 4)
        self.assertEqual(stats["workflow_count"], 2)
        self.assertEqual(stats["run_count"], 3)
        self.assertEqual(stats["unique_tasks"], 3)
        self.assertEqual(stats["success_rate"], 75.0)  # 3 success, 1 fail

    def test_get_db_stats_nonexistent(self):
        """Test stats for nonexistent database."""
        stats = get_db_stats("/nonexistent/db.db")

        self.assertFalse(stats["exists"])
        self.assertIn("message", stats)

    def test_get_workflow_stats(self):
        """Test retrieving statistics for a specific workflow."""
        stats = get_workflow_stats("taskfile1", self.db_path)

        self.assertTrue(stats["exists"])
        self.assertEqual(stats["total_records"], 3)
        self.assertEqual(stats["run_count"], 2)
        self.assertEqual(stats["unique_tasks"], 2)

    def test_get_workflow_stats_nonexistent(self):
        """Test stats for nonexistent workflow."""
        stats = get_workflow_stats("nonexistent", self.db_path)

        self.assertFalse(stats["exists"])
        self.assertIn("message", stats)

    def test_list_workflows(self):
        """Test listing all workflows."""
        workflows = list_workflows(self.db_path)

        self.assertEqual(len(workflows), 2)
        workflow_ids = [w["workflow"] for w in workflows]
        self.assertIn("taskfile1", workflow_ids)
        self.assertIn("taskfile2", workflow_ids)

    def test_list_runs(self):
        """Test listing runs for a workflow."""
        runs = list_runs("taskfile1", self.stats_db)

        self.assertEqual(len(runs), 2)
        self.assertEqual(runs[0]["task_count"], 1)  # Most recent first
        self.assertEqual(runs[1]["task_count"], 2)

    def test_list_tasks(self):
        """Test listing unique tasks for a workflow."""
        tasks = list_tasks("taskfile1", self.stats_db)

        self.assertEqual(len(tasks), 2)
        task_ids = [t["task_id"] for t in tasks]
        self.assertIn("task1", task_ids)
        self.assertIn("task2", task_ids)

    def test_get_visualization_data_include_all_workflows_sqlite(self):
        """include_all_workflows should embed all workflows in one payload (SQLite path)."""
        data = get_visualization_data(
            "taskfile1",
            self.db_path,
            include_all_workflows=True,
        )

        self.assertTrue(data["exists"])
        self.assertEqual(data["workflow"], "taskfile1")
        self.assertEqual(len(data["runs"]), 3)
        self.assertEqual(len(data["task_results"]), 4)

        workflows = {r["workflow"] for r in data["runs"]}
        self.assertIn("taskfile1", workflows)
        self.assertIn("taskfile2", workflows)

    def test_get_visualization_data_include_all_requires_selected_workflow(self):
        """include_all_workflows still fails when selected workflow does not exist."""
        data = get_visualization_data(
            "missing-workflow",
            self.db_path,
            include_all_workflows=True,
        )

        self.assertFalse(data["exists"])
        self.assertIn("No runs found for workflow", data["message"])

    def test_clear_workflow_dry_run(self):
        """Test dry run of clearing workflow data."""
        count = clear_workflow("taskfile1", self.db_path, dry_run=True)

        self.assertEqual(count, 3)  # Would delete 3 records

        # Verify nothing was deleted
        stats = get_db_stats(self.db_path)
        self.assertEqual(stats["total_records"], 4)

    def test_clear_workflow(self):
        """Test clearing all data for a workflow."""
        count = clear_workflow("taskfile1", self.db_path, dry_run=False)

        self.assertEqual(count, 3)  # Deleted 3 records

        # Verify deletion
        stats = get_db_stats(self.db_path)
        self.assertEqual(stats["total_records"], 1)  # Only taskfile2 remains

    def test_clear_run(self):
        """Test clearing all data for a run."""
        count = clear_run("run1", self.db_path, dry_run=False)

        self.assertEqual(count, 2)  # Deleted 2 records

        # Verify deletion
        stats = get_db_stats(self.db_path)
        self.assertEqual(stats["total_records"], 2)

    def test_clear_before_date(self):
        """Test clearing data before a specific date."""
        # Clear data older than 2 days
        cutoff = (datetime.now() - timedelta(days=2)).isoformat()
        count = clear_before_date(cutoff, self.db_path, dry_run=False)

        self.assertGreaterEqual(count, 2)  # Should clear week-old data

    def test_clear_all(self):
        """Test clearing all database data."""
        count = clear_all(self.db_path, dry_run=False)

        self.assertEqual(count, 4)  # All records deleted

        # Verify deletion
        stats = get_db_stats(self.db_path)
        self.assertEqual(stats["total_records"], 0)

    def test_vacuum_database(self):
        """Test database vacuum operation."""
        size_before, size_after = vacuum_database(self.db_path)

        self.assertGreaterEqual(size_before, 0)
        self.assertGreaterEqual(size_after, 0)
        # After vacuum, size should be <= original size
        self.assertLessEqual(size_after, size_before)

    def test_export_to_csv(self):
        """Test exporting task results to CSV."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            csv_path = f.name

        try:
            count = export_to_csv(csv_path, db_path=self.db_path)

            self.assertEqual(count, 4)  # All records exported
            self.assertTrue(os.path.exists(csv_path))

            # Verify CSV content
            with open(csv_path, "r") as f:
                lines = f.readlines()
                self.assertEqual(len(lines), 5)  # Header + 4 records

        finally:
            if os.path.exists(csv_path):
                os.unlink(csv_path)

    def test_export_to_csv_with_filters(self):
        """Test exporting with workflow filter."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            csv_path = f.name

        try:
            count = export_to_csv(csv_path, workflow="taskfile1", db_path=self.db_path)

            self.assertEqual(count, 3)  # Only taskfile1 records

        finally:
            if os.path.exists(csv_path):
                os.unlink(csv_path)

    def test_show_run_details(self):
        """Test showing detailed run information."""
        details = show_run_details("run1", self.db_path)

        self.assertTrue(details["exists"])
        self.assertEqual(details["run_id"], "run1")
        self.assertEqual(details["workflow"], "taskfile1")
        self.assertEqual(details["task_count"], 2)
        self.assertEqual(details["success_count"], 2)
        self.assertEqual(len(details["tasks"]), 2)

    def test_show_run_details_nonexistent(self):
        """Test showing details for nonexistent run."""
        details = show_run_details("nonexistent", self.db_path)

        self.assertFalse(details["exists"])
        self.assertIn("message", details)

    def test_show_task_history(self):
        """Test showing task execution history."""
        history = show_task_history("sig1", self.db_path)

        self.assertTrue(history["exists"])
        self.assertEqual(history["task_signature"], "sig1")
        self.assertEqual(history["task_id"], "task1")
        self.assertEqual(history["execution_count"], 2)  # Ran twice
        self.assertEqual(len(history["executions"]), 2)

    def test_show_task_history_nonexistent(self):
        """Test showing history for nonexistent task."""
        history = show_task_history("nonexistent", self.db_path)

        self.assertFalse(history["exists"])
        self.assertIn("message", history)


class TestDatabaseAdminUtilitiesDDB(unittest.TestCase):
    """Tests for db_admin functions using a mock DynamoDB stats_db.

    These are symmetric with TestDatabaseAdminUtilities but exercise the
    DDB dispatch path in list_runs, list_tasks, and get_visualization_data.
    """

    def _make_mock_stats_db(self):
        """Return a MagicMock that mimics DynamoDBStatsDatabase method returns."""
        now = datetime.now()
        yesterday = now - timedelta(days=1)
        week_ago = now - timedelta(days=7)

        mock_db = MagicMock()

        # Two runs for taskfile1 ordered most-recent-first (as get_runs_for_workflow returns)
        mock_db.get_runs_for_workflow.return_value = [
            {
                "run_id": "run2",
                "start_time": yesterday.isoformat(),
                "end_time": yesterday.isoformat(),
                "duration_seconds": 1.0,
                "status": "Success",
                "task_count": 1,
                "success_count": 0,
                "failure_count": 1,
                "max_workers": 2,
            },
            {
                "run_id": "run1",
                "start_time": week_ago.isoformat(),
                "end_time": week_ago.isoformat(),
                "duration_seconds": 3.5,
                "status": "Success",
                "task_count": 2,
                "success_count": 2,
                "failure_count": 0,
                "max_workers": 2,
            },
        ]

        # Distinct signatures for taskfile1
        mock_db.get_workflow_signatures.return_value = ["sig1", "sig2"]

        _run1_tasks = [
            {
                "task_id": "task1",
                "task_signature": "sig1",
                "workflow": "taskfile1",
                "instance": "inst1",
                "process": "proc1",
                "parameters": "{}",
                "status": "Success",
                "start_time": week_ago.isoformat(),
                "end_time": week_ago.isoformat(),
                "duration_seconds": 1.5,
                "retry_count": 0,
                "error_message": None,
                "predecessors": None,
                "stage": None,
                "safe_retry": None,
                "timeout": None,
                "cancel_at_timeout": None,
                "require_predecessor_success": None,
                "succeed_on_minor_errors": None,
            },
            {
                "task_id": "task2",
                "task_signature": "sig2",
                "workflow": "taskfile1",
                "instance": "inst1",
                "process": "proc2",
                "parameters": "{}",
                "status": "Success",
                "start_time": week_ago.isoformat(),
                "end_time": week_ago.isoformat(),
                "duration_seconds": 2.0,
                "retry_count": 0,
                "error_message": None,
                "predecessors": None,
                "stage": None,
                "safe_retry": None,
                "timeout": None,
                "cancel_at_timeout": None,
                "require_predecessor_success": None,
                "succeed_on_minor_errors": None,
            },
        ]
        _run2_tasks = [
            {
                "task_id": "task1",
                "task_signature": "sig1",
                "workflow": "taskfile1",
                "instance": "inst1",
                "process": "proc1",
                "parameters": "{}",
                "status": "Fail",
                "start_time": yesterday.isoformat(),
                "end_time": yesterday.isoformat(),
                "duration_seconds": 1.0,
                "retry_count": 1,
                "error_message": "Error message",
                "predecessors": None,
                "stage": None,
                "safe_retry": None,
                "timeout": None,
                "cancel_at_timeout": None,
                "require_predecessor_success": None,
                "succeed_on_minor_errors": None,
            },
        ]
        mock_db.get_run_results.side_effect = lambda run_id: {
            "run1": _run1_tasks,
            "run2": _run2_tasks,
        }.get(run_id, [])

        # get_run_info returns raw DDB items (with Decimal numerics)
        mock_db.get_run_info.side_effect = lambda run_id: {
            "run1": {
                "run_id": "run1",
                "workflow": "taskfile1",
                "taskfile_path": "/path/tasks.txt",
                "start_time": week_ago.isoformat(),
                "end_time": week_ago.isoformat(),
                "duration_seconds": Decimal("3.5"),
                "status": "Success",
                "task_count": Decimal("2"),
                "success_count": Decimal("2"),
                "failure_count": Decimal("0"),
                "taskfile_name": "Daily Load",
                "taskfile_description": None,
                "taskfile_author": None,
                "max_workers": Decimal("2"),
                "retries": Decimal("0"),
                "result_file": None,
                "exclusive": None,
                "optimize": None,
            },
            "run2": {
                "run_id": "run2",
                "workflow": "taskfile1",
                "taskfile_path": "/path/tasks.txt",
                "start_time": yesterday.isoformat(),
                "end_time": yesterday.isoformat(),
                "duration_seconds": Decimal("1.0"),
                "status": "Success",
                "task_count": Decimal("1"),
                "success_count": Decimal("0"),
                "failure_count": Decimal("1"),
                "taskfile_name": "Daily Load",
                "taskfile_description": None,
                "taskfile_author": None,
                "max_workers": Decimal("2"),
                "retries": Decimal("0"),
                "result_file": None,
                "exclusive": None,
                "optimize": None,
            },
        }.get(run_id)

        return mock_db

    # ------------------------------------------------------------------
    # list_runs DDB path
    # ------------------------------------------------------------------

    def test_list_runs_returns_correct_count(self):
        runs = list_runs("taskfile1", self._make_mock_stats_db())
        self.assertEqual(len(runs), 2)

    def test_list_runs_most_recent_first(self):
        runs = list_runs("taskfile1", self._make_mock_stats_db())
        self.assertEqual(runs[0]["run_id"], "run2")
        self.assertEqual(runs[1]["run_id"], "run1")

    def test_list_runs_task_count(self):
        runs = list_runs("taskfile1", self._make_mock_stats_db())
        self.assertEqual(runs[0]["task_count"], 1)
        self.assertEqual(runs[1]["task_count"], 2)

    def test_list_runs_success_rate(self):
        runs = list_runs("taskfile1", self._make_mock_stats_db())
        self.assertAlmostEqual(runs[0]["success_rate"], 0.0)  # run2: 0/1
        self.assertAlmostEqual(runs[1]["success_rate"], 100.0)  # run1: 2/2

    def test_list_runs_total_duration_sums_task_results(self):
        runs = list_runs("taskfile1", self._make_mock_stats_db())
        # run2: 1 task × 1.0 s
        self.assertAlmostEqual(runs[0]["total_duration"], 1.0)
        # run1: 1.5 s + 2.0 s = 3.5 s
        self.assertAlmostEqual(runs[1]["total_duration"], 3.5)

    def test_list_runs_respects_limit(self):
        runs = list_runs("taskfile1", self._make_mock_stats_db(), limit=1)
        self.assertEqual(len(runs), 1)

    def test_list_runs_returns_required_keys(self):
        runs = list_runs("taskfile1", self._make_mock_stats_db())
        required = {"run_id", "start_time", "task_count", "success_rate", "total_duration"}
        self.assertTrue(required.issubset(runs[0].keys()))

    def test_list_runs_empty_when_no_runs(self):
        mock_db = MagicMock()
        mock_db.get_runs_for_workflow.return_value = []
        runs = list_runs("missing", mock_db)
        self.assertEqual(runs, [])

    # ------------------------------------------------------------------
    # list_tasks DDB path
    # ------------------------------------------------------------------

    def test_list_tasks_returns_unique_signatures(self):
        tasks = list_tasks("taskfile1", self._make_mock_stats_db())
        signatures = {t["task_signature"] for t in tasks}
        self.assertIn("sig1", signatures)
        self.assertIn("sig2", signatures)

    def test_list_tasks_aggregates_across_all_runs(self):
        tasks = list_tasks("taskfile1", self._make_mock_stats_db())
        sig1 = next(t for t in tasks if t["task_signature"] == "sig1")
        # sig1 appears in both run1 and run2
        self.assertEqual(sig1["execution_count"], 2)

    def test_list_tasks_success_rate(self):
        tasks = list_tasks("taskfile1", self._make_mock_stats_db())
        sig1 = next(t for t in tasks if t["task_signature"] == "sig1")
        self.assertAlmostEqual(sig1["success_rate"], 50.0)  # 1 of 2
        sig2 = next(t for t in tasks if t["task_signature"] == "sig2")
        self.assertAlmostEqual(sig2["success_rate"], 100.0)  # 1 of 1

    def test_list_tasks_avg_duration(self):
        tasks = list_tasks("taskfile1", self._make_mock_stats_db())
        sig1 = next(t for t in tasks if t["task_signature"] == "sig1")
        # (1.5 + 1.0) / 2 = 1.25
        self.assertAlmostEqual(sig1["avg_duration"], 1.25)

    def test_list_tasks_returns_required_keys(self):
        tasks = list_tasks("taskfile1", self._make_mock_stats_db())
        required = {"task_signature", "execution_count", "success_rate", "avg_duration"}
        self.assertTrue(required.issubset(tasks[0].keys()))

    def test_list_tasks_empty_when_no_runs(self):
        mock_db = MagicMock()
        mock_db.get_runs_for_workflow.return_value = []
        mock_db.get_workflow_signatures.return_value = []
        tasks = list_tasks("missing", mock_db)
        self.assertEqual(tasks, [])

    # ------------------------------------------------------------------
    # get_visualization_data DDB path
    # ------------------------------------------------------------------

    def test_get_visualization_data_exists(self):
        data = get_visualization_data("taskfile1", self._make_mock_stats_db())
        self.assertTrue(data["exists"])

    def test_get_visualization_data_run_count(self):
        data = get_visualization_data("taskfile1", self._make_mock_stats_db())
        self.assertEqual(len(data["runs"]), 2)

    def test_get_visualization_data_task_result_count(self):
        data = get_visualization_data("taskfile1", self._make_mock_stats_db())
        # run1 has 2 tasks, run2 has 1 task
        self.assertEqual(len(data["task_results"]), 3)

    def test_get_visualization_data_task_results_have_run_id(self):
        data = get_visualization_data("taskfile1", self._make_mock_stats_db())
        for tr in data["task_results"]:
            self.assertIn("run_id", tr)

    def test_get_visualization_data_converts_decimal_duration_to_float(self):
        data = get_visualization_data("taskfile1", self._make_mock_stats_db())
        for run in data["runs"]:
            if run.get("duration_seconds") is not None:
                self.assertIsInstance(run["duration_seconds"], float)

    def test_get_visualization_data_converts_decimal_task_count_to_int(self):
        data = get_visualization_data("taskfile1", self._make_mock_stats_db())
        for run in data["runs"]:
            if run.get("task_count") is not None:
                self.assertIsInstance(run["task_count"], int)

    def test_get_visualization_data_includes_run_metadata_from_run_info(self):
        data = get_visualization_data("taskfile1", self._make_mock_stats_db())
        run1 = next(r for r in data["runs"] if r["run_id"] == "run1")
        self.assertEqual(run1["taskfile_name"], "Daily Load")
        self.assertEqual(run1["taskfile_path"], "/path/tasks.txt")
        self.assertEqual(run1["workflow"], "taskfile1")

    def test_get_visualization_data_not_exists_when_no_runs(self):
        mock_db = MagicMock()
        mock_db.get_runs_for_workflow.return_value = []
        data = get_visualization_data("missing", mock_db)
        self.assertFalse(data["exists"])
        self.assertIn("message", data)


if __name__ == "__main__":
    unittest.main()
