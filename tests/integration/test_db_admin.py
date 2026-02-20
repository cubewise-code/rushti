"""Integration tests for database administration with real SQLite database.

Tests db_admin module operations against a real SQLite database to verify
end-to-end functionality of stats management, querying, and maintenance.
"""

import csv
import os
import tempfile
import unittest
from datetime import datetime, timedelta

from rushti.db_admin import (
    clear_before_date,
    clear_run,
    clear_workflow,
    export_to_csv,
    get_db_stats,
    get_workflow_stats,
    list_runs,
    list_workflows,
    list_tasks,
    show_run_details,
    show_task_history,
    vacuum_database,
)
from rushti.stats import StatsDatabase


class TestDatabaseAdminIntegration(unittest.TestCase):
    """Integration tests for database administration operations."""

    def setUp(self):
        """Create a temporary database and populate with real data."""
        self.temp_db = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".db")
        self.temp_db.close()
        self.db_path = self.temp_db.name

        # Create and populate database using StatsDatabase
        self.stats_db = StatsDatabase(db_path=self.db_path, enabled=True)

        # Simulate real execution data
        now = datetime.now()
        yesterday = now - timedelta(days=1)
        week_ago = now - timedelta(days=7)

        # Start run 1
        self.stats_db.start_run(run_id="20240101_100000", workflow="daily-etl")

        # Record tasks for run 1
        self.stats_db.record_task(
            run_id="20240101_100000",
            task_id="load_data",
            instance="tm1srv01",
            process="Load.Customers",
            parameters={"pDataSource": "SQL"},
            success=True,
            start_time=week_ago,
            end_time=week_ago + timedelta(seconds=5),
            retry_count=0,
            error_message=None,
            workflow="daily-etl",
        )

        self.stats_db.record_task(
            run_id="20240101_100000",
            task_id="process_data",
            instance="tm1srv01",
            process="Process.Aggregates",
            parameters={},
            success=True,
            start_time=week_ago + timedelta(seconds=5),
            end_time=week_ago + timedelta(seconds=10),
            retry_count=0,
            error_message=None,
            workflow="daily-etl",
        )

        # Complete run 1
        self.stats_db.complete_run(run_id="20240101_100000", success_count=2, failure_count=0)

        # Start run 2 (with a failure)
        self.stats_db.start_run(run_id="20240102_100000", workflow="daily-etl")

        self.stats_db.record_task(
            run_id="20240102_100000",
            task_id="load_data",
            instance="tm1srv01",
            process="Load.Customers",
            parameters={"pDataSource": "SQL"},
            success=False,
            start_time=yesterday,
            end_time=yesterday + timedelta(seconds=2),
            retry_count=1,
            error_message="Connection timeout",
            workflow="daily-etl",
        )

        self.stats_db.complete_run(run_id="20240102_100000", success_count=0, failure_count=1)

        # Create a different taskfile
        self.stats_db.start_run(run_id="20240103_100000", workflow="monthly-reports")

        self.stats_db.record_task(
            run_id="20240103_100000",
            task_id="generate_report",
            instance="tm1srv02",
            process="Report.Monthly",
            parameters={"pMonth": "01"},
            success=True,
            start_time=now,
            end_time=now + timedelta(seconds=15),
            retry_count=0,
            error_message=None,
            workflow="monthly-reports",
        )

        self.stats_db.complete_run(run_id="20240103_100000", success_count=1, failure_count=0)

    def tearDown(self):
        """Clean up temporary database."""
        self.stats_db.close()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)

    def test_get_db_stats_with_real_data(self):
        """Test getting overall database statistics."""
        stats = get_db_stats(self.db_path)

        self.assertTrue(stats["exists"])
        self.assertEqual(stats["total_records"], 4)  # 4 task results
        self.assertEqual(stats["workflow_count"], 2)  # 2 taskfiles
        self.assertEqual(stats["run_count"], 3)  # 3 runs
        self.assertEqual(stats["unique_tasks"], 3)  # 3 unique task signatures
        self.assertEqual(stats["success_rate"], 75.0)  # 3 success, 1 fail

    def test_get_taskfile_stats_with_real_data(self):
        """Test getting taskfile-specific statistics."""
        stats = get_workflow_stats("daily-etl", self.db_path)

        self.assertTrue(stats["exists"])
        self.assertEqual(stats["workflow"], "daily-etl")
        self.assertEqual(stats["total_records"], 3)
        self.assertEqual(stats["run_count"], 2)
        self.assertEqual(stats["unique_tasks"], 2)
        self.assertAlmostEqual(stats["success_rate"], 66.7, places=1)

    def test_list_taskfiles_with_real_data(self):
        """Test listing all taskfiles."""
        taskfiles = list_workflows(self.db_path)

        self.assertEqual(len(taskfiles), 2)
        workflow_ids = [tf["workflow"] for tf in taskfiles]
        self.assertIn("daily-etl", workflow_ids)
        self.assertIn("monthly-reports", workflow_ids)

        # Check taskfile details
        daily_etl = next(tf for tf in taskfiles if tf["workflow"] == "daily-etl")
        self.assertEqual(daily_etl["run_count"], 2)
        self.assertEqual(daily_etl["record_count"], 3)

    def test_list_runs_with_real_data(self):
        """Test listing runs for a taskfile."""
        runs = list_runs("daily-etl", self.db_path)

        self.assertEqual(len(runs), 2)
        # Most recent first
        self.assertEqual(runs[0]["run_id"], "20240102_100000")
        self.assertEqual(runs[1]["run_id"], "20240101_100000")

        # Check run details
        self.assertEqual(runs[0]["success_count"], 0)
        self.assertEqual(runs[1]["success_count"], 2)

    def test_list_tasks_with_real_data(self):
        """Test listing unique tasks for a taskfile."""
        tasks = list_tasks("daily-etl", self.db_path)

        self.assertEqual(len(tasks), 2)
        task_ids = [t["task_id"] for t in tasks]
        self.assertIn("load_data", task_ids)
        self.assertIn("process_data", task_ids)

        # Check task details
        load_task = next(t for t in tasks if t["task_id"] == "load_data")
        self.assertEqual(load_task["instance"], "tm1srv01")
        self.assertEqual(load_task["process"], "Load.Customers")
        self.assertEqual(load_task["run_count"], 2)  # Ran in both runs

    def test_clear_taskfile_with_real_data(self):
        """Test clearing all data for a taskfile."""
        # Clear daily-etl
        count = clear_workflow("daily-etl", self.db_path, dry_run=False)

        self.assertEqual(count, 3)  # 3 records deleted

        # Verify deletion
        stats = get_db_stats(self.db_path)
        self.assertEqual(stats["total_records"], 1)  # Only monthly-reports remains
        self.assertEqual(stats["workflow_count"], 1)

    def test_clear_run_with_real_data(self):
        """Test clearing all data for a specific run."""
        count = clear_run("20240101_100000", self.db_path, dry_run=False)

        self.assertEqual(count, 2)  # 2 task results deleted

        # Verify task results were deleted (run metadata may remain)
        stats = get_db_stats(self.db_path)
        self.assertEqual(stats["total_records"], 2)  # Only 2 task results remain

    def test_clear_before_date_with_real_data(self):
        """Test clearing data before a specific date."""
        # Clear data older than 2 days
        cutoff = (datetime.now() - timedelta(days=2)).isoformat()
        count = clear_before_date(cutoff, self.db_path, dry_run=False)

        self.assertGreaterEqual(count, 2)  # Week-old data should be cleared

        # Verify remaining data is recent
        stats = get_db_stats(self.db_path)
        self.assertLessEqual(stats["total_records"], 2)

    def test_vacuum_database_with_real_data(self):
        """Test database vacuum operation."""
        # Delete some data first
        clear_run("20240101_100000", self.db_path, dry_run=False)

        # Vacuum
        size_before, size_after = vacuum_database(self.db_path)

        self.assertGreater(size_before, 0)
        self.assertGreater(size_after, 0)
        # Vacuum should reduce or maintain size
        self.assertLessEqual(size_after, size_before)

    def test_export_to_csv_all_data(self):
        """Test exporting all data to CSV."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            csv_path = f.name

        try:
            count = export_to_csv(csv_path, db_path=self.db_path)

            self.assertEqual(count, 4)  # All 4 task results
            self.assertTrue(os.path.exists(csv_path))

            # Verify CSV content
            with open(csv_path, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                self.assertEqual(len(rows), 4)

                # Check first row has expected fields
                self.assertIn("task_id", rows[0])
                self.assertIn("status", rows[0])
                self.assertIn("duration_seconds", rows[0])

        finally:
            if os.path.exists(csv_path):
                os.unlink(csv_path)

    def test_export_to_csv_filtered_by_taskfile(self):
        """Test exporting with taskfile filter."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            csv_path = f.name

        try:
            count = export_to_csv(csv_path, workflow="daily-etl", db_path=self.db_path)

            self.assertEqual(count, 3)  # Only daily-etl records

        finally:
            if os.path.exists(csv_path):
                os.unlink(csv_path)

    def test_show_run_details_with_real_data(self):
        """Test showing detailed run information."""
        details = show_run_details("20240101_100000", self.db_path)

        self.assertTrue(details["exists"])
        self.assertEqual(details["run_id"], "20240101_100000")
        self.assertEqual(details["workflow"], "daily-etl")
        self.assertEqual(details["success_count"], 2)
        self.assertEqual(details["error_count"], 0)
        # Verify task details
        self.assertEqual(len(details["tasks"]), 2)

        # Check task details
        task_ids = [t["task_id"] for t in details["tasks"]]
        self.assertIn("load_data", task_ids)
        self.assertIn("process_data", task_ids)

    def test_show_run_details_with_failures(self):
        """Test showing run details for a run with failures."""
        details = show_run_details("20240102_100000", self.db_path)

        self.assertTrue(details["exists"])
        self.assertEqual(details["success_count"], 0)
        self.assertEqual(details["error_count"], 1)
        self.assertEqual(details["success_rate"], 0.0)

        # Check error message is included
        failed_task = details["tasks"][0]
        self.assertEqual(failed_task["status"], "Fail")
        self.assertIn("Connection timeout", failed_task["error"])

    def test_show_task_history_with_real_data(self):
        """Test showing task execution history."""
        # Get task signature for load_data task
        tasks = list_tasks("daily-etl", self.db_path)
        load_task = next(t for t in tasks if t["task_id"] == "load_data")
        signature = load_task["task_signature"]

        history = show_task_history(signature, self.db_path)

        self.assertTrue(history["exists"])
        self.assertEqual(history["task_id"], "load_data")
        self.assertEqual(history["instance"], "tm1srv01")
        self.assertEqual(history["process"], "Load.Customers")
        self.assertEqual(history["execution_count"], 2)  # Ran twice
        self.assertEqual(len(history["executions"]), 2)

        # Check executions are ordered by time (most recent first)
        self.assertEqual(history["executions"][0]["run_id"], "20240102_100000")
        self.assertEqual(history["executions"][1]["run_id"], "20240101_100000")

        # Check status
        self.assertEqual(history["executions"][0]["status"], "Fail")
        self.assertEqual(history["executions"][1]["status"], "Success")


if __name__ == "__main__":
    unittest.main()
